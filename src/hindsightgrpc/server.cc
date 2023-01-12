/*
 * Copyright 2022 Max Planck Institute for Software Systems *
 */

#include "server.h"

#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>

#include <iostream>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <atomic>

#include <json.hpp>

#include "topology.h"
#include "../tracing/grpc_propagation.h"

#include "opentelemetry/trace/scope.h"
#include "opentelemetry/trace/span_startoptions.h"
#include "opentelemetry/trace/context.h"
#include "opentelemetry/context/runtime_context.h"
#include "opentelemetry/trace/span_metadata.h"
#include "opentelemetry/trace/propagation/detail/hex.h"

extern "C" {
  #include "hindsight.h"
  #include "common.h"
}

#include "hindsightgrpc.grpc.pb.h"

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;
using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using hindsightgrpc::HindsightGRPC;
using hindsightgrpc::ExecRequest;
using hindsightgrpc::ExecReply;
using opentelemetry::trace::Scope;
using opentelemetry::trace::TraceId;
using opentelemetry::trace::SpanId;
using opentelemetry::trace::TraceFlags;
using json = nlohmann::json;
namespace detail = opentelemetry::trace::propagation::detail;

// Hindsight is enabled or disabled based on command-line arguments
bool hindsight_enabled = false;
#define HINDSIGHT(x) {if (hindsight_enabled) {x}}

// Opentelemetry is enabled or disabled based on command-line arguments
bool opentelemetry_enabled = false;
#define OPENTELEMETRY(x) {if (opentelemetry_enabled) {x}}

// Request debugging is specified as an RPC argument
//   so this macro is actually always-on
#define REQUESTDEBUG(x) {x}

std::atomic<int64_t> TRIGGER{1};
namespace hindsightgrpc {

// Used by command-line to set hindsight tracing on or off
void set_hindsight_enabled(bool is_enabled) {
  hindsight_enabled = is_enabled;
}

// Used by command-line to set opentelemetry on or off
void set_opentelemetry_enabled(bool is_enabled) {
  opentelemetry_enabled = is_enabled;
}

ServerImpl::ServerImpl(ServiceConfig config,
                       std::map<std::string, AddressInfo> addresses,
                       bool nocompute, std::map<int, float> triggers,
                       int instance_id, int max_outstanding_requests)
    : alive(true),
      clients(),
      config(config),
      addresses(addresses),
      nocompute_(nocompute),
      instance_id(instance_id),
      max_outstanding_requests(max_outstanding_requests),
      awaiting(0),
      processing(0),
      awaitingchildren(0),
      finishing(0),
      completed(0)
       {
  for (auto &p : triggers) {
    // trigger probabilities are typically small (e.g. 0.1, 0.01) so
    // we don't use, e.g. rand() % p to decide to trigger. instead we
    // use rand() < RAND_MAX * p
    int queue_id = p.first;
    float trigger_probability = p.second;
    uint64_t trigger_below;
    if (trigger_probability <= 0) trigger_below = 0;
    else if (trigger_probability >= 1) trigger_below = UINT64_MAX;
    else {
      trigger_below = RAND_MAX / (uint64_t) round(1.0/trigger_probability);
    }
    this->triggers[queue_id] = trigger_below;
  }
}

ServerImpl::~ServerImpl() {
  Shutdown();
}

void ServerImpl::Run(int nhandlers, bool debug) {
  AddressInfo info = addresses[config.Name()];
  std::string server_address(info.deploy_addr + ":" + info.ports[instance_id]);

  // Build the gRPC server
  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service_);
  for (int i = 0; i < nhandlers; i++) {
    cqs.push_back(builder.AddCompletionQueue());
  }
  server_ = builder.BuildAndStart();
  std::cout << "Server listening on " << server_address << std::endl;
  std::cout << "Server config " << config << std::endl;

  std::string local_address = info.breadcrumbs[instance_id];
  std::cout << "Using " << local_address
            << " for local breadcrumb" << std::endl;

  // Start the handler threads
  std::cout << "Starting " << nhandlers << " handlers" << std::endl;
  for (int i = 0; i < nhandlers; i++) {
    ServerHandler* handler =
      new ServerHandler(this, i, cqs[i].get(), local_address, config);
    handlers.push_back(handler);
    threads.push_back(std::thread(&ServerHandler::Run, handler));
  }

  if (debug) {
    threads.push_back(std::thread(&ServerImpl::PrintThread, this));
  }
}

void ServerImpl::Shutdown() {
  server_->Shutdown();
  alive = false;
  // Always shutdown the completion queue after the server.
  Join();
}

void ServerImpl::Join() {
  for (int i = 0; i < threads.size(); i++) {
    threads[i].join();
  }
}

uint64_t now() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
    std::chrono::high_resolution_clock::now().time_since_epoch()).count();
}

void ServerImpl::PrintThread() {
  std::cout << "PrintThread running\n";
  // Ignore first second of requests
  uint64_t lead_in = 1000000;
  usleep(lead_in);


  uint64_t print_every = 100000;

  uint64_t last_awaiting;
  uint64_t last_processing;
  uint64_t last_awaitingchildren;
  uint64_t last_finishing;
  uint64_t last_completed;

  uint64_t cur_awaiting;
  uint64_t cur_processing;
  uint64_t cur_awaitingchildren;
  uint64_t cur_finishing;
  uint64_t cur_completed;
  
  last_awaiting = awaiting;
  last_processing = processing;
  last_awaitingchildren = awaitingchildren;
  last_finishing = finishing;
  last_completed = completed;

  // print per second
  uint64_t last_print = now();
  uint64_t next_print = last_print + print_every;
  while (alive) {
    uint64_t t;
    while ((t = now()) < next_print && alive) {
      usleep(10000);
    }

    cur_awaiting = awaiting;
    cur_processing = processing;
    cur_awaitingchildren = awaitingchildren;
    cur_finishing = finishing;
    cur_completed = completed;

    printf("-- Admitting  %lu (%lu)\n", cur_awaiting - cur_processing, cur_awaiting - last_awaiting);
    printf("   Processing %lu (%lu)\n", cur_processing - cur_awaitingchildren, cur_processing - last_processing);
    printf("   Children   %lu (%lu)\n", cur_awaitingchildren - cur_finishing, cur_awaitingchildren - last_awaitingchildren);
    printf("   Finishing  %lu (%lu)\n", cur_finishing - cur_completed, cur_finishing - last_finishing);
    printf("   Completed  %lu\n", cur_completed - last_completed);


    last_awaiting = cur_awaiting;
    last_processing = cur_processing;
    last_awaitingchildren = cur_awaitingchildren;
    last_finishing = cur_finishing;
    last_completed = cur_completed;
    
    next_print = next_print + print_every;
    last_print = t;
  }
}

ChildClient* ServerImpl::GetClient(std::string address) {
  std::lock_guard<std::mutex> guard(clients_mutex);
  auto it = clients.find(address);
  if (it != clients.end()) {
    return it->second;
  }
  ChildClient* client = new ChildClient(address);
  clients[address] = client;
  return client;
}

void ServerHandler::Run() {
  // Spawn a new CallData instance to serve new clients.
  PrepareNextRequest();
  void* tag;  // uniquely identifies a request.
  bool ok;

  gpr_timespec deadline;
  deadline.clock_type = GPR_TIMESPAN;
  deadline.tv_sec = 0;
  deadline.tv_nsec = 0;
  grpc::CompletionQueue::NextStatus status;

  int drain_threshold = server_->max_outstanding_requests / 2;

  while (server_->alive) {
    // Block waiting to read the next event from the completion queue. The
    // event is uniquely identified by its tag, which in this case is the
    // memory address of a CallData instance.
    // The return value of Next should always be checked. This return value
    // tells us whether there is any kind of event or cq_ is shutting down.
    // GPR_ASSERT(cq_->Next(&tag, &ok));
    // // reinterpret_cast<Callback*>(tag)->Proceed(ok);
    // static_cast<Callback*>(tag)->Proceed(ok);

    if (draining) {
      if (admitting_requests == 0) {
        status = cq_->AsyncNext(&tag, &ok, deadline);
        if (status == grpc::CompletionQueue::NextStatus::GOT_EVENT) {
          static_cast<Callback*>(tag)->Proceed(ok);

        } else if (status == grpc::CompletionQueue::NextStatus::TIMEOUT) {
          draining = false;
          PrepareNextRequest();

        } else {
          break;
        }
      } else {
        GPR_ASSERT(cq_->Next(&tag, &ok));
        static_cast<Callback*>(tag)->Proceed(ok);
      }
    } else {
      GPR_ASSERT(cq_->Next(&tag, &ok));
      static_cast<Callback*>(tag)->Proceed(ok);

      if (outstanding_requests >= drain_threshold) {
        draining = true;
      }
    }
  }
  cq_->Shutdown();
}

/*
parent_span_id + 1 : "HindsightGRPC/Exec"
parent_span_id + 2 : "HindsightGRPC/Exec/Process"
  parent_span_id + 2 + 10000 + 2*i : "HindsightGRPC/ChildCall"
  parent_span_id + 2 + 10000 + 2*i + 1 : "HindsightGRPC/ChildCall/Prepare"
parent_span_id + 3 : "HindsightGRPC/Exec/Finish"
parent_span_id + 4 : "HindsightGRPC/Exec/Complete"
*/

void ServerHandler::PrepareNextRequest() {
  // The completion queue stuff happens within the request class
  if (!draining && admitting_requests == 0 && outstanding_requests < server_->max_outstanding_requests) {
    new Request(this, request_id_seed++);
    outstanding_requests++;
    admitting_requests++;
  }
}

// Assumed not to be thread safe
ChildClient* ServerHandler::GetClient(std::string address) {
  auto it = clients.find(address);
  if (it != clients.end()) {
    return it->second;
  }
  ChildClient* client = server_->GetClient(address);
  clients[address] = client;
  return client;
}


Request::Request(ServerHandler* handler, int requestid) : handler_(handler),
    id(requestid), service_(&handler->server_->service_), responder_(&ctx_),
    status_(CREATE), outstanding_children(0) {
  // Invoke the serving logic right away.
  Proceed(true);
}

Request::~Request() {}

void Request::Proceed(bool ok) {
  if (status_ == CREATE) {
    status_ = PROCESS;
    service_->RequestExec(&ctx_, &request_, &responder_, handler_->cq_,
      handler_->cq_, this);
    handler_->server_->awaiting++;

  } else if (status_ == PROCESS) {
    if (!ok) {
      // The completion queue is shutting down
      // and we don't actually have a request
      delete this;
      return;
    }
    handler_->server_->processing++;

    start_time = nanos();

    std::string api = request_.api();

    // Debug logging is orthogonal to tracing
    REQUESTDEBUG(
      if (request_.debug()) {
        std::cout << "[DEBUG] Received:\n" << request_.DebugString()
                  << "===" << std::endl;
        std::cout << "[DEBUG] Received context:\n";
        auto &metadata = ctx_.client_metadata();
        for (auto it = metadata.begin(); it != metadata.end(); ++it) {
          std::cout << "  " << (*it).first << ": " << (*it).second << std::endl;
        }
        std::cout << "===" << std::endl;
      }
    )

    std::shared_ptr<Scope> exec_scope;
    OPENTELEMETRY(
      // Create the end-to-end request span using the received trace metadata
      opentelemetry::trace::StartSpanOptions options;
      options.parent = extractContextFromRPC();
      request_span = handler_->tracer_->StartSpan("HindsightGRPC/Exec", options);
      request_span->SetAttribute("API", api);
      request_span->SetAttribute("Interval", request_.interval());

      // Extract breadcrumb
      auto it = ctx_.client_metadata().find("breadcrumb");
      if (it != ctx_.client_metadata().end()) {
        request_span->SetAttribute("Breadcrumb", std::string(it->second.data()));
      }

      // span_ will be the active span until exec_scope is destroyed
      exec_scope = std::make_shared<Scope>(request_span);
    )

    handler_->admitting_requests--;
    handler_->PrepareNextRequest();

    uint64_t span_id;
    HINDSIGHT(
      if (request_.has_hindsight()) {
        auto &hindsight_context = request_.hindsight();

        hs_ = std::make_shared<HindsightTraceState>(
            hindsight_context.trace_id(), hindsight_context.span_id());

        span_id = hs_->parent_span_id + 1;
        for (int i = 0; i < hindsight_context.breadcrumb_size(); i++) {
          std::string breadcrumb = hindsight_context.breadcrumb(i);
          // hs_->ReportBreadcrumb(breadcrumb);
          hs_->LogSpanAttributeStr(span_id, "Breadcrumb", breadcrumb);
        }
      } else {
        // should raise exception directly
        // hs_ = std::make_shared<HindsightTraceState>((uint64_t) rand());
        // TODO(jcmace): not in this way and not sure if here
      }


      hs_->LogSpanStart(span_id);
      hs_->LogSpanName(span_id, "HindsightGRPC/Exec");
      hs_->LogTracer(span_id, "hindsight");
      hs_->LogSpanParent(span_id, hs_->parent_span_id);
      hs_->LogSpanKind(span_id, 0);
      hs_->LogSpanAttributeStr(span_id, "API", api);
      hs_->LogSpanAttribute(span_id, "Interval", request_.interval());
    )

    std::shared_ptr<Scope> process_scope;
    nostd::shared_ptr<Span> span;
    OPENTELEMETRY(
      // Create a nested span for PROCESS specifically
      span = handler_->tracer_->StartSpan("HindsightGRPC/Exec/Process");
      process_scope = std::make_shared<Scope>(span);
    )
    HINDSIGHT(
      span_id = hs_->parent_span_id + 2;
      hs_->LogSpanStart(span_id);
      hs_->LogSpanName(span_id, "HindsightGRPC/Exec/Process");
      hs_->LogTracer(span_id, "hindsight");
      hs_->LogSpanParent(span_id, hs_->parent_span_id + 1);
      hs_->LogSpanKind(span_id, 0);
    )

    API& api_info = handler_->server_->config.get_api(api);

    REQUESTDEBUG(
      if (request_.debug()) {
        std::cout << "[DEBUG] Executing API\n" << api_info << "===" << std::endl;
      }
    )
    OPENTELEMETRY(
      span->AddEvent("Executing API");
      span->SetAttribute("Exec", api_info.exec);
    )
    HINDSIGHT(
      hs_->LogSpanEvent(span_id, "Executing API");
      hs_->LogSpanAttribute(span_id, "Exec", api_info.exec);
    )

    // Computation can be disabled via the nocompute command line argument
    int64_t exec_duration = 0;
    if (!handler_->server_->nocompute_) {
      MatrixConfig config = handler_->server_->config.get_matrix_config(api);

      REQUESTDEBUG(
        if (request_.debug()) {
          std::cout << "[DEBUG] Executing MatrixConfig " << config << std::endl;
        }
      )

      uint64_t begin = nanos();
      double result = hindsightgrpc::matrix_multiply(config);
      exec_duration = nanos() - begin;

      REQUESTDEBUG(
        if (request_.debug()) {
          std::cout << "[DEBUG] Took " << exec_duration
                    << " nanos to calculate " << result << std::endl;
        }
      )
    }

    OPENTELEMETRY(
      span->SetAttribute("MatrixExec", exec_duration);
    )
    HINDSIGHT(
      hs_->LogSpanAttribute(span_id, "MatrixExec", exec_duration);
    )


    OPENTELEMETRY(
      span->AddEvent("Calling Children");
    )
    HINDSIGHT(
      hs_->LogSpanEvent(span_id, "Calling Children");
    )

    // Use the child-call services based on the API info in the ServiceConfig
    std::vector<Outcall*> child_calls;
    for (auto& child : api_info.children) {
      if (rand() % 100 < child.probability) {
        if (child.subcalls.size() > 0) {
          // randomly choosing an instance of the target services
          assert(child.subcalls.size() != 1);
          child_calls.push_back(
              &child.subcalls[rand() % child.subcalls.size()]);
        } else {
          child_calls.push_back(&child);
        }
      }
    }

    handler_->server_->awaitingchildren++;
    if (child_calls.size() > 0) {
      InvokeChildren(child_calls, span_id);

      OPENTELEMETRY(
        span->AddEvent("Awaiting Child Responses");
      )
      HINDSIGHT(
        hs_->LogSpanEvent(span_id, "Awaiting Child Responses");
      )
    } else {
      OPENTELEMETRY(
        span->AddEvent("Not making child calls");
      )
      HINDSIGHT(
        hs_->LogSpanEvent(span_id, "Not making child calls");
      )

      Complete();
    }

    REQUESTDEBUG(
      if (request_.debug()) {
        std::cout << "[DEBUG] Finished Handling Request" << std::endl;
      }
    )

    OPENTELEMETRY(
      // End the inner span but leave the outer span
      span->End();
    )
    HINDSIGHT(
      hs_->LogSpanEnd(span_id);
    )
  } else if (status_ == FINISH) {
    std::shared_ptr<Scope> parentscope;
    std::shared_ptr<Scope> scope;
    nostd::shared_ptr<Span> span;
    OPENTELEMETRY(
      parentscope = std::make_shared<Scope>(request_span);
      span = handler_->tracer_->StartSpan("HindsightGRPC/Exec/Finish");
      scope = std::make_shared<Scope>(span);
    )
    uint64_t span_id;
    HINDSIGHT(
      span_id = hs_->parent_span_id + 3;
      hs_->LogSpanStart(span_id);
      hs_->LogSpanName(span_id, "HindsightGRPC/Exec/Finish");
      hs_->LogTracer(span_id, "hindsight");
      hs_->LogSpanParent(span_id, hs_->parent_span_id + 1);
      hs_->LogSpanKind(span_id, 0);
    )


    OPENTELEMETRY(
      span->AddEvent("Finishing request");
    )
    HINDSIGHT(
      hs_->LogSpanEvent(span_id, "Finishing request");
    )

    if (!ok) {
    // if (request_.mutable_hindsight()->triggerflag() && (rand() % 100 < 10)) {
      OPENTELEMETRY(
        span->SetStatus(opentelemetry::trace::StatusCode::kError, "RPC response was not OK");
      )
      HINDSIGHT(
        hs_->LogSpanStatus(span_id, (int) opentelemetry::trace::StatusCode::kError, "RPC response was not OK");
      )
    // }
      REQUESTDEBUG(
        if (request_.debug()) {
          std::cout << "[DEBUG] RPC Response NOT ok\n";
        }
      )
    } else {
      OPENTELEMETRY(
        span->SetStatus(opentelemetry::trace::StatusCode::kOk, "RPC Response was OK");
      )
      HINDSIGHT(
        hs_->LogSpanStatus(span_id, (int) opentelemetry::trace::StatusCode::kOk, "RPC response was OK");
      )
      REQUESTDEBUG(
        if (request_.debug()) {
          std::cout << "[DEBUG] Request complete\n";
        }
      )
    }

    if (request_.mutable_hindsight()->triggerflag()) {
      // fire the trigger only when 
      for (auto &p : handler_->server_->triggers) {
        int queue_id = p.first;
        uint64_t trigger_threshold = p.second;
        if (rand() >= trigger_threshold) continue;

        auto trigger_count = TRIGGER++;

        OPENTELEMETRY(
          std::string trigger_name = "TriggerQueue" + std::to_string(queue_id);
          span->SetAttribute(trigger_name, (int64_t)queue_id);
          // int valus is weirdly not recognized by the tail processors
          // span->SetAttribute("Trigger", queue_id);
          span->SetAttribute("Trigger", std::to_string(trigger_count));
        )

        HINDSIGHT(
          std::string trigger_name = "TriggerQueue" + std::to_string(queue_id);
          hs_->LogSpanAttribute(span_id, trigger_name, queue_id);
          hs_->LogSpanAttribute(span_id, "Trigger", queue_id);
        )

        REQUESTDEBUG(
          if (request_.debug()) {
            std::cout << "[DEBUG] Triggering for queue " << queue_id << std::endl;
          }
        )

      }
#ifdef latency_trigger
      // std::cout << "duration: " << float(nanos() - hs_->start_time) / 1000000 << " ms\n";
      // Todo: a config file for different trigger policy
      if (nanos() - start_time > 100) {
        HINDSIGHT(
          std::string trigger_name = "TriggerQueue" + std::to_string(TRIGGER_ID_HEAD_BASED_SAMPLING);
          hs_->LogSpanAttribute(span_id, trigger_name, TRIGGER_ID_HEAD_BASED_SAMPLING);
          hs_->LogSpanAttribute(span_id, "Trigger", TRIGGER_ID_HEAD_BASED_SAMPLING);
          // triggers_fire(&hindsight.triggers, TRIGGER_ID_HEAD_BASED_SAMPLING, hs_->trace_id, hs_->trace_id);
        )
        OPENTELEMETRY(
          std::string trigger_name = "TriggerQueue" + std::to_string(TRIGGER_ID_HEAD_BASED_SAMPLING);
          span->SetAttribute(trigger_name, TRIGGER_ID_HEAD_BASED_SAMPLING);
          span->SetAttribute("Trigger", TRIGGER_ID_HEAD_BASED_SAMPLING);
        )
      }
#endif
    }

    OPENTELEMETRY(
      // for mapping child calls
      span->SetAttribute("LocalAddress", handler_->local_address);
      span->AddEvent("Request complete");
    )
    HINDSIGHT(
      hs_->LogSpanEvent(span_id, "Request complete");
    )

    OPENTELEMETRY(
      span->End();
    )
    HINDSIGHT(
      hs_->LogSpanEnd(span_id);
    )

    handler_->outstanding_requests--;
    handler_->server_->completed++;

    // Once in the FINISH state, deallocate ourselves (CallData).
    delete this;

  } else {
    std::cout << "Unexpected transition" << std::endl;
  }
}

void Request::InvokeChildren(std::vector<Outcall*> outcalls, uint64_t span_id) {
  status_ = AWAITCHILDREN;

  for (auto& outcall : outcalls) {
    std::string address = outcall->server_addr;
    ChildClient* client = handler_->GetClient(address);
    outstanding_children++;
    // 10000 as a hard code interval between parent and child spans
    client->Call(this, outcall, 10000 + span_id);
    span_id += 2;
  }
}

void Request::ChildResponseReceived(ChildCall* call, bool ok) {
  std::shared_ptr<Scope> scope;
  OPENTELEMETRY(
    scope = std::make_shared<Scope>(request_span);
  )


  if (!ok) {
    OPENTELEMETRY(
      call->childcall_span->AddEvent("Failed to invoke child");
    )
    HINDSIGHT(
      hs_->LogSpanEvent(call->id_, "Failed to invoke child");
    )
    REQUESTDEBUG(
      if (request_.debug()) {
        std::cout << "[DEBUG] Failed to invoke child " << *call->outcall_ << std::endl;
      }
    )
  } else {
    OPENTELEMETRY(
      call->childcall_span->AddEvent("Child response received");
    )
    HINDSIGHT(
      hs_->LogSpanEvent(call->id_, "Child response received");
    )

    if (call->status.ok()) {
      OPENTELEMETRY(
        call->childcall_span->SetAttribute("Response payload", call->reply.payload());
        call->childcall_span->SetStatus(opentelemetry::trace::StatusCode::kOk, "Child response was OK");
      )
      HINDSIGHT(
        hs_->LogSpanAttributeStr(call->id_, "Response payload", call->reply.payload());
        hs_->LogSpanStatus(call->id_, (int) opentelemetry::trace::StatusCode::kOk, "Child response was OK");
      )
      REQUESTDEBUG(
        if (request_.debug()) {
          std::cout << "[DEBUG] Child response received from " << *call->outcall_ << std::endl;
          std::cout << "[DEBUG] Child response payload: " << call->reply.payload() << std::endl;
        }
      )

    } else {
      OPENTELEMETRY(
        call->childcall_span->SetStatus(opentelemetry::trace::StatusCode::kError, "Child response was not OK");
      )
      HINDSIGHT(
        hs_->LogSpanStatus(call->id_, (int) opentelemetry::trace::StatusCode::kError, "Child response was not OK");
      )
      REQUESTDEBUG(
        if (request_.debug()) {
          std::cout << "[DEBUG] Child RPC failed " << *call->outcall_ << std::endl;
        }
      )
    }
  }

  HINDSIGHT(
    hs_->LogSpanEnd(call->id_);
  )
  
  delete call;

  outstanding_children--;
  if (outstanding_children == 0) {
    Complete();
  }
}

void Request::Complete() { 
  nostd::shared_ptr<Span> span;
  std::shared_ptr<Scope> scope;
  OPENTELEMETRY(
    span = handler_->tracer_->StartSpan("HindsightGRPC/Exec/Complete");
    scope = std::make_shared<Scope>(span);
  )
  uint64_t span_id;
  HINDSIGHT(
    span_id = hs_->parent_span_id + 4;
    hs_->LogSpanStart(span_id);
    hs_->LogSpanName(span_id, "HindsightGRPC/Exec/Complete");
    hs_->LogTracer(span_id, "hindsight");
    hs_->LogSpanParent(span_id, hs_->parent_span_id + 1);
    hs_->LogSpanKind(span_id, 0);
  )

  std::string prefix("Hello ");
  reply_.set_payload(prefix + request_.api());

  HINDSIGHT(
    reply_.mutable_hindsight()->set_trace_id(hs_->trace_id);
    reply_.mutable_hindsight()->add_breadcrumb(handler_->local_address);
  )

  handler_->server_->finishing++;
  status_ = FINISH;
  responder_.Finish(reply_, Status::OK, this);

  OPENTELEMETRY(
    span->AddEvent("Sending RPC response");
    span->End();
  )
  HINDSIGHT(
    hs_->LogSpanEvent(span_id, "Sending RPC response");
    hs_->LogSpanEnd(span_id);
    hs_->LogSpanEnd(hs_->parent_span_id + 1);
  )
}

SpanContext Request::extractContextFromRPC() {
#ifdef PROPAGATOR
  // compare with trace metadata extracted from the received RPC
  auto defaults = opentelemetry::context::Context{};
  auto carrier = GrpcServerCarrier(&this->ctx_);
  auto received_context = handler_->propagator_->Extract(carrier, defaults);
  auto remote_span = opentelemetry::trace::GetSpan(received_context);
#endif

  auto otel_context = request_.otel();
  auto trace_id_hex = nostd::string_view(otel_context.trace_id());
  auto span_id_hex = nostd::string_view(otel_context.span_id());
  bool sample_flag = otel_context.sample();

  if (!detail::IsValidHex(trace_id_hex) || !detail::IsValidHex(span_id_hex)) {
    // throw exception
  }

  uint8_t trace_id[16];
  uint8_t span_id[8];
  if (!detail::HexToBinary(trace_id_hex, trace_id, sizeof(trace_id)) ||
      !detail::HexToBinary(span_id_hex, span_id, sizeof(span_id))) {
    // throw exception
  }

  uint8_t flags = sample_flag;
  auto span_context = SpanContext(TraceId(trace_id), SpanId(span_id), TraceFlags(flags), true);

  if (!request_.mutable_hindsight()->triggerflag()) {
    // compare span_context withremote_span
    // assert(span_context == remote_span);
  }

  if (!span_context.IsValid()) {
    // throw exception
  }
  return span_context;
}

ChildClient::ChildClient(std::string address) : address(address),
  channel(grpc::CreateChannel(address, grpc::InsecureChannelCredentials())),
  stub(HindsightGRPC::NewStub(channel)) {}

ChildClient::~ChildClient() {}

ChildCall* ChildClient::Call(Request* parent, Outcall* outcall, int id) {
  //TODO: Modify this to accept an Outcall parameter
  ChildCall* call = new ChildCall(this, parent, outcall, id);
  call->SendCall();
  return call;
}

ChildCall::ChildCall(ChildClient* child, Request* parent, Outcall* outcall, int id) : child_(child),
  parent_(parent), outcall_(outcall), id_(id) {
  
  OPENTELEMETRY(
    this->childcall_span = parent_->handler_->tracer_->StartSpan("HindsightGRPC/ChildCall");
  )
  HINDSIGHT(
    parent->hs_->LogSpanStart(id_);
    parent->hs_->LogSpanName(id_, "HindsightGRPC/ChildCall");
    parent->hs_->LogTracer(id_, "hindsight");
    parent->hs_->LogSpanParent(id_, parent->hs_->parent_span_id + 2);
    parent->hs_->LogSpanKind(id_, 0);
  )
}

ChildCall::~ChildCall() {}

void ChildCall::SendCall() {
  std::shared_ptr<Scope> childcall_scope;
  std::shared_ptr<Scope> scope;
  nostd::shared_ptr<Span> span;
  OPENTELEMETRY(
    childcall_scope = std::make_shared<Scope>(childcall_span);

    childcall_span->AddEvent("Making child RPC call");

    span = parent_->handler_->tracer_->StartSpan("HindsightGRPC/ChildCall/Prepare");
    span->SetAttribute("Destination", outcall_->service_name);
    span->SetAttribute("Breadcrumb", outcall_->breadcrumb);
    span->SetAttribute("API", outcall_->api_name);

    scope = std::make_shared<Scope>(span);
  )
  HINDSIGHT(
    parent_->hs_->LogSpanStart(id_+1);
    parent_->hs_->LogSpanName(id_+1, "HindsightGRPC/ChildCall/Prepare");
    parent_->hs_->LogTracer(id_+1, "hindsight");
    parent_->hs_->LogSpanParent(id_+1, id_);
    parent_->hs_->LogSpanKind(id_+1, 0);
    parent_->hs_->LogSpanAttributeStr(id_+1, "Destination", outcall_->service_name);
    parent_->hs_->LogSpanAttributeStr(id_+1, "Breadcrumb", outcall_->breadcrumb);
    parent_->hs_->LogSpanAttributeStr(id_+1, "API", outcall_->api_name);
    // parent_->hs_->ReportBreadcrumb(outcall_->breadcrumb);
  )

  REQUESTDEBUG(
    if (parent_->request_.debug()) {
      std::cout << "[DEBUG] Making Child RPC call to " << outcall_ << " " << *outcall_ << std::endl;
    }
  )

  // Create the RPC request
  ExecRequest request;
  request.set_api(outcall_->api_name);
  request.set_payload("payload");
  request.set_interval(parent_->request_.interval());


  /* Context propagation */
  REQUESTDEBUG(
    request.set_debug(parent_->request_.debug());
  )
  OPENTELEMETRY(
    auto current_ctx = opentelemetry::context::RuntimeContext::GetCurrent();
#ifdef PROPAGATOR
    // Inject the OT context into the gRPC context
    GrpcClientCarrier carrier(&context);
    parent_->handler_->propagator_->Inject(carrier, current_ctx);
    context.AddMetadata("breadcrumb", parent_->handler_->local_address);
#endif
    // inject current span id into the request
    SpanContext span_context = opentelemetry::trace::GetSpan(current_ctx)->GetContext();
    char tid_buffer[32];
    span_context.trace_id().ToLowerBase16(nostd::span<char, 32>{&tid_buffer[0], 32});
    request.mutable_otel()->set_trace_id(std::string(tid_buffer, 32));
    char sid_buffer[16];
    span_context.span_id().ToLowerBase16(nostd::span<char, 16>{&sid_buffer[0], 16});
    request.mutable_otel()->set_span_id(std::string(sid_buffer, 16));
    request.mutable_otel()->set_sample(span_context.IsSampled() ? true : false);
  )
  HINDSIGHT(
    request.mutable_hindsight()->set_trace_id(parent_->hs_->trace_id);
    request.mutable_hindsight()->set_span_id(parent_->hs_->parent_span_id + 2);
    request.mutable_hindsight()->add_breadcrumb(
        parent_->handler_->local_address);
  )

  // Start the call using the parent request's completion queue
  response_reader = child_->stub->PrepareAsyncExec(&context, request,
    parent_->handler_->cq_);
  response_reader->StartCall();

  // Register this object's Proceed method as the callback upon completion
  response_reader->Finish(&reply, &status, this);

  OPENTELEMETRY(
    span->AddEvent("Child RPC call initiated");
    span->End();
  )
  HINDSIGHT(
    parent_->hs_->LogSpanEvent(id_+1, "Child RPC call initiated");
    parent_->hs_->LogSpanEnd(id_+1);
  )
}

// The callback invoked by gRPC when a response is received
void ChildCall::Proceed(bool ok)  {
  parent_->ChildResponseReceived(this, ok);
}

}  // namespace hindsightgrpc
