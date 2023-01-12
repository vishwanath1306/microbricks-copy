/*
 * Copyright 2022 Max Planck Institute for Software Systems *
 */

#pragma once
#ifndef SRC_HINDSIGHTGRPC_SERVER_H_
#define SRC_HINDSIGHTGRPC_SERVER_H_

#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>
#include <json.hpp>

#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <atomic>
#include <mutex>
#include <map>


extern "C" {
  #include "hindsight.h"
}

#include "hindsightgrpc.grpc.pb.h"
#include "opentelemetry/trace/provider.h"
#include "opentelemetry/trace/tracer.h"
#include "opentelemetry/context/propagation/global_propagator.h"
#include "opentelemetry/context/propagation/text_map_propagator.h"

#include "topology.h"
#include "../tracing/opentelemetry.h"
#include "../tracing/hindsight_extensions.h"

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
using json = nlohmann::json;

namespace nostd = opentelemetry::nostd;
using opentelemetry::trace::Tracer;
using opentelemetry::trace::Span;
using opentelemetry::context::propagation::TextMapPropagator;
using opentelemetry::trace::SpanContext;

namespace hindsightgrpc {

class ServerHandler;
class ChildClient;

// Used by command-line to set hindsight tracing on or off
extern void set_hindsight_enabled(bool is_enabled);

// Used by command-line to set opentelemetry on or off
extern void set_opentelemetry_enabled(bool is_enabled);

/* A simple async gRPC server that can run multiple threads. */
class ServerImpl final {
 public:
  ServerImpl(ServiceConfig config, std::map<std::string, AddressInfo> addresses,
             bool nocompute, std::map<int, float> triggers, int instance_id, int max_outstanding_requests);
  ~ServerImpl();

  /* Runs the specified number of handler threads */
  void Run(int nthreads, bool debug);
  void PrintThread();

  /* Initiates shutdown of the RPC server and awaits handlers */
  void Shutdown();

  /* Waits for all handlers to complete */
  void Join();

  /* Thread-safe access to RPC clients*/
  ChildClient* GetClient(std::string address);

 public:
  HindsightGRPC::AsyncService service_;
  std::atomic_bool alive;
  ServiceConfig config;

  // An argument to ServerImpl, provided by the command-line --nocompute option.
  const bool nocompute_;

  // Triggering
  std::map<int, uint64_t> triggers;

  // instance id
  const int instance_id;

  // Admission control -- max requests per handler
  const int max_outstanding_requests;

  std::atomic_uint64_t awaiting;
  std::atomic_uint64_t processing;
  std::atomic_uint64_t awaitingchildren;
  std::atomic_uint64_t finishing;
  std::atomic_uint64_t completed;

 private:
  // Clients to other RPC servers
  std::mutex clients_mutex;
  std::map<std::string, ChildClient*> clients;

  std::map<std::string, AddressInfo> addresses;

  // Server multi-threading
  std::vector<std::thread> threads;
  std::vector<ServerHandler*> handlers;

  // gRPC bits
  std::vector<std::unique_ptr<ServerCompletionQueue>> cqs;
  std::unique_ptr<Server> server_;

};

/* A handler thread of the server */
class ServerHandler {
 public:
  ServerHandler(ServerImpl* server, int handlerid, ServerCompletionQueue* cq, std::string local_address, ServiceConfig config) :
    server_(server), handlerid_(handlerid), cq_(cq), request_id_seed(0),
    clients(), local_address(local_address), config(config), outstanding_requests(0), admitting_requests(0), draining(false) {
      tracer_ = opentelemetry::trace::Provider::GetTracerProvider()->GetTracer("hindsight");
      propagator_ = opentelemetry::context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
    }
  ~ServerHandler() {}

  void Run();
  void PrepareNextRequest();
  ChildClient* GetClient(std::string address);

 private:
  int request_id_seed;
  std::map<std::string, ChildClient*> clients;
  ServiceConfig config;

 public:
  ServerImpl* server_;
  int handlerid_;
  ServerCompletionQueue* cq_;

  // OpenTelemetry stuff
  nostd::shared_ptr<Tracer> tracer_;
  nostd::shared_ptr<TextMapPropagator> propagator_;

  // Hindsight stuff
  std::string local_address;

  // Admission control
  int outstanding_requests;
  int admitting_requests;
  bool draining;
};

class Request;
class ChildCall;

/* A client to another gRPC server */
class ChildClient {
 public:
  explicit ChildClient(std::string address);
  ~ChildClient();

  ChildCall* Call(Request* parent, Outcall* outcall, int id);

 public:
  std::string address;
  std::shared_ptr<Channel> channel;
  std::unique_ptr<HindsightGRPC::Stub> stub;
};

/* gRPC's completion queue uses void* pointers for any events.
We'll assume that all of these void* pointers are instances of this
Callback class.  We'll call the Proceed function */
class Callback {
 public:
  virtual void Proceed(bool ok) = 0;
  virtual ~Callback(){}
};

// A request to this server
class Request : public Callback {
 public:
  Request(ServerHandler* handler, int requestid);
  ~Request();

  void Proceed(bool ok);
  void InvokeChildren(std::vector<Outcall*> calls, uint64_t span_id);
  void ChildResponseReceived(ChildCall* call, bool ok);
  void Complete();

  SpanContext extractContextFromRPC();

 public:
  int id;
  ServerHandler* handler_;
  HindsightGRPC::AsyncService* service_;

  // gRPC request and response
  ExecRequest request_;
  ExecReply reply_;

  // gRPC pieces about the request
  ServerContext ctx_;
  ServerAsyncResponseWriter<ExecReply> responder_;

  // OpenTelemetry
  nostd::shared_ptr<Span> request_span; // The span representing the end-to-end RPC request

  // Hindsight (in non-OpenTelemetry mode)
  std::shared_ptr<HindsightTraceState> hs_; // the trace state
  uint64_t start_time; // used for latency trigger

  // Implemented as a state machine similar to the gRPC async example
  enum CallStatus { CREATE, PROCESS, AWAITCHILDREN, FINISH };
  CallStatus status_;

  int outstanding_children;

};

// A call to another RPC server
class ChildCall : public Callback {
 public:
  ChildCall(ChildClient* child, Request* parent, Outcall* outcall, int id);
  ~ChildCall();

  // Initiates the call
  void SendCall();

  // The callback invoked by gRPC when a response is received
  void Proceed(bool ok);

 private:
  // Server pieces
  ChildClient* child_;
  Request* parent_;

  // gRPC pieces
  ClientContext context;
  std::unique_ptr<ClientAsyncResponseReader<ExecReply>> response_reader;

 public:
  // gRPC reply
  Status status;
  ExecReply reply;

  // Topology config
  Outcall* outcall_;

  // OpenTelemetry
  nostd::shared_ptr<Span> childcall_span;

  // Hindsight
  int id_;
};


}  // namespace hindsightgrpc

#endif  // SRC_HINDSIGHTGRPC_SERVER_H_
