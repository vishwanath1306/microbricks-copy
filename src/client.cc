/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <atomic>
#include <chrono>
#include <map>
#include <iterator>
#include <csignal>
#include <random>

#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>

#include "hindsightgrpc/server.h"

#include "hindsightgrpc.grpc.pb.h"
#include <argp.h>

#include "opentelemetry/sdk/trace/id_generator.h"

const char *program_version = "client 1.0";
const char *program_bug_address = "<cld-science@mpi-sws.org>";
static char doc[] = "A gRPC async client to the server.  The SERV argument specifies which server from the topology file to connect to."
                    "To run a standalone client, simply run ./server standalone";
static char args_doc[] = "SERV";

static struct argp_option options[] = {
  {"concurrency",  'c', "NUM",  0,  "The number of concurrent client threads to run.  Each thread has its own RPC client.  Default 1." },
  {"requests",  'r', "NUM",  0,  "If running as an closed-loop client, this specifies the number of concurrent outstanding requests per client.  If running as an open-loop client, this specifies the request rate per second per client.  Default 1." },  
  {"openloop",  'o', 0,  0,  "If set, runs as an open-loop client.  If left unset, runs as a closed-loop client" },
  {"limit",  'l', "LIMIT",  0,  "The total number of requests to submit before exiting.  Set to 0 for no limit.  Default 0." },
  {"debug",  'd', 0,  0,  "Print debug information on all servers.  If debug is enabled, the default value for limit will be set to 1." },
  {"topology", 't', "FILE", 0, "A topology file.  This is required.  See config/example_topology.json for an example." },
  {"addresses", 'a', "FILE", 0, "An addresses file.  This is required.  See config/example_addresses.json for an example." },
  {"interval", 'i', "NUM", 0, "Interval size in seconds, default 10.  Each trace will log the interval when it was generated." },
  // only for opentelemetry based tracers
  {"sampling",  's', "NUM",  0,  "Probability of head-based sampling. Default 1." },
  { 0 }
};

struct arguments {
  bool debug;
  bool openloop;
  int concurrency;
  int requests;
  int limit;
  int interval;
  char* service_name;
  char* topology_filename;
  char* addresses_filename;
  float sampling;
};

static error_t parse_opt (int key, char *arg, struct argp_state *state) {
  struct arguments *arguments = (struct arguments*) state->input;

  switch (key)
    {
    case 'c':
      arguments->concurrency = atoi(arg);
      break;
    case 'r':
      arguments->requests = atoi(arg);
      break;
    case 'o':
      arguments->openloop = true;
      break;
    case 'l':
      arguments->limit = atoi(arg);
      break;
    case 'd':
      arguments->debug = true;
      break;
    case 't':
      arguments->topology_filename = arg;
      break;
    case 'i':
      arguments->interval = atoi(arg);
      break;
    case 'a':
      arguments->addresses_filename = arg;
      break;
    case 's':
      arguments->sampling = atof(arg);
    case ARGP_KEY_ARG:
      if (state->arg_num >= 1)
        /* Too many arguments. */
        argp_usage (state);

      arguments->service_name = arg;
      break;

    case ARGP_KEY_END:
      if (state->arg_num < 1)
        /* Not enough arguments. */
        argp_usage (state);
      break;

    default:
      return ARGP_ERR_UNKNOWN;
    }
  return 0;
}

static struct argp argp = { options, parse_opt, args_doc, doc };

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using hindsightgrpc::HindsightGRPC;
using hindsightgrpc::ExecRequest;
using hindsightgrpc::ExecReply;
using opentelemetry::sdk::trace::IdGenerator;
using opentelemetry::sdk::trace::RandomIdGenerator;

bool debug;
float sample_probability;

uint32_t max_requests;
std::atomic_uint64_t global_count;


std::atomic_bool alive;
std::atomic_bool iserror;

uint64_t now() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
    std::chrono::high_resolution_clock::now().time_since_epoch()).count();
}
class HindsightGRPCClient {
 public:
  uint64_t min_latency = 0xFFFFFFFFFFFFFFFF;
  uint64_t max_latency = 0;
  double avg_latency = 0;
  // request start time stamp
  uint64_t begin; // epoch time in microseconds
  uint64_t interval; // microseconds
  // generating trace id in the client
  IdGenerator* id_generator = new RandomIdGenerator();
  bool openloop;
  int requests;

  // For generating openloop interarrival times
  std::minstd_rand rng;
  std::exponential_distribution<double> exp;

  explicit HindsightGRPCClient(int id, std::shared_ptr<Channel> channel, 
      const std::map<std::string, hindsightgrpc::API> apis, uint64_t interval_s, bool openloop, int requests)
      : stub_(HindsightGRPC::NewStub(channel)), apis_(apis), begin(now()), interval(interval_s * 1000000ULL), 
      openloop(openloop), requests(requests), rng(id), exp(((double) requests) / 1000000000.0) {}

  void ExecNext() {
    auto api_iter = apis_.begin();
    std::advance(api_iter, rand() % apis_.size());
    Exec(api_iter->first);
  }

  // Assembles the client's payload and sends it to the server.
  void Exec(const std::string& api_name) {

    // Call object to store rpc data
    AsyncClientCall* call = new AsyncClientCall;
    call->start_time = now();

    // for client side statistics
    uint64_t interval = call->start_time / this->interval;

    // Data we are sending to the server.
    ExecRequest request;
    request.set_api(api_name);
    request.set_debug(debug);
    request.set_interval(interval);

    // generating trace id and making head-based sampling decision
    auto tid_raw = id_generator->GenerateTraceId();
    char tid_buffer[32];
    tid_raw.ToLowerBase16(nostd::span<char, 32>{&tid_buffer[0], 32});
    request.mutable_otel()->set_trace_id(std::string(tid_buffer, 32));
    // special span id
    request.mutable_otel()->set_span_id(std::string("ffffffffffffffff"));
    // set the sample flag with probability specified by user commands
    request.mutable_otel()->set_sample(rand() / sample_probability > RAND_MAX ? false : true);

    // truncate the first 64 bits to fit into hindsight
    uint64_t trace_id = *((uint64_t*) tid_raw.Id().data());
    request.mutable_hindsight()->set_trace_id(trace_id);
    // 0 means there is no real parent span
    request.mutable_hindsight()->set_span_id(0);
    request.mutable_hindsight()->set_triggerflag(true);

    // Create the RPC object, but don't actually send it yet
    call->response_reader =
        stub_->PrepareAsyncExec(&call->context, request, &cq_);

    // StartCall initiates the RPC call
    call->response_reader->StartCall();

    // Register the call object to handle the RPC response
    call->response_reader->Finish(&call->reply, &call->status, (void*)call);
  }

  // Loop while listening for completed responses.
  // Prints out the response from the server.
  void AsyncCompleteRpc() {
    void* got_tag;
    bool ok = false;

    uint32_t start_recording = now() + 1000000; // 1 second lead-in before recording request latency
    uint32_t sent_count = 0;
    uint32_t received_count = 0;
    uint64_t max_outstanding = 2 * requests;

    // Both open-loop and closed-loop will poll the completion queue with a timeout.
    // For closed-loop it will wake up every 100ms
    // For open-loop the wake-up depends on request rate
    gpr_timespec deadline;
    deadline.clock_type = GPR_TIMESPAN;
    deadline.tv_sec = 0;
    deadline.tv_nsec = 100000000;

    // Used for open-loop
    uint64_t ns_per_request = 1000000000LL / requests;
    uint64_t next_request_at = now() * 1000 + ns_per_request * ((double) rng()) / ((double) rng.max());

    // If closed-loop, submit initial requests
    if (!openloop) {
      for (int i = 0; i < requests; i++) {
        sent_count++;
        ExecNext();
      }
    }

    // Block until the next result is available in the completion queue "cq".
    while (alive) {
      grpc::CompletionQueue::NextStatus status;
      if (openloop) {
        uint64_t t = now() * 1000;
        if (t > next_request_at) {
          status = grpc::CompletionQueue::NextStatus::TIMEOUT;
        } else {
          deadline.tv_sec = (next_request_at - t) / 1000000000LL;
          deadline.tv_nsec = (next_request_at - t) % 1000000000LL;
          status = cq_.AsyncNext(&got_tag, &ok, deadline);
        }
      } else {
        status = cq_.AsyncNext(&got_tag, &ok, deadline);
      }

      if (status == grpc::CompletionQueue::NextStatus::TIMEOUT) {

        // Openloop submits requests on a timer
        if (openloop) {
          next_request_at += exp(rng);
          if (max_requests == 0 || sent_count < max_requests) {
            if (sent_count - received_count < max_outstanding) {
              sent_count++;
              ExecNext();
            }
          } else if (received_count < max_requests) {
            continue;
          } else {
            break;
          }
        }

      } else if (status == grpc::CompletionQueue::NextStatus::GOT_EVENT) {

        // The tag in this example is the memory location of the call object
        AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);

        // Verify that the request was completed successfully. Note that "ok"
        // corresponds solely to the request for updates introduced by Finish().
        if (!ok) {
          if (!iserror) {
            iserror = true;
            std::cout << "Error in RPC CQ\n";
          }
        } else if (!call->status.ok()) {
          if (!iserror) {
            iserror = true;
            std::cout << "Call did not return OK status\n";
          }
        } else {
          /* Calculate statistics of successful completed requests */
          iserror = false;
          received_count++;
          global_count++;

          if (call->start_time > start_recording) {
            uint64_t request_latency = now() - call->start_time;
            if (request_latency > max_latency) {
              max_latency = request_latency;
            } else if (request_latency < min_latency) {
              min_latency = request_latency;
            }
            avg_latency += request_latency;
          }
        }

        // Once we're complete, deallocate the call object.
        delete call;

        // Closed loop keeps submitting requests when previous one completes
        if (!openloop) {
          if (max_requests == 0 || sent_count < max_requests) {
            sent_count++;
            ExecNext();
          } else if (received_count < max_requests) {
            continue;
          } else {
            break;
          }
        }

      } else {
        break;
      }
    }

    avg_latency /= received_count;
  }

 private:
  // struct for keeping state and data information
  struct AsyncClientCall {
    // Container for the data we expect from the server.
    ExecReply reply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // Storage for the status of the RPC upon completion.
    Status status;

    // used for latency tracking
    uint64_t start_time;

    std::unique_ptr<ClientAsyncResponseReader<ExecReply>> response_reader;
  };

  // Out of the passed in Channel comes the stub, stored here, our view of the
  // server's exposed services.
  std::unique_ptr<HindsightGRPC::Stub> stub_;
  const std::map<std::string, hindsightgrpc::API> apis_;

  // The producer-consumer queue we use to communicate asynchronously with the
  // gRPC runtime.
  CompletionQueue cq_;
};

void printthread(struct arguments arguments, std::atomic_bool* alive, std::vector<std::shared_ptr<HindsightGRPCClient>>* clients) {
  // Ignore first second of requests
  uint64_t lead_in = 1000000;
  usleep(lead_in);

  uint64_t start_running = now();
  uint64_t start_count = global_count;

  uint64_t print_every = 1000000;

  // print per second
  uint64_t last_print = start_running;
  uint64_t current_count = start_count;
  uint64_t next_print = last_print + print_every;
  while (*alive) {
    uint64_t t;
    while ((t = now()) < next_print && *alive) {
      usleep(10000);
    }
    uint64_t next_count = global_count;
    uint64_t duration = t - last_print;

    double duration_s = ((double) duration) / 1000000.0;
    double tput = ((double) (next_count - current_count)) / duration_s;
    printf("%.0f requests/s (%d total)\n", tput, (next_count - current_count));

    next_print = next_print + print_every;
    current_count = next_count;
    last_print = t;
  }
  
  uint64_t t = now();
  double throughput = 1000000. * (global_count-start_count) / (t - start_running);

  uint64_t min_latency = 0xFFFFFFFFFFFFFFFF;
  uint64_t max_latency = 0;
  double avg_latency = 0;

  for (auto client : (*clients)) {
    min_latency = std::min(min_latency, client->min_latency);
    max_latency = std::max(max_latency, client->max_latency);
    avg_latency += client->avg_latency / arguments.concurrency / 1000;
  }

  std::cout << "Duration: " << ((t - start_running) / 1000000) << std::endl; 
  std::cout << "Total requests: " << (global_count-start_count) << std::endl;
  std::cout << "overall throughput: " << throughput << " requests/s\n";

  std::cout << "Average / Max / Min latency of a request is: " << avg_latency
            << "/" << max_latency / float(1000) << "/"
            << min_latency / float(1000) << " ms\n";
}

void exitHandler(int signum) {
  exit(signum);
}

void shutdownHandler(int signum) {
  std::cout << "Exiting\n";
  alive = false;

  signal(SIGTERM, exitHandler);
  signal(SIGINT, exitHandler);
}


char standalone_service_name[] = "service1";
char standalone_topology_filename[] = "../config/single_server_topology.json";
char standalone_addresses_filename[] = "../config/single_server_addresses.json";


int main(int argc, char** argv) {

  struct arguments arguments;

  /* Default values. */
  arguments.concurrency = 1;
  arguments.requests = 1;
  arguments.debug = false;
  arguments.limit = -1;
  arguments.service_name = NULL;
  arguments.topology_filename = NULL;
  arguments.addresses_filename = NULL;
  arguments.interval = 10;
  arguments.sampling = 1;
  arguments.openloop = false;

  /* Parse the arguments */
  argp_parse (&argp, argc, argv, 0, 0, &arguments);

  sample_probability = arguments.sampling;

  if (arguments.requests < 1) {
    std::cout << "Must use a positive value for -r --requests; got " << arguments.requests << std::endl;
    return 1;
  }

  /* If 'standalone' is specified as the service name, it is a special case */
  if (strcmp(arguments.service_name, "standalone") == 0) {
    std::cout << "Using the built-in standalone configuration" << std::endl;
    arguments.service_name = standalone_service_name;
    arguments.topology_filename = standalone_topology_filename;
    arguments.addresses_filename = standalone_addresses_filename;
  }

  /* Load the topology file and find the named service */
  if (arguments.topology_filename == NULL) {
    std::cerr << "Expected a topology file to be specified" << std::endl;
    return 0;
  }
  std::cout << "Loading topology from " << arguments.topology_filename << std::endl;
  json config = hindsightgrpc::parse_config(arguments.topology_filename);

  /* Load the addresses */
  if (arguments.addresses_filename == NULL) {
    std::cerr << "Expected an addresses file to be specified" << std::endl;
    return 0;
  }
  std::cout << "Loading addresses from " << arguments.addresses_filename << std::endl;
  json addr_config = hindsightgrpc::parse_config(arguments.addresses_filename);
  std::map<std::string, hindsightgrpc::AddressInfo> addresses = hindsightgrpc::get_address_map(addr_config);
  
  hindsightgrpc::ServiceConfig service_config = hindsightgrpc::get_service_config(config, arguments.service_name, addresses);
  if (service_config.Name() == "") {
    std::cerr << "Unable to find service " << arguments.service_name << " in topology " << arguments.topology_filename << std::endl;
    return 1; 
  }
  std::vector<std::string> connection_addresses = addresses[arguments.service_name].connection_addresses;

  // Now set up the client threads
  debug = arguments.debug;
  global_count = 0;
  if (arguments.limit >= 0) {
    max_requests = arguments.limit;
  } else {
    max_requests = debug ? 1 : 0;
  }
  alive = true;
  iserror = false;


  // register signal SIGINT and signal handler
  signal(SIGTERM, shutdownHandler);
  signal(SIGINT, shutdownHandler);


  std::vector<std::thread> threads;
  const std::map<std::string, hindsightgrpc::API> apis = service_config.get_apis();

  std::vector<std::shared_ptr<HindsightGRPCClient>> clients;
  for (int i = 0; i < arguments.concurrency; i++) {
    // clients.push_back(std::make_shared<HindsightGRPCClient>(i,
    //     grpc::CreateChannel(connection_address, grpc::InsecureChannelCredentials()), apis, 
    //     arguments.interval, arguments.openloop, arguments.requests));
    auto connection_address = connection_addresses[rand() % connection_addresses.size()];
    clients.push_back(std::make_shared<HindsightGRPCClient>(i,
        grpc::CreateChannel(connection_address, grpc::InsecureChannelCredentials()), apis, arguments.interval, arguments.openloop, arguments.requests));

    // Spawn reader thread that loops indefinitely
    threads.push_back(
        std::thread(&HindsightGRPCClient::AsyncCompleteRpc, clients[i]));
  }

  std::atomic_bool printer_alive{true};

  std::thread printer = std::thread(&printthread, arguments, &printer_alive, &clients);


  if (max_requests == 0) {
    std::cout << "Press control-c to quit" << std::endl << std::endl;
  }
  
  for (int i = 0; i < threads.size(); i++) {
    threads[i].join();
  }

  printer_alive = false;
  printer.join();

  return 0;
}
