/*
 * Copyright 2022 Max Planck Institute for Software Systems *
 */

#include "hindsightgrpc/server.h"
#include "hindsightgrpc/topology.h"
#include "tracing/opentelemetry.h"
#include "tracing/hindsight_opentelemetry.h"
#include <map>
#include <string>
#include <argp.h>


const char *program_version = "hindsight-grpc 1.0";
const char *program_bug_address = "<cld-science@mpi-sws.org>";
static char doc[] = "A gRPC-based benchmarking program for building a topology"
                    " of RPC servers that connect to each other.  Addresses "
                    "and topologies are provided via a config file -- see the "
                    "config directory for examples.\n"
                    "To run a standalone server, run ./server standalone -- "
                    "otherwise, SERV must be specified along with an addresses"
                    " and topology file, and SERV must be defined in the topol"
                    "ogy file and match the serv argument given to the hindsig"
                    "t agent.";
static char args_doc[] = "SERV";

static struct argp_option options[] = {
  {"concurrency",  'c', "NUM",  0,  "The server concurrency, ie the number of request processing threads to run" },
  {"tracing",  'x', "TRACER",  0,  "Tracing to use, optional.  TRACER can be one of: "
                                   "none, hindsight, ot-hindsight, ot-jaeger, ot-stdout, ot-noop, ot-local.  "
                                   "`none` disables tracing.  "
                                   "`hindsight` uses direct Hindsight instrumentation.  "
                                   "`ot-noop` enables OpenTelemetry but uses a NoOp tracer.  "
                                   "`ot-stdout` logs OpenTelemetry spans to stdout.  Useful for testing and debugging.  "
                                   "`ot-jaeger` OpenTelemetry configured with Jaeger -- not currently implemented.  "
                                   "`ot-local` Logs OpenTelemetry spans to a small in-memory ring buffer.  "
                                   "`ot-hindsight` Hindsight's OpenTelemetry tracer.  It's better to use hindsight than ot-hindsight."},
  {"trigger",  'f', "ID:P",  0,  "Install a trigger for queue ID with probability P.  " },
  {"nocompute",  'n', 0,  0,  "Disables RPC computation, overriding the `exec` value from the topology file.  This makes all RPCs do no computation and return immediately." },
  {"debug",  'd', 0,  0,  "Turn on debug printing" },
  {"max_requests",  'm', "NUM",  0,  "Maximum number of concurrently-executing requests per handler.  Default 100" },
  {"topology", 't', "FILE", 0, "A topology file.  This is required.  See config/example_topology.json for an example." },
  {"addresses", 'a', "FILE", 0, "An addresses file.  This is required.  See config/example_addresses.json for an example." },
  {"otel_host", 'h', "HOST", 0, "Address of the OpenTelemetry collector to send spans. This is required for ot-jaeger." },
  {"otel_port", 'p', "NUM", 0, "Port of the OpenTelemetry collector to send spans. This is required for ot-jaeger." },
  {"otel_simple", 's', 0, 0, "If this flag is set, use the OpenTelemetry simple span processor.  Otherwise uses the batch processor." },
  {"instance_id", 'i', "NUM", 0, "Instance id of the assigned service. Default 0." },
  { 0 }
};

struct arguments {
  std::string tracing;
  bool nocompute;
  int server_threads;
  char* service_name;
  char* topology_filename;
  char* addresses_filename;
  std::string otel_collector_host;
  int otel_collector_port;
  bool otel_batch_exporter;
  int instance_id;
  int max_requests;
  std::map<int, float> triggers;
  bool debug;
};

static error_t parse_opt (int key, char *arg, struct argp_state *state) {
  struct arguments *arguments = (struct arguments*) state->input;

  switch (key)
    {
    case 'c':
      arguments->server_threads = atoi(arg);
      break;
    case 'x':
      arguments->tracing = std::string(arg);
      break;
    case 'n':
      arguments->nocompute = true;
      break;
    case 't':
      arguments->topology_filename = arg;
      break;
    case 'f': {
      std::string s = std::string(arg);
      std::string delimiter = ":";
      size_t pos = s.find(delimiter);
      if (pos == std::string::npos) {
        std::cout << "Invalid trigger " << s << " -- expected form is QUEUEID:PROBABILITY e.g. 7:0.5" << std::endl;
        argp_usage(state);
      } else {
        std::string queue_id_str = s.substr(0, pos);
        std::string trigger_probability_str = s.substr(pos+1, s.size());
        int queue_id = atoi(queue_id_str.c_str());
        float trigger_probability = atof(trigger_probability_str.c_str());
        arguments->triggers[queue_id] = trigger_probability;
      }
      break;
    }
    case 'a':
      arguments->addresses_filename = arg;
      break;
    case 'h':
      arguments->otel_collector_host = std::string(arg);
      break;
    case 'p':
      arguments->otel_collector_port = atoi(arg);
      break;
    case 'm':
      arguments->max_requests = atoi(arg);
      break;
    case 's':
      arguments->otel_batch_exporter = false;
      break;
    case 'i':
      arguments->instance_id = atoi(arg);
      break;
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

char standalone_service_name[] = "service1";
char standalone_topology_filename[] = "../config/single_server_topology.json";
char standalone_addresses_filename[] = "../config/single_server_addresses.json";

int main(int argc, char** argv) {

  struct arguments arguments;

  /* Default values. */
  arguments.tracing = "none";
  arguments.nocompute = false;
  arguments.server_threads = 1;
  arguments.service_name = NULL;
  arguments.topology_filename = NULL;
  arguments.addresses_filename = NULL;
  arguments.otel_collector_host = "none";
  arguments.otel_collector_port = -1;
  arguments.otel_batch_exporter = true;
  arguments.debug = false;
  arguments.instance_id = 0;
  arguments.max_requests = 100;

  /* Parse the arguments */
  argp_parse (&argp, argc, argv, 0, 0, &arguments);

  /* If 'standalone' is specified as the service name, it is a special case */
  if (strcmp(arguments.service_name, "standalone") == 0) {
    std::cout << "Using the built-in standalone configuration" << std::endl;
    arguments.service_name = standalone_service_name;
    arguments.topology_filename = standalone_topology_filename;
    arguments.addresses_filename = standalone_addresses_filename;
    arguments.otel_collector_host = "localhost";
    arguments.otel_collector_port = 6832;
  }

  /* Load the topology file */
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
  
  /* Find the named service */
  hindsightgrpc::ServiceConfig service_config = hindsightgrpc::get_service_config(config, arguments.service_name, addresses);
  if (service_config.Name() == "") {
    std::cerr << "Unable to find service " << arguments.service_name << " in topology " << arguments.topology_filename << std::endl;
    return 1; 
  }

  /* Generate the matrix multiplication configs for the APIs */
  service_config.generate_matrix_configs();
  service_config.print_matrix_configs();  

  /* Configure tracing */
  if (arguments.tracing == "none") {
    std::cout << "No tracing configured." << std::endl;
    hindsightgrpc::set_hindsight_enabled(false);
    hindsightgrpc::set_opentelemetry_enabled(false);

  } else if (arguments.tracing == "hindsight") {
    std::cout << "Using Hindsight tracing (without OpenTelemetry)." << std::endl;
    hindsightgrpc::set_hindsight_enabled(true);
    hindsightgrpc::set_opentelemetry_enabled(false);

    hindsightgrpc::AddressInfo info = addresses[arguments.service_name];
    std::string breadcrumb = info.breadcrumbs[arguments.instance_id];
    hindsightgrpc::initHindsight(std::string(arguments.service_name), breadcrumb);

  } else if (arguments.tracing == "ot-hindsight") {
    std::cout << "Using Hindsight tracing with OpenTelemetry." << std::endl;
    hindsightgrpc::set_hindsight_enabled(false);
    hindsightgrpc::set_opentelemetry_enabled(true);

    hindsightgrpc::AddressInfo info = addresses[arguments.service_name];
    std::string breadcrumb = info.breadcrumbs[arguments.instance_id];
    hindsightgrpc::initHindsightOpenTelemetry(std::string(arguments.service_name), breadcrumb);

  } else if (arguments.tracing == "ot-stdout") {
    std::cout << "Using stdout tracing with OpenTelemetry." << std::endl;
    hindsightgrpc::set_hindsight_enabled(false);
    hindsightgrpc::set_opentelemetry_enabled(true);

    hindsightgrpc::initStdoutOpenTelemetry();

  } else if (arguments.tracing == "ot-noop") {
    std::cout << "Using OpenTelemetry with noop tracing." << std::endl;
    hindsightgrpc::set_hindsight_enabled(false);
    hindsightgrpc::set_opentelemetry_enabled(true);

    hindsightgrpc::initNoopOpenTelemetry();  

  } else if (arguments.tracing == "ot-local") {
    std::cout << "Using OpenTelemetry with local in-memory tracing." << std::endl;
    hindsightgrpc::set_hindsight_enabled(false);
    hindsightgrpc::set_opentelemetry_enabled(true);

    hindsightgrpc::initLocalMemoryOpenTelemetry();    

  } else if (arguments.tracing == "ot-jaeger") {
    std::cout << "Using Jaeger tracing with OpenTelemetry." << std::endl;
    hindsightgrpc::set_hindsight_enabled(false);
    hindsightgrpc::set_opentelemetry_enabled(true);
    if (arguments.otel_collector_host == "none") {
      std::cerr << "Expected an address of otel_collector to be specified" << std::endl;
      return 0;
    }

    if (arguments.otel_collector_port < 0) {
      std::cerr << "Expected a port of otel_collector to be specified" << std::endl;
      return 0;
    }
    hindsightgrpc::initJaegerOpenTelemetry(arguments.otel_collector_host, arguments.otel_collector_port, arguments.otel_batch_exporter);
  } else {
    std::cout << "Unknown tracing type " << arguments.tracing << std::endl;
    return 1;
  }

  // Print the triggers
  for (auto &p : arguments.triggers) {
    std::cout << "Trigger " << p.first << "=" << p.second << std::endl;
  }

  // Start the server
  hindsightgrpc::ServerImpl server(service_config, addresses,
                                   arguments.nocompute, arguments.triggers,
                                   arguments.instance_id, arguments.max_requests);
  server.Run(arguments.server_threads, arguments.debug);
  server.Join();

  return 0;
}
