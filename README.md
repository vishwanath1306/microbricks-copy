# MicroBricks

`microbricks` is a simple RPC server that can be deployed multiple times in a pre-defined topology.  We provide several out-of-the-box topologies including one derived from an Alibaba trace dataset.  The purpose of `microbricks` is to serve as a benchmarking application for different distributed tracing frameworks, such as opentracing and, of course, `hindsight`.  Both `hindsight` and `microbricks` are ongoing research projects, so documentation is incomplete.

### hindsight-grpc

During development, `microbricks` was also called `hindsight-grpc`, and any mention of `hindsight-grpc` in the repository is synonymous with `microbricks`.

## Pre-requisites

### gRPC

Install gRPC, following the gRPC c++ quickstart instructions [here](https://grpc.io/docs/languages/cpp/quickstart/).

At the time of writing, the instructions used gRPC v1.43.0

During gRPC installation, you should follow the instructions for installing gRPC to a local cmake install directory.  In the instructions they refer to this as `$MY_INSTALL_DIR`.  Remember this (or set it in your bashrc) because we will use it when building this project:

* Later on, if we want to install a package, we would add the cmake flag `-DCMAKE_INSTALL_PREFIX=$MY_INSTALL_DIR` to instruct cmake to install to the local directory
* Later on, when building this project, we will be setting the cmake flag `-DCMAKE_PREFIX_PATH=$MY_INSTALL_DIR` to instruct cmake to pull the grpc dependencies from this local directory.

***Note:*** when I followed the default instructions on an MPI machine, everything worked *except* for installing zlib, which for some reason was ignoring the local directory installation flag.  If you encounter this problem it can be rectified by adding `-DgRPC_ZLIB_PROVIDER=package` to your cmake command, before building and installing.

### OpenTelemetry

To install opentelemetry, first we must install its own dependencies

#### Googletest

```
git clone https://github.com/google/googletest.git -b release-1.11.0
cd googletest
mkdir build
cd build
cmake -DCMAKE_PREFIX_PATH=$MY_INSTALL_DIR -DCMAKE_INSTALL_PREFIX=$MY_INSTALL_DIR ..
make -j
make install
```

#### Google benchmark

```
git clone https://github.com/google/benchmark.git -b v1.6.1
cd benchmark
cmake -E make_directory "build"
cmake -E chdir "build" cmake -DBENCHMARK_DOWNLOAD_DEPENDENCIES=on -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=$MY_INSTALL_DIR -DCMAKE_INSTALL_PREFIX=$MY_INSTALL_DIR -DBENCHMARK_ENABLE_GTEST_TESTS=OFF ../
cmake --build "build" --config Release 
cmake --build "build" --config Release --target install
```

#### Thrift

```
# https://thrift.apache.org/docs/BuildingFromSource
sudo apt-get install automake bison flex g++ git libboost-all-dev libevent-dev libssl-dev libtool make pkg-config qt5-default libcurl4-gnutls-dev
wget https://dlcdn.apache.org/thrift/0.16.0/thrift-0.16.0.tar.gz
tar -xvf thrift-0.16.0.tar.gz
cd thrift-0.16.0
./bootstrap.sh
# we only build thrift C, C++ and Python libraries
./configure --without-java --without-go
make
// system wise installation
sudo make install
```

#### Opentelemetry

Important: OpenTelemetry Jaeger exporter has a bug that we must patch.  Assuming you have `hindsight-grpc` checked out in `~/hindsight-grpc`

```
git clone -b v1.1.0 --recurse-submodules  https://github.com/open-telemetry/opentelemetry-cpp
cd opentelemetry-cpp
git apply ~/hindsight-grpc/opentelemetry-1.1.0.patch
```

Then proceed to build

```
mkdir build
cd build
cmake -DWITH_ABSEIL=ON -DWITH_JAEGER=ON -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=$MY_INSTALL_DIR -DCMAKE_INSTALL_PREFIX=$MY_INSTALL_DIR ..
make
make install
```

### Hindsight

Follow the instructions to build and install Hindsight https://gitlab.mpi-sws.org/cld/tracing/hindsight

Unlike gRPC, Hindsight must be installed system-wide using sudo.

## Building this project

```
mkdir build
cd build
cmake -DCMAKE_PREFIX_PATH=$MY_INSTALL_DIR -DCMAKE_BUILD_TYPE=Release ..
make
```

This should produce some binaries in the `build` directory

## Running

### Running a Server

After building, you can run a standalone server using the following command:

```
./server standalone
```

To enable hindsight, run

```
./server --tracing=hindsight standalone
```

To run a server from a more complicated topology, you must specify a topology and addresses file, as well as a matching service_name that is defined in those files.

```
./server -t /path/to/topology.json -a /path/to/addresses.json [--tracing=hindsight] service_name
```
This will run the server.

For example, the full command for the standalone server can alternately be specified as:
```
./server -t ../config/single_server_topology.json -a ../config/single_server_addresses.json [--tracing=hindsight] service1
```

For more information about the topology and addresses file, see below.

#### Server Command-Line Arguments

Run `./server --help` for a full description of server arguments:

```
Usage: server [OPTION...] SERV
A gRPC-based benchmarking program for building a topology of RPC servers that
connect to each other.  Addresses and topologies are provided via a config file
-- see the config directory for examples.
To run a standalone server, run ./server standalone -- otherwise, SERV must be
specified along with an addresses and topology file, and SERV must be defined
in the topology file and match the serv argument given to the hindsigt agent.

  -a, --addresses=FILE       An addresses file.  This is required.  See
                             config/example_addresses.json for an example.
  -c, --concurrency=NUM      The server concurrency, ie the number of request
                             processing threads to run
  -f, --trigger=ID:P         Install a trigger for queue ID with probability P.
  -n, --nocompute            Disables RPC computation, overriding the `exec`
                             value from the topology file.  This makes all RPCs
                             do no computation and return immediately.
  -h, --otel_host=HOST       Address of the OpenTelemetry collector to send spans.
                             This is required for ot-jaeger.
  -p, --otel_port=NUM        Port of the OpenTelemetry collector to send spans.
                             This is required for ot-jaeger.
  -s, --otel_simple          If this flag is set, use the OpenTelemetry simple
                             span processor.  Otherwise uses the batch
                             processor.
  -i, --instance_id=NUM      Instance id of the assigned service. Default 0.
  -t, --topology=FILE        A topology file.  This is required.  See
                             config/example_topology.json for an example.
  -x, --tracing=TRACER       Tracing to use, optional.  TRACER can be one of:
                             none, hindsight, ot-hindsight, ot-jaeger,
                             ot-stdout, ot-noop, ot-local.  `none` disables
                             tracing.  `hindsight` uses direct Hindsight
                             instrumentation.  `ot-noop` enables OpenTelemetry
                             but uses a NoOp tracer.  `ot-stdout` logs
                             OpenTelemetry spans to stdout.  Useful for testing
                             and debugging.  `ot-jaeger` OpenTelemetry
                             configured with Jaeger -- not currently
                             implemented.  `ot-local` Logs OpenTelemetry spans
                             to a small in-memory ring buffer.  `ot-hindsight`
                             Hindsight's OpenTelemetry tracer.  It's better to
                             use hindsight than ot-hindsight.
  -?, --help                 Give this help list
      --usage                Give a short usage message

```

***Disabling Computation.***  Servers will perform some dummy computation according to the `exec` value specified in the topology file.  `exec` roughly corresponds to cpu-milliseconds.  When starting a server, you can use the `--nocompute` flag to disable the dummy computation entirely, making RPCs basic request-response.

***Choosing a tracer.***  The server is instrumented with OpenTracing and there are several OpenTracing tracers you can choose from by specifying the `--tracing` flag.  By specifying `--tracing=ot-hindsight` you can use Hindsight's OpenTelemetry integration.  Alternatively, by specifying `--tracing=hindsight` you can use Hindsight's direct (non-OpenTelemetry) instrumentation.  We recommend using `--tracing=hindsight` instead of `--tracing=ot-hindsight`.

***Firing triggers.***  You can install triggers in a server to randomly fire with a specific probability.  You can add more than one trigger.  Use the `--trigger` flag to do so.  `--trigger=7:0.5` will install a trigger for queue ID `7` with probability `0.5`.  By default no triggers are installed.  If OpenTelemetry is being used, then when a trigger is fired, it will add two attributes to the span: one with key `Trigger` and one with key `TriggerQueue{$QUEUEID}`, both with value queue ID.  For example, if the trigger `7` fires, we will get a span with `Trigger`:`7` and `TriggerQueue7`:`7`.  The reason for multiple attributes is to handle the case where we have multiple triggers installed.

### Running a Client

To run a client to the standalone server, use the following command:

```
./client standalone
```

The client continuously submits requests and prints its throughput to the command line once per second.  You can use the `--limit` flag to only submit a fixed number of requests before exiting.  You can use the `--debug` flag to instruct all servers to print debug information for the request.

As with the server, you can run a client to a more complicated topology by specifying a topology and addresses file

```
./client -t /path/to/topology.json -a /path/to/addresses.json service_name
```

For example, the full command for the standalone client can alternately be specified as:
```
./client -t ../config/single_server_topology.json -a ../config/single_server_addresses.json service1
```

***Printing debug info.*** If you run a client with the `--debug` flag, e.g. `./client --debug standalone` it will instruct all servers to print detailed information about this request.  

#### Client Command-Line Arguments

Run `./client --help` for a full description of client arguments:

```
Usage: client [OPTION...] SERV
A gRPC async client to the server.  The SERV argument specifies which server
from the topology file to connect to.To run a standalone client, simply run
./server standalone

  -a, --addresses=FILE       An addresses file.  This is required.  See
                             config/example_addresses.json for an example.
  -c, --concurrency=NUM      The number of concurrent client threads to run.
                             Each thread has its own RPC client.  Default 1.
  -d, --debug                Print debug information on all servers.  If debug
                             is enabled, the default value for limit will be
                             set to 1.
  -i, --interval=NUM         Interval size in seconds, default 10.  Each trace
                             will log the interval when it was generated.
  -l, --limit=LIMIT          The total number of requests to submit before
                             exiting.  Set to 0 for no limit.  Default 0.
  -o, --openloop             If set, runs as an open-loop client.  If left
                             unset, runs as a closed-loop client
  -r, --requests=NUM         If running as an closed-loop client, this
                             specifies the number of concurrent outstanding
                             requests per client.  If running as an open-loop
                             client, this specifies the request rate per second
                             per client.  Default 1.
  -s, --sampling=NUM         Probability of head-based sampling. Default 1.
  -t, --topology=FILE        A topology file.  This is required.  See
                             config/example_topology.json for an example.
  -?, --help                 Give this help list
      --usage                Give a short usage message


Mandatory or optional arguments to long options are also mandatory or optional
for any corresponding short options.
```

#### Running multiple servers

To run a slightly more complicated topology, you can run the following on the same machine

```
./server --t ../config/example_topology.json  -a ../config/example_addresses.json service1
./server --t ../config/example_topology.json  -a ../config/example_addresses.json service2
./server --t ../config/example_topology.json  -a ../config/example_addresses.json service3
``` 

Then run a client:

```
./client --t ../config/example_topology.json  -a ../config/example_addresses.json service1
```

### Calculating trace completeness

To calculate trace completeness, make sure Hindsight's collector is persisting the trace data to disk (see the instructions [here](https://gitlab.mpi-sws.org/cld/tracing/hindsight/-/blob/main/docs/collector.md)).

Once the data is written to disk, you can use the `./process` utility to calculate trace completion, using as input the file outputted by the Hindsight collector.

```
./process $TRACEFILENAME
```

You will see output like this:

```
$ ./process /local/traces.out
Processing /local/newtraces.out
Read 453665 buffers from /local/newtraces.out
57436 traces total
   I Trigger Status   Count    Pct Description
 All     All      0   55980  97.47 Valid
                  4       2   0.00 Buffers ended with a partial fragment of trace data
                  7    1454   2.53 The number of RPCs executed did not match the number of child calls made.
 All       x      4       2   0.35 Buffers ended with a partial fragment of trace data
                  7     576  99.65 The number of RPCs executed did not match the number of child calls made.
 All       7      0   55980  98.46 Valid
                  7     878   1.54 The number of RPCs executed did not match the number of child calls made.
   0     All      0   10635  91.69 Valid
                  7     964   8.31 The number of RPCs executed did not match the number of child calls made.
   0       x      7     432 100.00 The number of RPCs executed did not match the number of child calls made.
   0       7      0   10635  95.24 Valid
                  7     532   4.76 The number of RPCs executed did not match the number of child calls made.
   1     All      0    6161  96.69 Valid
                  7     211   3.31 The number of RPCs executed did not match the number of child calls made.
   1       7      0    6161  96.69 Valid
                  7     211   3.31 The number of RPCs executed did not match the number of child calls made.
   2     All      0    5093  97.36 Valid
                  7     138   2.64 The number of RPCs executed did not match the number of child calls made.
   2       x      7       3 100.00 The number of RPCs executed did not match the number of child calls made.
```

In the above output, the results are grouped by interval (the `I` column) and by trigger.

There are also some extra options for verbose output:

```
Usage: process [OPTION...] FILENAME
Process data received by Hindsight's backend into traces and calculate trace
completion.  Takes as argument the collector data file

  -d, --debug                Print debug information.  Spammy.
  -w, --warn                 Print information about malformed traces.
  -?, --help                 Give this help list
      --usage                Give a short usage message
```

### Explanation of topology file

TODO

### Explanation of addresses file

TODO

### Explanation of matrix_benchmarks

TODO
