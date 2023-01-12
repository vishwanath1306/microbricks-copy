import json
import subprocess
import signal
import argparse
import threading
import time
import numpy as np
import math
import os

parser = argparse.ArgumentParser(description='Run a hindsight-grpc benchmark')
parser.add_argument("out", metavar="DIR", type=str, help="Output directory for process logs and results")
parser.add_argument("--benchmark", metavar="BENCHMARK", type=str, default="two", help="Name of the benchmark to run.  Currently supported benchmarks: one two four.  Default two")
parser.add_argument("--tmp", metavar="DIR", type=str, default="", help='Output directory for temporary output (primarily, collector traces).  If not specified then we wont write traces to disk')
parser.add_argument("--hindsight", metavar="PATH", type=str, default="~/hindsight", help='Path to hindsight directory. Default ~/hindsight')
parser.add_argument("--ot_collector", metavar="PATH", type=str, default="~/otel46/otelcontribcol_linux_amd64", help='Path to opentelemetry collector binary.  Default ~/otel46/otelcontribcol_linux_amd64')
parser.add_argument("--server_concurrency", metavar="NUM", type=int, default=8, help='Value to use for server --concurrency parameter.  Default 8')
parser.add_argument("--server_rate", metavar="NUM", type=int, default=10, help='Network bandwidth limit for hindsight agents. Default 10')
parser.add_argument("--nocompute", action='store_true', help='Set servers to -nocompute mode.  Default false')
parser.add_argument("--tracing", metavar="TRACER", type=str, default="none", help='Tracer to use for server --tracing parameter.  Valid options come from the ./server binary -- none hindsight ot-jaeger etc.  Default none')
parser.add_argument('-c', "--clients", metavar="NUM", type=int, default=1, help='Number of concurrent clients for client --concurrency parameter.  Default 1.')
parser.add_argument('-r', "--requests", metavar="NUM", type=int, default=1, help='Number of requests to configure for client --requests parameter.  Default 1')
parser.add_argument("--openloop", action='store_true', help='Sets clients to -openloop mode.  Default false.')
parser.add_argument('-s', "--sampling", metavar="NUM", type=str, default="", help='Sampling / trigger percentage to use.  With opentelemetry this sets head-sampling probability.  With hindsight this sets trigger percentage.  Default 0')
parser.add_argument("--silent", action='store_true', help='By default we will prompt before proceeding with the experiment.  Set this to disable.')
parser.add_argument('-d', "--duration", metavar="NUM", type=int, default=30, help='Benchmark duration in seconds.  Default 30')


class Benchmark:
    
    def __init__(self, name, topology, gateways):
        self.name = name
        self.topology = topology
        self.gateways = gateways
        self.addresses_filename = "../config/%s_addresses.json" % topology
        self.topology_filename = "../config/%s_topology.json" % topology
        self.otel_config_filename = "../config/sample_otel_collector_config.yaml"
        self.files = []
        self.processes = []
    
    def load(self):
        with open(self.addresses_filename, "r") as f:
            data = json.load(f)
        self.services = dict()
        for service in data["addresses"]:
            self.services[service["name"]] = service
    
    def summarize(self, args):
        print("Benchmark %s:" % (self.name, ))
        print("  Writing output to %s" % args.out)
        if args.tmp == "":
            print("  Not writing temporary output")
        else:
            print("  Writing temporary output to %s" % args.tmp)

        tracing = "tracing=%s" % args.tracing
        if args.tracing=="hindsight" or args.tracing=="ot-hindsight":
            if args.sampling == "":
                tracing = "tracing=%s, no triggers" % args.tracing
            else:
                tracing = "tracing=%s, trigger=%s" % (args.tracing, args.sampling)
        elif args.tracing.startswith("ot-"):
            if args.sampling == "":
                tracing = "tracing=%s, no sampling" % args.tracing
            else:
                tracing = "tracing=%s, head-sampling=%s" % (args.tracing, args.sampling)
        
        if args.nocompute:
            print("  Servers: %d, concurrency=%d, %s" % (len(self.services), args.server_concurrency, tracing))
        else:
            print("  Servers: %d nocompute, concurrency=%d, %s" % (len(self.services), args.server_concurrency, tracing))
        
        if args.openloop:
            print("  Clients: %d openloop, %d r/s (%d r/s total)" % (args.clients, args.requests, args.requests * args.clients))
        else:
            print("  Clients: %d closedloop, concurrency %d (%d total)" % (args.clients, args.requests, args.requests * args.clients))
        
        if args.tracing == "hindsight" or args.tracing=="ot-hindsight":
            print("  Hindsight Agents: %d; 1 collector; 1 coordinator" % len(self.services))
        elif args.tracing == "ot-jaeger":
            print("  OpenTelemetry Collectors: 1")
        
        print("Benchmark duration: %ds" % args.duration)
        


    
    def reset_shm(self, args):
        files = ["complete_queue", "triggers_queue", "pool", "breadcrumbs_queue", "available_queue"]
        for service in self.services:
            for file in files:
                cmd = ["rm", "-v", "/dev/shm/%s__%s" % (service, file)]
                child = subprocess.Popen(cmd)
                child.wait()
        print("Reset shm")
    
    def mkdirs(self, args):
        mkdirs = False
        if not os.path.isdir(args.out):
            print("out dir %s does not exist." % args.out)
            mkdirs = True
        if args.tmp != "":
            if not os.path.isdir(args.tmp):
                print("tmp dir %s does not exist." % args.tmp)
                mkdirs = True
        if mkdirs and not args.silent:
            print("Press <return> to create it or CTRL-C to abort")
            input()
        
        if not os.path.isdir(args.out):
            os.makedirs(args.out)
        if args.tmp != "" and not os.path.isdir(args.tmp):
            os.makedirs(args.tmp)
    
    def get_cpu_usage(self, args):
        pids = ",".join([str(p.pid) for p in self.processes])
        cmd_args = ["ps"]
        cmd_args += ["--ppid", pids]
        cmd_args += ["-p", pids]
        cmd_args += ["-o", "%cpu,%mem,cmd"]
        print(" ".join(cmd_args))
        output = "%s/cpu.out" % args.out
        with open(output, "w") as f:
            child = subprocess.Popen(cmd_args, stdout=f, stderr=f)
            child.wait()


    def stop(self, args):
        for p in self.processes:
            try:
                os.killpg(os.getpgid(p.pid), signal.SIGINT)
            except Exception as e:
                print(str(e))                    
        for p in self.processes:
            p.wait()
        for f in self.files:
            try:
                f.close()
            except Exception as e:
                print(str(e))
        self.processes = []
        self.files = []
    
    def run_client(self, args):
        cmd_args = ["./client"]
        cmd_args += ["--addresses=%s" % self.addresses_filename]
        cmd_args += ["--topology=%s" % self.topology_filename]
        cmd_args += ["--concurrency=%d" % args.clients]
        cmd_args += ["--requests=%d" % args.requests]
        if args.sampling != "" and args.tracing.startswith("ot-"):
            cmd_args += ["--sampling=%s" % args.sampling]
        if args.openloop:
            cmd_args += ["--openloop"]
        cmd_args += self.gateways

        cmd = [str(v) for v in cmd_args]
        print(" ".join(cmd))
        
        output = "%s/client.out" % args.out
        f = open(output, "w")
        self.files.append(f)

        p = subprocess.Popen(cmd, stdout=f, stderr=f, cwd="../build", preexec_fn=os.setsid)
        self.processes.append(p)
    
    def run_servers(self, args):
        trigger_installed = False
        for servicename, service in self.services.items():
            cmd_args = ["./server"]
            cmd_args += ["--addresses=%s" % self.addresses_filename]
            cmd_args += ["--topology=%s" % self.topology_filename]
            cmd_args += ["--concurrency=%d" % args.server_concurrency]
            if args.sampling != "" and (args.tracing == "hindsight" or args.tracing == "ot-hindsight") and not trigger_installed:
                cmd_args += ["--trigger=7:%s" % args.sampling]
                trigger_installed = True # only fire the trigger at one server
            if args.tracing == "ot-jaeger":
                cmd_args += ["--otel_host=localhost"]
                cmd_args += ["--otel_port=6833"] # all servers should be able to use same OT port
            if args.nocompute:
                cmd_args += ["--nocompute"]
            cmd_args += ["--tracing=%s" % args.tracing]
            cmd_args += [service["name"]]

            cmd = [str(v) for v in cmd_args]
            print(" ".join(cmd))
            
            output = "%s/%s.out" % (args.out, servicename)
            f = open(output, "w")
            self.files.append(f)

            p = subprocess.Popen(cmd, stdout=f, stderr=f, cwd="../build", preexec_fn=os.setsid)
            self.processes.append(p)

    def run_agents(self, args):
        if args.tracing != "hindsight" and args.tracing != "ot-hindsight":
            return []

        for servicename, service in self.services.items():
            cmd_args = ["go", "run", "cmd/agent2/main.go"]
            cmd_args += ["-serv", service["name"]]
            cmd_args += ["-host", service["hostname"]]
            cmd_args += ["-port", service["agent_port"]]
            cmd_args += ["-lc", "localhost:5252"]
            cmd_args += ["-r", "localhost:5253"]
            cmd_args += ["-rate", args.server_rate]

            cmd = [str(v) for v in cmd_args]
            print(" ".join(cmd))

            output = "%s/agent_%s.out" % (args.out, servicename)
            f = open(output, "w")
            self.files.append(f)

            p = subprocess.Popen(cmd, stdout=f, stderr=f, cwd="%s/agent" % args.hindsight, preexec_fn=os.setsid)
            self.processes.append(p)

    def run_backends(self, args):
        if args.tracing == "hindsight" or args.tracing == "ot-hindsight":
            cmd_args = [
                "go", "run", "cmd/collector/main.go",
                "-port", "5253"
            ]
            if args.tmp != "":
                cmd_args += ["-out", "%s/collector.out" % args.tmp]

            cmd = [str(v) for v in cmd_args]
            print(" ".join(cmd))

            output = "%s/hindsight_collector.out" % args.out
            f = open(output, "w")
            self.files.append(f)

            p = subprocess.Popen(cmd, stdout=f, stderr=f, cwd="%s/agent" % args.hindsight, preexec_fn=os.setsid)
            self.processes.append(p)


        if args.tracing == "hindsight" or args.tracing == "ot-hindsight":
            cmd_args = [
                "go", "run", "cmd/coordinator/main.go",
                "-port", "5252"
            ]
            if args.tmp != "":
                cmd_args += ["-out", "%s/coordinator.out" % args.tmp]

            cmd = [str(v) for v in cmd_args]
            print(" ".join(cmd))

            output = "%s/hindsight_coordinator.out" % args.out
            f = open(output, "w")
            self.files.append(f)

            p = subprocess.Popen(cmd, stdout=f, stderr=f, cwd="%s/agent" % args.hindsight, preexec_fn=os.setsid)
            self.processes.append(p)
        
        if args.tracing == "ot-jaeger":
            cmd_args = [
                args.ot_collector,
                "--config", "%s/%s" % (os.getcwd(), self.otel_config_filename)
            ]

            cmd = [str(v) for v in cmd_args]
            print(" ".join(cmd))

            output = "%s/opentelemetry_collector.out" % args.out
            f = open(output, "w")
            self.files.append(f)

            p = subprocess.Popen(cmd, stdout=f, stderr=f, cwd=args.tmp, preexec_fn=os.setsid)
            self.processes.append(p)
    
    def process_traces(self, args):
        if args.tmp == "":
            return

        cmd_args = ["./process"]
        cmd_args += ["%s/collector.out" % args.tmp]

        cmd = [str(v) for v in cmd_args]
        print(" ".join(cmd))
        
        output = "%s/process.out" % args.out
        f = open(output, "w")
        self.files.append(f)

        p = subprocess.Popen(cmd, stdout=f, stderr=f, cwd="../build")
        p.wait()
        


benchmarks = dict([
    ("one", Benchmark("one", "single_server", ["service1"])),
    ("two", Benchmark("two", "two", ["service1"])),
    ("four", Benchmark("four", "four", ["service1"])),
    ("ten", Benchmark("ten", "ten", ["service1"]))
])


def run(args):
    valid_tracers = [
        "none", "hindsight", "ot-noop", "ot-stdout", 
        "ot-local", "ot-jaeger", "ot-hindsight"]
    if args.tracing not in valid_tracers:
        print("Unknown tracer %s" % args.tracing)
        print("Expected one of: %s" % valid_tracers)
        exit(1)
    
    if args.benchmark not in benchmarks:
        print("Unknown benchmark %s" % args.benchmark)
        print("Expected one of: %s" % list(benchmarks.keys()))
        exit(1)

    if args.tracing == "ot-jaeger" and args.tmp == "":
        print("Cannot run ot-jaeger tracing without setting --tmp directory")
        exit(1)
    
    benchmark = benchmarks[args.benchmark]
    benchmark.load()
    benchmark.summarize(args)

    if not args.silent:
        print("Proceed with benchmark %s? Press <return> to continue or CTRL-C to abort" % args.benchmark)
        input()

    benchmark.reset_shm(args)
    benchmark.mkdirs(args)

    exit_flag = threading.Event()

    def signal_handler(sig, frame):
        print("Killing experiment processes...")
        exit_flag.set()
    signal.signal(signal.SIGINT, signal_handler)


    print("Running for %d seconds" % args.duration)
    benchmark.run_servers(args)
    benchmark.run_backends(args)
    time.sleep(5)
    benchmark.run_agents(args)
    benchmark.run_client(args)
    

    exit_flag.wait(args.duration)

    print("Stopping benchmark...")
    benchmark.get_cpu_usage(args)
    benchmark.stop(args)
    benchmark.process_traces(args)

if __name__ == '__main__':
    args = parser.parse_args()
    args.tmp = os.path.expanduser(args.tmp)
    args.out = os.path.expanduser(args.out)
    args.hindsight = os.path.expanduser(args.hindsight)
    args.ot_collector = os.path.expanduser(args.ot_collector)
    exit(run(args))
