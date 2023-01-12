import json
import csv
import time
import subprocess
import signal
import argparse
import time
import numpy as np
import math
import os
import pandas as pd

parser = argparse.ArgumentParser(description='Run hindsight-grpc in multiple different configurations to measure application-level overhead.  Experiments in papers use the default arguments')
parser.add_argument("out", metavar="DIR", type=str, help="Output directory for process logs and results")
parser.add_argument("--benchmark", metavar="BENCHMARK", type=str, default="two", help="Name of the benchmark to run.  Currently supported benchmarks: one two four.  Default two")
parser.add_argument("--hindsight", metavar="PATH", type=str, default="~/hindsight", help='Path to hindsight directory. Default ~/hindsight')
parser.add_argument("--ot_collector", metavar="PATH", type=str, default="~/otel46/otelcontribcol_linux_amd64", help='Path to opentelemetry collector binary.  Default ~/otel46/otelcontribcol_linux_amd64')
parser.add_argument("--server_concurrency", metavar="NUM", type=int, default=8, help='Value to use for server --concurrency parameter.  Default 8.')
parser.add_argument("--nocompute", action='store_true', help='Set servers to -nocompute mode.  Default false')
parser.add_argument("--verbose", action='store_true', help='Print more info about experiments')
parser.add_argument("--silent", action='store_true', help="Don't ask for confirmation to run experiment")
parser.add_argument("--fromscratch", action='store_true', help='By default if results already exist for an experiment it will be skipped.  Set this flag to re-run all experiments from scratch even if there are existing results.')
parser.add_argument("--results", action='store_true', help='Run with this flag to skip actually running the experiments and just process existing results')
parser.add_argument("--tracing", metavar="TRACER", type=str, default="none,hindsight,ot-jaeger", help='Tracers to use as comma-separated list.  Default none,hindsight,ot-jaeger')
# parser.add_argument("--openloop", metavar="NUM", type=str, default="100,200,300,400,500,600,700,800,900,1000,1100,1200,1300,1400,1500,1600,1700,1800,1900,2000,2100,2200,2300,2400,2500,2600,2700,2800,2900,3000,3100,3200,3300,3400,3500,3600,3700,3800,3900,4000", help='Integers as a comma-separated list.  Will run open-loop experiments for each integer specified as the request rate.  Rates are per-client')
# parser.add_argument("--clients", metavar="NUM", type=str, default="20", help='Number of clients to run. Default 64.')
parser.add_argument("--openloop", metavar="NUM", type=str, default="133,266,400,533,666,800,933,1066,1200,1333,1466,1600,1733,1866,2000,2133,2266,2400,2533,2666,2800,2933,3066,3200,3333,3466,3600,3733,3866,4000,4133,4266,4400,4533,4666,4800,4933,5066,5200,5333", help='Integers as a comma-separated list.  Will run open-loop experiments for each integer specified as the request rate.  Rates are per-client')
parser.add_argument("--clients", metavar="NUM", type=str, default="15", help='Number of clients to run. Default 64.')
parser.add_argument("--closedloop", metavar="NUM", type=str, default="8", help='Integers as a comma-separated list.  Will run closed-loop experiments for each integer specified as the concurrency.  Default 4.  Rates are per-client')
parser.add_argument('-s', "--sampling", metavar="NUM", type=str, default="0,0.01,0.1,1", help='Sampling / trigger percentage to use.  With opentelemetry this sets head-sampling probability.  With hindsight this sets trigger percentage.  Comma-separated string of floats.  Default 0,0.01,0.1,1')
parser.add_argument('-d', "--duration", metavar="NUM", type=int, default=60, help='Benchmark duration')
parser.add_argument("--repeat", metavar="NUM", type=int, default=3, help='Number of repetitions of each experiment.  Default 3.')

cutoffs = {
  "ot-jaeger": {
    "1": 45057
  }
}

def make_cmd(args, outdir, tmpdir, tracing, requests, sampling, openloop):
  cmd_args = ["python3", "run_benchmark.py"]
  cmd_args += ["--benchmark", args.benchmark]
  cmd_args += ["--server_concurrency", args.server_concurrency]
  if args.nocompute:
    cmd_args += ["--nocompute"]
  cmd_args += ["--tracing", tracing]
  if tracing == "hindsight" or tracing == "ot-hindsight":
    cmd_args += ["--hindsight", args.hindsight]
  elif tracing.startswith("ot-"):
    cmd_args += ["--ot_collector", args.ot_collector]
  if openloop:
    cmd_args += ["--openloop"]
  cmd_args += ["-r", requests]
  cmd_args += ["-c", args.clients]
  cmd_args += ["-s", sampling]
  cmd_args += ["--silent"]
  cmd_args += ["-d", args.duration]
  cmd_args += ["--tmp", tmpdir]
  cmd_args += [outdir]

  cmd = [str(v) for v in cmd_args]
  if args.verbose:
    print(" ".join(cmd))
  return cmd

def make_cmds(args):
  openloops = [r for r in args.openloop.split(",") if r != ""]
  closedloops = [c for c in args.closedloop.split(",") if c != ""]
  tracers = [t for t in args.tracing.split(",") if t != ""]
  samplerates = [r for r in args.sampling.split(",") if r != ""]

  base_outdir = "%s/out" % args.out
  base_tmpdir = "%s/tmp" % args.out

  cmds = []

  skipped = dict()

  # Do closed loops first
  for requests in closedloops:
    concurrency = int(requests) * int(args.clients)
    for tracer in tracers:
      if tracer == "none":
        for i in range(1, args.repeat+1):
          if i == 1:
            experiment_name = "closedloop-%d-notracing" % (concurrency, )
          else:
            experiment_name = "closedloop-%d-notracing-%d" % (concurrency, i)
          outdir = "%s/%s" % (base_outdir, experiment_name)
          # tmpdir = "%s/%s" % (base_tmpdir, experiment_name)
          tmpdir = base_tmpdir
          cmd = make_cmd(args, outdir, tmpdir, tracer, requests, "0", False)
          data = {
            "name": experiment_name,
            "outdir": outdir,
            "tmpdir": tmpdir,
            "workload": "closedloop",
            "tracer": tracer,
            "requests": concurrency,
            "samplerate": 0,
            "cmd": cmd,
            "exists": os.path.isdir(outdir),
            "repeat": i
          }
          cmds.append(data)
      else:
        for samplerate in samplerates:
          for i in range(1, args.repeat+1):
            if i == 1:
              experiment_name = "closedloop-%d-%s-%s" % (concurrency, tracer, samplerate)
            else:
              experiment_name = "closedloop-%d-%s-%s-%d" % (concurrency, tracer, samplerate, i)
            outdir = "%s/%s" % (base_outdir, experiment_name)
            # tmpdir = "%s/%s" % (base_tmpdir, experiment_name)
            tmpdir = base_tmpdir
            cmd = make_cmd(args, outdir, tmpdir, tracer, requests, samplerate, False)
            data = {
              "name": experiment_name,
              "outdir": outdir,
              "tmpdir": tmpdir,
              "workload": "closedloop",
              "tracer": tracer,
              "requests": concurrency,
              "samplerate": samplerate,
              "cmd": cmd,
              "exists": os.path.isdir(outdir),
              "repeat": i
            }
            cmds.append(data)

  # Then do openloops
  for requests in openloops:
    rate = int(requests) * int(args.clients)
    for tracer in tracers:
      if tracer == "none":
        for i in range(1, args.repeat+1):
          if i == 1:
            experiment_name = "openloop-%d-notracing" % (rate, )
          else:
            experiment_name = "openloop-%d-notracing-%d" % (rate, i)
          outdir = "%s/%s" % (base_outdir, experiment_name)
          # tmpdir = "%s/%s" % (base_tmpdir, experiment_name)
          tmpdir = base_tmpdir
          cmd = make_cmd(args, outdir, tmpdir, tracer, requests, "0", True)
          data = {
            "name": experiment_name,
            "outdir": outdir,
            "tmpdir": tmpdir,
            "workload": "openloop",
            "tracer": tracer,
            "requests": rate,
            "samplerate": 0,
            "cmd": cmd,
            "exists": os.path.isdir(outdir),
            "repeat": i
          }
          cmds.append(data)
      else:
        for samplerate in samplerates:
          if tracer in cutoffs and samplerate in cutoffs[tracer]:
            cutoff = cutoffs[tracer][samplerate]
            if rate > cutoff:
              if (tracer, samplerate) not in skipped:
                skipped[(tracer, samplerate)] = 1
              else:
                skipped[(tracer, samplerate)] += 1
              continue
            
          for i in range(1, args.repeat+1):
            if i == 1:
              experiment_name = "openloop-%d-%s-%s" % (rate, tracer, samplerate)
            else:
              experiment_name = "openloop-%d-%s-%s-%d" % (rate, tracer, samplerate, i)
            outdir = "%s/%s" % (base_outdir, experiment_name)
            # tmpdir = "%s/%s" % (base_tmpdir, experiment_name)
            tmpdir = base_tmpdir
            cmd = make_cmd(args, outdir, tmpdir, tracer, requests, samplerate, True)
            data = {
              "name": experiment_name,
              "outdir": outdir,
              "tmpdir": tmpdir,
              "workload": "openloop",
              "tracer": tracer,
              "requests": rate,
              "samplerate": samplerate,
              "cmd": cmd,
              "exists": os.path.isdir(outdir),
              "repeat": i
            }
            cmds.append(data)
  
  if not args.results:
    for (tracer, samplerate), count in skipped.items():
      print("Skipping %d experiments for %s-%s (cutoff at %d r/s)" % (count, tracer, samplerate, cutoffs[tracer][samplerate]))
  
  return cmds

def run_experiments(args):

  cmds = make_cmds(args)
  to_run = []
  to_skip = []
  if args.results:
    to_skip = cmds
  elif args.fromscratch:
    to_run = cmds
  else:
    to_run = [c for c in cmds if not c["exists"]]
    to_skip = [c for c in cmds if c["exists"]]

  if len(to_run) > 0:
    if len(to_skip) > 0:
      print("Skipping %d experiments with existing results" % len(to_skip))
    print("Run %d experiments?  Total duration %ds." % (len(to_run), len(to_run) * (10+args.duration)))
    if not args.silent:
      print("Press <return> to continue or CTRL-C to abort")
      input()
    else:
      print("Running (--silent)")

  if not os.path.isdir(args.out):
    os.makedirs(args.out)
  f = open("%s/summary2.csv" % args.out, "w")
  writer = csv.writer(f)

  headers = ["name", "workload", "repeat", "tracer", "requests", "samplerate", "total_requests", "throughput", "throughput2", "latency", "max_latency", "min_latency"]
  writer.writerow(headers)

  ran = 0
  processed = 0
  skipped = 0
  try :
    # Process existing results first
    for data in to_skip:
      if get_results(args, data):
        row = [data[h] if h in data else "" for h in headers]
        writer.writerow(row)
        f.flush()
        processed += 1
      else:
        skipped += 1
    
    # Run any experiments
    for i, data in enumerate(to_run):
      cmd = data["cmd"]
      print("Experiment %d/%d" % (i+1, len(to_run)))
      print(" ".join(cmd))
      p = subprocess.Popen(cmd)
      p.wait()
      time.sleep(2)
      ran += 1

      if get_results(args, data):
        row = [data[h] if h in data else "" for h in headers]
        writer.writerow(row)
        f.flush()
        processed += 1
      else:
        skipped += 1
  finally:
    f.close()
  
  print("Ran %d experiments" % ran)
  print("Processed %d experiment results (skipped %d missing or invalid)" % (processed, skipped))



def findline(lines, prefix):
  ls = [l for l in lines if l.startswith(prefix)]
  if len(ls) == 0:
    return None
  else:
    line = ls[-1]
    return line[len(prefix):].strip()

def get_results(args, data):
  try:
    client_output = "%s/client.out" % data["outdir"]
    with open(client_output, "r") as f:
      lines = f.readlines()

    req_line = findline(lines, "Total requests: ")
    if req_line is not None:
      data["total_requests"] = int(req_line)

    tput_line = findline(lines, "overall throughput: ")
    if tput_line is not None:
      data["throughput"] = float(tput_line.split(" ")[0])  

    lcy_line = findline(lines, "Average / Max / Min latency of a request is: ")
    if lcy_line is not None:
      splits = lcy_line.split(" ")[0].split("/")
      data["latency"] = float(splits[0])
      data["max_latency"] = float(splits[1])
      data["min_latency"] = float(splits[2])
    
    tput_lines = [l for l in lines if l.strip().endswith(" total)")]
    tputs = [int(l.split(" ")[0]) for l in tput_lines]
    tputs = tputs[-12:-2]
    if len(tputs) > 0:
      data["throughput2"] = float(sum(tputs)) / float(len(tputs))
    
    return True
  except Exception as e:
    if args.verbose:
      print(e)
    return False


# def process_output(outdir):
#     df = None
#     for thread in threads:
#         for payload_size in payload_sizes:
#             for buffer_size in buffer_sizes:
#                 filename = "%s/%dthreads_%dpayload_%dbufsize.out" % (outdir, thread, payload_size, buffer_size)
#                 with open(filename, "r") as f:
#                     lines = f.readlines()
#                 headerline = [l.strip() for l in lines if l.startswith("headers:")][0]
#                 datalines = [l.strip() for l in lines if l.startswith("data:")]
#                 datalines = datalines[int(len(datalines)/2):len(datalines)-1]
#                 headers = headerline.split("\t")[1:]
#                 data = [l.split("\t")[1:] for l in datalines]
#                 rows = [dict(zip(headers, d)) for d in data]
#                 for row in rows:
#                     row["thread"] = thread
#                     row["payload_size"] = payload_size
#                     row["buffer_size"] = buffer_size
#                 if df is None:
#                     df = pd.DataFrame(rows)
#                 else:
#                     df = df.append(rows)
#     df = df.apply(pd.to_numeric)
#     return df




if __name__ == '__main__':
    args = parser.parse_args()
    run_experiments(args)
    # df = process_output(args.outdir)
    # df["total_released"] = df["null_released"]+df["pool_released"]
    # df["released_bytes"] = df["total_released"] * df["buffer_size"]
    # df["goodput_bufs"] = df["pool_released"] * (df["traces"] - df["invalidtraces"]) / df["traces"]
    # df["goodput_bytes"] = df["released_bytes"] * (df["traces"] - df["invalidtraces"]) / df["traces"]
    # means = df.groupby(["thread", "payload_size", "buffer_size"])[["traces", "invalidtraces", "tracepoints", "bytes", "null_released", "pool_released", "total_released", "released_bytes", "goodput_bufs", "goodput_bytes"]].mean()


    # # means = df.groupby("thread")[["begin", "tracepoint", "end"]].mean()
    # means.to_csv("scatter2.out")