OUTDIR=/local/hindsightexperiments/agentcpu
python3 run_benchmark.py --tmp $OUTDIR/tmp --tracing hindsight -c 8 -r 32 -s 0.01 -d 60 $OUTDIR/out