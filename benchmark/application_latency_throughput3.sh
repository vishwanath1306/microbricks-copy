OUTDIR=/local/hindsightexperiments/application_latency_throughput3
OPENLOOP=200,400,600,800,1000,1200,1400,1600,1800,2000,2200,2400,2600,2800,3000,3200,3400,3600,3800,4000,4200,4400,4600,4800,5000,5200,5400,5600
python3 application_overheads.py $OUTDIR --repeat 1 --openloop=$OPENLOOP --clients=5 --closedloop=
