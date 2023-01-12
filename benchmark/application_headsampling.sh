OUTDIR=/local/hindsightexperiments/application_headsampling
python3 application_overheads.py --nocompute $OUTDIR --repeat 5 --openloop= --tracing=ot-jaeger --sampling=0,0.00125,0.0025,0.005,0.01,0.025,0.05,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0
python3 application_overheads.py --nocompute $OUTDIR --repeat 5 --silent --openloop= --tracing=hindsight,none --sampling=0
python3 application_overheads.py --nocompute $OUTDIR --repeat 5 --silent --openloop= --tracing=hindsight,none,ot-jaeger --sampling=0,0.00125,0.0025,0.005,0.01,0.025,0.05,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0 --results 