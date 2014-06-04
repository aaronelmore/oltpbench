#!/bin/bash

ecmd='./oltpbenchmark --b ycsb -c config/pg_ycsb_config.xml --execute=true '
ccmd='./oltpbenchmark --b ycsb -c config/pg_ycsb_config.xml --create=true --load=true '

RUNS=( 1 10 100 1000 5000 10000 20000 50000 100000 200000 400000 1000000)

echo "running"
for i in "${RUNS[@]}"
do
   runcmd="$ccmd  -o md$i"
   echo $runcmd
   $runcmd
   runcmd="python tools/databases/multi.py -n $i"
   $runcmd
   runcmd="$ecmd  -o md$i"
   $runcmd &> md$i.log
   runcmd="python tools/databases/multi.py -n 0 -c"
   $runcmd
done

