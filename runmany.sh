#!/bin/bash

cmd='./oltpbenchmark --b ycsb -c config/pg_ycsb_config.xml --create=true --load=true --execute=true '

RUNS=( 1 10 100 1000 5000 10000 20000 50000 100000 200000 400000 1000000)


echo "running"
for i in "${RUNS[@]}"
do
   runcmd="$cmd -nt $i -o nt$i "
   echo $runcmd
   $runcmd &> nt$i.log
done

