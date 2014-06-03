#!/bin/bash

cmd='./oltpbenchmark --b ycsb -c config/pg_ycsb_config.xml --create=true --load=true --execute=true '

RUNS=( 1 10 100 1000 10000)


echo "running"
for i in "${RUNS[@]}"
do
   runcmd="$cmd -nt $i -o nt$i"
   echo $runcmd
   $runcmd
done

