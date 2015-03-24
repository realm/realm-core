#!/bin/bash
#
# run transaction benchmarks
#
# (C) Copyright 2012 by Realm, Inc. <http://www.realm.com/>
#
PATH=/bin:/usr/bin

Nrec=100000 # number of records
duration=120 # run time in secords
Nmax=10 # max number of threads

function bench {
    db=$1

    out=${db}-tps.dat
    rm -f $out
    echo "# Database: ${db}"  >> $out
    echo "# Duration: ${duration}" >> $out
    echo "# Readers Writers TPS(Reader) TPS(Writer) TPS(total)" >> $out
    for i in $(seq 0 $Nmax)
    do 
        for j in $(seq 0 $Nmax)
        do
            echo -n "$j $i " >> $out
            rm -f test_${db}*
            ./transact -w $i -r $j -f test_${db} -d ${db} -s -n $Nrec -t $duration >> $out
            rm -f test_${db}*
        done
    done
}

bench "realm"
bench "sqlite"
bench "mysql"
bench "sqlite-wal"
