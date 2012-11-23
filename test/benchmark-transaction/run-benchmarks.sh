#!/bin/bash
#
# run transaction benchmarks
#
# (C) Copyright 2012 by TightDB, Inc. <http://www.tightdb.com/>
#
PATH=/bin:/usr/bin

Nrec=100000 # number of records
Nwrit=1000  # number of write transactions (per write thread)
Nread=1000  # number of read transactions (per read thread)

function bench {
    db=$1

    out=${db}-tps.dat
    rm -f $out
    echo "# Database: ${db}"  >> $out
    echo "# Nwriters: $Nwrit" >> $out
    echo "# Nreaders: $Nread" >> $out
    echo "# Nrecords: $Nrec"  >> $out
    echo "# Readers Writers TPS(Reader) TPS(Writer)" >> $out
    for i in $(seq 0 10)
    do 
        for j in $(seq 0 10)
        do
            echo -n "$j $i " >> $out
            rm -f test.${db}*
            ./transact -w $i -r $j -f test.${db} -W $Nwrit -R $Nread -n $Nrec -d ${db} >> $out
        done
    done
}

bench "tdb"
bench "sqlite"
