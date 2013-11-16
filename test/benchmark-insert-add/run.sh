#!/bin/bash
PATH=/bin:/usr/bin
N=5000
n=10

./add_insert         -N $(($N*100)) -n $(($n*100)) > append_table_inmem.dat
./add_insert -i      -N $(($N*100)) -n $(($n*100)) > insert_table_inmem.dat
./add_insert -s mem  -N $N         -n $n         > append_transact_inmem.dat
./add_insert -i -s mem  -N $N      -n $n         > insert_transact_inmem.dat
./add_insert -g      -N $N         -n $n         > append_group.dat
./add_insert -g -i   -N $N         -n $n         > insert_group.dat
./add_insert -s full -N $N         -n $n         > append_transact_full.dat
./add_insert -i -s full -N $N      -n $n         > insert_transact_full.dat
./add_insert -s async -N $(($N*10)) -n $(($n*10)) > append_transact_async.dat
./add_insert -i -s async -N $(($N*10)) -n $(($n*10)) > insert_transact_async.dat
./performance.gnuplot
