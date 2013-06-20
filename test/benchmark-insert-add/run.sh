#!/bin/bash
PATH=/bin:/usr/bin

./add_insert -N 50000000 -n 50000      > append_table_inmem.dat
./add_insert -i -N 50000000 -n 50000   > insert_table_inmem.dat
./add_insert -s mem -N 500000 -n 5000  > append_transact_inmem.dat
./add_insert -s mem -N 500000 -n 5000  > insert_transact_inmem.dat
./add_insert -s full -N 500000 -n 5000 > append_transact_full.dat
./add_insert -s full -N 500000 -n 5000 > insert_transact_full.dat
./performance.gnuplot
