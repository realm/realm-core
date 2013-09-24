#!/usr/bin/gnuplot
set datafile separator ";"
set terminal pdfcairo color enhanced
set output "performance.pdf"

# ./add_insert -N 50000000 -n 50000 | tee append_table_inmem.dat
set title "In-memory table append"
set xlabel "Number of rows"
set ylabel "Rows/sec"
plot "append_table_inmem.dat" using 1:3 with lines

# ./add_insert -i -N 50000000 -n 50000 | tee insert_table_inmem.dat
set title "In-memory table insert"
set xlabel "Number of rows"
set ylabel "Rows/sec"
plot "insert_table_inmem.dat" using 1:3 with lines

# ./add_insert -s mem -N 50000 -n 500 | tee append_transact_inmem.dat
set title "In-memory transaction append"
set xlabel "Number of rows"
set ylabel "Rows/sec"
plot "append_transact_inmem.dat" using 1:3 with lines

# ./add_insert -s mem -N 50000 -n 500 | tee insert_transact_inmem.dat
set title "In-memory transaction insert"
set xlabel "Number of rows"
set ylabel "Rows/sec"
plot "insert_transact_inmem.dat" using 1:3 with lines

# ./add_insert -g -N 50000 -n 500 | tee append_group_dat
set title "To-disk group append"
set xlabel "Number of rows"
set ylabel "Rows/sec"
plot "append_group.dat" using 1:3 with lines

# ./add_insert -g -i -N 50000 -n 500 | tee insert_group.dat
set title "To-disk group insert"
set xlabel "Number of rows"
set ylabel "Rows/sec"
plot "insert_group.dat" using 1:3 with lines

# ./add_insert -s full -N 50000 -n 500 | tee append_transact_full.dat
set title "To-disk sync transaction append"
set xlabel "Number of rows"
set ylabel "Rows/sec"
plot "append_transact_full.dat" using 1:3 with lines

# ./add_insert -s full -N 50000 -n 500 | tee insert_transact_full.dat
set title "To-disk sync transaction insert"
set xlabel "Number of rows"
set ylabel "Rows/sec"
plot "insert_transact_full.dat" using 1:3 with lines

# ./add_insert -s async -N 50000 -n 500 | tee append_transact_async.dat
set title "To-disk async transaction append"
set xlabel "Number of rows"
set ylabel "Rows/sec"
plot "append_transact_async.dat" using 1:3 with lines

# ./add_insert -s async -N 50000 -n 500 | tee insert_transact_async.dat
set title "To-disk async transaction insert"
set xlabel "Number of rows"
set ylabel "Rows/sec"
plot "insert_transact_async.dat" using 1:3 with lines
