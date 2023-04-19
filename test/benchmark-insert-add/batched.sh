#!/bin/bash
PATH=/bin:/usr/bin
N=1000000
n=50000
m=5

p=1
for i in $(seq 1 $m); do
    ./add_insert -s full -N $N -n $n > append_transact_full_${p}.dat
    p=$((10*$p))
done

tmpfile=$(mktemp /tmp/$0.XXXXXX)
echo "set datafile separator \";\"" >> $tmpfile
echo "set terminal pdfcairo color enhanced" >> $tmpfile
echo "set output \"batched.pdf\"" >> $tmpfile
echo "set title \"Batched transactions to-disk\"" >> $tmpfile
echo "set xlabel \"Number of rows\"" >> $tmpfile
echo "set ylabel \"Rows/sec\"" >> $tmpfile
echo -n "plot " >> $tmpfile
p=1
for i in $(seq 1 $m); do
    echo -n "\"append_transact_full_${p}.dat\" u 1:3 t \"$p\" w l " >> $tmpfile
    if [ "$i" -ne "$m" ]; then
        echo -n "," >> $tmpfile
    fi
    p=$((10*$p))
done
gnuplot $tmpfile
rm -f $tmpfile
