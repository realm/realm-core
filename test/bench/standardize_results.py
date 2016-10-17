#!/usr/bin/env python
#
# useage:
# $ ./standardize_results.py path/to/unixtimestamp_file.csv
#
# this script will read the benchmark results from the
# specified csv file and transform the file to a
# standardized format. This is necessary because
# previous formats did not include all data and the
# ordering of columns has switched at different points.
# For more details, check the git history of the
# /test/util/benchmark_results.cpp file for how the
# output format has changed over time.

import csv, errno, os, sys, time

def transform_min_max_avg(infile):

def transform_avg_min_max(infile):

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print 'This python script expects exactly one argument, the name of the file to transform'
        return
    infile = sys.argv[1]
    infile = os.path.expanduser(infile)
    if !os.path.isfile(infile):
        print 'Cannot open file: ' + infile
        return
    fname = os.path.basename(infile)
    if len(fname) == 0 || len(fname.split("_")) != 2:
        print 'This python script expects the input file to be of the form timestamp_sha1.csv'
        return
    timestamp = fname.split("_")[0]
    numeric_timestamp = 0
    try:
        numeric_timestamp = int(timestamp)
    except ValueError:
        print 'The unix timestamp is not in the correct format for file: ' + infile
        return
    if numeric_timestamp < 1431419520:
        # before commit 1565bb5cd1fdf2a192c1075d440f8092f75b69b4
        # output was of format min,max,avg
        transform_min_max_avg(infile)
    elif numeric_timestamp < 1444388520:
        # before commit dffe372fe9fdf19b82002e25eafe6c7a072a34f9
        # output was of format avg,min,max
        transform_avg_min_max(infile)
    else:
        # everything afterwards is min,max,median,avg
        # noop

