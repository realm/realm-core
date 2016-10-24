#!/usr/bin/env python
#
# This script can produce csv files locally or send
# results to a remote influx 1.0.2 database.
#
# Local useage:
# $ ./parse-bench-hist.py --local [outputdir [inputdir]]
#
# This script will read any csv files from the
# input directory (default is the folder created by
# the gen-bench-hist.sh script) and output combined
# results organized by benchmark function in the
# outputdir (default is ./bench-hist-results)
# the generated csv files will show graphs of benchmark
# performance per function per build. The x axis of builds
# is ordered by the "modified" timestamp of the csv files
# from the input directory.
#
# remote useage:
# $ ./parse-bench-hist.py --remote ip_address [inputdir [inputfile]]
#
# This script will read the csv benchmark results and send them
# to a remote influx database at ip_address. If no inputdir is
# specified then all csv files from the gen-bench-hist.sh script
# are parsed. If only an input directory is specified then all
# csv files from this directory are parsed. If both the input
# directory and the inputfile are specified, then only this
# single csv file is parsed.
#
# Results will be curled to the specified ip address in the format:
# function,machine=${machid} min=x,max=y,median=z,avg=w,sha=${gitsha} unixtimestamp

from stat import S_ISREG, ST_MTIME, ST_MODE
import csv, errno, os, sys, time

def printUseageAndQuit():
    print ("This python script can produce local csv files or "
          "send benchmark stats to a remote influx database.")
    print "Useage:"
    print "./parse-bench-hist.py --local [outputdir [inputdir]]"
    print "./parse-bench-hist.py --remote ip_address [inputdir [inputfile]]"
    exit()

def getFilesByModDate(dirpath, suffix='.csv'):
    # get all entries in the directory w/ stats
    entries = (os.path.join(dirpath, fn) for fn in os.listdir(dirpath) if fn.endswith(suffix))
    entries = ((os.stat(path), path) for path in entries)
    # leave only regular files, insert creation date
    # uses `ST_MTIME` to sort by a modification date
    entries = ((stat[ST_MTIME], path)
           for stat, path in entries if S_ISREG(stat[ST_MODE]))
    return sorted(entries)

def getFilesByName(dirpath, suffix='.csv'):
    entries = (os.path.join(dirpath, fn) for fn in os.listdir(dirpath) if fn.endswith(suffix))
    return sorted(entries)

def mkdirs(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

def getMachId():
    machid = "unknown"
    if os.path.isfile("/var/lib/dbus/machine-id"):
        with open("/var/lib/dbus/machine-id") as f:
            machid = f.readline().strip()
    elif os.path.isfile("/etc/machine-id"):
        with open("/etc/machine-id") as f:
            machid = f.readline().strip()
    else:
        machid = os.popen('ifconfig en0 | awk \'/ether/{print $2}\'').read().strip()
    return machid

def find_ndx(inlist, item):
    ndx = 0
    try:
        ndx = inlist.index(item)
    except ValueError:
        ndx = -1
    return ndx

def transform(inputdir, outputdir, filelist):
    for inputfile in filelist:
        sha = os.path.splitext(os.path.basename(inputfile))[0]
        print "column sha:" + sha
        with open(inputfile) as fin:
            csvr = csv.reader(fin)
            header = csvr.next()
            min_ndx = find_ndx(header, "min")
            max_ndx = find_ndx(header, "max")
            med_ndx = find_ndx(header, "median")
            avg_ndx = find_ndx(header, "avg")
            print "min at: " + str(min_ndx) + " max at: " + str(max_ndx) + " median at" + str(med_ndx) + " avg at: " + str(avg_ndx)
            for row in csvr:
                if len(row) < 5:
                    break
                benchmark = row[0]
                outfilename = outputdir + benchmark + ".csv"
                row[0] = sha
                # make the file if not exist and read contents
                lines = ['','','','','']
                if not os.path.exists(outfilename):
                    open(outfilename, 'w+').close()
                with open(outfilename, 'r') as fout:
                    fout.seek(0)
                    lines = [line.rstrip('\n') for line in fout]
                    if len(lines) < 5:
                        lines = ['','','','','']
                endline = ",\n"
                with open(outfilename, 'w+') as fout:
                    fout.seek(0)
                    newrow = lines[0] + sha + endline
                    fout.write(newrow)
                    newrow = lines[1] + row[min_ndx] + endline if min_ndx >= 0 else lines[1] + endline
                    fout.write(newrow)
                    newrow = lines[2] + row[max_ndx] + endline if max_ndx >= 0 else lines[2] + endline
                    fout.write(newrow)
                    newrow = lines[3] + row[med_ndx] + endline if med_ndx >= 0 else lines[3] + endline
                    fout.write(newrow)
                    newrow = lines[4] + row[avg_ndx] + endline if avg_ndx >= 0 else lines[4] + endline
                    fout.write(newrow)

                    fout.truncate()

def transform_local():
    machid = getMachId()
    outputdir = "./bench-hist-results/"
    if len(sys.argv) >= 3:
        outputdir = sys.argv[2]
    outputdir = os.path.expanduser(outputdir)
    mkdirs(outputdir)
    print "results will be written to " + outputdir

    inputdir = "~/.realm/core/benchmarks/" + str(machid)
    if len(sys.argv) >= 4:
        inputdir = sys.argv[3]
    inputdir = os.path.expanduser(inputdir)
    print "looking for csv files in " + inputdir
    files = getFilesByName(inputdir)

    transform(inputdir, outputdir, files)

def transform_remote():
    machid = getMachId()
    if len(sys.argv) <= 2:
        print "Must specify the remote ip address of the influx database."
        printUseageAndQuit()
    remoteip = sys.argv[2]
    inputdir = "~/.realm/core/benchmarks/" + str(machid)
    if len(sys.argv) > 3:
        inputdir = sys.argv[3]
    inputfile = ""
    if len(sys.argv) > 4:
        inputfile = sys.argv[4]

#function,machine=${machid} min=x,max=y,median=z,avg=w,sha=${gitsha} unixtimestamp

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "Missing arguments."
        printUseageAndQuit()
    locality = sys.argv[1]
    if locality == "--local":
        transform_local()
    elif locality == "--remote":
        transform_remote()
    else:
        print "Expecting either '--local' or '--remote' as the second argument."
        printUseageAndQuit()

