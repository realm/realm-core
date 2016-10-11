#!/usr/bin/env python
#
# useage:
# $ ./parse-bench-hist.py [outputdir [inputdir]]
#
# this script will read any csv files from the
# input directory (default is the folder created by
# the gen-bench-hist.sh script) and output combined
# results organized by benchmark function in the
# outputdir (default is ./bench-hist-results)
# the generated csv files will show graphs of benchmark
# performance per function per build. The x axis of builds
# is ordered by the "modified" timestamp of the csv files
# from the input directory.

from stat import S_ISREG, ST_MTIME, ST_MODE
import csv, errno, os, sys, time

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

def transform(inputdir, outputdir, filelist):
    for inputfile in filelist:
        sha = os.path.splitext(os.path.basename(inputfile))[0]
        print "column sha:" + sha
        with open(inputfile) as fin:
            csvr = csv.reader(fin)
            header = csvr.next()
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
                with open(outfilename, 'w+') as fout:
                    fout.seek(0)
                    for i in range(0, 5):
                        newrow = lines[i] + row[i] + ",\n"
                        fout.write(newrow)
                    fout.truncate()

if __name__ == "__main__":
    machid = getMachId()
    outputdir = "./bench-hist-results/"
    if len(sys.argv) >= 2:
        outputdir = sys.argv[1]
    outputdir = os.path.expanduser(outputdir)
    mkdirs(outputdir)
    print "results will be written to " + outputdir

    inputdir = "~/.realm/core/benchmarks/" + str(machid)
    if len(sys.argv) >= 3:
        inputdir = sys.argv[2]
    inputdir = os.path.expanduser(inputdir)
    print "looking for csv files in " + inputdir
    files = getFilesByName(inputdir)
    
    transform(inputdir, outputdir, files)
    #for cdate, path in files:
    #    print time.ctime(cdate), os.path.basename(path)


