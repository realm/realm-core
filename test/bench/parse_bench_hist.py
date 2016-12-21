#!/usr/bin/env python
#
# This script can produce csv files locally or send
# results to a remote influx 1.0.2 database.
#
# Local useage:
# $ ./parse_bench_hist.py --local [outputdir [inputdir]]
# $ ./parse_bench_hist.py --local-html [outputdir [inputdir]]
#
# --local-html is the same as --local but it also produces a
# results.html page with graphs of the output.
#
# This script will read any csv files from the
# input directory (default is the folder created by
# the gen_bench_hist.sh script) and output combined
# results organized by benchmark function in the
# outputdir (default is ./bench-hist-results)
# the generated csv files will show graphs of benchmark
# performance per function per build. The x axis of builds
# is ordered by the "modified" timestamp of the csv files
# from the input directory.
#
# The input directory relies on a machine id which can be
# specified through the environment variable REALM_BENCH_MACHID
# If not provided an attempt will be made to automatically discover
# a unique hardware based id. The benchmark tools version is also
# required as the first line in the `benchmark_version` file.
#
# remote useage:
# $ ./parse_bench_hist.py --remote ip_address [inputdir [inputfile]]
#
# This script will read the csv benchmark results and send them
# to a remote influx database at ip_address. If no inputdir is
# specified then all csv files from the gen_bench_hist.sh script
# are parsed. If only an input directory is specified then all
# csv files from this directory are parsed. If both the input
# directory and the inputfile are specified, then only this
# single csv file is parsed.
#
# Results will be curled to the specified ip address in the format:
# function,machine=${machid} min=x,max=y,median=z,avg=w,sha=${gitsha} unixtimestamp

from stat import S_ISREG, ST_MTIME, ST_MODE
from report_generator import generateReport
import csv, errno, os, subprocess, sys, time

def printUseageAndQuit():
    print ("This python script can produce local csv files or "
          "send benchmark stats to a remote influx database.")
    print "Useage:"
    print "./parse_bench_hist.py --local [outputdir [inputdir]]"
    print "./parse_bench_hist.py --local-html [outputdir [inputdir]]"
    print "./parse_bench_hist.py --remote ip_address [inputdir [inputfile]]"
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
    try:
        machid = os.environ['REALM_BENCH_MACHID']
    except KeyError:
        if os.path.isfile("/var/lib/dbus/machine-id"):
            with open("/var/lib/dbus/machine-id") as f:
                machid = f.readline().strip()
        elif os.path.isfile("/etc/machine-id"):
            with open("/etc/machine-id") as f:
                machid = f.readline().strip()
        elif os.path.isfile("/etc/hostname"):
            with open("/etc/hostname") as f:
                machid = f.readline().strip()
        else:
            machid = os.popen('ifconfig en0 | awk \'/ether/{print $2}\'').read().strip()
    if not machid.strip():
        machid = "unknown"
    return machid

# The version of these benchmark scripts is set in the file
# "benchmark_version" see that file for more details.
def getBenchmarkVersion():
    benchmark_version = "unknown"
    with open('benchmark_version', 'r') as f:
        benchmark_version = f.readline().strip()
    return benchmark_version

def find_ndx(inlist, item):
    ndx = 0
    try:
        ndx = inlist.index(item)
    except ValueError:
        ndx = -1
    return ndx

def getReadableSha(verboseSha):
    process = subprocess.Popen(["git", "describe", verboseSha], stdout=subprocess.PIPE)
    output = process.communicate()[0]
    output = output.replace('\n', '')
    if not output:
        return verboseSha
    return output

def transform(inputdir, destination, filelist, handler):
    for inputfile in filelist:
        file_name = os.path.splitext(os.path.basename(inputfile))[0].split("_")
        if len(file_name) != 2:
            print "Expecting input files of format 'timestamp_sha.csv'"
            exit()
        timestamp = file_name[0]
        sha = file_name[1]
        tag = getReadableSha(sha)
        print "column sha:" + sha
        with open(inputfile) as fin:
            csvr = csv.reader(fin)
            header = {}
            try:
                header = csvr.next()
            except StopIteration:
               print "skipping empty file: " + str(inputfile)
               continue;
            min_ndx = find_ndx(header, "min")
            max_ndx = find_ndx(header, "max")
            med_ndx = find_ndx(header, "median")
            avg_ndx = find_ndx(header, "avg")
            for row in csvr:
                if len(row) < 5:
                    break
                benchmark = row[0]
                row_min = row[min_ndx] if min_ndx >= 0 else ""
                row_max = row[max_ndx] if max_ndx >= 0 else ""
                row_med = row[med_ndx] if med_ndx >= 0 else ""
                row_avg = row[avg_ndx] if avg_ndx >= 0 else ""
                info = { 'min':row_min, 'max':row_max, 'med':row_med, 'avg':row_avg,
                         'function':benchmark, 'sha':sha, 'time':timestamp, 'dest':destination,
                         'tag': tag}
                handler(info)

# remove existing files (old results) but only the
# first time since we need to open and append to files
# this allows us to run the script multiple times in the
# same output directory
existing = []
def refresh_file_once(filename):
    if filename not in existing:
        try:
            os.remove(filename)
        except:
            pass
    existing.append(filename)

# format is: sha, tag, min, max, med, avg,
#            sha1, ...
#            ..., ...
def handle_local_vertical(info):
    outfilename = info['dest'] + info['function'] + ".csv"
    refresh_file_once(outfilename)
    endline = ',\n'
    header = ''
    keys = ['sha', 'tag', 'min', 'max', 'med', 'avg']
    if not os.path.exists(outfilename):
        header = ','.join(map(str, keys)) + endline
    with open(outfilename, 'a') as fout:
        fout.write(header)
        data = ','.join([info[e] for e in keys]) + endline
        fout.write(data)

# format is sha, sha1,sha2,sha3...
#           min, ...
#           max, ...
#           med, ...
#           avg, ...
def handle_local_horizontal(info):
    outfilename = info['dest'] + info['function'] + ".csv"
    refresh_file_once(outfilename)
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
	keys = ['sha', 'min', 'max', 'med', 'avg']
	for idx in xrange(len(keys)):
		newrow = lines[idx] + info[keys[idx]] + endline
		fout.write(newrow)

        fout.truncate()

def handle_remote(info):
    info['mach'] = getMachId()
    info['nanotime'] = str(info['time']) + "000000000"
    #function,machine="${machid}" min=x,max=y,median=z,avg=w,sha="${gitsha}" unix_timestamp_in_nanoseconds
    #note that string types must be quoted
    payload = "%(function)s,machine=\"%(mach)s\" min=%(min)s,max=%(max)s,med=%(med)s,avg=%(avg)s,sha=\"%(sha)s\" %(nanotime)s" % info
    #curl -i -XPOST 'remote_ip' --data-binary 'payload'
    subprocess.call(['curl', '-i', '-XPOST', info['dest'], '--data-binary', payload])

def transform_local(html=False):
    machid = getMachId()
    version = getBenchmarkVersion()
    outputdir = "./bench-hist-results/"
    if len(sys.argv) >= 3:
        outputdir = sys.argv[2]
    outputdir = os.path.expanduser(outputdir)
    mkdirs(outputdir)
    print "results will be written to " + outputdir

    inputdir = "~/.realm/core/benchmarks/"
    if len(sys.argv) >= 4:
        inputdir = sys.argv[3]
    if len(sys.argv) > 4:
        print "Unexpected extra arguments."
        printUseageAndExit()
    inputdir = os.path.expanduser(inputdir) + str(version) + "/" + str(machid)
    print "looking for csv files in " + inputdir
    files = getFilesByName(inputdir)

    transform(inputdir, outputdir, files, handle_local_vertical)

    if html is True:
        generateReport(outputdir, getFilesByName(outputdir))

def transform_remote():
    machid = getMachId()
    version = getBenchmarkVersion()
    if len(sys.argv) <= 2:
        print "Must specify the remote ip address of the influx database."
        printUseageAndQuit()
    remoteip = sys.argv[2]
    inputdir = "~/.realm/core/benchmarks/" + str(version) + "/" + str(machid)
    if len(sys.argv) > 3:
        inputdir = sys.argv[3]
    inputdir = os.path.expanduser(inputdir)
    files = []
    if len(sys.argv) > 4:
        files = [ sys.argv[4] ]
    else:
        files = getFilesByName(inputdir)
    if len(sys.argv) > 5:
        print "Unexpected extra arguments."
        printUseageAndQuit()
    transform(inputdir, remoteip, files, handle_remote)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "Missing arguments."
        printUseageAndQuit()
    locality = sys.argv[1]
    if locality == "--local":
        transform_local()
    elif locality == "--local-html":
        transform_local(html=True)
    elif locality == "--remote":
        transform_remote()
    else:
        print "Expecting either '--local', '--local-html', or '--remote' as the second argument."
        printUseageAndQuit()

