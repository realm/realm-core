import os
import sys
import getopt

def get_zero_bytes_of_file(filename):
    count = 0
    print "entering::"
    with open(filename, "rb") as f:
        byte = f.read(1)
        while byte:
            if byte == '\0':
                count = count + 1
            byte = f.read(1)
    return count

def get_file_size(filename):
    statinfo = os.stat(filename)
    return statinfo.st_size


def get_library_size(rootBuildDir):
    # librealm.a is the same binary name on mac/linux
    return get_file_size(os.path.join(rootBuildDir, "src/realm/librealm.a"))


def get_loc(directory, recursive=True):
    loc = 0
    for root, dirs, files in os.walk(directory, followlinks=False):
        if (recursive is False and root != directory):
            continue
        for filename in files:
            if (filename.endswith('.hpp') or filename.endswith('.cpp')):
                fullpath = os.path.join(root, filename)
                with open(fullpath, 'r') as f:
                    # use strip to exclude empty lines
                    loc += len(list(filter(lambda x: x.strip(), f)))
    return loc


def get_lines_of_code(rootSourceDir):
    return get_loc(os.path.join(rootSourceDir, "src"))


def get_lines_of_test_code(rootSourceDir):
    loc = get_loc(os.path.join(rootSourceDir, "test"), recursive=False)
    loc += get_loc(os.path.join(rootSourceDir, "test/util"), recursive=False)
    return loc


def get_binary_sizes(rootBuildDir):
    pathPrefix = os.path.join(rootBuildDir, "test/benchmark-common-tasks/")
    results = []
    sizes = [0, 1000, 2000, 4000, 8000, 10000]
    for size in sizes:
        name = str(size) + "bytes.realm"
        os.system(pathPrefix + "realm-stats store_binary " + name + " " + str(size))
        results.append((str(size) + " byte blob", get_file_size(name)))
    return results


def get_transaction_sizes(rootBuildDir):
    pathPrefix = os.path.join(rootBuildDir, "test/benchmark-common-tasks/")
    results = []
    # stores (num_transactions, num_rows)
    tests = [(100, 10)]
    for test in tests:
        name = str(test[0]) + "transactions_" + str(test[1]) + "rows.realm"
        os.system(pathPrefix + "realm-stats make_transactions " + name
                  + " " + str(test[0]) + " " + str(test[1]))
        results.append((name, get_file_size(name)))
        results.append((name + "empty space", get_zero_bytes_of_file(name)))
    return results


def format(description, values):
    return str(description) + ":" + str(values) + "\n"


def do_collect_stats(rootBuildDir, rootSourceDir):
    library_sizes = [("librealm.a", get_library_size(rootBuildDir))]
    binary_sizes = get_binary_sizes(rootBuildDir)
    transaction_sizes = get_transaction_sizes(rootBuildDir)
    loc = [("Realm Code", get_lines_of_code(rootSourceDir)),
           ("Test Code", get_lines_of_test_code(rootSourceDir))]
    stats = [
        ("Realm Library Size", library_sizes),
        ("Size of a Realm File Containing Blobs", binary_sizes),
        ("Size of a Realm File With Many Transactions", transaction_sizes),
        ("Lines of Code", loc)]
    output = ""
    for s in stats:
        output += format(s[0], s[1])

    outputDirectory = "./"
    with open(outputDirectory + str('stats.txt'), 'w+') as outputFile:
        outputFile.write(output)


def print_useage():
    print "This program gives statistics about the current realm-core build."
    print "Two arguments are required: "
    print "\t-the source code root directory"
    print "\t-the build root directory."
    print "collect_stats.py -b <build-root-dir> -s <source-root-dir>"


def main(argv):
    source = ''
    build = ''
    try:
        opts, args = getopt.getopt(argv, "hb:s:",
                                   ["build-root-dir=", "source-root-dir="])
    except getopt.GetoptError:
        print_useage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print_useage()
            sys.exit()
        elif opt in ("-b", "--build-root-dir"):
            build = arg
        elif opt in ("-s", "--source-root-dir"):
            source = arg
    if not source or not build:
        print_useage()
        sys.exit(2)

    do_collect_stats(build, source)


if __name__ == "__main__":
    main(sys.argv[1:])

