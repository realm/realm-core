import os

def get_file_size(filename):
    statinfo = os.stat(filename)
    return statinfo.st_size

def get_library_size(rootBuildDir):
    return get_file_size(os.path.join(rootBuildDir, "src/realm/librealm.a")) # same name on mac/linux

def get_loc(directory, recursive = True):
    loc = 0
    for root, dirs, files in os.walk(directory, followlinks = False):
        if (recursive == False and root != directory): continue
        for filename in files:
            if (filename.endswith('.hpp') or filename.endswith('.cpp')):
                fullpath = os.path.join(root, filename)
                print fullpath
                with open(fullpath, 'r') as f:
                    loc += len(list(filter(lambda x: x.strip(), f))) # excludes empty lines
    return loc

def get_lines_of_code(rootSourceDir):
    return get_loc(os.path.join(rootSourceDir, "src"))

def get_lines_of_test_code(rootSourceDir):
    loc = get_loc(os.path.join(rootSourceDir, "test"), recursive = False)
    loc += get_loc(os.path.join(rootSourceDir, "test/util"), recursive = False)
    return loc

def get_realm_sizes(rootBuildDir):
    pathPrefix = os.path.join(rootBuildDir, "test/bench/stats/")
    results = []
    sizes = [0, 1000, 2000, 4000, 8000, 10000]
    for size in sizes:
        name = str(size) + "bytes.realm"
        os.system(pathPrefix + "realm-stats " + name + " " + str(size))
        results.append((str(size) + " bytes", get_file_size(name)))
    return results

def format(description, values):
    return str(description) + ":" + str(values) + "\n"

def do_collect_stats(rootBuildDir, rootSourceDir):
    stats = [("Realm Library Size", [("librealm.a", get_library_size(rootBuildDir))]),
             ("Disk Space Used by Realm (bytes)", get_realm_sizes(rootBuildDir)),
             ("Lines of Code", [("Realm Code", get_lines_of_code(rootSourceDir)),
                                ("Test Code", get_lines_of_test_code(rootSourceDir))])]
    output = ""
    for s in stats:
        output += format(s[0], s[1])

    outputDirectory = "./"
    with open(outputDirectory + str('stats.txt'), 'w+') as outputFile:
        outputFile.write(output)

