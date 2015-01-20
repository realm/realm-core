#include <iostream>

#include <unistd.h>

#include <tightdb.hpp>

#include "../util/timer.hpp"

using namespace std;
using namespace tightdb;
using namespace tightdb::util;


namespace {

enum Mode {
    mode_UseShared,
    mode_UseGroup,
    mode_UseTable
};

TIGHTDB_TABLE_5(TestTable,
                x,  Int,
                s1, String,
                b,  Bool,
                s2, String,
                s3, String)


void usage()
{
    cout << "Usage: add_insert [-h] [-s mem|full|async] [-a]" << endl;
    cout << "  -h : this text" << endl;
    cout << "  -s : use shared group (default: no)" << endl;
    cout << "  -i : insert at front (defalut: no - append)" << endl;
    cout << "  -N : number of rows to add" << endl;
    cout << "  -n : rows between print outs" << endl;
    cout << "  -g : use group (default: no)" << endl;
    cout << "  -r : rows/commit (default: 1)" << endl;
    cout << "  -R : insert at random position (only useful with -i)" << endl;
}

} // anonymous namespace


int main(int argc, char *argv[])
{
    size_t N = 100000000;
    size_t n = 50000;
    size_t rows_per_commit = 1;
    TestTable t;

    int c;
    extern char *optarg;

    bool use_shared    = false;
    SharedGroup::DurabilityLevel dlevel = SharedGroup::durability_Full;
    bool do_insert     = false;
    bool use_group     = false;
    bool random_insert = false;

    // FIXME: 'getopt' is POSIX/Linux specific. We should replace with
    // code similar to what appears in main() in
    // "tightdb_tools/src/tightdb/tools/prompt/prompt.cpp".
    while ((c = getopt(argc, argv, "hs:iN:n:r:gR")) != EOF) {
        switch (c) {
            case 'h':
                usage();
                return 0;
            case 'R':
                random_insert = true;
                break;
            case 's':
                use_shared = true;
                if (strcmp(optarg, "mem") == 0) {
                    dlevel = SharedGroup::durability_MemOnly;
                }
                else {
                    if (strcmp(optarg, "full") == 0) {
                        dlevel = SharedGroup::durability_Full;
                    }
                    else {
                        if (strcmp(optarg, "async") == 0) {
                            dlevel = SharedGroup::durability_Async;
                        }
                        else {
                            cout << "durability must be either mem or full" << endl;
                            return 1;
                        }
                    }
                }
                break;
            case 'i':
                do_insert = true;
                break;
            case 'N':
                N = size_t(atol(optarg));
                break;
            case 'n':
                n = size_t(atol(optarg));
                break;
            case 'g':
                use_group = true;
                break;
            case 'r':
                rows_per_commit = size_t(atol(optarg));
                break;
            default:
                cout << "Unknown option" << endl;
                usage();
                return 1;
        }
    }

    if (use_group && use_shared) {
        cout << "You cannot specify -g and -s at the same time." << endl;
        usage();
        return 1;
    }

    Mode m;
    if (use_group) {
        m = mode_UseGroup;
    }
    else {
        if (use_shared) {
            m = mode_UseShared;
        }
        else {
            m = mode_UseTable;
        }
    }

    cout << "# Parameters: " << endl;
    cout << "#  number of rows    : " << N << endl;
    cout << "#  rows per commit   : " << rows_per_commit << endl;
    cout << "#  output frequency  : " << n << endl;
    cout << "#  mode              : " << m << endl;
    if (do_insert) {
        cout << "#  do inserts" << endl;
        cout << "#  random insert     : " << random_insert << endl;
    }

    if (random_insert) {  // initialize RNG
        srandom(0);
    }

    while (File::exists("test.tightdb.lock")) {
        usleep(10000);
    }
    File::try_remove("test.tightdb");
    File::try_remove("gtest.tightdb");

    SharedGroup sg = SharedGroup("test.tightdb", false, dlevel);
    Group g("gtest.tightdb", Group::mode_ReadWrite);

    switch(m) {
        case mode_UseShared: {
            WriteTransaction wt(sg);
            BasicTableRef<TestTable> t = wt.add_table<TestTable>("test");
            wt.commit();
            break;
        }
        case mode_UseGroup: {
            BasicTableRef<TestTable> t = g.add_table<TestTable>("test");
            try {
                g.commit();
            }
            catch (RuntimeError& e) {
                cerr << "Cannot create table: " << e.what() << endl;
                return 1;
            }
            break;
        }
        case mode_UseTable:
            break;
    }

    test_util::Timer timer(test_util::Timer::type_RealTime);
    for(size_t i=0; i<N/rows_per_commit; ++i) {
        switch(m) {
            case mode_UseShared: {
                WriteTransaction wt(sg);
                BasicTableRef<TestTable> t1 = wt.get_table<TestTable>("test");
                {
                    for(size_t j=0; j<rows_per_commit; ++j) {
                        if (do_insert) {
                            size_t k = 0;
                            if (random_insert && t1->size() > 0) {
                                k = size_t(random() % t1->size());
                            }
                            t1->insert(k, N, "Hello", i%2, "World", "Smurf");
                        }
                        else {
                            t1->add(N, "Hello", i%2, "World", "Smurf");
                        }
                    }
                }
                wt.commit();
                break;
            }
            case mode_UseGroup: {
                BasicTableRef<TestTable> t1 = g.get_table<TestTable>("test");
                for(size_t j=0; j<rows_per_commit; ++j) {
                    if (do_insert) {
                        size_t k = 0;
                        if (random_insert && t1->size() > 0) {
                            k = size_t(random() % t1->size());
                        }
                        t1->insert(k, N, "Hello", i%2, "World", "Smurf");
                    }
                    else {
                        t1->add(N, "Hello", i%2, "World", "Smurf");
                    }
                }
                try {
                    g.commit();
                }
                catch (File::PermissionDenied& e) {
                    cerr << "commit (permission denied): " << e.what() << endl;
                    return 1;
                }
                catch (RuntimeError& e) {
                    cerr << "commit (runtime error): " << e.what() << endl;
                    return 1;
                }
                break;
            }
            case mode_UseTable:
                for(size_t j=0; j<rows_per_commit; ++j) {
                    if (do_insert) {
                        size_t k = 0;
                        if (random_insert && t.size() > 0) {
                            k = size_t(random() % t.size());
                        }
                        t.insert(k, N, "Hello", i%2, "World", "Smurf");
                    }
                    else {
                        t.add(N, "Hello", i%2, "World", "Smurf");
                    }
                }
                break;
        }

        if (((i*rows_per_commit) % n) == 0 && i > 0) {
            double dt = timer.get_elapsed_time();
            cout << i*rows_per_commit << ";" << dt << ";" << double(i*rows_per_commit)/dt << ";" << dt/double(i*rows_per_commit) << endl;
        }
    }
    return 0;
}

