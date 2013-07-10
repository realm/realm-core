#include <tightdb.hpp>
#include <tightdb/group_shared.hpp>
#include <unistd.h>
#include <time.h>
#include <cstdio>

using namespace tightdb;
using namespace std;

enum Mode {USE_SHARED, USE_GROUP, USE_TABLE};

TIGHTDB_TABLE_5(TestTable,
                x,  Int,
                s1, String,
                b,  Bool,
                s2, String,
                s3, String)

double delta_time(struct timespec ts_1, struct timespec ts_2) {
    return (double)ts_2.tv_sec+1e-9*(double)ts_2.tv_nsec - ((double)ts_1.tv_sec+1e-9*(double)ts_1.tv_nsec);
}


void usage(void) {
    cout << "Usage: add_insert [-h] [-s mem|full] [-a]" << endl;
    cout << "  -h : this text" << endl;
    cout << "  -s : use shared group (default: no)" << endl;
    cout << "  -i : insert at front (defalut: no - append)" << endl;
    cout << "  -N : number of rows to add" << endl;
    cout << "  -n : rows between print outs" << endl;
    cout << "  -g : use group (default: no)" << endl;
    cout << "  -r : rows/commit (default: 1)" << endl;
    exit(-1);
}


int main(int argc, char *argv[]) {
    size_t N = 100000000;
    size_t n = 50000;
    size_t rows_per_commit = 1;
    struct timespec ts_begin, ts_now;
    TestTable t;

    int c;
    extern char *optarg;

    bool use_shared = false;
    SharedGroup::DurabilityLevel dlevel;
    bool do_insert  = false;
    bool use_group  = false;

    while ((c = getopt(argc, argv, "hs:iN:n:r:g")) != EOF) {
        switch (c) {
            case 'h':
                usage();
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
                        cout << "durability must be either mem or full" << endl;
                        exit(-1);
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
        }
    }

    if (use_group && use_shared) {
        cout << "You cannot specify -g and -s at the same time." << endl;
        usage();
    }

    Mode m;
    if (use_group) {
        m = USE_GROUP;
    }
    else {
        if (use_shared) {
            m = USE_SHARED;
        }
        else {
            m = USE_TABLE;
        }
    }

    cout << "# Parameters: " << endl;
    cout << "#  number of rows    : " << N << endl;
    cout << "#  rows per commit   : " << rows_per_commit << endl;
    cout << "#  output frequency  : " << n << endl;
    cout << "#  mode              : " << m << endl;
    if (do_insert) {
        cout << "# do inserts" << endl;
    }

    File::try_remove("test.tightdb");
    File::try_remove("test.tightdb.lock");
    File::try_remove("gtest.tightdb");

    SharedGroup sg = SharedGroup("test.tightdb", false, dlevel);
    Group g("gtest.tightdb", Group::mode_ReadWrite);

    switch (m) {
        case USE_SHARED:
            {
                WriteTransaction wt(sg);
                BasicTableRef<TestTable> t = wt.get_table<TestTable>("test");
                wt.commit();
            }
            break;
        case USE_GROUP:
            BasicTableRef<TestTable> t = g.get_table<TestTable>("test");
            try {
                g.commit();
            }
            catch (std::runtime_error& e) {
                cerr << "Cannot create table: " << e.what() << endl;
                exit(-1);
            }
            break;
    }

    clock_gettime(CLOCK_REALTIME, &ts_begin);
    for(size_t i=0; i<N/rows_per_commit; ++i) {
        switch (m) {
            case USE_SHARED: {
                WriteTransaction wt(sg);
                BasicTableRef<TestTable> t1 = wt.get_table<TestTable>("test");
                {
                    for(size_t j=0; j<rows_per_commit; ++j) {
                        if (do_insert) {
                            t1->insert(0, N, "Hello", i%2, "World", "Smurf");
                        }
                        else {
                            t1->add(N, "Hello", i%2, "World", "Smurf");
                        }
                    }
                }
                wt.commit();
                break;
            }
            case USE_GROUP: {
                BasicTableRef<TestTable> t1 = g.get_table<TestTable>("test");
                for(size_t j=0; j<rows_per_commit; ++j) {
                    if (do_insert) {
                        t1->insert(0, N, "Hello", i%2, "World", "Smurf");
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
                    exit(-1);
                }
                catch (std::runtime_error& e) {
                    cerr << "commit (runtime error): " << e.what() << endl;
                    exit(-1);
                }
                break;
            }
            case USE_TABLE:
                for(size_t j=0; j<rows_per_commit; ++j) {
                    if (do_insert) {
                        t.insert(0, N, "Hello", i%2, "World", "Smurf");
                    }
                    else {
                        t.add(N, "Hello", i%2, "World", "Smurf");
                    }
                }
                break;
        }

        if (((i*rows_per_commit) % n) == 0 && i > 0) {
            clock_gettime(CLOCK_REALTIME, &ts_now);
            double dt = delta_time(ts_begin, ts_now);
            cout << i*rows_per_commit << ";" << dt << ";" << double(i*rows_per_commit)/dt << ";" << dt/double(i*rows_per_commit) << endl;
        }
    }
    return 0;
}
