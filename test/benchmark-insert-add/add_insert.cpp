#include <tightdb.hpp>
#include <tightdb/group_shared.hpp>
#include <unistd.h>
#include <time.h>

using namespace tightdb;
using namespace std;

TIGHTDB_TABLE_5(TestTable,
                x,  Int,
                s1, String,
                b,  Bool,
                s2, String,
                s3, String)

double delta_time(struct timespec ts_1, struct timespec ts_2) {
    return (double)ts_2.tv_sec+1e-9*(double)ts_2.tv_nsec - ((double)ts_1.tv_sec+1e-9*(double)ts_1.tv_nsec);
}




int main(int argc, char *argv[]) {
    size_t N = 100000000;
    size_t n = 50000;
    struct timespec ts_begin, ts_now;
    TestTable t;

    int c;
    extern char *optarg;

    bool use_shared = false;
    SharedGroup::DurabilityLevel dlevel;
    bool do_insert  = false;

    while ((c = getopt(argc, argv, "hs:iN:n:")) != EOF) {
        switch (c) {
            case 'h':
                cout << "Usage: add_insert [-h] [-s mem|full] [-a]" << endl;
                cout << "  -h : this text" << endl;
                cout << "  -s : use shared group (default: no)" << endl;
                cout << "  -i : insert at front (defalut: no - append)" << endl;
                cout << "  -N : number of rows to add" << endl;
                cout << "  -n : rows between print outs" << endl;
                exit(-1);
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
            default:
                cout << "Unknown option" << endl;
                exit(-1);
        }
    }

    SharedGroup sg = SharedGroup("test.tightdb", false, dlevel);

    if (use_shared) {
        File::try_remove("test.tightdb");
        File::try_remove("test.tightdb.lock");
        {
            WriteTransaction wt(sg);
            BasicTableRef<TestTable> t = wt.get_table<TestTable>("test");
            wt.commit();
        }
    }


    clock_gettime(CLOCK_REALTIME, &ts_begin);
    for(size_t i=0; i<N; ++i) {
        if (use_shared) {
            {
                WriteTransaction wt(sg);
                BasicTableRef<TestTable> t1 = wt.get_table<TestTable>("test");
                if (do_insert) {
                    t1->insert(0, N, "Hello", i%2, "World", "Smurf");
                }
                else {
                    t1->add(N, "Hello", i%2, "World", "Smurf");
                }
                wt.commit();
            }
        }
        else {
            if (do_insert) {
                t.insert(0, N, "Hello", i%2, "World", "Smurf");
            }
            else {
                t.add(N, "Hello", i%2, "World", "Smurf");
            }
        }

        if ((i % n) == 0 && i > 0) {
            clock_gettime(CLOCK_REALTIME, &ts_now);
            double dt = delta_time(ts_begin, ts_now);
            cout << i << ";" << dt << ";" << double(i)/dt << endl;
        }
    }
    return 0;
}
