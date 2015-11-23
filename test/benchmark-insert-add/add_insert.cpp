#include <iostream>

#include <unistd.h>

#include <realm.hpp>

#include "../util/timer.hpp"

using namespace realm;
using namespace realm::util;


namespace {

enum Mode {
    mode_UseShared,
    mode_UseGroup,
    mode_UseTable
};

REALM_TABLE_5(TestTable,
                x,  Int,
                s1, String,
                b,  Bool,
                s2, String,
                s3, String)


void usage()
{
    std::cout << "Usage: add_insert [-h] [-s mem|full|async] [-a]" << std::endl;
    std::cout << "  -h : this text" << std::endl;
    std::cout << "  -s : use shared group (default: no)" << std::endl;
    std::cout << "  -i : insert at front (defalut: no - append)" << std::endl;
    std::cout << "  -N : number of rows to add" << std::endl;
    std::cout << "  -n : rows between print outs" << std::endl;
    std::cout << "  -g : use group (default: no)" << std::endl;
    std::cout << "  -r : rows/commit (default: 1)" << std::endl;
    std::cout << "  -R : insert at random position (only useful with -i)" << std::endl;
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
    // "realm_tools/src/realm/tools/prompt/prompt.cpp".
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
                            std::cout << "durability must be either mem or full" << std::endl;
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
                std::cout << "Unknown option" << std::endl;
                usage();
                return 1;
        }
    }

    if (use_group && use_shared) {
        std::cout << "You cannot specify -g and -s at the same time." << std::endl;
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

    std::cout << "# Parameters: " << std::endl;
    std::cout << "#  number of rows    : " << N << std::endl;
    std::cout << "#  rows per commit   : " << rows_per_commit << std::endl;
    std::cout << "#  output frequency  : " << n << std::endl;
    std::cout << "#  mode              : " << m << std::endl;
    if (do_insert) {
        std::cout << "#  do inserts" << std::endl;
        std::cout << "#  random insert     : " << random_insert << std::endl;
    }

    if (random_insert) {  // initialize RNG
        srandom(0);
    }

    while (File::exists("test.realm.lock")) {
        usleep(10000);
    }
    File::try_remove("test.realm");
    File::try_remove("gtest.realm");

    SharedGroup sg = SharedGroup("test.realm", false, dlevel);
    Group g("gtest.realm", NULL, Group::mode_ReadWrite);

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
            catch (std::runtime_error& e) {
                std::cerr << "Cannot create table: " << e.what() << std::endl;
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
                    std::cerr << "commit (permission denied): " << e.what() << std::endl;
                    return 1;
                }
                catch (std::runtime_error& e) {
                    std::cerr << "commit (runtime error): " << e.what() << std::endl;
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
            std::cout << i*rows_per_commit << ";" << dt << ";" << double(i*rows_per_commit)/dt << ";" << dt/double(i*rows_per_commit) << std::endl;
        }
    }
    return 0;
}

