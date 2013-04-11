#include <ctime>
#include <iostream>

#include <tightdb.hpp>

TIGHTDB_TABLE_2(Alpha,
                foo, Int,
                bar, Int)

using namespace std;
using namespace tightdb;

int main()
{
    remove("/tmp/benchmark-prealloc.tightdb");
    SharedGroup sg("/tmp/benchmark-prealloc.tightdb");

    remove("/tmp/benchmark-prealloc-interfere1.tightdb");
    SharedGroup sg_interfere1("/tmp/benchmark-prealloc-interfere1.tightdb");

    remove("/tmp/benchmark-prealloc-interfere2.tightdb");
    SharedGroup sg_interfere2("/tmp/benchmark-prealloc-interfere2.tightdb");

    remove("/tmp/benchmark-prealloc-interfere3.tightdb");
    SharedGroup sg_interfere3("/tmp/benchmark-prealloc-interfere3.tightdb");

    for (int i=0; i<100; ++i) {
        cerr << ".";
        for (int j=0; j<100; ++j) {
            {
                WriteTransaction wt(sg);
                Alpha::Ref t = wt.get_table<Alpha>("alpha");
                for (int j=0; j<1000; ++j) t->add(65536,65536);
                wt.commit();
            }
            // Interference
            for (int k=0; k<2; ++k) {
                {
                    WriteTransaction wt(sg_interfere1);
                    Alpha::Ref t = wt.get_table<Alpha>("alpha");
                    for (int j=0; j<100; ++j) t->add(65536,65536);
                    wt.commit();
                }
                {
                    WriteTransaction wt(sg_interfere2);
                    Alpha::Ref t = wt.get_table<Alpha>("alpha");
                    for (int j=0; j<400; ++j) t->add(65536,65536);
                    wt.commit();
                }
                {
                    WriteTransaction wt(sg_interfere3);
                    Alpha::Ref t = wt.get_table<Alpha>("alpha");
                    for (int j=0; j<1600; ++j) t->add(65536,65536);
                    wt.commit();
                }
            }
        }
    }
    cerr << "\n";

    time_t begin = time(0);

    for (int i=0; i<100; ++i) {
        cerr << "x";
        for (int j=0; j<10; ++j) {
            {
                WriteTransaction wt(sg);
                Alpha::Ref t = wt.get_table<Alpha>("alpha");
                t->column().foo += 1;
                t->column().bar += 1;
                wt.commit();
            }
        }
    }
    cerr << "\n";

    time_t end = time(0);

    cerr << "Write transactions per second = " << (( 1000 / double(end - begin) )) << endl;
}
