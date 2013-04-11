#include <tightdb.hpp>
#include <iostream>

TIGHTDB_TABLE_2(Alpha,
                foo, Int,
                bar, Int)

using namespace std;
using namespace tightdb;

int main()
{
    SharedGroup sg("/tmp/benchmark-prealloc.tightdb");

    for (int i=0; i<10000; ++i) {
        WriteTransaction wt(sg);
        Alpha::Ref t = wt.get_table<Alpha>("alpha");
        t->add(65536,65536);
        wt.commit();
//        cerr << ".";
    }
}
