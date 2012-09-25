#include <cstring>
#include <iostream>

#include <UnitTest++.h>
#include "d:/m/src/tightdb.hpp"
#include <assert.h>
#include <tightdb/column.hpp>

#if defined(_MSC_VER) && defined(_DEBUG)
//    #include <vld.h>
#endif

using namespace std;

TIGHTDB_TABLE_2(TupleTableType,
                first,  Int,
                second, String)



int main(int argc, char* argv[])
{
    /*

    TupleTableType ttt;

    for(size_t t = 0; t < 10000; t++) {
        ttt.add(1, "a");
        ttt.add(4, "b");
        ttt.add(7, "c");
        ttt.add(10, "a");
        ttt.add(1, "b");
        ttt.add(4, "c");
    }

    ttt.column().second.set_index();
    printf("go");
    for(size_t t = 0; t < 1000; ++t) {
        size_t s = ttt.where().second.equal("a").first.sum(ttt);
        assert(10*11 == s);

        TupleTableType::View tv = ttt.where().second.equal("a").find_all(ttt);
        assert(10*2 == tv.size());
    }

    return 0;



    */




    bool const no_error_exit_staus = 2 <= argc && strcmp(argv[1], "--no-error-exit-staus") == 0;

#ifdef TIGHTDB_DEBUG
    cout << "Running Debug unit tests\n\n";
#else
    cout << "Running Release unit tests\n\n";
#endif

    const int res = UnitTest::RunAllTests();

#ifdef _MSC_VER
    getchar(); // wait for key
#endif

    return no_error_exit_staus ? 0 : res;
}
