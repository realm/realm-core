#include <cstring>
#include <iostream>

#include <UnitTest++.h>

#include <tightdb/column.hpp>

#if defined(_MSC_VER) && defined(_DEBUG)
//    #include <vld.h>
#endif

using namespace std;

#include <tightdb/array.hpp>
using namespace tightdb;

int main(int argc, char* argv[])
{
    bool const no_error_exit_staus = 2 <= argc && strcmp(argv[1], "--no-error-exit-staus") == 0;

#ifdef TIGHTDB_DEBUG
    cout << "Running Debug unit tests\n\n";
#else
    cout << "Running Release unit tests\n\n";
#endif

#if 0
    static Array a;

    printf("size( Array() ) = %d bytes.\n", sizeof(a));
    for (size_t i = 0; i< 1; ++i) {
        a.add(0); 
        size_t v1, v2, v3;
        v1 = a.Get(i);
        v2 = a.Size();
        v3 = a.GetBitWidth();
        printf("  val = %d, size = %d, bitw = %d \n", v1, v2, v3);
    }
    MemStats stat;            
    a.Stats(stat);
    printf("Stat: alllocated: %d, used: %d, count: %d.\n", stat.allocated, stat.used, stat.array_count);

    a.Clear();
    printf(" --- clear() \n");
    for (size_t i = 0; i< 1000; ++i) {
        a.add(1); 
        size_t v1, v2, v3;
        v1 = a.Get(i);
        v2 = a.Size();
        v3 = a.GetBitWidth();
        //printf("  val = %d, size = %d, bitw = %d \n", v1, v2, v3);
    }
    MemStats stat2;            
    a.Stats(stat2);
    printf("Stat: alllocated: %d, used: %d, count: %d.\n", stat2.allocated, stat2.used, stat2.array_count);

    const int res = 1;
#else
    const int res =UnitTest::RunAllTests();
#endif 

#ifdef _MSC_VER
    getchar(); // wait for key
#endif

    return no_error_exit_staus ? 0 : res;
}
