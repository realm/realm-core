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

    const int res =UnitTest::RunAllTests();

#ifdef _MSC_VER
    getchar(); // wait for key
#endif

    return no_error_exit_staus ? 0 : res;
}
