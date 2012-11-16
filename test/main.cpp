#include <cstring>
#include <iostream>
#include <UnitTest++.h>
#include <tightdb/utilities.hpp>
#include <tightdb/tightdb_nmmintrin.h>
#if defined(_MSC_VER) && defined(_DEBUG)
//    #include "C:\\Program Files (x86)\\Visual Leak Detector\\include\\vld.h"
#endif

using namespace std;

int main(int argc, char* argv[])
{
    bool const no_error_exit_staus = 2 <= argc && strcmp(argv[1], "--no-error-exit-staus") == 0;

#ifdef TIGHTDB_DEBUG
    cout << "Running Debug unit tests\n";
#else
    cout << "Running Release unit tests\n";
#endif

#ifdef TIGHTDB_COMPILER_SSE
    cout << "Compiler supported SSE (auto detect): Yes\n";
#else
    cout << "Compiler supported SSE (auto detect): No\n";
#endif

    cout << "This CPU supports SSE (auto detect):  " << (tightdb::cpuid_sse<42>() ? "4.2" : (tightdb::cpuid_sse<30>() ? "3.0" : "None"));
    cout << "\n\n";

    const int res = UnitTest::RunAllTests();

#ifdef _MSC_VER
    getchar(); // wait for key
#endif
    return no_error_exit_staus ? 0 : res;
}