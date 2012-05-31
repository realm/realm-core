#include <UnitTest++.h>
#include <cstring>
#include <string>
#include <math.h>
#include "column.hpp"
#if defined(_MSC_VER) && defined(_DEBUG)
    #include <vld.h>
#endif

int main(int argc, char const *const argv[])
{
    bool const no_error_exit_staus = 2 <= argc && strcmp(argv[1], "--no-error-exit-staus") == 0;

#ifdef _DEBUG
    std::cout << "Running Debug unit tests\n\n";
#else
    std::cout << "Running Release unit tests\n\n";
#endif

    const int res = UnitTest::RunAllTests();

#ifdef _MSC_VER
    getchar(); // wait for key
#endif

    return no_error_exit_staus ? 0 : res;
}