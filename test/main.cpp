
#include <UnitTest++.h>
#include <cstring>
#include <string>
#include <math.h>
#include "column.hpp"

int main(int argc, char const *const argv[])
{
    bool const no_error_exit_staus = 2 <= argc && strcmp(argv[1], "--no-error-exit-staus") == 0;

    const int res = UnitTest::RunAllTests();

#ifdef _MSC_VER
    getchar(); // wait for key
#endif

    return no_error_exit_staus ? 0 : res;
}
