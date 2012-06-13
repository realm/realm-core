
#include <UnitTest++.h>
#include <cstring>
#include <string>
#include <math.h>
#include "column.hpp"
#if defined(_MSC_VER) && defined(_DEBUG)
//    #include <vld.h> 
#endif

#include "../src/array.hpp"
#include "testsettings.hpp"
#include "large_tests/verified_integer.hpp"
#include "query_conditions.hpp"
using namespace tightdb;


int main(int argc, char const *const argv[])
{

    bool const no_error_exit_staus = 2 <= argc && strcmp(argv[1], "--no-error-exit-staus") == 0;


/*
    tightdb::Array a;
    // 1618 ms

    for(size_t t = 0; t < 2000; t++)
        a.add(1ULL*100ULL);

    UnitTest::Timer timer; 
    int duration = 1000000;

    for(size_t n = 0; n < 20; n++) {
        timer.Start();
        for(size_t t = 0; t < 500*1000ULL; t++) {
            volatile int64_t s = a.sum();
        }
        if(timer.GetTimeInMs() < duration)
            duration = timer.GetTimeInMs();
    }

    printf("%d ms\n", duration);
    
    */


/*
    int x = 0;

tightdb::Column a;
    // 1618 ms


    for(size_t t = 0; t < 100000; t++)
        a.add(1234);
 //   a.Insert(100, 100);

    UnitTest::Timer timer; 
    int duration = 1000000;

    for(size_t n = 0; n < 20; n++) {
        timer.Start();
        for(size_t t = 0; t < 10*1000*1000ULL; t++) {
            size_t p = t % 100000;
            x += a.Get(p);
        }
        if(timer.GetTimeInMs() < duration)
            duration = timer.GetTimeInMs();
    }

    printf("%d ms\n", duration);
    
    return x;
    */


/*
tightdb::Array a;
    // 1618 ms


    for(size_t t = 0; t < 100; t++)
        a.add(123);
 //   a.Insert(100, 100);

    UnitTest::Timer timer; 
    int duration = 100000;

    for(size_t n = 0; n < 20; n++) {
        timer.Start();
        for(size_t t = 0; t < 4000000ULL; t++) {
            volatile size_t x = a.find_first(100);
        }
        if(timer.GetTimeInMs() < duration)
            duration = timer.GetTimeInMs();
    }

    printf("%d ms\n", duration);
    */

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
