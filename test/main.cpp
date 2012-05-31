
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

uint64_t rand2(int bitwidth = 64)
{
    uint64_t i = (int64_t)rand() * (int64_t)rand() * (int64_t)rand() * (int64_t)rand() * (int64_t)rand();
    if(bitwidth < 64) {
        const uint64_t mask = ((1ULL << bitwidth) - 1ULL);
        i &= mask;
    }
    return i;
}


void aaa(void)
{
    const uint64_t ITER_PER_BITWIDTH = 16 * 1000 * TEST_DURATION * TEST_DURATION * TEST_DURATION;
    const uint64_t SEED = 123;

    VerifiedInteger a;
    Array res;

    srand(SEED);
    size_t current_bitwidth = 0;
    unsigned int trend = 5;

    for(current_bitwidth = 0; current_bitwidth < 65; current_bitwidth++) {
        for(size_t iter = 0; iter < ITER_PER_BITWIDTH; iter++) {

//          if(rand() % 10 == 0) printf("Input bitwidth around ~%d, , a.Size()=%d\n", (int)current_bitwidth, (int)a.Size());

            if (!(rand2() % (ITER_PER_BITWIDTH / 100))) {
                trend = (unsigned int)rand2() % 10;
                a.find_first(rand2(current_bitwidth));
                a.find_all(res, rand2(current_bitwidth));
                size_t start = rand2() % (a.Size() + 1);
                a.Sum(start, start + rand2() % (a.Size() + 1 - start));
                a.maximum(start, start + rand2() % (a.Size() + 1 - start));
                a.minimum(start, start + rand2() % (a.Size() + 1 - start));
            }

            if (rand2() % 10 > trend && a.Size() < ITER_PER_BITWIDTH / 100) {
                uint64_t l = rand2(current_bitwidth);
                if(rand2() % 2 == 0) {
                    // Insert
                    const size_t pos = rand2() % (a.Size() + 1);
                    a.Insert(pos, l);
                }
                else {
                    // Add
                    a.add(l);
                }
            }
            else if(a.Size() > 0) {
                // Delete
                const size_t i = rand2() % a.Size();
                a.Delete(i);
            }
        }
    }

    // Cleanup
    a.Destroy();
    res.Destroy();
}


int main(int argc, char const *const argv[])
{
    bool const no_error_exit_staus = 2 <= argc && strcmp(argv[1], "--no-error-exit-staus") == 0;

    aaa();

/*
    tightdb::Array a;

    for(size_t t = 0; t < 10000; t++)
        a.add(123);

    UnitTest::Timer timer; 
    timer.Start();
    
    for(size_t t = 0; t < 100000; t++) {
        a.find_first(100);
    }

    printf("%d ms\n", timer.GetTimeInMs());

    return 0;
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
