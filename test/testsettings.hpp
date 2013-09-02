#ifndef TESTSETTINGS_H
#define TESTSETTINGS_H

#ifndef TEST_DURATION
    #define TEST_DURATION 0    // Only brief unit tests. < 1 sec
    //#define TEST_DURATION 1  // All unit tests, plus monkey tests. ~1 minute
    //#define TEST_DURATION 2  // Same as 2, but longer monkey tests. 8 minutes
    //#define TEST_DURATION 3
#endif

// Robustness tests are not enable by default, because they interfere badly with Valgrind.
// #define TEST_ROBUSTNESS

// Wrap pthread function calls with the pthread bug finding tool (program execution will be slower) by
// #including pthread_test.h. Works both in debug and release mode.
//#define TIGHTDB_PTHREADS_TEST

// Transaction stress tests in test_transactions_lasse.cpp that take a long time to run
//#define STRESSTEST1
//#define STRESSTEST2
//#define STRESSTEST3
//#define STRESSTEST4

// Bypass an overflow bug in BinaryData. Todo/fixme
#define TIGHTDB_BYPASS_BINARYDATA_BUG

// Bypass as crash when doing optimize+set_index+clear+add. Todo/fixme
#define TIGHTDB_BYPASS_OPTIMIZE_CRASH_BUG

#endif
