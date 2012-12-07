#ifndef TESTSETTINGS_H
#define TESTSETTINGS_H

#ifndef TEST_DURATION
    #define TEST_DURATION 0    // Only brief unit tests. < 1 sec
    //#define TEST_DURATION 1  // All unit tests, plus monkey tests. ~1 minute
    //#define TEST_DURATION 2  // Same as 2, but longer monkey tests. 8 minutes
    //#define TEST_DURATION 3
#endif

// Wrap pthread function calls with the pthread bug finding tool (program execution will be slower) by 
// #including pthread_test.h. Works both in debug and release mode.
//#define TIGHTDB_PTHREADS_TEST

// Two transaction stress tests in testtransactions_lasse.cpp that take a long time to run
//#define STRESSTEST1
//#define STRESSTEST2

#endif
