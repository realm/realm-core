#ifndef TESTSETTINGS_H
#define TESTSETTINGS_H

#ifndef TEST_DURATION
#  define TEST_DURATION 0    // Only brief unit tests. < 1 sec
//#  define TEST_DURATION 1  // All unit tests, plus monkey tests. ~1 minute
//#  define TEST_DURATION 2  // Same as 2, but longer monkey tests. 8 minutes
//#  define TEST_DURATION 3
#endif

// Some threading robustness tests are not enable by default, because
// they interfere badly with Valgrind.
#define TEST_THREAD_ROBUSTNESS 0

// Wrap pthread function calls with the pthread bug finding tool (program execution will be slower) by
// #including pthread_test.h. Works both in debug and release mode.
//#define TIGHTDB_PTHREADS_TEST

#define TEST_COLUMN_MIXED
#define TEST_ALLOC
#define TEST_ARRAY
#define TEST_ARRAY_BINARY
#define TEST_ARRAY_BLOB
#define TEST_ARRAY_FLOAT
#define TEST_ARRAY_STRING
#define TEST_ARRAY_STRING_LONG
#define TEST_COLUMN
#define TEST_COLUMN_BASIC
#define TEST_COLUMN_BINARY
#define TEST_COLUMN_FLOAT
#define TEST_COLUMN_MIXED
#define TEST_COLUMN_STRING
#define TEST_FILE
#define TEST_FILE_LOCKS
#define TEST_GROUP
//#define TEST_INDEX // not implemented yet
#define TEST_INDEX_STRING
#define TEST_LANG_BIND_HELPER
#define TEST_QUERY
#define TEST_SHARED
#define TEST_STRING_DATA
#define TEST_BINARY_DATA
#define TEST_TABLE
#define TEST_TABLE_VIEW
#define TEST_LINK_VIEW
#define TEST_THREAD
#define TEST_TRANSACTIONS
#define TEST_TRANSACTIONS_LASSE
#define TEST_REPLICATION
#define TEST_UTF8
#define TEST_COLUMN_LARGE
#define TEST_JSON
#define TEST_LINKS

// Takes a long time. Also currently fails to reproduce the Java bug, but once it has been identified, this
// test could perhaps be modified to trigger it (unless it's a language binding problem).
//#define JAVA_MANY_COLUMNS_CRASH

#endif
