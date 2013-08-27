#include <cstring>
#include <fstream>
#include <iostream>
#include <unistd.h>
#include <wait.h>

#include <tightdb/column.hpp>
#include <tightdb.hpp>
#include <tightdb/group_shared.hpp>
#include <tightdb/column_string.hpp>
#include <tightdb/column_string_enum.hpp>
#include <tightdb/column_mixed.hpp>
#include <tightdb/array_binary.hpp>
#include <tightdb/array_string_long.hpp>

#define CHECK(v) do { if (v) break; cerr << __LINE__ << ": CHECK failed" << endl; } while (false)
#define CHECK_EQUAL(a, b) do { if (check_equal((a),(b))) break; cerr << __LINE__ << ": CHECK_EQUAL failed: " << (a) << " vs " << (b) << endl; } while(false)
#define CHECK_THROW(v, e) do { try { v; } catch (e&) { break; } cerr << __LINE__ << ": CHECK_THROW failed: Expected " # e << endl; } while(false)

using namespace tightdb;
using namespace std;


namespace {

template<class A, class B> inline bool check_equal(const A& a, const B& b) { return a == b; }
inline bool check_equal(const char* a, const char* b) { return strcmp(a, b) == 0; }

} // anonymous namespace


namespace {
TIGHTDB_TABLE_4(TestTableShared,
                first,  Int,
                second, Int,
                third,  Bool,
                fourth, String)


} // anonymous namespace


namespace  {

#define INCREMENTS 10

void* IncrementEntry(void* arg)
{
    try 
    {
        const size_t row_ndx = (size_t)arg;

        // Open shared db
        SharedGroup sg("test_shared.tightdb", 
                       false, SharedGroup::durability_Async );

        for (size_t i = 0; i < INCREMENTS; ++i) {

            // Increment cell
            {

                WriteTransaction wt(sg);
                TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
                t1[row_ndx].first += 1;
                // FIXME: For some reason this takes ages when running
                // inside valgrind, it is probably due to the "extreme
                // overallocation" bug. The 1000 transactions performed
                // here can produce a final database file size of more
                // than 1 GiB. Really! And that is a table with only 10
                // rows. It is about 1 MiB per transaction.
                wt.commit();
            }
            // Verify in new transaction so that we interleave
            // read and write transactions

            {
                ReadTransaction rt(sg);
                TestTableShared::ConstRef t = rt.get_table<TestTableShared>("test");

                const int64_t v = t[row_ndx].first;
                const int64_t expected = i+1;
                CHECK_EQUAL(expected, v);
            }

        }

    } catch (runtime_error e) {
        printf("Thread exiting due to runtime exception\n");
        printf("what(): %s\n", e.what());
    } catch (...) {
        printf("Thread exiting for unknown reason\n");
        printf("\n");
    }
    return 0;
}

} // anonymous namespace

void single_threaded()
{
    printf("Single threaded client\n");
    // Do some changes in a async db
    {
        SharedGroup db("asynctest.tightdb", false, SharedGroup::durability_Async);

        for (size_t n = 0; n < 100; ++n) {
            //printf("t %d\n", (int)n);
            WriteTransaction wt(db);
            TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
            t1->add(1, n, false, "test");
            wt.commit();
        }
    }

    // Wait for async_commit process to shutdown
    while (File::exists("asynctest.tightdb.lock")) {
        usleep(100);
    }
    // Read the db again in normal mode to verify
    {
        SharedGroup db("asynctest.tightdb");

        for (size_t n = 0; n < 100; ++n) {
            ReadTransaction rt(db);
            TestTableShared::ConstRef t1 = rt.get_table<TestTableShared>("test");
            CHECK(t1->size() == 100);
        }
    }
}

void make_table(size_t rows) 
{
    File::try_remove("test_shared.tightdb");
    File::try_remove("test_alone.tightdb");
    // Create first table in group
#if 1
    {
        SharedGroup sg("test_shared.tightdb");
        WriteTransaction wt(sg);
        TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
        for (size_t i = 0; i < rows; ++i) {
            t1->add(0, 2, false, "test");
        }
        wt.commit();
    }
#if 0
#else
    {
        SharedGroup sg("test_shared.tightdb", 
                       false, SharedGroup::durability_Async);
        WriteTransaction wt(sg);
        TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
        for (size_t i = 0; i < rows; ++i) {
            t1->add(0, 2, false, "test");
        }
        wt.commit();
    }
#endif
    // Wait for async_commit process to shutdown
    while (File::exists("test_shared.tightdb.lock")) {
        usleep(100);
    }
#else
    {
        Group g("test_alone.tightdb", Group::mode_ReadWrite);
        TestTableShared::Ref t1 = g.get_table<TestTableShared>("test");
        for (size_t i = 0; i < rows; ++i) {
            t1->add(0, 2, false, "test");
        }
        printf("Writing db\n");
        g.commit();
    }
#endif
}

void multi_threaded(size_t thread_count, size_t base) 
{
    printf("Multithreaded client\n");

    // Do some changes in a async db
    {

        printf("Spawning test threads\n");
        pthread_t* threads = new pthread_t[thread_count];

        // Create all threads
        for (size_t i = 0; i < thread_count; ++i) {
            const int rc = pthread_create(&threads[i], NULL, IncrementEntry, (void*)(i+base));
            CHECK_EQUAL(0, rc);
        }

        // Wait for all threads to complete
        for (size_t i = 0; i < thread_count; ++i) {
            const int rc = pthread_join(threads[i], NULL);
            CHECK_EQUAL(0, rc);
        }

        delete[] threads;
        printf("Threads done, verifying\n");

        // Verify that the changes were made
        {
            SharedGroup sg("test_shared.tightdb", 
                           false, SharedGroup::durability_Async);
            ReadTransaction rt(sg);
            TestTableShared::ConstRef t = rt.get_table<TestTableShared>("test");

            for (size_t i = 0; i < thread_count; ++i) {
                const int64_t v = t[i+base].first;
                CHECK_EQUAL(INCREMENTS, v);
            }
        }
    }
}

void validate_and_clear(size_t rows, int result)
{
    // Wait for async_commit process to shutdown
    while (File::exists("test_shared.tightdb.lock")) {
        usleep(100);
    }
    // Verify - once more, in sync mode - that the changes were made
    {
        printf("Reopening in sync mode and verifying\n");
        SharedGroup sg("test_shared.tightdb");
        WriteTransaction wt(sg);
        TestTableShared::Ref t = wt.get_table<TestTableShared>("test");
        
        for (size_t i = 0; i < rows; ++i) {
            const int64_t v = t[i].first;
            t[i].first = 0;
            CHECK_EQUAL(result, v);
        }
        wt.commit();
    }
}

void multi_process(int numprocs, size_t numthreads) 
{
    for (int i=0; i<numprocs; i++) {
        if (fork()==0) {
            fprintf(stderr, "Forked!\n");
            multi_threaded(numthreads, i*numthreads);
            exit(0);
        }
    }
    int status = 0;
    for (int i=0; i<numprocs; i++) wait(&status);
    fprintf(stderr,"Joined\n");
}

int main()
{
    // wait for any daemon hanging around to exit
    File::try_remove("test_shared.tightdb.lock");
    File::try_remove("asynctest.tightdb.lock");
    usleep(100);
    // Clean up old state
    File::try_remove("asynctest.tightdb");

    single_threaded();

    make_table(1);
/*
    multi_threaded(10,0);
    validate_and_clear(10, INCREMENTS);

    for (int k=1; k<10; k++) {
        fprintf(stderr,"Spawning processes\n");
        multi_process(10,10);
        validate_and_clear(100,INCREMENTS);
    }
*/
}
