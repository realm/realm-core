#include <cstring>
#include <fstream>
#include <iostream>
#include <unistd.h>

#include <tightdb/column.hpp>
#include <tightdb.hpp>
#include <tightdb/group_shared.hpp>
#include <tightdb/column_string.hpp>
#include <tightdb/column_string_enum.hpp>
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

void* IncrementEntry(void* arg)
{
    try 
    {
        const size_t row_ndx = (size_t)arg;

/*
        // Open shared db
        SharedGroup sg("test_shared.tightdb", 
                       false, SharedGroup::durability_Async);
        for (size_t i = 0; i < 100; ++i) {

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
*/
    } catch (runtime_error e) {
        printf("Thread exiting due to runtime exception\n");
        printf("what(): %s\n", e.what());
        sleep(1);
        exit(1);
    } catch (...) {
        printf("Thread exiting for unknown reason\n");
        printf("\n");
    }
    printf("thread done\n");
    sleep(1);
    printf("thread returning 0\n");
    return 0;
}

} // anonymous namespace

void single_threaded()
{
    // Clean up old state
    File::try_remove("asynctest.tightdb");
    File::try_remove("asynctest.tightdb.lock");
    // wait for daemon to exit
    sleep(1);
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

    File::try_remove("asynctest.tightdb.lock");
    sleep(1);
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

void multi_threaded() 
{
    // Clean up old state
    File::try_remove("test_shared.tightdb");
    File::try_remove("test_shared.tightdb.lock");
    sleep(1);
    printf("Multithreaded client\n");
    const size_t thread_count = 2;

    // Do some changes in a async db
    {
/*
        SharedGroup sg("test_shared.tightdb", 
                       false, SharedGroup::durability_Async);
        // Create first table in group
        {
            WriteTransaction wt(sg);
            TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
            for (size_t i = 0; i < thread_count; ++i) {
                t1->add(0, 2, false, "test");
            }
            wt.commit();
        }
*/
        printf("Spawning test threads\n");
        pthread_t threads[thread_count];

        // Create all threads
        for (size_t i = 0; i < thread_count; ++i) {
            const int rc = pthread_create(&threads[i], NULL, IncrementEntry, (void*)i);
            CHECK_EQUAL(0, rc);
        }

        // Wait for all threads to complete
        for (size_t i = 0; i < thread_count; ++i) {
            const int rc = pthread_join(threads[i], NULL);
            CHECK_EQUAL(0, rc);
        }
        printf("Threads done, verifying\n");
/*
        // Verify that the changes were made
        {
            ReadTransaction rt(sg);
            TestTableShared::ConstRef t = rt.get_table<TestTableShared>("test");

            for (size_t i = 0; i < thread_count; ++i) {
                const int64_t v = t[i].first;
                CHECK_EQUAL(100, v);
            }
        }
*/
    }
    sleep(1);
    File::try_remove("test_shared.tightdb.lock");
/*
    sleep(1);
    // Verify - once more, in sync mode - that the changes were made
    {
        printf("Reopening in sync mode and verifying\n");
        SharedGroup sg("test_shared.tightdb");
        ReadTransaction rt(sg);
        TestTableShared::ConstRef t = rt.get_table<TestTableShared>("test");
        
        for (size_t i = 0; i < thread_count; ++i) {
            const int64_t v = t[i].first;
            CHECK_EQUAL(100, v);
        }
    }
    File::try_remove("test_shared.tightdb.lock");
*/

}


int main()
{
    // single_threaded();

    multi_threaded();
}
