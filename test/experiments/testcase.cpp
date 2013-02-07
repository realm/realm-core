#include <cstring>
#include <iostream>
#include <tightdb.hpp>
#include <tightdb/group_shared.hpp>

#define CHECK(v) do { if (v) break; cerr << __LINE__ << ": CHECK failed" << endl; } while(false)
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

void* IncrementEntry(void* arg)
{
    const size_t row_ndx = (size_t)arg;

    // Open shared db
    SharedGroup sg("test_shared.tightdb");

    for (size_t i = 0; i < 100; ++i) {
        // Increment cell
        {
            WriteTransaction wt(sg);
            TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
            t1[row_ndx].first += 1;
            // FIXME: For some reason this takes ages when running
            // inside valgrind, it is probably due to the "extreme
            // overallocation" bug. 100 transactions as simple as this
            // one can produce a final database file size of more than
            // 100 MiB.
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
cerr << ".";
    }
    return 0;
}

} // anonymous namespace


int main()
{
    // Delete old files if there
    remove("test_shared.tightdb");
    remove("test_shared.tightdb.lock"); // also the info file

    {
        // Create a new shared db
        SharedGroup sg("test_shared.tightdb");

        const size_t thread_count = 10;

        // Create first table in group
        {
            WriteTransaction wt(sg);
            TestTableShared::Ref t1 = wt.get_table<TestTableShared>("test");
            for (size_t i = 0; i < thread_count; ++i) {
                t1->add(0, 2, false, "test");
            }
            wt.commit();
        }

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

        // Verify that the changes were made
        {
            ReadTransaction rt(sg);
            TestTableShared::ConstRef t = rt.get_table<TestTableShared>("test");

            for (size_t i = 0; i < thread_count; ++i) {
                const int64_t v = t[i].first;
                CHECK_EQUAL(100, v);
            }
        }
    }

    return 0;
}
