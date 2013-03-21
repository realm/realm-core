#include <cstring>
#include <iostream>

#include <tightdb/column.hpp>
#include <tightdb.hpp>
#include <tightdb/group_shared.hpp>

#include <UnitTest++.h>

/*
#define CHECK(v) do { if (v) break; cerr << __LINE__ << ": CHECK failed" << endl; } while(false)
#define CHECK_EQUAL(a, b) do { if (check_equal((a),(b))) break; cerr << __LINE__ << ": CHECK_EQUAL failed: " << (a) << " vs " << (b) << endl; } while(false)
#define CHECK_THROW(v, e) do { try { v; } catch (e&) { break; } cerr << __LINE__ << ": CHECK_THROW failed: Expected " # e << endl; } while(false)
*/

using namespace tightdb;
using namespace std;


namespace {

/*
template<class A, class B> inline bool check_equal(const A& a, const B& b) { return a == b; }
inline bool check_equal(const char* a, const char* b) { return strcmp(a, b) == 0; }
*/

} // anonymous namespace


namespace {

TIGHTDB_TABLE_5(GATable,
                    user_id, String,
                    country, String,
                    build,   String,
                    event_1, Int,
                    event_2, Int)

} // anonymous namespace


int main()
{
    UnitTest::Timer timer;

    {
        Group g;
        GATable::Ref t = g.get_table<GATable>("firstevents");

        for (size_t i = 0; i < 100000; ++i) {
            const int64_t r1 = rand() % 1000;
            const int64_t r2 = rand() % 1000;

//            t->add("10", "US", "1.0", r1, r2);

            if (i%2 == 0) {
                t->add("10", "US", "1.0", r1, r2);
            }
            else {
                t->add("10", "DK", "1.0", r1, r2);
            }
        }
//        t->optimize();
        g.write("ga_test.tightdb");
    }

    Group g("ga_test.tightdb");
    GATable::Ref t = g.get_table<GATable>("firstevents");

    timer.Start();
    size_t c1 = 0;
    for (size_t i = 0; i < 10000; ++i) {
        c1 += t->column().country.count("US");
    }
    const int s1 = timer.GetTimeInMs();
    cout << "search time 1: " << s1 << " : " << c1 << endl;

    timer.Start();
    GATable::Query q = t->where().country.equal("US");
    size_t c2 = 0;
    for (size_t i = 0; i < 1000; ++i) {
        c2 += q.count();
    }
    const int s2 = timer.GetTimeInMs();
    cout << "search time 2: " << s2 << " : " << c2 << endl;
}
