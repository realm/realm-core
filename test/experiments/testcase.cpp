#include <cstring>
#include <iostream>
#include <tightdb.hpp>
#include <tightdb/group_shared.hpp>

#define CHECK(v) if (!(v)) cerr << __LINE__ << ": CHECK failed" << endl
#define CHECK_EQUAL(a, b) if (!check_equal((a),(b))) cerr << __LINE__ << ": CHECK_EQUAL failed: " << (a) << " vs " << (b) << endl

using namespace tightdb;
using namespace std;


namespace {

template<class A, class B> inline bool check_equal(const A& a, const B& b) { return a == b; }
inline bool check_equal(const char* a, const char* b) { return strcmp(a, b) == 0; }

} // anonymous namespace


namespace {

TIGHTDB_TABLE_1(TestTableShared,
                first,  Int)

} // anonymous namespace


int main()
{
    // Delete old files if there
    remove("test_shared.tightdb");
    remove("test_shared.tightdb.lock"); // also the info file

    {
        // Create a new shared db
        SharedGroup shared("test_shared.tightdb");
        CHECK(shared.is_valid());

        {
            // Open the same db again (in empty state)
            SharedGroup shared2("test_shared.tightdb");
            CHECK(shared2.is_valid());

            // Add a new table
            {
                Group& g = shared2.begin_write();
                TestTableShared::Ref t = g.get_table<TestTableShared>("test");
            }
            shared2.commit();
        }
    }

    return 0;
}
