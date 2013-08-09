#include <cstring>
#include <fstream>
#include <iostream>

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


int main()
{
    // Clean up old state
    File::try_remove("asynctest.tightdb");
    File::try_remove("asynctest.tightdb.lock");

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
        sleep(1);
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
