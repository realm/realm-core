#include <cstring>
#include <iostream>

#include <tightdb/column.hpp>
#include <tightdb.hpp>
#include <tightdb/group_shared.hpp>
#include <tightdb/column_string.hpp>
#include <tightdb/column_string_enum.hpp>
#include <tightdb/array_string_long.hpp>

//#include <UnitTest++.h>

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

TIGHTDB_TABLE_2(TupleTableType,
                first,  Int,
                second, String)

} // anonymous namespace


int main()
{
    TupleTableType ttt;

    ttt.add(1, "BLAAbaergroed");
    ttt.add(1, "BLAAbaergroedandMORE");
    ttt.add(1, "BLAAbaergroed2");

    TupleTableType::Query q1 = ttt.where().second.equal("blaabaerGROED", false);
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(1, tv1.size());
    CHECK_EQUAL(0, tv1.get_source_ndx(0));
}
