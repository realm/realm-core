#include <iostream>
#include <tightdb.hpp>
#include <tightdb/group_shared.hpp>
#include <tightdb/lang_bind_helper.hpp>

using namespace tightdb;
using namespace std;

namespace {

#define CHECK(v) if (!(v)) cerr << __LINE__ << ": CHECK failed" << endl
#define CHECK_EQUAL(a, b) if ((a)!=(b)) cerr << __LINE__ << ": CHECK_EQUAL failed: " << (a) << " vs " << (b) << endl

enum Days {
    Mon,
    Tue,
    Wed,
    Thu,
    Fri,
    Sat,
    Sun
};

TIGHTDB_TABLE_4(TestTableGroup,
                first,  String,
                second, Int,
                third,  Bool,
                fourth, Enum<Days>)

TIGHTDB_TABLE_1(TestTableGroup2,
                second, Subtable<TestTableGroup>)

} // namespace

int main()
{
    Table* table = LangBindHelper::new_table();
    CHECK(table->is_valid());
    LangBindHelper::unbind_table_ref(table);

    return 0;
}
