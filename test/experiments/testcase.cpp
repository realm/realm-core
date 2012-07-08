#include <iostream>
#include <tightdb.hpp>
#include <tightdb/group_shared.hpp>

using namespace tightdb;
using namespace std;

namespace {

#define CHECK(v) if (!(v)) cerr << __LINE__ << ": CHECK failed" << endl
#define CHECK_EQUAL(a, b) if ((a)!=(b)) cerr << __LINE__ << ": CHECK_EQUAL failed: " << (a) << " vs " << (b) << endl

} // namespace

int main()
{
    // Create table with all column types
    Table table;
    Spec& s = table.get_spec();
    s.add_column(COLUMN_TYPE_STRING, "string");
    table.update_from_spec();

    // Add some rows
    for (size_t i = 0; i < 5; ++i) {
        table.insert_string(0, i, "s");
        table.insert_done();
    }

    // Test Clear
    table.clear();

    return 0;
}
