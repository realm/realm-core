#include <tightdb.hpp>

using namespace tightdb;

TIGHTDB_TABLE_1(TestTable,
                value, Int)

int main()
{
    Group db;
    TestTable::Ref t = db.get_table<TestTable>("test");
    return t ? 0 : 1;
}
