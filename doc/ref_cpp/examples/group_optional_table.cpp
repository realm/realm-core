// @@Example: ex_cpp_group_optional_table @@
// @@Fold@@
#include <iostream>
#include <tightdb.hpp>

using namespace std;
using namespace tightdb;

TIGHTDB_TABLE_2(PeopleTable,
                name, String,
                age, Int)

void optional_table(const Group& group)
{
// @@EndFold@@
    assert(group.has_table<PeopleTable>("people") || !group.has_table("people"));
    PeopleTable::ConstRef table = g.get_table<PeopleTable>("people");
    cout << table->get_column_count() << "\n";
// @@Fold@@
}

int main()
{
    Group group;

    PeopleTable::Ref table = group.get_table<PeopleTable>("people");
    table->add("Mary", 14);
    table->add("Joe", 17);
    table->add("Jack", 22);

    optional_table(group);
}
// @@EndFold@@
// @@EndExample@@
