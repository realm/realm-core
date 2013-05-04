// @@Example: ex_cpp_group_traverse @@
// @@Fold@@
#include <iostream>
#include <tightdb.hpp>

using namespace std;
using namespace tightdb;

TIGHTDB_TABLE_2(PeopleTable,
                name, String,
                age, Int)

void traverse(const Group& group)
{
// @@EndFold@@
    for (size_t i=0; i<group.size(); ++i) {
        StringData table_name = group.get_table_name(i);
        ConstTableRef table = group.get_table(table_name);
        cout << table_name << " " << table->get_column_count() << "\n";
    }
// @@Fold@@
}

int main()
{
    Group group;

    PeopleTable::Ref table = group.get_table<PeopleTable>("people");
    table->add("Mary", 14);
    table->add("Joe", 17);
    table->add("Jack", 22);

    traverse(group);
}
// @@EndFold@@
// @@EndExample@@
