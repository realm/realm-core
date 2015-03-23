// @@Example: ex_cpp_group_optional_table @@
// @@Fold@@
#include <cassert>
#include <iostream>
#include <tightdb.hpp>

using namespace std;
using namespace realm;

REALM_TABLE_2(PeopleTable,
                name, String,
                age, Int)

void optional_table(const Group& group)
{
// @@EndFold@@
    PeopleTable::ConstRef table = group.get_table<PeopleTable>("people");
    if (table)
        cout << table->get_column_count() << "\n";
// @@Fold@@
}

int main()
{
    Group group;

    PeopleTable::Ref table = group.add_table<PeopleTable>("people");
    table->add("Mary", 14);
    table->add("Joe", 17);
    table->add("Jack", 22);

    optional_table(group);
}
// @@EndFold@@
// @@EndExample@@
