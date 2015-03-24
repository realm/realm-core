// @@Example: ex_cpp_group_traverse @@
// @@Fold@@
#include <iostream>
#include <realm.hpp>

using namespace std;
using namespace realm;

REALM_TABLE_2(PeopleTable,
                name, String,
                age, Int)

void traverse(const Group& group)
{
// @@EndFold@@
    if (!group.is_empty()) {
        cout << "Tables in group and number of columns in them:" << endl;
        size_t n = group.size();
        for (size_t i = 0; i < n; ++i) {
            StringData table_name = group.get_table_name(i);
            ConstTableRef table = group.get_table(table_name);
            cout << table_name << " " << table->get_column_count() << "\n";
        }
        cout << "End of group contents" << endl;
    }
    else {
        cout << "Group is empty" << endl;
    }
// @@Fold@@
}

int main()
{
    Group group;

    PeopleTable::Ref table = group.add_table<PeopleTable>("people");
    table->add("Mary", 14);
    table->add("Joe", 17);
    table->add("Jack", 22);

    traverse(group);
}
// @@EndFold@@
// @@EndExample@@
