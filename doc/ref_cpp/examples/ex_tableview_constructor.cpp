// @@Example: ex_cpp_tableview_constructor @@
// @@Fold@@
#include <realm.hpp>
#include <assert.h>

REALM_TABLE_2(PeopleTable,
                name, String,
                age, Int)

int main()
{
    PeopleTable table;

// @@EndFold@@
    table.add("Joe",   57);
    table.add("Mary",  14);
    table.add("Alice", 42);
    table.add("Jack",  32);

    // Create 1-to-1 view of table
    PeopleTable::View view = table.where().find_all();
    assert(view.size() == 4);
// @@Fold@@
}
// @@EndFold@@
// @@EndExample@@
