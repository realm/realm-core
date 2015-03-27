// @@Example: ex_cpp_tableview_clear @@
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
    table.add("Joe",   57); // match
    table.add("Mary",  14);
    table.add("Alice", 42); // match
    table.add("Jack",  32); // match

    PeopleTable::View view = table.where().age.greater(18).find_all();
    view.clear();
// @@Fold@@
    assert(view.size() == 0);
    assert(table.size() == 1);
}
// @@EndFold@@
// @@EndExample@@
