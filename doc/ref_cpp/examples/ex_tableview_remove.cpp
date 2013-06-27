// @@Example: ex_cpp_tableview_clear @@
// @@Fold@@
#include <tightdb.hpp>
#include <assert.h>

TIGHTDB_TABLE_2(PeopleTable,
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

    assert(!strcmp(view[1].name.data(), "Alice"));

    view.remove(1);

    assert(!strcmp(view[1].name.data(), "Jack"));
    assert(table.size() == 3);
// @@Fold@@
}
// @@EndFold@@
// @@EndExample@@
