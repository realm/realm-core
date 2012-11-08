// @@Example: ex_cpp_tableview_constructor @@
#include <tightdb.hpp>

TIGHTDB_TABLE_2(PeopleTable,
                name, String,
                age, Int)

int main()
{
    PeopleTable table;

    table.add("Joe",   57);
    table.add("Mary",  14);
    table.add("Alice", 42);
    table.add("Jack",  32); 

// @@Fold@@
    // Create 1-to-1 view of table
    PeopleTable::View view = table.where().find_all(table);
// @@EndFold@@
    assert(view.size() == 4);
}
// @@EndExample@@
