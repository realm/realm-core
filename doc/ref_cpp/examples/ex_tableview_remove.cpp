// @@Example: ex_cpp_tableview_clear @@
#include <tightdb.hpp>

TIGHTDB_TABLE_2(PeopleTable,
                name, String,
                age, Int)

int main()
{
    PeopleTable table;

// @@Fold@@
    table.add("Joe",   57); // match
    table.add("Mary",  14); 
    table.add("Alice", 42); // match
    table.add("Jack",  32); // match

    PeopleTable::View view = table.where().age.greater(18).find_all(table);

    assert(view[1].name == "Alice");

    view.remove(1);

    assert(view[1].name == "Jack");
    assert(table.size() == 3);
// @@EndFold@@
}
// @@EndExample@@
