// @@Example: ex_cpp_tableview_get_source_rowid @@
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

    // Select rows where age > 18
    PeopleTable::View view = table.where().age.greater(18).find_all(table);
    assert(view.get_source_ndx(0) == 0);
    assert(view.get_source_ndx(1) == 2);
    assert(view.get_source_ndx(2) == 3);
// @@EndFold@@
}
// @@EndExample@@
