// @@Example: ex_cpp_tableview_max @@
#include <tightdb.hpp>

TIGHTDB_TABLE_3(PeopleTable,
                name, String,
                age, Int,
                weight, Int)

void main()
{
    PeopleTable table;

// @@Fold@@
    table.add("Joe",  17, 50);  // match
    table.add("Mary", 14, 60);
    table.add("Jack", 22, 70);  // match
    table.add("Jill", 21, 80);  // match

    PeopleTable::View view = table.where().age.greater(15).find_all(table);
    
    int64_t heaviest = view.column().weight.maximum();
    assert(heaviest == 80);
// @@EndFold@@
}
// @@EndExample@@
