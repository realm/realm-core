// @@Example: ex_cpp_tableview_max @@
// @@Fold@@
#include <tightdb.hpp>
#include <assert.h>

REALM_TABLE_3(PeopleTable,
                name, String,
                age, Int,
                weight, Int)

int main()
{
    PeopleTable table;

// @@EndFold@@
    table.add("Joe",  17, 50);  // match
    table.add("Mary", 14, 60);
    table.add("Jack", 22, 70);  // match
    table.add("Jill", 21, 80);  // match

    PeopleTable::View view = table.where().age.greater(15).find_all();

    int64_t heaviest = view.column().weight.maximum();
// @@Fold@@
    assert(heaviest == 80);
}
// @@EndFold@@
// @@EndExample@@
