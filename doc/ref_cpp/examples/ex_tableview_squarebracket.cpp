// @@Example: ex_cpp_tableview_squarebracket @@
// @@Fold@@
#include <tightdb.hpp>
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
    int64_t age = view[2].age;
    assert(age == 32);
// @@Fold@@
}
// @@EndFold@@
// @@EndExample@@
