// @@Example: ex_cpp_typed_query_greaterThan @@
// @@Fold@@
#include <tightdb.hpp>
#include <assert.h>

REALM_TABLE_2(PeopleTable,
                name,  String,
                age,   Int)

int main()
{
    PeopleTable table;

// @@EndFold@@
    table.add("Mary", 14);
    table.add("Joe",  40);
    table.add("Jack", 41);
    table.add("Jill", 37);

    // Find rows where age > 37
    PeopleTable::View view1 = table.where().age.greater(37).find_all();
// @@Fold@@
    assert(view1.size() == 2);
    assert(view1[0].name == "Joe");
    assert(view1[1].name == "Jack");
}
// @@EndExample@@
// @@EndFold@@
