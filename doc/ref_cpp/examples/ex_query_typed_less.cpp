// @@Example: ex_cpp_typed_query_lessThan @@
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
    table.add("Mary", 14);  // match
    table.add("Joe",  40);
    table.add("Jack", 41);
    table.add("Jill", 37);

    // Find rows where age < 20
    PeopleTable::View view1 = table.where().age.less(20).find_all();
// @@Fold@@
    assert(view1.size() == 1);
    assert(view1[0].name == "Mary");
}
// @@Fold@@
// @@EndExample@@
