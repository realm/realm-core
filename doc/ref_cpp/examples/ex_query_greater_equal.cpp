// @@Example: ex_cpp_query_greaterThanOrEqual @@
// @@Fold@@
#include <tightdb.hpp>

TIGHTDB_TABLE_2(PeopleTable,
                name,  String,
                age,   Int)

int main()
{
    PeopleTable table;

    table.add("Mary", 14);
    table.add("Joe",  40);  // match
    table.add("Jack", 41);  // match
    table.add("Jill", 37);
// @@EndFold@@
    // Find rows where age >= 40
    PeopleTable::View view1 = table.where().age.greater_equal(40).find_all(table);
// @@Fold@@
    assert(view1.size() == 2);
    assert(view1[0].name == "Joe");
    assert(view1[1].name == "Jack");
}
// @@EndFold@@
// @@EndExample@@