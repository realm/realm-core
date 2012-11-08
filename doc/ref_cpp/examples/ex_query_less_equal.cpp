// @@Example: ex_cpp_query_lessThanOrEqual @@
// @@Fold@@
#include <tightdb.hpp>

TIGHTDB_TABLE_2(PeopleTable,
                name,  String,
                age,   Int)

int main()
{
    PeopleTable table;

    table.add("Mary", 14);
    table.add("Joe",  40);
    table.add("Jack", 41);
    table.add("Jill", 37);

// @@EndFold@@
    // Find rows where age <= 37
    PeopleTable::View view1 = table.where().age.less_equal(37).find_all(table);
// @@Fold@@

    assert(view1.size() == 2);
    assert(view1[0].name == "Mary");
    assert(view1[1].name == "Jill");
}
// @@EndFold@@
// @@EndExample@@
