// @@Example: ex_cpp_query_or @@
// @@Fold@@
#include <tightdb.hpp>

TIGHTDB_TABLE_2(PeopleTable,
                name, String,
                age, Int)

int main()
{
    PeopleTable table;

    table.add("Mary", 14); // Match
    table.add("Joe",  17); // Match
    table.add("Jack", 22);

// @@EndFold@@
    // Find rows where age == 14 || age == 17
    PeopleTable::Query query = table.where().age.equal(14).Or().age.equal(17);
    PeopleTable::View view = query.find_all(table);
// @@Fold@@

    // Expected result
    assert(view.size() == 2);
    assert(view[0].name == "Mary");
    assert(view[1].name == "Joe");
}
// @@Fold@@
// @@EndExample@@