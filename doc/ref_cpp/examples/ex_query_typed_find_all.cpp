// @@Example: ex_cpp_typed_query_findall @@
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
    table.add("Mary",  34);
    table.add("Joe",   37);
    table.add("Alice", 12);
    table.add("Jack",  75);
    table.add("Bob",   10);
    table.add("Peter", 40);

    // For rows where age >= 18
    PeopleTable::Query query = table.where().age.greater_equal(18);

    // Find all matching rows of entire table
    PeopleTable::View view1 = query.find_all();
// @@Fold@@

    assert(view1.size() == 4);
    assert(view1[0].name == "Mary");
    assert(view1[1].name == "Joe");
    assert(view1[2].name == "Jack");
    assert(view1[3].name == "Peter");
// @@EndFold@@

    // Find matches between 2'nd (Joe) and 4'th (Jack) row, both inclusive.
    PeopleTable::View view2 = query.find_all(1, 4);

// @@Fold@@
    assert(view2.size() == 2);
    assert(view2[0].name == "Joe");
    assert(view2[1].name == "Jack");

// @@EndFold@@
    // Find first 2 matches of table
    size_t start = 0;
    PeopleTable::View view3 = query.find_all(start, size_t(-1), 2);

// @@Fold@@
    assert(view3.size() == 2);
    assert(view3[0].name == "Mary");
    assert(view3[1].name == "Joe");

// @@EndFold@@
    // Find next 2 matches of table
    start = view3.get_source_ndx(view3.size() - 1) + 1; // start = 1 + 1 = 2
    PeopleTable::View view4 = query.find_all(start, size_t(-1), 2);

// @@Fold@@
    assert(view4.size() == 2);
    assert(view4[0].name == "Jack");
    assert(view4[1].name == "Peter");
}
// @@EndFold@@
// @@EndExample@@
