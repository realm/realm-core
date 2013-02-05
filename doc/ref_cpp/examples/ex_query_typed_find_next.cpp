// @@Example: ex_cpp_typed_query_find_next @@
// @@Fold@@
#include <tightdb.hpp>

TIGHTDB_TABLE_2(PeopleTable,
                name, String,
                age, Int)

int main()
{
    PeopleTable table;

// @@EndFold@@
    table.add("Mary",  34);  // match
    table.add("Joe",   37);  // match
    table.add("Alice", 12);
    table.add("Jack",  75);  // match

    size_t m;
    PeopleTable::Query query = table.where().age.greater_equal(18);

    // Find first matching row
    m = query.find_next(size_t(-1));
    assert(m == 0);

    // Find successive matches
    m = query.find_next(m);
    assert(m == 1);

    m = query.find_next(m);
    assert(m == 3);

    // No more matches
    m = query.find_next(m);
    assert(m == size_t(-1));
// @@Fold@@
}
// @@EndFold@@
// @@EndExample@@
