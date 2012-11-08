// @@Example: ex_cpp_query_constructor @@
#include <tightdb.hpp>

TIGHTDB_TABLE_2(PeopleTable,
                name, String,
                age, Int)

void main()
{
    PeopleTable table;

    table.add("Mary", 14);      // Match
    table.add("Joe", 17);       // Match
    table.add("Jack", 22);      // Match

    // Create selection query with no criterias yet (will match all rows)
    PeopleTable::Query query = table.where();

    // Expected result
    assert(query.count(table) == 3);
}
// @@EndExample@@