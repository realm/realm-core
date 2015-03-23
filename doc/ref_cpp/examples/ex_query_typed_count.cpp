// @@Example: ex_cpp_typed_query_count @@
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
    table.add("Mary",  14);
    table.add("Joe",   17);
    table.add("Alice", 42);
    table.add("Jack",  22);
    table.add("Bob",   50);
    table.add("Frank", 12);

    // Select rows where age < 18
    PeopleTable::Query query = table.where().age.less(18);

    // Count all matching rows of entire table
    size_t count1 = query.count();
    assert(count1 == 3);

    // Very fast way to test if there are at least 2 matches in the table
    size_t count2 = query.count(0, size_t(-1), 2);
    assert(count2 == 2);

    // Count matches in latest 3 rows of the table
    size_t count3 = query.count(table.size() - 3, table.size());
    assert(count3 == 1);
// @@Fold@@
}
// @@EndFold@@
// @@EndExample@@
