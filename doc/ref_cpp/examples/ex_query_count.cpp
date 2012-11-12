// @@Example: ex_cpp_query_count @@
// @@Fold@@
#include <tightdb.hpp>

TIGHTDB_TABLE_2(PeopleTable,
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
    size_t count1 = query.count(table);
// @@Fold@@
    assert(count1 == 3);
// @@EndFold@@

    // Very fast way to test if there are at least 2 matches in the table
    size_t count2 = query.count(table, 0, size_t(-1), 2);                
// @@Fold@@
    assert(count2 == 2);
// @@EndFold@@

    // Count matches in latest 3 rows                             
    size_t count3 = query.count(table, table.size() - 3, table.size());  
// @@Fold@@
    assert(count3 == 1);
}
// @@EndFold@@
// @@EndExample@@