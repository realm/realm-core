// @@Example: ex_cpp_query_delete @@
// @@Fold@@
#include <tightdb.hpp>

TIGHTDB_TABLE_2(PeopleTable,
                name, String,
                age, Int)

int main()
{
    PeopleTable table;

    table.add("Mary",  14);  // match
    table.add("Joe",   17);  // match
    table.add("Alice", 42);     
    table.add("Jack",  22);  // match
    table.add("Bob",   50);
    table.add("Frank", 12);  // match
// @@EndFold@@
    // Delete rows where age >= 13 && age <= 20
    PeopleTable::Query query = table.where().age.between(13, 20);
    size_t removed = query.remove(table);
// @@Fold@@

    // 4 rows deleted
    assert(removed == 4);

    // 2 rows left
    assert(table.size() == 2);
}
// @@EndExample@@
// @@EndFold@@
