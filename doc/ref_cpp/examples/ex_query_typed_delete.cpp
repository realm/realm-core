// @@Example: ex_cpp_typed_query_remove @@
// @@Fold@@
#include <realm.hpp>
#include <assert.h>

REALM_TABLE_2(PeopleTable,
                name, String,
                age, Int)

int main()
{
    PeopleTable table;

// @@EndFold@@
    table.add("Mary",  13);  // match
    table.add("Joe",   20);  // match
    table.add("Alice", 42);
    table.add("Jack",  17);  // match
    table.add("Bob",   50);
    table.add("Frank", 18);  // match

    // Delete rows where age >= 13 && age <= 20
    PeopleTable::Query query = table.where().age.between(13, 20);
    size_t removed = query.remove();
// @@Fold@@

    // 4 rows deleted
    assert(removed == 4);

    // 2 rows left
    assert(table.size() == 2);
}
// @@EndExample@@
// @@EndFold@@
