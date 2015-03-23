// @@Example: ex_cpp_typed_query_min @@
// @@Fold@@
#include <tightdb.hpp>
#include <assert.h>

REALM_TABLE_3(PeopleTable,
                name, String,
                age, Int,
                weight, Int)

int main()
{
    PeopleTable table;

// @@EndFold@@
    table.add("Mary", 14, 35);  // match
    table.add("Joe",  17, 40);  // match
    table.add("Jack", 22, 41);
    table.add("Jill", 21, 37);

    // Calculate sum of weight where age >= 13 && age <= 19
    PeopleTable::Query query = table.where().age.between(13, 19);
    size_t matchcount;
    int64_t weight = query.weight.minimum(&matchcount);
// @@Fold@@

    // Verify that matchcount > 0 for the return value to make sense
    assert(matchcount == 2);

    // Expected result
    assert(weight == 35);

}
// @@EndFold@@
// @@EndExample@@
