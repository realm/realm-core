// @@Example: ex_cpp_typed_query_avg @@
// @@Fold@@
#include <realm.hpp>
#include <assert.h>

REALM_TABLE_3(PeopleTable,
                name, String,
                age, Int,
                weight, Int)

int main()
{
    PeopleTable table;

// @@EndFold@@
    table.add("Joe",  17, 50);
    table.add("Jack", 22, 60);
    table.add("Mary", 14, 70);
    table.add("Jill", 21, 80);

    // Find average weight for rows where age >= 13 && age <= 20
    PeopleTable::Query query1 = table.where().age.between(13, 20);
    double avg1 = query1.weight.average();
// @@Fold@@
    assert(avg1 == (50.0 + 70.0) / 2.0);
// @@EndFold@@

    // Find average weight of 2'nd to 3'rd row of table, both inclusive
    PeopleTable::Query query2 = table.where();
    double avg2 = query2.weight.average(NULL, 1, 3);
// @@Fold@@

    assert(avg2 == (60.0 + 70.0) / 2.0);
}
// @@EndFold@@
// @@EndExample@@
