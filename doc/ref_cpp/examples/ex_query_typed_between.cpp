// @@Example: ex_cpp_typed_query_between @@
// @@Fold@@
#include <tightdb.hpp>
#include <assert.h>

REALM_TABLE_3(PeopleTable,
                name,  String,
                age,   Int,
                hired, DateTime)

int main()
{
    PeopleTable table;

// @@EndFold@@
    table.add("Mary", 59, tightdb::DateTime(1998,  6, 14));
    table.add("Joe",  40, tightdb::DateTime(2010,  4, 24));
    table.add("Jack", 41, tightdb::DateTime(2012, 10,  5));
    table.add("Jill", 37, tightdb::DateTime(2006,  7,  1));

    // Find rows where age <= 37 && age >= 40
    PeopleTable::View view1 = table.where().age.between(37, 40).find_all();

// @@Fold@@
    assert(view1.size() == 2);
    assert(view1[0].name == "Joe");
    assert(view1[1].name == "Jill");

// @@EndFold@@
    // Find people where hired year == 2012 using a 'between' clause
    PeopleTable::View view5 = table.where().hired.between(tightdb::DateTime(2012,  1,  1,  0,  0,  0).get_datetime(),
                                                          tightdb::DateTime(2012, 12, 31, 23, 59, 59).get_datetime()).find_all();
// @@Fold@@
    assert(view5.size() == 1 && view5[0].name == "Jack");
}
// @@EndFold@@
// @@EndExample@@
