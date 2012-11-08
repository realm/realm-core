// @@Example: ex_cpp_query_between @@
// @@Fold@@
#include <tightdb.hpp>

TIGHTDB_TABLE_3(PeopleTable,
                name,  String,
                age,   Int,
                hired, Date)

int main()
{
    PeopleTable table;

// @@EndFold@@
    table.add("Mary", 59, tightdb::Date(1998,  6, 14));
    table.add("Joe",  40, tightdb::Date(2010,  4, 24));
    table.add("Jack", 41, tightdb::Date(2012, 10,  5));
    table.add("Jill", 37, tightdb::Date(2006,  7,  1));

    // Find rows where age <= 37 && age >= 40
    PeopleTable::View view1 = table.where().age.between(37, 40).find_all(table);

// @@Fold@@
    assert(view1.size() == 2);
    assert(view1[0].name == "Joe");
    assert(view1[1].name == "Jill");

// @@EndFold@@
    // Find people where hired year == 2012 using a 'between' clause
    PeopleTable::View view5 = table.where().hired.between(tightdb::Date(2012,  1,  1,  0,  0,  0).get_date(), 
                                                          tightdb::Date(2012, 12, 31, 23, 59, 59).get_date()).find_all(table); 
// @@Fold@@
    assert(view5.size() == 1 && view5[0].name == "Jack");
}
// @@EndFold@@
// @@EndExample@@