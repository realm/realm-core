// @@Example: ex_cpp_typed_query_equals @@
// @@Fold@@
#include <tightdb.hpp>
#include <assert.h>

REALM_TABLE_5(PeopleTable,
                name,  String,
                age,   Int,
                male,  Bool,
                hired, DateTime,
                photo, Binary)

int main()
{
    PeopleTable table;

// @@EndFold@@
    table.add("Mary",  28, false, realm::DateTime(2012,  1, 24), realm::BinaryData("bin \0 data 1", 12));
    table.add("Frank", 56, true,  realm::DateTime(2008,  4, 15), realm::BinaryData("bin \0 data 2", 12));
    table.add("Bob",   24, true,  realm::DateTime(2010, 12,  1), realm::BinaryData("bin \0 data 3", 12));

    // Find rows where age == 56
    PeopleTable::View view1 = table.where().age.equal(56).find_all();
// @@Fold@@
    assert(view1.size() == 1 && view1[0].name == "Frank");
// @@EndFold@@

    // Find rows where name == "Frank"
    PeopleTable::View view2 = table.where().name.equal("Frank").find_all();
// @@Fold@@
    assert(view2.size() == 1 && view2[0].name == "Frank");
// @@EndFold@@
    // Find rows where male == true
    PeopleTable::View view3 = table.where().male.equal(true).find_all();
// @@Fold@@
    assert(view3.size() == 2 && view3[0].name == "Frank" && view3[1].name == "Bob");
// @@EndFold@@

    // Find people hired 2012-Jan-24, 00:00:00
    PeopleTable::View view4 = table.where().hired.equal(realm::DateTime(2012, 1, 24).get_datetime()).find_all();
// @@Fold@@
    assert(view4.size() == 1 && view4[0].name == "Mary");

// @@EndFold@@
    // Find people where hired year == 2012 (hour:minute:second is default initialized to 00:00:00)
    PeopleTable::View view5 = table.where().hired.greater_equal(realm::DateTime(2012, 1, 1).get_datetime())
                                           .hired.less(         realm::DateTime(2013, 1, 1).get_datetime()).find_all();
// @@Fold@@
    assert(view5.size() == 1 && view5[0].name == "Mary");
// @@EndFold@@

    // Find people where photo equals the binary data "bin \0\n data 1"
    PeopleTable::View view6 = table.where().photo.equal(realm::BinaryData("bin \0 data 3", 12)).find_all();
// @@Fold@@
  //  assert(view6.size() == 1 && view6[0].name == "Bob");
}
// @@EndFold@@
// @@EndExample@@
