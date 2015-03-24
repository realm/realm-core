// @@Example: ex_cpp_typed_query_endsWith @@
// @@Fold@@
#include <realm.hpp>
#include <assert.h>

REALM_TABLE_1(PeopleTable,
                name,  String)

int main()
{
    PeopleTable table;
// @@EndFold@@
    table.add("Mary");
    table.add("Joe");
    table.add("Jack");
    table.add("Jill");
    table.add("oe");

    // Find names ending with "oe", case sensitive
    PeopleTable::View view1 = table.where().name.ends_with("oe").find_all();
// @@Fold@@
    assert(view1.size() == 2);
    assert(view1[0].name == "Joe");
    assert(view1[1].name == "oe");
// @@EndFold@@

    // Will find none because search is case sensitive
    PeopleTable::View view2 = table.where().name.ends_with("OE").find_all();
// @@Fold@@
    assert(view2.size() == 0);
// @@EndFold@@

#ifdef _MSC_VER
    // Case insensitive search only supported on Windows
    PeopleTable::View view3 = table.where().name.ends_with("oE", false).find_all();
// @@Fold@@
    assert(view3.size() == 2);
    assert(view3[0].name == "Joe");
    assert(view3[1].name == "oe");
#endif
}
// @@EndExample@@
// @@EndFold@@
