// @@Example: ex_cpp_typed_query_startsWith @@
// @@Fold@@
#include <tightdb.hpp>
#include <assert.h>

REALM_TABLE_1(PeopleTable,
                name,  String)

int main()
{
// @@EndFold@@
    PeopleTable table;
    table.add("Mary");
    table.add("Joe");
    table.add("Jack");
    table.add("Jill");
    table.add("Jo");

    // Find names beginning with "Jo"
    PeopleTable::View view1 = table.where().name.begins_with("Jo").find_all();
    assert(view1.size() == 2);
    assert(view1[0].name == "Joe");
    assert(view1[1].name == "Jo");

    // Will find none because it's case sensitive
    PeopleTable::View view2 = table.where().name.begins_with("JO").find_all();
    assert(view2.size() == 0);

#ifdef _MSC_VER
    // Case insensitive search only supported on Windows
    PeopleTable::View view3 = table.where().name.begins_with("JO", false).find_all();
// @@Fold@@
    assert(view3.size() == 2);
    assert(view3[0].name, "Joe");
    assert(view3[1].name, "Jo");
// @@EndFold@@
#endif
// @@Fold@@
}
// @@EndFold@@
// @@EndExample@@
