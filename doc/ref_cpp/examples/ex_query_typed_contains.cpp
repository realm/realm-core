// @@Example: ex_cpp_typed_query_contains @@
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

    // Find names containing the substring "ac"
    PeopleTable::View view1 = table.where().name.contains("ac").find_all();
// @@Fold@@
    assert(view1.size() == 1);
    assert(view1[0].name == "Jack");
// @@EndFold@@

    // Will find none because it's case sensitive
    PeopleTable::View view2 = table.where().name.contains("AC").find_all();
// @@Fold@@
    assert(view2.size() == 0);
// @@EndFold@@

#ifdef _MSC_VER
    // Case insensitive search only supported on Windows
    PeopleTable::View view3 = table.where().name.contains("AC", false).find_all();
// @@Fold@@

    assert(view3.size() == 1);
    assert(view3[0].name == "Jack");
#endif
}
// @@EndFold@@
// @@EndExample@@
