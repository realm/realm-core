// @@Example: ex_cpp_group_constructor_plain @@
// @@Fold@@
#include <realm.hpp>
#include <realm/util/file.hpp>

using namespace realm;

REALM_TABLE_2(PeopleTable,
                name, String,
                age, Int)

void func()
{
// @@EndFold@@
    // Create an empty group
    Group g;

    // Create a table in the group
    PeopleTable::Ref table = g.add_table<PeopleTable>("people");
// @@Fold@@

    table->add("Mary", 14);
    table->add("Joe", 17);
    table->add("Jack", 22);

    g.write("people.realm");
}

int main()
{
    func();
    util::File::remove("people.realm");
}
// @@EndFold@@
// @@EndExample@@
