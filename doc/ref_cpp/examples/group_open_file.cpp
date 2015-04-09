// @@Example: ex_cpp_group_open_file @@
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
    // Create a group from a file:
    Group::unattached_tag tag;
    Group g(tag);
    g.open("people.realm", 0, Group::mode_ReadWrite);

// @@Fold@@
    PeopleTable::Ref table = g.add_table<PeopleTable>("people");

    table->add("Mary", 14);
    table->add("Joe", 17);
    table->add("Jack", 22);

    g.write("people_new.realm");
}

int main()
{
    Group g;
    g.write("people.realm");
    func();
    util::File::remove("people.realm");
    util::File::remove("people_new.realm");
}
// @@EndFold@@
// @@EndExample@@
