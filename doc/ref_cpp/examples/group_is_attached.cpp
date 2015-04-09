// @@Example: ex_cpp_group_is_attached @@
// @@Fold@@
#include <realm.hpp>
#include <realm/util/file.hpp>

using namespace realm;

// @@EndFold@@
REALM_TABLE_2(PeopleTable,
                name, String,
                age, Int)

void func(Group& g)
{
    if (!g.is_attached())
        g.open("people.realm");

    PeopleTable::Ref table = g.add_table<PeopleTable>("people");

    table->add("Mary", 14);
    table->add("Joe", 17);
    table->add("Jack", 22);

    g.write("people_new.realm");
}
// @@Fold@@

int main()
{
    // Create a group with storage implicitly attached
    Group g;
    // Serialize to a file
    g.write("people.realm");

    // Create a new group without attaced storage
    Group g2((Group::unattached_tag()));
    func(g2);
    util::File::remove("people.realm");
    util::File::remove("people_new.realm");
}
// @@EndFold@@
// @@EndExample@@
