// @@Example: ex_cpp_group_unattached @@
// @@Fold@@
#include <tightdb.hpp>
#include <tightdb/util/file.hpp>

using namespace realm;

REALM_TABLE_2(PeopleTable,
                name, String,
                age, Int)

void func(Group& g)
{
    if (!g.is_attached())
        g.open("people.tightdb");

    PeopleTable::Ref table = g.add_table<PeopleTable>("people");

    table->add("Mary", 14);
    table->add("Joe", 17);
    table->add("Jack", 22);

    g.write("people_new.tightdb");
}

int main()
{
    // Create a group with storage implicitly attached
    Group g;
    // Serialize to a file
    g.write("people.tightdb");

// @@EndFold@@
    // Create a group without attached storage
    Group g2((Group::unattached_tag()));
    func(g2);
// @@Fold@@
    util::File::remove("people.tightdb");
    util::File::remove("people_new.tightdb");
}
// @@EndFold@@
// @@EndExample@@
