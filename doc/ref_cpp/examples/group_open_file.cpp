// @@Example: ex_cpp_group_open_file @@
// @@Fold@@
#include <tightdb.hpp>
#include <tightdb/util/file.hpp>

using namespace tightdb;

TIGHTDB_TABLE_2(PeopleTable,
                name, String,
                age, Int)

void func()
{
// @@EndFold@@
    // Create a group from a file:
    Group::unattached_tag tag;
    Group g(tag);
    g.open("people.tightdb", Group::mode_ReadWrite);

// @@Fold@@
    PeopleTable::Ref table = g.add_table<PeopleTable>("people");

    table->add("Mary", 14);
    table->add("Joe", 17);
    table->add("Jack", 22);

    g.write("people_new.tightdb");
}

int main()
{
    Group g;
    g.write("people.tightdb");
    func();
    util::File::remove("people.tightdb");
    util::File::remove("people_new.tightdb");
}
// @@EndFold@@
// @@EndExample@@
