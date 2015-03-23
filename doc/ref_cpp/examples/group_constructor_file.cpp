// @@Example: ex_cpp_group_constructor_file @@
// @@Fold@@
#include <tightdb.hpp>
#include <tightdb/util/file.hpp>

using namespace tightdb;

REALM_TABLE_2(PeopleTable,
                name, String,
                age, Int)

void func()
{
// @@EndFold@@
    // Create group with a file as backing store
    Group g("people.tightdb");
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
