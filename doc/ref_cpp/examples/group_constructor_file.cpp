// @@Example: ex_cpp_group_constructor_file @@
// @@Fold@@
#include <tightdb.hpp>
#include <tightdb/file.hpp>

using namespace tightdb;

// @@EndFold@@
TIGHTDB_TABLE_2(PeopleTable,
                name, String,
                age, Int)

void func()
{
    Group g("people.tightdb");
    PeopleTable::Ref table = g.get_table<PeopleTable>("people");

    table->add("Mary", 14);
    table->add("Joe", 17);
    table->add("Jack", 22);

    g.write("people_new.tightdb");
}
// @@Fold@@

int main()
{
    Group g;
    g.write("people.tightdb");
    func();
    File::remove("people.tightdb");
    File::remove("people_new.tightdb");
}
// @@EndFold@@
// @@EndExample@@
