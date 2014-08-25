// @@Example: ex_cpp_group_commit @@
// @@Fold@@
#include <tightdb.hpp>
#include <tightdb/util/file.hpp>

using namespace tightdb;

// @@EndFold@@
TIGHTDB_TABLE_2(PeopleTable,
                name, String,
                age, Int)

void func(Group& g)
{
    PeopleTable::Ref table = g.add_table<PeopleTable>("people");

    table->add("Mary", 14);
    table->add("Joe", 17);
    table->add("Jack", 22);

    g.commit();
}
// @@Fold@@

int main()
{
    Group g("people.tightdb", Group::mode_ReadWrite);
    func(g);
    util::File::remove("people.tightdb");
}
// @@EndFold@@
// @@EndExample@@
