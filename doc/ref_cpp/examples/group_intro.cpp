// @@Example: ex_cpp_group_intro @@
// @@Fold@@
#include <iostream>
#include <tightdb.hpp>
#include <tightdb/util/file.hpp>

using namespace std;
using namespace tightdb;

// @@EndFold@@
TIGHTDB_TABLE_2(PeopleTable,
                name, String,
                age, Int)

void func()
{
    Group g("people.tightdb", Group::mode_ReadWrite);

    PeopleTable::Ref table = g.add_table<PeopleTable>("people");

    table->add("Mary", 14);
    table->add("Joe", 17);
    table->add("Jack", 22);

    g.commit();
}
// @@Fold@@

int main()
{
    func();
    util::File::remove("people.tightdb");
}
// @@EndFold@@
// @@EndExample@@
