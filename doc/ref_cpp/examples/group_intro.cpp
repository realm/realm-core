// @@Example: ex_cpp_group_intro @@
// @@Fold@@
#include <tightdb.hpp>
#include <tightdb/file.hpp>
#include <iostream>

using namespace tightdb;
using namespace std;

// @@EndFold@@
TIGHTDB_TABLE_2(PeopleTable,
                name, String,
                age, Int)

void func()
{
    Group g("people.tightdb", Group::mode_ReadWrite);

    PeopleTable::Ref table = g.get_table<PeopleTable>("people");

    table->add("Mary", 14);
    table->add("Joe", 17);
    table->add("Jack", 22);

    g.commit();
}
// @@Fold@@

int main()
{
    func();
    File::remove("people.tightdb");
}
// @@EndFold@@
// @@EndExample@@
