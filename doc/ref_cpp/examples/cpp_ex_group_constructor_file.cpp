#include <tightdb.hpp>

using namespace tightdb;

TIGHTDB_TABLE_2(PeopleTable,
                name, String,
                age, Int)

int main()
{
    Group g("people.tightdb");
    PeopleTable::Ref table = g.get_table<PeopleTable>("people");

    table.add("Mary", 14);
    table.add("Joe", 17);
    table.add("Jack", 22);

    g.write("people_new.tightdb");
}
