// @@Example: ex_cpp_group_to_json @@
// @@Fold@@
#include <iostream>
#include <sstream>
#include <tightdb.hpp>
#include <tightdb/util/file.hpp>

using namespace std;
using namespace tightdb;

TIGHTDB_TABLE_2(PeopleTable,
                name, String,
                age, Int)

int main()
{
    Group group;

    PeopleTable::Ref table = group.add_table<PeopleTable>("people");
    table->add("Mary", 14);
    table->add("Joe", 17);
    table->add("Jack", 22);
// @@EndFold@@

    ostringstream ss;
    group.to_json(ss);
    cout << ss.str() << endl;

    // or using less memory:
    group.to_json(cout);
    cout << endl;

// @@Fold@@
}
// @@EndFold@@
// @@EndExample@@
