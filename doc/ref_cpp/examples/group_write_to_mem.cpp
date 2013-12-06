// @@Example: ex_cpp_group_write_to_mem @@
// @@Fold@@
#include <cstddef>
#include <cstdlib>
#include <tightdb.hpp>
#include <tightdb/util/file.hpp>

using namespace std;
using namespace tightdb;

// @@EndFold@@
TIGHTDB_TABLE_2(PeopleTable,
                name, String,
                age, Int)

int main()
{
    Group g;

    PeopleTable::Ref table = g.get_table<PeopleTable>("people");
    table->add("Mary", 14);
    table->add("Joe",  17);
    table->add("Jack", 22);

    BinaryData buffer = g.write_to_mem();
    try {
        Group g2(buffer, /* take_ownership: */ false);

        PeopleTable::Ref table2 = g2.get_table<PeopleTable>("people");
        cout << table2[2].age << endl;
        free(const_cast<char*>(buffer.data()));
    } 
    catch (...) {
        free(const_cast<char*>(buffer.data()));
        throw;
    }
}
// @@EndExample@@
