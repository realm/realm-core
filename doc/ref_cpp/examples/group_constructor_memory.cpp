// @@Example: ex_cpp_group_constructor_memory @@
// @@Fold@@
#include <cstddef>
#include <cstdlib>
#include <tightdb.hpp>
#include <tightdb/util/file.hpp>

using namespace std;
using namespace tightdb;

TIGHTDB_TABLE_2(PeopleTable,
                name, String,
                age, Int)

void func(BinaryData buffer)
{
// @@EndFold@@
    // Create a group using the buffer as backing store
    Group g(buffer, /* take_ownership: */ false);

    // Get a table, or create it if it doesn't exist
    PeopleTable::Ref table = g.add_table<PeopleTable>("people");
// @@Fold@@

    table->add("Mary", 14);
    table->add("Joe",  17);
    table->add("Jack", 22);

    g.write("people.tightdb");
}

int main()
{
    Group g;
    BinaryData buffer = g.write_to_mem();
    try {
        func(buffer);
    }
    catch (...) {
        free(const_cast<char*>(buffer.data()));
        throw;
    }
    free(const_cast<char*>(buffer.data()));
    util::File::remove("people.tightdb");
}
// @@EndFold@@
// @@EndExample@@
