// @@Example: ex_cpp_group_constructor_memory @@
// @@Fold@@
#include <cstddef>
#include <cstdlib>
#include <tightdb.hpp>
#include <tightdb/file.hpp>

using namespace std;
using namespace tightdb;

// @@EndFold@@
TIGHTDB_TABLE_2(PeopleTable,
                name, String,
                age, Int)

void func(BinaryData buffer)
{
    bool take_ownership = false;
    Group g(buffer, take_ownership);
    PeopleTable::Ref table = g.get_table<PeopleTable>("people");

    table->add("Mary", 14);
    table->add("Joe",  17);
    table->add("Jack", 22);

    g.write("people.tightdb");
}
// @@Fold@@

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
    File::remove("people.tightdb");
}
// @@EndFold@@
// @@EndExample@@
