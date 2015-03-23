// @@Example: ex_cpp_group_open_memory @@
// @@Fold@@
#include <cstddef>
#include <cstdlib>
#include <tightdb.hpp>
#include <tightdb/util/file.hpp>

using namespace std;
using namespace realm;

REALM_TABLE_2(PeopleTable,
                name, String,
                age, Int)

void func(BinaryData buffer)
{
// @@EndFold@@
    // Create a group from a buffer:
    Group::unattached_tag tag;
    Group g(tag);
    g.open(buffer, /* take_ownership: */ false);
// @@Fold@@

    PeopleTable::Ref table = g.add_table<PeopleTable>("people");

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
