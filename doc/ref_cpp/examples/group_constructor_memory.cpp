#include <cstddef>
#include <cstdlib>
#include <tightdb.hpp>

using namespace std;
using namespace tightdb;

// @@Example: ex_cpp_group_constructor_memory @@
TIGHTDB_TABLE_2(PeopleTable,
                name, String,
                age, Int)

void func(const char* data, size_t size)
{
    Group g(data, size);
    PeopleTable::Ref table = g.get_table<PeopleTable>("people");

    table.add("Mary", 14);
    table.add("Joe", 17);
    table.add("Jack", 22);

    g.write("people.tightdb");
}

int main()
{
    Group g;
    size_t size;
    char* data = g.write_to_mem(size);
    if (!data) return 1;
    try {
        func(data, size);
    }
    catch (...) {
        free(data);
        throw;
    }
    free(data);
}
// @@EndExample@@
