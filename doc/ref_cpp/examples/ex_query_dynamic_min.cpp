// @@Example: ex_cpp_dyn_query_min @@
#include <tightdb.hpp>
#include <assert.h>

using namespace realm;

int main()
{
    // Create following table dynamically:

// @@Show@@
    // Name    Age
    // ------------
    // Bob     27
    // Alice   50
    // Peter   44

// @@EndShow@@
    Group group;
    TableRef table = group.add_table("test");
    table->add_column(type_String, "Name");
    table->add_column(type_Int,    "Age");

    table->add_empty_row();
    table->set_string(0, 0, "Bob");
    table->insert_int(1, 0, 27);

    table->add_empty_row();
    table->set_string(0, 1, "Alice");
    table->insert_int(1, 1, 50);

    table->add_empty_row();
    table->set_string(0, 2, "Peter");
    table->insert_int(1, 2, 44);

// @@Show@@
    // Find the minimum Age (column 1) of entire table (no criteria)
    int64_t min = table->where().minimum_int(1);
    assert(min == 27);

    // Find the minimum Age (column 1) where Name (column 0) contains "e"
    Query q = table->where().contains(0, "e");
    min = q.minimum_int(1);
    assert(min == 44);
// @@EndShow@@
}
// @@EndExample@@
