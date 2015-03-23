// @@Example: ex_cpp_dyn_query_sum @@
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
    // Find the sum of Age (column 1) of entire table (no criteria)
    int64_t sum = table->where().sum_int(1);
    assert(sum == 27 + 50 + 44);

    // Find the sum of Age (column 1) where Name (column 0) contains "e"
    Query q = table->where().contains(0, "e");
    sum = q.sum_int(1);
    assert(sum == 50 + 44);
// @@EndShow@@
}
// @@EndExample@@
