// @@Example: ex_cpp_dyn_query_find_next @@
#include <realm.hpp>
#include <assert.h>

using namespace realm;

int main()
{
    // Create following table dynamically:

// @@Show@@
    // name    age
    // ------------
    // Alice    27
    // Bob      50
    // Peter    44

// @@EndShow@@
    Group group;
    TableRef table = group.add_table("test");
    table->add_column(type_String, "name");
    table->add_column(type_Int,    "age");

    table->add_empty_row();
    table->set_string(0, 0, "Alice");
    table->insert_int(1, 0, 27);

    table->add_empty_row();
    table->set_string(0, 1, "Bob");
    table->insert_int(1, 1, 50);

    table->add_empty_row();
    table->set_string(0, 2, "Peter");
    table->insert_int(1, 2, 44);

// @@Show@@
    // Find rows where age (column 1) < 50
    Query query = table->where().less(1, 50);

    // Find first match
    size_t match = query.find();
    assert(match == 0); // Alice

    // Find next match
    match = query.find(match + 1);
    assert(match == 2); // Alice

    // No more matches
    match = query.find(match + 1);
    assert(match == size_t(-1));
// @@EndShow@@
}
// @@EndExample@@
