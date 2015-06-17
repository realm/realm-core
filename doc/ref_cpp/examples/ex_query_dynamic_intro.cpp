// @@Example: ex_cpp_dyn_query_intro @@
#include <realm.hpp>
#include <assert.h>

using namespace realm;

int main()
{
    // Create following table dynamically:

// @@Show@@
    // name    age
    // ------------
    // Alice    14
    // Bob      17
    // Peter    22

// @@EndShow@@
    Group group;
    TableRef table = group.add_table("test");
    table->add_column(type_String, "name");
    table->add_column(type_Int,    "age");

    table->add_empty_row();
    table->set_string(0, 0, "Alice");
    table->insert_int(1, 0, 14);

    table->add_empty_row();
    table->set_string(0, 1, "Bob");
    table->insert_int(1, 1, 17);

    table->add_empty_row();
    table->set_string(0, 2, "Peter");
    table->insert_int(1, 2, 22);

// @@Show@@
    // Find rows where age > 20
    Query query = table->where().greater(1, 20);
    TableView view = query.find_all();

    assert(view.size() == 1);
    assert(view.get_string(0, 0) == "Peter");
// @@EndShow@@
}
// @@EndExample@@
