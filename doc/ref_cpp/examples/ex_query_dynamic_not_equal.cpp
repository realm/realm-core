// @@Example: ex_cpp_dyn_query_notEquals @@
#include <realm.hpp>
#include <assert.h>

using namespace realm;
using namespace std;

int main()
{
    // Create following table dynamically:

// @@Show@@
    // name    age
    // ------------
    // Alice    27
    // Bob      50

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

// @@Show@@
    // Find rows where age (column 1) != 27
    TableView view = table->where().not_equal(1, 27).find_all();
    assert(view.size() == 1);
    assert(view.get_string(0, 0) == "Bob");

    // Find rows where name (column 0) != "Bob"
    view = table->where().not_equal(0, "Bob").find_all();
    assert(view.size() == 1);
    assert(view.get_string(0, 0) == "Alice");
// @@EndShow@@
}
// @@EndExample@@
