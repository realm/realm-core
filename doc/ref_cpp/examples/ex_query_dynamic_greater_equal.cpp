// @@Example: ex_cpp_dyn_query_greaterThanOrEqual @@
#include <tightdb.hpp>
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
    // Find rows where age (column 1) >= 44
    Query query = table->where().greater_equal(1, 44);
    TableView view = query.find_all();

    assert(view.size() == 2);
    assert(view.get_string(0, 0) == "Bob");
    assert(view.get_string(0, 1) == "Peter");
// @@EndShow@@
}
// @@EndExample@@
