// @@Example: ex_cpp_dyn_query_less @@
#include <tightdb.hpp>

using namespace tightdb;
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
    TableRef table = group.get_table("test");

    Spec& s = table->get_spec();
    s.add_column(COLUMN_TYPE_STRING, "name");
    s.add_column(COLUMN_TYPE_INT,    "age");
    table->update_from_spec();

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
    assert(!strcmp(view.get_string(0, 0), "Bob"));

    // Find rows where name (column 0) != "Bob"
    view = table->where().not_equal(0, "Bob").find_all();
    assert(view.size() == 1);
    assert(!strcmp(view.get_string(0, 0), "Alice"));
// @@EndShow@@
}
// @@EndExample@@