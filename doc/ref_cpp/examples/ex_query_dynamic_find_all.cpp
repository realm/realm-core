// @@Example: ex_cpp_dyn_query_do @@
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
    // Peter    44

// @@EndShow@@
    Group group;
    TableRef table = group.get_table("test");

    Spec& s = table->get_spec();
    s.add_column(type_String, "name");
    s.add_column(type_Int,    "age");
    table->update_from_spec();

    table->add_empty_row();
    table->set_string(0, 0, "Mary");
    table->insert_int(1, 0, 14);

    table->add_empty_row();
    table->set_string(0, 1, "Joe");
    table->insert_int(1, 1, 17);

    table->add_empty_row();
    table->set_string(0, 2, "Jack");
    table->insert_int(1, 2, 22);

    table->add_empty_row();
    table->set_string(0, 2, "Bob");
    table->insert_int(1, 2, 80);

    table->add_empty_row();
    table->set_string(0, 2, "Alice");
    table->insert_int(1, 2, 71);

// @@Show@@
    // Find rows where (age > 12 && age < 20) || name == "Alice"
    Query query = table->where().less(1, 50);
    TableView view = query.find_all();

    assert(view.size() == 2);
    assert(!strcmp(view.get_string(0, 0), "Alice"));
    assert(!strcmp(view.get_string(0, 1), "Peter"));
// @@EndShow@@
}
// @@EndExample@@
