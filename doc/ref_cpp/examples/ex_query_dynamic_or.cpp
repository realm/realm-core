// @@Example: ex_cpp_dyn_query_or @@
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
    // Mary    14
    // Joe     17
    // Jack    22
    // Bob     80


// @@EndShow@@
    Group group;
    TableRef table = group.add_table("test");
    table->add_column(type_String, "name");
    table->add_column(type_Int,    "age");

    table->add_empty_row(4);
    table->set_string(0, 0, "Mary");
    table->insert_int(1, 0, 14);

    table->set_string(0, 1, "Joe");
    table->insert_int(1, 1, 17);

    table->set_string(0, 2, "Jack");
    table->insert_int(1, 2, 22);

    table->set_string(0, 3, "Bob");
    table->insert_int(1, 3, 80);

// @@Show@@
    // Find rows where age < 15 || name == "Jack"
    Query query = table->where().less(1, 15).Or().equal(0, "Jack");

    TableView view = query.find_all();
    assert(view.size() == 2);
    assert(view.get_string(0, 0) == "Mary");
    assert(view.get_string(0, 1) == "Jack");
// @@EndShow@@
}
// @@EndExample@@
