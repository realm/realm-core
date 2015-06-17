// @@Example: ex_cpp_dyn_query_endsWith @@
#include <realm.hpp>
#include <assert.h>

using namespace realm;

int main()
{
    Group group;
    TableRef table = group.add_table("test");
    table->add_column(type_String, "Name");

    table->add_empty_row(5);
// @@Show@@
    table->set_string(0, 0, "Mary");
    table->set_string(0, 1, "Joe");
    table->set_string(0, 2, "Jack");
    table->set_string(0, 3, "Jill");
    table->set_string(0, 4, "oe");

    // Find names (column 0) ending with "oe", case sensitive
    TableView view1 = table->where().ends_with(0, "oe").find_all();
    assert(view1.size() == 2);
    assert(view1.get_string(0, 0) == "Joe");
    assert(view1.get_string(0, 1) == "oe");

    // Will find none because search is case sensitive
    TableView view2 = table->where().ends_with(0, "OE").find_all();
    assert(view2.size() == 0);

#ifdef _MSC_VER
    // Case insensitive search only supported on Windows
    TableView view3 = table->where().ends_with(0, "oE", false).find_all();

    assert(view3.size() == 2);
    assert(view3.get_string(0, 0), "Joe");
    assert(view3.get_string(0, 1), "oe");
#endif
// @@EndShow@@
}
// @@EndExample@@
