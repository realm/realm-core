// @@Example: ex_cpp_dyn_query_startsWith @@
#include <cassert>
#include <realm.hpp>

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
    table->set_string(0, 4, "Jo");

    // Find Names (column 0) beginning with "Jo"
    TableView view1 = table->where().begins_with(0, "Jo").find_all();
    assert(view1.size() == 2);
    assert(view1.get_string(0,0) == "Joe");
    assert(view1.get_string(0,1) == "Jo");

    // Will find no Names (column 0) because it's case sensitive
    TableView view2 = table->where().begins_with(0, "JO").find_all();
    assert(view2.size() == 0);

#ifdef _MSC_VER
    // Case insensitive search only supported on Windows
    TableView view3 = table->where().begins_with(0, "JO", false).find_all();

    assert(view3.size() == 2);
    assert(view3.get_string(0,0) == "Joe");
    assert(view3.get_string(0,1) == "Jo");
#endif
// @@EndShow@@
}
// @@EndExample@@
