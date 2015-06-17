// @@Example: ex_cpp_dyn_query_constructor @@
#include <realm.hpp>

using namespace realm;

int main()
{
// @@Show@@
    Group group;
    TableRef table = group.add_table("test");

    // Create query with no criterias which will match all rows
    Query q = table->where();
// @@EndShow@@
}
// @@EndExample@@
