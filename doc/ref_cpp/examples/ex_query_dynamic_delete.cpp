// @@Example: ex_cpp_dyn_query_delete @@
#include <tightdb.hpp>
#include <assert.h>

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
    TableRef table = group.add_table("test");

    table->add_column(type_String, "name");
    table->add_column(type_Int,    "age");

    table->add_empty_row();
    table->set_string(0, 0, "Alice");
    table->set_int(1, 0, 27);

    table->add_empty_row();
    table->set_string(0, 1, "Bob");
    table->set_int(1, 1, 50);

    table->add_empty_row();
    table->set_string(0, 2, "Peter");
    table->set_int(1, 2, 44);

// @@Show@@
    // Find rows where age (column 1) < 48
    Query query = table->where().less(1, 48);
    size_t removed = query.remove();

    // 2 rows deleted
    assert(removed == 2);

    // 1 row left
    assert(table->size() == 1);
// @@EndShow@@
}
// @@EndExample@@
