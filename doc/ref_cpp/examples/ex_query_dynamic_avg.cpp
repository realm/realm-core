// @@Example: ex_cpp_dyn_query_avg @@
#include <tightdb.hpp>

using namespace tightdb;

int main()
{
    // Create following table dynamically:

// @@Show@@
    // Name    Age
    // ------------
    // Alice    27
    // Bob      50
    // Peter    44

// @@EndShow@@
    Group group;
    TableRef table = group.get_table("test");

    Spec& s = table->get_spec();
    s.add_column(COLUMN_TYPE_STRING, "Name");
    s.add_column(COLUMN_TYPE_INT,    "Age");
    table->update_from_spec();

    table->add_empty_row(3);
    table->set_string(0, 0, "Alice");
    table->insert_int(1, 0, 27);

    table->set_string(0, 1, "Bob");
    table->insert_int(1, 1, 50);

    table->set_string(0, 2, "Peter");
    table->insert_int(1, 2, 44);

// @@Show@@
    // Find the average Age (column 1) of entire table (no criteria)
    double avg = table->where().average(1);
    assert(avg == (27 + 50 + 44) / 3.0);

    // Find the average Age (column 1) where Name (column 0) contains "e" 
    Query q = table->where().contains(0, "e");
    avg = q.average(1);
    assert(avg == (27 + 44) / 2.0);
// @@EndShow@@
}
// @@EndExample@@