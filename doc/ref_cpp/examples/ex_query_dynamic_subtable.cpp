// @@Example: ex_cpp_dyn_query_subtable @@
// @@Fold@@
#include <realm.hpp>
#include <assert.h>

using namespace realm;
using namespace std;

int main()
{
// @@EndFold@@
    /*
    Create following dynamically typed table with below subtables,
    and then find Names who have at least 1 Score greater than 500
    (Peter and Bob).

    Names           Scores
    ------------------------------
                    Score
                    --------------
    Peter           623
                    260
                    811

    Bob             223
                    160
                    912

    Alice           123
                    111
    */
// @@Fold@@
    Group group;
    TableRef table = group.add_table("MainTable");

    table->add_column(type_String, "Names");
    table->add_column(type_Table, "Scores");

    vector<size_t> path;
    path.push_back(1);
    table->add_subcolumn(path, type_Int, "Score");

    table->add_empty_row();
    table->set_string(0, 0, "Peter");
    TableRef subtable = table->get_subtable(1, 0);
    subtable->add_empty_row(3);
    subtable->set_int(0, 0, 623);
    subtable->set_int(0, 0, 260);
    subtable->set_int(0, 0, 811);

    table->add_empty_row();
    table->set_string(0, 1, "Bob");
    subtable = table->get_subtable(1, 1);
    subtable->add_empty_row(3);
    subtable->set_int(0, 0, 223);
    subtable->set_int(0, 0, 160);
    subtable->set_int(0, 0, 912);

    table->add_empty_row();
    table->set_string(0, 2, "Alice");
    subtable = table->get_subtable(1, 2);
    subtable->add_empty_row(2);
    subtable->set_int(0, 0, 123);
    subtable->set_int(0, 0, 111);

    // @@EndFold@@
    // Find all people who have at least 1 score greater than 500
    Query q = table->where();
    q.subtable(1); // The subtables are stored in the second column (column index 1)
    q.greater(0, 500);
    q.end_subtable();

    TableView tv = q.find_all();

    assert(tv.size() == 2);
    assert(tv.get_string(0, 0) == "Peter");
    assert(tv.get_string(0, 1) == "Bob");
// @@Fold@@
}
// @@EndFold@@
// @@EndExample@@
