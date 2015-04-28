// @@Example: ex_cpp_dyn_table_lower_upper_bound @@
// @@Fold@@
#include <realm.hpp>

using namespace realm;

// @@EndFold@@
// Using lower_bound_int() to insert new elements before existsing ones
// with the same value.
void insert_sorted_1(Table& table, int sorted_value, StringData extra_value)
{
    size_t i = table.lower_bound_int(0, sorted_value);
    table.insert_empty_row(i);
    table.set_int(0, i, sorted_value);
    table.set_string(1, i, extra_value);
}

// Using upper_bound_int() to insert new elements after existsing ones
// with the same value.
void insert_sorted_2(Table& table, int sorted_value, StringData extra_value)
{
    size_t i = table.upper_bound_int(0, sorted_value);
    table.insert_empty_row(i);
    table.set_int(0, i, sorted_value);
    table.set_string(1, i, extra_value);
}
// @@Fold@@

int main()
{
    Table t;
    t.add_column(type_Int, "foo");
    t.add_column(type_String, "bar");

    // before duplicates
    insert_sorted_1(t, 3, "a"); // <3a>
    insert_sorted_1(t, 1, "b"); // <1b> 3a
    insert_sorted_1(t, 3, "c"); // 1b <3c> 3a
    insert_sorted_1(t, 2, "d"); // 1b <2d> 3c 3a

    // after duplicates
    insert_sorted_2(t, 2, "e"); // 1b 2d <2e> 3c 3a
    insert_sorted_2(t, 3, "f"); // 1b 2d 2e 3c 3a <3f>
    insert_sorted_2(t, 1, "g"); // 1b <1g> 2d 2e 3c 3a 3f
    insert_sorted_2(t, 2, "h"); // 1b 1g 2d 2e <2h> 3c 3a 3f
}
// @@EndFold@@
// @@EndExample@@
