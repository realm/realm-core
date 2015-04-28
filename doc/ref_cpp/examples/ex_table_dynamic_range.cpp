// @@Example: ex_cpp_dyn_table_get_range_view@@
// @@Fold@@
#include <realm.hpp>

using namespace realm;

// @@EndFold@@

// @@Fold@@
int main()
{
    Table t;
    t.add_column(type_Int, "foo");

    t.add_empty_row(100);
    for(size_t i=0; i<100; ++i) {
        t.set_int(0, i, i);
    }

    TableView tv = t.get_range_view(10, 20);
    std::cout << tv.size() << " numbers: " << tv.sum_int(0) << std::endl;
}
// @@EndFold@@
// @@EndExample@@
