#include <realm.hpp>
#include <realm/group_shared.hpp>
#include <sstream>

using namespace realm;

int main(int, char*[]) {
    Table t;

    t.add_column(type_String, "name");
    size_t name_ndx = t.get_column_index("name");

    t.add_empty_row();
    t.set_string(name_ndx, 0,"Joe");

    t.add_empty_row();
    t.set_string(name_ndx, 1, "Joe");

    // forgot about age
    t.add_column(type_Int, "age");
    size_t age_ndx = t.get_column_index("age");
    t.set_int(age_ndx, 0, 32);
    t.set_int(age_ndx, 1, 18);

    std::cout << "Total: " << t.sum_int(age_ndx) << std::endl;
    std::stringstream ss;
    t.to_json(ss);
    std::cout << "JSON:  " << ss.str() << std::endl;
}
