// @@Example: ex_cpp_group_tostring @@
// @@Fold@@
#include <iostream>
#include <sstream>
#include <realm.hpp>
#include <realm/util/file.hpp>

using namespace realm;

REALM_TABLE_2(PeopleTable,
                name, String,
                age, Int)

int main()
{
    Group group;

    PeopleTable::Ref table = group.add_table<PeopleTable>("people");
    table->add("Mary", 14);
    table->add("Joe", 17);
    table->add("Jack", 22);
// @@EndFold@@

    std::ostringstream ss;
    group.to_string(ss);
    std::cout << ss.str() << std::endl;

    // or using less memory:
    group.to_string(std::cout);
    std::cout << std::endl;

// @@Fold@@
}
// @@EndFold@@
// @@EndExample@@
