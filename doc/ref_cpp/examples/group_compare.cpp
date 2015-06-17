// @@Example: ex_cpp_group_operator_equal @@
// @@Fold@@
#include <iostream>
#include <realm.hpp>
#include <realm/util/file.hpp>

using namespace realm;

int main()
{
    Group g;
    g.write("people_1.realm");
    g.write("people_2.realm");

// @@EndFold@@
    Group group_1("people_1.realm");
    Group group_2("people_2.realm");

    if (group_1 == group_2) std::cout << "EQUAL\n";
// @@Fold@@
    util::File::remove("people_1.realm");
    util::File::remove("people_2.realm");
}
// @@EndFold@@
// @@EndExample@@
