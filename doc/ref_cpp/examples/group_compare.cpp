// @@Example: ex_cpp_group_operator_equal @@
// @@Fold@@
#include <iostream>
#include <tightdb.hpp>
#include <tightdb/util/file.hpp>

using namespace std;
using namespace realm;

int main()
{
    Group g;
    g.write("people_1.tightdb");
    g.write("people_2.tightdb");

// @@EndFold@@
    Group group_1("people_1.tightdb");
    Group group_2("people_2.tightdb");

    if (group_1 == group_2) cout << "EQUAL\n";
// @@Fold@@
    util::File::remove("people_1.tightdb");
    util::File::remove("people_2.tightdb");
}
// @@EndFold@@
// @@EndExample@@
