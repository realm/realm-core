// @@Example: ex_cpp_typed_query_notEquals @@
// @@Fold@@
#include <tightdb.hpp>
#include <assert.h>

REALM_TABLE_2(PeopleTable,
                name,  String,
                age,   Int)

int main()
{
    PeopleTable table;

    table.add("Mary",  28);
    table.add("Frank", 56);

// @@EndFold@@
    // Find rows where age != 56
    PeopleTable::View view1 = table.where().age.not_equal(56).find_all();
// @@Fold@@

    assert(view1.size() == 1 && view1[0].name == "Mary");
// @@EndFold@@

    // Find rows where name != "Frank"
    PeopleTable::View view2 = table.where().name.not_equal("Frank").find_all();
// @@Fold@@
    assert(view2.size() == 1 && view2[0].name == "Mary");
}
// @@EndFold@@
// @@EndExample@@
