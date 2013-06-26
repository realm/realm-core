// @@Example: ex_cpp_typed_query_intro @@
#include <tightdb.hpp>
#include <assert.h>

TIGHTDB_TABLE_2(PeopleTable,
                name, String,
                age, Int)

int main()
{
    PeopleTable table;

    table.add("Mary", 14);      // Match
    table.add("Joe", 17);       // Match
    table.add("Jack", 22);

    // Find rows where age >= 13 && age <= 19
    PeopleTable::Query query = table.where().age.between(13, 19);
    PeopleTable::View view = query.find_all();

    // Expected result
    assert(view.size() == 2);
    assert(!strcmp(view[0].name.data(), "Mary"));
    assert(!strcmp(view[1].name.data(), "Joe"));
}
// @@EndExample@@
