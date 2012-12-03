// @@Example: ex_cpp_typed_query_intro @@
#include <tightdb.hpp>

TIGHTDB_TABLE_2(PeopleTable,
                name, String,
                age, Int)

void main()
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
    assert(!strcmp(view1[0].name, "Mary"));
    assert(!strcmp(view1[1].name, "Joe"));
}
// @@EndExample@@