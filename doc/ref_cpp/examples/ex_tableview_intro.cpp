// @@Example: ex_cpp_tableview_into @@
#include <realm.hpp>
#include <assert.h>

REALM_TABLE_2(PeopleTable,
                name, String,
                age, Int)

int main()
{
    PeopleTable table;

    table.add("Joe",   57);
    table.add("Mary",  14);
    table.add("Alice", 42);
    table.add("Jack",  32);

    // Select rows where age > 18
    PeopleTable::View view = table.where().age.greater(18).find_all();

    assert(view.size() == 3);
    assert(view[0].name == "Joe");
    assert(view[1].name == "Alice");
    assert(view[2].name == "Jack");

    // Sort result according to age
    view.column().age.sort();
    assert(view[0].age == 32);
    assert(view[1].age == 42);
    assert(view[2].age == 57);

    // Find position of Alice in sorted view
    size_t alice = view.column().name.find_first("Alice");
    assert(alice == 1);
}
// @@EndExample@@
