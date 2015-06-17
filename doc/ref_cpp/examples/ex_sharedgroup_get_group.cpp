// @@Example: ex_cpp_shared_group_get_group @@
// @@Fold@@
#include <cassert>
#include <iostream>
#include <realm.hpp>
#include <realm/util/file.hpp>

using namespace realm;

// Define schema for main table
REALM_TABLE_3(PeopleTable,
                  name,   String,
                  age,    Int,
                  hired,  Bool)


void traverse(const Group& group)
{
    if (!group.is_empty()) {
        std::cout << "Tables in group and number of columns in them:" << std::endl;
        size_t n = group.size();
        for (size_t i = 0; i < n; ++i) {
            StringData table_name = group.get_table_name(i);
            ConstTableRef table = group.get_table(table_name);
            std::cout << table_name << " " << table->get_column_count() << "\n";
        }
        std::cout << "End of group contents" << std::endl;
    }
    else {
        std::cout << "Group is empty" << std::endl;
    }
}


void func()
{
    // Create a new shared group
    SharedGroup db("shared_db.realm");

    {
// @@EndFold@@
        ReadTransaction trx(db);
        // get the underlying group, because we want to use it to call a function
        // working on groups (traverse):
        const Group& g = trx.get_group();
        traverse(g);
// @@Fold@@
    }
}

int main()
{
    func();
    util::File::remove("shared_db.realm");
}
// @@EndFold@@
// @@EndExample@@
