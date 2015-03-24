#include <iostream>
#include <realm.hpp>
#include <realm/group_shared.hpp>
#include <realm/util/file.hpp>

using namespace realm;
using namespace std;

namespace {

REALM_TABLE_1(MyTable,
                text, String)

} // namespace

// Results:
// -rw-r--r--  1 kristian kristian 1092616192 Oct 12 15:32 over_alloc_1.db
// -rw-r--r--  1 kristian kristian    1048576 Oct 12 15:32 over_alloc_2.db

int main()
{
    int n_outer = 3000;
    int n_inner = 42;

    // Many transactions
    {
        File::try_remove("over_alloc_1.realm");
        File::try_remove("over_alloc_1.realm.lock");
        SharedGroup db("over_alloc_1.realm");
        if (!db.is_valid()) throw runtime_error("Failed to open database 1");

        for (int i=0; i<n_outer; ++i) {
            {
                Group& group = db.begin_write();
                MyTable::Ref table = group.get_table<MyTable>("my_table");
                for (int j=0; j<n_inner; ++j) {
                    table->add("x");
                }
            }
            db.commit();
        }
    }

    // One transaction
    {
        File::try_remove("over_alloc_2.realm");
        File::try_remove("over_alloc_2.realm.lock");
        SharedGroup db("over_alloc_2.realm");
        if (!db.is_valid()) throw runtime_error("Failed to open database 2");

        {
            Group& group = db.begin_write();
            MyTable::Ref table = group.get_table<MyTable>("my_table");
            for (int i=0; i<n_outer; ++i) {
                for (int j=0; j<n_inner; ++j) {
                    table->add("x");
                }
            }
        }
        db.commit();
    }

    return 0;
}
