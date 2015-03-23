// @@Example: ex_cpp_shared_group_get_table @@
// @@Fold@@
#include <cassert>
#include <tightdb.hpp>
#include <tightdb/util/file.hpp>

using namespace tightdb;

// Define schema for main table
REALM_TABLE_3(PeopleTable,
                  name,   String,
                  age,    Int,
                  hired,  Bool)

void func()
{
    // Create a new shared group
    SharedGroup db("shared_db.tightdb");

    // Do a write transaction
    {
// @@EndFold@@
        WriteTransaction trx(db);

        // Get table (creating it if it does not exist)
        PeopleTable::Ref employees = trx.add_table<PeopleTable>("employees");
// @@Fold@@

        // Add initial rows (with sub-tables)
        if (employees->is_empty()) {
            employees->add("joe", 42, false);
            employees->add("jessica", 22, true);
        }

        trx.commit();
    }

    // Verify changes in read-only transaction
    {
        ReadTransaction trx(db);
        PeopleTable::ConstRef employees = trx.get_table<PeopleTable>("employees");

        // Do a query
        PeopleTable::Query q = employees->where().hired.equal(true);
        PeopleTable::View view = q.find_all();

        // Verify result
        assert(view.size() == 1 && view[0].name == "jessica");
    }
}

int main()
{
    func();
    util::File::remove("shared_db.tightdb");
}
// @@EndFold@@
// @@EndExample@@
