// @@Example: ex_cpp_shared_group_readtrans @@
// @@Fold@@
#include <cassert>
#include <tightdb.hpp>
#include <tightdb/file.hpp>

using namespace tightdb;

// Define schema for sub-table
TIGHTDB_TABLE_2(PhoneTable,
                  type,   String,
                  number, String)

// Define schema for main table
TIGHTDB_TABLE_4(PeopleTable,
                  name,   String,
                  age,    Int,
                  hired,  Bool,
                  phones, Subtable<PhoneTable>)

// @@EndFold@@
void func()
{
    // Create a new shared group
    SharedGroup db("shared_db.tightdb");

    // @@Fold@@
    // Do a write transaction
    {
        WriteTransaction trx(db);

        // Get table (creating it if it does not exist)
        PeopleTable::Ref employees = trx.get_table<PeopleTable>("employees");

        // Add initial rows (with sub-tables)
        if (employees->is_empty()) {
            employees->add("joe", 42, false, NULL);
            employees[0].phones->add("home", "324-323-3214");
            employees[0].phones->add("work", "321-564-8678");

            employees->add("jessica", 22, true, NULL);
            employees[1].phones->add("mobile", "434-426-4646");
            employees[1].phones->add("school", "345-543-5345");
        }

        trx.commit();
    }
    // @@EndFold@@

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
// @@Fold@@

int main()
{
    func();
    File::remove("shared_db.tightdb");
}
// @@EndFold@@
// @@EndExample@@
