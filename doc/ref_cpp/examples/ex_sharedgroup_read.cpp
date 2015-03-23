// @@Example: ex_cpp_shared_group_read @@
// @@Fold@@
#include <cassert>
#include <tightdb.hpp>
#include <tightdb/util/file.hpp>

using namespace realm;

// Define schema for main table
REALM_TABLE_3(PeopleTable,
                  name,   String,
                  age,    Int,
                  hired,  Bool)

// @@EndFold@@
void some_function_making_it_impossible_to_use_RAII(SharedGroup& g, PeopleTable::ConstRef employees)
{
    // Get table
    assert(employees->column().age.sum() == 64);

    g.end_read();
}

void func()
{
    // Create a new shared group
    SharedGroup db("shared_db.tightdb");

    {
        Group& g = db.begin_write();
        PeopleTable::Ref employees = g.add_table<PeopleTable>("employees");
        employees->add("joe", 42, false);
        employees->add("jessica", 22, true);
        db.commit();

        try {
            // Do a read transaction
            const Group& g = db.begin_read();
            PeopleTable::ConstRef empl = g.get_table<PeopleTable>("employees");
            some_function_making_it_impossible_to_use_RAII(db, empl);
        }
        catch (...) {
            db.end_read();
            throw;
        }
    }
}

// @@Fold@@
int main()
{
    func();
    util::File::remove("shared_db.tightdb");
}
// @@EndFold@@
// @@EndExample@@
