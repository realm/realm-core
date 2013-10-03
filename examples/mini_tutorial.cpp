#include <iostream>
using namespace std;

#include <tightdb.hpp>
using namespace tightdb;

// defining a table
TIGHTDB_TABLE_2(MyTable,
//              columns: types:
                name,    String,
                age,     Int)

int main() {
    // Create an in-memory database
    SharedGroup db("persons.tightdb", false, SharedGroup::durability_MemOnly);

    // In a write transaction:
    // - create a table
    // - add rows
    {
        WriteTransaction tr(db);
        MyTable::Ref table = tr.get_table<MyTable>("persons");

        table->add("Jill", 40);
        table->add("Mary", 20);
        table->add("Phil", 43);
        table->add("Sara", 47);

        tr.commit();
    }

    // In a read transaction:
    // - calculate number of rows and total age
    // - find persons in the forties
    {
        ReadTransaction tr(db);

        MyTable::ConstRef table = tr.get_table<MyTable>("persons");
        cout << table->size() << " " << table->column().age.sum() << endl;

        MyTable::View view = table->where().age.between(40, 49).find_all();
        for(size_t i=0; i<view.size(); ++i) {
            cout << view[i].name << endl;
        }
    }
}
