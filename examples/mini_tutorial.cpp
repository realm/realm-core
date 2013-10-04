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
    // create an in-memory database
    SharedGroup db("persons.tightdb", false, SharedGroup::durability_MemOnly);
    {
        // a write transaction
        WriteTransaction tr(db);
        // create a table
        MyTable::Ref table = tr.get_table<MyTable>("persons");
        // add three rows
        table->add("Mary", 40);
        table->add("Mary", 20);
        table->add("Phil", 43);
        // commit changes
        tr.commit();
    }
    {
        // a read transaction
        ReadTransaction tr(db);
        // get the table
        MyTable::ConstRef table = tr.get_table<MyTable>("persons");
        // calculate number of rows and total age
        cout << table->size() << " " << table->column().age.sum() << endl;
        // find persons in the forties
        MyTable::View view = table->where().age.between(40, 49).find_all();
        for(size_t i=0; i<view.size(); ++i) {
            cout << view[i].name << endl;
        }
    }
}
