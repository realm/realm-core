#include <iostream>
using namespace std;

// @@Example: create_table @@
#include <tightdb.hpp>
#include <group.hpp>
using namespace tightdb;

// defining a table
TIGHTDB_TABLE_3(MyTable,
//              columns: types:
                name,    String,
                age,     Int,
                hired,   Bool)

int main() {
    // creating an instance of the table
    MyTable table;
    // @@EndExample@@
    
    // @@Example: insert_rows @@
    table.add("Mary", 21, false);
    table.add("Lars", 21, true);
    table.add("Phil", 43, false);
    table.add("Anni", 54, true);
    // @@EndExample@@
    
    
    // @@Example: insert_at_index @@
    table.insert(2, "Frank", 34, true);
    // @@EndExample@@
    
    
    // @@Example: number_of_rows @@
    cout << table.size() << endl; // => 6
    cout << (table.is_empty() ? "Empty" : "Not empty") << endl; // => Not Empty
    // @@EndExample@@
    
    // @@Example: accessing_rows @@
    // Getting values
    const char* name = table[3].name;  // => 'Anni'
    int age          = table[3].age;   // => 54
    bool hired       = table[3].hired; // => true
    
    // Changing values
    table[3].age = 43; // Getting younger
    table[3].age += 1; // Happy birthday!
    // @@EndExample@@
    
    // @@Example: last_row @@
    cout << table.back().name << endl;                 // => "Anni"
    // @@EndExample@@
    
    // @@Example: updating_entire_row @@
    //table.set(4, "Eric", 50, true);
    // @@EndExample@@
    
    // @@Example: deleting_row @@
    table.remove(2);
    cout << table.size(); // => 5
    // @@EndExample@@
    
    cout << endl << endl;
    
    // @@Example: iteration @@
    for (size_t i = 0; i < table.size(); ++i) {
        MyTable::Cursor row = table[i];
        cout << row.name << " is " << row.age << " years old." << endl;
    }
    // @@EndExample@@
    
    
    // @@Example: simple_seach @@
    size_t row_ndx;
    row_ndx = table.cols().name.find_first("Philip");  // row = (size_t)-1
    row_ndx = table.cols().name.find_first("Mary");    // row = 1
    
    TableView view = table.cols().age.find_all(21);
    size_t cnt = view.size();         // cnt => 2
    // @@EndExample@@
    
    cout << endl << endl;
    
    // @@Example: advanced_search @@
    // Create query (current employees between 20 and 30 years old)
    Query q = table.where().hired.equal(true)    // implicit logical-AND
                           .age.between(20, 30);
    
    // Get number of matching entries
    cout << q.count(table) << endl;      // => 2
    
    // Get the average age
    cout << q.average(table, 1) << endl; // => 26
    
    // Execute the query and return a table (view)
    TableView res = q.find_all(table);
    for (size_t i = 0; i < res.size(); ++i) {
        const char* name = res.get_string(0, i);
        const int age    = res.get_int(1, i);
        
        cout << i << ": " << name << " is " << age << " years old." << endl;
    }
    // @@EndExample@@
    
    cout << endl << endl;
    
    // @@Example: serialisation @@
    // Create Table in Group
    Group group;
    BasicTableRef<MyTable> t = group.get_table<MyTable>("employees");
    
    // Add some rows
    t->add("John", 20, true);
    t->add("Mary", 21, false);
    t->add("Lars", 21, true);
    t->add("Phil", 43, false);
    t->add("Anni", 54, true);
    
    // Write to disk
    group.write("employees.tightdb");
    
    // Load a group from disk (and print contents)
    Group fromDisk("employees.tightdb");
    BasicTableRef<MyTable> diskTable = fromDisk.get_table<MyTable>("employees");
    for (size_t i = 0; i < diskTable->size(); i++)
        cout << i << ": " << diskTable[i].name << endl;
    
    // Write same group to memory buffer
    size_t len;
    const char* const buffer = group.write_to_mem(len);
    
    // Load a group from memory (and print contents)
    Group fromMem(buffer, len);
    BasicTableRef<MyTable> memTable = fromMem.get_table<MyTable>("employees");
    for (size_t i = 0; i < memTable->size(); i++)
        cout << i << ": " << memTable[i].name << endl; 
    // @@EndExample@@
}

