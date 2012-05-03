// @@Example: create_table @@
#include "tightdb.h"

// defining a table
TDB_TABLE_3(PeopleTable,
            name,  String,
            age,   Int,
            hired, Bool)

// creating an instance of the table
PeopleTable table;
}
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
cout << table.size(); // => 6
cout << (table.is_empty() ? "Empty" : "Not empty"); // => Not Empty
// @@EndExample@@

// @@Example: accessing_rows @@
// Getting values
const char* name = table[5].name;  // => 'Anni'
int age          = table[5].age;   // => 54
bool hired       = table[5].hired; // => true

// Changing values
table[5].age = 43; // Getting younger
table[5].age += 1; // Happy birthday!
// @@EndExample@@

// @@Example: last_row @@
table.back().name;                 // => "Anni"
// @@EndExample@@

// @@Example: updating_entire_row @@
table.set(4, "Eric", 50, true);
// @@EndExample@@

// @@Example: deleting_row @@
table.remove(2);
cout << table.size(); // => 5
// @@EndExample@@

// @@Example: iteration @@
for (size_t i = 0; i < table.size(); ++i) {
    PeopleTable::Cursor row = table[i];
    cout << row.name << " is " << row.age << " years old." << endl;
}
// @@EndExample@@


// @@Example: simple_seach @@
size_t row;
row = table.name.find("Philip");  // row = (size_t)-1
row = table.name.find("Mary");    // row = 1

TableView view = table.age.find_all(21);
size_t cnt = view.size();         // cnt => 2
// @@EndExample@@


// @@Example: advanced_search @@
// Create query (current employees between 20 and 30 years old)
Query q = table.where().hired.equal(true)    // implicit logical-AND
                          .age.between(20, 30);

 // Get number of matching entries
 cout << q.size();      // => 2

 // Get the average age
 cout << q.age.avg();   // => 26

 // Execute the query and return a table (view)
 TableView res = q.find_all(table);
 for (size_t i = 0; i < res.size(); ++i) {
    cout << i << ": " << res[i].name << " is "
         << res[i].age << " years old." << endl;
 }
// @@EndExample@@


// @@Example: serialisation @@
TDB_TABLE_3(PeopleTable,
            name,  String,
            age,   Int,
            hired, Bool)
 
// Create Table in Group
Group group;
PeopleTable& table = group.get_table<PeopleTable>("My great table");
 
// Add some rows
table.add("John", 20, true);
table.add("Mary", 21, false);
table.add("Lars", 21, true);
table.add("Phil", 43, false);
table.add("Anni", 54, true);

// Write to disk
group.write("employees.tightdb");

// Load a group from disk (and print contents)
Group fromDisk("employees.tightdb");
PeopleTable& diskTable = fromDisk.get_table<PeopleTable>("employees");
for (size_t i = 0; i < diskTable.size(); i++)
    cout << i << ": " << diskTable[i].name << endl;

// Write same group to memory buffer
size_t len;
const char* const buffer = group.write_to_mem(len);

// Load a group from memory (and print contents)
Group fromMem(buffer, len);
PeopleTable& memTable = fromMem.get_table<PeopleTable>("employees");
for (size_t i = 0; i < memTable.size_t(); i++)
    cout << i << ": " << memTable[i].name << endl; 
// @@EndExample@@
