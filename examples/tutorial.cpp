#include <iostream>
#include <realm.hpp>

using namespace realm;

// @@Example: create_table @@

// defining a table
REALM_TABLE_3(MyTable,
//              columns: types:
                name,    String,
                age,     Int,
                hired,   Bool)

int main()
{
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
    std::cout << table.size() << std::endl; // => 6
    std::cout << (table.is_empty() ? "Empty" : "Not empty") << std::endl; // => Not Empty
    // @@EndExample@@

    // @@Example: accessing_rows @@
    // Getting values
    StringData name  = table[3].name;  // => 'Anni'
    int age          = table[3].age;   // => 54
    bool hired       = table[3].hired; // => true

    // Changing values
    table[3].age = 43; // Getting younger
    table[3].age += 1; // Happy birthday!
    // @@EndExample@@

    static_cast<void>(age);
    static_cast<void>(hired);

    // @@Example: last_row @@
    std::cout << table.back().name << std::endl;                 // => "Anni"
    // @@EndExample@@

    // @@Example: updating_entire_row @@
    //table.set(4, "Eric", 50, true);
    // @@EndExample@@

    // @@Example: deleting_row @@
    table.remove(2);
    std::cout << table.size(); // => 5
    // @@EndExample@@

    std::cout << std::endl << std::endl;

    // @@Example: iteration @@
    for (size_t i = 0; i < table.size(); ++i) {
        MyTable::Cursor row = table[i];
        std::cout << row.name << " is " << row.age << " years old." << std::endl;
    }
    // @@EndExample@@


    // @@Example: simple_seach @@
    size_t row_index;
    row_index = table.column().name.find_first("Philip");  // => not_found (-1)
    row_index = table.column().name.find_first("Mary");    // => 1
    static_cast<void>(row_index);

    MyTable::View view = table.column().age.find_all(21);
    size_t cnt = view.size();         // cnt => 2
    // @@EndExample@@

    static_cast<void>(cnt);

    std::cout << std::endl << std::endl;

    // @@Example: advanced_search @@
    // Create query (current employees between 20 and 30 years old)
    MyTable::Query q = table.where().hired.equal(true)    // implicit logical-AND
                                    .age.between(20, 30);

    // Get number of matching entries
    std::cout << q.count() << std::endl;      // => 2

    // Get the average age
    std::cout << q.age.average() << std::endl; // => 26

    // Execute the query and return a table (view)
    MyTable::View res = q.find_all();
    for (size_t i = 0; i < res.size(); ++i) {
        StringData name = res[i].name;
        int        age  = res[i].age;

        std::cout << i << ": " << name << " is " << age << " years old." << std::endl;
    }
    // @@EndExample@@

    std::cout << std::endl << std::endl;

    // @@Example: serialisation @@
    // Create Table in Group
    Group group;
    MyTable::Ref t = group.add_table<MyTable>("employees");

    // Add some rows
    t->add("John", 20, true);
    t->add("Mary", 21, false);
    t->add("Lars", 21, true);
    t->add("Phil", 43, false);
    t->add("Anni", 54, true);

    // Remove database file if already existing
    remove("employees.realm");

    // Write to disk
    group.write("employees.realm");

    // Load a group from disk (and print contents)
    Group fromDisk("employees.realm");
    MyTable::Ref diskTable = fromDisk.get_table<MyTable>("employees");
    for (size_t i = 0; i < diskTable->size(); ++i)
        std::cout << i << ": " << diskTable[i].name << std::endl;

    // Write same group to memory buffer
    BinaryData buffer = group.write_to_mem();

    // Load a group from memory (and print contents)
    Group fromMem(buffer);
    MyTable::Ref memTable = fromMem.get_table<MyTable>("employees");
    for (size_t i = 0; i < memTable->size(); i++)
        std::cout << i << ": " << memTable[i].name << std::endl;
    // @@EndExample@@

    // @@Example: transaction @@
    // Open a shared group
    SharedGroup db("employees.realm");

    // Read transaction
    {
        ReadTransaction tr(db); // start transaction
        MyTable::ConstRef table = tr.get_table<MyTable>("employees");

        // Print table contents
        for (size_t i = 0; i < table->size(); ++i)
            std::cout << i << ": " << table[i].name << std::endl;
    }

    // Write transaction (will rollback on error)
    {
        WriteTransaction tr(db); // start transaction
        MyTable::Ref table = tr.get_table<MyTable>("employees");

        // Add row to table
        table->add("Bill", 53, true);

        tr.commit(); // end transaction
    }
    // @@EndExample@@
}

