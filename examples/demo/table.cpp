#include <tightdb.hpp>
#include <tightdb/group_shared.hpp>

using namespace realm;
using namespace std;

REALM_TABLE_2(Phones,
                desc, String,
                number, String)

REALM_TABLE_4(People,
                name, String,
                age,  Int,
                hired, Bool,
                phones, Subtable<Phones>)

int main(int, char*[]) {

    People t;

    // row 0
    t.add();
    t[0].name  = "John";
    t[0].age   = 20;
    t[0].hired = true;
    t[0].phones->add();
    t[0].phones[0].desc   = "home";
    t[0].phones[0].number = "555-1234-555";

    // row 1
    t.add();
    t[1].name  = "Mary";
    t[1].age   = 21;
    t[1].hired = false;

    // row 2
    t.add();
    t[2].name  = "Lars";
    t[2].age   = 32;
    t[2].hired = true;
    t[2].phones->add();
    t[2].phones[0].desc   = "work";
    t[2].phones[0].number = "555-4433-222";

    // row 3
    t.add();
    t[3].name = "Phil";
    t[3].age  = 43;
    t[3].hired = false;

    // row 4
    t.add();
    t[4].name = "Anni";
    t[4].age  = 54;
    t[4].hired = true;
    t[4].phones->add();
    t[4].phones[0].desc   = "work";
    t[4].phones[0].number = "555-5544-123";
    t[4].phones->add();
    t[4].phones[1].desc   = "home";
    t[4].phones[1].number = "555-4321-999";

    // Iterating
    for(size_t i=0; i<t.size(); ++i) {
        cout << t[i].name << " has " << t[i].phones->size() << " phones" << endl;
        for(size_t j=0; j<t[i].phones->size(); ++j) {
            cout << " " << t[i].phones[j].desc << ": " << t[i].phones[j].number << endl;
        }
    }

    // Deleting a row
    t.remove(2);

    // Insert new row
    t.insert_empty_row(2);
    t[2].name  = "Bill";
    t[2].age   = 34;
    t[2].hired = false;

    // Number of rows
    cout << "Rows:    " << t.size() << endl;
}
