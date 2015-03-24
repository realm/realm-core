#include <realm.hpp>

using namespace realm;
using namespace std;

REALM_TABLE_3(People,
                name, String,
                age,  Int,
                hired, Bool)

REALM_TABLE_2(Books,
                title, String,
                author, String)

int main()
{
    // Create group
    Group g1;

    // Create table (as reference)
    People::Ref t1 = g1.add_table<People>("people");

    // Add rows
    t1->add("John", 13, true);
    t1->add("Mary", 18, false);
    t1->add("Lars", 16, true);
    t1->add("Phil", 43, false);
    t1->add("Anni", 20, true);

    // And another table
    Books::Ref t2 = g1.get_table<Books>("books");
    t2->add("I, Robot", "Isaac Asimov");
    t2->add("Childhood's End", "Arthur C. Clarke");

    // and save to disk
    g1.write("test.tdb");

    // Read a group from disk
    Group g2("test.tdb");
    Books::Ref t3 = g2.get_table<Books>("books");
    cout << "Table Books" << endl;
    for(size_t i=0; i<t3->size(); ++i) {
        cout << "'" << t3[i].title << "' by " << t3[i].author << endl;
    }
}
