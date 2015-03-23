#include <tightdb.hpp>

using namespace tightdb;
using namespace std;

REALM_TABLE_3(People,
                name, String,
                age,  Int,
                hired, Bool)


int main(int, char*[]) {
    // Create table
    People t;

    // Add rows
    t.add("John", 13, true);
    t.add("Mary", 18, false);
    t.add("Lars", 16, true);
    t.add("Phil", 43, false);
    t.add("Anni", 20, true);

    // Get a view
    People::View v1 = t.where().hired.equal(true).find_all();
    cout << "Hired: " << v1.size() << endl;

    // Retire seniors
    People::View v2 = t.where().age.greater(65).find_all();
    for(size_t i=0; i<v2.size(); ++i) {
        v2[i].hired = false;
    }

    // Remove teenagers
    People::View v3 = t.where().age.between(13, 19).find_all();
    v3.clear();

    cout << "Rows: " << t.size() << endl;
}
