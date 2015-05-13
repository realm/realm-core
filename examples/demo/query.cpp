#include <realm.hpp>

using namespace realm;

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

    // Create query
    // -> hired teenagers
    People::Query q1 = t.where().age.between(13, 19).hired.equal(true);
    std::cout << "No. teenagers: " << q1.count() << std::endl;

    // Another query
    // -> names with i or a
    People::Query q2 = t.where().name.contains("i").Or().name.contains("a");
    std::cout << "Min: " << q2.age.minimum() << std::endl;
    std::cout << "Max: " << q2.age.maximum() << std::endl;
}
