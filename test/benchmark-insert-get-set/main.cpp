#include <cstdlib>
#include <algorithm>
#include <iostream>

#include <tightdb.hpp>

#include "../util/timer.hpp"

using namespace std;
using namespace tightdb;


namespace {

TIGHTDB_TABLE_1(IntTable,
                i, Int)


inline int_fast64_t read(IntTable& table, const vector<size_t> order)
{
    int_fast64_t dummy = 0;
    size_t n = order.size();
    for (size_t i = 0; i != n; ++i)
        dummy += table[order[i]].i;
    return dummy;
}

inline void write(IntTable& table, const vector<size_t> order)
{
    size_t n = order.size();
    for (size_t i = 0; i != n; ++i)
        table[order[i]].i = 125;
}

inline void insert(IntTable& table, const vector<size_t> order)
{
    size_t n = order.size();
    for (size_t i = 0; i != n; ++i)
        table.insert(order[i], 127);
}

inline void erase(IntTable& table, const vector<size_t> order)
{
    size_t n = order.size();
    for (size_t i = 0; i != n; ++i)
        table.remove(order[i]);
}

} // anonymous namepsace


int main()
{
    const size_t target_size = 1100*1000L;
    const int num_tables = 50;
    cout << "Number of tables: " << num_tables << endl;
    cout << "Elements per table: " << target_size << endl;

    vector<size_t> rising_order;
    vector<size_t> falling_order;
    vector<size_t> random_order;
    vector<size_t> random_insert_order;
    vector<size_t> random_erase_order;
    for (size_t i = 0; i != target_size; ++i) {
        rising_order.push_back(i);
        falling_order.push_back(target_size-1-i);
        random_order.push_back(i);
        random_insert_order.push_back(rand() % (i+1));
        random_erase_order.push_back(rand() % (target_size-i));
    }
    random_shuffle(random_order.begin(), random_order.end());

    IntTable tables_1[num_tables], tables_2[num_tables];

    int_fast64_t dummy = 0;

    test_util::Timer timer_total(test_util::Timer::type_UserTime);
    test_util::Timer timer(test_util::Timer::type_UserTime);
    {
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            insert(tables_1[i], rising_order);
        cout << "Insert at end (compact):    "<<timer<<endl;
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            dummy += read(tables_1[i], rising_order);
        cout << "Sequential read (compact):  "<<timer<<endl;
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            dummy += read(tables_1[i], random_order);
        cout << "Random read (compact):      "<<timer<<endl;
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            write(tables_1[i], rising_order);
        cout << "Sequential write (compact): "<<timer<<endl;
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            write(tables_1[i], random_order);
        cout << "Random write (compact):     "<<timer<<endl;
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            erase(tables_1[i], falling_order);
        cout << "Erase from end (compact):   "<<timer<<endl;
    }
    {
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            insert(tables_2[i], random_insert_order);
        cout << "Random insert (general):    "<<timer<<endl;
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            dummy += read(tables_2[0], rising_order);
        cout << "Sequential read (general):  "<<timer<<endl;
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            dummy += read(tables_2[0], random_order);
        cout << "Random read (general):      "<<timer<<endl;
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            write(tables_2[i], rising_order);
        cout << "Sequential write (general): "<<timer<<endl;
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            write(tables_2[i], random_order);
        cout << "Random write (general):     "<<timer<<endl;
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            erase(tables_2[i], random_erase_order);
        cout << "Random erase (general):     "<<timer<<endl;
    }

    cout << "Total time: "<<timer_total<<endl;

    cout << "dummy = "<<dummy<<" (to avoid over-optimization)"<<endl;
}
