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

inline int_fast64_t sequential_read(IntTable& table)
{
    int_fast64_t dummy = 0;
    size_t n = table.size();
    for (size_t i = 0; i != n; ++i)
        dummy += table[i].i;
    return dummy;
}

inline void sequential_write(IntTable& table)
{
    size_t n = table.size();
    for (size_t i = 0; i != n; ++i)
        table[i].i = 126;
}

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


inline void append(IntTable& table, size_t n)
{
    for (size_t i = 0; i != n; ++i)
        table.add(127);
}

inline void erase_all_from_end(IntTable& table)
{
    size_t n = table.size();
    for (size_t i = 0; i != n; ++i)
        table.remove(n-i-1);
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

    vector<size_t> random_read_write_order;
    vector<size_t> random_insert_order;
    vector<size_t> random_erase_order;
    for (size_t i = 0; i != target_size; ++i) {
        random_read_write_order.push_back(i);
        random_insert_order.push_back(rand() % (i+1));
        random_erase_order.push_back(rand() % (target_size-i));
    }
    random_shuffle(random_read_write_order.begin(),
                   random_read_write_order.end());

    int_fast64_t dummy = 0;

    test_util::Timer timer(test_util::Timer::type_UserTime);
    {
        IntTable tables[num_tables];
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            append(tables[i], target_size);
        cout << "Insert at end (compact):    "<<timer<<endl;
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            dummy += sequential_read(tables[i]);
        cout << "Sequential read (compact):  "<<timer<<endl;
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            dummy += read(tables[i], random_read_write_order);
        cout << "Random read (compact):      "<<timer<<endl;
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            sequential_write(tables[i]);
        cout << "Sequential write (compact): "<<timer<<endl;
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            dummy += read(tables[i], random_read_write_order);
        cout << "Random write (compact):     "<<timer<<endl;
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            erase_all_from_end(tables[i]);
        cout << "Erase from end (compact):   "<<timer<<endl;
    }
    {
        IntTable tables[num_tables];
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            insert(tables[i], random_insert_order);
        cout << "Random insert (general):    "<<timer<<endl;
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            dummy += sequential_read(tables[i]);
        cout << "Sequential read (general):  "<<timer<<endl;
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            dummy += read(tables[i], random_read_write_order);
        cout << "Random read (general):      "<<timer<<endl;
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            sequential_write(tables[i]);
        cout << "Sequential write (general): "<<timer<<endl;
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            dummy += read(tables[i], random_read_write_order);
        cout << "Random write (general):     "<<timer<<endl;
        timer.reset();
        for (int i = 0; i != num_tables; ++i)
            erase(tables[i], random_erase_order);
        cout << "Random erase (general):     "<<timer<<endl;
    }

    cout << "dummy = "<<dummy<<" (to avoid over-optimization)"<<endl;
}
