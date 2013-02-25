#include <ctime>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <iomanip>

#include <tightdb.hpp>

using namespace std;
using namespace tightdb;

int64_t sum_int_1(const Table&);
int64_t sum_int_2(const Table&);
int64_t sum_int_3(const Table&);

double sum_double_1(const Table&);
double sum_double_2(const Table&);
double sum_double_3(const Table&);

size_t max_string_size_2(const Table&);
size_t max_string_size_3(const Table&);

size_t max_long_string_size_2(const Table&);
size_t max_long_string_size_2_2(const Table&);
size_t max_long_string_size_3(const Table&);


int main()
{
    Table t;
    t.add_column(type_Int,    "i");
    t.add_column(type_Double, "d");
    t.add_column(type_String, "s");
    t.add_column(type_String, "S");
    for (int i=0; i<1000000; ++i) {
        t.insert_int(0, i, (int64_t(1) << 48) + i);
        t.insert_double(1, i, i);
        t.insert_string(2, i, "foo");
        t.insert_string(3, i, "01234567890123456789"); // Long string
        t.insert_done();
    }
    cout << "Insert done!" << endl;
    cout.precision(1000);

    cout << "Int 1      = " << sum_int_1(t) << endl;
    cout << "Int 2      = " << sum_int_2(t) << endl;
    cout << "Int 3      = " << sum_int_3(t) << endl;

    cout << "Double 1   = " << sum_double_1(t) << endl;
    cout << "Double 2   = " << sum_double_2(t) << endl;
    cout << "Double 3   = " << sum_double_3(t) << endl;

    cout << "String 2   = " << max_string_size_2(t) << endl;
    cout << "String 3   = " << max_string_size_3(t) << endl;

/*
    cout << "LongS 2    = " << max_long_string_size_2(t) << endl;
    cout << "LongS 2.2  = " << max_long_string_size_2_2(t) << endl;
    cout << "LongS 3    = " << max_long_string_size_3(t) << endl;
*/

    cout << fixed << setprecision(2);

    const int n = 30000;

    {
        const int m = n;
        time_t begin = time(0);
        for (int i=0; i<m; ++i) sum_int_1(t);
        time_t elapsed = time(0) - begin;
        cout << "Int 1      = " << setw(7) << (double(m)/elapsed) << " / second" << endl;
    }

    {
        const int m = n / 2;
        time_t begin = time(0);
        for (int i=0; i<m; ++i) sum_int_2(t);
        time_t elapsed = time(0) - begin;
        cout << "Int 2      = " << setw(7) << (double(m)/elapsed) << " / second" << endl;
    }

    {
        const int m = n / 58;
        time_t begin = time(0);
        for (int i=0; i<m; ++i) sum_int_3(t);
        time_t elapsed = time(0) - begin;
        cout << "Int 3      = " << setw(7) << (double(m)/elapsed) << " / second" << endl;
    }



    {
        const int m = n / 7;
        time_t begin = time(0);
        for (int i=0; i<m; ++i) sum_double_1(t);
        time_t elapsed = time(0) - begin;
        cout << "Double 1   = " << setw(7) << (double(m)/elapsed) << " / second" << endl;
    }

    {
        const int m = n / 2;
        time_t begin = time(0);
        for (int i=0; i<m; ++i) sum_double_2(t);
        time_t elapsed = time(0) - begin;
        cout << "Double 2   = " << setw(7) << (double(m)/elapsed) << " / second" << endl;
    }

    {
        const int m = n / 52;
        time_t begin = time(0);
        for (int i=0; i<m; ++i) sum_double_3(t);
        time_t elapsed = time(0) - begin;
        cout << "Double 3   = " << setw(7) << (double(m)/elapsed) << " / second" << endl;
    }



    {
        const int m = n / 8;
        time_t begin = time(0);
        for (int i=0; i<m; ++i) max_string_size_2(t);
        time_t elapsed = time(0) - begin;
        cout << "String 2   = " << setw(7) << (double(m)/elapsed) << " / second" << endl;
    }

    {
        const int m = n / 92;
        time_t begin = time(0);
        for (int i=0; i<m; ++i) max_string_size_3(t);
        time_t elapsed = time(0) - begin;
        cout << "String 3   = " << setw(7) << (double(m)/elapsed) << " / second" << endl;
    }



/*
    {
        const int m = n / 8;
        time_t begin = time(0);
        for (int i=0; i<m; ++i) max_long_string_size_2(t);
        time_t elapsed = time(0) - begin;
        cout << "LongS 2    = " << setw(7) << (double(m)/elapsed) << " / second" << endl;
    }

    {
        const int m = n / 5;
        time_t begin = time(0);
        for (int i=0; i<m; ++i) max_long_string_size_2_2(t);
        time_t elapsed = time(0) - begin;
        cout << "LongS  2.2 = " << setw(7) << (double(m)/elapsed) << " / second" << endl;
    }

    {
        const int m = n / 92;
        time_t begin = time(0);
        for (int i=0; i<m; ++i) max_long_string_size_3(t);
        time_t elapsed = time(0) - begin;
        cout << "LongS 3    = " << setw(7) << (double(m)/elapsed) << " / second" << endl;
    }
*/
}
