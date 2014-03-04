#include <cstdlib>
#include <functional>
#include <iostream>

#include <tightdb.hpp>

using namespace std;
using namespace tightdb;

double sum1(const Table&);
double sum2(const Table&);

int main()
{
    Table t;
    t.add_column(type_Int, "v");
    for (int i=0; i<1000000; ++i) {
        t.insert_int(0, i, 10000);
        t.insert_done();
    }
    cout << "Insert done!" << endl;

    for (int i=0; i<100000; ++i) sum1(t);
    cout << "1" << endl;

/*
    for (int i=0; i<100000; ++i) sum2(t);
    cout << "2" << endl;
*/

/*
    double sum1 = t.sum_double(0);
    double sum2 = t.foldl_double(0, plus<double>(), 0.0);

    cout << "Sum1 = " << sum1 << endl;
    cout << "Sum2 = " << sum2 << endl;
*/
}
