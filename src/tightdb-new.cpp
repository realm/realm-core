#include "tightdb.hpp"

/*

Intantiate correct column types - requires a new form of Spec building.

consider MyTableSpec: public cols_types<a, b, c, d>

New opportunity to privatize accessor copying! No, because a class cannot declare friendship with anaother class specified as template parameter.

Iterators, queries, and TableViews do not hold a reference count on subtables, so it is up to the application to ensure that a TableRef exists.

Copy rows
Copy tables

*/




TDB_TABLE_2(MySubTable,
            Int, foo,
            Int, bar)

TDB_TABLE_2(MyTable,
            Int, val,
            MySubTable, tab)



int main()
{
    MyTable t;

    {
        int v = t[0].val;
        (void)v;

        t[0].val = 7;
    }

    {
        int v = t.cols().val[0];
        (void)v;

        t.cols().val[0] = 7;
    }

    {
        double v = t[0].tab[0].bar;
        (void)v;

        t[0].tab[0].bar = 7;
    }

    {
        int v = t.cols().tab[0]->cols().bar[0];
        (void)v;

        t.cols().tab[0]->cols().bar[0] = 7;
    }

    {
        int v = t.cols().tab[0][0].bar;
        (void)v;

        t.cols().tab[0][0].bar = 7;
    }

    MyTable::Query().val.Equal(7).Or().val.Equal(8).Delete(t);

    return 0;
}
