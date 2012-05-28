#include <iostream>

#include "table_accessors.hpp"
#include "query_expr.hpp"

using namespace std;



struct MySubtableSpec: tightdb::SpecBase {
    typedef typename tightdb::TypeAppend< void, int >::type Columns1;
    typedef typename tightdb::TypeAppend< Columns1, int >::type Columns;

    template<template<int> class Col, class Init> struct ColNames {
        typename Col<0>::type alpha;
        typename Col<1>::type beta;
        ColNames(Init i): alpha(i), beta(i) {}
    };

    static const char* const* dyn_col_names()
    {
        static const char* l[] = { "alpha", "beta" };
        return l;
    }
};

typedef tightdb::BasicTable<MySubtableSpec> MySubtable;




struct MyTableSpec: tightdb::SpecBase {
    typedef typename tightdb::TypeAppend< void, int >::type Columns1;
    typedef typename tightdb::TypeAppend< Columns1, int >::type Columns2;
    typedef typename tightdb::TypeAppend< Columns2, tightdb::SpecBase::Subtable<MySubtable> >::type Columns;

    template<template<int> class Col, class Init> struct ColNames {
        typename Col<0>::type foo;
        typename Col<1>::type bar;
        typename Col<2>::type baz;
        ColNames(Init i): foo(i), bar(i), baz(i) {}
    };

    static const char* const* dyn_col_names()
    {
        static const char* l[] = { "foo", "bar", "baz" };
        return l;
    }
};

typedef tightdb::BasicTable<MyTableSpec> MyTable;



size_t my_count(const MyTable& table)
{
    MyTable::QueryRow t;
//    MySubtable::QueryRow s;
//    return table.count(exists(t.baz, s.alpha < 7));
//    return table.count(!(!t.foo || false));
//    return table.count(t.foo > 1111);
    return table.exists(t.foo % t.bar > 1111);
}

size_t my_exists(const MyTable& table)
{
    MyTable::QueryRow t;
    return table.exists(false || true);
}



int main()
{
    MyTable t;
    cout << my_count(t) << endl;
    return 0;
}
