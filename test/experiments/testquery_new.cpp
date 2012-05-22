#include <iostream>
#include "query_new.hpp"

using namespace std;


struct MySubtableSpec: tightdb::SpecBase {
    typedef typename tightdb::TypeAppend< void, int >::type ColTypes1;
    typedef typename tightdb::TypeAppend< ColTypes1, int >::type ColTypes;

    template<template<int> class Col, class Init> struct Columns {
        typename Col<0>::type alpha;
        typename Col<1>::type beta;
        Columns(Init i): alpha(i), beta(i) {}
    };

    static const char* const* col_names()
    {
        static const char* l[] = { "alpha", "beta" };
        return l;
    }

    struct ConvenienceMethods { // FIXME: Note: ConvenienceMethods may not contain any virtual methods, nor may they add any data memebers (check this by TIGHTDB_STATIC_ASSERT(sizeof(Derivative: ConvenienceMethods) == 1))
        void add(int alpha, int beta)
        {
            table<MySubtableSpec>(this)->add((tuple(), alpha, beta));
        }
    };
};

typedef tightdb::BasicTable<MySubtableSpec> MySubtable;




struct MyTableSpec: tightdb::SpecBase {
    typedef typename tightdb::TypeAppend< void, int >::type ColTypes1;
    typedef typename tightdb::TypeAppend< ColTypes1, Enum<MyEnum> >::type ColTypes2;
    typedef typename tightdb::TypeAppend< ColTypes2, Subtable<MySubtable> >::type ColTypes;

    template<template<int> class Col, class Init> struct Columns {
        typename Col<0>::type foo;
        typename Col<1>::type bar;
        typename Col<2>::type baz;
        Columns(Init i): foo(i), bar(i), baz(i) {}
    };

    static const char* const* col_names()
    {
        static const char* l[] = { "foo", "bar", "baz" };
        return l;
    }

    struct ConvenienceMethods {
        void add(int foo, Enum<MyEnum> bar, Subtable<MySubtable> baz)
        {
            table<MySubtableSpec>(this)->add((tuple(), foo, bar, baz));
        }
    };
};

typedef tightdb::BasicTable<MyTableSpec> MyTable;



size_t my_count(const MyTable& table)
{
    MyTable::QueryRow t;
//  MySubtable::QueryRow s;
//  return table.count(exists(t.baz, s.alpha < 7));
//    return table.count(!(!t.foo || false));
    return table.count(t.foo > 1111);
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
