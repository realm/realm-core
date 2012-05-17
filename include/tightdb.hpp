/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/
#ifndef TIGHTDB_HPP
#define TIGHTDB_HPP

#include "../src/table_basic.hpp"


#define TIGHTDB_TABLE_1(Table, name1, type1) \
struct Table##Spec: tightdb::SpecBase { \
    typedef tightdb::TypeAppend< void, type1 >::type ColTypes; \
 \
    template<template<int> class Col, class Init> struct Columns { \
        typename Col<0>::type name1; \
        Columns(Init i): name1(i, #name1) {} \
    }; \
 \
    template<class C, class T1> \
    static void insert(std::size_t i, const C& cols, const T1& v1) \
    { \
        cols.name1._insert(i, v1); \
    } \
}; \
typedef tightdb::BasicTable<Table##Spec> Table;


#define TIGHTDB_TABLE_2(Table, name1, type1, name2, type2) \
struct Table##Spec: tightdb::SpecBase { \
    typedef tightdb::TypeAppend< void, type1 >::type ColTypes1; \
    typedef tightdb::TypeAppend< ColTypes1, type2 >::type ColTypes; \
 \
    template<template<int> class Col, class Init> struct Columns { \
        typename Col<0>::type name1; \
        typename Col<1>::type name2; \
        Columns(Init i): name1(i, #name1), name2(i, #name2) {} \
    }; \
 \
    template<class C, class T1, class T2> \
    static void insert(std::size_t i, const C& cols, const T1& v1, const T2& v2) \
    { \
        cols.name1._insert(i, v1); \
        cols.name2._insert(i, v2); \
    } \
}; \
typedef tightdb::BasicTable<Table##Spec> Table;


#define TIGHTDB_TABLE_3(Table, name1, type1, name2, type2, name3, type3) \
struct Table##Spec: tightdb::SpecBase { \
    typedef tightdb::TypeAppend< void, type1 >::type ColTypes1; \
    typedef tightdb::TypeAppend< ColTypes1, type2 >::type ColTypes2; \
    typedef tightdb::TypeAppend< ColTypes2, type3 >::type ColTypes; \
 \
    template<template<int> class Col, class Init> struct Columns { \
        typename Col<0>::type name1; \
        typename Col<1>::type name2; \
        typename Col<2>::type name3; \
        Columns(Init i): name1(i, #name1), name2(i, #name2), name3(i, #name3) {} \
    }; \
 \
    template<class C, class T1, class T2, class T3> \
    static void insert(std::size_t i, const C& cols, const T1& v1, const T2& v2, const T3& v3) \
    { \
        cols.name1._insert(i, v1); \
        cols.name2._insert(i, v2); \
        cols.name3._insert(i, v3); \
    } \
}; \
typedef tightdb::BasicTable<Table##Spec> Table;


#define TIGHTDB_TABLE_4(Table, name1, type1, name2, type2, name3, type3, name4, type4) \
struct Table##Spec: tightdb::SpecBase { \
    typedef tightdb::TypeAppend< void, type1 >::type ColTypes1; \
    typedef tightdb::TypeAppend< ColTypes1, type2 >::type ColTypes2; \
    typedef tightdb::TypeAppend< ColTypes2, type3 >::type ColTypes3; \
    typedef tightdb::TypeAppend< ColTypes3, type4 >::type ColTypes; \
 \
    template<template<int> class Col, class Init> struct Columns { \
        typename Col<0>::type name1; \
        typename Col<1>::type name2; \
        typename Col<2>::type name3; \
        typename Col<3>::type name4; \
        Columns(Init i): name1(i, #name1), name2(i, #name2), name3(i, #name3), name4(i, #name4) {} \
    }; \
 \
    template<class C, class T1, class T2, class T3, class T4> \
    static void insert(std::size_t i, const C& cols, const T1& v1, const T2& v2, const T3& v3, const T4& v4) \
    { \
        cols.name1._insert(i, v1); \
        cols.name2._insert(i, v2); \
        cols.name3._insert(i, v3); \
        cols.name4._insert(i, v4); \
    } \
}; \
typedef tightdb::BasicTable<Table##Spec> Table;


#define TIGHTDB_TABLE_5(Table, name1, type1, name2, type2, name3, type3, name4, type4, name5, type5) \
struct Table##Spec: tightdb::SpecBase { \
    typedef tightdb::TypeAppend< void, type1 >::type ColTypes1; \
    typedef tightdb::TypeAppend< ColTypes1, type2 >::type ColTypes2; \
    typedef tightdb::TypeAppend< ColTypes2, type3 >::type ColTypes3; \
    typedef tightdb::TypeAppend< ColTypes3, type4 >::type ColTypes4; \
    typedef tightdb::TypeAppend< ColTypes4, type5 >::type ColTypes; \
 \
    template<template<int> class Col, class Init> struct Columns { \
        typename Col<0>::type name1; \
        typename Col<1>::type name2; \
        typename Col<2>::type name3; \
        typename Col<3>::type name4; \
        typename Col<4>::type name5; \
        Columns(Init i): name1(i, #name1), name2(i, #name2), name3(i, #name3), name4(i, #name4), name5(i, #name5) {} \
    }; \
 \
    template<class C, class T1, class T2, class T3, class T4, class T5> \
    static void insert(std::size_t i, const C& cols, const T1& v1, const T2& v2, const T3& v3, const T4& v4, const T5& v5) \
    { \
        cols.name1._insert(i, v1); \
        cols.name2._insert(i, v2); \
        cols.name3._insert(i, v3); \
        cols.name4._insert(i, v4); \
        cols.name5._insert(i, v5); \
    } \
}; \
typedef tightdb::BasicTable<Table##Spec> Table;


#define TIGHTDB_TABLE_6(Table, name1, type1, name2, type2, name3, type3, name4, type4, name5, type5, name6, type6) \
struct Table##Spec: tightdb::SpecBase { \
    typedef tightdb::TypeAppend< void, type1 >::type ColTypes1; \
    typedef tightdb::TypeAppend< ColTypes1, type2 >::type ColTypes2; \
    typedef tightdb::TypeAppend< ColTypes2, type3 >::type ColTypes3; \
    typedef tightdb::TypeAppend< ColTypes3, type4 >::type ColTypes4; \
    typedef tightdb::TypeAppend< ColTypes4, type5 >::type ColTypes5; \
    typedef tightdb::TypeAppend< ColTypes5, type6 >::type ColTypes; \
 \
    template<template<int> class Col, class Init> struct Columns { \
        typename Col<0>::type name1; \
        typename Col<1>::type name2; \
        typename Col<2>::type name3; \
        typename Col<3>::type name4; \
        typename Col<4>::type name5; \
        typename Col<5>::type name6; \
        Columns(Init i): name1(i, #name1), name2(i, #name2), name3(i, #name3), name4(i, #name4), name5(i, #name5), name6(i, #name6) {} \
    }; \
 \
    template<class C, class T1, class T2, class T3, class T4, class T5, class T6> \
    static void insert(std::size_t i, const C& cols, const T1& v1, const T2& v2, const T3& v3, const T4& v4, const T5& v5, const T6& v6) \
    { \
        cols.name1._insert(i, v1); \
        cols.name2._insert(i, v2); \
        cols.name3._insert(i, v3); \
        cols.name4._insert(i, v4); \
        cols.name5._insert(i, v5); \
        cols.name6._insert(i, v6); \
    } \
}; \
typedef tightdb::BasicTable<Table##Spec> Table;


#define TIGHTDB_TABLE_7(Table, name1, type1, name2, type2, name3, type3, name4, type4, name5, type5, name6, type6, name7, type7) \
struct Table##Spec: tightdb::SpecBase { \
    typedef tightdb::TypeAppend< void, type1 >::type ColTypes1; \
    typedef tightdb::TypeAppend< ColTypes1, type2 >::type ColTypes2; \
    typedef tightdb::TypeAppend< ColTypes2, type3 >::type ColTypes3; \
    typedef tightdb::TypeAppend< ColTypes3, type4 >::type ColTypes4; \
    typedef tightdb::TypeAppend< ColTypes4, type5 >::type ColTypes5; \
    typedef tightdb::TypeAppend< ColTypes5, type6 >::type ColTypes6; \
    typedef tightdb::TypeAppend< ColTypes6, type7 >::type ColTypes; \
 \
    template<template<int> class Col, class Init> struct Columns { \
        typename Col<0>::type name1; \
        typename Col<1>::type name2; \
        typename Col<2>::type name3; \
        typename Col<3>::type name4; \
        typename Col<4>::type name5; \
        typename Col<5>::type name6; \
        typename Col<6>::type name7; \
        Columns(Init i): name1(i, #name1), name2(i, #name2), name3(i, #name3), name4(i, #name4), name5(i, #name5), name6(i, #name6), name7(i, #name7) {} \
    }; \
 \
    template<class C, class T1, class T2, class T3, class T4, class T5, class T6, class T7> \
    static void insert(std::size_t i, const C& cols, const T1& v1, const T2& v2, const T3& v3, const T4& v4, const T5& v5, const T6& v6, const T7& v7) \
    { \
        cols.name1._insert(i, v1); \
        cols.name2._insert(i, v2); \
        cols.name3._insert(i, v3); \
        cols.name4._insert(i, v4); \
        cols.name5._insert(i, v5); \
        cols.name6._insert(i, v6); \
        cols.name7._insert(i, v7); \
    } \
}; \
typedef tightdb::BasicTable<Table##Spec> Table;


#define TIGHTDB_TABLE_8(Table, name1, type1, name2, type2, name3, type3, name4, type4, name5, type5, name6, type6, name7, type7, name8, type8) \
struct Table##Spec: tightdb::SpecBase { \
    typedef tightdb::TypeAppend< void, type1 >::type ColTypes1; \
    typedef tightdb::TypeAppend< ColTypes1, type2 >::type ColTypes2; \
    typedef tightdb::TypeAppend< ColTypes2, type3 >::type ColTypes3; \
    typedef tightdb::TypeAppend< ColTypes3, type4 >::type ColTypes4; \
    typedef tightdb::TypeAppend< ColTypes4, type5 >::type ColTypes5; \
    typedef tightdb::TypeAppend< ColTypes5, type6 >::type ColTypes6; \
    typedef tightdb::TypeAppend< ColTypes6, type7 >::type ColTypes7; \
    typedef tightdb::TypeAppend< ColTypes7, type8 >::type ColTypes; \
 \
    template<template<int> class Col, class Init> struct Columns { \
        typename Col<0>::type name1; \
        typename Col<1>::type name2; \
        typename Col<2>::type name3; \
        typename Col<3>::type name4; \
        typename Col<4>::type name5; \
        typename Col<5>::type name6; \
        typename Col<6>::type name7; \
        typename Col<7>::type name8; \
        Columns(Init i): name1(i, #name1), name2(i, #name2), name3(i, #name3), name4(i, #name4), name5(i, #name5), name6(i, #name6), name7(i, #name7), name8(i, #name8) {} \
    }; \
 \
    template<class C, class T1, class T2, class T3, class T4, class T5, class T6, class T7, class T8> \
    static void insert(std::size_t i, const C& cols, const T1& v1, const T2& v2, const T3& v3, const T4& v4, const T5& v5, const T6& v6, const T7& v7, const T8& v8) \
    { \
        cols.name1._insert(i, v1); \
        cols.name2._insert(i, v2); \
        cols.name3._insert(i, v3); \
        cols.name4._insert(i, v4); \
        cols.name5._insert(i, v5); \
        cols.name6._insert(i, v6); \
        cols.name7._insert(i, v7); \
        cols.name8._insert(i, v8); \
    } \
}; \
typedef tightdb::BasicTable<Table##Spec> Table;


#endif // TIGHTDB_HPP
