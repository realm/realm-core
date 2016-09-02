/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <iostream>

#include <realm/table_accessors.hpp>
//#include <realm/query_expr.hpp>
#include "query_expr.hpp"


struct MySubtableSpec : realm::SpecBase {
    typedef typename realm::TypeAppend<void, int>::type Columns1;
    typedef typename realm::TypeAppend<Columns1, int>::type Columns;

    template <template <int> class Col, class Init>
    struct ColNames {
        typename Col<0>::type alpha;
        typename Col<1>::type beta;
        ColNames(Init i)
            : alpha(i)
            , beta(i)
        {
        }
    };

    static const char* const* dyn_col_names()
    {
        static const char* l[] = {"alpha", "beta"};
        return l;
    }
};

typedef realm::BasicTable<MySubtableSpec> MySubtable;


struct MyTableSpec : realm::SpecBase {
    typedef typename realm::TypeAppend<void, int>::type Columns1;
    typedef typename realm::TypeAppend<Columns1, int>::type Columns2;
    typedef typename realm::TypeAppend<Columns2, realm::SpecBase::Subtable<MySubtable>>::type Columns;

    template <template <int> class Col, class Init>
    struct ColNames {
        typename Col<0>::type foo;
        typename Col<1>::type bar;
        typename Col<2>::type baz;
        ColNames(Init i)
            : foo(i)
            , bar(i)
            , baz(i)
        {
        }
    };

    static const char* const* dyn_col_names()
    {
        static const char* l[] = {"foo", "bar", "baz"};
        return l;
    }
};

typedef realm::BasicTable<MyTableSpec> MyTable;


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
    std::cout << my_count(t) << std::endl;
    return 0;
}
