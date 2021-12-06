/*************************************************************************
 *
 * Copyright 2020 Realm Inc.
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

#include "testsettings.hpp"
#ifdef TEST_QUERY

#include <cstdlib> // itoa()
#include <initializer_list>
#include <limits>
#include <vector>
#include <chrono>

#include <realm.hpp>
#include <realm/column_integer.hpp>
#include <realm/array_bool.hpp>
#include <realm/history.hpp>
#include <realm/query_expression.hpp>
#include <realm/index_string.hpp>
#include <realm/query_expression.hpp>
#include "test.hpp"
#include "test_table_helper.hpp"
#include "test_types_helper.hpp"

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;


// Test independence and thread-safety
// -----------------------------------
//
// All tests must be thread safe and independent of each other. This
// is required because it allows for both shuffling of the execution
// order and for parallelized testing.
//
// In particular, avoid using std::rand() since it is not guaranteed
// to be thread safe. Instead use the API offered in
// `test/util/random.hpp`.
//
// All files created in tests must use the TEST_PATH macro (or one of
// its friends) to obtain a suitable file system path. See
// `test/util/test_path.hpp`.
//
//
// Debugging and the ONLY() macro
// ------------------------------
//
// A simple way of disabling all tests except one called `Foo`, is to
// replace TEST(Foo) with ONLY(Foo) and then recompile and rerun the
// test suite. Note that you can also use filtering by setting the
// environment varible `UNITTEST_FILTER`. See `README.md` for more on
// this.
//
// Another way to debug a particular test, is to copy that test into
// `experiments/testcase.cpp` and then run `sh build.sh
// check-testcase` (or one of its friends) from the command line.


TEST(Query_BigString)
{
    Table ttt;
    auto col_int = ttt.add_column(type_Int, "1");
    auto col_str = ttt.add_column(type_String, "2");

    ttt.create_object().set_all(1, "a");
    ObjKey res1 = ttt.where().equal(col_str, "a").find();
    CHECK_EQUAL(ttt.get_object(res1).get<Int>(col_int), 1);

    const char* medium_string = "40 chars  40 chars  40 chars  40 chars  ";
    ttt.create_object().set_all(2, medium_string);
    ObjKey res2 = ttt.where().equal(col_str, medium_string).find();
    CHECK_EQUAL(ttt.get_object(res2).get<Int>(col_int), 2);

    const char* long_string = "70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  ";
    ttt.create_object().set_all(3, long_string);
    ObjKey res3 = ttt.where().equal(col_str, long_string).find();
    CHECK_EQUAL(ttt.get_object(res3).get<Int>(col_int), 3);
}


TEST(Query_Limit)
{
    Table ttt;
    auto col_id = ttt.add_column(type_Int, "id");
    auto col_int = ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    ttt.create_object().set_all(0, 1, "a");
    ttt.create_object().set_all(1, 2, "a"); //
    ttt.create_object().set_all(2, 3, "X");
    ttt.create_object().set_all(3, 1, "a");
    ttt.create_object().set_all(4, 2, "a"); //
    ttt.create_object().set_all(5, 3, "X");
    ttt.create_object().set_all(6, 1, "a");
    ttt.create_object().set_all(7, 2, "a"); //
    ttt.create_object().set_all(8, 3, "X");
    ttt.create_object().set_all(9, 1, "a");
    ttt.create_object().set_all(10, 2, "a"); //
    ttt.create_object().set_all(11, 3, "X");
    ttt.create_object().set_all(12, 1, "a");
    ttt.create_object().set_all(13, 2, "a"); //
    ttt.create_object().set_all(14, 3, "X");

    Query q1 = ttt.where().equal(col_int, 2);

    TableView tv1 = q1.find_all(0, size_t(-1), 2);
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(1, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(4, tv1[1].get<Int>(col_id));

    TableView tv2 = q1.find_all(5, size_t(-1), 2);
    CHECK_EQUAL(2, tv2.size());
    CHECK_EQUAL(7, tv2[0].get<Int>(col_id));
    CHECK_EQUAL(10, tv2[1].get<Int>(col_id));

    TableView tv3 = q1.find_all(11, size_t(-1), 2);
    CHECK_EQUAL(1, tv3.size());
    CHECK_EQUAL(13, tv3[0].get<Int>(col_id));


    Query q2 = ttt.where();
    TableView tv4 = q2.find_all(0, 5, 3);
    CHECK_EQUAL(3, tv4.size());

    Query q3 = ttt.where();
    TableView tv5 = q3.find_all(0, 3, 5);
    CHECK_EQUAL(3, tv5.size());
}


TEST(Query_FindAll1)
{
    Table ttt;
    auto col_id = ttt.add_column(type_Int, "id");
    auto col_int = ttt.add_column(type_Int, "1");
    auto col_str = ttt.add_column(type_String, "2");

    ttt.create_object().set_all(0, 1, "a");
    ttt.create_object().set_all(1, 2, "a");
    ttt.create_object().set_all(2, 3, "X");
    ttt.create_object().set_all(3, 4, "a");
    ttt.create_object().set_all(4, 5, "a");
    ttt.create_object().set_all(5, 6, "X");
    ttt.create_object().set_all(6, 7, "X");

    Query q1 = ttt.where().equal(col_str, "a").greater(col_int, 2).not_equal(col_int, 4);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(4, tv1[0].get<Int>(col_id));

    Query q2 = ttt.where().equal(col_str, "X").greater(col_int, 4);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(tv2.size(), 2);
    CHECK_EQUAL(5, tv2[0].get<Int>(col_id));
    CHECK_EQUAL(6, tv2[1].get<Int>(col_id));
}

TEST(Query_FindAll2)
{
    Table ttt;
    auto col_id = ttt.add_column(type_Int, "id");
    auto col_int = ttt.add_column(type_Int, "1");
    auto col_str = ttt.add_column(type_String, "2");

    ttt.create_object().set_all(0, 1, "a");
    ttt.create_object().set_all(1, 2, "a");
    ttt.create_object().set_all(2, 3, "X");
    ttt.create_object().set_all(3, 4, "a");
    ttt.create_object().set_all(4, 5, "a");
    ttt.create_object().set_all(5, 11, "X");
    ttt.create_object().set_all(6, 0, "X");

    Query q2 = ttt.where().not_equal(col_str, "a").less(col_int, 3);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(tv2.size(), 1);
    CHECK_EQUAL(6, tv2[0].get<Int>(col_id));
}

TEST(Query_FindAllBetween)
{
    Table ttt;
    auto col_id = ttt.add_column(type_Int, "id");
    auto col_int = ttt.add_column(type_Int, "1");
    ttt.add_column(type_String, "2");

    ttt.create_object().set_all(0, 1, "a");
    ttt.create_object().set_all(1, 2, "a");
    ttt.create_object().set_all(2, 3, "X");
    ttt.create_object().set_all(3, 4, "a");
    ttt.create_object().set_all(4, 5, "a");
    ttt.create_object().set_all(5, 11, "X");
    ttt.create_object().set_all(6, 3, "X");

    Query q2 = ttt.where().between(col_int, 3, 5);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(tv2.size(), 4);
    CHECK_EQUAL(2, tv2[0].get<Int>(col_id));
    CHECK_EQUAL(3, tv2[1].get<Int>(col_id));
    CHECK_EQUAL(4, tv2[2].get<Int>(col_id));
    CHECK_EQUAL(6, tv2[3].get<Int>(col_id));
}


TEST(Query_FindAllOr)
{
    Table ttt;
    auto col_id = ttt.add_column(type_Int, "id");
    auto col_int = ttt.add_column(type_Int, "1");
    auto col_str = ttt.add_column(type_String, "2");

    ttt.create_object().set_all(0, 1, "a");
    ttt.create_object().set_all(1, 2, "a");
    ttt.create_object().set_all(2, 3, "X");
    ttt.create_object().set_all(3, 4, "a");
    ttt.create_object().set_all(4, 5, "a");
    ttt.create_object().set_all(5, 6, "a");
    ttt.create_object().set_all(6, 7, "X");
    ttt.create_object().set_all(7, 8, "z");

    // first == 5 || second == X
    Query q1 = ttt.where().equal(col_int, 5).Or().equal(col_str, "X");
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(3, tv1.size());
    CHECK_EQUAL(2, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(4, tv1[1].get<Int>(col_id));
    CHECK_EQUAL(6, tv1[2].get<Int>(col_id));

    // second == X || second == b || second == z || first == -1
    Query q2 =
        ttt.where().equal(col_str, "X").Or().equal(col_str, "b").Or().equal(col_str, "z").Or().equal(col_int, -1);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(3, tv2.size());
    CHECK_EQUAL(2, tv2[0].get<Int>(col_id));
    CHECK_EQUAL(6, tv2[1].get<Int>(col_id));
    CHECK_EQUAL(7, tv2[2].get<Int>(col_id));
}


TEST(Query_FindAllParens1)
{
    Table ttt;
    auto col_id = ttt.add_column(type_Int, "id");
    auto col_int = ttt.add_column(type_Int, "1");
    auto col_str = ttt.add_column(type_String, "2");

    ttt.create_object().set_all(0, 1, "a");
    ttt.create_object().set_all(1, 2, "a");
    ttt.create_object().set_all(2, 3, "X");
    ttt.create_object().set_all(3, 3, "X");
    ttt.create_object().set_all(4, 4, "a");
    ttt.create_object().set_all(5, 5, "a");
    ttt.create_object().set_all(6, 11, "X");

    // first > 3 && (second == X)
    Query q1 = ttt.where().greater(col_int, 3).group().equal(col_str, "X").end_group();
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(1, tv1.size());
    CHECK_EQUAL(6, tv1[0].get<Int>(col_id));
}


TEST(Query_FindAllOrParan)
{
    Table ttt;
    auto col_id = ttt.add_column(type_Int, "id");
    auto col_int = ttt.add_column(type_Int, "1");
    auto col_str = ttt.add_column(type_String, "2");

    ttt.create_object().set_all(0, 1, "a");
    ttt.create_object().set_all(1, 2, "a");
    ttt.create_object().set_all(2, 3, "X"); //
    ttt.create_object().set_all(3, 4, "a");
    ttt.create_object().set_all(4, 5, "a"); //
    ttt.create_object().set_all(5, 6, "a");
    ttt.create_object().set_all(6, 7, "X"); //
    ttt.create_object().set_all(7, 2, "X");

    // (first == 5 || second == X && first > 2)
    Query q1 = ttt.where().group().equal(col_int, 5).Or().equal(col_str, "X").greater(col_int, 2).end_group();
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(3, tv1.size());
    CHECK_EQUAL(2, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(4, tv1[1].get<Int>(col_id));
    CHECK_EQUAL(6, tv1[2].get<Int>(col_id));
}


TEST(Query_FindAllOrNested0)
{
    Table ttt;
    auto col_id = ttt.add_column(type_Int, "id");
    auto col_int = ttt.add_column(type_Int, "1");
    auto col_str = ttt.add_column(type_String, "2");

    ttt.create_object().set_all(0, 1, "a");
    ttt.create_object().set_all(1, 2, "a");
    ttt.create_object().set_all(2, 3, "X");
    ttt.create_object().set_all(3, 3, "X");
    ttt.create_object().set_all(4, 4, "a");
    ttt.create_object().set_all(5, 5, "a");
    ttt.create_object().set_all(6, 11, "X");
    ttt.create_object().set_all(7, 8, "Y");

    // first > 3 && (first == 5 || second == X)
    Query q1 = ttt.where().greater(col_int, 3).group().equal(col_int, 5).Or().equal(col_str, "X").end_group();
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(5, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(6, tv1[1].get<Int>(col_id));
}

TEST(Query_FindAllOrNested)
{
    Table ttt;
    auto col_id = ttt.add_column(type_Int, "id");
    auto col_int = ttt.add_column(type_Int, "1");
    auto col_str = ttt.add_column(type_String, "2");

    ttt.create_object().set_all(0, 1, "a");
    ttt.create_object().set_all(1, 2, "a");
    ttt.create_object().set_all(2, 3, "X");
    ttt.create_object().set_all(3, 3, "X");
    ttt.create_object().set_all(4, 4, "a");
    ttt.create_object().set_all(5, 5, "a");
    ttt.create_object().set_all(6, 11, "X");
    ttt.create_object().set_all(7, 8, "Y");

    // first > 3 && (first == 5 || second == X || second == Y)
    Query q1 = ttt.where()
                   .greater(col_int, 3)
                   .group()
                   .equal(col_int, 5)
                   .Or()
                   .equal(col_str, "X")
                   .Or()
                   .equal(col_str, "Y")
                   .end_group();
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(3, tv1.size());
    CHECK_EQUAL(5, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(6, tv1[1].get<Int>(col_id));
    CHECK_EQUAL(7, tv1[2].get<Int>(col_id));
}

TEST(Query_FindAllOrNestedInnerGroup)
{
    Table ttt;
    auto col_id = ttt.add_column(type_Int, "id");
    auto col_int = ttt.add_column(type_Int, "1");
    auto col_str = ttt.add_column(type_String, "2");

    ttt.create_object().set_all(0, 1, "a");
    ttt.create_object().set_all(1, 2, "a");
    ttt.create_object().set_all(2, 3, "X");
    ttt.create_object().set_all(3, 3, "X");
    ttt.create_object().set_all(4, 4, "a");
    ttt.create_object().set_all(5, 5, "a");
    ttt.create_object().set_all(6, 11, "X");
    ttt.create_object().set_all(7, 8, "Y");

    // first > 3 && (first == 5 || (second == X || second == Y))
    Query q1 = ttt.where()
                   .greater(col_int, 3)
                   .group()
                   .equal(col_int, 5)
                   .Or()
                   .group()
                   .equal(col_str, "X")
                   .Or()
                   .equal(col_str, "Y")
                   .end_group()
                   .end_group();
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(3, tv1.size());
    CHECK_EQUAL(5, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(6, tv1[1].get<Int>(col_id));
    CHECK_EQUAL(7, tv1[2].get<Int>(col_id));
}

TEST(Query_FindAllOrPHP)
{
    Table ttt;
    auto col_id = ttt.add_column(type_Int, "id");
    auto col_int = ttt.add_column(type_Int, "1");
    auto col_str = ttt.add_column(type_String, "2");

    ttt.create_object().set_all(0, 1, "Joe");
    ttt.create_object().set_all(1, 2, "Sara");
    ttt.create_object().set_all(2, 3, "Jim");

    // (second == Jim || second == Joe) && first = 1
    Query q1 = ttt.where().group().equal(col_str, "Jim").Or().equal(col_str, "Joe").end_group().equal(col_int, 1);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(0, tv1[0].get<Int>(col_id));

    q1 = ttt.where().group().equal(col_str, "Jim").Or().equal(col_str, "Joe").end_group().equal(col_int, 3);
    tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1[0].get<Int>(col_id));
}

TEST(Query_FindAllParens2)
{
    Table ttt;
    auto col_id = ttt.add_column(type_Int, "id");
    auto col_int = ttt.add_column(type_Int, "1");

    ttt.create_object().set_all(0, 1);
    ttt.create_object().set_all(1, 2);
    ttt.create_object().set_all(2, 3);
    ttt.create_object().set_all(3, 3);
    ttt.create_object().set_all(4, 4);
    ttt.create_object().set_all(5, 5);
    ttt.create_object().set_all(6, 11);

    // ()
    Query q1 = ttt.where().group().end_group();
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(7, tv1.size());

    // ()((first > 3()) && (()))
    q1 = ttt.where()
             .group()
             .end_group()
             .group()
             .group()
             .greater(col_int, 3)
             .group()
             .end_group()
             .end_group()
             .group()
             .group()
             .end_group()
             .end_group()
             .end_group();
    tv1 = q1.find_all();
    CHECK_EQUAL(3, tv1.size());
    CHECK_EQUAL(4, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(5, tv1[1].get<Int>(col_id));
    CHECK_EQUAL(6, tv1[2].get<Int>(col_id));
}


TEST(Query_FindAllBool)
{
    Table table;
    auto col_id = table.add_column(type_Int, "id");
    auto col_bool = table.add_column(type_Bool, "2");

    table.create_object().set_all(0, true);
    table.create_object().set_all(1, false);
    table.create_object().set_all(2, true);
    table.create_object().set_all(3, false);

    Query q1 = table.where().equal(col_bool, true);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(0, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(2, tv1[1].get<Int>(col_id));

    Query q2 = table.where().equal(col_bool, false);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(1, tv2[0].get<Int>(col_id));
    CHECK_EQUAL(3, tv2[1].get<Int>(col_id));
}

TEST(Query_FindAllBegins)
{
    Table table;
    auto col_id = table.add_column(type_Int, "id");
    auto col_str = table.add_column(type_String, "2");

    table.create_object().set_all(0, "fo");
    table.create_object().set_all(1, "foo");
    table.create_object().set_all(2, "foobar");

    Query q1 = table.where().begins_with(col_str, StringData("foo"));
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(1, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(2, tv1[1].get<Int>(col_id));
}

TEST(Query_FindAllEnds)
{
    Table table;
    auto col_id = table.add_column(type_Int, "id");
    auto col_str = table.add_column(type_String, "2");

    table.create_object().set_all(0, "barfo");
    table.create_object().set_all(1, "barfoo");
    table.create_object().set_all(2, "barfoobar");

    Query q1 = table.where().ends_with(col_str, StringData("foo"));
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(1, tv1.size());
    CHECK_EQUAL(1, tv1[0].get<Int>(col_id));
}


TEST(Query_FindAllContains)
{
    Table table;
    auto col_id = table.add_column(type_Int, "id");
    auto col_str = table.add_column(type_String, "2");

    table.create_object().set_all(0, "foo");
    table.create_object().set_all(1, "foobar");
    table.create_object().set_all(2, "barfoo");
    table.create_object().set_all(3, "barfoobaz");
    table.create_object().set_all(4, "fo");
    table.create_object().set_all(5, "fobar");
    table.create_object().set_all(6, "barfo");

    Query q1 = table.where().contains(col_str, StringData("foo"));
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(4, tv1.size());
    CHECK_EQUAL(0, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(1, tv1[1].get<Int>(col_id));
    CHECK_EQUAL(2, tv1[2].get<Int>(col_id));
    CHECK_EQUAL(3, tv1[3].get<Int>(col_id));

    q1 = table.where().like(col_str, StringData("*foo*"));
    tv1 = q1.find_all();
    CHECK_EQUAL(4, tv1.size());
    CHECK_EQUAL(0, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(1, tv1[1].get<Int>(col_id));
    CHECK_EQUAL(2, tv1[2].get<Int>(col_id));
    CHECK_EQUAL(3, tv1[3].get<Int>(col_id));
}

TEST(Query_FindAllLikeStackOverflow)
{
    std::string str(100000, 'x');
    StringData sd(str);

    Table table;
    auto col = table.add_column(type_String, "strings");
    ObjKey k = table.create_object().set(col, sd).get_key();

    auto res = table.where().like(col, sd).find();
    CHECK_EQUAL(res, k);
}

TEST(Query_FindAllLikeCaseInsensitive)
{
    Table table;
    auto col_id = table.add_column(type_Int, "id");
    auto col_str = table.add_column(type_String, "2");

    table.create_object().set_all(0, "Foo");
    table.create_object().set_all(1, "FOOBAR");
    table.create_object().set_all(2, "BaRfOo");
    table.create_object().set_all(3, "barFOObaz");
    table.create_object().set_all(4, "Fo");
    table.create_object().set_all(5, "Fobar");
    table.create_object().set_all(6, "baRFo");

    Query q1 = table.where().like(col_str, StringData("*foo*"), false);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(4, tv1.size());
    CHECK_EQUAL(0, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(1, tv1[1].get<Int>(col_id));
    CHECK_EQUAL(2, tv1[2].get<Int>(col_id));
    CHECK_EQUAL(3, tv1[3].get<Int>(col_id));
}

TEST(Query_Binary)
{
    Table t;
    t.add_column(type_Int, "1");
    auto c1 = t.add_column(type_Binary, "2");

    const char bin[64] = {6, 3, 9, 5, 9, 7, 6, 3, 2, 6, 0, 0, 5, 4, 2, 4, 5, 7, 9, 5, 7, 1,
                          1, 2, 0, 8, 3, 8, 0, 9, 6, 8, 4, 7, 3, 4, 9, 5, 2, 3, 6, 2, 7, 4,
                          0, 3, 7, 6, 2, 3, 5, 9, 3, 1, 2, 1, 0, 5, 5, 2, 9, 4, 5, 9};

    const char bin_2[4] = {6, 6, 6, 6}; // Not occuring above

    std::vector<ObjKey> keys;
    t.create_objects(9, keys);

    t.get_object(keys[0]).set_all(0, BinaryData(bin + 0, 16));
    t.get_object(keys[1]).set_all(0, BinaryData(bin + 0, 32));
    t.get_object(keys[2]).set_all(0, BinaryData(bin + 0, 48));
    t.get_object(keys[3]).set_all(0, BinaryData(bin + 0, 64));
    t.get_object(keys[4]).set_all(0, BinaryData(bin + 16, 48));
    t.get_object(keys[5]).set_all(0, BinaryData(bin + 32, 32));
    t.get_object(keys[6]).set_all(0, BinaryData(bin + 48, 16));
    t.get_object(keys[7]).set_all(0, BinaryData(bin + 24, 16)); // The "odd ball"
    t.get_object(keys[8]).set_all(0, BinaryData(bin + 0, 32));  // Repeat an entry

    CHECK_EQUAL(0, t.where().equal(c1, BinaryData(bin + 16, 16)).count());
    CHECK_EQUAL(1, t.where().equal(c1, BinaryData(bin + 0, 16)).count());
    CHECK_EQUAL(1, t.where().equal(c1, BinaryData(bin + 48, 16)).count());
    CHECK_EQUAL(2, t.where().equal(c1, BinaryData(bin + 0, 32)).count());

    CHECK_EQUAL(9, t.where().not_equal(c1, BinaryData(bin + 16, 16)).count());
    CHECK_EQUAL(8, t.where().not_equal(c1, BinaryData(bin + 0, 16)).count());

    CHECK_EQUAL(0, t.where().begins_with(c1, BinaryData(bin + 8, 16)).count());
    CHECK_EQUAL(1, t.where().begins_with(c1, BinaryData(bin + 16, 16)).count());
    CHECK_EQUAL(4, t.where().begins_with(c1, BinaryData(bin + 0, 32)).count());
    CHECK_EQUAL(5, t.where().begins_with(c1, BinaryData(bin + 0, 16)).count());
    CHECK_EQUAL(1, t.where().begins_with(c1, BinaryData(bin + 48, 16)).count());
    CHECK_EQUAL(9, t.where().begins_with(c1, BinaryData(bin + 0, 0)).count());

    CHECK_EQUAL(0, t.where().ends_with(c1, BinaryData(bin + 40, 16)).count());
    CHECK_EQUAL(1, t.where().ends_with(c1, BinaryData(bin + 32, 16)).count());
    CHECK_EQUAL(3, t.where().ends_with(c1, BinaryData(bin + 32, 32)).count());
    CHECK_EQUAL(4, t.where().ends_with(c1, BinaryData(bin + 48, 16)).count());
    CHECK_EQUAL(1, t.where().ends_with(c1, BinaryData(bin + 0, 16)).count());
    CHECK_EQUAL(9, t.where().ends_with(c1, BinaryData(bin + 64, 0)).count());

    CHECK_EQUAL(0, t.where().contains(c1, BinaryData(bin_2)).count());
    CHECK_EQUAL(5, t.where().contains(c1, BinaryData(bin + 0, 16)).count());
    CHECK_EQUAL(5, t.where().contains(c1, BinaryData(bin + 16, 16)).count());
    CHECK_EQUAL(4, t.where().contains(c1, BinaryData(bin + 24, 16)).count());
    CHECK_EQUAL(4, t.where().contains(c1, BinaryData(bin + 32, 16)).count());
    CHECK_EQUAL(9, t.where().contains(c1, BinaryData(bin + 0, 0)).count());

    {
        TableView tv = t.where().equal(c1, BinaryData(bin + 0, 32)).find_all();
        if (tv.size() == 2) {
            CHECK_EQUAL(keys[1], tv.get_key(0));
            CHECK_EQUAL(keys[8], tv.get_key(1));
        }
        else
            CHECK(false);
    }

    {
        TableView tv = t.where().contains(c1, BinaryData(bin + 24, 16)).find_all();
        if (tv.size() == 4) {
            CHECK_EQUAL(keys[2], tv.get_key(0));
            CHECK_EQUAL(keys[3], tv.get_key(1));
            CHECK_EQUAL(keys[4], tv.get_key(2));
            CHECK_EQUAL(keys[7], tv.get_key(3));
        }
        else
            CHECK(false);
    }
}

TEST(Query_Enums)
{
    Table table;
    auto col_int = table.add_column(type_Int, "1");
    auto col_str = table.add_column(type_String, "2");


    for (size_t i = 0; i < 5; ++i) {
        table.create_object().set_all(1, "abd");
        table.create_object().set_all(2, "eftg");
        table.create_object().set_all(5, "hijkl");
        table.create_object().set_all(8, "mnopqr");
        table.create_object().set_all(9, "stuvxyz");
    }

    table.enumerate_string_column(col_str);

    Query q1 = table.where().equal(col_str, "eftg");
    TableView tv1 = q1.find_all();

    CHECK_EQUAL(5, tv1.size());
    CHECK_EQUAL(2, tv1[0].get<Int>(col_int));
    CHECK_EQUAL(2, tv1[1].get<Int>(col_int));
    CHECK_EQUAL(2, tv1[2].get<Int>(col_int));
    CHECK_EQUAL(2, tv1[3].get<Int>(col_int));
    CHECK_EQUAL(2, tv1[4].get<Int>(col_int));
}


TEST_TYPES(Query_CaseSensitivity, std::true_type, std::false_type)
{
    constexpr bool nullable = TEST_TYPE::value;

    Table ttt;
    auto col = ttt.add_column(type_String, "2", nullable);

    ObjKey k = ttt.create_object().set(col, "BLAAbaergroed").get_key();
    ttt.create_object().set(col, "BLAAbaergroedandMORE");
    ttt.create_object().set(col, "BLAAbaergroedZ");
    ttt.create_object().set(col, "BLAAbaergroedZ");
    ttt.create_object().set(col, "BLAAbaergroedZ");

    Query q1 = ttt.where().equal(col, "blaabaerGROED", false);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(1, tv1.size());
    CHECK_EQUAL(k, tv1.get_key(0));

    Query q2 = ttt.where().equal(col, "blaabaerGROEDz", false);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(3, tv2.size());

    ttt.add_search_index(col);

    Query q3 = ttt.where().equal(col, "blaabaerGROEDz", false);
    TableView tv3 = q3.find_all();
    CHECK_EQUAL(3, tv3.size());
}

#define uY "\x0CE\x0AB"            // greek capital letter upsilon with dialytika (U+03AB)
#define uYd "\x0CE\x0A5\x0CC\x088" // decomposed form (Y followed by two dots)
#define uy "\x0CF\x08B"            // greek small letter upsilon with dialytika (U+03AB)
#define uyd "\x0cf\x085\x0CC\x088" // decomposed form (Y followed by two dots)

#define uA "\x0c3\x085"       // danish capital A with ring above (as in BLAABAERGROED)
#define uAd "\x041\x0cc\x08a" // decomposed form (A (41) followed by ring)
#define ua "\x0c3\x0a5"       // danish lower case a with ring above (as in blaabaergroed)
#define uad "\x061\x0cc\x08a" // decomposed form (a (41) followed by ring)

#if (defined(_WIN32) || defined(__WIN32__) || defined(_WIN64))

TEST(Query_Unicode2)
{
    Table table;
    auto col_id = table.add_column(type_Int, "id");
    auto col_str = table.add_column(type_String, "2");

    table.create_object().set_all(0, uY);
    table.create_object().set_all(1, uYd);
    table.create_object().set_all(2, uy);
    table.create_object().set_all(3, uyd);

    Query q1 = table.where().equal(col_str, uY, false);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(0, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(2, tv1[1].get<Int>(col_id));

    Query q2 = table.where().equal(col_str, uYd, false);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(2, tv2.size());
    CHECK_EQUAL(1, tv2[0].get<Int>(col_id));
    CHECK_EQUAL(3, tv2[1].get<Int>(col_id));

    Query q3 = table.where().equal(col_str, uYd, true);
    TableView tv3 = q3.find_all();
    CHECK_EQUAL(1, tv3.size());
    CHECK_EQUAL(1, tv3[0].get<Int>(col_id));
}


TEST(Query_Unicode3)
{
    Table table;
    auto col_id = table.add_column(type_Int, "id");
    auto col_str = table.add_column(type_String, "2");

    table.create_object().set_all(0, uA);
    table.create_object().set_all(1, uAd);
    table.create_object().set_all(2, ua);
    table.create_object().set_all(3, uad);

    Query q1 = table.where().equal(col_str, uA, false);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(0, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(2, tv1[1].get<Int>(col_id));

    Query q2 = table.where().equal(col_str, ua, false);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(2, tv2.size());
    CHECK_EQUAL(0, tv2[0].get<Int>(col_id));
    CHECK_EQUAL(2, tv2[1].get<Int>(col_id));

    Query q3 = table.where().equal(col_str, uad, false);
    TableView tv3 = q3.find_all();
    CHECK_EQUAL(2, tv3.size());
    CHECK_EQUAL(1, tv3[0].get<Int>(col_id));
    CHECK_EQUAL(3, tv3[1].get<Int>(col_id));

    Query q4 = table.where().equal(col_str, uad, true);
    TableView tv4 = q4.find_all();
    CHECK_EQUAL(1, tv4.size());
    CHECK_EQUAL(3, tv4[0].get<Int>(col_id));
}

#endif

TEST(Query_FindAllBeginsUnicode)
{
    Table table;
    auto col_id = table.add_column(type_Int, "id");
    auto col_str = table.add_column(type_String, "2");

    table.create_object().set_all(0, uad "fo");
    table.create_object().set_all(1, uad "foo");
    table.create_object().set_all(2, uad "foobar");

    Query q1 = table.where().begins_with(col_str, StringData(uad "foo"));
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(1, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(2, tv1[1].get<Int>(col_id));
}


TEST(Query_FindAllEndsUnicode)
{
    Table table;
    auto col_id = table.add_column(type_Int, "id");
    auto col_str = table.add_column(type_String, "2");

    table.create_object().set_all(0, "barfo");
    table.create_object().set_all(1, "barfoo" uad);
    table.create_object().set_all(2, "barfoobar");

    Query q1 = table.where().ends_with(col_str, StringData("foo" uad));
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(1, tv1.size());
    CHECK_EQUAL(1, tv1[0].get<Int>(col_id));

    Query q2 = table.where().ends_with(col_str, StringData("foo" uAd), false);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(1, tv2.size());
    CHECK_EQUAL(1, tv2[0].get<Int>(col_id));
}


TEST(Query_FindAllContainsUnicode)
{
    Table table;
    auto col_id = table.add_column(type_Int, "id");
    auto col_str = table.add_column(type_String, "2");

    table.create_object().set_all(0, uad "foo");
    table.create_object().set_all(1, uad "foobar");
    table.create_object().set_all(2, "bar" uad "foo");
    table.create_object().set_all(3, uad "bar" uad "foobaz");
    table.create_object().set_all(4, uad "fo");
    table.create_object().set_all(5, uad "fobar");
    table.create_object().set_all(6, uad "barfo");

    Query q1 = table.where().contains(col_str, StringData(uad "foo"));
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(4, tv1.size());
    CHECK_EQUAL(0, tv1[0].get<Int>(col_id));
    CHECK_EQUAL(1, tv1[1].get<Int>(col_id));
    CHECK_EQUAL(2, tv1[2].get<Int>(col_id));
    CHECK_EQUAL(3, tv1[3].get<Int>(col_id));

    Query q2 = table.where().contains(col_str, StringData(uAd "foo"), false);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(4, tv2.size());
    CHECK_EQUAL(0, tv2[0].get<Int>(col_id));
    CHECK_EQUAL(1, tv2[1].get<Int>(col_id));
    CHECK_EQUAL(2, tv2[2].get<Int>(col_id));
    CHECK_EQUAL(3, tv2[3].get<Int>(col_id));
}

TEST(Query_SyntaxCheck)
{
    Table table;
    auto col_int = table.add_column(type_Int, "1");
    table.add_column(type_String, "2");

    std::string s;

    table.create_object().set_all(1, "a");
    table.create_object().set_all(2, "a");
    table.create_object().set_all(3, "X");

    Query q1 = table.where().equal(col_int, 2).end_group();
    s = q1.validate();
    CHECK(s != "");

    Query q2 = table.where().group().group().equal(col_int, 2).end_group();
    s = q2.validate();
    CHECK(s != "");

    Query q3 = table.where().equal(col_int, 2).Or();
    s = q3.validate();
    CHECK(s != "");

    Query q4 = table.where().Or().equal(col_int, 2);
    s = q4.validate();
    CHECK(s != "");

    Query q5 = table.where().equal(col_int, 2);
    s = q5.validate();
    CHECK(s == "");

    Query q6 = table.where().group().equal(col_int, 2);
    s = q6.validate();
    CHECK(s != "");

    // FIXME: Work is currently underway to fully support locale
    // independent case folding as defined by Unicode. Reenable this test
    // when is becomes available.
    /*
    Query q7 = ttt.where().equal(1, "\xa0", false);
#ifdef REALM_DEBUG
    s = q7.verify();
    CHECK(s != "");
#endif
    */
}

TEST(Query_TestTV_where)
{
    // When using .where(&tv), tv can have any order, and the resulting view will retain its order
    Table table;
    auto col_int = table.add_column(type_Int, "1");
    auto col_str = table.add_column(type_String, "2");

    table.create_object().set_all(1, "a");
    table.create_object().set_all(2, "a");
    table.create_object().set_all(3, "c");

    TableView v = table.where().greater(col_int, 1).find_all();

    Query q1 = table.where(&v);
    CHECK_EQUAL(2, q1.count());

    Query q3 = table.where(&v).equal(col_str, "a");
    CHECK_EQUAL(1, q3.count());

    Query q4 = table.where(&v).between(col_int, 3, 6);
    CHECK_EQUAL(1, q4.count());
}


TEST(Query_SumMinMaxAvg)
{
    Table t;

    auto int_col = t.add_column(type_Int, "1");
    auto date_col = t.add_column(type_Timestamp, "3");
    auto float_col = t.add_column(type_Float, "4");
    auto double_col = t.add_column(type_Double, "5");
    auto decimal_col = t.add_column(type_Decimal, "6");
    auto mixed_col = t.add_column(type_Mixed, "7");

    std::vector<ObjKey> keys;
    t.create_objects(9, keys);
    t.get_object(keys[0]).set_all(1, Timestamp{200, 0}, 1.0f, 2.0, Decimal128{1.1}, Mixed{Decimal128{1.0}});
    t.get_object(keys[1]).set_all(1, Timestamp{100, 0}, 1.0f, 1.0, Decimal128{2.2}, Mixed{1.0f});
    t.get_object(keys[2]).set_all(1, Timestamp{100, 0}, 1.0f, 1.0, Decimal128{3.3}, Mixed{2.2f});
    t.get_object(keys[3]).set_all(1, Timestamp{100, 0}, 1.0f, 1.0, Decimal128{4.4}, Mixed{Decimal128{2.2}});
    t.get_object(keys[4]).set_all(2, Timestamp{300, 0}, 3.0f, 3.0, Decimal128{5.5}, Mixed{StringData("foo")});
    t.get_object(keys[5]).set_all(3, Timestamp{50, 0}, 5.0f, 5.0, Decimal128{6.6}, Mixed{Timestamp()});
    t.get_object(keys[6]).set_all(0, Timestamp{100, 0}, 1.0f, 1.0, Decimal128{7.7}, Mixed{});
    t.get_object(keys[7]).set_all(0, Timestamp{3000, 0}, 30.0f, 30.0, Decimal128{8.8}, Mixed{42});
    t.get_object(keys[8]).set_all(0, Timestamp{5, 0}, 0.5f, 0.5, Decimal128{9.9}, Mixed{0.1});

    CHECK_EQUAL(9, t.where().sum_int(int_col));

    CHECK_EQUAL(0, t.where().minimum_int(int_col));
    CHECK_EQUAL(3, t.where().maximum_int(int_col));
    CHECK_EQUAL(Decimal128{9.9}, t.where().maximum_decimal128(decimal_col));
    CHECK_EQUAL(Mixed{"foo"}, t.where().maximum_mixed(mixed_col));
    CHECK_EQUAL(Decimal128{1.1}, t.where().minimum_decimal128(decimal_col));
    CHECK_EQUAL(Mixed{0.1}, t.where().minimum_mixed(mixed_col));
    CHECK_EQUAL(Decimal128{49.5}, t.where().sum_decimal128(decimal_col));
    CHECK_EQUAL(Mixed{48.5}, t.where().sum_mixed(mixed_col));
    CHECK_EQUAL(Decimal128{49.5 / 9}, t.where().average_decimal128(decimal_col));
    Decimal128 avg_mixed = t.where().average_mixed(mixed_col);
    Decimal128 expected_avg_mixed = Decimal128{48.5 / 6};
    Decimal128 allowed_epsilon{0.001};
    CHECK(avg_mixed <= (expected_avg_mixed + allowed_epsilon) && avg_mixed >= (expected_avg_mixed - allowed_epsilon));
    t.get_object(keys[6]).set<Mixed>(mixed_col, Mixed{false});
    CHECK_EQUAL(Mixed{false}, t.where().minimum_mixed(mixed_col));

    ObjKey resindex;

    t.where().maximum_int(int_col, &resindex);
    CHECK_EQUAL(keys[5], resindex);

    t.where().minimum_int(int_col, &resindex);
    CHECK_EQUAL(keys[6], resindex);

    t.where().maximum_float(float_col, &resindex);
    CHECK_EQUAL(keys[7], resindex);

    t.where().minimum_float(float_col, &resindex);
    CHECK_EQUAL(keys[8], resindex);

    t.where().maximum_double(double_col, &resindex);
    CHECK_EQUAL(keys[7], resindex);

    t.where().minimum_double(double_col, &resindex);
    CHECK_EQUAL(keys[8], resindex);

    t.where().maximum_timestamp(date_col, &resindex);
    CHECK_EQUAL(keys[7], resindex);

    t.where().minimum_timestamp(date_col, &resindex);
    CHECK_EQUAL(keys[8], resindex);

    // Now with condition (tests another code path in Array::minmax())
    t.where().not_equal(int_col, 0).minimum_double(double_col, &resindex);
    CHECK_EQUAL(keys[1], resindex);

    t.where().not_equal(int_col, 0).minimum_float(float_col, &resindex);
    CHECK_EQUAL(keys[0], resindex);

    t.where().not_equal(int_col, 0).minimum_timestamp(date_col, &resindex);
    CHECK_EQUAL(keys[5], resindex);

    t.where().not_equal(int_col, 0).maximum_timestamp(date_col, &resindex);
    CHECK_EQUAL(keys[4], resindex);

    CHECK_APPROXIMATELY_EQUAL(1, t.where().average_int(int_col), 0.001);

    CHECK_EQUAL(t.where().maximum_timestamp(date_col), Timestamp(3000, 0));
    CHECK_EQUAL(t.where().minimum_timestamp(date_col), Timestamp(5, 0));
}


TEST(Query_Avg)
{
    Table t;
    auto col = t.add_column(type_Int, "1");

    t.create_object().set(col, 10);
    CHECK_EQUAL(10, t.where().average_int(col));
    t.create_object().set(col, 30);
    CHECK_EQUAL(20, t.where().average_int(col));
}

TEST(Query_Avg2)
{
    Table t;
    auto col_int = t.add_column(type_Int, "1");
    auto col_str = t.add_column(type_String, "2");

    size_t cnt;

    t.create_object().set_all(10, "a");
    t.create_object().set_all(100, "b");
    t.create_object().set_all(20, "a");
    t.create_object().set_all(100, "b");
    t.create_object().set_all(100, "b");
    t.create_object().set_all(30, "a");

    CHECK_EQUAL(60, t.where().equal(col_str, "a").sum_int(col_int));

    CHECK_EQUAL(20, t.where().equal(col_str, "a").average_int(col_int, &cnt));
    CHECK_EQUAL(3, cnt);
    CHECK_EQUAL(100, t.where().equal(col_str, "b").average_int(col_int, &cnt));
    CHECK_EQUAL(3, cnt);
}


TEST(Query_OfByOne)
{
    Table t;
    auto col_int = t.add_column(type_Int, "1");
    t.add_column(type_String, "2");
    size_t cluster_size = (REALM_MAX_BPNODE_SIZE > 256) ? 256 : 4;
    for (size_t i = 0; i < cluster_size * 2; ++i) {
        t.create_object().set_all(1, "a");
    }

    // Top
    Obj obj0 = t.get_object(0);
    obj0.set(col_int, 0);
    ObjKey res = t.where().equal(col_int, 0).find();
    CHECK_EQUAL(obj0.get_key(), res);
    obj0.set(col_int, 1); // reset

    // Before split
    Obj obj1 = t.get_object(cluster_size - 1);
    obj1.set(col_int, 0);
    res = t.where().equal(col_int, 0).find();
    CHECK_EQUAL(obj1.get_key(), res);
    obj1.set(col_int, 1); // reset

    // After split
    Obj obj2 = t.get_object(cluster_size);
    obj2.set(col_int, 0);
    res = t.where().equal(col_int, 0).find();
    CHECK_EQUAL(obj2.get_key(), res);
    obj2.set(col_int, 1); // reset

    // Before end
    Obj obj3 = t.get_object((cluster_size * 2) - 1);
    obj3.set(col_int, 0);
    res = t.where().equal(col_int, 0).find();
    CHECK_EQUAL(obj3.get_key(), res);
    obj3.set(col_int, 1); // reset
}


TEST(Query_AllTypesDynamicallyTyped)
{
    for (int nullable = 0; nullable < 2; nullable++) {
        bool n = (nullable == 1);

        Table table;
        auto col_boo = table.add_column(type_Bool, "boo", n);
        auto col_int = table.add_column(type_Int, "int", n);
        auto col_flt = table.add_column(type_Float, "flt", n);
        auto col_dbl = table.add_column(type_Double, "dbl", n);
        auto col_str = table.add_column(type_String, "str", n);
        auto col_bin = table.add_column(type_Binary, "bin", n);
        auto col_dat = table.add_column(type_Timestamp, "dat", n);
        auto col_lst = table.add_column_list(type_Int, "int_list");

        const char bin[4] = {0, 1, 2, 3};
        BinaryData bin1(bin, sizeof bin / 2);
        BinaryData bin2(bin, sizeof bin);
        Timestamp time_now(time(nullptr), 0);

        Obj obj0 = table.create_object().set_all(false, 54, 0.7f, 0.8, StringData("foo"), bin1, Timestamp(0, 0));
        Obj obj1 = table.create_object().set_all(true, 506, 7.7f, 8.8, StringData("banach"), bin2, time_now);
        obj1.get_list<Int>(col_lst).add(100);

        CHECK_EQUAL(1, table.where().equal(col_boo, false).count());
        CHECK_EQUAL(1, table.where().equal(col_int, int64_t(54)).count());
        CHECK_EQUAL(1, table.where().equal(col_flt, 0.7f).count());
        CHECK_EQUAL(1, table.where().equal(col_dbl, 0.8).count());
        CHECK_EQUAL(1, table.where().equal(col_str, "foo").count());
        CHECK_EQUAL(1, table.where().equal(col_bin, bin1).count());
        CHECK_EQUAL(1, table.where().equal(col_dat, Timestamp(0, 0)).count());
        //    CHECK_EQUAL(1, table.where().equal(7, subtab).count());
        //    CHECK_EQUAL(1, table.where().equal(8, mix_int).count());

        Query query = table.where().equal(col_boo, false);

        ObjKey ndx;

        CHECK_EQUAL(54, query.minimum_int(col_int));
        query.minimum_int(col_int, &ndx);
        CHECK_EQUAL(obj0.get_key(), ndx);

        CHECK_EQUAL(54, query.maximum_int(col_int));
        query.maximum_int(col_int, &ndx);
        CHECK_EQUAL(obj0.get_key(), ndx);

        CHECK_EQUAL(54, query.sum_int(col_int));
        CHECK_EQUAL(54, query.average_int(col_int));

        CHECK_EQUAL(0.7f, query.minimum_float(col_flt));
        query.minimum_float(col_flt, &ndx);
        CHECK_EQUAL(obj0.get_key(), ndx);

        CHECK_EQUAL(0.7f, query.maximum_float(col_flt));
        query.maximum_float(col_flt, &ndx);
        CHECK_EQUAL(obj0.get_key(), ndx);

        CHECK_EQUAL(0.7f, query.sum_float(col_flt));
        CHECK_EQUAL(0.7f, query.average_float(col_flt));

        CHECK_EQUAL(0.8, query.minimum_double(col_dbl));
        query.minimum_double(col_dbl, &ndx);
        CHECK_EQUAL(obj0.get_key(), ndx);

        CHECK_EQUAL(0.8, query.maximum_double(col_dbl));
        query.maximum_double(col_dbl, &ndx);
        CHECK_EQUAL(obj0.get_key(), ndx);

        CHECK_EQUAL(0.8, query.sum_double(col_dbl));
        CHECK_EQUAL(0.8, query.average_double(col_dbl));
    }
}


TEST(Query_AggregateSortedView)
{
    Table table;
    auto col = table.add_column(type_Double, "col");

    const int count = REALM_MAX_BPNODE_SIZE * 2;
    for (int i = 0; i < count; ++i)
        table.create_object().set(col, double(i + 1)); // no 0s to reduce chance of passing by coincidence

    TableView tv = table.where().greater(col, 1.0).find_all();
    tv.sort(col, false);

    CHECK_EQUAL(2.0, tv.minimum_double(col));
    CHECK_EQUAL(count, tv.maximum_double(col));
    CHECK_APPROXIMATELY_EQUAL((count + 1) * count / 2, tv.sum_double(col), .1);
}


TEST(Query_DeepCopy)
{
    // NOTE: You can only create a copy of a fully constructed; i.e. you cannot copy a query which is missing an
    // end_group(). Run Query::validate() to see if it's fully constructed.

    Table t;
    auto col_int = t.add_column(type_Int, "1");
    auto col_str = t.add_column(type_String, "2");
    auto col_dbl = t.add_column(type_Double, "3");

    ObjKey k0 = t.create_object().set_all(1, "1", 1.1).get_key();
    t.create_object().set_all(2, "2", 2.2);
    ObjKey k2 = t.create_object().set_all(3, "3", 3.3).get_key();
    ObjKey k3 = t.create_object().set_all(4, "4", 4.4).get_key();

    // Explicit use of Value<>() makes query_expression node instead of query_engine.
    Query q = t.column<Int>(col_int) > Value<Int>(2);


    // Test if we can execute a copy
    Query q2(q);

    CHECK_EQUAL(k2, q2.find());


    // See if we can execute a copy of a deleted query. The copy should not contain references to the original.
    Query* q3 = new Query(q);
    Query* q4 = new Query(*q3);
    delete q3;


    // Attempt to overwrite memory of the deleted q3 by allocating various sized objects so that a spurious execution
    // of methods on q3 can be detected (by making unit test crash).
    char* tmp[1000];
    for (size_t i = 0; i < sizeof(tmp) / sizeof(tmp[0]); i++) {
        tmp[i] = new char[i];
        memset(tmp[i], 0, i);
    }
    for (size_t i = 0; i < sizeof(tmp) / sizeof(tmp[0]); i++) {
        delete[] tmp[i];
    }

    CHECK_EQUAL(k2, q4->find());
    delete q4;

    // See if we can append a criteria to a query
    // Explicit use of Value<>() makes query_expression node instead of query_engine
    Query q5 = t.column<Int>(col_int) > Value<Int>(2);
    q5.greater(col_dbl, 4.0);
    CHECK_EQUAL(k3, q5.find());

    // See if we can append a criteria to a copy without modifying the original (copy should not contain references
    // to original). Tests query_expression integer node.
    // Explicit use of Value<>() makes query_expression node instead of query_engine
    Query q6 = t.column<Int>(col_int) > Value<Int>(2);
    Query q7(q6);

    q7.greater(col_dbl, 4.0);
    CHECK_EQUAL(k3, q7.find());
    CHECK_EQUAL(k2, q6.find());


    // See if we can append a criteria to a copy without modifying the original (copy should not contain references
    // to original). Tests query_engine integer node.
    Query q8 = t.column<Int>(col_int) > 2;
    Query q9(q8);

    q9.greater(col_dbl, 4.0);
    CHECK_EQUAL(k3, q9.find());
    CHECK_EQUAL(k2, q8.find());


    // See if we can append a criteria to a copy without modifying the original (copy should not contain references
    // to original). Tests query_engine string node.
    Query q10 = t.column<String>(col_str) != "2";
    Query q11(q10);

    q11.greater(col_dbl, 4.0);
    CHECK_EQUAL(k3, q11.find());
    CHECK_EQUAL(k0, q10.find());

    // Test and_query() on a copy
    Query q12 = t.column<Int>(col_int) > 2;
    Query q13(q12);

    q13.and_query(t.column<String>(col_str) != "3");
    CHECK_EQUAL(k3, q13.find());
    CHECK_EQUAL(k2, q12.find());
}

TEST(Query_TableViewMoveAssign1)
{
    Table t;
    auto col_int = t.add_column(type_Int, "1");

    t.create_object().set(col_int, 1);
    t.create_object().set(col_int, 2);
    t.create_object().set(col_int, 3);
    t.create_object().set(col_int, 4);

    // temporary query is created, then q makes and stores a deep copy and then temporary is destructed
    // Explicit use of Value<>() makes query_expression node instead of query_engine
    Query q = t.column<Int>(col_int) > Value<Int>(2);

    // now deep copy should be destructed and replaced by new temporary
    TableView tv = q.find_all();

    // the original should still work; destruction of temporaries and deep copies should have no references
    // to original
    tv = q.find_all();
}

TEST(Query_TableViewMoveAssignLeak2)
{
    Table t;
    auto col_int = t.add_column(type_Int, "1");
    auto col_str = t.add_column(type_String, "2");
    auto col_dbl = t.add_column(type_Double, "3");

    Query q = t.column<Int>(col_int) < t.column<double>(col_dbl) && t.column<String>(col_str) == "4";
    TableView tv = q.find_all();

    // Upon each find_all() call, tv copies the query 'q' into itself. See if this copying works
    tv = q.find_all();
    tv = q.find_all();
    tv = q.find_all();
    tv = q.find_all();
    tv = q.find_all();

    tv.sort(col_int, true);

    tv = q.find_all();

    Query q2 = t.column<Int>(col_int) <= t.column<double>(col_dbl);
    tv = q2.find_all();
    q.and_query(q2);
    tv = q.find_all();

    tv.sync_if_needed();

    ObjKey t2 = q.find();
    static_cast<void>(t2);
    tv = q.find_all();
    tv.sync_if_needed();
    t2 = q.find();
    tv.sync_if_needed();
    tv = q.find_all();
    tv.sync_if_needed();
    t2 = q.find();
    tv.sync_if_needed();
    tv = q.find_all();
    tv.sync_if_needed();
    tv = q.find_all();
    tv.sync_if_needed();

    Query q3;

    q2 = t.column<Int>(col_int) <= t.column<double>(col_dbl);
    q3 = q2;

    q3.find();
    q2.find();
}


TEST(Query_DeepCopyLeak1)
{
    // NOTE: You can only create a copy of a fully constructed; i.e. you cannot copy a query which is missing an
    // end_group(). Run Query::validate() to see if it's fully constructed.

    Table t;
    auto col_int = t.add_column(type_Int, "1");
    auto col_dbl = t.add_column(type_Double, "3");

    // See if copying of a mix of query_expression and query_engine nodes will leak
    Query q =
        !(t.column<Int>(col_int) > Value<Int>(2) && t.column<Int>(col_int) > 2 && t.column<double>(col_dbl) > 2.2) ||
        t.column<Int>(col_int) == 4 || t.column<Int>(col_int) == Value<Int>(4);
    Query q2(q);
    Query q3(q2);
}

TEST(Query_DeepCopyTest)
{
    // If Query::first vector was relocated because of push_back, then Query would crash, because referenced
    // pointers were pointing into it.
    Table table;
    table.add_column(type_Int, "first");

    Query q1 = table.where();

    Query q2(q1);

    q2.group();
    q2.end_group();
}

TEST(Query_StringIndexCrash)
{
    // Test for a crash which occured when a query testing for equality on a
    // string index was deep-copied after being run
    Table table;
    auto col = table.add_column(type_String, "s", true);
    table.add_search_index(col);

    Query q = table.where().equal(col, StringData(""));
    q.count();
    Query q2(q);
}

TEST(Query_NullStrings)
{
    Table table;
    auto col = table.add_column(type_String, "s", true);

    Query q;
    TableView v;

    // Short strings
    auto k0 = table.create_object().set<String>(col, "Albertslund").get_key(); // Normal non-empty string
    auto k1 = table.create_object().set<String>(col, realm::null()).get_key(); // NULL string
    auto k2 = table.create_object().set<String>(col, "").get_key();            // Empty string

    q = table.column<StringData>(col) == realm::null();
    v = q.find_all();
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(k1, v.get_key(0));

    q = table.column<StringData>(col) != realm::null();
    v = q.find_all();
    CHECK_EQUAL(2, v.size());
    CHECK_EQUAL(k0, v.get_key(0));
    CHECK_EQUAL(k2, v.get_key(1));

    // contrary to SQL, comparisons with realm::null() can be true in Realm (todo, discuss if we want this behaviour)
    q = table.column<StringData>(col) != StringData("Albertslund");
    v = q.find_all();
    CHECK_EQUAL(2, v.size());
    CHECK_EQUAL(k1, v.get_key(0));
    CHECK_EQUAL(k2, v.get_key(1));

    q = table.column<StringData>(col) == "";
    v = q.find_all();
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(k2, v.get_key(0));

    // Medium strings (16+)
    table.get_object(k0).set<String>(col, "AlbertslundAlbertslundAlbert");

    q = table.column<StringData>(col) == realm::null();
    v = q.find_all();
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(k1, v.get_key(0));

    q = table.column<StringData>(col) == "";
    v = q.find_all();
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(k2, v.get_key(0));

    // Long strings (64+)
    table.get_object(k0).set<String>(col,
                                     "AlbertslundAlbertslundAlbertslundAlbertslundAlbertslundAlbertslundAlbertslund");
    q = table.column<StringData>(col) == realm::null();
    v = q.find_all();
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(k1, v.get_key(0));

    q = table.column<StringData>(col) == "";
    v = q.find_all();
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(k2, v.get_key(0));
}

TEST(Query_Nulls_Fuzzy)
{
    for (int attributes = 1; attributes < 5; attributes++) {
        Random random(random_int<unsigned long>());

        for (size_t t = 0; t < 10; t++) {
            Table table;
            auto col = table.add_column(type_String, "string", true);

            if (attributes == 0) {
            }
            if (attributes == 1) {
                table.add_search_index(col);
            }
            else if (attributes == 2) {
                table.enumerate_string_column(col);
            }
            else if (attributes == 3) {
                table.add_search_index(col);
                table.enumerate_string_column(col);
            }
            else if (attributes == 4) {
                table.enumerate_string_column(col);
                table.add_search_index(col);
            }

            // map that is kept in sync with the column so that we can compare with it
            std::map<ObjKey, std::string> v;

            // ArrayString capacity starts at 128 bytes, so we need lots of elements
            // to test if relocation works
            for (size_t i = 0; i < 100; i++) {
                unsigned char action = static_cast<unsigned char>(random.draw_int_max<unsigned int>(100));

                if (action > 48 && table.size() < 10) {
                    // Generate string with equal probability of being empty, null, short, medium and long, and with
                    // their contents having equal proability of being either random or a duplicate of a previous
                    // string. When it's random, each char must have equal probability of being 0 or non-0
                    char buf[] =
                        "This string is around 90 bytes long, which falls in the long-string type of Realm strings";
                    char* buf1 = static_cast<char*>(malloc(sizeof(buf)));
                    memcpy(buf1, buf, sizeof(buf));
                    char buf2[] =
                        "                                                                                         ";

                    StringData sd;
                    std::string st;

                    if (fastrand(1) == 0) {
                        // null string
                        sd = realm::null();
                        st = "null";
                    }
                    else {
                        // non-null string
                        size_t len = static_cast<size_t>(fastrand(3));
                        if (len == 0)
                            len = 0;
                        else if (len == 1)
                            len = 7;
                        else if (len == 2)
                            len = 27;
                        else
                            len = 73;

                        if (fastrand(1) == 0) {
                            // duplicate string
                            sd = StringData(buf1, len);
                            st = std::string(buf1, len);
                        }
                        else {
                            // random string
                            for (size_t s = 0; s < len; s++) {
                                if (fastrand(100) > 20)
                                    buf2[s] = 0; // zero byte
                                else
                                    buf2[s] = static_cast<char>(fastrand(255)); // random byte
                            }
                            // no generated string can equal "null" (our vector magic value for null) because
                            // len == 4 is not possible
                            sd = StringData(buf2, len);
                            st = std::string(buf2, len);
                        }
                    }

                    try {
                        size_t pos = random.draw_int_max<size_t>(100000);
                        auto k = table.create_object(ObjKey(int64_t(pos))).set<String>(col, sd).get_key();

                        v.emplace(k, st);
                    }
                    catch (...) {
                    }
                    free(buf1);
                }
                else if (table.size() > 0) {
                    // delete
                    size_t row = random.draw_int_max<size_t>(table.size() - 1);
                    Obj obj = table.get_object(row);
                    obj.remove();
                    v.erase(obj.get_key());
                }


                CHECK_EQUAL(table.size(), v.size());
                for (auto& o : table) {
                    auto k = o.get_key();
                    if (v[k] == "null") {
                        CHECK(o.get<String>(col).is_null());
                    }
                    else {
                        CHECK(o.get<String>(col) == v[k]);
                    }
                }
            }
        }
    }
}

TEST(Query_BinaryNull)
{
    Table table;
    auto col = table.add_column(type_Binary, "first", true);

    auto k0 = table.create_object().set(col, BinaryData()).get_key();
    auto k1 = table.create_object()
                  .set(col, BinaryData("", 0))
                  .get_key(); // NOTE: Specify size = 0, else size turns into 1!
    auto k2 = table.create_object().set(col, BinaryData("foo")).get_key();

    TableView t;

    // Next gen syntax
    t = (table.column<BinaryData>(col) == BinaryData()).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(k0, t.get_key(0));

    t = (BinaryData() == table.column<BinaryData>(col)).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(k0, t.get_key(0));

    t = (table.column<BinaryData>(col) == BinaryData("", 0)).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(k1, t.get_key(0));

    t = (BinaryData("", 0) == table.column<BinaryData>(col)).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(k1, t.get_key(0));

    t = (table.column<BinaryData>(col) != BinaryData("", 0)).find_all();
    CHECK_EQUAL(2, t.size());
    CHECK_EQUAL(k0, t.get_key(0));
    CHECK_EQUAL(k2, t.get_key(1));

    t = (BinaryData("", 0) != table.column<BinaryData>(col)).find_all();
    CHECK_EQUAL(2, t.size());
    CHECK_EQUAL(k0, t.get_key(0));
    CHECK_EQUAL(k2, t.get_key(1));


    // Old syntax
    t = table.where().equal(col, BinaryData()).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(k0, t.get_key(0));

    t = table.where().equal(col, BinaryData("", 0)).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(k1, t.get_key(0));

    t = table.where().equal(col, BinaryData("foo")).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(k2, t.get_key(0));

    t = table.where().not_equal(col, BinaryData()).find_all();
    CHECK_EQUAL(2, t.size());
    CHECK_EQUAL(k1, t.get_key(0));
    CHECK_EQUAL(k2, t.get_key(1));

    t = table.where().not_equal(col, BinaryData("", 0)).find_all();
    CHECK_EQUAL(2, t.size());
    CHECK_EQUAL(k0, t.get_key(0));
    CHECK_EQUAL(k2, t.get_key(1));

    t = table.where().begins_with(col, BinaryData()).find_all();
    CHECK_EQUAL(3, t.size());

    t = table.where().begins_with(col, BinaryData("", 0)).find_all();
    CHECK_EQUAL(2, t.size());
    CHECK_EQUAL(k1, t.get_key(0));
    CHECK_EQUAL(k2, t.get_key(1));

    t = table.where().begins_with(col, BinaryData("foo")).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(k2, t.get_key(0));

    t = table.where().ends_with(col, BinaryData()).find_all();
    CHECK_EQUAL(3, t.size());

    t = table.where().ends_with(col, BinaryData("", 0)).find_all();
    CHECK_EQUAL(2, t.size());
    CHECK_EQUAL(k1, t.get_key(0));
    CHECK_EQUAL(k2, t.get_key(1));

    t = table.where().ends_with(col, BinaryData("foo")).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(k2, t.get_key(0));
}

TEST(Query_IntegerNullOldQueryEngine)
{
    /*
        first   second  third
         null      100      1
            0     null      2
          123      200      3
          null    null      4
    */
    Table table;
    auto c0 = table.add_column(type_Int, "first", true);
    auto c1 = table.add_column(type_Int, "second", true);
    auto c2 = table.add_column(type_Int, "third", false);

    auto k0 = table.create_object(ObjKey(4), {/*      */ {c1, 100}, {c2, 1}}).get_key();
    auto k1 = table.create_object(ObjKey(5), {{c0, 0}, /*        */ {c2, 2}}).get_key();
    auto k2 = table.create_object(ObjKey(6), {{c0, 123}, {c1, 200}, {c2, 3}}).get_key();
    auto k3 = table.create_object(ObjKey(7), {/*                 */ {c2, 7}}).get_key();

    TableView t;

    t = table.where().equal(c0, null{}).find_all();
    CHECK_EQUAL(2, t.size());
    CHECK_EQUAL(k0, t.get_key(0));
    CHECK_EQUAL(k3, t.get_key(1));

    t = table.where().equal(c1, null{}).find_all();
    CHECK_EQUAL(2, t.size());
    CHECK_EQUAL(k1, t.get_key(0));
    CHECK_EQUAL(k3, t.get_key(1));

    t = table.where().equal(c0, 0).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(k1, t.get_key(0));

    t = table.where().equal(c0, 123).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(k2, t.get_key(0));

    t = table.where().not_equal(c0, null{}).find_all();
    CHECK_EQUAL(2, t.size());
    CHECK_EQUAL(k1, t.get_key(0));
    CHECK_EQUAL(k2, t.get_key(1));

    t = table.where().not_equal(c0, 0).find_all();
    CHECK_EQUAL(3, t.size());
    CHECK_EQUAL(k0, t.get_key(0));
    CHECK_EQUAL(k2, t.get_key(1));
    CHECK_EQUAL(k3, t.get_key(2));

    t = table.where().greater(c0, 0).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(k2, t.get_key(0));

    t = table.where().greater(c2, 5).find_all();
    CHECK_EQUAL(1, t.size());
    CHECK_EQUAL(k3, t.get_key(0));
}

TEST(Query_IntegerNonNull)
{
    Table table;
    auto col = table.add_column(type_Int, "first", false);

    table.create_object().set(col, 123);
    table.create_object().set(col, 456);
    table.create_object();

    TableView t;

    // Fixme, should you be able to query a non-nullable column against null?
    //    t = table.where().equal(0, null{}).find_all();
    //    CHECK_EQUAL(0, t.size());
}

TEST(Query_64BitValues)
{
    Group g;
    ObjKey m;
    TableRef table = g.add_table("table");
    auto c0 = table->add_column(type_Int, "key");
    auto c1 = table->add_column(type_Int, "16bit");

    const int64_t start = 4485019129LL;
    const int64_t count = 20; // First 16 SSE-searched, four fallback
    const int64_t min = std::numeric_limits<int64_t>::min();
    const int64_t max = std::numeric_limits<int64_t>::max();

    for (size_t i = 0; i < count; ++i) {
        table->create_object().set(c0, start + i);
    }

    auto it = table->begin();
    for (int64_t v = 5; v > 0; v--) {
        // Insert values 5, 4, 3, 2, 1
        it->set(c1, v);
        ++it;
    }

    m = table->where().less(c1, 4).find();
    CHECK_EQUAL(2, m.value);

    m = table->where().less(c1, 5).find();
    CHECK_EQUAL(1, m.value);

    CHECK_EQUAL(0, table->where().less(c0, min).count());
    CHECK_EQUAL(0, table->where().less(c0, start).count());
    CHECK_EQUAL(1, table->where().less(c0, start + 1).count());
    CHECK_EQUAL(count, table->where().less(c0, start + count).count());
    CHECK_EQUAL(count, table->where().less(c0, max).count());

    CHECK_EQUAL(0, table->where().less_equal(c0, min).count());
    CHECK_EQUAL(1, table->where().less_equal(c0, start).count());
    CHECK_EQUAL(count, table->where().less_equal(c0, start + count).count());
    CHECK_EQUAL(count, table->where().less_equal(c0, max).count());

    CHECK_EQUAL(count, table->where().greater(c0, min).count());
    CHECK_EQUAL(count - 1, table->where().greater(c0, start).count());
    CHECK_EQUAL(1, table->where().greater(c0, start + count - 2).count());
    CHECK_EQUAL(0, table->where().greater(c0, start + count - 1).count());
    CHECK_EQUAL(0, table->where().greater(c0, max).count());

    CHECK_EQUAL(count, table->where().greater_equal(c0, min).count());
    CHECK_EQUAL(count, table->where().greater_equal(c0, start).count());
    CHECK_EQUAL(count - 1, table->where().greater_equal(c0, start + 1).count());
    CHECK_EQUAL(1, table->where().greater_equal(c0, start + count - 1).count());
    CHECK_EQUAL(0, table->where().greater_equal(c0, start + count).count());
    CHECK_EQUAL(0, table->where().greater_equal(c0, max).count());
}

namespace {

void create_columns(TableRef table, bool nullable = true)
{
    table->add_column(type_Int, "Price", nullable);
    table->add_column(type_Float, "Shipping", nullable);
    table->add_column(type_String, "Description", nullable);
    table->add_column(type_Double, "Rating", nullable);
    table->add_column(type_Bool, "Stock", nullable);
    table->add_column(type_Timestamp, "Delivery date", nullable);
    table->add_column(type_Binary, "Photo", nullable);
}

bool equals(TableView& tv, const std::vector<int64_t>& keys)
{
    if (tv.size() != keys.size()) {
        return false;
    }

    size_t sz = tv.size();
    for (size_t i = 0; i < sz; i++) {
        if (tv.get_key(i).value != keys[i]) {
            return false;
        }
    }

    return true;
}

void fill_data(TableRef table)
{
    table->create_object().set_all(1, null(), null(), 1.1, true, Timestamp(12345, 0));
    table->create_object().set_all(null(), null(), "foo", 2.2, null(), null());
    table->create_object().set_all(3, 30.f, "bar", null(), false, Timestamp(12345, 67));
}
} // unnamed namespace

TEST(Query_NullShowcase)
{
    /*
    Here we show how comparisons and arithmetic with null works in queries. Basic rules:

    null    +, -, *, /          value   ==   null
    null    +, -, *, /          null    ==   null

    null    ==, >=, <=]         null    ==   true
    null    !=, >, <            null    ==   false

    null    ==, >=, <=, >, <    value   ==   false
    null    !=                  value   ==   true

    This does NOT follow SQL! In particular, (null == null) == true and
    (null != value) == true.

    NOTE NOTE: There is currently only very little syntax checking.

    NOTE NOTE: For BinaryData, use BinaryData() instead of null().

        Price<int>      Shipping<float>     Description<String>     Rating<double>      Stock<bool>
    Delivery<OldDateTime>   Photo<BinaryData>
        -------------------------------------------------------------------------------------------------------------------------------------
    0   null            null                null                    1.1                 true          2016-2-2 "foo"
    1   10              null                "foo"                   2.2                 null          null
    zero-lenght non-null
    2   20              30.0                "bar"                   3.3                 false         2016-6-6 null
    */

    Group g;
    TableRef table = g.add_table("Inventory");
    create_columns(table);

    Obj obj0 = table->create_object();
    Obj obj1 = table->create_object();
    Obj obj2 = table->create_object();

    // Default values for all nullable columns
    for (auto col : table->get_column_keys()) {
        CHECK(obj0.is_null(col));
    }

    obj0.set_all(null(), null(), null(), 1.1, true, Timestamp(12345, 0), BinaryData("foo"));
    obj1.set_all(10, null(), "foo", 2.2, null(), null(), BinaryData("", 0));
    obj2.set_all(20, 30.f, "bar", 3.3, false, Timestamp(12345, 67), null());

    auto col_price = table->get_column_key("Price");
    auto col_shipping = table->get_column_key("Shipping");
    auto col_rating = table->get_column_key("Rating");
    auto col_date = table->get_column_key("Delivery date");
    Columns<Int> price = table->column<Int>(col_price);
    Columns<Float> shipping = table->column<Float>(col_shipping);
    Columns<Double> rating = table->column<Double>(col_rating);
    Columns<Bool> stock = table->column<Bool>(table->get_column_key("Stock"));
    Columns<Timestamp> delivery = table->column<Timestamp>(col_date);
    Columns<BinaryData> photo = table->column<BinaryData>(table->get_column_key("Photo"));

    // check int/double type mismatch error handling
    CHECK_THROW_ANY(table->column<Int>(table->get_column_key("Description")));

    TableView tv;

    tv = (price == null()).find_all();
    CHECK(equals(tv, {0}));

    tv = (price != null()).find_all();
    CHECK(equals(tv, {1, 2}));

    // Note that this returns rows with null, which differs from SQL!
    tv = (price == shipping).find_all();
    CHECK(equals(tv, {0})); // null == null

    // If you add a != null criteria, you would probably get what most users intended, like in SQL
    tv = (price == shipping && price != null()).find_all();
    CHECK(equals(tv, {}));

    tv = (price != shipping).find_all();
    CHECK(equals(tv, {1, 2})); // 10 != null

    tv = (price < 0 || price > 0).find_all();
    CHECK(equals(tv, {1, 2}));

    // Shows that null + null == null, and 10 + null == null, and null < 100 == false
    tv = table->query("Price + Shipping < 100").find_all();
    CHECK(equals(tv, {2}));

    //  null < 0 == false
    tv = (price < 0).find_all();
    CHECK(equals(tv, {}));

    //  null > 0 == false
    tv = (price == 0).find_all();
    CHECK(equals(tv, {}));

    // (null == 0) == false
    tv = (price > 0).find_all();
    CHECK(equals(tv, {1, 2}));

    // Doubles
    // (null > double) == false
    tv = (price > rating).find_all();
    CHECK(equals(tv, {1, 2}));

    tv = table->query("Price + Rating == null").find_all();
    CHECK(equals(tv, {0}));

    tv = table->query("Price + Rating != null").find_all();
    CHECK(equals(tv, {1, 2}));


    // Booleans
    tv = (stock == true).find_all();
    CHECK(equals(tv, {0}));

    tv = (stock == false).find_all();
    CHECK(equals(tv, {2}));

    tv = (stock == null()).find_all();
    CHECK(equals(tv, {1}));

    tv = (stock != null()).find_all();
    CHECK(equals(tv, {0, 2}));

    // Dates
    tv = (delivery == Timestamp(12345, 67)).find_all();
    CHECK(equals(tv, {2}));

    tv = (delivery != Timestamp(12345, 67)).find_all();
    CHECK(equals(tv, {0, 1}));

    tv = (delivery == null()).find_all();
    CHECK(equals(tv, {1}));

    tv = (delivery != null()).find_all();
    CHECK(equals(tv, {0, 2}));

    // BinaryData
    //
    // BinaryData only supports == and !=, and you cannot compare two columns - only a column and a constant
    tv = (photo == BinaryData("foo")).find_all();
    CHECK(equals(tv, {0}));

    tv = (photo == BinaryData("", 0)).find_all();
    CHECK(equals(tv, {1}));

    tv = (photo == BinaryData()).find_all();
    CHECK(equals(tv, {2}));

    tv = (photo != BinaryData("foo")).find_all();
    CHECK(equals(tv, {1, 2}));

    // Old query syntax
    tv = table->where().equal(col_price, null()).find_all();
    CHECK(equals(tv, {0}));

    tv = table->where().not_equal(col_price, null()).find_all();
    CHECK(equals(tv, {1, 2}));

    // You can also compare against user-given null with > and <, but only in the expression syntax!
    tv = (price > null()).find_all();
    CHECK(equals(tv, {}));
    tv = table->query("Price + Rating > null").find_all();
    CHECK(equals(tv, {}));

    // As stated above, if you want to use `> null()`, you cannot do it in the old syntax. This is for source
    // code simplicity (would need tons of new method overloads that also need unit test testing, etc). So
    // following is not possible and will not compile
    // (tv = table->where().greater(0, null()).find_all());

    // Nullable floats in old syntax
    tv = table->where().equal(col_shipping, null()).find_all();
    CHECK(equals(tv, {0, 1}));

    tv = table->where().not_equal(col_shipping, null()).find_all();
    CHECK(equals(tv, {2}));

    tv = table->where().greater(col_shipping, 0.0f).find_all();
    CHECK(equals(tv, {2}));

    tv = table->where().less(col_shipping, 20.0f).find_all();
    CHECK(equals(tv, {}));

    // TableView
    size_t count;
    int64_t i;
    double d;
    Timestamp dt;
    tv = table->where().find_all();

    // Integer column
    i = tv.maximum_int(col_price);
    CHECK_EQUAL(i, 20);

    i = tv.minimum_int(col_price);
    CHECK_EQUAL(i, 10);

    count = 123;
    d = tv.average_int(col_price, &count);
    CHECK_APPROXIMATELY_EQUAL(d, 15., 0.001);
    CHECK_EQUAL(count, 2);

    i = tv.sum_int(col_price);
    CHECK_EQUAL(i, 30);


    // Float column
    d = tv.maximum_float(col_shipping);
    CHECK_EQUAL(d, 30.);

    d = tv.minimum_float(col_shipping);
    CHECK_EQUAL(d, 30.);

    count = 123;
    d = tv.average_float(col_shipping, &count);
    CHECK_APPROXIMATELY_EQUAL(d, 30., 0.001);
    CHECK_EQUAL(count, 1);

    d = tv.sum_float(col_shipping);
    CHECK_APPROXIMATELY_EQUAL(d, 30., 0.001);

    // Double column
    d = tv.maximum_double(col_rating);
    CHECK_EQUAL(d, 3.3);
    d = tv.minimum_double(col_rating);
    CHECK_EQUAL(d, 1.1);
    d = tv.average_double(col_rating);
    CHECK_APPROXIMATELY_EQUAL(d, (1.1 + 2.2 + 3.3) / 3, 0.001);
    d = tv.sum_double(col_rating);
    CHECK_APPROXIMATELY_EQUAL(d, 1.1 + 2.2 + 3.3, 0.001);

    // OldDateTime column
    dt = tv.maximum_timestamp(col_date);
    CHECK_EQUAL(dt, Timestamp(12345, 67));
    dt = tv.minimum_timestamp(col_date);
    CHECK_EQUAL(dt, Timestamp(12345, 0));

#if 0 // FIXME?
    // NaN
    // null converts to 0 when calling get_float() on it. We intentionally do not return the bit pattern
    // for internal Realm representation, because that's a NaN, hence making it harder for the end user
    // to distinguish between his own NaNs and null
    CHECK_EQUAL(obj0.get<Float>(col_shipping), 0);
#endif

    obj0.set<Float>(col_shipping, std::numeric_limits<float>::signaling_NaN());
    obj1.set<Float>(col_shipping, std::numeric_limits<float>::quiet_NaN());

    // Realm may return a signalling/quiet NaN that is different from the signalling/quiet NaN you stored
    // (the IEEE standard defines a sequence of bits in the NaN that can have custom contents). Realm does
    // not preserve these bits.
    CHECK(std::isnan(obj0.get<Float>(col_shipping)));
    CHECK(std::isnan(obj1.get<Float>(col_shipping)));


    // FIXME: std::numeric_limits<float>::signaling_NaN() seems broken in VS2015 in that it returns a non-
    // signaling NaN. A bug report has been filed to Microsoft. Update: It turns out that on 32-bit Intel
    // Architecture (at least on my Core i7 in 32 bit code), if you push a float-NaN (fld instruction) that
    // has bit 22 clear (indicates it's signaling), and pop it back (fst instruction), the FPU will toggle
    // that bit into being set. All this needs further investigation, so a P2 has been created. Note that
    // IEEE just began specifying signaling vs. non-signaling NaNs in 2008. Also note that all this seems
    // to work fine on ARM in both 32 and 64 bit mode.

#if !defined(_WIN32) && !REALM_ARCHITECTURE_X86_32
    CHECK(null::is_signaling(obj0.get<Float>(col_shipping)));
#endif

#ifndef _WIN32 // signaling_NaN() may be broken in VS2015 (see long comment above)
    CHECK(!null::is_signaling(obj1.get<Float>(col_shipping)));
#endif

    CHECK(!obj0.is_null(col_shipping));
    CHECK(!obj1.is_null(col_shipping));

    obj0.set<Double>(col_rating, std::numeric_limits<double>::signaling_NaN());
    obj1.set<Double>(col_rating, std::numeric_limits<double>::quiet_NaN());
    CHECK(std::isnan(obj0.get<Double>(col_rating)));
    CHECK(std::isnan(obj1.get<Double>(col_rating)));

// signaling_NaN() broken in VS2015, and broken in 32bit intel
#if !defined(_WIN32) && !REALM_ARCHITECTURE_X86_32
    CHECK(null::is_signaling(obj0.get<Double>(col_rating)));
    CHECK(!null::is_signaling(obj1.get<Double>(col_rating)));
#endif

    CHECK(!obj0.is_null(col_rating));
    CHECK(!obj1.is_null(col_rating));

    // NOTE NOTE Queries on float/double columns that contain user-given NaNs are undefined.
}


// Test error handling and default values (user gives bad column type, is_null() returns false,
// get_float() must return 0.9 for null entries, etc, etc)
TEST(Query_Null_DefaultsAndErrorhandling)
{
    // Non-nullable columns: Tests is_nullable() and set_null()
    {
        Group g;
        TableRef table = g.add_table("Inventory");
        create_columns(table, false /* nullability */);

        Obj obj = table->create_object();

        auto all_cols = table->get_column_keys();

        for (auto col : all_cols) {
            CHECK(!table->is_nullable(col));
        }

        // is_null() on non-nullable column returns false. If you want it to throw, then do so
        // in the language binding
        for (auto col : all_cols) {
            CHECK(!obj.is_null(col));
        }

        for (auto col : all_cols) {
            CHECK_THROW_ANY(obj.set_null(col));
        }

        // verify that set_null() did not have any side effects
        for (auto col : all_cols) {
            CHECK(!obj.is_null(col));
        }
    }

    // Nullable columns: Tests that default value is null, and tests is_nullable() and set_null()
    {
        Group g;
        TableRef table = g.add_table("Inventory");
        create_columns(table);

        Obj obj = table->create_object();

        auto all_cols = table->get_column_keys();

        for (auto col : all_cols) {
            CHECK(table->is_nullable(col));
        }

        // default values should be null
        for (auto col : all_cols) {
            CHECK(obj.is_null(col));
        }

#if 0
        // calling get() on a numeric column must return following:
        CHECK_EQUAL(table->get_int(0, 0), 0);
        CHECK_EQUAL(table->get_float(1, 0), 0.0f);
        CHECK_EQUAL(table->get_double(3, 0), 0.0);
        CHECK_EQUAL(table->get_bool(4, 0), false);
        CHECK_EQUAL(table->get_olddatetime(5, 0), OldDateTime(0));
#endif
        // Set everything to non-null values
        char bin = 8;
        obj.set_all(0, 0.f, StringData("", 0), 0., false, Timestamp(1, 2), Binary(&bin, sizeof(bin)));

        for (auto col : all_cols) {
            CHECK(!obj.is_null(col));
        }

        for (auto col : all_cols) {
            obj.set_null(col);
        }

        for (auto col : all_cols) {
            CHECK(obj.is_null(col));
        }
    }
}

// Tests queries that compare two columns with eachother in various ways. The columns have different
// integral types
TEST(Query_Null_Two_Columns)
{
    Group g;
    TableRef table = g.add_table("Inventory");
    create_columns(table);
    fill_data(table);

    auto col_price = table->get_column_key("Price");
    auto col_shipping = table->get_column_key("Shipping");
    auto col_description = table->get_column_key("Description");
    auto col_rating = table->get_column_key("Rating");
    auto col_date = table->get_column_key("Delivery date");
    Columns<Int> price = table->column<Int>(col_price);
    Columns<Float> shipping = table->column<Float>(col_shipping);
    Columns<String> description = table->column<String>(col_description);
    Columns<Double> rating = table->column<Double>(col_rating);
    Columns<Bool> stock = table->column<Bool>(table->get_column_key("Stock"));
    Columns<Timestamp> delivery = table->column<Timestamp>(col_date);
    Columns<BinaryData> photo = table->column<BinaryData>(table->get_column_key("Photo"));

    TableView tv;

    /*
    Price<int>      Shipping<float>     Description<String>     Rating<double>      Stock<bool> Delivery<Timestamp>
    ----------------------------------------------------------------------------------------------------------------
    0   1           null                null                    1.1                 true          12345, 0
    1   null        null                "foo"                   2.2                 null          null
    2   3           30.0                "bar"                   null                false         12345, 67
    */

    tv = (shipping > rating).find_all();
    CHECK(equals(tv, {}));

    tv = (shipping < rating).find_all();
    CHECK(equals(tv, {}));

    tv = (price == rating).find_all();
    CHECK(equals(tv, {}));

    tv = (price != rating).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (shipping == rating).find_all();
    CHECK(equals(tv, {}));

    tv = (shipping != rating).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    // Comparison column with itself
    tv = (shipping == shipping).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (shipping > shipping).find_all();
    CHECK(equals(tv, {}));

    tv = (shipping < shipping).find_all();
    CHECK(equals(tv, {}));

    tv = (shipping <= shipping).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (shipping >= shipping).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (rating == rating).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (rating != rating).find_all();
    CHECK(equals(tv, {}));

    tv = (rating > rating).find_all();
    CHECK(equals(tv, {}));

    tv = (rating < rating).find_all();
    CHECK(equals(tv, {}));

    tv = (rating >= rating).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (rating <= rating).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (stock == stock).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (stock != stock).find_all();
    CHECK(equals(tv, {}));

    tv = (price == price).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (price != price).find_all();
    CHECK(equals(tv, {}));

    tv = (price > price).find_all();
    CHECK(equals(tv, {}));

    tv = (price < price).find_all();
    CHECK(equals(tv, {}));

    tv = (price >= price).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (price <= price).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (delivery == delivery).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (delivery != delivery).find_all();
    CHECK(equals(tv, {}));

    tv = (delivery > delivery).find_all();
    CHECK(equals(tv, {}));

    tv = (delivery < delivery).find_all();
    CHECK(equals(tv, {}));

    tv = (delivery >= delivery).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (delivery <= delivery).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (description == description).find_all();
    CHECK(equals(tv, {0, 1, 2}));

    tv = (description != description).find_all();
    CHECK(equals(tv, {}));

    // Test a few untested things
    tv = table->where().equal(col_rating, null()).find_all();
    CHECK(equals(tv, {2}));

    tv = table->where().equal(col_price, null()).find_all();
    CHECK(equals(tv, {1}));

    tv = table->where().not_equal(col_rating, null()).find_all();
    CHECK(equals(tv, {0, 1}));

    tv = table->where().between(col_price, 2, 4).find_all();
    CHECK(equals(tv, {2}));

    // between for floats
    tv = table->where().between(col_shipping, 10.f, 40.f).find_all();
    CHECK(equals(tv, {2}));

    tv = table->where().between(col_shipping, 0.f, 20.f).find_all();
    CHECK(equals(tv, {}));

    tv = table->where().between(col_shipping, 40.f, 100.f).find_all();
    CHECK(equals(tv, {}));

    // between for doubles
    tv = table->where().between(col_rating, 0., 100.).find_all();
    CHECK(equals(tv, {0, 1}));

    tv = table->where().between(col_rating, 1., 2.).find_all();
    CHECK(equals(tv, {0}));

    tv = table->where().between(col_rating, 2., 3.).find_all();
    CHECK(equals(tv, {1}));

    tv = table->where().between(col_rating, 3., 100.).find_all();
    CHECK(equals(tv, {}));
}

// Between, count, min and max
TEST(Query_Null_BetweenMinMax_Nullable)
{
    Group g;
    TableRef table = g.add_table("Inventory");
    create_columns(table);
    table->create_object();
    auto col_price = table->get_column_key("Price");
    auto col_shipping = table->get_column_key("Shipping");
    auto col_rating = table->get_column_key("Rating");
    auto col_date = table->get_column_key("Delivery date");

    /*
    Price<int>      Shipping<float>     Description<String>     Rating<double>      Stock<bool>
    Delivery<OldDateTime>     ts<Timestamp>
    --------------------------------------------------------------------------------------------------------------------------------------
    null            null                null                    null                null            null null
    */

    TableView tv;
    ObjKey match;
    size_t count;

    // Here we test max/min/average with 0 rows used to compute the value, either becuase all inputs are null or
    // becuase 0 rows exist.
    auto test_tv = [&]() {
        // int
        match = ObjKey(123);
        tv.maximum_int(col_price, &match);
        CHECK_EQUAL(match, realm::null_key);

        match = ObjKey(123);
        tv.minimum_int(col_price, &match);
        CHECK_EQUAL(match, realm::null_key);

        CHECK_EQUAL(tv.sum_int(col_price), 0);
        count = 123;
        CHECK_EQUAL(tv.average_int(col_price, &count), 0.);
        CHECK_EQUAL(count, 0);

        // float
        match = ObjKey(123);
        tv.maximum_float(col_shipping, &match);
        CHECK_EQUAL(match, realm::null_key);

        match = ObjKey(123);
        tv.minimum_float(col_shipping, &match);
        CHECK_EQUAL(match, realm::null_key);

        CHECK_EQUAL(tv.sum_float(col_shipping), 0.);
        count = 123;
        CHECK_EQUAL(tv.average_float(col_shipping, &count), 0.);
        CHECK_EQUAL(count, 0);

        // double
        match = ObjKey(123);
        tv.maximum_double(col_rating, &match);
        CHECK_EQUAL(match, realm::null_key);

        match = ObjKey(123);
        tv.minimum_double(col_rating, &match);
        CHECK_EQUAL(match, realm::null_key);

        CHECK_EQUAL(tv.sum_double(col_rating), 0.);
        count = 123;
        CHECK_EQUAL(tv.average_double(col_rating, &count), 0.);
        CHECK_EQUAL(count, 0);

        // date
        match = ObjKey(123);
        tv.maximum_timestamp(col_date, &match);
        CHECK_EQUAL(match, realm::null_key);

        match = ObjKey(123);
        tv.minimum_timestamp(col_date, &match);
        CHECK_EQUAL(match, realm::null_key);
    };

    // There are rows in TableView but they all point to null
    tv = table->where().find_all();
    test_tv();

    // There are 0 rows in TableView
    tv = table->where().equal(col_price, 123).find_all();
    test_tv();

    // Now we test that average does not include nulls in row count:
    /*
    Price<int>      Shipping<float>     Description<String>     Rating<double>      Stock<bool> Delivery<OldDateTime>
    ----------------------------------------------------------------------------------------------------------------
    null            null                null                    null                null            null
    10              10.f                null                    10.                 null            null
    */

    table->create_object().set_all(10, 10.f, null(), 10.);

    tv = table->where().find_all();
    count = 123;
    CHECK_EQUAL(tv.average_int(col_price, &count), 10);
    CHECK_EQUAL(count, 1);
    count = 123;
    CHECK_EQUAL(tv.average_float(col_shipping, &count), 10.);
    CHECK_EQUAL(count, 1);
    count = 123;
    CHECK_EQUAL(tv.average_double(col_rating, &count), 10.);
    CHECK_EQUAL(count, 1);
}


// If number of rows is larger than 8, they can be loaded in chunks by the query system. Test if this works by
// creating a large table with nulls in arbitrary places and query for nulls. Verify the search result manually.
// Do that for all Realm types.
TEST(Query_Null_ManyRows)
{
    Group g;
    TableRef table = g.add_table("Inventory");
    create_columns(table);

    auto col_price = table->get_column_key("Price");
    auto col_shipping = table->get_column_key("Shipping");
    auto col_description = table->get_column_key("Description");
    auto col_rating = table->get_column_key("Rating");
    auto col_date = table->get_column_key("Delivery date");
    Columns<Int> price = table->column<Int>(col_price);
    Columns<Float> shipping = table->column<Float>(col_shipping);
    Columns<String> description = table->column<String>(col_description);
    Columns<Double> rating = table->column<Double>(col_rating);
    Columns<Bool> stock = table->column<Bool>(table->get_column_key("Stock"));
    Columns<Timestamp> delivery = table->column<Timestamp>(col_date);

    // Create lots of non-null rows
    for (size_t t = 0; t < 2000; t++) {
        table->create_object().set_all(123, 30.f, "foo", 12.3, true, Timestamp(1, 2));
    }

    // Reference lists used to verify query results
    std::vector<int64_t> nulls;     // List of rows that have all fields set to null
    std::vector<int64_t> non_nulls; // List of non-null rows

    auto all_cols = table->get_column_keys();

    // Fill in nulls in random rows, at each 10'th row on average
    for (size_t t = 0; t < table->size() / 10; t++) {
        // Bad but fast random generator
        size_t prime = 883;
        size_t random = ((t + prime) * prime + t) % table->size();
        Obj obj = table->get_object(random);

        // Test if already null (simplest way to avoid dublicates in our nulls vector)
        if (!obj.is_null(col_price)) {
            for (auto col : all_cols) {
                obj.set_null(col);
            }
            nulls.push_back(obj.get_key().value);
        }
    }

    // Fill out non_nulls vector
    for (auto& o : *table) {
        if (!o.is_null(col_price))
            non_nulls.push_back(o.get_key().value);
    }

    std::sort(nulls.begin(), nulls.end());
    TableView tv;

    // Search for nulls and non-nulls and verify matches against our manually created `nulls` and non_nulls vectors.
    // Do that for all Realm data types
    tv = (price == null()).find_all();
    CHECK(equals(tv, nulls));

    tv = (price != null()).find_all();
    CHECK(equals(tv, non_nulls));

    tv = (shipping == null()).find_all();
    CHECK(equals(tv, nulls));

    tv = (shipping != null()).find_all();
    CHECK(equals(tv, non_nulls));

    tv = (description == null()).find_all();
    CHECK(equals(tv, nulls));

    tv = (description != null()).find_all();
    CHECK(equals(tv, non_nulls));

    tv = (rating == null()).find_all();
    CHECK(equals(tv, nulls));

    tv = (rating != null()).find_all();
    CHECK(equals(tv, non_nulls));

    tv = (stock == null()).find_all();
    CHECK(equals(tv, nulls));

    tv = (stock != null()).find_all();
    CHECK(equals(tv, non_nulls));

    tv = (delivery == null()).find_all();
    CHECK(equals(tv, nulls));

    tv = (delivery != null()).find_all();
    CHECK(equals(tv, non_nulls));
}

TEST(Query_Null_Sort)
{
    Group g;
    TableRef table = g.add_table("Inventory");
    create_columns(table);

    auto k0 = table->create_object().set_all(0, 0.f, "0", 0.0, false, Timestamp(0, 0)).get_key();
    auto k1 = table->create_object().get_key();
    auto k2 = table->create_object().set_all(2, 2.f, "2", 2.0, true, Timestamp(2, 0)).get_key();

    auto all_cols = table->get_column_keys();
    for (int i = 0; i <= 5; i++) {
        TableView tv = table->where().find_all();
        CHECK(tv.size() == 3);

        tv.sort(all_cols[i], true);
        CHECK_EQUAL(tv.get_key(0), k1);
        CHECK_EQUAL(tv.get_key(1), k0);
        CHECK_EQUAL(tv.get_key(2), k2);

        tv = table->where().find_all();
        tv.sort(all_cols[i], false);
        CHECK_EQUAL(tv.get_key(0), k2);
        CHECK_EQUAL(tv.get_key(1), k0);
        CHECK_EQUAL(tv.get_key(2), k1);
    }
}

TEST(Query_LinkCounts)
{
    Group group;
    TableRef table1 = group.add_table("table1");
    auto col_str = table1->add_column(type_String, "str");

    auto k0 = table1->create_object().set(col_str, "abc").get_key();
    auto k1 = table1->create_object().set(col_str, "def").get_key();
    auto k2 = table1->create_object().set(col_str, "ghi").get_key();

    TableRef table2 = group.add_table("table2");
    auto col_int = table2->add_column(type_Int, "int");
    auto col_link = table2->add_column(*table1, "link");
    auto col_linklist = table2->add_column_list(*table1, "linklist");

    table2->create_object().set_all(0);
    table2->create_object().set_all(1, k1).get_linklist(col_linklist).add(k1);
    auto ll = table2->create_object().set_all(2, k2).get_linklist(col_linklist);
    ll.add(k1);
    ll.add(k2);

    Query q;
    ObjKey match;

    // Verify that queries against the count of a LinkList column work.
    q = table2->column<Link>(col_linklist).count() == 0;
    match = q.find();
    CHECK_EQUAL(k0, match);

    q = table2->column<Link>(col_linklist).count() == 1;
    match = q.find();
    CHECK_EQUAL(k1, match);

    q = table2->column<Link>(col_linklist).count() >= 1;
    auto tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(k1, tv.get_key(0));
    CHECK_EQUAL(k2, tv.get_key(1));


    // Verify that queries against the count of a Link column work.
    q = table2->column<Link>(col_link).count() == 0;
    match = q.find();
    CHECK_EQUAL(k0, match);

    q = table2->column<Link>(col_link).count() == 1;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(k1, tv.get_key(0));
    CHECK_EQUAL(k2, tv.get_key(1));

    // Verify that reusing the count expression works.
    auto link_count = table2->column<Link>(col_linklist).count();
    size_t match_count = (link_count == 0).count();
    CHECK_EQUAL(1, match_count);

    match_count = (link_count >= 1).count();
    CHECK_EQUAL(2, match_count);

    // Verify that combining the count expression with other queries on the same table works.
    q = table2->column<Link>(col_linklist).count() == 1 && table2->column<Int>(col_int) == 1;
    match = q.find();
    CHECK_EQUAL(k1, match);
}

struct TestLinkList {
    ColKey add_link_column(TableRef source, TableRef dest)
    {
        return source->add_column_list(*dest, "linklist");
    }
    void create_object_with_links(TableRef table, ColKey col, std::vector<ObjKey> links)
    {
        LnkLst ll = table->create_object().get_linklist(col);
        for (auto link : links) {
            ll.add(link);
        }
    }
    void add_links_to(TableRef table, ColKey col, ObjKey obj, std::vector<ObjKey> links)
    {
        LnkLst ll = table->get_object(obj).get_linklist(col);
        for (auto link : links) {
            ll.add(link);
        }
    }
};


struct TestLinkSet {
    ColKey add_link_column(TableRef source, TableRef dest)
    {
        return source->add_column_set(*dest, "linkset");
    }
    void create_object_with_links(TableRef table, ColKey col, std::vector<ObjKey> links)
    {
        LnkSet ls = table->create_object().get_linkset(col);
        for (auto link : links) {
            ls.insert(link);
        }
    }
    void add_links_to(TableRef table, ColKey col, ObjKey obj, std::vector<ObjKey> links)
    {
        LnkSet ls = table->get_object(obj).get_linkset(col);
        for (auto link : links) {
            ls.insert(link);
        }
    }
};

struct TestDictionaryLinkValues {
    ColKey add_link_column(TableRef source, TableRef dest)
    {
        return source->add_column_dictionary(*dest, "linkdictionary");
    }
    void create_object_with_links(TableRef table, ColKey col, std::vector<ObjKey> links)
    {
        Dictionary dict = table->create_object().get_dictionary(col);
        for (auto link : links) {
            std::string key = util::format("key_%1", keys_added++);
            dict.insert(Mixed(StringData(key)), Mixed(link));
        }
    }
    void add_links_to(TableRef table, ColKey col, ObjKey obj, std::vector<ObjKey> links)
    {
        Dictionary dict = table->get_object(obj).get_dictionary(col);
        for (auto link : links) {
            std::string key = util::format("key_%1", keys_added++);
            dict.insert(Mixed(StringData(key)), Mixed(link));
        }
    }

    size_t keys_added = 0;
};

TEST_TYPES(Query_Link_Container_Minimum, TestLinkList, TestLinkSet, TestDictionaryLinkValues)
{
    Group group;
    TableRef table1 = group.add_table("table1");
    auto col_int = table1->add_column(type_Int, "int", /* nullable */ true);
    auto col_float = table1->add_column(type_Float, "float", /* nullable */ true);
    auto col_double = table1->add_column(type_Double, "double", /* nullable */ true);

    // table1
    // 0: 789 789.0f 789.0
    // 1: 456 456.0f 456.0
    // 2: 123 123.0f 123.0
    // 3: null null null

    auto k0 = table1->create_object().set_all(789, 789.f, 789.).get_key();
    auto k1 = table1->create_object().set_all(456, 456.f, 456.).get_key();
    auto k2 = table1->create_object().set_all(123, 123.f, 123.).get_key();
    auto k3 = table1->create_object().get_key();

    TEST_TYPE test_container;
    TableRef table2 = group.add_table("table2");
    ColKey col_linktest = test_container.add_link_column(table2, table1);

    // table2
    // 0: { }
    // 1: { 1 }
    // 2: { 1, 2 }
    // 3: { 1, 2, 3 }

    test_container.create_object_with_links(table2, col_linktest, {});
    test_container.create_object_with_links(table2, col_linktest, {k1});
    test_container.create_object_with_links(table2, col_linktest, {k1, k2});
    test_container.create_object_with_links(table2, col_linktest, {k1, k2, k3});

    Query q;
    TableView tv;

    q = table2->column<Link>(col_linktest).column<Int>(col_int).min() == 123;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(k2, tv.get_key(0));
    CHECK_EQUAL(k3, tv.get_key(1));

    q = table2->column<Link>(col_linktest).column<Int>(col_int).min() == 456;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(k1, tv.get_key(0));

    q = table2->column<Link>(col_linktest).column<Int>(col_int).min() == null();
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(k0, tv.get_key(0));

    q = table2->column<Link>(col_linktest).column<Float>(col_float).min() == 123.0f;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(k2, tv.get_key(0));
    CHECK_EQUAL(k3, tv.get_key(1));

    q = table2->column<Link>(col_linktest).column<Float>(col_float).min() == 456.0f;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(k1, tv.get_key(0));

    q = table2->column<Link>(col_linktest).column<Double>(col_double).min() == 123.0;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(k2, tv.get_key(0));
    CHECK_EQUAL(k3, tv.get_key(1));

    q = table2->column<Link>(col_linktest).column<Double>(col_double).min() == 456.0;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(k1, tv.get_key(0));
}

TEST_TYPES(Query_Link_MaximumSumAverage, TestLinkList, TestLinkSet, TestDictionaryLinkValues)
{
    Group group;
    TableRef table1 = group.add_table("table1");
    auto col_int = table1->add_column(type_Int, "int", /* nullable */ true);
    auto col_flt = table1->add_column(type_Float, "float", /* nullable */ true);
    auto col_dbl = table1->add_column(type_Double, "double", /* nullable */ true);

    // table1
    // 0: 123 123.0f 123.0
    // 1: 456 456.0f 456.0
    // 2: 789 789.0f 789.0
    // 3: null null null

    ObjKeys keys({3, 5, 7, 9});
    table1->create_objects(keys);
    auto it = table1->begin();
    it->set_all(123, 123.f, 123.);
    (++it)->set_all(456, 456.f, 456.);
    (++it)->set_all(789, 789.f, 789.);

    TEST_TYPE test_container;
    TableRef table2 = group.add_table("table2");
    auto col_double = table2->add_column(type_Double, "double");
    auto col_link = table2->add_column(*table1, "link");
    ColKey col_linktest = test_container.add_link_column(table2, table1);

    // table2
    // 0: 456.0 ->0 { }
    // 1: 456.0 ->1 { 1 }
    // 2: 456.0 ->2 { 1, 2 }
    // 3: 456.0 ->3 { 1, 2, 3 }

    auto k0 = table2->create_object().set_all(456.0, keys[0]).get_key();
    auto k1 = table2->create_object().set_all(456.0, keys[1]).get_key();
    auto k2 = table2->create_object().set_all(456.0, keys[2]).get_key();
    auto k3 = table2->create_object().set_all(456.0, keys[3]).get_key();

    test_container.add_links_to(table2, col_linktest, k0, {});
    test_container.add_links_to(table2, col_linktest, k1, {keys[1]});
    test_container.add_links_to(table2, col_linktest, k2, {keys[1], keys[2]});
    test_container.add_links_to(table2, col_linktest, k3, {keys[1], keys[2], keys[3]});

    Query q;
    TableView tv;

    // Maximum.

    q = table2->column<Link>(col_linktest).column<Int>(col_int).max() == 789;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(k2, tv.get_key(0));
    CHECK_EQUAL(k3, tv.get_key(1));

    q = table2->column<Link>(col_linktest).column<Int>(col_int).max() == 456;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(k1, tv.get_key(0));

    q = table2->column<Link>(col_linktest).column<Int>(col_int).max() == null();
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(k0, tv.get_key(0));

    q = table2->column<Link>(col_linktest).column<Int>(col_int).max() == table2->link(col_link).column<Int>(col_int);
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(k1, tv.get_key(0));
    CHECK_EQUAL(k2, tv.get_key(1));

    q = table2->column<Link>(col_linktest).column<Int>(col_int).max() == table2->column<Double>(col_double);
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(k1, tv.get_key(0));


    q = table2->column<Link>(col_linktest).column<Float>(col_flt).max() == 789.0f;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(k2, tv.get_key(0));
    CHECK_EQUAL(k3, tv.get_key(1));

    q = table2->column<Link>(col_linktest).column<Float>(col_flt).max() == 456.0f;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(k1, tv.get_key(0));


    q = table2->column<Link>(col_linktest).column<Double>(col_dbl).max() == 789.0;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(k2, tv.get_key(0));
    CHECK_EQUAL(k3, tv.get_key(1));

    q = table2->column<Link>(col_linktest).column<Double>(col_dbl).max() == 456.0;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(k1, tv.get_key(0));


    // Sum.
    // Floating point results below may be inexact for some combination of architectures, compilers, and compiler
    // flags.

    q = table2->column<Link>(col_linktest).column<Int>(col_int).sum() == 1245;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(k2, tv.get_key(0));
    CHECK_EQUAL(k3, tv.get_key(1));

    q = table2->column<Link>(col_linktest).column<Int>(col_int).sum() == 456;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(k1, tv.get_key(0));

    q = table2->column<Link>(col_linktest).column<Int>(col_int).sum() == table2->link(col_link).column<Int>(col_int);
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(k1, tv.get_key(0));

    q = table2->column<Link>(col_linktest).column<Int>(col_int).sum() == table2->column<Double>(col_double);
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(k1, tv.get_key(0));


    q = table2->column<Link>(col_linktest).column<Float>(col_flt).sum() == 1245.0f;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(k2, tv.get_key(0));
    CHECK_EQUAL(k3, tv.get_key(1));

    q = table2->column<Link>(col_linktest).column<Float>(col_flt).sum() == 456.0f;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(k1, tv.get_key(0));


    q = table2->column<Link>(col_linktest).column<Double>(col_dbl).sum() == 1245.0;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(k2, tv.get_key(0));
    CHECK_EQUAL(k3, tv.get_key(1));

    q = table2->column<Link>(col_linktest).column<Double>(col_dbl).sum() == 456.0;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(k1, tv.get_key(0));


    // Average.
    // Floating point results below may be inexact for some combination of architectures, compilers, and compiler
    // flags.

    q = table2->column<Link>(col_linktest).column<Int>(col_int).average() == 622.5;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(k2, tv.get_key(0));
    CHECK_EQUAL(k3, tv.get_key(1));

    q = table2->column<Link>(col_linktest).column<Int>(col_int).average() == 456;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(k1, tv.get_key(0));

    q = table2->column<Link>(col_linktest).column<Int>(col_int).average() == null();
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(k0, tv.get_key(0));

    q = table2->column<Link>(col_linktest).column<Int>(col_int).average() <
        table2->link(col_link).column<Int>(col_int);
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(k2, tv.get_key(0));

    q = table2->column<Link>(col_linktest).column<Int>(col_int).average() == table2->column<Double>(col_double);
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(k1, tv.get_key(0));


    q = table2->column<Link>(col_linktest).column<Float>(col_flt).average() == 622.5;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(k2, tv.get_key(0));
    CHECK_EQUAL(k3, tv.get_key(1));

    q = table2->column<Link>(col_linktest).column<Float>(col_flt).average() == 456.0f;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(k1, tv.get_key(0));


    q = table2->column<Link>(col_linktest).column<Double>(col_dbl).average() == 622.5;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(k2, tv.get_key(0));
    CHECK_EQUAL(k3, tv.get_key(1));

    q = table2->column<Link>(col_linktest).column<Double>(col_dbl).average() == 456.0;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(k1, tv.get_key(0));
}

TEST_TYPES(Query_OperatorsOverLink, TestLinkList, TestLinkSet, TestDictionaryLinkValues)
{
    Group group;
    TableRef table1 = group.add_table("table1");
    table1->add_column(type_Int, "int");
    table1->add_column(type_Double, "double");

    // table1
    // 0: 2 2.0
    // 1: 3 3.0

    ObjKeys keys({5, 6});
    table1->create_objects(keys);
    table1->get_object(keys[0]).set_all(2, 2.0);
    table1->get_object(keys[1]).set_all(3, 3.0);

    TEST_TYPE test_container;
    TableRef table2 = group.add_table("table2");
    table2->add_column(type_Int, "int");
    ColKey col_linktest = test_container.add_link_column(table2, table1);

    // table2
    // 0:  0 { }
    // 1:  4 { 0 }
    // 2:  4 { 1, 0 }

    table2->create_object();
    auto k1 = table2->create_object().set_all(4).get_key();
    auto k2 = table2->create_object().set_all(4).get_key();

    test_container.add_links_to(table2, col_linktest, k1, {keys[0]});
    test_container.add_links_to(table2, col_linktest, k2, {keys[1], keys[0]});

    Query q;
    TableView tv;

    // Binary operators.

    // Rows 1 and 2 should match this query as 2 * 2 == 4.
    // Row 0 should not as the multiplication will not produce any results.
    std::string link_prop = table2->get_column_name(col_linktest);
    q = table2->query(link_prop + ".int * 2 == int");
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(k1, tv.get_key(0));
    CHECK_EQUAL(k2, tv.get_key(1));

    // Rows 1 and 2 should match this query as 2 * 2 == 4.
    // Row 0 should not as the multiplication will not produce any results.
    q = table2->query("int == 2 * " + link_prop + ".int");
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(k1, tv.get_key(0));
    CHECK_EQUAL(k2, tv.get_key(1));

    // Rows 1 and 2 should match this query as 2.0 * 2.0 == 4.0.
    // Row 0 should not as the multiplication will not produce any results.
    q = table2->query(link_prop + ".double * 2 == int");
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(k1, tv.get_key(0));
    CHECK_EQUAL(k2, tv.get_key(1));

    // Rows 1 and 2 should match this query as 2.0 * 2.0 == 4.0.
    // Row 0 should not as the multiplication will not produce any results.
    q = table2->query("int == 2 * " + link_prop + ".double");
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(k1, tv.get_key(0));
    CHECK_EQUAL(k2, tv.get_key(1));
}

TEST(Query_CompareLinkedColumnVsColumn)
{
    Group group;
    TableRef table1 = group.add_table("table1");
    auto col_int = table1->add_column(type_Int, "int");
    auto col_dbl = table1->add_column(type_Double, "double");

    // table1
    // 0: 2 2.0
    // 1: 3 3.0

    ObjKeys keys({5, 6});
    table1->create_objects(keys);
    table1->get_object(keys[0]).set_all(2, 2.0);
    table1->get_object(keys[1]).set_all(3, 3.0);

    TableRef table2 = group.add_table("table2");
    auto col_int2 = table2->add_column(type_Int, "int");
    auto col_link1 = table2->add_column(*table1, "link1");
    auto col_link2 = table2->add_column(*table1, "link2");

    // table2
    // 0: 2 {   } { 0 }
    // 1: 4 { 0 } { 1 }
    // 2: 4 { 1 } {   }

    auto k0 = table2->create_object().set_all(2, null(), keys[0]).get_key();
    auto k1 = table2->create_object().set_all(4, keys[0], keys[1]).get_key();
    auto k2 = table2->create_object().set_all(4, keys[1], null()).get_key();

    Query q;
    TableView tv;

    q = table2->link(col_link1).column<Int>(col_int) < table2->column<Int>(col_int2);
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(k1, tv.get_key(0));
    CHECK_EQUAL(k2, tv.get_key(1));

    q = table2->link(col_link1).column<Double>(col_dbl) < table2->column<Int>(col_int2);
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(k1, tv.get_key(0));
    CHECK_EQUAL(k2, tv.get_key(1));

    q = table2->link(col_link2).column<Int>(col_int) == table2->column<Int>(col_int2);
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(k0, tv.get_key(0));
}

TEST(Query_CompareThroughUnaryLinks)
{
    Group group;
    TableRef table1 = group.add_table("table1");
    auto col_int = table1->add_column(type_Int, "int");
    auto col_dbl = table1->add_column(type_Double, "double");
    auto col_str = table1->add_column(type_String, "string");

    // table1
    // 0: 2 2.0 "abc"
    // 1: 3 3.0 "def"
    // 2: 8 8.0 "def"

    ObjKeys keys({5, 6, 7});
    table1->create_objects(keys);
    table1->get_object(keys[0]).set_all(2, 2.0, "abc");
    table1->get_object(keys[1]).set_all(3, 3.0, "def");
    table1->get_object(keys[2]).set_all(8, 8.0, "def");

    TableRef table2 = group.add_table("table2");
    auto col_link1 = table2->add_column(*table1, "link1");
    auto col_link2 = table2->add_column(*table1, "link2");

    // table2
    // 0: {   } { 0 }
    // 1: { 0 } { 1 }
    // 2: { 1 } { 2 }
    // 3: { 2 } {   }

    table2->create_object().set_all(null(), keys[0]).get_key();
    auto k1 = table2->create_object().set_all(keys[0], keys[1]).get_key();
    auto k2 = table2->create_object().set_all(keys[1], keys[2]).get_key();
    table2->create_object().set_all(keys[2], null()).get_key();

    Query q;
    TableView tv;

    q = table2->link(col_link1).column<Int>(col_int) < table2->link(col_link2).column<Int>(col_int);
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(k1, tv.get_key(0));
    CHECK_EQUAL(k2, tv.get_key(1));

    q = table2->link(col_link1).column<Double>(col_dbl) < table2->link(col_link2).column<Double>(col_dbl);
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(k1, tv.get_key(0));
    CHECK_EQUAL(k2, tv.get_key(1));

    q = table2->link(col_link1).column<String>(col_str) == table2->link(col_link2).column<String>(col_str);
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(k2, tv.get_key(0));
}

TEST(Query_DeepLink)
{

    //
    // +---------+--------+------------+
    // | int     | bool   | list       |
    // +---------+--------+------------+
    // |       0 | true   | null       |
    // |       1 | false  | 0          |
    // |       2 | true   | 0, 1       |
    // |       N | even(N)| 0, .., N-1 |
    // +---------+--------+-------------+

    const int N = 10;

    Group group;
    TableRef table = group.add_table("test");
    table->add_column(type_Int, "int");
    auto col_bool = table->add_column(type_Bool, "bool");
    auto col_linklist = table->add_column_list(*table, "list");

    for (int j = 0; j < N; ++j) {
        TableView view = table->where().find_all();

        Obj obj = table->create_object().set_all(j, (j % 2) == 0);
        auto ll = obj.get_linklist(col_linklist);
        for (size_t i = 0; i < view.size(); ++i) {
            ll.add(view.get_key(i));
        }
    }

    Query query = table->link(col_linklist).column<Bool>(col_bool) == true;
    TableView view = query.find_all();
    CHECK_EQUAL(N - 1, view.size());
}

TEST(Query_LinksToDeletedOrMovedRow)
{
    // This test is not that relevant with stable keys
    Group group;

    TableRef source = group.add_table("source");
    TableRef target = group.add_table("target");

    auto col_link = source->add_column(*target, "link");
    auto col_name = target->add_column(type_String, "name");

    ObjKeys keys({4, 6, 8});
    target->create_objects(keys);
    target->get_object(keys[0]).set(col_name, "A");
    target->get_object(keys[1]).set(col_name, "B");
    target->get_object(keys[2]).set(col_name, "C");

    source->create_object().set(col_link, keys[0]);
    source->create_object().set(col_link, keys[1]).get_key();
    source->create_object().set(col_link, keys[2]);

    Query qA = source->column<Link>(col_link) == target->get_object(keys[0]);
    Query qB = source->column<Link>(col_link) == target->get_object(keys[1]);
    Query qC = source->column<Link>(col_link) == target->get_object(keys[2]);

    // Remove first object
    target->remove_object(keys[0]);

    // Row A should not be found as it has been removed.
    TableView tvA = qA.find_all();
    CHECK_EQUAL(0, tvA.size());

    // Row B should be found as it was not changed.
    TableView tvB = qB.find_all();
    CHECK_EQUAL(1, tvB.size());
    CHECK_EQUAL(keys[1], tvB[0].get<ObjKey>(col_link));
    CHECK_EQUAL("B", tvB.get(0).get_linked_object(col_link).get<String>(col_name));

    // Row C should still be found
    TableView tvC = qC.find_all();
    CHECK_EQUAL(1, tvC.size());
    CHECK_EQUAL(keys[2], tvC[0].get<ObjKey>(col_link));
    CHECK_EQUAL("C", tvC.get(0).get_linked_object(col_link).get<String>(col_name));
}

// Triggers bug in compare_relation()
TEST(Query_BrokenFindGT)
{
    Group group;
    TableRef table = group.add_table("test");
    auto col = table->add_column(type_Int, "int");

    const size_t rows = 12;
    for (size_t i = 0; i < rows; ++i) {
        table->create_object().set(col, int64_t(i + 2));
    }

    table->create_object().set(col, 1);
    table->create_object().set(col, 1);
    table->create_object().set(col, 1);

    for (size_t i = 0; i < 3; ++i) {
        table->create_object().set(col, int64_t(i + 2));
    }

    CHECK_EQUAL(18, table->size());

    Query q = table->where().greater(col, 1);
    TableView tv = q.find_all();
    CHECK_EQUAL(15, tv.size());

    for (size_t i = 0; i < tv.size(); ++i) {
        CHECK_NOT_EQUAL(1, tv[i].get<Int>(col));
    }
}

// Small fuzzy test also to trigger bugs such as the compare_relation() bug above
TEST(Query_FuzzyFind)
{
    // TEST_DURATION is normally 0.
    for (size_t iter = 0; iter < 50 + TEST_DURATION * 2000; iter++) {
        Group group;
        TableRef table = group.add_table("test");
        auto col = table->add_column(type_Int, "int");

        // The bug happened when values were stored in 4 bits or less. So create a table full of such random values
        const size_t rows = 18;
        for (size_t i = 0; i < rows; ++i) {
            // Produce numbers -3 ... 17. Just to test edge cases around 4-bit values also
            int64_t t = (fastrand() % 21) - 3;

            table->create_object().set(col, t);
        }

        for (int64_t s = -2; s < 18; s++) {
            Query q_g = table->where().greater(col, s);
            TableView tv_g = q_g.find_all();
            for (size_t i = 0; i < tv_g.size(); ++i) {
                CHECK(tv_g[i].get<Int>(col) > s);
            }

            Query q_l = table->where().less(col, s);
            TableView tv_l = q_l.find_all();
            for (size_t i = 0; i < tv_l.size(); ++i) {
                CHECK(tv_l[i].get<Int>(col) < s);
            }

            Query q_le = table->where().less_equal(col, s);
            TableView tv_le = q_le.find_all();
            for (size_t i = 0; i < tv_le.size(); ++i) {
                CHECK(tv_le[i].get<Int>(col) <= s);
            }

            // Sum of values greater + less-or-equal should be total number of rows. This ensures that both
            // 1) no search results are *omitted* from find_all(), and no 2) results are *false* positives
            CHECK(tv_g.size() + tv_le.size() == rows);
        }
    }
}

TEST(Query_AverageNullableColumns)
{
    Table table;
    auto col_int = table.add_column(type_Int, "int", true);
    auto col_float = table.add_column(type_Float, "float", true);
    auto col_double = table.add_column(type_Double, "double", true);

    CHECK_EQUAL(0, table.where().average_int(col_int));
    CHECK_EQUAL(0, table.where().average_float(col_float));
    CHECK_EQUAL(0, table.where().average_double(col_double));

    //
    // +-----+-------+--------+
    // | int | float | double |
    // +-----+-------+--------+
    // |   2 |     2 |      2 |
    // |   4 |     4 |      4 |
    // +-----+-------+--------+

    table.create_object().set_all(2, 2.0f, 2.0);
    table.create_object().set_all(4, 4.0f, 4.0);

    CHECK_EQUAL(3, table.where().average_int(col_int));
    CHECK_EQUAL(3, table.where().average_float(col_float));
    CHECK_EQUAL(3, table.where().average_double(col_double));

    // Add a row with nulls in each column. These nulls must be treated as not existing, that is,
    // it must be such that the average of 2 + 2 + null == 2.
    table.create_object();

    CHECK_EQUAL(3, table.where().average_int(col_int));
    CHECK_EQUAL(3, table.where().average_float(col_float));
    CHECK_EQUAL(3, table.where().average_double(col_double));
}

TEST(Query_NegativeNumbers)
{
    for (size_t nullable = 0; nullable < 2; ++nullable) {
        Group group;
        TableRef table = group.add_table("test");
        auto c0 = table->add_column(type_Int, "int", nullable == 0);

        int64_t id = -1;
        for (size_t i = 0; i < 10; ++i) {
            table->create_object().set_all(id--);
        }

        CHECK_EQUAL(10, table->where().between(c0, -10, -1).find_all().size());
        CHECK_EQUAL(10, (table->column<Int>(c0) > -11).find_all().size());
        CHECK_EQUAL(10, table->where().greater(c0, -11).find_all().size());
        CHECK_EQUAL(10, (table->column<Int>(c0) >= -10).find_all().size());
        CHECK_EQUAL(10, table->where().greater_equal(c0, -10).find_all().size());
        CHECK_EQUAL(10, (table->column<Int>(c0) < 128).find_all().size());
        CHECK_EQUAL(10, table->where().less(c0, 128).find_all().size());
        CHECK_EQUAL(10, (table->column<Int>(c0) < 127).find_all().size());
        CHECK_EQUAL(10, table->where().less(c0, 127).find_all().size());
        CHECK_EQUAL(10, (table->column<Int>(c0) <= -1).find_all().size());
        CHECK_EQUAL(10, table->where().less_equal(c0, -1).find_all().size());
        CHECK_EQUAL(10, (table->column<Int>(c0) < 0).find_all().size());
        TableView view = table->where().less(c0, 0).find_all();
        CHECK_EQUAL(10, view.size());

        id = -1;
        for (size_t i = 0; i < view.size(); ++i) {
            if (nullable == 0) {
                CHECK_EQUAL(id, view.get(i).get<Optional<Int>>(c0));
            }
            else {
                CHECK_EQUAL(id, view.get(i).get<Int>(c0));
            }
            id--;
        }
    }
}

template <class T>
int64_t unbox(const T& val)
{
    return val;
}

template <>
int64_t unbox(const util::Optional<int64_t>& val)
{
    return *val;
}

TEST_TYPES(Query_EqualityInts, int64_t, util::Optional<int64_t>)
{
    Group group;
    TableRef table = group.add_table("test");
    auto col_ndx = table->add_column(type_Int, "int", std::is_same<TEST_TYPE, util::Optional<int64_t>>::value);

    int64_t id = -1;
    int64_t sum = 0;
    constexpr static size_t num_rows = REALM_MAX_BPNODE_SIZE + 10;
    for (size_t i = 0; i < num_rows; ++i) {
        sum += id;
        table->create_object().set<Int>(col_ndx, id++);
    }

    bool first = true;
    for (auto& obj : *table) {
        int64_t target = unbox(obj.get<TEST_TYPE>(col_ndx));
        Query q_eq = table->where().equal(col_ndx, target);
        CHECK_EQUAL(q_eq.find(), obj.get_key());
        CHECK_EQUAL(q_eq.count(), 1);
        CHECK_EQUAL(q_eq.sum_int(col_ndx), target);
        CHECK_EQUAL(q_eq.average_int(col_ndx), target);

        Query q_neq = table->where().not_equal(col_ndx, target);
        CHECK_EQUAL(q_neq.find(), first ? ObjKey(1) : ObjKey(0));
        CHECK_EQUAL(q_neq.count(), num_rows - 1);
        CHECK_EQUAL(q_neq.sum_int(col_ndx), sum - target);
        CHECK_EQUAL(q_neq.average_int(col_ndx), (sum - target) / double(num_rows - 1));
        first = false;
    }
}

// Exposes bug that would lead to nulls being included as 0 value in average when performed
// on Query. When performed on TableView or Table, it worked OK.
TEST(Query_MaximumSumAverage)
{
    for (int nullable = 0; nullable < 2; nullable++) {
        bool n = (nullable == 1);
        Group group;
        TableRef table1 = group.add_table("table1");
        ColKey c0 = table1->add_column(type_Int, "int1", /* nullable */ n);
        ColKey c1 = table1->add_column(type_Int, "int2", /* nullable */ n);
        ColKey c2 = table1->add_column(type_Double, "d", /* nullable */ n);

        // Create three identical columns with values: For the nullable case:
        //      3, 4, null
        // For non-nullable iteration:
        //      3, 4

        table1->create_object().set_all(3, 3, 3.0);
        table1->create_object().set_all(4, 4, 4.0);
        if (n)
            table1->create_object();

        // Average
        {
            double d;

            // Those that have criterias include all rows, also those with null
            d = table1->where().average_int(c0);
            CHECK_APPROXIMATELY_EQUAL(d, 7. / 2., 0.001);

            d = table1->where().average_int(c1);
            CHECK_APPROXIMATELY_EQUAL(d, 7. / 2., 0.001);

            // Criteria on same column as average
            d = table1->where().not_equal(c0, 1234).average_int(c0);
            CHECK_APPROXIMATELY_EQUAL(d, 7. / 2., 0.001);

            // Criteria on other column than average (triggers different code paths)
            d = table1->where().not_equal(c0, 1234).average_int(c1);
            CHECK_APPROXIMATELY_EQUAL(d, 7. / 2., 0.001);

            // Average of double, criteria on integer
            d = table1->where().not_equal(c0, 1234).average_double(c2);
            CHECK_APPROXIMATELY_EQUAL(d, 7. / 2., 0.001);

            d = table1->where().not_equal(c2, 1234.).average_double(c2);
            CHECK_APPROXIMATELY_EQUAL(d, 7. / 2., 0.001);

            d = (table1->column<Int>(c0) == null()).average_int(c0);
            CHECK_EQUAL(d, 0);

            d = (table1->column<Int>(c0) != null()).average_int(c0);
            CHECK_APPROXIMATELY_EQUAL(d, 7. / 2., 0.001);

            // Those with criteria now only include some rows, whereof none are null
            d = table1->where().average_int(c0);
            CHECK_APPROXIMATELY_EQUAL(d, 7. / 2., 0.001);

            d = table1->where().average_int(c1);
            CHECK_APPROXIMATELY_EQUAL(d, 7. / 2., 0.001);

            // Criteria on same column as average
            d = table1->where().equal(c0, 3).average_int(c0);
            CHECK_APPROXIMATELY_EQUAL(d, 3., 0.001);

            // Criteria on other column than average (triggers different code paths)
            d = table1->where().equal(c0, 3).average_int(c1);
            CHECK_APPROXIMATELY_EQUAL(d, 3., 0.001);

            // Average of double, criteria on integer
            d = table1->where().not_equal(c0, 3).average_double(c2);
            CHECK_APPROXIMATELY_EQUAL(d, 4., 0.001);

            d = table1->where().equal(c2, 3.).average_double(c2);
            CHECK_APPROXIMATELY_EQUAL(d, 3., 0.001);

            // Now using null as criteria
            d = (table1->column<Int>(c0) != null()).average_double(c2);
            CHECK_APPROXIMATELY_EQUAL(d, 7. / 2., 0.001);

            d = (table1->column<Double>(c2) != null()).average_double(c2);
            CHECK_APPROXIMATELY_EQUAL(d, 7. / 2., 0.001);

            d = (table1->column<Int>(c0) != null()).average_int(c0);
            CHECK_APPROXIMATELY_EQUAL(d, 7. / 2., 0.001);

            d = (table1->column<Int>(c1) != null()).average_int(c0);
            CHECK_APPROXIMATELY_EQUAL(d, 7. / 2., 0.001);
        }


        // Maximum
        {
            int64_t d;
            double dbl;
            // Those that have criterias include all rows, also those with null
            d = table1->where().maximum_int(c0);
            CHECK_EQUAL(d, 4);

            d = table1->where().maximum_int(c1);
            CHECK_EQUAL(d, 4);

            // Criteria on same column as maximum
            d = table1->where().not_equal(c0, 1234).maximum_int(c0);
            CHECK_EQUAL(d, 4);

            // Criteria on other column than maximum (triggers different code paths)
            d = table1->where().not_equal(c0, 1234).maximum_int(c1);
            CHECK_EQUAL(d, 4);

            // Average of double, criteria on integer
            dbl = table1->where().not_equal(c0, 1234).maximum_double(c2);
            CHECK_EQUAL(d, 4);

            dbl = table1->where().not_equal(c2, 1234.).maximum_double(c2);
            CHECK_EQUAL(d, 4.);

            // Those with criteria now only include some rows, whereof none are null
            d = table1->where().maximum_int(c0);
            CHECK_EQUAL(d, 4);

            d = table1->where().maximum_int(c1);
            CHECK_EQUAL(d, 4);

            // Criteria on same column as maximum
            d = table1->where().equal(c0, 4).maximum_int(c0);
            CHECK_EQUAL(d, 4);

            // Criteria on other column than maximum (triggers different code paths)
            d = table1->where().equal(c0, 4).maximum_int(c1);
            CHECK_EQUAL(d, 4);

            // Average of double, criteria on integer
            dbl = table1->where().not_equal(c0, 3).maximum_double(c2);
            CHECK_EQUAL(dbl, 4.);

            dbl = table1->where().equal(c2, 3.).maximum_double(c2);
            CHECK_EQUAL(dbl, 3.);

            // Now using null as criteria
            dbl = (table1->column<Int>(c0) != null()).maximum_double(c2);
            CHECK_EQUAL(dbl, 4.);

            dbl = (table1->column<Double>(c2) != null()).maximum_double(c2);
            CHECK_EQUAL(dbl, 4.);

            d = (table1->column<Int>(c0) != null()).maximum_int(c0);
            CHECK_EQUAL(d, 4);

            d = (table1->column<Int>(c1) != null()).maximum_int(c0);
            CHECK_EQUAL(d, 4);
        }


        // Minimum
        {
            int64_t d;
            double dbl;
            // Those that have criterias include all rows, also those with null
            d = table1->where().minimum_int(c0);
            CHECK_EQUAL(d, 3);

            d = table1->where().minimum_int(c1);
            CHECK_EQUAL(d, 3);

            // Criteria on same column as minimum
            d = table1->where().not_equal(c0, 1234).minimum_int(c0);
            CHECK_EQUAL(d, 3);

            // Criteria on other column than minimum (triggers different code paths)
            d = table1->where().not_equal(c0, 1234).minimum_int(c1);
            CHECK_EQUAL(d, 3);

            // Average of double, criteria on integer
            dbl = table1->where().not_equal(c0, 1234).minimum_double(c2);
            CHECK_EQUAL(dbl, 3);

            dbl = table1->where().not_equal(c2, 1234.).minimum_double(c2);
            CHECK_EQUAL(dbl, 3.);


            // Those with criteria now only include some rows, whereof none are null
            d = table1->where().minimum_int(c0);
            CHECK_EQUAL(d, 3);

            d = table1->where().minimum_int(c1);
            CHECK_EQUAL(d, 3);

            // Criteria on same column as minimum
            d = table1->where().equal(c0, 4).minimum_int(c0);
            CHECK_EQUAL(d, 4);

            // Criteria on other column than minimum (triggers different code paths)
            d = table1->where().equal(c0, 4).minimum_int(c1);
            CHECK_EQUAL(d, 4);

            // Average of double, criteria on integer
            dbl = table1->where().not_equal(c0, 3).minimum_double(c2);
            CHECK_EQUAL(dbl, 4.);

            dbl = table1->where().equal(c2, 3.).minimum_double(c2);
            CHECK_EQUAL(dbl, 3.);

            // Now using null as criteria
            dbl = (table1->column<Int>(c0) != null()).minimum_double(c2);
            CHECK_EQUAL(dbl, 3.);

            dbl = (table1->column<Double>(c2) != null()).minimum_double(c2);
            CHECK_EQUAL(dbl, 3.);

            d = (table1->column<Int>(c0) != null()).minimum_int(c0);
            CHECK_EQUAL(d, 3);

            d = (table1->column<Int>(c1) != null()).minimum_int(c0);
            CHECK_EQUAL(d, 3);
        }

        // Sum
        {
            int64_t d;
            double dbl;
            // Those that have criterias include all rows, also those with null
            d = table1->where().sum_int(c0);
            CHECK_EQUAL(d, 7);

            // Criteria on same column as maximum
            d = table1->where().not_equal(c0, 1234).sum_int(c0);
            CHECK_EQUAL(d, 7);

            // Criteria on other column than maximum (triggers different code paths)
            d = table1->where().not_equal(c0, 1234).sum_int(c1);
            CHECK_EQUAL(d, 7);

            d = (table1->column<Int>(c0) == null()).sum_int(c0);
            CHECK_EQUAL(d, 0);

            d = (table1->column<Int>(c0) != null()).sum_int(c0);
            CHECK_EQUAL(d, 7);

            // Average of double, criteria on integer
            dbl = table1->where().not_equal(c0, 1234).sum_double(c2);
            CHECK_EQUAL(dbl, 7.);

            dbl = table1->where().not_equal(c2, 1234.).sum_double(c2);
            CHECK_APPROXIMATELY_EQUAL(dbl, 7., 0.001);


            // Those with criteria now only include some rows, whereof none are null
            d = table1->where().sum_int(c0);
            CHECK_EQUAL(d, 7);

            d = table1->where().sum_int(c1);
            CHECK_EQUAL(d, 7);

            // Criteria on same column as maximum
            d = table1->where().equal(c0, 4).sum_int(c0);
            CHECK_EQUAL(d, 4);

            // Criteria on other column than maximum (triggers different code paths)
            d = table1->where().equal(c0, 4).sum_int(c1);
            CHECK_EQUAL(d, 4);

            // Average of double, criteria on integer
            dbl = table1->where().not_equal(c0, 3).sum_double(c2);
            CHECK_APPROXIMATELY_EQUAL(dbl, 4., 0.001);

            dbl = table1->where().equal(c2, 3.).sum_double(c2);
            CHECK_APPROXIMATELY_EQUAL(dbl, 3., 0.001);

            // Now using null as criteria
            dbl = (table1->column<Int>(c0) != null()).sum_double(c2);
            CHECK_APPROXIMATELY_EQUAL(dbl, 7., 0.001);

            dbl = (table1->column<Double>(c2) != null()).sum_double(c2);
            CHECK_APPROXIMATELY_EQUAL(dbl, 7., 0.001);

            d = (table1->column<Int>(c0) != null()).sum_int(c0);
            CHECK_EQUAL(d, 7);

            d = (table1->column<Int>(c1) != null()).sum_int(c0);
            CHECK_EQUAL(d, 7);
        }


        // Count
        {
            int64_t d;
            d = table1->where().count();
            CHECK_EQUAL(d, n ? 3 : 2);

            d = table1->where().not_equal(c0, 1234).count();
            CHECK_EQUAL(d, n ? 3 : 2);

            d = table1->where().equal(c0, 4).count();
            CHECK_EQUAL(d, 1);

            d = table1->where().not_equal(c0, 3).count();
            CHECK_EQUAL(d, n ? 2 : 1);

            d = table1->where().equal(c2, 3.).count();
            CHECK_EQUAL(d, 1);

            // Now using null as criteria
            d = (table1->column<Int>(c0) != null()).count();
            CHECK_EQUAL(d, 2);

            d = (table1->column<Double>(c2) != null()).count();
            CHECK_EQUAL(d, 2);

            d = (table1->column<Int>(c0) == null()).count();
            CHECK_EQUAL(d, n ? 1 : 0);

            d = (table1->column<Int>(c0) != null()).count();
            CHECK_EQUAL(d, 2);

            d = (table1->column<Int>(c1) != null()).count();
            CHECK_EQUAL(d, 2);
        }
    }
}

TEST(Query_ReferDeletedLinkView)
{
    // Queries and TableViews that depend on a deleted LinkList will now produce valid empty-like results
    // (find() returns npos, find_all() returns empty TableView, sum() returns 0, etc.).
    // They will no longer throw exceptions or crash.
    Group group;
    TableRef table = group.add_table("table");
    auto col_link = table->add_column_list(*table, "children");
    auto col_int = table->add_column(type_Int, "age");
    auto links = table->create_object().set(col_int, 123).get_linklist(col_link);
    Query q = table->where(links);
    TableView tv = q.find_all();

    // TableView that depends on LinkView soon to be deleted
    TableView tv_sorted = links.get_sorted_view(col_int);

    // First test depends_on_deleted_object()
    CHECK(!tv_sorted.depends_on_deleted_object());
    TableView tv2 = table->where(&tv).find_all();
    CHECK(!tv2.depends_on_deleted_object());

    // Delete LinkList so LinkView gets detached
    table->remove_object(table->begin());
    CHECK(!links.is_attached());
    CHECK(tv_sorted.depends_on_deleted_object());

    // See if "Query that depends on LinkView" returns sane "empty"-like values
    CHECK_EQUAL(q.find_all().size(), 0);
    CHECK_EQUAL(q.find(), null_key);
    CHECK_EQUAL(q.sum_int(col_int), 0);
    CHECK_EQUAL(q.count(), 0);
    size_t rows;
    q.average_int(col_int, &rows);
    CHECK_EQUAL(rows, 0);

    tv_sorted.sync_if_needed();
    // See if "TableView that depends on LinkView" returns sane "empty"-like values
    tv_sorted.average_int(col_int, &rows);
    CHECK_EQUAL(rows, 0);

    // Now check a "Query that depends on (TableView that depends on LinkView)"
    Query q2 = table->where(&tv_sorted);
    CHECK_EQUAL(q2.count(), 0);
    CHECK_EQUAL(q2.find(), null_key);

    CHECK(!links.is_attached());
    tv.sync_if_needed();

    // PLEASE NOTE that 'tv' will still return true in this case! Even though it indirectly depends on
    // the LinkView through multiple levels!
    CHECK(tv.is_attached());

    // Before executing any methods on a LinkList, you must still always check is_attached(). If you
    // call links->add() on a deleted LinkViewRef (where is_attached() == false), it will assert
    CHECK(!links.is_attached());
}

TEST(Query_SubQueries)
{
    Group group;

    TableRef origin = group.add_table("origin");
    TableRef target = group.add_table("target");

    // add some more columns to origin and target
    auto col_int_t = target->add_column(type_Int, "integers");
    auto col_string_t = target->add_column(type_String, "strings");
    // in order to use set_all, columns involved in set_all must be inserted first.
    auto col_link_o = origin->add_column_list(*target, "link");


    // add some rows
    origin->create_object(ObjKey(0));
    origin->create_object(ObjKey(1));
    origin->create_object(ObjKey(2));

    target->create_object(ObjKey(0)).set_all(400, "hello");
    target->create_object(ObjKey(1)).set_all(500, "world");
    target->create_object(ObjKey(2)).set_all(600, "!");
    target->create_object(ObjKey(3)).set_all(600, "world");

    // set some links
    auto links0 = origin->get_object(ObjKey(0)).get_linklist(col_link_o);
    links0.add(ObjKey(1));

    auto links1 = origin->get_object(ObjKey(1)).get_linklist(col_link_o);
    links1.add(ObjKey(1));
    links1.add(ObjKey(2));

    ObjKey match;
    TableView tv;
    Query q;
    Query sub_query;

    // The linked rows for rows 0 and 2 all match ("world", 500). Row 2 does by virtue of having no rows.
    sub_query = target->column<String>(col_string_t) == "world" && target->column<Int>(col_int_t) == 500;
    q = origin->column<Link>(col_link_o, sub_query).count() == origin->column<Link>(col_link_o).count();
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(ObjKey(0), tv.get_key(0));
    CHECK_EQUAL(ObjKey(2), tv.get_key(1));

    // No linked rows match ("world, 600).
    sub_query = target->column<String>(col_string_t) == "world" && target->column<Int>(col_int_t) == 600;
    q = origin->column<Link>(col_link_o, sub_query).count() >= 1;
    match = q.find();
    CHECK_EQUAL(match, null_key);

    // Rows 0 and 1 both have at least one linked row that matches ("world", 500).
    sub_query = target->column<String>(col_string_t) == "world" && target->column<Int>(col_int_t) == 500;
    q = origin->column<Link>(col_link_o, sub_query).count() >= 1;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(ObjKey(0), tv.get_key(0));
    CHECK_EQUAL(ObjKey(1), tv.get_key(1));

    // Row 1 has at least one linked row that matches ("!", 600).
    sub_query = target->column<String>(col_string_t) == "!" && target->column<Int>(col_int_t) == 600;
    q = origin->column<Link>(col_link_o, sub_query).count() >= 1;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(ObjKey(1), tv.get_key(0));

    // Row 1 has two linked rows that contain either "world" or 600.
    sub_query = target->column<String>(col_string_t) == "world" || target->column<Int>(col_int_t) == 600;
    q = origin->column<Link>(col_link_o, sub_query).count() == 2;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(ObjKey(1), tv.get_key(0));

    // Rows 0 and 2 have at most one linked row that contains either "world" or 600. Row 2 does by virtue of having no
    // rows.
    sub_query = target->column<String>(col_string_t) == "world" || target->column<Int>(col_int_t) == 600;
    q = origin->column<Link>(col_link_o, sub_query).count() <= 1;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(ObjKey(0), tv.get_key(0));
    CHECK_EQUAL(ObjKey(2), tv.get_key(1));
}

// Ensure that Query's move constructor and move assignment operator don't result in
// a TableView owned by the query being double-deleted when the queries are destroyed.
TEST(Query_MoveDoesntDoubleDelete)
{
    Table table;
    ConstTableRef ref = ConstTableRef::unsafe_create(&table);
    {
        Query q1(ref, std::unique_ptr<TableView>(new TableView()));
        Query q2 = std::move(q1);
    }

    {
        Query q1(ref, std::unique_ptr<TableView>(new TableView()));
        Query q2;
        q2 = std::move(q1);
    }
}

TEST(Query_Timestamp)
{
    ObjKey match;
    size_t cnt;
    Table table;
    auto col_first = table.add_column(type_Timestamp, "first", true);
    auto col_second = table.add_column(type_Timestamp, "second", true);
    Columns<Timestamp> first = table.column<Timestamp>(col_first);
    Columns<Timestamp> second = table.column<Timestamp>(col_second);

    std::vector<ObjKey> keys;
    table.create_objects(6, keys);
    table.get_object(keys[0]).set(col_first, Timestamp(111, 222));
    table.get_object(keys[1]).set(col_first, Timestamp(111, 333));
    table.get_object(keys[2]).set(col_first, Timestamp(333, 444)).set(col_second, Timestamp(222, 222));
    table.get_object(keys[3]).set(col_first, Timestamp{});
    table.get_object(keys[4]).set(col_first, Timestamp(0, 0));
    table.get_object(keys[5]).set(col_first, Timestamp(-1000, 0));


    CHECK(table.get_object(keys[0]).get<Timestamp>(col_first) == Timestamp(111, 222));

    match = (first == Timestamp(111, 222)).find();
    CHECK_EQUAL(match, keys[0]);

    match = (first != Timestamp(111, 222)).find();
    CHECK_EQUAL(match, keys[1]);

    match = (first > Timestamp(111, 222)).find();
    CHECK_EQUAL(match, keys[1]);

    match = (first < Timestamp(111, 333)).find();
    CHECK_EQUAL(match, keys[0]);

    match = (first == Timestamp(0, 0)).find();
    CHECK_EQUAL(match, keys[4]);

    match = (first < Timestamp(111, 333)).find();
    CHECK_EQUAL(match, keys[0]);

    match = (first < Timestamp(0, 0)).find();
    CHECK_EQUAL(match, keys[5]);

    // Note: .count(), not find()
    cnt = (first < Timestamp(0, 0)).count();
    CHECK_EQUAL(cnt, 1);

    cnt = (first != Timestamp{}).count();
    CHECK_EQUAL(cnt, 5);

    cnt = (first != null{}).count();
    CHECK_EQUAL(cnt, 5);

    cnt = (first != Timestamp(0, 0)).count();
    CHECK_EQUAL(cnt, 5);

    cnt = (first > null{}).count();
    CHECK_EQUAL(cnt, 0);

    cnt = (first < null{}).count();
    CHECK_EQUAL(cnt, 0);

    cnt = (first >= null{}).count();
    CHECK_EQUAL(cnt, 1);

    cnt = (first <= null{}).count();
    CHECK_EQUAL(cnt, 1);

    cnt = (first != Timestamp(0, 0)).count();
    CHECK_EQUAL(cnt, 5);

    match = (first < Timestamp(-100, 0)).find();
    CHECK_EQUAL(match, keys[5]);

    cnt = (first >= Timestamp(std::numeric_limits<int64_t>::min(), -Timestamp::nanoseconds_per_second + 1)).count();
    CHECK_EQUAL(cnt, 5);

    cnt = (first > Timestamp(std::numeric_limits<int64_t>::min(), -Timestamp::nanoseconds_per_second + 1)).count();
    CHECK_EQUAL(cnt, 5);

    cnt = (first <= Timestamp(std::numeric_limits<int64_t>::max(), Timestamp::nanoseconds_per_second - 1)).count();
    CHECK_EQUAL(cnt, 5);

    cnt = (first < Timestamp(std::numeric_limits<int64_t>::max(), Timestamp::nanoseconds_per_second - 1)).count();
    CHECK_EQUAL(cnt, 5);

    // Left-hand-side being Timestamp() constant, right being column
    match = (Timestamp(111, 222) == first).find();
    CHECK_EQUAL(match, keys[0]);

    match = (Timestamp{} == first).find();
    CHECK_EQUAL(match, keys[3]);

    match = (Timestamp(111, 222) > first).find();
    CHECK_EQUAL(match, keys[4]);

    match = (Timestamp(111, 333) < first).find();
    CHECK_EQUAL(match, keys[2]);

    match = (Timestamp(111, 222) >= first).find();
    CHECK_EQUAL(match, keys[0]);

    match = (Timestamp(111, 111) >= first).find();
    CHECK_EQUAL(match, keys[4]);

    match = (Timestamp(333, 444) <= first).find();
    CHECK_EQUAL(match, keys[2]);

    match = (Timestamp(111, 300) <= first).find();
    CHECK_EQUAL(match, keys[1]);

    match = (Timestamp(111, 222) != first).find();
    CHECK_EQUAL(match, keys[1]);

    // Compare column with self
    match = (first == first).find();
    CHECK_EQUAL(match, keys[0]);

    match = (first != first).find();
    CHECK_EQUAL(match, null_key);

    match = (first > first).find();
    CHECK_EQUAL(match, null_key);

    match = (first < first).find();
    CHECK_EQUAL(match, null_key);

    match = (first >= first).find();
    CHECK_EQUAL(match, keys[0]);

    match = (first <= first).find();
    CHECK_EQUAL(match, keys[0]);

    // Two different columns
    match = (first == second).find();
    CHECK_EQUAL(match, keys[3]); // null == null

    match = (first > second).find();
    CHECK_EQUAL(match, keys[2]); // Timestamp(333, 444) > Timestamp(111, 222)

    match = (first < second).find();
    CHECK_EQUAL(match, null_key); // Note that (null < null) == false
}

TEST(Query_TimestampCount)
{
    Table table;
    auto col_date = table.add_column(type_Timestamp, "date", true);
    for (int i = 0; i < 10; i++) {
        table.create_object().set(col_date, Timestamp(i / 4, i % 4));
    }
    table.get_object(5).set_null(col_date);

    // Timestamps : {0,0}, {0,1}, {0,2}, {0,3}, {1,0}, {}, {1,2}, {1,3}, {2,0}, {2,1}

    auto timestamps = table.column<Timestamp>(col_date);

    CHECK_EQUAL((timestamps > Timestamp(0, 3)).count(), 5);
    CHECK_EQUAL((timestamps >= Timestamp(0, 3)).count(), 6);
    CHECK_EQUAL((timestamps < Timestamp(1, 3)).count(), 6);
    CHECK_EQUAL((timestamps <= Timestamp(1, 3)).count(), 7);
    CHECK_EQUAL((timestamps == Timestamp(0, 2)).count(), 1);
    CHECK_EQUAL((timestamps != Timestamp(0, 2)).count(), 9);
    CHECK_EQUAL((timestamps == Timestamp()).count(), 1);
    CHECK_EQUAL((timestamps != Timestamp()).count(), 9);
}

TEST(Query_Timestamp_Null)
{
    // Test that querying for null on non-nullable column (with default value being non-null value) is
    // possible (i.e. does not throw or fail) and also gives no search matches.
    Table table;
    ObjKey match;

    auto col0 = table.add_column(type_Timestamp, "first", false);
    auto col1 = table.add_column(type_Timestamp, "second", true);
    ObjKey k0 = table.create_object().get_key();

    Columns<Timestamp> first = table.column<Timestamp>(col0);
    Columns<Timestamp> second = table.column<Timestamp>(col1);

    match = (first == Timestamp{}).find();
    CHECK_EQUAL(match, null_key);

    match = (second == Timestamp{}).find();
    CHECK_EQUAL(match, k0);
}

// Ensure that coyping a Query copies a restricting TableView if the query owns the view.
TEST(Query_CopyRestrictingTableViewWhenOwned)
{
    Table table;
    ConstTableRef ref = ConstTableRef::unsafe_create(&table);
    {
        Query q1(ref, std::unique_ptr<TableView>(new TableView()));
        Query q2(q1);

        // Reset the source query, destroying the original TableView.
        q1 = {};

        // Operations on the copied query that touch the restricting view should not crash.
        CHECK_EQUAL(0, q2.count());
    }

    {
        Query q1(ref, std::unique_ptr<TableView>(new TableView()));
        Query q2;
        q2 = q1;

        // Reset the source query, destroying the original TableView.
        q1 = {};

        // Operations on the copied query that touch the restricting view should not crash.
        CHECK_EQUAL(0, q2.count());
    }
}

TEST(Query_SyncViewIfNeeded)
{
    Group group;
    TableRef source = group.add_table("source");
    TableRef target = group.add_table("target");

    auto col_links = source->add_column_list(*target, "link");
    auto col_id = target->add_column(type_Int, "id");

    auto reset_table_contents = [&] {
        source->clear();
        target->clear();

        for (int64_t i = 0; i < 15; ++i) {
            target->create_object(ObjKey(i)).set(col_id, i);
        }

        LnkLst ll = source->create_object().get_linklist(col_links);
        for (size_t i = 6; i < 15; ++i) {
            ll.add(ObjKey(i));
        }
    };

    // Restricting TableView. Query::sync_view_if_needed() syncs the TableView if needed.
    {
        reset_table_contents();
        TableView restricting_view = target->where().greater(col_id, 5).find_all();
        Query q = target->where(&restricting_view).less(col_id, 10);

        // Bring the view out of sync with the table.
        target->get_object(ObjKey(7)).set(col_id, -7);
        target->get_object(ObjKey(8)).set(col_id, -8);

        // Verify that the query uses the view as-is.
        CHECK_EQUAL(4, q.count());
        CHECK_EQUAL(false, restricting_view.is_in_sync());

        // And that syncing the query brings the view back into sync.
        auto version = q.sync_view_if_needed();
        CHECK_EQUAL(true, restricting_view.is_in_sync());
        CHECK_EQUAL(2, q.count());
        CHECK_EQUAL(version[0].first, target->get_key());
        CHECK_EQUAL(version[0].second, target->get_content_version());
    }

    // Restricting LinkView.
    {
        reset_table_contents();
        LnkLst restricting_view = source->begin()->get_linklist(col_links);
        Query q = target->where(restricting_view).less(col_id, 10);
        CHECK_EQUAL(restricting_view.size(), 9);
        CHECK_EQUAL(q.count(), 4);

        auto content = source->get_content_version();
        // Modify the underlying table to remove rows from the LinkView.
        target->remove_object(ObjKey(7));
        target->remove_object(ObjKey(8));
        CHECK_NOT_EQUAL(content, source->get_content_version());

        // A LnkLst is always in sync:
        CHECK_EQUAL(true, restricting_view.is_in_sync());
        CHECK_EQUAL(restricting_view.size(), 7);
        // The query correctly takes into account the changes to the restricting view
        CHECK_EQUAL(2, q.count());

        // And that syncing the query does nothing.
        auto version = q.sync_view_if_needed();
        CHECK_EQUAL(true, restricting_view.is_in_sync());
        CHECK_EQUAL(version[0].first, target->get_key());
        CHECK_EQUAL(version[0].second, target->get_content_version());
        CHECK_EQUAL(2, q.count());
    }

    // No restricting view. Query::sync_view_if_needed() does nothing.
    {
        reset_table_contents();
        Query q = target->where().greater(col_id, 5).less(col_id, 10);

        target->get_object(ObjKey(7)).set(col_id, -7);
        target->get_object(ObjKey(8)).set(col_id, -8);

        CHECK_EQUAL(2, q.count());

        auto version = q.sync_view_if_needed();
        CHECK_EQUAL(version[0].first, target->get_key());
        CHECK_EQUAL(version[0].second, target->get_content_version());
        CHECK_EQUAL(2, q.count());
    }

    // Query that is not associated with a Table. Query::sync_view_if_needed() does nothing.
    {
        reset_table_contents();
        Query q;

        auto version = q.sync_view_if_needed();
        CHECK(version.empty());
    }
}

// Ensure that two queries can be combined via Query::and_query, &&, and || even if one of them has no conditions.
TEST(Query_CombineWithEmptyQueryDoesntCrash)
{
    Table table;
    auto col_id = table.add_column(type_Int, "id");

    table.create_object().set(col_id, 0);
    table.create_object().set(col_id, 1);
    table.create_object().set(col_id, 2);

    {
        Query q = table.where().equal(col_id, 1);
        q.and_query(table.where());
        CHECK_EQUAL(1, q.find_all().size());
    }

    {
        Query q1 = table.where().equal(col_id, 1);
        Query q2 = table.where();
        q1.and_query(q2);
        CHECK_EQUAL(1, q1.count());
    }

    {
        Query q1 = table.where().equal(col_id, 1);
        Query q2 = table.where();
        q2.and_query(q1);
        CHECK_EQUAL(1, q2.count());
    }

    {
        Query q = table.where();
        q.and_query(table.where().equal(col_id, 1));
        CHECK_EQUAL(1, q.count());
    }

    {
        Query q1 = table.where().equal(col_id, 1);
        Query q2 = q1 && table.where();
        CHECK_EQUAL(1, q2.count());

        Query q3 = table.where() && q1;
        CHECK_EQUAL(1, q3.count());
    }

    {
        Query q1 = table.where().equal(col_id, 1);
        Query q2 = q1 || table.where();
        CHECK_EQUAL(1, q2.count());

        Query q3 = table.where() || q1;
        CHECK_EQUAL(1, q3.count());
    }
}

// Check that queries take into account restricting views, but still
// return row index into the underlying table
TEST(Query_AccountForRestrictingViews)
{
    Table table;
    auto col_id = table.add_column(type_Int, "id");

    table.create_object().set(col_id, 42);
    table.create_object().set(col_id, 43);
    table.create_object().set(col_id, 44);

    {
        // Create initial table view
        TableView results = table.where().equal(col_id, 44).find_all();
        CHECK_EQUAL(1, results.size());
        CHECK_EQUAL(44, results[0].get<Int>(col_id));

        // Create query based on restricting view
        Query q = Query(results.get_parent()->where(&results));
        ObjKey obj_key = q.find();
        CHECK_EQUAL(obj_key, results.get_key(0));
    }
}

/*

// These tests fail on Windows due to lack of tolerance for invalid UTF-8 in the case mapping methods

TEST(Query_UTF8_Contains)
{
    Group group;
    TableRef table1 = group.add_table("table1");
    table1->add_column(type_String, "str1");
    table1->add_empty_row();
    table1->set_string(0, 0, StringData("\x0ff\x000", 2));
    size_t m = table1->column<String>(0).contains(StringData("\x0ff\x000", 2), false).count();
    CHECK_EQUAL(1, m);
}


TEST(Query_UTF8_Contains_Fuzzy)
{
    Table table;
    table.add_column(type_String, "str1");
    table.add_empty_row();

    for (size_t t = 0; t < 10000; t++) {
        char haystack[10];
        char needle[7];

        for (size_t c = 0; c < 10; c++)
            haystack[c] = char(fastrand());

        for (size_t c = 0; c < 7; c++)
            needle[c] = char(fastrand());

        table.set_string(0, 0, StringData(haystack, 10));

        table.column<String>(0).contains(StringData(needle, fastrand(7)), false).count();
        table.column<String>(0).contains(StringData(needle, fastrand(7)), true).count();
    }
}
*/

TEST(Query_ArrayLeafRelocate)
{
    for (size_t iter = 0; iter < 10; iter++) {
        // Tests crash where a query node would have a SequentialGetter that pointed to an old array leaf
        // that was relocated. https://github.com/realm/realm-core/issues/2269
        // The above description does not apply to the cluster based implementation.
        Group group;

        TableRef contact = group.add_table("contact");
        TableRef contact_type = group.add_table("contact_type");

        auto col_int = contact_type->add_column(type_Int, "id");
        auto col_str = contact_type->add_column(type_String, "str");
        auto col_link = contact->add_column_list(*contact_type, "link");

        std::vector<ObjKey> contact_type_keys;
        std::vector<ObjKey> contact_keys;
        contact_type->create_objects(10, contact_type_keys);
        contact->create_objects(10, contact_keys);

        Query q1 = (contact->link(col_link).column<Int>(col_int) == 0);
        Query q2 = contact_type->where().equal(col_int, 0);
        Query q3 = contact_type->query("id + id == 0");
        Query q4 = (contact_type->column<Int>(col_int) == 0);
        Query q5 = (contact_type->column<String>(col_str) == "hejsa");

        TableView tv = q1.find_all();
        TableView tv2 = q2.find_all();
        TableView tv3 = q3.find_all();
        TableView tv4 = q4.find_all();
        TableView tv5 = q5.find_all();

        contact->add_column(type_Float, "extra");
        contact_type->add_column(type_Float, "extra");

        for (size_t t = 0; t < REALM_MAX_BPNODE_SIZE + 1; t++) {
            Obj contact_obj = contact->create_object();
            Obj contact_type_obj = contact_type->create_object();
            //  contact_type.get()->set_string(1, t, "hejsa");

            auto ll = contact_obj.get_linklist(col_link);
            ll.add(contact_type_obj.get_key());

            if (t == 0 || t == REALM_MAX_BPNODE_SIZE) {
                tv.sync_if_needed();
                tv2.sync_if_needed();
                tv3.sync_if_needed();
                tv4.sync_if_needed();
                tv5.sync_if_needed();
            }
        }
    }
}


TEST(Query_ColumnDeletionSimple)
{
    Table foo;
    auto col_int0 = foo.add_column(type_Int, "a");
    auto col_int1 = foo.add_column(type_Int, "b");

    std::vector<ObjKey> keys;
    foo.create_objects(10, keys);

    foo.get_object(keys[3]).set(col_int0, 123);
    foo.get_object(keys[4]).set(col_int0, 123);
    foo.get_object(keys[7]).set(col_int0, 123);
    foo.get_object(keys[2]).set(col_int1, 456);
    foo.get_object(keys[4]).set(col_int1, 456);

    auto q1 = foo.column<Int>(col_int0) == 123;
    auto q2 = foo.column<Int>(col_int1) == 456;
    auto q3 = q1 || q2;
    TableView tv1 = q1.find_all();
    TableView tv2 = q2.find_all();
    TableView tv3 = q3.find_all();
    CHECK_EQUAL(tv1.size(), 3);
    CHECK_EQUAL(tv2.size(), 2);
    CHECK_EQUAL(tv3.size(), 4);

    foo.remove_column(col_int0);

    size_t x = 0;
    CHECK_LOGIC_ERROR(x = q1.count(), LogicError::column_does_not_exist);
    CHECK_LOGIC_ERROR(tv1.sync_if_needed(), LogicError::column_does_not_exist);
    CHECK_EQUAL(x, 0);
    CHECK_EQUAL(tv1.size(), 0);

    // This one should succeed in spite the column index is 1 and we
    x = q2.count();
    tv2.sync_if_needed();
    CHECK_EQUAL(x, 2);
    CHECK_EQUAL(tv2.size(), 2);

    x = 0;
    CHECK_LOGIC_ERROR(x = q3.count(), LogicError::column_does_not_exist);
    CHECK_LOGIC_ERROR(tv3.sync_if_needed(), LogicError::column_does_not_exist);
    CHECK_EQUAL(x, 0);
    CHECK_EQUAL(tv3.size(), 0);
}


TEST(Query_ColumnDeletionExpression)
{
    Table foo;
    auto col_int0 = foo.add_column(type_Int, "a");
    auto col_int1 = foo.add_column(type_Int, "b");
    auto col_date2 = foo.add_column(type_Timestamp, "c");
    auto col_date3 = foo.add_column(type_Timestamp, "d");
    auto col_str4 = foo.add_column(type_String, "e");
    auto col_float5 = foo.add_column(type_Float, "f");
    auto col_bin6 = foo.add_column(type_Binary, "g");

    Obj obj0 = foo.create_object();
    Obj obj1 = foo.create_object();
    Obj obj2 = foo.create_object();
    Obj obj3 = foo.create_object();
    Obj obj4 = foo.create_object();
    obj0.set(col_int0, 0);
    obj1.set(col_int0, 1);
    obj2.set(col_int0, 2);
    obj3.set(col_int0, 3);
    obj4.set(col_int0, 4);
    obj0.set(col_int1, 0);
    obj1.set(col_int1, 0);
    obj2.set(col_int1, 3);
    obj3.set(col_int1, 5);
    obj4.set(col_int1, 3);
    obj0.set(col_date2, Timestamp(100, 100));
    obj0.set(col_date3, Timestamp(200, 100));
    obj0.set(col_str4, StringData("Hello, world"));
    obj0.set(col_float5, 3.141592f);
    obj1.set(col_float5, 1.0f);
    obj0.set(col_bin6, BinaryData("Binary", 6));

    // Expression
    auto q = foo.query("a == b + 1");
    // TwoColumnsNode
    auto q1 = foo.column<Int>(col_int0) == foo.column<Int>(col_int1);
    TableView tv = q.find_all();
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv1.size(), 1);

    foo.remove_column(col_int0);
    size_t x = 0;
    CHECK_LOGIC_ERROR(x = q.count(), LogicError::column_does_not_exist);
    CHECK_LOGIC_ERROR(tv.sync_if_needed(), LogicError::column_does_not_exist);
    CHECK_LOGIC_ERROR(tv1.sync_if_needed(), LogicError::column_does_not_exist);
    CHECK_EQUAL(x, 0);
    CHECK_EQUAL(tv.size(), 0);

    q = foo.column<Timestamp>(col_date2) < foo.column<Timestamp>(col_date3);
    // TimestampNode
    q1 = foo.column<Timestamp>(col_date3) == Timestamp(200, 100);
    tv = q.find_all();
    tv1 = q1.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv1.size(), 1);
    foo.remove_column(col_date3);
    CHECK_LOGIC_ERROR(tv.sync_if_needed(), LogicError::column_does_not_exist);
    CHECK_LOGIC_ERROR(tv1.sync_if_needed(), LogicError::column_does_not_exist);

    // StringNodeBase
    q = foo.column<String>(col_str4) == StringData("Hello, world");
    q1 = !(foo.column<String>(col_str4) == StringData("Hello, world"));
    tv = q.find_all();
    tv1 = q1.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv1.size(), 4);
    foo.remove_column(col_str4);
    CHECK_LOGIC_ERROR(tv.sync_if_needed(), LogicError::column_does_not_exist);
    CHECK_LOGIC_ERROR(tv1.sync_if_needed(), LogicError::column_does_not_exist);

    // FloatDoubleNode
    q = foo.column<Float>(col_float5) > 0.0f;
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    foo.remove_column(col_float5);
    CHECK_LOGIC_ERROR(tv.sync_if_needed(), LogicError::column_does_not_exist);

    // BinaryNode
    q = foo.column<Binary>(col_bin6) != BinaryData("Binary", 6);
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 4);
    foo.remove_column(col_bin6);
    CHECK_LOGIC_ERROR(tv.sync_if_needed(), LogicError::column_does_not_exist);
}


TEST(Query_ColumnDeletionLinks)
{
    Group g;
    TableRef foo = g.add_table("foo");
    TableRef bar = g.add_table("bar");
    TableRef foobar = g.add_table("foobar");

    auto col_int0 = foobar->add_column(type_Int, "int");

    auto col_int1 = bar->add_column(type_Int, "int");
    auto col_link0 = bar->add_column(*foobar, "link");

    auto col_link1 = foo->add_column(*bar, "link");

    std::vector<ObjKey> foobar_keys;
    std::vector<ObjKey> bar_keys;
    std::vector<ObjKey> foo_keys;
    foobar->create_objects(5, foobar_keys);
    bar->create_objects(5, bar_keys);
    foo->create_objects(10, foo_keys);

    for (int i = 0; i < 5; i++) {
        foobar->get_object(foobar_keys[i]).set(col_int0, i);
        bar->get_object(bar_keys[i]).set(col_int1, i);
        bar->get_object(bar_keys[i]).set(col_link0, foobar_keys[i]);
        foo->get_object(foo_keys[i]).set(col_link1, bar_keys[i]);
    }
    auto q = foo->link(col_link1).link(col_link0).column<Int>(col_int0) == 2;
    auto q1 = foo->column<Link>(col_link1).is_null();
    auto q2 = foo->column<Link>(col_link1) == bar->get_object(bar_keys[2]);

    auto tv = q.find_all();
    auto cnt = q1.count();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(cnt, 5);
    cnt = q2.count();
    CHECK_EQUAL(cnt, 1);

    // remove integer column, should not affect query
    bar->remove_column(col_int1);
    tv.sync_if_needed();
    CHECK_EQUAL(tv.size(), 1);
    // remove link column, disaster
    bar->remove_column(col_link0);
    CHECK_LOGIC_ERROR(bar->report_invalid_key(col_link0), LogicError::column_does_not_exist);
    CHECK_LOGIC_ERROR(tv.sync_if_needed(), LogicError::column_does_not_exist);
    foo->remove_column(col_link1);
    CHECK_LOGIC_ERROR(foo->report_invalid_key(col_link1), LogicError::column_does_not_exist);
    CHECK_LOGIC_ERROR(q1.count(), LogicError::column_does_not_exist);
    CHECK_LOGIC_ERROR(q2.count(), LogicError::column_does_not_exist);
}


TEST(Query_CaseInsensitiveIndexEquality_CommonNumericPrefix)
{
    Table table;
    auto col_ndx = table.add_column(type_String, "id");
    table.add_search_index(col_ndx);

    ObjKey key0 = table.create_object().set(col_ndx, "111111111111111111111111").get_key();
    table.create_object().set(col_ndx, "111111111111111111111112");

    Query q = table.where().equal(col_ndx, "111111111111111111111111", false);
    CHECK_EQUAL(q.count(), 1);
    TableView tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv[0].get_key(), key0);
}


TEST_TYPES(Query_CaseInsensitiveNullable, std::true_type, std::false_type)
{
    Table table;
    bool nullable = true;
    constexpr bool with_index = TEST_TYPE::value;
    auto col_ndx = table.add_column(type_String, "id", nullable);
    if (with_index) {
        table.add_search_index(col_ndx);
    }

    table.create_object().set(col_ndx, "test");
    table.create_object().set(col_ndx, "words");
    ObjKey key2 = table.create_object().get_key();
    ObjKey key3 = table.create_object().get_key();
    table.create_object().set(col_ndx, "");
    table.create_object().set(col_ndx, "");

    bool case_sensitive = true;
    StringData null_string;
    Query q = table.where().equal(col_ndx, null_string, case_sensitive);
    CHECK_EQUAL(q.count(), 2);
    TableView tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv.get_key(0), key2);
    CHECK_EQUAL(tv.get_key(1), key3);
    Query q2 = table.where().contains(col_ndx, null_string, case_sensitive);
    CHECK_EQUAL(q2.count(), 6);
    tv = q2.find_all();
    CHECK_EQUAL(tv.size(), 6);

    case_sensitive = false;
    q = table.where().equal(col_ndx, null_string, case_sensitive);
    CHECK_EQUAL(q.count(), 2);
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv.get_key(0), key2);
    CHECK_EQUAL(tv.get_key(1), key3);
    q2 = table.where().contains(col_ndx, null_string, case_sensitive);
    CHECK_EQUAL(q2.count(), 6);
    tv = q2.find_all();
    CHECK_EQUAL(tv.size(), 6);
}


TEST_TYPES(Query_Rover, std::true_type, std::false_type)
{
    constexpr bool nullable = TEST_TYPE::value;

    Table table;
    auto col = table.add_column(type_String, "name", nullable);
    table.add_search_index(col);

    table.create_object().set(col, "ROVER");
    table.create_object().set(col, "Rover");

    Query q = table.where().equal(col, "rover", false);
    CHECK_EQUAL(q.count(), 2);
    TableView tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
}

TEST(Query_StringPrimaryKey)
{
    Table table;
    auto col = table.add_column(type_String, "name");
    table.set_primary_key_column(col);

    table.create_object_with_primary_key("RASMUS");
    table.create_object_with_primary_key("Rasmus");

    Query q = table.where().equal(col, "rasmus", false);
    CHECK_EQUAL(q.count(), 2);
    TableView tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
}

TEST(Query_IntOnly)
{
    Table table;
    auto c0 = table.add_column(type_Int, "i1");
    auto c1 = table.add_column(type_Int, "i2");

    table.create_object(ObjKey(7)).set_all(7, 6);
    table.create_object(ObjKey(19)).set_all(19, 9);
    table.create_object(ObjKey(5)).set_all(19, 22);
    table.create_object(ObjKey(21)).set_all(2, 6);

    auto q = table.column<Int>(c1) == 6;
    ObjKey key = q.find();
    CHECK_EQUAL(key, ObjKey(7));

    TableView tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv.get(0).get_key(), ObjKey(7));
    CHECK_EQUAL(tv.get(1).get_key(), ObjKey(21));

    auto q1 = table.where(&tv).equal(c0, 2);
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(tv1.size(), 1);
    CHECK_EQUAL(tv1.get(0).get_key(), ObjKey(21));

    q1 = table.where(&tv).greater(c0, 5);
    tv1 = q1.find_all();
    CHECK_EQUAL(tv1.size(), 1);
    CHECK_EQUAL(tv1.get(0).get_key(), ObjKey(7));

    q = table.column<Int>(c0) == 19 && table.column<Int>(c1) == 9;
    key = q.find();
    CHECK_EQUAL(key.value, 19);

    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv.get(0).get_key(), ObjKey(19));

    // Two column expression
    q = table.column<Int>(c0) < table.column<Int>(c1);
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv.get(0).get_key(), ObjKey(5));
    CHECK_EQUAL(tv.get(1).get_key(), ObjKey(21));
}

TEST(Query_LinksTo)
{
    Query q;
    ObjKey found_key;
    Group group;

    TableRef source = group.add_table("source");
    TableRef target = group.add_table("target");

    auto col_link = source->add_column(*target, "link");
    auto col_linklist = source->add_column_list(*target, "linklist");

    std::vector<ObjKey> target_keys;
    target->create_objects(10, target_keys);

    std::vector<ObjKey> source_keys;
    source->create_objects(10, source_keys);

    source->get_object(source_keys[2]).set(col_link, target_keys[2]);
    source->get_object(source_keys[5]).set(col_link, target_keys[5]);
    source->get_object(source_keys[8]).set(col_link, target_keys[5]);
    source->get_object(source_keys[9]).set(col_link, target_keys[9]);

    q = source->column<Link>(col_link) == target->get_object(target_keys[2]);
    found_key = q.find();
    CHECK_EQUAL(found_key, source_keys[2]);

    q = source->where().equal(col_link, Mixed(target_keys[2]));
    found_key = q.find();
    CHECK_EQUAL(found_key, source_keys[2]);

    q = source->column<Link>(col_link) == target->get_object(target_keys[5]);
    found_key = q.find();
    CHECK_EQUAL(found_key, source_keys[5]);
    q = source->where().equal(col_link, Mixed(target_keys[5]));
    auto tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    q = source->where().not_equal(col_link, Mixed(target_keys[5]));
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 8);
    q = source->where().equal(col_link, Mixed(ObjLink(source->get_key(), target_keys[5]))); // Wrong table
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 0);

    q = source->column<Link>(col_link) == target->get_object(target_keys[9]);
    found_key = q.find();
    CHECK_EQUAL(found_key, source_keys[9]);

    q = source->column<Link>(col_link) == target->get_object(target_keys[0]);
    found_key = q.find();
    CHECK_EQUAL(found_key, null_key);

    q = source->column<Link>(col_link).is_null();
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 6);
    q = source->where().equal(col_link, Mixed()); // Null
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 6);

    q = source->column<Link>(col_link) != null();
    found_key = q.find();
    CHECK_EQUAL(found_key, source_keys[2]);
    q = source->where().not_equal(col_link, Mixed()); // Null
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 4);

    auto linklist = source->get_object(source_keys[1]).get_linklist_ptr(col_linklist);
    linklist->add(target_keys[6]);
    linklist = source->get_object(source_keys[2]).get_linklist_ptr(col_linklist);
    linklist->add(target_keys[0]);
    linklist->add(target_keys[1]);
    linklist->add(target_keys[2]);
    linklist = source->get_object(source_keys[8]).get_linklist_ptr(col_linklist);
    linklist->add(target_keys[0]);
    linklist->add(target_keys[5]);
    linklist->add(target_keys[7]);

    q = source->column<Link>(col_linklist) == target->get_object(target_keys[5]);
    found_key = q.find();
    CHECK_EQUAL(found_key, source_keys[8]);

    q = source->column<Link>(col_linklist) != target->get_object(target_keys[6]);
    found_key = q.find();
    CHECK_EQUAL(found_key, source_keys[2]);

    q = source->where().equal(col_linklist, Mixed(target_keys[0]));
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);
    q = source->where().not_equal(col_linklist, Mixed(target_keys[6]));
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 2);

    q = source->where().equal(col_linklist, Mixed());
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 0); // LinkList never matches null
    q = source->where().not_equal(col_linklist, Mixed());
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 3);
}

TEST(Query_Group_bug)
{
    // Tests for a bug in queries with OR nodes at different nesting levels

    Group g;
    TableRef service_table = g.add_table("service");
    TableRef profile_table = g.add_table("profile");
    TableRef person_table = g.add_table("person");

    auto col_service_id = service_table->add_column(type_String, "id");
    auto col_service_link = service_table->add_column_list(*profile_table, "profiles");

    auto col_profile_id = profile_table->add_column(type_String, "role");
    auto col_profile_link = profile_table->add_column(*service_table, "services");

    auto col_person_id = person_table->add_column(type_String, "id");
    auto col_person_link = person_table->add_column_list(*service_table, "services");

    auto sk0 = service_table->create_object().set(col_service_id, "service_1").get_key();
    auto sk1 = service_table->create_object().set(col_service_id, "service_2").get_key();

    auto pk0 = profile_table->create_object().set(col_profile_id, "profile_1").get_key();
    auto pk1 = profile_table->create_object().set(col_profile_id, "profile_2").get_key();
    auto pk2 = profile_table->create_object().set(col_profile_id, "profile_3").get_key();
    auto pk3 = profile_table->create_object().set(col_profile_id, "profile_4").get_key();
    auto pk4 = profile_table->create_object().set(col_profile_id, "profile_5").get_key();

    {
        auto ll0 = service_table->get_object(sk0).get_linklist(col_service_link);
        auto ll1 = service_table->get_object(sk1).get_linklist(col_service_link);
        ll0.add(pk0);
        ll0.add(pk1);
        ll1.add(pk2);
        ll0.add(pk3);
        ll0.add(pk4);
    }

    profile_table->get_object(pk0).set(col_profile_link, sk0);
    profile_table->get_object(pk1).set(col_profile_link, sk0);
    profile_table->get_object(pk2).set(col_profile_link, sk1);
    profile_table->get_object(pk3).set(col_profile_link, sk0);
    profile_table->get_object(pk4).set(col_profile_link, sk0);

    person_table->create_object().set(col_person_id, "person_1").get_linklist(col_person_link).add(sk0);
    person_table->create_object().set(col_person_id, "person_2").get_linklist(col_person_link).add(sk0);
    person_table->create_object().set(col_person_id, "person_3").get_linklist(col_person_link).add(sk1);
    person_table->create_object().set(col_person_id, "person_4").get_linklist(col_person_link).add(sk0);
    person_table->create_object().set(col_person_id, "person_5").get_linklist(col_person_link).add(sk0);

    realm::Query q0 =
        person_table->where()
            .group()

            .group()
            .and_query(person_table->link(col_person_link)
                           .link(col_service_link)
                           .column<String>(col_profile_id)
                           .equal("profile_1"))
            .Or()
            .and_query(person_table->link(col_person_link)
                           .link(col_service_link)
                           .column<String>(col_profile_id)
                           .equal("profile_2"))
            .end_group()

            .group()
            .and_query(person_table->link(col_person_link).column<String>(col_service_id).equal("service_1"))
            .end_group()

            .end_group()

            .Or()

            .group()
            .equal(col_person_id, "person_3")
            .end_group();

    CHECK_EQUAL(5, q0.count());
}

TEST(Query_TwoColumnUnaligned)
{
    Group g;
    TableRef table = g.add_table("table");
    ColKey a_col_ndx = table->add_column(type_Int, "a");
    ColKey b_col_ndx = table->add_column(type_Int, "b");

    // Adding 1001 rows causes arrays in the 2 columns to be aligned differently
    // (on a 0 and on an 8 address resp)
    auto matches = 0;
    for (int i = 0; i < 1001; ++i) {
        Obj obj = table->create_object();
        obj.set(a_col_ndx, i);
        if (i % 88) {
            obj.set(b_col_ndx, i + 5);
        }
        else {
            obj.set(b_col_ndx, i);
            matches++;
        }
    }

    Query q = table->column<Int>(a_col_ndx) == table->column<Int>(b_col_ndx);
    size_t cnt = q.count();
    CHECK_EQUAL(cnt, matches);
}


TEST(Query_IntOrQueryOptimisation)
{
    Group g;
    TableRef table = g.add_table("table");
    auto col_optype = table->add_column(type_String, "optype");
    auto col_active = table->add_column(type_Bool, "active");
    auto col_id = table->add_column(type_Int, "id");

    for (int i = 0; i < 100; i++) {
        auto obj = table->create_object();
        obj.set<bool>(col_active, (i % 10) != 0);
        obj.set<int>(col_id, i);
        if (i == 0) obj.set(col_optype, "CREATE");
        if (i == 1) obj.set(col_optype, "DELETE");
        if (i == 2) obj.set(col_optype, "CREATE");
    }
    auto optype = table->column<String>(col_optype);
    auto active = table->column<Bool>(col_active);
    auto id = table->column<Int>(col_id);

    Query q;
    q = (id == 0 && optype == "CREATE") || id == 1;
    CHECK_EQUAL(q.count(), 2);

    q = id == 1 || (id == 0 && optype == "DELETE");
    CHECK_EQUAL(q.count(), 1);

    q = table->where().equal(col_id, 1).Or().equal(col_id, 0).Or().equal(col_id, 2);
    CHECK_EQUAL(q.count(), 3);
}

TEST_IF(Query_IntOrQueryPerformance, TEST_DURATION > 0)
{
    using std::chrono::duration_cast;
    using std::chrono::microseconds;

    Group g;
    TableRef table = g.add_table("table");
    auto ints_col_key = table->add_column(type_Int, "ints");
    auto nullable_ints_col_key = table->add_column(type_Int, "nullable_ints", true);

    const int null_frequency = 1000;
    int num_nulls_added = 0;
    int limit = 100000;
    for (int i = 0; i < limit; ++i) {
        if (i % null_frequency == 0) {
            auto o = table->create_object().set_all(i);
            o.set_null(nullable_ints_col_key);
            ++num_nulls_added;
        }
        else {
            auto o = table->create_object().set_all(i, i);
        }
    }

    auto run_queries = [&](int num_matches) {
        // std::cout << "num_matches: " << num_matches << std::endl;
        Query q_ints = table->column<Int>(ints_col_key) == -1;
        Query q_nullables =
            (table->column<Int>(nullable_ints_col_key) == -1).Or().equal(nullable_ints_col_key, realm::null());
        for (int i = 0; i < num_matches; ++i) {
            q_ints = q_ints.Or().equal(ints_col_key, i);
            q_nullables = q_nullables.Or().equal(nullable_ints_col_key, i);
        }

        auto before = std::chrono::steady_clock().now();
        size_t ints_count = q_ints.count();
        auto after = std::chrono::steady_clock().now();
        // std::cout << "ints count: " << duration_cast<microseconds>(after - before).count() << " us" << std::endl;

        before = std::chrono::steady_clock().now();
        size_t nullable_ints_count = q_nullables.count();
        after = std::chrono::steady_clock().now();
        // std::cout << "nullable ints count: " << duration_cast<microseconds>(after - before).count() << " us"
        //           << std::endl;

        size_t expected_nullable_query_count =
            num_matches + num_nulls_added - (((num_matches - 1) / null_frequency) + 1);
        CHECK_EQUAL(ints_count, num_matches);
        CHECK_EQUAL(nullable_ints_count, expected_nullable_query_count);
    };

    run_queries(2);
    run_queries(2048);

    table->add_search_index(ints_col_key);
    table->add_search_index(nullable_ints_col_key);

    run_queries(2);
    run_queries(2048);
}

TEST(Query_IntIndexed)
{
    Group g;
    TableRef table = g.add_table("table");
    auto col_id = table->add_column(type_Int, "id");

    for (int i = 0; i < 100; i++) {
        table->create_object().set_all(i % 10);
    }

    table->add_search_index(col_id);
    Query q = table->where().equal(col_id, 1);
    CHECK_EQUAL(q.count(), 10);
    auto tv = q.find_all();
    CHECK_EQUAL(tv.size(), 10);
}

TEST(Query_IntIndexedRandom)
{
    Random random(random_int<int>());

    Group g;
    TableRef table = g.add_table("table");
    auto col_id = table->add_column(type_Int, "id");
    auto col_val = table->add_column(type_Int, "val");

    for (int i = 0; i < 100000; i++) {
        table->create_object().set(col_id, random.draw_int_max(20)).set(col_val, random.draw_int_max(100));
    }

    for (const char* str : {"id == 1", "id == 1 and val > 50"}) {
        table->remove_search_index(col_id);
        Query q = table->query(str);
        auto before = std::chrono::steady_clock().now();
        size_t c1 = q.count();
        auto after = std::chrono::steady_clock().now();
        auto count_without_index = std::chrono::duration_cast<std::chrono::microseconds>(after - before).count();
        before = std::chrono::steady_clock().now();
        auto tv1 = q.find_all();
        after = std::chrono::steady_clock().now();
        auto find_all_without_index = std::chrono::duration_cast<std::chrono::microseconds>(after - before).count();

        table->add_search_index(col_id);
        before = std::chrono::steady_clock().now();
        size_t c2 = q.count();
        after = std::chrono::steady_clock().now();
        auto count_with_index = std::chrono::duration_cast<std::chrono::microseconds>(after - before).count();
        CHECK_EQUAL(c1, c2);
        before = std::chrono::steady_clock().now();
        auto tv2 = q.find_all();
        after = std::chrono::steady_clock().now();
        auto find_all_with_index = std::chrono::duration_cast<std::chrono::microseconds>(after - before).count();
        CHECK_EQUAL(tv1.size(), tv2.size());
        CHECK_EQUAL(tv1.size(), c1);

        /*
        std::cout << "Query: " << str << std::endl;
        std::cout << "count without index: " << count_without_index << " us" << std::endl;
        std::cout << "find all without index: " << find_all_without_index << " us" << std::endl;
        std::cout << "count with index: " << count_with_index << " us" << std::endl;
        std::cout << "find all with index: " << find_all_with_index << " us" << std::endl;
         */
        static_cast<void>(count_without_index);
        static_cast<void>(find_all_without_index);
        static_cast<void>(count_with_index);
        static_cast<void>(find_all_with_index);
    }
}

TEST(Query_IntFindInNextLeaf)
{
    Group g;
    TableRef table = g.add_table("table");
    auto col_id = table->add_column(type_Int, "id");

    // num_misses > MAX_BPNODE_SIZE to check results on other leafs
    constexpr int num_misses = 1000 * 2 + 10;
    for (int i = 0; i < num_misses; i++) {
        table->create_object().set(col_id, i % 10);
    }
    table->create_object().set(col_id, 20);

    auto check_results = [&]() {
        for (int i = 0; i < 10; ++i) {
            Query qi = table->where().equal(col_id, i);
            CHECK_EQUAL(qi.count(), num_misses / 10);
        }
        Query q20 = table->where().equal(col_id, 20);
        CHECK_EQUAL(q20.count(), 1);
    };
    check_results();
    table->add_search_index(col_id);
    check_results();
}

TEST(Query_IntIndexOverLinkViewNotInTableOrder)
{
    Group g;

    TableRef child_table = g.add_table("child");
    auto col_child_id = child_table->add_column(type_Int, "id");
    child_table->add_search_index(col_child_id);

    auto k0 = child_table->create_object().set(col_child_id, 3).get_key();
    auto k1 = child_table->create_object().set(col_child_id, 2).get_key();

    TableRef parent_table = g.add_table("parent");
    auto col_parent_children = parent_table->add_column_list(*child_table, "children");

    auto parent_obj = parent_table->create_object();
    auto children = parent_obj.get_linklist(col_parent_children);
    // Add in reverse order so that the query node sees declining start indices
    children.add(k1);
    children.add(k0);

    // Query via linkview
    Query q = child_table->where(children).equal(col_child_id, 3);
    // Call find() twice. This caused a memory lead at some point. Must pass a memory leak test.
    CHECK_EQUAL(k0, q.find());
    CHECK_EQUAL(k0, q.find());
    CHECK_EQUAL(k1, child_table->where(children).equal(col_child_id, 2).find());

    // Query directly
    CHECK_EQUAL(k0, child_table->where().equal(col_child_id, 3).find());
    CHECK_EQUAL(k1, child_table->where().equal(col_child_id, 2).find());
}

TEST(Query_MixedTypeQuery)
{
    Group g;
    auto table = g.add_table("Foo");
    auto col_int = table->add_column(type_Int, "int");
    auto col_double = table->add_column(type_Double, "double");
    for (int64_t i = 0; i < 100; i++) {
        table->create_object().set(col_int, i).set(col_double, 100. - i);
    }

    auto tv = (table->column<Int>(col_int) > 9.5).find_all();
    CHECK_EQUAL(tv.size(), 90);
    auto tv1 = (table->column<Int>(col_int) > table->column<Double>(col_double)).find_all();
    CHECK_EQUAL(tv1.size(), 49);
}

TEST(Query_LinkListIntPastOneIsNull)
{
    Group g;
    auto table_foo = g.add_table("Foo");
    auto table_bar = g.add_table("Bar");
    auto col_int = table_foo->add_column(type_Int, "int", true);
    auto col_list = table_bar->add_column_list(*table_foo, "foo_link");
    std::vector<util::Optional<int64_t>> values = {{0}, {1}, {2}, {util::none}};
    auto bar_obj = table_bar->create_object();
    auto list = bar_obj.get_linklist(col_list);

    for (size_t i = 0; i < values.size(); i++) {
        auto obj = table_foo->create_object();
        obj.set(col_int, values[i]);
        list.add(obj.get_key());
    }

    Query q = table_bar->link(col_list).column<Int>(col_int) == realm::null();

    CHECK_EQUAL(q.count(), 1);
}

TEST(Query_LinkView_StrIndex)
{
    Group g;
    auto table_foo = g.add_table_with_primary_key("class_Foo", type_String, "id");
    auto col_id = table_foo->get_column_key("id");

    auto table_bar = g.add_table("class_Bar");
    auto col_list = table_bar->add_column_list(*table_foo, "link");

    auto foo = table_foo->create_object_with_primary_key("97625fdc-d3f8-4c45-9a4d-dc8c83c657a1");
    auto bar = table_bar->create_object();
    auto ll = bar.get_linklist(col_list);
    ll.add(foo.get_key());

    auto q = table_foo->where(ll).equal(col_id, "97625fdc-d3f8-4c45-9a4d-dc8c83c657a1");
    CHECK_EQUAL(q.count(), 1);
}

TEST(Query_StringOrShortStrings)
{
    Group g;
    TableRef table = g.add_table("table");
    auto col_value = table->add_column(type_String, "value");

    std::string strings[] = {"0", "1", "2"};
    for (auto& str : strings) {
        table->create_object().set(col_value, str);
    }

    for (auto& str : strings) {
        Query q = table->where()
                      .group()
                      .equal(col_value, StringData(str))
                      .Or()
                      .equal(col_value, StringData("not present"))
                      .end_group();
        CHECK_EQUAL(q.count(), 1);
    }
}

TEST(Query_StringOrMediumStrings)
{
    Group g;
    TableRef table = g.add_table("table");
    auto col_value = table->add_column(type_String, "value");

    std::string strings[] = {"0", "1", "2"};
    for (auto& str : strings) {
        str.resize(16, str[0]); // Make the strings long enough to require ArrayStringLong
        table->create_object().set(col_value, str);
    }

    for (auto& str : strings) {
        Query q = table->where()
                      .group()
                      .equal(col_value, StringData(str))
                      .Or()
                      .equal(col_value, StringData("not present"))
                      .end_group();
        CHECK_EQUAL(q.count(), 1);
    }
}

TEST(Query_StringOrLongStrings)
{
    Group g;
    TableRef table = g.add_table("table");
    auto col_value = table->add_column(type_String, "value");

    std::string strings[] = {"0", "1", "2"};
    for (auto& str : strings) {
        str.resize(64, str[0]); // Make the strings long enough to require ArrayBigBlobs
        table->create_object().set(col_value, str);
    }

    for (auto& str : strings) {
        Query q = table->where()
                      .group()
                      .equal(col_value, StringData(str))
                      .Or()
                      .equal(col_value, StringData("not present"))
                      .end_group();
        CHECK_EQUAL(q.count(), 1);
    }
}

TEST(Query_LinkViewAnd)
{
    Group g;

    TableRef child_table = g.add_table("child");
    auto col_child_id = child_table->add_column(type_Int, "id");
    auto col_child_name = child_table->add_column(type_String, "name");

    auto k0 = child_table->create_object().set(col_child_id, 3).set(col_child_name, "Adam").get_key();
    auto k1 = child_table->create_object().set(col_child_id, 2).set(col_child_name, "Jeff").get_key();

    TableRef parent_table = g.add_table("parent");
    auto col_parent_children = parent_table->add_column_list(*child_table, "children");

    auto parent_obj = parent_table->create_object();
    auto children = parent_obj.get_linklist(col_parent_children);
    children.add(k0);
    children.add(k1);

    Query q1 = child_table->where(children).equal(col_child_id, 3);
    Query q2 = child_table->where(children).equal(col_child_name, "Jeff");
    CHECK_EQUAL(k0, q1.find());
    CHECK_EQUAL(k1, q2.find());
    q1.and_query(q2);
    CHECK_NOT(q1.find());
}

TEST(Query_LinksWithIndex)
{
    Group g;

    TableRef target = g.add_table("target");
    auto col_value = target->add_column(type_String, "value");
    auto col_date = target->add_column(type_Timestamp, "date");
    target->add_search_index(col_value);
    target->add_search_index(col_date);

    TableRef foo = g.add_table("foo");
    auto col_foo = foo->add_column_list(*target, "linklist");
    auto col_location = foo->add_column(type_String, "location");
    auto col_score = foo->add_column(type_Int, "score");
    foo->add_search_index(col_location);
    foo->add_search_index(col_score);

    TableRef middle = g.add_table("middle");
    auto col_link = middle->add_column(*target, "link");

    TableRef origin = g.add_table("origin");
    auto col_linklist = origin->add_column_list(*middle, "linklist");

    std::vector<StringData> strings{"Copenhagen", "Aarhus", "Odense", "Aalborg", "Faaborg"};
    auto now = std::chrono::system_clock::now();
    std::chrono::seconds d{0};
    for (auto& str : strings) {
        target->create_object().set(col_value, str).set(col_date, Timestamp(now + d));
        d = d + std::chrono::seconds{1};
    }

    auto m0 = middle->create_object().set(col_link, target->find_first(col_value, strings[0])).get_key();
    auto m1 = middle->create_object().set(col_link, target->find_first(col_value, strings[2])).get_key();
    auto m2 = middle->create_object().set(col_link, target->find_first(col_value, strings[2])).get_key();
    auto m3 = middle->create_object().set(col_link, target->find_first(col_value, strings[2])).get_key();
    auto m4 = middle->create_object().set(col_link, target->find_first(col_value, strings[3])).get_key();

    auto obj0 = origin->create_object();
    obj0.get_linklist(col_linklist).add(m3);

    auto obj1 = origin->create_object();
    auto ll1 = obj1.get_linklist(col_linklist);
    ll1.add(m1);
    ll1.add(m2);

    origin->create_object().get_linklist(col_linklist).add(m4);
    origin->create_object().get_linklist(col_linklist).add(m3);
    auto obj4 = origin->create_object();
    obj4.get_linklist(col_linklist).add(m0);

    Query q = origin->link(col_linklist).link(col_link).column<String>(col_value) == "Odense";
    CHECK_EQUAL(q.find(), obj0.get_key());
    auto tv = q.find_all();
    CHECK_EQUAL(tv.size(), 3);

    auto ll = foo->create_object().set(col_location, "Fyn").set(col_score, 5).get_linklist(col_foo);
    ll.add(target->find_first(col_value, strings[2]));
    ll.add(target->find_first(col_value, strings[4]));

    Query q1 =
        origin->link(col_linklist).link(col_link).backlink(*foo, col_foo).column<String>(col_location) == "Fyn";
    CHECK_EQUAL(q1.find(),  obj0.get_key());
    Query q2 = origin->link(col_linklist).link(col_link).backlink(*foo, col_foo).column<Int>(col_score) == 5;
    CHECK_EQUAL(q2.find(),  obj0.get_key());

    // Make sure that changes in the table are reflected in the query result
    middle->get_object(m3).set(col_link, target->find_first(col_value, strings[1]));
    CHECK_EQUAL(q.find(), obj1.get_key());

    q = origin->link(col_linklist).link(col_link).column<Timestamp>(col_date) == Timestamp(now);
    CHECK_EQUAL(q.find(), obj4.get_key());
}

TEST(Query_NotImmediatelyBeforeKnownRange)
{
    Group g;
    TableRef parent = g.add_table("parent");
    TableRef child = g.add_table("child");
    auto col_link = parent->add_column_list(*child, "list");
    auto col_str = child->add_column(type_String, "value");
    child->add_search_index(col_str);

    Obj obj = parent->create_object();
    auto k0 = child->create_object().set(col_str, "a").get_key();
    auto k1 = child->create_object().set(col_str, "b").get_key();
    auto list = obj.get_linklist(col_link);
    list.insert(0, k0);
    list.insert(0, k1);

    Query q = child->where(list).Not().equal(col_str, "a");
    CHECK_EQUAL(q.count(), 1);
}

TEST_TYPES(Query_PrimaryKeySearchForNull, Prop<String>, Prop<Int>, Prop<ObjectId>, Nullable<String>, Nullable<Int>,
           Nullable<ObjectId>)
{
    using type = typename TEST_TYPE::type;
    using underlying_type = typename TEST_TYPE::underlying_type;
    Table table;
    TestValueGenerator gen;
    auto col = table.add_column(TEST_TYPE::data_type, "property", TEST_TYPE::is_nullable);
    table.set_primary_key_column(col);
    underlying_type v0 = gen.convert_for_test<underlying_type>(42);
    underlying_type v1 = gen.convert_for_test<underlying_type>(43);
    Mixed mixed_null;
    auto obj0 = table.create_object_with_primary_key(v0);
    auto obj1 = table.create_object_with_primary_key(v1);

    auto verify_result_count = [&](Query& q, size_t expected_count) {
        CHECK_EQUAL(q.count(), expected_count);
        TableView tv = q.find_all();
        CHECK_EQUAL(tv.size(), expected_count);
    };
    Query q = table.where().equal(col, v0);
    verify_result_count(q, 1);
    q = table.where().equal(col, v1);
    verify_result_count(q, 1);

    CHECK_EQUAL(table.find_first(col, v0), obj0.get_key());
    CHECK_EQUAL(table.find_first(col, v1), obj1.get_key());
    CHECK_NOT(table.find_first(col, type{}));
}

TEST_TYPES(Query_Mixed, std::true_type, std::false_type)
{
    bool has_index = TEST_TYPE::value;
    Group g;
    auto table = g.add_table("Foo");
    auto origin = g.add_table("Origin");
    auto col_any = table->add_column(type_Mixed, "any");
    auto col_int = table->add_column(type_Int, "int");
    auto col_link = origin->add_column(*table, "link");
    auto col_mixed = origin->add_column(type_Mixed, "mixed");
    auto col_links = origin->add_column_list(*table, "links");

    if (has_index)
        table->add_search_index(col_any);

    size_t int_over_50 = 0;
    size_t nb_strings = 0;
    for (int64_t i = 0; i < 100; i++) {
        if (i % 4) {
            if (i > 50)
                int_over_50++;
            table->create_object().set(col_any, Mixed(i)).set(col_int, i);
        }
        else {
            std::string str = "String" + util::to_string(i);
            table->create_object().set(col_any, Mixed(str)).set(col_int, i);
            nb_strings++;
        }
    }
    std::string str2bin("String2Binary");
    table->get_object(15).set(col_any, Mixed());
    table->get_object(75).set(col_any, Mixed(75.));
    table->get_object(28).set(col_any, Mixed(BinaryData(str2bin)));
    table->get_object(25).set(col_any, Mixed(3.));
    table->get_object(35).set(col_any, Mixed(Decimal128("3")));
    table->get_object(80).set(col_any, Mixed("abcdefgh"));
    table->get_object(81).set(col_any, Mixed(int64_t(0x6867666564636261)));

    auto it = table->begin();
    for (int64_t i = 0; i < 10; i++) {
        auto obj = origin->create_object();
        auto ll = obj.get_linklist(col_links);

        obj.set(col_link, it->get_key());
        if (i % 3) {
            obj.set(col_mixed, Mixed(i));
        }
        else {
            obj.set(col_mixed, Mixed(table->begin()->get_link()));
        }
        for (int64_t j = 0; j < 10; j++) {
            ll.add(it->get_key());
            ++it;
        }
    }

    // g.to_json(std::cout);
    auto tv = (table->column<Mixed>(col_any) > Mixed(50)).find_all();
    CHECK_EQUAL(tv.size(), int_over_50);
    tv = (table->column<Mixed>(col_any) > 50).find_all();
    CHECK_EQUAL(tv.size(), int_over_50);
    tv = (table->column<Mixed>(col_any) == 37).find_all();
    CHECK_EQUAL(tv.size(), 1);
    tv = table->where().equal(col_any, Mixed(37)).find_all();
    CHECK_EQUAL(tv.size(), 1);
    tv = (table->column<Mixed>(col_any) >= 50).find_all();
    CHECK_EQUAL(tv.size(), int_over_50 + 1);
    tv = (table->column<Mixed>(col_any) <= 50).find_all();
    CHECK_EQUAL(tv.size(), 100 - int_over_50 - nb_strings - 1);
    tv = (table->column<Mixed>(col_any) < 50).find_all();
    CHECK_EQUAL(tv.size(), 100 - int_over_50 - nb_strings - 2);
    tv = (table->column<Mixed>(col_any) < 50 || table->column<Mixed>(col_any) > 50).find_all();
    CHECK_EQUAL(tv.size(), 100 - nb_strings - 2);
    tv = (table->column<Mixed>(col_any) != 50).find_all();
    CHECK_EQUAL(tv.size(), 99);

    tv = table->where().greater(col_any, Mixed(50)).find_all();
    CHECK_EQUAL(tv.size(), int_over_50);
    tv = table->where().greater(col_any, 50).find_all();
    CHECK_EQUAL(tv.size(), int_over_50);

    tv = table->where().equal(col_any, null()).find_all();
    CHECK_EQUAL(tv.size(), 1);
    tv = table->where().not_equal(col_any, null()).find_all();
    CHECK_EQUAL(tv.size(), 99);

    tv = table->where().begins_with(col_any, StringData("String2")).find_all(); // 20, 24, 28
    CHECK_EQUAL(tv.size(), 3);
    tv = table->where().begins_with(col_any, BinaryData("String2", 7)).find_all(); // 20, 24, 28
    CHECK_EQUAL(tv.size(), 3);

    tv = table->where().contains(col_any, StringData("TRIN"), false).find_all();
    CHECK_EQUAL(tv.size(), 24);
    tv = table->where().contains(col_any, Mixed("TRIN"), false).find_all();
    CHECK_EQUAL(tv.size(), 24);

    tv = table->where().like(col_any, StringData("Strin*")).find_all();
    CHECK_EQUAL(tv.size(), 24);

    tv = table->where().ends_with(col_any, StringData("4")).find_all(); // 4, 24, 44, 64, 84
    CHECK_EQUAL(tv.size(), 5);
    char bin[1] = {0x34};
    tv = table->where().ends_with(col_any, BinaryData(bin)).find_all(); // 4, 24, 44, 64, 84
    CHECK_EQUAL(tv.size(), 5);

    tv = table->where().equal(col_any, "String2Binary", true).find_all();
    CHECK_EQUAL(tv.size(), 1);

    tv = table->where().equal(col_any, "string2binary", false).find_all();
    CHECK_EQUAL(tv.size(), 1);

    tv = table->where().not_equal(col_any, "string2binary", false).find_all();
    CHECK_EQUAL(tv.size(), 99);

    tv = (table->column<Mixed>(col_any) == StringData("String48")).find_all();
    CHECK_EQUAL(tv.size(), 1);
    tv = (table->column<Mixed>(col_any) == 3.).find_all();
    CHECK_EQUAL(tv.size(), 3);
    tv = (table->column<Mixed>(col_any) == table->column<Int>(col_int)).find_all();
    CHECK_EQUAL(tv.size(), 71);

    tv = (table->column<Mixed>(col_any) == StringData("abcdefgh")).find_all();
    CHECK_EQUAL(tv.size(), 1);

    ObjLink link_to_first = table->begin()->get_link();
    tv = (origin->column<Mixed>(col_mixed) == Mixed(link_to_first)).find_all();
    CHECK_EQUAL(tv.size(), 4);
    tv = (origin->where().links_to(col_mixed, link_to_first)).find_all();
    CHECK_EQUAL(tv.size(), 4);
    tv = (origin->where().equal(col_link, Mixed(link_to_first))).find_all();
    CHECK_EQUAL(tv.size(), 1);
    tv = (origin->where().equal(col_links, Mixed(link_to_first))).find_all();
    CHECK_EQUAL(tv.size(), 1);
    auto q = origin->where().not_equal(col_links, Mixed(link_to_first));
    auto d = q.get_description();
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 10);
    q = origin->query(d);
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 10);
    tv = (origin->link(col_links).column<Mixed>(col_any) > 50).find_all();
    CHECK_EQUAL(tv.size(), 5);
    tv = (origin->link(col_link).column<Mixed>(col_any) > 50).find_all();
    CHECK_EQUAL(tv.size(), 2);
    tv = (origin->link(col_links).column<Mixed>(col_any).contains("string2bin", false)).find_all();
    CHECK_EQUAL(tv.size(), 1);
    tv = (origin->link(col_links).column<Mixed>(col_any).like("*ring*", false)).find_all();
    CHECK_EQUAL(tv.size(), 10);
    tv = (origin->link(col_links).column<Mixed>(col_any).begins_with("String", true)).find_all();
    CHECK_EQUAL(tv.size(), 10);
    tv = (origin->link(col_links).column<Mixed>(col_any).ends_with("g40", true)).find_all();
    CHECK_EQUAL(tv.size(), 1);
}

TEST(Query_ListOfMixed)
{
    Group g;
    auto table = g.add_table("Foo");
    auto origin = g.add_table("Origin");
    auto col_any = table->add_column_list(type_Mixed, "any");
    auto col_int = origin->add_column(type_Int, "int");
    auto col_link = origin->add_column(*table, "link");
    auto col_links = origin->add_column_list(*table, "links");
    size_t expected = 0;

    for (int64_t i = 0; i < 100; i++) {
        auto obj = table->create_object();
        auto list = obj.get_list<Mixed>(col_any);
        if (i % 4) {
            list.add(i);
            if (i > 50)
                expected++;
        }
        else if ((i % 10) == 0) {
            list.add(100.);
            expected++;
        }
        if (i % 3) {
            std::string str = "String" + util::to_string(i);
            list.add(str);
        }
    }
    auto it = table->begin();
    for (int64_t i = 0; i < 10; i++) {
        auto obj = origin->create_object();
        obj.set(col_int, 100);
        auto ll = obj.get_linklist(col_links);

        obj.set(col_link, it->get_key());
        for (int64_t j = 0; j < 10; j++) {
            ll.add(it->get_key());
            ++it;
        }
    }

    // g.to_json(std::cout, 2);
    auto tv = (table->column<Lst<Mixed>>(col_any) > 50).find_all();
    CHECK_EQUAL(tv.size(), expected);
    tv = (origin->link(col_links).column<Lst<Mixed>>(col_any) > 50).find_all();
    CHECK_EQUAL(tv.size(), 8);
    tv = (origin->link(col_link).column<Lst<Mixed>>(col_any) > 50).find_all();
    CHECK_EQUAL(tv.size(), 7);
    tv = (origin->link(col_links).column<Lst<Mixed>>(col_any) == origin->column<Int>(col_int)).find_all();
    CHECK_EQUAL(tv.size(), 5);
}

TEST(Query_Dictionary)
{
    Group g;
    auto foo = g.add_table("foo");
    auto origin = g.add_table("origin");
    auto col_dict = foo->add_column_dictionary(type_Mixed, "dict");
    auto col_link = origin->add_column(*foo, "link");
    auto col_links = origin->add_column_list(*foo, "links");
    size_t expected = 0;

    for (int64_t i = 0; i < 100; i++) {
        auto obj = foo->create_object();
        Dictionary dict = obj.get_dictionary(col_dict);
        bool incr = false;
        if (i % 4) {
            dict.insert("Value", i);
            if (i > 50)
                incr = true;
        }
        else if ((i % 10) == 0) {
            dict.insert("Foo", "Bar");
            dict.insert("Value", 100.);
            incr = true;
        }
        if (i % 3) {
            std::string str = "String" + util::to_string(i);
            dict.insert("Value", str);
            incr = false;
        }
        dict.insert("Dummy", i);
        if (incr) {
            expected++;
        }
    }

    auto it = foo->begin();
    for (int64_t i = 0; i < 10; i++) {
        auto obj = origin->create_object();

        obj.set(col_link, it->get_key());

        auto ll = obj.get_linklist(col_links);
        for (int64_t j = 0; j < 10; j++) {
            ll.add(it->get_key());
            ++it;
        }
    }

    // g.to_json(std::cout);
    auto tv = (foo->column<Dictionary>(col_dict).key("Value") > Mixed(50)).find_all();
    CHECK_EQUAL(tv.size(), expected);
    tv = (foo->column<Dictionary>(col_dict) > 50).find_all(); // Any key will do
    CHECK_EQUAL(tv.size(), 50);                               // 0 and 51..99

    tv = (origin->link(col_link).column<Dictionary>(col_dict).key("Value") > 50).find_all();
    CHECK_EQUAL(tv.size(), 3);
    tv = (origin->link(col_links).column<Dictionary>(col_dict).key("Value") > 50).find_all();
    CHECK_EQUAL(tv.size(), 6);
    tv = (origin->link(col_links).column<Dictionary>(col_dict) > 50).find_all();
    CHECK_EQUAL(tv.size(), 6);
    tv = (origin->link(col_links).column<Dictionary>(col_dict).key("Value") == null()).find_all();
    CHECK_EQUAL(tv.size(), 7);

    tv = (foo->column<Dictionary>(col_dict).keys().begins_with("F")).find_all();
    CHECK_EQUAL(tv.size(), 5);
    tv = (origin->link(col_link).column<Dictionary>(col_dict).keys() == "Foo").find_all();
    CHECK_EQUAL(tv.size(), 5);
}

TEST(Query_DictionaryTypedLinks)
{
    Group g;
    auto dog = g.add_table("dog");
    auto cat = g.add_table("cat");
    auto person = g.add_table("person");
    auto col_data = person->add_column_dictionary(type_Mixed, "data");
    auto col_dog_name = dog->add_column(type_String, "Name");
    auto col_dog_parent = dog->add_column(*dog, "Parent");
    auto col_cat_name = cat->add_column(type_String, "Name");

    auto fido = dog->create_object().set(col_dog_name, "Fido");
    auto pluto = dog->create_object().set(col_dog_name, "Pluto");
    pluto.set(col_dog_parent, fido.get_key());
    auto vaks = dog->create_object().set(col_dog_name, "Vaks");
    auto marie = cat->create_object().set(col_cat_name, "Marie");
    auto berlioz = cat->create_object().set(col_cat_name, "Berlioz");
    auto toulouse = cat->create_object().set(col_cat_name, "Toulouse");

    auto john = person->create_object().get_dictionary(col_data);
    auto paul = person->create_object().get_dictionary(col_data);

    john.insert("Name", "John");
    john.insert("Pet", pluto);

    paul.insert("Name", "Paul");
    paul.insert("Pet", marie);

    // g.to_json(std::cout, 5);

    auto cnt = (person->column<Dictionary>(col_data).key("Pet").property("Name") == StringData("Pluto")).count();
    CHECK_EQUAL(cnt, 1);
    cnt = (person->column<Dictionary>(col_data).key("Pet").property("Name") == StringData("Marie")).count();
    CHECK_EQUAL(cnt, 1);
    cnt = (person->column<Dictionary>(col_data).key("Pet").property("Parent").property("Name") == StringData("Fido"))
              .count();
    CHECK_EQUAL(cnt, 1);
}

TEST(Query_TypeOfValue)
{
    Group g;
    auto table = g.add_table("Foo");
    auto origin = g.add_table("Origin");
    auto col_any = table->add_column(type_Mixed, "mixed");
    auto col_int = table->add_column(type_Int, "int");
    auto col_primitive_list = table->add_column_list(type_Mixed, "list");
    auto col_link = origin->add_column(*table, "link");
    auto col_links = origin->add_column_list(*table, "links");
    size_t nb_ints = 0;
    size_t nb_strings = 0;
    for (int64_t i = 0; i < 100; i++) {
        if (i % 4) {
            nb_ints++;
            table->create_object().set(col_any, Mixed(i)).set(col_int, i);
        }
        else {
            std::string str = "String" + util::to_string(i);
            table->create_object().set(col_any, Mixed(str)).set(col_int, i);
            nb_strings++;
        }
    }
    std::string bin_data("String2Binary");
    table->get_object(15).set(col_any, Mixed());
    nb_ints--;
    table->get_object(75).set(col_any, Mixed(75.));
    nb_ints--;
    table->get_object(28).set(col_any, Mixed(BinaryData(bin_data)));
    nb_strings--;
    table->get_object(25).set(col_any, Mixed(3.));
    nb_ints--;
    table->get_object(35).set(col_any, Mixed(Decimal128("3")));
    nb_ints--;

    auto list_0 = table->get_object(0).get_list<Mixed>(col_primitive_list);
    list_0.add(Mixed{1});
    list_0.add(Mixed{Decimal128(10)});
    list_0.add(Mixed{Double{100}});
    auto list_1 = table->get_object(1).get_list<Mixed>(col_primitive_list);
    list_1.add(Mixed{std::string("hello")});
    list_1.add(Mixed{1000});

    auto it = table->begin();
    for (int64_t i = 0; i < 10; i++) {
        auto obj = origin->create_object();
        auto ll = obj.get_linklist(col_links);

        obj.set(col_link, it->get_key());
        for (int64_t j = 0; j < 10; j++) {
            ll.add(it->get_key());
            ++it;
        }
    }

    auto tv = (table->column<Mixed>(col_any).type_of_value() == TypeOfValue(std::string("string"))).find_all();
    CHECK_EQUAL(tv.size(), nb_strings);
    tv = (table->column<Mixed>(col_any).type_of_value() == TypeOfValue(std::string("double"))).find_all();
    CHECK_EQUAL(tv.size(), 2);
    tv = (table->column<Mixed>(col_any).type_of_value() == TypeOfValue(std::string("Decimal128"))).find_all();
    CHECK_EQUAL(tv.size(), 1);
    tv = (table->column<Mixed>(col_any).type_of_value() == TypeOfValue(BinaryData(bin_data))).find_all();
    CHECK_EQUAL(tv.size(), 1);
    tv = (table->column<Mixed>(col_any).type_of_value() == TypeOfValue(util::none)).find_all();
    CHECK_EQUAL(tv.size(), 1);
    tv = (table->column<Mixed>(col_any).type_of_value() == TypeOfValue(type_String)).find_all();
    CHECK_EQUAL(tv.size(), nb_strings);
    tv = (table->column<Mixed>(col_any).type_of_value() == TypeOfValue(col_int)).find_all();
    CHECK_EQUAL(tv.size(), nb_ints);
    tv = (table->column<Lst<Mixed>>(col_primitive_list).type_of_value() == TypeOfValue(col_int)).find_all();
    CHECK_EQUAL(tv.size(), 2);
    tv = (table->column<Lst<Mixed>>(col_primitive_list).type_of_value() == TypeOfValue(type_Decimal)).find_all();
    CHECK_EQUAL(tv.size(), 1);
    tv = (table->column<Lst<Mixed>>(col_primitive_list).type_of_value() == TypeOfValue(type_Int)).find_all();
    CHECK_EQUAL(tv.size(), 2);
    tv = (table->column<Lst<Mixed>>(col_primitive_list, ExpressionComparisonType::All).type_of_value() ==
              TypeOfValue(TypeOfValue::Attribute::Numeric) &&
          table->column<Lst<Mixed>>(col_primitive_list).size() > 0)
             .find_all();
    CHECK_EQUAL(tv.size(), 1);
}

TEST(Query_links_to_with_bpnode_split)
{
    // The bug here is that LinksToNode would read a LinkList as a simple Array
    // instead of a BPTree. So this only worked when the number of items < REALM_MAX_BPNODE_SIZE
    Group g;
    auto table = g.add_table("Foo");
    auto origin = g.add_table("Origin");
    auto col_int = table->add_column(type_Int, "int");
    auto col_link = origin->add_column(*table, "link");
    auto col_links = origin->add_column_list(*table, "links");
    constexpr size_t num_items = REALM_MAX_BPNODE_SIZE + 1;
    for (size_t i = 0; i < num_items; ++i) {
        table->create_object().set(col_int, int64_t(i));
    }
    for (size_t i = 0; i < num_items; ++i) {
        auto obj = origin->create_object();
        auto it_i = table->begin();
        it_i.go(i);
        obj.set(col_link, it_i->get_key());
        auto list = obj.get_linklist(col_links);
        for (auto it = table->begin(); it != table->end(); ++it) {
            list.add(it->get_key());
        }
    }

    for (auto it = table->begin(); it != table->end(); ++it) {
        Query q = origin->where().links_to(col_links, it->get_key());
        CHECK_EQUAL(q.count(), num_items);
        Query q2 = origin->where().links_to(col_link, it->get_key());
        CHECK_EQUAL(q2.count(), 1);
    }
}

#endif // TEST_QUERY
