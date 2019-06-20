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

#include "testsettings.hpp"
#ifdef TEST_JSON

#include <algorithm>
#include <limits>
#include <string>
#include <fstream>
#include <ostream>

#include <realm.hpp>

#include "util/misc.hpp"
#include "util/jsmn.hpp"

#include "test.hpp"

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

namespace {

const bool generate_all = false;

// After modifying json methods in core, set above generate_all = true to
// make the unit tests output their results to files. Then inspect the
// files manually to see if the json is correct.
//
// Finally set generate_all = false and commit them to github which will
// make all successive runs compare their produced json with these files
//
// All produced json is automatically checked for syntax regardless of
// the setting of generate_all. This is done using the 'jsmn' parser.

void setup_multi_table(Table& table, size_t rows)
{
    // Create table with all column types
    table.add_column(type_Int, "int");                                     //  0
    table.add_column(type_Bool, "bool");                                   //  1
    table.add_column(type_Timestamp, "date");                              //  2
    table.add_column(type_Float, "float");                                 //  3
    table.add_column(type_Double, "double");                               //  4
    table.add_column(type_String, "string");                               //  5
    table.add_column(type_String, "string_long");                          //  6
    ColKey string_big = table.add_column(type_String, "string_big_blobs"); //  7
    ColKey string_enum = table.add_column(type_String, "string_enum");     //  8 - becomes StringEnumColumn
    ColKey binary = table.add_column(type_Binary, "binary");               //  9

    std::vector<std::string> strings;
    for (size_t i = 0; i < rows; ++i) {
        std::stringstream out;
        out << "string" << i;
        strings.push_back(out.str());
    }
    for (size_t i = 0; i < rows; ++i) {
        int64_t sign = (i % 2 == 0) ? 1 : -1;
        std::string long_str = strings[i] + " very long string.........";
        auto obj = table.create_object().set_all(int64_t(i * sign), (i % 2 ? true : false), Timestamp{12345, 0},
                                                 123.456f * sign, 9876.54321 * sign, strings[i], long_str);
        switch (i % 2) {
            case 0: {
                std::string s = strings[i];
                s += " very long string.........";
                for (int j = 0; j != 4; ++j)
                    s += " big blobs big blobs big blobs"; // +30
                obj.set(string_big, s);
                break;
            }
            case 1:
                obj.set(string_big, "");
                break;
        }
        switch (i % 3) {
            case 0:
                obj.set(string_enum, "enum1");
                break;
            case 1:
                obj.set(string_enum, "enum2");
                break;
            case 2:
                obj.set(string_enum, "enum3");
                break;
        }
        obj.set(binary, BinaryData("binary", 7));
    }

    // We also want a StringEnumColumn
    table.enumerate_string_column(string_enum);
}

bool json_test(std::string json, std::string expected_file, bool generate)
{
    std::string file_name = get_test_resource_path();
    file_name += expected_file + ".json";

    jsmn_parser p;
    jsmntok_t* t = new jsmntok_t[10000];
    jsmn_init(&p);
    int r = jsmn_parse(&p, json.c_str(), strlen(json.c_str()), t, 10000);
    delete[] t;
    if (r < 0)
        return false;

    if (generate) {
        // Generate the testdata to compare. After doing this,
        // verify that the output is correct with a json validator:
        // http://jsonformatter.curiousconcept.com/
        std::ofstream test_file(file_name.c_str(), std::ios::out | std::ios::binary);
        test_file << json;
        std::cerr << "\n----------------------------------------\n";
        std::cerr << "Generated " << expected_file << ":\n";
        std::cerr << json << "\n----------------------------------------\n";
        return true;
    }
    else {
        std::string expected;
        std::ifstream test_file(file_name.c_str(), std::ios::in | std::ios::binary);

        // fixme, find a way to use CHECK from a function
        if (!test_file.good())
            return false;
        if (test_file.fail())
            return false;
        getline(test_file, expected);
        if (json != expected) {
            std::cout << json << std::endl;
            std::cout << expected << std::endl;
            std::string path = "bad_" + file_name;
            File out(path, File::mode_Write);
            out.write(json);
            std::cerr << "\n error result in '" << std::string(path) << "'\n";
            return false;
        }
        return true;
    }
}


TEST(Json_NoLinks)
{
    Table table;
    setup_multi_table(table, 15);

    std::stringstream ss;
    table.to_json(ss);
    CHECK(json_test(ss.str(), "expect_json", false));

    return;
}

/*
For tables with links, the link_depth argument in to_json() means following:

link_depth = -1:
    Follow links to infinite depth, but only follow each link exactly once. Not suitable if cycles exist because they
    make it complex to find out what link is being followed for a table that has multiple outgoing links

link_depth >= 0:
    Follow all possible permitations of link paths that are at most link_depth links deep. A link can be taken any
    number if times.

A link which isn't followed (bottom of link_depth has been met, or link has already been followed with
    link_depth = -1) is printed as a simple sequence of integers of row indexes in the link column.
*/
TEST(Json_LinkList1)
{
    // Basic non-cyclic LinkList test that also tests column and table renaming
    Group group;

    TableRef table1 = group.add_table("table1");
    TableRef table2 = group.add_table("table2");
    TableRef table3 = group.add_table("table3");

    // add some more columns to table1 and table2
    table1->add_column(type_Int, "col1");
    table1->add_column(type_String, "str1");

    table2->add_column(type_Int, "col1");
    table2->add_column(type_String, "str2");

    table3->add_column(type_Int, "col1");
    table3->add_column(type_String, "str2");

    // add some rows
    auto obj0 = table1->create_object().set_all(100, "foo");
    auto obj1 = table1->create_object().set_all(200, "!");
    table1->create_object().set_all(300, "bar");

    table2->create_object().set_all(400, "hello");
    auto k21 = table2->create_object().set_all(500, "world").get_key();
    auto k22 = table2->create_object().set_all(600, "!").get_key();

    auto k30 = table3->create_object().set_all(700, "baz").get_key();
    table3->create_object().set_all(800, "test");
    auto k32 = table3->create_object().set_all(900, "hi").get_key();

    ColKey col_link2 = table1->add_column_link(type_LinkList, "linkA", *table2);
    ColKey col_link3 = table1->add_column_link(type_LinkList, "linkB", *table3);

    // set some links
    auto ll0 = obj0.get_linklist(col_link2); // Links to table 2
    ll0.add(k21);

    auto ll1 = obj1.get_linklist(col_link2); // Links to table 2
    ll1.add(k21);
    ll1.add(k22);

    auto ll2 = obj0.get_linklist(col_link3); // Links to table 3
    ll2.add(k30);
    ll2.add(k32);

    std::stringstream ss;

    // Now try different link_depth arguments
    table1->to_json(ss);
    CHECK(json_test(ss.str(), "expected_json_linklist1_1", generate_all));

    ss.str("");
    table1->to_json(ss, -1);
    CHECK(json_test(ss.str(), "expected_json_linklist1_2", generate_all));

    ss.str("");
    table1->to_json(ss, 0);
    CHECK(json_test(ss.str(), "expected_json_linklist1_3", generate_all));

    ss.str("");
    table1->to_json(ss, 1);
    CHECK(json_test(ss.str(), "expected_json_linklist1_4", generate_all));

    ss.str("");
    table1->to_json(ss, 2);
    CHECK(json_test(ss.str(), "expected_json_linklist1_5", generate_all));

    // Column and table renaming
    std::map<std::string, std::string> m;
    m["str1"] = "STR1";
    m["linkA"] = "LINKA";
    m["table1"] = "TABLE1";
    ss.str("");
    table1->to_json(ss, 2, &m);
    CHECK(json_test(ss.str(), "expected_json_linklist1_6", generate_all));
}

TEST(Json_LinkListCycle)
{
    // Cycle in LinkList
    Group group;

    TableRef table1 = group.add_table("table1");
    TableRef table2 = group.add_table("table2");

    table1->add_column(type_String, "str1");
    table2->add_column(type_String, "str2");

    // add some rows
    auto t10 = table1->create_object().set_all("hello");
    table1->create_object().set_all("world");

    auto t20 = table2->create_object().set_all("foo");

    auto col_link1 = table1->add_column_link(type_LinkList, "linkA", *table2);
    auto col_link2 = table2->add_column_link(type_LinkList, "linkB", *table1);

    // set some links
    auto links1 = t10.get_linklist(col_link1);
    links1.add(t20.get_key());

    auto links2 = t20.get_linklist(col_link2);
    links2.add(t10.get_key());

    // create json

    std::stringstream ss;

    // Now try different link_depth arguments
    table1->to_json(ss);
    CHECK(json_test(ss.str(), "expected_json_linklist_cycle1", generate_all));

    ss.str("");
    table1->to_json(ss, -1);
    CHECK(json_test(ss.str(), "expected_json_linklist_cycle2", generate_all));

    ss.str("");
    table1->to_json(ss, 0);
    CHECK(json_test(ss.str(), "expected_json_linklist_cycle3", generate_all));

    ss.str("");
    table1->to_json(ss, 1);
    CHECK(json_test(ss.str(), "expected_json_linklist_cycle4", generate_all));

    ss.str("");
    table1->to_json(ss, 2);
    CHECK(json_test(ss.str(), "expected_json_linklist_cycle5", generate_all));

    ss.str("");
    table1->to_json(ss, 3);
    CHECK(json_test(ss.str(), "expected_json_linklist_cycle6", generate_all));
}

TEST(Json_LinkCycles)
{
    // Cycle in Link
    Group group;

    TableRef table1 = group.add_table("table1");
    TableRef table2 = group.add_table("table2");

    table1->add_column(type_String, "str1");
    table2->add_column(type_String, "str2");

    // add some rows
    auto t10 = table1->create_object().set_all("hello");
    table1->create_object().set_all("world");

    auto t20 = table2->create_object().set_all("foo");

    ColKey col_link1 = table1->add_column_link(type_Link, "linkA", *table2);
    ColKey col_link2 = table2->add_column_link(type_Link, "linkB", *table1);

    // set some links
    table1->begin()->set(col_link1, t20.get_key());
    table2->begin()->set(col_link2, t10.get_key());

    std::stringstream ss;

    // Now try different link_depth arguments
    table1->to_json(ss);
    CHECK(json_test(ss.str(), "expected_json_link_cycles1", generate_all));

    ss.str("");
    table1->to_json(ss, -1);
    CHECK(json_test(ss.str(), "expected_json_link_cycles2", generate_all));

    ss.str("");
    table1->to_json(ss, 0);
    CHECK(json_test(ss.str(), "expected_json_link_cycles3", generate_all));

    ss.str("");
    table1->to_json(ss, 1);
    CHECK(json_test(ss.str(), "expected_json_link_cycles4", generate_all));

    ss.str("");
    table1->to_json(ss, 2);
    CHECK(json_test(ss.str(), "expected_json_link_cycles5", generate_all));
}
} // anonymous namespace

#endif // TEST_TABLE
