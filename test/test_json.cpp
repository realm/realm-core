#include "testsettings.hpp"
#ifdef TEST_JSON

#include <algorithm>
#include <limits>
#include <string>
#include <fstream>
#include <ostream>

#include <realm.hpp>
#include <realm/lang_bind_helper.hpp>

#include "util/misc.hpp"
#include "util/jsmn.hpp"

#include "test.hpp"

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;
using unit_test::TestResults;


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

static bool generate_all = false;

// After modifying json methods in core, set above generate_all = true to
// make the unit tests output their results to files. Then inspect the
// files manually to see if the json is correct.
//
// Finally set generate_all = false and commit them to github which will
// make all successive runs compare their produced json with these files
//
// All produced json is automatically checked for syntax regardless of
// the setting of generate_all. This is done using the 'jsmn' parser.

void setup_multi_table(Table& table, size_t rows, size_t sub_rows, bool fixed_subtab_sizes = false)
{
    // Create table with all column types
    {
        DescriptorRef sub1;
        table.add_column(type_Int, "int");              //  0
        table.add_column(type_Bool, "bool");             //  1
        table.add_column(type_DateTime, "date");             //  2
        table.add_column(type_Float, "float");            //  3
        table.add_column(type_Double, "double");           //  4
        table.add_column(type_String, "string");           //  5
        table.add_column(type_String, "string_long");      //  6
        table.add_column(type_String, "string_big_blobs"); //  7
        table.add_column(type_String, "string_enum");      //  8 - becomes ColumnStringEnum
        table.add_column(type_Binary, "binary");           //  9
        table.add_column(type_Table, "tables", &sub1);    // 10
        table.add_column(type_Mixed, "mixed");            // 11
        sub1->add_column(type_Int, "sub_first");
        sub1->add_column(type_String, "sub_second");
    }

    table.add_empty_row(rows);

    for (size_t i = 0; i < rows; ++i) {
        int64_t sign = (i % 2 == 0) ? 1 : -1;
        table.set_int(0, i, int64_t(i*sign));
    }
    for (size_t i = 0; i < rows; ++i)
        table.set_bool(1, i, (i % 2 ? true : false));
    for (size_t i = 0; i < rows; ++i)
        table.set_datetime(2, i, 12345);
    for (size_t i = 0; i < rows; ++i) {
        int64_t sign = (i % 2 == 0) ? 1 : -1;
        table.set_float(3, i, 123.456f*sign);
    }
    for (size_t i = 0; i < rows; ++i) {
        int64_t sign = (i % 2 == 0) ? 1 : -1;
        table.set_double(4, i, 9876.54321*sign);
    }
    std::vector<std::string> strings;
    for (size_t i = 0; i < rows; ++i) {
        std::stringstream out;
        out << "string" << i;
        strings.push_back(out.str());
    }
    for (size_t i = 0; i < rows; ++i)
        table.set_string(5, i, strings[i]);
    for (size_t i = 0; i < rows; ++i)
        table.set_string(6, i, strings[i] + " very long string.........");
    for (size_t i = 0; i < rows; ++i) {
        switch (i % 2) {
        case 0: {
                    std::string s = strings[i];
                    s += " very long string.........";
                    for (int j = 0; j != 4; ++j)
                        s += " big blobs big blobs big blobs"; // +30
                    table.set_string(7, i, s);
                    break;
        }
        case 1:
            table.set_string(7, i, "");
            break;
        }
    }
    for (size_t i = 0; i < rows; ++i) {
        switch (i % 3) {
        case 0:
            table.set_string(8, i, "enum1");
            break;
        case 1:
            table.set_string(8, i, "enum2");
            break;
        case 2:
            table.set_string(8, i, "enum3");
            break;
        }
    }
    for (size_t i = 0; i < rows; ++i)
        table.set_binary(9, i, BinaryData("binary", 7));
    for (size_t i = 0; i < rows; ++i) {
        int64_t sign = (i % 2 == 0) ? 1 : -1;
        size_t n = sub_rows;
        if (!fixed_subtab_sizes)
            n += i;
        for (size_t j = 0; j != n; ++j) {
            TableRef subtable = table.get_subtable(10, i);
            int64_t val = -123 + i*j * 1234 * sign;
            subtable->insert_empty_row(j);
            subtable->set_int(0, j, val);
            subtable->set_string(1, j, "sub");
        }
    }
    for (size_t i = 0; i < rows; ++i) {
        int64_t sign = (i % 2 == 0) ? 1 : -1;
        switch (i % 8) {
        case 0:
            table.set_mixed(11, i, false);
            break;
        case 1:
            table.set_mixed(11, i, int64_t(i*i*sign));
            break;
        case 2:
            table.set_mixed(11, i, "string");
            break;
        case 3:
            table.set_mixed(11, i, DateTime(123456789));
            break;
        case 4:
            table.set_mixed(11, i, BinaryData("binary", 7));
            break;
        case 5: {
                    // Add subtable to mixed column
                    // We can first set schema and contents when the entire
                    // row has been inserted
                    table.set_mixed(11, i, Mixed::subtable_tag());
                    TableRef subtable = table.get_subtable(11, i);
                    subtable->add_column(type_Int, "first");
                    subtable->add_column(type_String, "second");
                    for (size_t j = 0; j != 2; ++j) {
                        subtable->insert_empty_row(j);
                        subtable->set_int(0, j, i*i*j*sign);
                        subtable->set_string(1, j, "mixed sub");
                    }
                    break;
        }
        case 6:
            table.set_mixed(11, i, float(123.1*i*sign));
            break;
        case 7:
            table.set_mixed(11, i, double(987.65*i*sign));
            break;
        }
    }

    // We also want a ColumnStringEnum
    table.optimize();
}

bool json_test(std::string json, std::string expected_file, bool generate)
{
    std::string file_name = get_test_resource_path();
    file_name += expected_file + ".json";

    jsmn_parser p;
    jsmntok_t *t = new jsmntok_t[10000];
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
        if(!test_file.good())
            return false;
        if (test_file.fail())
            return false;
        getline(test_file, expected);
        if (json != expected) {
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
    setup_multi_table(table, 15, 2);

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
    table1->add_empty_row(3);
    table1->set_int(0, 0, 100);
    table1->set_string(1, 0, "foo");
    table1->set_int(0, 1, 200);
    table1->set_string(1, 1, "!");
    table1->set_int(0, 2, 300);
    table1->set_string(1, 2, "bar");

    table2->add_empty_row(3);
    table2->set_int(0, 0, 400);
    table2->set_string(1, 0, "hello");
    table2->set_int(0, 1, 500);
    table2->set_string(1, 1, "world");
    table2->set_int(0, 2, 600);
    table2->set_string(1, 2, "!");

    table3->add_empty_row(3);
    table3->set_int(0, 0, 700);
    table3->set_string(1, 0, "baz");
    table3->set_int(0, 1, 800);
    table3->set_string(1, 1, "test");
    table3->set_int(0, 2, 900);
    table3->set_string(1, 2, "hi");

    size_t col_link2 = table1->add_column_link(type_LinkList, "linkA", *table2);
    size_t col_link3 = table1->add_column_link(type_LinkList, "linkB", *table3);

    // set some links
    LinkViewRef links1;

    links1 = table1->get_linklist(col_link2, 0);
    links1->add(1);

    links1 = table1->get_linklist(col_link2, 1);
    links1->add(1);
    links1->add(2);

    links1 = table1->get_linklist(col_link3, 0);
    links1->add(0);
    links1->add(2);

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
    table1->add_empty_row(2);
    table1->set_string(0, 0, "hello");
    table1->set_string(0, 1, "world");

    table2->add_empty_row(1);
    table2->set_string(0, 0, "foo");

    size_t col_link1 = table1->add_column_link(type_LinkList, "linkA", *table2);
    size_t col_link2 = table2->add_column_link(type_LinkList, "linkB", *table1);

    // set some links
    LinkViewRef links1;
    LinkViewRef links2;

    links1 = table1->get_linklist(col_link1, 0);
    links1->add(0);

    links2 = table2->get_linklist(col_link2, 0);
    links2->add(0);

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
    table1->add_empty_row(2);
    table1->set_string(0, 0, "hello");
    table1->set_string(0, 1, "world");

    table2->add_empty_row(1);
    table2->set_string(0, 0, "foo");

    size_t col_link1 = table1->add_column_link(type_Link, "linkA", *table2);
    size_t col_link2 = table2->add_column_link(type_Link, "linkB", *table1);

    // set some links
    table1->set_link(col_link1, 0, 0);
    table2->set_link(col_link2, 0, 0);

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
