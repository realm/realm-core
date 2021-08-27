
#include "test.hpp"
#include <realm/parser/driver.hpp>
#include <realm/table_view.hpp>

using namespace realm;
using json = nlohmann::json;

// basic consts and props
json int_const = {{"kind", "constant"}, {"value", 3}, {"type", "int"}};
json string_const = {{"kind", "constant"}, {"value", "Bob"}, {"type", "string"}};
json float_const = {{"kind", "constant"}, {"value", 2.22}, {"type", "float"}};
json long_const = {{"kind", "constant"}, {"value", LONG_MAX}, {"type", "long"}};
json double_const = {{"kind", "constant"}, {"value", 2.2222222}, {"type", "double"}};
json bool_const_true = {{"kind", "constant"}, {"value", true}, {"type", "bool"}};
json bool_const_false = {{"kind", "constant"}, {"value", false}, {"type", "bool"}};
json int_prop = {{"kind", "property"}, {"path", json::array({"age"})}};
json string_prop = {{"kind", "property"}, {"path", json::array({"name"})}};
json float_prop = {{"kind", "property"}, {"path", json::array({"fee"})}};
json long_prop = {{"kind", "property"}, {"path", json::array({"salary"})}};
json double_prop = {{"kind", "property"}, {"path", json::array({"longitude"})}};
json bool_prop = {{"kind", "property"}, {"path", json::array({"isInteresting"})}};

// null consts
json int_null_const = {{"kind", "constant"}, {"value", nullptr}, {"type", "int"}};
json string_null_const = {{"kind", "constant"}, {"value", nullptr}, {"type", "string"}};
json float_null_const = {{"kind", "constant"}, {"value", nullptr}, {"type", "float"}};
json long_null_const = {{"kind", "constant"}, {"value", nullptr}, {"type", "long"}};
json double_null_const = {{"kind", "constant"}, {"value", nullptr}, {"type", "double"}};

// string_ops consts
json begins_with_const = {{"kind", "constant"}, {"value", "Bi"}, {"type", "string"}};
json ends_with_const = {{"kind", "constant"}, {"value", "e"}, {"type", "string"}};
json contains_const = {{"kind", "constant"}, {"value", "J"}, {"type", "string"}};
json like_const = {{"kind", "constant"}, {"value", "*e"}, {"type", "string"}};

json begins_with_const_non_case = {{"kind", "constant"}, {"value", "b"}, {"type", "string"}};
json ends_with_const_non_case = {{"kind", "constant"}, {"value", "E"}, {"type", "string"}};
json contains_const_non_case = {{"kind", "constant"}, {"value", "O"}, {"type", "string"}};
json like_const_non_case = {{"kind", "constant"}, {"value", "b*"}, {"type", "string"}};
json string_const_non_case = {{"kind", "constant"}, {"value", "joel"}, {"type", "string"}};

// int comparisons
json int_eq = {{"kind", "eq"}, {"left", int_prop}, {"right", int_const}};
json int_neq = {{"kind", "neq"}, {"left", int_prop}, {"right", int_const}};
json int_gt = {{"kind", "gt"}, {"left", int_prop}, {"right", int_const}};
json int_gte = {{"kind", "gte"}, {"left", int_prop}, {"right", int_const}};
json int_lt = {{"kind", "lt"}, {"left", int_prop}, {"right", int_const}};
json int_lte = {{"kind", "lte"}, {"left", int_prop}, {"right", int_const}};

// string comparisons
json string_eq = {{"kind", "eq"}, {"left", string_prop}, {"right", string_const}};
json string_eq_non_case = {
    {"kind", "eq"}, {"caseSensitivity", false}, {"left", string_prop}, {"right", string_const}};
json string_neq = {{"kind", "neq"}, {"left", string_prop}, {"right", string_const}};
json string_gt = {{"kind", "gt"}, {"left", string_prop}, {"right", string_const}};
json string_gte = {{"kind", "gte"}, {"left", string_prop}, {"right", string_const}};
json string_lt = {{"kind", "lt"}, {"left", string_prop}, {"right", string_const}};
json string_lte = {{"kind", "lte"}, {"left", string_prop}, {"right", string_const}};

// float comparisons
json float_eq = {{"kind", "eq"}, {"left", float_prop}, {"right", float_const}};
json float_neq = {{"kind", "neq"}, {"left", float_prop}, {"right", float_const}};
json float_gt = {{"kind", "gt"}, {"left", float_prop}, {"right", float_const}};
json float_gte = {{"kind", "gte"}, {"left", float_prop}, {"right", float_const}};
json float_lt = {{"kind", "lt"}, {"left", float_prop}, {"right", float_const}};
json float_lte = {{"kind", "lte"}, {"left", float_prop}, {"right", float_const}};

// long comparisons
json long_eq = {{"kind", "eq"}, {"left", long_prop}, {"right", long_const}};
json long_neq = {{"kind", "neq"}, {"left", long_prop}, {"right", long_const}};
json long_gt = {{"kind", "gt"}, {"left", long_prop}, {"right", long_const}};
json long_gte = {{"kind", "gte"}, {"left", long_prop}, {"right", long_const}};
json long_lt = {{"kind", "lt"}, {"left", long_prop}, {"right", long_const}};
json long_lte = {{"kind", "lte"}, {"left", long_prop}, {"right", long_const}};

// double comparisons
json double_eq = {{"kind", "eq"}, {"left", double_prop}, {"right", double_const}};
json double_neq = {{"kind", "neq"}, {"left", double_prop}, {"right", double_const}};
json double_gt = {{"kind", "gt"}, {"left", double_prop}, {"right", double_const}};
json double_gte = {{"kind", "gte"}, {"left", double_prop}, {"right", double_const}};
json double_lt = {{"kind", "lt"}, {"left", double_prop}, {"right", double_const}};
json double_lte = {{"kind", "lte"}, {"left", double_prop}, {"right", double_const}};

// null comparisons
json int_null_eq = {{"kind", "eq"}, {"left", int_prop}, {"right", int_null_const}};
json string_null_eq = {{"kind", "eq"}, {"left", string_prop}, {"right", string_null_const}};
json float_null_eq = {{"kind", "eq"}, {"left", float_prop}, {"right", float_null_const}};
json long_null_eq = {{"kind", "eq"}, {"left", long_prop}, {"right", long_null_const}};
json double_null_eq = {{"kind", "eq"}, {"left", double_prop}, {"right", double_null_const}};

// commutative expressions
json int_commutative_eq = {{"kind", "eq"}, {"left", int_const}, {"right", int_prop}};
json float_commutative_eq = {{"kind", "eq"}, {"left", float_const}, {"right", float_prop}};
json string_commutative_eq = {{"kind", "eq"}, {"left", string_const}, {"right", string_prop}};
json int_commutative_neq = {{"kind", "neq"}, {"left", int_const}, {"right", int_prop}};
json float_commutative_neq = {{"kind", "neq"}, {"left", float_const}, {"right", float_prop}};
json string_commutative_neq = {{"kind", "neq"}, {"left", string_const}, {"right", string_prop}};

// string operations
json string_begins_with = {{"kind", "beginsWith"}, {"left", string_prop}, {"right", begins_with_const}};
json string_ends_with = {{"kind", "endsWith"}, {"left", string_prop}, {"right", ends_with_const}};
json string_contains = {{"kind", "contains"}, {"left", string_prop}, {"right", contains_const}};
json string_like = {{"kind", "like"}, {"left", string_prop}, {"right", like_const}};

// case insensitive ops
json string_begins_with_non_case = {
    {"kind", "beginsWith"}, {"caseSensitivity", false}, {"left", string_prop}, {"right", begins_with_const_non_case}};
json string_ends_with_non_case = {
    {"kind", "endsWith"}, {"caseSensitivity", false}, {"left", string_prop}, {"right", ends_with_const_non_case}};
json string_contains_non_case = {
    {"kind", "contains"}, {"caseSensitivity", false}, {"left", string_prop}, {"right", contains_const_non_case}};
json string_like_non_case = {
    {"kind", "like"}, {"caseSensitivity", false}, {"left", string_prop}, {"right", like_const_non_case}};

// sort operations
json sort_int_asc{{"isAscending", true}, {"property", "age"}};
json sort_int_desc{{"isAscending", false}, {"property", "age"}};
json sort_string_asc{{"isAscending", true}, {"property", "name"}};
json sort_string_desc{{"isAscending", false}, {"property", "name"}};
json sort_float_asc{{"isAscending", true}, {"property", "fee"}};
json sort_float_desc{{"isAscending", false}, {"property", "fee"}};

// bool comparisons
json bool_eq_true = {{"kind", "eq"}, {"left", bool_const_true}, {"right", bool_prop}};
json bool_eq_false = {{"kind", "eq"}, {"left", bool_const_false}, {"right", bool_prop}};

json empty_object = json::object();
json empty_where_clause = {{"whereClauses", json::array()}};

Query verify_query(test_util::unit_test::TestContext& test_context, TableRef table, json json, size_t num_results)
{
    std::unique_ptr<Arguments> no_arguments(new NoArguments());
    Query q = table->query(json.dump());
    size_t q_count = q.count();
    std::string description = q.get_description("");
    CHECK_EQUAL(q_count, num_results);
    if (q_count != num_results) {
        std::cout << "the query for the above failure is: '" << description << "'" << std::endl;
    }
    return q;
}

Query verify_query_args(test_util::unit_test::TestContext& test_context, TableRef t, json json,
                        const std::vector<Mixed>& arg_list, size_t num_results)
{
    // query_parser::AnyContext ctx;
    // realm::query_parser::ArgumentConverter<util::Any, query_parser::AnyContext> args(ctx, arg_list, num_args);
    std::string test = json.dump();
    Query q = t->query(json.dump(), arg_list);
    size_t q_count = q.count();
    std::string description = q.get_description("");
    CHECK_EQUAL(q_count, num_results);
    if (q_count != num_results) {
        std::cout << "the query for the above failure is: '" << description << "'" << std::endl;
    }
    return q;
}

json simple_query(json comparison)
{
    json e = json::object();
    e["expression"] = comparison;
    json j = json::object();
    j["whereClauses"] = json::array({e});
    return j;
}


TEST(test_json_query_parser_simple)
{
    Group g;
    std::string table_name = "person";
    TableRef t = g.add_table(table_name);
    ColKey int_col = t->add_column(type_Int, "age", true);
    ColKey string_col = t->add_column(type_String, "name", true);
    ColKey float_col = t->add_column(type_Float, "fee", true);
    ColKey long_col = t->add_column(type_Int, "salary", true);
    ColKey double_col = t->add_column(type_Double, "longitude", true);
    t->add_column(type_Bool, "isInteresting", true);

    std::vector<std::string> names = {"Billy", "Bob", "Joe", "Jane", "Joel"};
    std::vector<float> fees = {2.0f, 2.23f, 2.22f, 2.25f, 3.73f};
    std::vector<long> salary = {10000, LONG_MAX, -3000, 2134, 5000};
    std::vector<double> longitude = {2.0, 2.23, 2.2222222, 2.25, 3.73};
    std::vector<bool> isInteresting = {true, false, true, false, true};
    std::vector<ObjKey> keys;
    t->create_objects(5, keys);
    for (size_t i = 0; i < t->size(); ++i) {
        t->get_object(keys[i]).set_all(int(i), StringData(names[i]), float(fees[i]), int64_t(salary[i]),
                                       double(longitude[i]), bool(isInteresting[i]));
    }

    verify_query(test_context, t, empty_object, 5);
    verify_query(test_context, t, empty_where_clause, 5);

    verify_query(test_context, t, simple_query(int_eq), 1);
    verify_query(test_context, t, simple_query(int_neq), 4);
    verify_query(test_context, t, simple_query(int_gt), 1);
    verify_query(test_context, t, simple_query(int_gte), 2);
    verify_query(test_context, t, simple_query(int_lt), 3);
    verify_query(test_context, t, simple_query(int_lte), 4);

    verify_query(test_context, t, simple_query(string_eq), 1);
    verify_query(test_context, t, simple_query(string_neq), 4);
    verify_query(test_context, t, simple_query(string_gt), 3);
    verify_query(test_context, t, simple_query(string_gte), 4);
    verify_query(test_context, t, simple_query(string_lt), 1);
    verify_query(test_context, t, simple_query(string_lte), 2);

    verify_query(test_context, t, simple_query(float_eq), 1);
    verify_query(test_context, t, simple_query(float_neq), 4);
    verify_query(test_context, t, simple_query(float_gt), 3);
    verify_query(test_context, t, simple_query(float_gte), 4);
    verify_query(test_context, t, simple_query(float_lt), 1);
    verify_query(test_context, t, simple_query(float_lte), 2);

    verify_query(test_context, t, simple_query(long_eq), 1);
    verify_query(test_context, t, simple_query(long_neq), 4);
    verify_query(test_context, t, simple_query(long_gt), 0);
    verify_query(test_context, t, simple_query(long_gte), 1);
    verify_query(test_context, t, simple_query(long_lt), 4);
    verify_query(test_context, t, simple_query(long_lte), 5);

    verify_query(test_context, t, simple_query(float_eq), 1);
    verify_query(test_context, t, simple_query(float_neq), 4);
    verify_query(test_context, t, simple_query(float_gt), 3);
    verify_query(test_context, t, simple_query(float_gte), 4);
    verify_query(test_context, t, simple_query(float_lt), 1);
    verify_query(test_context, t, simple_query(float_lte), 2);

    verify_query(test_context, t, simple_query(int_commutative_eq), 1);
    verify_query(test_context, t, simple_query(string_commutative_eq), 1);
    verify_query(test_context, t, simple_query(float_commutative_eq), 1);
    verify_query(test_context, t, simple_query(int_commutative_neq), 4);
    verify_query(test_context, t, simple_query(string_commutative_neq), 4);
    verify_query(test_context, t, simple_query(float_commutative_neq), 4);

    verify_query(test_context, t, simple_query(string_begins_with), 1);
    verify_query(test_context, t, simple_query(string_ends_with), 2);
    verify_query(test_context, t, simple_query(string_contains), 3);
    verify_query(test_context, t, simple_query(string_like), 2);

    verify_query(test_context, t, simple_query(string_begins_with_non_case), 2);
    verify_query(test_context, t, simple_query(string_ends_with_non_case), 2);
    verify_query(test_context, t, simple_query(string_contains_non_case), 3);
    verify_query(test_context, t, simple_query(string_like_non_case), 2);
    verify_query(test_context, t, simple_query(string_eq_non_case), 1);

    verify_query(test_context, t, simple_query(int_null_eq), 0);
    verify_query(test_context, t, simple_query(string_null_eq), 0);
    verify_query(test_context, t, simple_query(float_null_eq), 0);
    verify_query(test_context, t, simple_query(long_null_eq), 0);
    verify_query(test_context, t, simple_query(double_null_eq), 0);

    verify_query(test_context, t, simple_query(bool_eq_true), 3);
    verify_query(test_context, t, simple_query(bool_eq_false), 2);

    t->create_object().set(int_col, 1);
    t->create_object().set(string_col, "foo").set(float_col, 2.27f).set(long_col, 10).set(double_col, 10.3);
    verify_query(test_context, t, simple_query(int_null_eq), 1);
    verify_query(test_context, t, simple_query(string_null_eq), 1);
    verify_query(test_context, t, simple_query(float_null_eq), 1);
    verify_query(test_context, t, simple_query(long_null_eq), 1);
    verify_query(test_context, t, simple_query(double_null_eq), 1);
    const std::vector<Mixed> args = {Mixed(Int(2)), Mixed(Double(2.25)), Mixed(String("oe")), Mixed(Bool(true)),
                                     Mixed(Float(2.33))};
    std::vector<std::string> types = {"int", "double", "string", "bool", "float"};
    std::vector<json> properties = {int_prop, double_prop, string_prop, bool_prop, float_prop};
    std::vector<int> results = {1, 1, 0, 3, 0};
    size_t num_args = 5;
    for (size_t i = 0; i < num_args; i++) {
        std::stringstream ss;
        ss << "$" << i;
        std::string arg_nbr = ss.str();
        json arg_constant = {{"kind", "constant"}, {"value", arg_nbr}, {"type", "arg"}};
        json json = {{"kind", "eq"}, {"left", arg_constant}, {"right", properties[i]}};
        verify_query_args(test_context, t, simple_query(json), args, results[i]);
    }
}

json logical_query(std::string kind, json pred1, json pred2)
{
    json expr = {{"kind", kind}, {"left", pred1}, {"right", pred2}};
    json e = json::object();
    e["expression"] = expr;
    json j = json::object();
    j["whereClauses"] = json::array({e});
    return j;
}

json not_query(json pred)
{
    json not_sample = {{"kind", "not"}, {"expression", pred}};
    json e = json::object();
    e["expression"] = not_sample;
    json j = json::object();
    j["whereClauses"] = json::array({e});
    return j;
}

json multiple_where(std::vector<json> whereClauses)
{
    json q;
    q["whereClauses"] = json::array();
    for (json e : whereClauses) {
        json expr = json::object();
        expr["expression"] = e;
        q["whereClauses"].emplace_back(expr);
    }
    return q;
}

TEST(test_json_query_parser_logical)
{
    Group g;
    std::string table_name = "person";
    TableRef t = g.add_table(table_name);
    t->add_column(type_Int, "age");
    t->add_column(type_String, "name");
    t->add_column(type_Float, "fee", true);

    std::vector<std::string> names = {"Billy", "Bob", "Joe", "Jane", "Joel"};
    std::vector<double> fees = {2.0, 2.23, 2.22, 2.25, 3.73};
    std::vector<ObjKey> keys;
    t->create_objects(5, keys);
    for (size_t i = 0; i < t->size(); ++i) {
        t->get_object(keys[i]).set_all(int(i), StringData(names[i]), float(fees[i]));
    }
    verify_query(test_context, t, logical_query("and", string_lt, int_lt), 1);
    verify_query(test_context, t, logical_query("and", float_gt, int_eq), 1);
    verify_query(test_context, t, logical_query("and", float_eq, string_eq), 0);
    verify_query(test_context, t, logical_query("or", string_lt, int_lt), 3);
    verify_query(test_context, t, logical_query("or", int_gte, int_lt), 5);
    verify_query(test_context, t, logical_query("or", string_neq, float_lte), 4);
    verify_query(test_context, t, not_query(string_begins_with), 4);
    verify_query(test_context, t, not_query(string_neq), 1);
    verify_query(test_context, t, not_query(float_gt), 2);
    verify_query(test_context, t, not_query(int_lte), 1);

    std::vector<json> whereClauses;
    whereClauses.emplace_back(string_lt);
    whereClauses.emplace_back(int_lt);
    // should logically be the same as (string_lt && int_lt)
    verify_query(test_context, t, multiple_where(whereClauses), 1);
}

TableView get_sorted_view(TableRef t, json json)
{
    Query q = t->query(json.dump());
    return q.find_all();
}

json simple_query_sort(json comparison, std::vector<json> sorts)
{
    json query = simple_query(comparison);
    query["orderingClauses"] = json::array();
    for (json s : sorts) {
        query["orderingClauses"].emplace_back(s);
    }
    return query;
}

TEST(test_json_query_parser_sorting)
{
    Group g;
    std::string table_name = "person";
    TableRef t = g.add_table(table_name);
    ColKey age_col = t->add_column(type_Int, "age");
    ColKey name_col = t->add_column(type_String, "name");
    ColKey fee_col = t->add_column(type_Float, "fee", true);

    std::vector<std::string> names = {"Billy", "Bob", "Joe", "Jane", "Joel"};
    std::vector<double> fees = {2.0, 2.22, 2.25, 2.25, 3.73};
    std::vector<ObjKey> keys;
    t->create_objects(5, keys);
    for (size_t i = 0; i < t->size(); ++i) {
        t->get_object(keys[i]).set_all(int(i), StringData(names[i]), float(fees[i]));
    }

    // person:
    // name     age     fee
    // Billy     0      2.0
    // Bob       1      2.22
    // Joe       2      2.25
    // Jane      3      2.25
    // Joel      4      3.73

    // single sorts
    {
        auto check_tv = [&](TableView tv) {
            for (size_t row_ndx = 1; row_ndx < tv.size(); ++row_ndx) {
                CHECK(tv.get(row_ndx - 1).get<Int>(age_col) <= tv.get(row_ndx).get<Int>(age_col));
            }
        };
        std::vector<json> sorts;
        sorts.emplace_back(sort_int_asc);
        check_tv(get_sorted_view(t, simple_query_sort(int_neq, sorts)));
    }

    {
        auto check_tv = [&](TableView tv) {
            for (size_t row_ndx = 1; row_ndx < tv.size(); ++row_ndx) {
                CHECK(tv.get(row_ndx - 1).get<Int>(age_col) >= tv.get(row_ndx).get<Int>(age_col));
            }
        };
        std::vector<json> sorts;
        sorts.emplace_back(sort_int_desc);
        check_tv(get_sorted_view(t, simple_query_sort(int_neq, sorts)));
    }

    {
        auto check_tv = [&](TableView tv) {
            for (size_t row_ndx = 1; row_ndx < tv.size(); ++row_ndx) {
                CHECK(tv.get(row_ndx - 1).get<float>(fee_col) <= tv.get(row_ndx).get<float>(fee_col));
            }
        };
        std::vector<json> sorts;
        sorts.emplace_back(sort_float_asc);
        check_tv(get_sorted_view(t, simple_query_sort(float_neq, sorts)));
    }

    {
        auto check_tv = [&](TableView tv) {
            for (size_t row_ndx = 1; row_ndx < tv.size(); ++row_ndx) {
                CHECK(tv.get(row_ndx - 1).get<float>(fee_col) >= tv.get(row_ndx).get<float>(fee_col));
            }
        };
        std::vector<json> sorts;
        sorts.emplace_back(sort_float_desc);
        check_tv(get_sorted_view(t, simple_query_sort(float_neq, sorts)));
    }

    {
        auto check_tv = [&](TableView tv) {
            for (size_t row_ndx = 1; row_ndx < tv.size(); ++row_ndx) {
                CHECK(tv.get(row_ndx - 1).get<String>(name_col) <= tv.get(row_ndx).get<String>(name_col));
            }
        };
        std::vector<json> sorts;
        sorts.emplace_back(sort_string_asc);
        check_tv(get_sorted_view(t, simple_query_sort(float_neq, sorts)));
    }

    {
        auto check_tv = [&](TableView tv) {
            for (size_t row_ndx = 1; row_ndx < tv.size(); ++row_ndx) {
                CHECK(tv.get(row_ndx - 1).get<String>(name_col) >= tv.get(row_ndx).get<String>(name_col));
            }
        };
        std::vector<json> sorts;
        sorts.emplace_back(sort_string_desc);
        check_tv(get_sorted_view(t, simple_query_sort(float_neq, sorts)));
    }

    // multiple sorts
    {
        auto check_tv = [&](TableView tv) {
            for (size_t row_ndx = 1; row_ndx < tv.size(); ++row_ndx) {
                std::string s1 = tv.get(row_ndx - 1).get<String>(name_col);
                std::string s2 = tv.get(row_ndx).get<String>(name_col);
                float f1 = tv.get(row_ndx - 1).get<float>(fee_col);
                float f2 = tv.get(row_ndx).get<float>(fee_col);

                if (f1 == f2) {
                    CHECK(tv.get(row_ndx - 1).get<String>(name_col) >= tv.get(row_ndx).get<String>(name_col));
                }
                else {
                    CHECK(tv.get(row_ndx - 1).get<float>(fee_col) <= tv.get(row_ndx).get<float>(fee_col));
                }
            }
        };
        std::vector<json> sorts;
        // different orderings
        sorts.emplace_back(sort_float_asc);
        sorts.emplace_back(sort_string_desc);
        check_tv(get_sorted_view(t, simple_query_sort(float_neq, sorts)));
    }

    {
        auto check_tv = [&](TableView tv) {
            for (size_t row_ndx = 1; row_ndx < tv.size(); ++row_ndx) {
                std::string s1 = tv.get(row_ndx - 1).get<String>(name_col);
                std::string s2 = tv.get(row_ndx).get<String>(name_col);
                float f1 = tv.get(row_ndx - 1).get<float>(fee_col);
                float f2 = tv.get(row_ndx).get<float>(fee_col);

                if (f1 == f2) {
                    CHECK(tv.get(row_ndx - 1).get<String>(name_col) <= tv.get(row_ndx).get<String>(name_col));
                }
                else {
                    CHECK(tv.get(row_ndx - 1).get<float>(fee_col) <= tv.get(row_ndx).get<float>(fee_col));
                }
            }
        };
        std::vector<json> sorts;
        // same orderings
        sorts.emplace_back(sort_float_asc);
        sorts.emplace_back(sort_string_asc);
        check_tv(get_sorted_view(t, simple_query_sort(float_neq, sorts)));
    }
}

TEST(test_json_query_parser_links)
{
    Group g;
    TableRef t = g.add_table("class_Person");
    ColKey age_col = t->add_column(type_Int, "age");
    ColKey name_col = t->add_column(type_String, "name");
    ColKey link_col = t->add_column(*t, "buddy");
    std::vector<std::string> names = {"Billy", "Bob", "Joe", "Jane", "Joel"};
    std::vector<ObjKey> people_keys;
    t->create_objects(names.size(), people_keys);
    for (size_t i = 0; i < t->size(); ++i) {
        Obj obj = t->get_object(people_keys[i]);
        obj.set(age_col, int64_t(i));
        obj.set(name_col, StringData(names[i]));
        obj.set(link_col, people_keys[(i + 1) % t->size()]);
    }
    t->get_object(people_keys[4]).set_null(link_col);

    // tests:
    // age > 0
    // buddy.age > 0
    // buddy.buddy.age > 0
    // buddy.buddy.buddy.age > 0
    // buddy.buddy.buddy.buddy.age > 0
    // buddy.buddy.buddy.buddy.buddy.age > 0

    std::vector<int> results = {4, 4, 3, 2, 1, 0};
    for (size_t i = 0; i < results.size(); i++) {
        json int_path_prop = {{"kind", "property"}, {"path", json::array()}};
        for (size_t j = 0; j < i; j++) {
            int_path_prop["path"].push_back("buddy");
        }
        int_path_prop["path"].push_back("age");
        json int_path_const = {{"kind", "constant"}, {"value", 0}, {"type", "int"}};
        json int_path_gt = {{"kind", "gt"}, {"left", int_path_prop}, {"right", int_path_const}};
        verify_query(test_context, t, simple_query(int_path_gt), results[i]);
    }
}

json build_aggr(std::vector<std::string> path, std::string kind, std::string aggrType, std::string compOp,
                json constant)
{
    json aggr = json::object();
    aggr["path"] = json::array();
    aggr["kind"] = kind;
    for (std::string p : path) {
        aggr["path"].emplace_back(p);
    }
    aggr["aggrType"] = aggrType;

    json comp = json::object();
    comp["kind"] = compOp;
    comp["left"] = aggr;
    comp["right"] = constant;

    return simple_query(comp);
}

TEST(test_json_query_parser_aggregates)
{
    Group g;

    TableRef discounts = g.add_table("class_Discounts");
    ColKey discount_name_col = discounts->add_column(type_String, "promotion", true);
    ColKey discount_off_col = discounts->add_column(type_Double, "reduced_by");
    ColKey discount_active_col = discounts->add_column(type_Bool, "active");
    ColKey col_int_list = discounts->add_column_list(type_Int, "days_discounted");
    ColKey col_double_list = discounts->add_column_list(type_Double, "days_discounted_double");

    using discount_t = std::pair<double, bool>;
    std::vector<discount_t> discount_info = {{3.0, false}, {2.5, true}, {0.50, true}, {1.50, true}};
    std::vector<ObjKey> discount_keys;
    discounts->create_objects(discount_info.size(), discount_keys);
    for (size_t i = 0; i < discount_keys.size(); ++i) {
        Obj obj = discounts->get_object(discount_keys[i]);
        obj.set(discount_off_col, discount_info[i].first);
        obj.set(discount_active_col, discount_info[i].second);
        for (size_t j = 0; j <= i; j++) {
            obj.get_list<Int>(col_int_list).add(j);
            obj.get_list<Double>(col_double_list).add(j / (double)2);
        }
    }
    discounts->get_object(discount_keys[0]).set(discount_name_col, StringData("back to school"));
    discounts->get_object(discount_keys[1]).set(discount_name_col, StringData("pizza lunch special"));
    discounts->get_object(discount_keys[2]).set(discount_name_col, StringData("manager's special"));

    TableRef items = g.add_table("class_Items");
    ColKey item_name_col = items->add_column(type_String, "name");
    ColKey item_price_col = items->add_column(type_Double, "price");
    ColKey item_price_float_col = items->add_column(type_Float, "price_float");
    ColKey item_price_decimal_col = items->add_column(type_Decimal, "price_decimal");
    ColKey item_discount_col = items->add_column(*discounts, "discount");
    ColKey item_creation_date = items->add_column(type_Timestamp, "creation_date");
    using item_t = std::pair<std::string, double>;
    std::vector<item_t> item_info = {{"milk", 5.5}, {"oranges", 4.0}, {"pizza", 9.5}, {"cereal", 6.5}};
    std::vector<ObjKey> item_keys;
    items->create_objects(item_info.size(), item_keys);
    for (size_t i = 0; i < item_keys.size(); ++i) {
        Obj obj = items->get_object(item_keys[i]);
        obj.set(item_name_col, StringData(item_info[i].first));
        obj.set(item_price_col, item_info[i].second);
        obj.set(item_price_float_col, float(item_info[i].second));
        obj.set(item_price_decimal_col, Decimal128(item_info[i].second));
        obj.set(item_creation_date, Timestamp(static_cast<int64_t>(item_info[i].second * 10), 0));
    }
    items->get_object(item_keys[0]).set(item_discount_col, discount_keys[2]); // milk -0.50
    items->get_object(item_keys[2]).set(item_discount_col, discount_keys[1]); // pizza -2.5
    items->get_object(item_keys[3]).set(item_discount_col, discount_keys[0]); // cereal -3.0 inactive

    TableRef t = g.add_table("class_Person");
    ColKey id_col = t->add_column(type_Int, "customer_id");
    ColKey account_col = t->add_column(type_Double, "account_balance");
    ColKey items_col = t->add_column_list(*items, "items");
    ColKey account_float_col = t->add_column(type_Float, "account_balance_float");
    ColKey account_decimal_col = t->add_column(type_Decimal, "account_balance_decimal");
    ColKey account_creation_date_col = t->add_column(type_Timestamp, "account_creation_date");

    Obj person0 = t->create_object();
    Obj person1 = t->create_object();
    Obj person2 = t->create_object();

    person0.set(id_col, int64_t(0));
    person0.set(account_col, double(10.0));
    person0.set(account_float_col, float(10.0));
    person0.set(account_decimal_col, Decimal128(10.0));
    person0.set(account_creation_date_col, Timestamp(30, 0));
    person1.set(id_col, int64_t(1));
    person1.set(account_col, double(20.0));
    person1.set(account_float_col, float(20.0));
    person1.set(account_decimal_col, Decimal128(20.0));
    person1.set(account_creation_date_col, Timestamp(50, 0));
    person2.set(id_col, int64_t(2));
    person2.set(account_col, double(30.0));
    person2.set(account_float_col, float(30.0));
    person2.set(account_decimal_col, Decimal128(30.0));
    person2.set(account_creation_date_col, Timestamp(70, 0));

    LnkLst list_0 = person0.get_linklist(items_col);
    list_0.add(item_keys[0]);
    list_0.add(item_keys[1]);
    list_0.add(item_keys[2]);
    list_0.add(item_keys[3]);

    LnkLst list_1 = person1.get_linklist(items_col);
    for (size_t i = 0; i < 10; ++i) {
        list_1.add(item_keys[0]);
    }

    LnkLst list_2 = person2.get_linklist(items_col);
    list_2.add(item_keys[2]);
    list_2.add(item_keys[2]);
    list_2.add(item_keys[3]);

    std::vector<std::string> path = {"items", "price"};
    // items.@sum.price == 25.5
    json sum_double_const = {{"kind", "constant"}, {"value", 25.5}, {"type", "double"}};
    json query = build_aggr(path, "linkAggr", "sum", "eq", sum_double_const);
    verify_query(test_context, t, query, 2);
    // items.@min.price == 4.0
    json min_double_const = {{"kind", "constant"}, {"value", 4.0}, {"type", "double"}};
    query = build_aggr(path, "linkAggr", "min", "eq", min_double_const);
    verify_query(test_context, t, query, 1);
    // items.@max.price == 9.5
    json max_double_const = {{"kind", "constant"}, {"value", 9.5}, {"type", "double"}};
    query = build_aggr(path, "linkAggr", "max", "eq", max_double_const);
    verify_query(test_context, t, query, 2);
    // items.@avg.price == 6.375
    json avg_double_const = {{"kind", "constant"}, {"value", 6.375}, {"type", "double"}};
    query = build_aggr(path, "linkAggr", "avg", "eq", avg_double_const);
    verify_query(test_context, t, query, 1);
    // comparison towards properties
    json prop_path = json::array();
    prop_path.emplace_back("account_balance");
    json prop_account_balance = {{"kind", "property"}, {"path", prop_path}};
    // items.@sum.price > account_balance
    query = build_aggr(path, "linkAggr", "sum", "gt", prop_account_balance);
    verify_query(test_context, t, query, 2);
    // items.@min.price > account_balance
    query = build_aggr(path, "linkAggr", "min", "gt", prop_account_balance);
    verify_query(test_context, t, query, 0);
    // items.@max.price > account_balance
    query = build_aggr(path, "linkAggr", "max", "gt", prop_account_balance);
    verify_query(test_context, t, query, 0);
    // items.@avg.price > account_balance
    query = build_aggr(path, "linkAggr", "avg", "gt", prop_account_balance);
    verify_query(test_context, t, query, 0);

    // items.@avg.name > account_balance
    // can't aggregate strings
    path = {"items", "name"};
    query = build_aggr(path, "linkAggr", "avg", "gt", prop_account_balance);
    CHECK_THROW(verify_query(test_context, t, query, 0), query_parser::InvalidQueryError);

    // items.@avg.discount > account_balance
    // can't aggregate links
    path = {"items", "discount"};
    query = build_aggr(path, "linkAggr", "avg", "gt", prop_account_balance);
    CHECK_THROW(verify_query(test_context, t, query, 0), query_parser::InvalidQueryError);

    path = {"days_discounted"};

    json min_int_const = {{"kind", "constant"}, {"value", 0}, {"type", "int"}};
    query = build_aggr(path, "collectionAggr", "min", "eq", min_int_const);
    verify_query(test_context, discounts, query, 4);

    json max_int_const = {{"kind", "constant"}, {"value", 3}, {"type", "int"}};
    query = build_aggr(path, "collectionAggr", "max", "eq", max_int_const);
    verify_query(test_context, discounts, query, 1);

    json sum_int_const = {{"kind", "constant"}, {"value", 6}, {"type", "int"}};
    query = build_aggr(path, "collectionAggr", "sum", "eq", sum_int_const);
    verify_query(test_context, discounts, query, 1);

    json avg_const = {{"kind", "constant"}, {"value", 1.5}, {"type", "double"}};
    query = build_aggr(path, "collectionAggr", "avg", "eq", avg_const);
    verify_query(test_context, discounts, query, 1);


    path = {"days_discounted_double"};

    min_double_const = {{"kind", "constant"}, {"value", 0}, {"type", "double"}};
    query = build_aggr(path, "collectionAggr", "min", "eq", min_double_const);
    verify_query(test_context, discounts, query, 4);

    max_double_const = {{"kind", "constant"}, {"value", 1.5}, {"type", "double"}};
    query = build_aggr(path, "collectionAggr", "max", "eq", max_double_const);
    verify_query(test_context, discounts, query, 1);

    sum_double_const = {{"kind", "constant"}, {"value", 3}, {"type", "double"}};
    query = build_aggr(path, "collectionAggr", "sum", "eq", sum_double_const);
    verify_query(test_context, discounts, query, 1);

    avg_double_const = {{"kind", "constant"}, {"value", 0.75}, {"type", "double"}};
    query = build_aggr(path, "collectionAggr", "avg", "eq", avg_double_const);
    verify_query(test_context, discounts, query, 1);
}