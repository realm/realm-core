
#include "test.hpp"
#include <realm/parser/driver.hpp>

using namespace realm;
using json = nlohmann::json;

auto base = R"(
{
  "whereClauses": [],
  "orderingClauses": []
}
)"_json;

json int_const = { {"kind", "constant"}, {"value", 3}, {"type", "int"} };
json string_const = { {"kind", "constant"}, {"value", "Bob"}, {"type", "string"} };
json float_const = { {"kind", "constant"}, {"value", 2.22}, {"type", "float"} };
json int_prop = { {"kind", "property"}, {"name", "age"}, {"type", "int"} };
json string_prop = { {"kind", "property"}, {"name", "name"}, {"type", "string"} };
json float_prop = { {"kind", "property"}, {"name", "fee"}, {"type", "float"} };
//int comparisons
json int_eq_sample = { {"kind", "eq"}, {"left", int_prop}, {"right", int_const}};
json int_neq_sample = { {"kind", "neq"}, {"left", int_prop}, {"right", int_const}};
json int_gt_sample = { {"kind", "gt"}, {"left", int_prop}, {"right", int_const}};
json int_gte_sample = { {"kind", "gte"}, {"left", int_prop}, {"right", int_const}};
json int_lt_sample = { {"kind", "lt"}, {"left", int_prop}, {"right", int_const}};
json int_lte_sample = { {"kind", "lte"}, {"left", int_prop}, {"right", int_const}};
//string comparisons
json string_eq_sample = { {"kind", "eq"}, {"left", string_prop}, {"right", string_const}};
json string_neq_sample = { {"kind", "neq"}, {"left", string_prop}, {"right", string_const}};
json string_gt_sample = { {"kind", "gt"}, {"left", string_prop}, {"right", string_const}};
json string_gte_sample = { {"kind", "gte"}, {"left", string_prop}, {"right", string_const}};
json string_lt_sample = { {"kind", "lt"}, {"left", string_prop}, {"right", string_const}};
json string_lte_sample = { {"kind", "lte"}, {"left", string_prop}, {"right", string_const}};
//float comparisons
json float_eq_sample = { {"kind", "eq"}, {"left", float_prop}, {"right", float_const}};
json float_neq_sample = { {"kind", "neq"}, {"left", float_prop}, {"right", float_const}};
json float_gt_sample = { {"kind", "gt"}, {"left", float_prop}, {"right", float_const}};
json float_gte_sample = { {"kind", "gte"}, {"left", float_prop}, {"right", float_const}};
json float_lt_sample = { {"kind", "lt"}, {"left", float_prop}, {"right", float_const}};
json float_lte_sample = { {"kind", "lte"}, {"left", float_prop}, {"right", float_const}};




Query verify_query(test_util::unit_test::TestContext& test_context, TableRef table, json json, size_t num_results)
{
    JsonQueryParser parser;
    Query q = parser.query_from_json(table, json);
    size_t q_count = q.count();
    std::string description = q.get_description("");
    CHECK_EQUAL(q_count, num_results);
    if (q_count != num_results) {
        std::cout << "the query for the above failure is: '" << description << "'" << std::endl;
    }
    return q;
}


json construct_simple_query(json comparison){
    json e = json::object();
    e["expression"] = comparison;
    json j = json::object();
    j["whereClauses"] = json::array({e});
    return j;
}


TEST(test_comparisons)
{
    //construct table
    Group g;
    std::string table_name = "person";
    TableRef t = g.add_table(table_name);
    auto int_col_key = t->add_column(type_Int, "age");
    t->add_column(type_String, "name");
    t->add_column(type_Float, "fee", true);
    t->add_search_index(int_col_key);

    //populate table
    std::vector<std::string> names = {"Billy", "Bob", "Joe", "Jane", "Joel"};
    std::vector<double> fees = {2.0, 2.23, 2.22, 2.25, 3.73};
    std::vector<ObjKey> keys;
    t->create_objects(5, keys);
    for (size_t i = 0; i < t->size(); ++i) {
        t->get_object(keys[i]).set_all(int(i), StringData(names[i]), float(fees[i]));
    }

    //test
    verify_query(test_context, t, construct_simple_query(int_eq_sample), 1);
    verify_query(test_context, t, construct_simple_query(int_neq_sample), 4);
    verify_query(test_context, t, construct_simple_query(int_gt_sample), 1);
    verify_query(test_context, t, construct_simple_query(int_gte_sample), 2);
    verify_query(test_context, t, construct_simple_query(int_lt_sample), 3);
    verify_query(test_context, t, construct_simple_query(int_lte_sample), 4);


    verify_query(test_context, t, construct_simple_query(string_eq_sample), 1);
    verify_query(test_context, t, construct_simple_query(string_neq_sample), 4);
    verify_query(test_context, t, construct_simple_query(string_gt_sample), 3);
    verify_query(test_context, t, construct_simple_query(string_gte_sample), 4);
    verify_query(test_context, t, construct_simple_query(string_lt_sample), 1);
    verify_query(test_context, t, construct_simple_query(string_lte_sample), 2);

    verify_query(test_context, t, construct_simple_query(float_eq_sample), 1);
    verify_query(test_context, t, construct_simple_query(float_neq_sample), 4);
    verify_query(test_context, t, construct_simple_query(float_gt_sample), 3);
    verify_query(test_context, t, construct_simple_query(float_gte_sample), 4);
    verify_query(test_context, t, construct_simple_query(float_lt_sample), 1);
    verify_query(test_context, t, construct_simple_query(float_lte_sample), 2);
}


//test commutativity