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

#if defined(TEST_METRICS)
#include "test.hpp"


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

#if REALM_METRICS

#include <realm.hpp>
#include <realm/util/encrypted_file_mapping.hpp>
#include <realm/util/to_string.hpp>
#include <realm/replication.hpp>
#include <realm/history.hpp>

#include <future>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

using namespace realm;
using namespace realm::metrics;
using namespace realm::test_util;
using namespace realm::util;

TEST(Metrics_HasNoReportsWhenDisabled)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBOptions options(crypt_key());
    options.enable_metrics = false;
    DBRef sg = DB::create(*hist, options);
    CHECK(!sg->get_metrics());
    auto wt = sg->start_write();
    auto table = wt->add_table("table");
    auto col = table->add_column(type_Int, "first");
    std::vector<ObjKey> keys;
    table->create_objects(10, keys);
    wt->commit();
    auto rt = sg->start_read();
    table = rt->get_table("table");
    CHECK(bool(table));
    Query query = table->column<int64_t>(col) == 0;
    query.count();
    rt->end_read();
    CHECK(!sg->get_metrics());
}

TEST(Metrics_HasReportsWhenEnabled)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBOptions options(crypt_key());
    options.enable_metrics = true;
    DBRef sg = DB::create(*hist, options);
    CHECK(sg->get_metrics());
    auto wt = sg->start_write();
    auto table = wt->add_table("table");
    auto col = table->add_column(type_Int, "first");
    std::vector<ObjKey> keys;
    table->create_objects(10, keys);
    wt->commit();
    auto rt = sg->start_read();
    table = rt->get_table("table");
    CHECK(bool(table));
    Query query = table->column<int64_t>(col) == 0;
    query.count();
    rt->end_read();
    std::shared_ptr<Metrics> metrics = sg->get_metrics();
    CHECK(metrics);
    CHECK(metrics->num_query_metrics() != 0);
    CHECK(metrics->num_transaction_metrics() != 0);
}

TEST(Metrics_QueryTypes)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBOptions options(crypt_key());
    options.enable_metrics = true;
    DBRef sg = DB::create(*hist, options);
    CHECK(sg->get_metrics());
    auto wt = sg->start_write();
    auto table = wt->add_table("table");
    auto int_col = table->add_column(type_Int, "col_int");
    auto double_col = table->add_column(type_Double, "col_double");
    auto float_col = table->add_column(type_Float, "col_float");
    auto timestamp_col = table->add_column(type_Timestamp, "col_timestamp");
    std::vector<ObjKey> keys;
    table->create_objects(10, keys);
    wt->commit();
    auto rt = sg->start_read();
    table = rt->get_table("table");
    CHECK(bool(table));
    Query query = table->column<int64_t>(int_col) == 0;
    query.find();
    query.find_all();
    query.count();
    query.sum_int(int_col);
    query.average_int(int_col);
    query.maximum_int(int_col);
    query.minimum_int(int_col);

    query.sum_double(double_col);
    query.average_double(double_col);
    query.maximum_double(double_col);
    query.minimum_double(double_col);

    query.sum_float(float_col);
    query.average_float(float_col);
    query.maximum_float(float_col);
    query.minimum_float(float_col);

    ObjKey return_dummy;
    query.maximum_timestamp(timestamp_col, &return_dummy);
    query.minimum_timestamp(timestamp_col, &return_dummy);

    rt->end_read();
    std::shared_ptr<Metrics> metrics = sg->get_metrics();
    CHECK(metrics);
    CHECK_EQUAL(metrics->num_query_metrics(), 17);
    std::unique_ptr<Metrics::QueryInfoList> queries = metrics->take_queries();
    CHECK_EQUAL(metrics->num_query_metrics(), 0);
    CHECK(queries);
    CHECK_EQUAL(queries->size(), 17);
    CHECK_EQUAL(queries->at(0).get_type(), QueryInfo::type_Find);
    CHECK_EQUAL(queries->at(1).get_type(), QueryInfo::type_FindAll);
    CHECK_EQUAL(queries->at(2).get_type(), QueryInfo::type_Count);
    CHECK_EQUAL(queries->at(3).get_type(), QueryInfo::type_Sum);
    CHECK_EQUAL(queries->at(4).get_type(), QueryInfo::type_Average);
    CHECK_EQUAL(queries->at(5).get_type(), QueryInfo::type_Maximum);
    CHECK_EQUAL(queries->at(6).get_type(), QueryInfo::type_Minimum);

    CHECK_EQUAL(queries->at(7).get_type(), QueryInfo::type_Sum);
    CHECK_EQUAL(queries->at(8).get_type(), QueryInfo::type_Average);
    CHECK_EQUAL(queries->at(9).get_type(), QueryInfo::type_Maximum);
    CHECK_EQUAL(queries->at(10).get_type(), QueryInfo::type_Minimum);

    CHECK_EQUAL(queries->at(11).get_type(), QueryInfo::type_Sum);
    CHECK_EQUAL(queries->at(12).get_type(), QueryInfo::type_Average);
    CHECK_EQUAL(queries->at(13).get_type(), QueryInfo::type_Maximum);
    CHECK_EQUAL(queries->at(14).get_type(), QueryInfo::type_Minimum);

    CHECK_EQUAL(queries->at(15).get_type(), QueryInfo::type_Maximum);
    CHECK_EQUAL(queries->at(16).get_type(), QueryInfo::type_Minimum);
}

size_t find_count(std::string haystack, std::string needle)
{
    size_t find_pos = 0;
    size_t count = 0;
    while (find_pos < haystack.size()) {
        find_pos = haystack.find(needle, find_pos);
        if (find_pos == std::string::npos)
            break;
        ++find_pos;
        ++count;
    }
    return count;
}

void populate(DBRef sg)
{
    auto wt = sg->start_write();
    auto person = wt->add_table("person");
    auto pet = wt->add_table("pet");
    person->add_column(type_Int, "age");
    person->add_column(type_Double, "paid");
    person->add_column(type_Float, "weight");
    person->add_column(type_Timestamp, "date_of_birth");
    person->add_column(type_String, "name");
    person->add_column(type_Bool, "account_overdue");
    person->add_column(type_Binary, "data");
    auto owes_col = person->add_column_link(type_LinkList, "owes_coffee_to", *person);

    auto create_person = [&](int age, double paid, float weight, Timestamp dob, std::string name, bool overdue,
                             std::string data, std::vector<ObjKey> owes_coffee_to) {
        BinaryData bd(data);
        Obj obj = person->create_object().set_all(age, paid, weight, dob, name, overdue, bd);
        auto ll = obj.get_linklist(owes_col);
        for (auto key : owes_coffee_to) {
            ll.add(key);
        }
        return obj.get_key();
    };

    auto k0 = create_person(27, 28.80, 170.7f, Timestamp(27, 5), "Bob", true, "e72s", {});
    auto k1 = create_person(28, 10.70, 165.8f, Timestamp(28, 8), "Ryan", false, "s83f", {k0});
    auto k2 = create_person(33, 55.28, 183.3f, Timestamp(33, 3), "Cole", true, "s822k", {k1, k0});
    auto k3 = create_person(39, 22.72, 173.8f, Timestamp(39, 2), "Nathan", true, "h282l", {k1, k1, k0, k2});
    create_person(33, 29.28, 188.7f, Timestamp(33, 9), "Riley", false, "a208s", {k3, k3, k2, k1});

    pet->add_column(type_String, "name");
    pet->add_column_link(type_Link, "owner", *person);

    auto create_pet = [&](std::string name, ObjKey owner) { pet->create_object().set_all(name, owner); };

    create_pet("Fido", k0);
    create_pet("Max", k1);
    create_pet("Buddy", k2);
    create_pet("Rocky", k3);
    create_pet("Toby", k3);
    create_pet("Duke", k0);

    wt->commit();
}

TEST(Metrics_QueryEqual)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBOptions options(crypt_key());
    options.enable_metrics = true;
    DBRef sg = DB::create(*hist, options);
    populate(sg);

    std::string person_table_name = "person";
    std::string pet_table_name = "pet";
    std::string query_search_term = "==";

    auto wt = sg->start_write();
    TableRef person = wt->get_table(person_table_name);
    TableRef pet = wt->get_table(pet_table_name);
    CHECK(bool(person));

    CHECK_EQUAL(person->get_column_count(), 8);
    std::vector<std::string> column_names;
    for (auto col : person->get_column_keys()) {
        column_names.push_back(person->get_column_name(col));
    }
    for (auto col : pet->get_column_keys()) {
        column_names.push_back(pet->get_column_name(col));
    }

    Obj p0 = person->get_object(0);

    auto col_age = person->get_column_key("age");
    auto col_paid = person->get_column_key("paid");
    auto col_weight = person->get_column_key("weight");
    auto col_birth = person->get_column_key("date_of_birth");
    auto col_name = person->get_column_key("name");
    auto col_overdue = person->get_column_key("account_overdue");
    auto col_data = person->get_column_key("data");
    auto col_owes = person->get_column_key("owes_coffee_to");

    auto col_pet_name = pet->get_column_key("name");
    auto col_owner = pet->get_column_key("owner");

    Query q0 = person->column<int64_t>(col_age) == 0;
    Query q1 = person->column<double>(col_paid) == 0.0;
    Query q2 = person->column<float>(col_weight) == 0.0f;
    Query q3 = person->column<Timestamp>(col_birth) == Timestamp(0, 0);
    StringData name("");
    Query q4 = person->column<StringData>(col_name) == name;
    Query q5 = person->column<bool>(col_overdue) == false;
    BinaryData bd("");
    Query q6 = person->column<BinaryData>(col_data) == bd;
    Query q7 = person->column<Link>(col_owes) == p0;
    Query q8 = pet->column<String>(col_pet_name) == name;
    Query q9 = pet->column<Link>(col_owner) == p0;

    q0.find_all();
    q1.find_all();
    q2.find_all();
    q3.find_all();
    q4.find_all();
    q5.find_all();
    q6.find_all();
    q7.find_all();
    q8.find_all();
    q9.find_all();

    std::shared_ptr<Metrics> metrics = sg->get_metrics();
    CHECK(metrics);
    std::unique_ptr<Metrics::QueryInfoList> queries = metrics->take_queries();
    CHECK(queries);
    CHECK_EQUAL(queries->size(), 10);

    for (size_t i = 0; i < 10; ++i) {
        std::string description = queries->at(i).get_description();
        CHECK_EQUAL(find_count(description, column_names[i]), 1);
        CHECK_GREATER_EQUAL(find_count(description, query_search_term), 1);
    }
}

TEST(Metrics_QueryOrAndNot)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBOptions options(crypt_key());
    options.enable_metrics = true;
    DBRef sg = DB::create(*hist, options);
    populate(sg);

    std::string person_table_name = "person";
    std::string pet_table_name = "pet";
    std::string query_search_term = "==";
    std::string not_text = "!";

    auto wt = sg->start_write();
    TableRef person = wt->get_table(person_table_name);
    TableRef pet = wt->get_table(pet_table_name);
    CHECK(bool(person));

    CHECK_EQUAL(person->get_column_count(), 8);
    std::vector<std::string> column_names;
    for (auto col : person->get_column_keys()) {
        column_names.push_back(person->get_column_name(col));
    }

    auto col_age = person->get_column_key("age");
    auto col_paid = person->get_column_key("paid");
    auto col_weight = person->get_column_key("weight");
    Query q0 = person->column<int64_t>(col_age) == 0;
    Query q1 = person->column<double>(col_paid) == 0.0;
    Query q2 = person->column<float>(col_weight) == 0.1f;

    Query simple_and = q0 && q1;
    Query simple_or = q0 || q1;
    Query simple_not = !q0;

    Query or_and = q2 || (simple_and);
    Query and_or = simple_and || q2;
    Query or_nested = q2 || simple_or;
    Query and_nested = q2 && simple_and;
    Query not_simple_and = !(simple_and);
    Query not_simple_or = !(simple_or);
    Query not_or_and = !(or_and);
    Query not_and_or = !(and_or);
    Query not_or_nested = !(or_nested);
    Query not_and_nested = !(and_nested);
    Query and_true = q0 && std::unique_ptr<realm::Expression>(new TrueExpression);
    Query and_false = q0 && std::unique_ptr<realm::Expression>(new FalseExpression);

    simple_and.find_all();
    simple_or.find_all();
    simple_not.find_all();
    or_and.find_all();
    and_or.find_all();
    or_nested.find_all();
    and_nested.find_all();
    not_simple_and.find_all();
    not_simple_or.find_all();
    not_or_and.find_all();
    not_and_or.find_all();
    not_or_nested.find_all();
    not_and_nested.find_all();
    and_true.find_all();
    and_false.find_all();

    std::shared_ptr<Metrics> metrics = sg->get_metrics();
    CHECK(metrics);
    std::unique_ptr<Metrics::QueryInfoList> queries = metrics->take_queries();
    CHECK(queries);
    CHECK_EQUAL(queries->size(), 15);

    std::string and_description = queries->at(0).get_description();
    CHECK_EQUAL(find_count(and_description, " and "), 1);
    CHECK_EQUAL(find_count(and_description, column_names[0]), 1);
    CHECK_EQUAL(find_count(and_description, column_names[1]), 1);
    CHECK_EQUAL(find_count(and_description, query_search_term), 2);

    std::string or_description = queries->at(1).get_description();
    CHECK_EQUAL(find_count(or_description, " or "), 1);
    CHECK_EQUAL(find_count(or_description, column_names[0]), 1);
    CHECK_EQUAL(find_count(or_description, column_names[1]), 1);
    CHECK_EQUAL(find_count(or_description, query_search_term), 2);

    std::string not_description = queries->at(2).get_description();
    CHECK_EQUAL(find_count(not_description, not_text), 1);
    CHECK_EQUAL(find_count(not_description, column_names[0]), 1);
    CHECK_EQUAL(find_count(not_description, query_search_term), 1);

    std::string or_and_description = queries->at(3).get_description();
    CHECK_EQUAL(find_count(or_and_description, and_description), 1);
    CHECK_EQUAL(find_count(or_and_description, " or "), 1);
    CHECK_EQUAL(find_count(or_and_description, column_names[2]), 1);

    std::string and_or_description = queries->at(4).get_description();
    CHECK_EQUAL(find_count(and_or_description, and_description), 1);
    CHECK_EQUAL(find_count(and_or_description, " or "), 1);
    CHECK_EQUAL(find_count(and_or_description, column_names[2]), 1);

    std::string or_nested_description = queries->at(5).get_description();
    CHECK_EQUAL(find_count(or_nested_description, or_description), 1);
    CHECK_EQUAL(find_count(or_nested_description, " or "), 2);
    CHECK_EQUAL(find_count(or_nested_description, column_names[2]), 1);

    std::string and_nested_description = queries->at(6).get_description();
    CHECK_EQUAL(find_count(and_nested_description, and_description), 1);
    CHECK_EQUAL(find_count(and_nested_description, " and "), 2);
    CHECK_EQUAL(find_count(and_nested_description, column_names[2]), 1);

    std::string not_simple_and_description = queries->at(7).get_description();
    CHECK_EQUAL(find_count(not_simple_and_description, and_description), 1);
    CHECK_EQUAL(find_count(not_simple_and_description, not_text), 1);

    std::string not_simple_or_description = queries->at(8).get_description();
    CHECK_EQUAL(find_count(not_simple_or_description, or_description), 1);
    CHECK_EQUAL(find_count(not_simple_or_description, not_text), 1);

    std::string not_or_and_description = queries->at(9).get_description();
    CHECK_EQUAL(find_count(not_or_and_description, or_and_description), 1);
    CHECK_EQUAL(find_count(not_or_and_description, not_text), 1);

    std::string not_and_or_description = queries->at(10).get_description();
    CHECK_EQUAL(find_count(not_and_or_description, and_or_description), 1);
    CHECK_EQUAL(find_count(not_and_or_description, not_text), 1);

    std::string not_or_nested_description = queries->at(11).get_description();
    CHECK_EQUAL(find_count(not_or_nested_description, or_nested_description), 1);
    CHECK_EQUAL(find_count(not_or_nested_description, not_text), 1);

    std::string not_and_nested_description = queries->at(12).get_description();
    CHECK_EQUAL(find_count(not_and_nested_description, and_nested_description), 1);
    CHECK_EQUAL(find_count(not_and_nested_description, not_text), 1);

    std::string and_true_description = queries->at(13).get_description();
    CHECK_EQUAL(find_count(and_true_description, "and"), 1);
    CHECK_EQUAL(find_count(and_true_description, "TRUEPREDICATE"), 1);

    std::string and_false_description = queries->at(14).get_description();
    CHECK_EQUAL(find_count(and_false_description, "and"), 1);
    CHECK_EQUAL(find_count(and_false_description, "FALSEPREDICATE"), 1);
}


TEST(Metrics_LinkQueries)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBOptions options(crypt_key());
    options.enable_metrics = true;
    DBRef sg = DB::create(*hist, options);
    populate(sg);

    std::string person_table_name = "person";
    std::string pet_table_name = "pet";

    auto wt = sg->start_write();
    TableRef person = wt->get_table(person_table_name);
    TableRef pet = wt->get_table(pet_table_name);
    CHECK(bool(person));

    CHECK_EQUAL(person->get_column_count(), 8);
    std::vector<std::string> column_names;
    for (auto col : person->get_column_keys()) {
        column_names.push_back(person->get_column_name(col));
    }

    std::string pet_link_col_name = "owner";
    ColKey col_owner = pet->get_column_key(pet_link_col_name);
    auto col_age = person->get_column_key("age");

    Query q0 = pet->column<Link>(col_owner).is_null();
    Query q1 = pet->column<Link>(col_owner).is_not_null();
    Query q2 = pet->column<Link>(col_owner).count() == 1;
    Query q3 = pet->column<Link>(col_owner, person->column<int64_t>(col_age) >= 27).count() == 1;

    q0.find_all();
    q1.find_all();
    q2.find_all();
    q3.find_all();

    std::shared_ptr<Metrics> metrics = sg->get_metrics();
    CHECK(metrics);
    std::unique_ptr<Metrics::QueryInfoList> queries = metrics->take_queries();
    CHECK(queries);

    CHECK_EQUAL(queries->size(), 4);

    std::string null_links_description = queries->at(0).get_description();
    CHECK_EQUAL(find_count(null_links_description, "NULL"), 1);
    CHECK_EQUAL(find_count(null_links_description, pet_link_col_name), 1);

    std::string not_null_links_description = queries->at(1).get_description();
    CHECK_EQUAL(find_count(not_null_links_description, "NULL"), 1);
    CHECK_EQUAL(find_count(not_null_links_description, "!"), 1);
    CHECK_EQUAL(find_count(not_null_links_description, pet_link_col_name), 1);

    std::string count_link_description = queries->at(2).get_description();
    CHECK_EQUAL(find_count(count_link_description, "@count"), 1);
    CHECK_EQUAL(find_count(count_link_description, pet_link_col_name), 1);
    CHECK_EQUAL(find_count(count_link_description, "=="), 1);

    std::string link_subquery_description = queries->at(3).get_description();
    CHECK_EQUAL(find_count(link_subquery_description, "@count"), 1);
    CHECK_EQUAL(find_count(link_subquery_description, pet_link_col_name), 1);
    CHECK_EQUAL(find_count(link_subquery_description, "=="), 1);
    CHECK_EQUAL(find_count(link_subquery_description, column_names[0]), 1);
    CHECK_EQUAL(find_count(link_subquery_description, ">"), 1);
}


TEST(Metrics_LinkListQueries)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBOptions options(crypt_key());
    options.enable_metrics = true;
    DBRef sg = DB::create(*hist, options);
    populate(sg);

    std::string person_table_name = "person";
    std::string pet_table_name = "pet";

    auto wt = sg->start_write();
    TableRef person = wt->get_table(person_table_name);
    TableRef pet = wt->get_table(pet_table_name);
    CHECK(bool(person));

    CHECK_EQUAL(person->get_column_count(), 8);
    std::map<ColKey, std::string> column_names;
    for (auto col : person->get_column_keys()) {
        column_names[col] = person->get_column_name(col);
    }

    Obj p0 = person->get_object(0);

    ColKey ll_col_key = person->get_column_key("owes_coffee_to");
    auto col_name = person->get_column_key("name");
    auto col_paid = person->get_column_key("paid");

    Query q0 = person->column<Link>(ll_col_key).is_null();
    Query q1 = person->column<Link>(ll_col_key).is_not_null();
    Query q2 = person->column<Link>(ll_col_key).count() == 1;
    Query q3 = person->column<Link>(ll_col_key) == p0;
    Query q4 = person->column<Link>(ll_col_key).column<double>(col_paid).sum() >= 1;
    Query q5 = person->column<Link>(ll_col_key, person->column<String>(col_name) == "Bob").count() == 1;

    q0.find_all();
    q1.find_all();
    q2.find_all();
    q3.find_all();
    q4.find_all();
    q5.find_all();

    std::shared_ptr<Metrics> metrics = sg->get_metrics();
    CHECK(metrics);
    std::unique_ptr<Metrics::QueryInfoList> queries = metrics->take_queries();
    CHECK(queries);

    CHECK_EQUAL(queries->size(), 6);

    std::string null_links_description = queries->at(0).get_description();
    CHECK_EQUAL(find_count(null_links_description, "NULL"), 1);
    CHECK_EQUAL(find_count(null_links_description, column_names[ll_col_key]), 1);

    std::string not_null_links_description = queries->at(1).get_description();
    CHECK_EQUAL(find_count(not_null_links_description, "NULL"), 1);
    CHECK_EQUAL(find_count(not_null_links_description, "!"), 1);
    CHECK_EQUAL(find_count(not_null_links_description, column_names[ll_col_key]), 1);

    std::string count_link_description = queries->at(2).get_description();
    CHECK_EQUAL(find_count(count_link_description, "@count"), 1);
    CHECK_EQUAL(find_count(count_link_description, column_names[ll_col_key]), 1);
    CHECK_EQUAL(find_count(count_link_description, "=="), 1);

    std::string links_description = queries->at(3).get_description();
    CHECK_EQUAL(find_count(links_description, "O0"), 1);
    CHECK_EQUAL(find_count(links_description, column_names[ll_col_key]), 1);
    CHECK_EQUAL(find_count(links_description, "=="), 1);

    std::string sum_link_description = queries->at(4).get_description();
    CHECK_EQUAL(find_count(sum_link_description, "@sum"), 1);
    CHECK_EQUAL(find_count(sum_link_description, column_names[ll_col_key]), 1);
    CHECK_EQUAL(find_count(sum_link_description, column_names[col_paid]), 1);
    // the query system can choose to flip the argument order and operators so that >= might be <=
    CHECK_EQUAL(find_count(sum_link_description, "<=") + find_count(sum_link_description, ">="), 1);

    std::string link_subquery_description = queries->at(5).get_description();
    CHECK_EQUAL(find_count(link_subquery_description, "@count"), 1);
    CHECK_EQUAL(find_count(link_subquery_description, column_names[ll_col_key]), 1);
    CHECK_EQUAL(find_count(link_subquery_description, "=="), 2);
    CHECK_EQUAL(find_count(link_subquery_description, column_names[col_name]), 1);
}


TEST(Metrics_SubQueries)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBOptions options(crypt_key());
    options.enable_metrics = true;
    DBRef sg = DB::create(*hist, options);

    auto wt = sg->start_write();

    std::string table_name = "table";
    std::string int_col_name = "integers";
    std::string str_col_name = "strings";

    TableRef table = wt->add_table(table_name);

    auto col_list_int = table->add_column_list(type_Int, int_col_name);
    auto col_list_string = table->add_column_list(type_String, str_col_name);
    auto col_other = table->add_column(type_String, "other");

    std::vector<ObjKey> keys;
    table->create_objects(4, keys);

    // see Query_SubtableExpression
    auto set_int_list = [](LstPtr<Int> list, const std::vector<int64_t>& value_list) {
        list->clear();
        for (auto i : value_list) {
            list->add(i);
        }
    };
    auto set_string_list = [](LstPtr<String> list, const std::vector<int64_t>& value_list) {
        list->clear();
        for (auto i : value_list) {
            if (i < 100) {
                std::string str("Str_");
                str += util::to_string(i);
                list->add(StringData(str));
            }
            else {
                list->add(StringData());
            }
        }
    };
    set_int_list(table->get_object(keys[0]).get_list_ptr<Int>(col_list_int), std::vector<Int>({0, 1}));
    set_int_list(table->get_object(keys[1]).get_list_ptr<Int>(col_list_int), std::vector<Int>({2, 3, 4, 5}));
    set_int_list(table->get_object(keys[2]).get_list_ptr<Int>(col_list_int), std::vector<Int>({6, 7, 8, 9}));
    set_int_list(table->get_object(keys[3]).get_list_ptr<Int>(col_list_int), std::vector<Int>({}));

    set_string_list(table->get_object(keys[0]).get_list_ptr<String>(col_list_string), std::vector<Int>({0, 1}));
    set_string_list(table->get_object(keys[1]).get_list_ptr<String>(col_list_string), std::vector<Int>({2, 3, 4, 5}));
    set_string_list(table->get_object(keys[2]).get_list_ptr<String>(col_list_string),
                    std::vector<Int>({6, 7, 100, 8, 9}));
    table->get_object(keys[0]).set(col_other, StringData("foo"));
    table->get_object(keys[1]).set(col_other, StringData("str"));
    table->get_object(keys[2]).set(col_other, StringData("str_9_baa"));

    Query q0 = table->column<Lst<Int>>(col_list_int) == 10;
    Query q1 = table->column<Lst<Int>>(col_list_int).max() > 5;
    Query q2 = table->column<Lst<String>>(col_list_string).begins_with("Str");
    Query q3 = table->column<Lst<String>>(col_list_string) == "Str_0";

    CHECK_THROW(q0.find_all(), SerialisationError);
    CHECK_THROW(q1.find_all(), SerialisationError);
    CHECK_THROW(q2.find_all(), SerialisationError);
    CHECK_THROW(q3.find_all(), SerialisationError);

    wt->commit();
    /*
    std::shared_ptr<Metrics> metrics = sg->get_metrics();
    CHECK(metrics);
    std::unique_ptr<Metrics::QueryInfoList> queries = metrics->take_queries();
    CHECK(queries);

    CHECK_EQUAL(queries->size(), 4);

    std::string int_equal_description = queries->at(0).get_description();
    CHECK_EQUAL(find_count(int_equal_description, "=="), 1);
    CHECK_EQUAL(find_count(int_equal_description, int_col_name), 1);

    std::string int_max_description = queries->at(1).get_description();
    CHECK_EQUAL(find_count(int_max_description, "@max"), 1);
    CHECK_EQUAL(find_count(int_max_description, int_col_name), 1);

    std::string str_begins_description = queries->at(2).get_description();
    CHECK_EQUAL(find_count(str_begins_description, "BEGINSWITH"), 1);
    CHECK_EQUAL(find_count(str_begins_description, str_col_name), 1);

    std::string str_equal_description = queries->at(3).get_description();
    CHECK_EQUAL(find_count(str_equal_description, "=="), 1);
    CHECK_EQUAL(find_count(str_equal_description, str_col_name), 1);
    */
}

TEST(Metrics_TransactionTimings)
{
    ColKey col;
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBOptions options(crypt_key());
    options.enable_metrics = true;
    DBRef sg = DB::create(*hist, options);
    CHECK(sg->get_metrics());
    {
        auto wt = sg->start_write();
        auto table = wt->add_table("table");
        col = table->add_column(type_Int, "first");
        std::vector<ObjKey> keys;
        table->create_objects(10, keys);
        wt->commit();
    }
    {
        auto rt = sg->start_read();
        auto table = rt->get_table("table");
        CHECK(bool(table));
        Query query = table->column<int64_t>(col) == 0;
        query.count();
        rt->end_read();
    }

    using namespace std::literals::chrono_literals;
    {
        ReadTransaction rt(sg);
        std::this_thread::sleep_for(60ms);
    }
    {
        WriteTransaction wt(sg);
        TableRef t = wt.get_table("table");
        t->create_object();
        std::this_thread::sleep_for(80ms);
        wt.commit();
    }
    std::shared_ptr<Metrics> metrics = sg->get_metrics();
    CHECK(metrics);
    CHECK_NOT_EQUAL(metrics->num_query_metrics(), 0);
    CHECK_NOT_EQUAL(metrics->num_transaction_metrics(), 0);

    std::unique_ptr<Metrics::TransactionInfoList> transactions = metrics->take_transactions();
    CHECK(transactions);
    CHECK_EQUAL(metrics->num_transaction_metrics(), 0);

    CHECK_EQUAL(transactions->size(), 4);

    for (auto t : *transactions) {
        CHECK_GREATER(t.get_transaction_time(), 0);

        if (t.get_transaction_type() == TransactionInfo::read_transaction) {
            CHECK_EQUAL(t.get_fsync_time(), 0.0);
            CHECK_EQUAL(t.get_write_time(), 0.0);
        }
        else {
            if (!get_disable_sync_to_disk()) {
                CHECK_NOT_EQUAL(t.get_fsync_time(), 0.0);
            }
            CHECK_NOT_EQUAL(t.get_write_time(), 0.0);
            CHECK_LESS(t.get_fsync_time(), t.get_transaction_time());
            CHECK_LESS(t.get_write_time(), t.get_transaction_time());
        }
    }
    // give a margin of 100ms for transactions
    // this is causing sporadic CI failures so best not to assume any upper bound
    CHECK_GREATER(transactions->at(2).get_transaction_time(), 0.060);
    //CHECK_LESS(transactions->at(2).get_transaction_time(), 0.160);
    CHECK_GREATER(transactions->at(3).get_transaction_time(), 0.080);
    //CHECK_LESS(transactions->at(3).get_transaction_time(), 0.180);
}


TEST(Metrics_TransactionData)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBOptions options(crypt_key());
    options.enable_metrics = true;
    DBRef sg = DB::create(*hist, options);
    populate(sg);

    {
        ReadTransaction rt(sg);
    }
    {
        auto wt = sg->start_write();
        auto table_keys = wt->get_table_keys();
        TableRef t0 = wt->get_table(table_keys[0]);
        TableRef t1 = wt->get_table(table_keys[1]);
        std::vector<ObjKey> keys;
        t0->create_objects(3, keys);
        t1->create_objects(7, keys);
        wt->commit();
    }

    std::shared_ptr<Metrics> metrics = sg->get_metrics();
    CHECK(metrics);

    std::unique_ptr<Metrics::TransactionInfoList> transactions = metrics->take_transactions();
    CHECK(transactions);
    CHECK_EQUAL(metrics->num_transaction_metrics(), 0);

    CHECK_EQUAL(transactions->size(), 3);

    CHECK_EQUAL(transactions->at(0).get_total_objects(), 11);
    CHECK_EQUAL(transactions->at(1).get_total_objects(), 11);
    CHECK_EQUAL(transactions->at(2).get_total_objects(), 11 + 3 + 7);
}

TEST(Metrics_TransactionVersions)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBOptions options(crypt_key());
    options.enable_metrics = true;
    DBRef sg = DB::create(*hist, options);
    populate(sg);
    const size_t num_writes_while_pinned = 10;
    TableKey tk0;
    TableKey tk1;
    {
        auto rt = sg->start_read();
        auto table_keys = rt->get_table_keys();
        tk0 = table_keys[0];
        tk1 = table_keys[1];
    }
    {
        auto wt = sg->start_write();
        TableRef t0 = wt->get_table(tk0);
        TableRef t1 = wt->get_table(tk1);
        std::vector<ObjKey> keys;
        t0->create_objects(3, keys);
        t1->create_objects(7, keys);
        wt->commit();
    }
    {
        std::unique_ptr<Replication> hist2(make_in_realm_history(path));
        DBRef sg2 = DB::create(*hist2, options);

        // Pin this version. Note that since this read transaction is against a different shared group
        // it doesn't get tracked in the transaction metrics of the original shared group.
        ReadTransaction rt(sg2);

        for (size_t i = 0; i < num_writes_while_pinned; ++i) {
            auto wt = sg->start_write();
            TableRef t0 = wt->get_table(tk0);
            t0->create_object();
            wt->commit();
        }
    }

    std::shared_ptr<Metrics> metrics = sg->get_metrics();
    CHECK(metrics);

    std::unique_ptr<Metrics::TransactionInfoList> transactions = metrics->take_transactions();
    CHECK(transactions);
    CHECK_EQUAL(metrics->num_transaction_metrics(), 0);

    CHECK_EQUAL(transactions->size(), 3 + num_writes_while_pinned);

    CHECK_EQUAL(transactions->at(0).get_num_available_versions(), 2);
    CHECK_EQUAL(transactions->at(1).get_num_available_versions(), 2);
    CHECK_EQUAL(transactions->at(2).get_num_available_versions(), 2);

    for (size_t i = 0; i < num_writes_while_pinned; ++i) {
        CHECK_EQUAL(transactions->at(3 + i).get_num_available_versions(), 2 + i);
    }
}

TEST(Metrics_MaxNumTransactionsIsNotExceeded)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBOptions options(crypt_key());
    options.enable_metrics = true;
    options.metrics_buffer_size = 10;
    auto sg = DB::create(*hist, options);
    populate(sg); // 1
    {
        ReadTransaction rt(sg); // 2
    }
    {
        WriteTransaction wt(sg); // 3
        TableRef t0 = wt.get_table("person");
        TableRef t1 = wt.get_table("pet");
        for (int i = 0; i < 3; i++) {
            t0->create_object();
        }
        for (int i = 0; i < 7; i++) {
            t1->create_object();
        }
        wt.commit();
    }

    for (size_t i = 0; i < options.metrics_buffer_size; ++i) {
        ReadTransaction rt(sg);
    }

    std::shared_ptr<Metrics> metrics = sg->get_metrics();
    CHECK(metrics);

    CHECK_EQUAL(metrics->num_query_metrics(), 0);
    CHECK_EQUAL(metrics->num_transaction_metrics(), options.metrics_buffer_size);
    std::unique_ptr<Metrics::TransactionInfoList> transactions = metrics->take_transactions();
    CHECK(transactions);
    for (auto transaction : *transactions) {
        CHECK_EQUAL(transaction.get_transaction_type(),
                    realm::metrics::TransactionInfo::TransactionType::read_transaction);
    }
}

TEST(Metrics_MaxNumQueriesIsNotExceeded)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBOptions options(crypt_key());
    options.enable_metrics = true;
    options.metrics_buffer_size = 10;
    auto sg = DB::create(*hist, options);

    {
        auto tr = sg->start_write();
        auto table = tr->add_table("table");
        table->add_column(type_Int, "col_int");
        for (int i = 0; i < 10; i++) {
            table->create_object();
        }
        tr->commit();
    }

    {
        auto rt = sg->start_read();
        auto table = rt->get_table("table");
        auto int_col = table->get_column_key("col_int");
        CHECK(bool(table));
        Query query = table->column<int64_t>(int_col) == 0;
        for (size_t i = 0; i < 2 * options.metrics_buffer_size; ++i) {
            query.find();
        }
    }
    std::shared_ptr<Metrics> metrics = sg->get_metrics();
    CHECK(metrics);
    CHECK_EQUAL(metrics->num_query_metrics(), options.metrics_buffer_size);
}

// The number of decrypted pages is updated periodically by the governor.
// To test this, we need our own governor implementation which does not reclaim pages but runs at least once.
class NoPageReclaimGovernor : public realm::util::PageReclaimGovernor {
public:
    NoPageReclaimGovernor()
    {
        has_run_once = will_run.get_future();
    }
    int64_t get_current_target(size_t)
    {
        will_run.set_value();
        return no_match;
    }
    std::future<void> has_run_once;
    std::promise<void> will_run;
};

// this test relies on the global state of the number of decrypted pages and therefore must be run in isolation
NONCONCURRENT_TEST(Metrics_NumDecryptedPagesWithoutEncryption)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBOptions options(nullptr);
    options.enable_metrics = true;
    options.metrics_buffer_size = 10;
    auto sg = DB::create(*hist, options);

    {
        auto tr = sg->start_write();
        auto table = tr->add_table("table");

        // we need this here because other unit tests might be using encryption and we need a guarantee
        // that the global pages are from this shared group only.
        NoPageReclaimGovernor gov;
        realm::util::set_page_reclaim_governor(&gov);
        CHECK(gov.has_run_once.valid());
        gov.has_run_once.wait_for(std::chrono::seconds(2));

        tr->commit();
    }

    {
        auto rt = sg->start_read();
    }

    std::shared_ptr<Metrics> metrics = sg->get_metrics();
    CHECK(metrics);

    CHECK_EQUAL(metrics->num_transaction_metrics(), 2);
    std::unique_ptr<Metrics::TransactionInfoList> transactions = metrics->take_transactions();
    CHECK(transactions);
    CHECK_EQUAL(transactions->size(), 2);
    CHECK_EQUAL(transactions->at(0).get_transaction_type(),
                realm::metrics::TransactionInfo::TransactionType::write_transaction);
    CHECK_EQUAL(transactions->at(0).get_num_decrypted_pages(), 0);
    CHECK_EQUAL(transactions->at(1).get_transaction_type(),
                realm::metrics::TransactionInfo::TransactionType::read_transaction);
    CHECK_EQUAL(transactions->at(1).get_num_decrypted_pages(), 0);

    realm::util::set_page_reclaim_governor_to_default(); // the remainder of the test suite should use the default
}

// this test relies on the global state of the number of decrypted pages and therefore must be run in isolation
NONCONCURRENT_TEST_IF(Metrics_NumDecryptedPagesWithEncryption, REALM_ENABLE_ENCRYPTION)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBOptions options(crypt_key(true));
    options.enable_metrics = true;
    options.metrics_buffer_size = 10;
    auto sg = DB::create(*hist, options);

    {
        auto tr = sg->start_write();
        auto table = tr->add_table("table");

        NoPageReclaimGovernor gov;
        realm::util::set_page_reclaim_governor(&gov);
        CHECK(gov.has_run_once.valid());
        gov.has_run_once.wait_for(std::chrono::seconds(2));

        tr->commit();
    }

    {
        auto rt = sg->start_read();
    }

    std::shared_ptr<Metrics> metrics = sg->get_metrics();
    CHECK(metrics);

    CHECK_EQUAL(metrics->num_transaction_metrics(), 2);
    std::unique_ptr<Metrics::TransactionInfoList> transactions = metrics->take_transactions();
    CHECK(transactions);
    CHECK_EQUAL(transactions->size(), 2);
    CHECK_EQUAL(transactions->at(0).get_transaction_type(),
                realm::metrics::TransactionInfo::TransactionType::write_transaction);
    CHECK_EQUAL(transactions->at(0).get_num_decrypted_pages(), 1);
    CHECK_EQUAL(transactions->at(1).get_transaction_type(),
                realm::metrics::TransactionInfo::TransactionType::read_transaction);
    CHECK_EQUAL(transactions->at(1).get_num_decrypted_pages(), 1);

    realm::util::set_page_reclaim_governor_to_default(); // the remainder of the test suite should use the default
}

TEST(Metrics_MemoryChecks)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBOptions options(nullptr);
    options.enable_metrics = true;
    options.metrics_buffer_size = 10;
    auto sg = DB::create(*hist, options);
    populate(sg);

    {
        auto rt = sg->start_read();
    }

    std::shared_ptr<Metrics> metrics = sg->get_metrics();
    CHECK(metrics);

    CHECK_EQUAL(metrics->num_transaction_metrics(), 2);
    std::unique_ptr<Metrics::TransactionInfoList> transactions = metrics->take_transactions();
    CHECK(transactions);

    for (auto transaction : *transactions) {
        CHECK_GREATER(transaction.get_disk_size(), 0);
        CHECK_GREATER(transaction.get_free_space(), 0);
    }
}

#endif // REALM_METRICS
#endif // TEST_METRICS
