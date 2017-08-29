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

#include <realm/descriptor.hpp>
#include <realm/query_expression.hpp>
#include <realm/lang_bind_helper.hpp>
#include <realm/util/encrypted_file_mapping.hpp>
#include <realm/util/to_string.hpp>
#include <realm/replication.hpp>
#include <realm/history.hpp>

using namespace realm;
using namespace realm::metrics;
using namespace realm::test_util;
using namespace realm::util;

TEST(Metrics_HasNoReportsWhenDisabled)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    SharedGroupOptions options(crypt_key());
    options.enable_metrics = false;
    SharedGroup sg(*hist, options);
    CHECK(!sg.get_metrics());
    Group& g = sg.begin_write();
    auto table = g.add_table("table");
    table->add_column(type_Int, "first");
    table->add_empty_row(10);
    sg.commit();
    sg.begin_read();
    table = g.get_table("table");
    CHECK(bool(table));
    Query query = table->column<int64_t>(0) == 0;
    query.count();
    sg.end_read();
    CHECK(!sg.get_metrics());
}

TEST(Metrics_HasReportsWhenEnabled)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    SharedGroupOptions options(crypt_key());
    options.enable_metrics = true;
    SharedGroup sg(*hist, options);
    CHECK(sg.get_metrics());
    Group& g = sg.begin_write();
    auto table = g.add_table("table");
    table->add_column(type_Int, "first");
    table->add_empty_row(10);
    sg.commit();
    sg.begin_read();
    table = g.get_table("table");
    CHECK(bool(table));
    Query query = table->column<int64_t>(0) == 0;
    query.count();
    sg.end_read();
    std::shared_ptr<Metrics> metrics = sg.get_metrics();
    CHECK(metrics);
    CHECK(metrics->num_query_metrics() != 0);
    //CHECK(metrics->num_transaction_metrics() != 0);
}

TEST(Metrics_QueryTypes)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    SharedGroupOptions options(crypt_key());
    options.enable_metrics = true;
    SharedGroup sg(*hist, options);
    CHECK(sg.get_metrics());
    Group& g = sg.begin_write();
    auto table = g.add_table("table");
    size_t int_col = table->add_column(type_Int, "col_int");
    size_t double_col = table->add_column(type_Double, "col_double");
    size_t float_col = table->add_column(type_Float, "col_float");
    size_t timestamp_col = table->add_column(type_Timestamp, "col_timestamp");
    table->add_empty_row(10);
    sg.commit();
    sg.begin_read();
    table = g.get_table("table");
    CHECK(bool(table));
    Query query = table->column<int64_t>(0) == 0;
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

    size_t return_dummy;
    query.maximum_timestamp(timestamp_col, &return_dummy);
    query.minimum_timestamp(timestamp_col, &return_dummy);

    sg.end_read();
    std::shared_ptr<Metrics> metrics = sg.get_metrics();
    CHECK(metrics);
    CHECK(metrics->num_query_metrics() == 17);
    std::unique_ptr<Metrics::QueryInfoList> queries = metrics->take_queries();
    CHECK(metrics->num_query_metrics() == 0);
    CHECK(queries && queries->size() == 17);
    CHECK(queries->at(0).get_type() == QueryInfo::type_Find);
    CHECK(queries->at(1).get_type() == QueryInfo::type_FindAll);
    CHECK(queries->at(2).get_type() == QueryInfo::type_Count);
    CHECK(queries->at(3).get_type() == QueryInfo::type_Sum);
    CHECK(queries->at(4).get_type() == QueryInfo::type_Average);
    CHECK(queries->at(5).get_type() == QueryInfo::type_Maximum);
    CHECK(queries->at(6).get_type() == QueryInfo::type_Minimum);

    CHECK(queries->at(7).get_type() == QueryInfo::type_Sum);
    CHECK(queries->at(8).get_type() == QueryInfo::type_Average);
    CHECK(queries->at(9).get_type() == QueryInfo::type_Maximum);
    CHECK(queries->at(10).get_type() == QueryInfo::type_Minimum);

    CHECK(queries->at(11).get_type() == QueryInfo::type_Sum);
    CHECK(queries->at(12).get_type() == QueryInfo::type_Average);
    CHECK(queries->at(13).get_type() == QueryInfo::type_Maximum);
    CHECK(queries->at(14).get_type() == QueryInfo::type_Minimum);

    CHECK(queries->at(15).get_type() == QueryInfo::type_Maximum);
    CHECK(queries->at(16).get_type() == QueryInfo::type_Minimum);
}


#endif // REALM_METRICS
#endif // TEST_METRICS
