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
#ifdef TEST_QUERY

#include <realm.hpp>
#include <realm/history.hpp>

#include "test.hpp"
#include "test_table_helper.hpp"

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

// FIXME: Realign this to refer to a Transaction instead of a DB (ex SharedGroup)
namespace {
struct QueryInitHelper;

// Test a whole bunch of various permutations of operations on every query node
// type. This is done in a somewhat ridiculous CPS style to ensure complete
// control over when the Query instances are copied.
struct PreRun {
    template <typename Next>
    auto operator()(Query& q, Next&& next)
    {
        q.count();
        return next(q);
    }
};
struct CopyQuery {
    template <typename Next>
    auto operator()(Query& q, Next&& next)
    {
        Query copy(q);
        return next(copy);
    }
};
struct AndQuery {
    template <typename Next>
    auto operator()(Query& q, Next&& next)
    {
        return next(q.get_table()->where().and_query(q));
    }
};
struct HandoverQuery {
    template <typename Next>
    size_t operator()(Query& q, Next&& next)
    {
        auto old_tr = next.state.rt;
        size_t ret;

        {
            // Hand over the query to a new transaction and continue processing on that
            auto new_transaction = next.state.sg->start_read(old_tr->get_version_of_current_transaction());
            auto copy = new_transaction->import_copy_of(q, PayloadPolicy::Copy);
            next.state.rt = new_transaction;

            ret = next(*copy);

            new_transaction->end_read();
        }

        // Restore the old state
        next.state.rt = old_tr;

        return ret;
    }
};
struct SelfHandoverQuery {
    template <typename Next>
    auto operator()(Query& q, Next&& next)
    {
        // Export the query and then re-import it to the same SG
        return next(*next.state.rt->import_copy_of(q, PayloadPolicy::Copy));
    }
};
struct InsertColumn {
    template <typename Next>
    auto operator()(Query& q, Next&& next)
    {
        next.state.rt->advance_read();
        return next(q);
    }
};
struct GetCount {
    auto operator()(Query& q)
    {
        auto cnt = q.count();
#if 0
        try {
            std::cout << q.get_description() << " -> " << cnt << std::endl;
        }
        catch (const std::exception& e) {
            std::cout << e.what() << std::endl;
        }
#endif
        return cnt;
    }
};

template <typename Func, typename... Rest>
struct Compose {
    QueryInitHelper& state;
    auto operator()(Query& q)
    {
        return Func()(q, Compose<Rest...>{state});
    }
};

template <typename Func>
struct Compose<Func> {
    QueryInitHelper& state;
    auto operator()(Query& q)
    {
        return Func()(q);
    }
};

struct QueryInitHelper {
    test_util::unit_test::TestContext& test_context;
    DBRef sg;
    TransactionRef rt;
    DB::VersionID initial_version, extra_col_version;

    Table& get_table()
    {
        return *rt->get_table(TableKey(0));
    }
    template <typename Func>
    REALM_NOINLINE void operator()(Func&& fn);

    template <typename Func, typename... Mutations>
    REALM_NOINLINE size_t run(Func& fn);
};

template <typename Func>
void QueryInitHelper::operator()(Func&& fn)
{
    // get baseline result with no copies
    size_t count = run(fn);
    CHECK_EQUAL(count, (run<Func, InsertColumn>(fn)));
    CHECK_EQUAL(count, (run<Func, PreRun, InsertColumn>(fn)));

    // copy the query, then run
    CHECK_EQUAL(count, (run<Func, CopyQuery>(fn)));
    CHECK_EQUAL(count, (run<Func, AndQuery>(fn)));
    CHECK_EQUAL(count, (run<Func, HandoverQuery>(fn)));
    CHECK_EQUAL(count, (run<Func, SelfHandoverQuery>(fn)));

    // run, copy the query, rerun
    CHECK_EQUAL(count, (run<Func, PreRun, CopyQuery>(fn)));
    CHECK_EQUAL(count, (run<Func, PreRun, AndQuery>(fn)));
    CHECK_EQUAL(count, (run<Func, PreRun, HandoverQuery>(fn)));
    CHECK_EQUAL(count, (run<Func, PreRun, SelfHandoverQuery>(fn)));

    // copy the query, insert column, then run
    CHECK_EQUAL(count, (run<Func, CopyQuery, InsertColumn>(fn)));
    CHECK_EQUAL(count, (run<Func, AndQuery, InsertColumn>(fn)));
    CHECK_EQUAL(count, (run<Func, HandoverQuery, InsertColumn>(fn)));

    // run, copy the query, insert column, rerun
    CHECK_EQUAL(count, (run<Func, PreRun, CopyQuery, InsertColumn>(fn)));
    CHECK_EQUAL(count, (run<Func, PreRun, AndQuery, InsertColumn>(fn)));
    CHECK_EQUAL(count, (run<Func, PreRun, HandoverQuery, InsertColumn>(fn)));

    // insert column, copy the query, then run
    CHECK_EQUAL(count, (run<Func, InsertColumn, CopyQuery>(fn)));
    CHECK_EQUAL(count, (run<Func, InsertColumn, AndQuery>(fn)));
    CHECK_EQUAL(count, (run<Func, InsertColumn, HandoverQuery>(fn)));

    // run, insert column, copy the query, rerun
    CHECK_EQUAL(count, (run<Func, PreRun, InsertColumn, CopyQuery>(fn)));
    CHECK_EQUAL(count, (run<Func, PreRun, InsertColumn, AndQuery>(fn)));
    CHECK_EQUAL(count, (run<Func, PreRun, InsertColumn, HandoverQuery>(fn)));
}

template <typename Func, typename... Mutations>
size_t QueryInitHelper::run(Func& fn)
{
    rt = sg->start_read(initial_version);
    auto table = rt->get_table(TableKey(0));
    size_t count;
    Query query = table->where();
    fn(query, [&](auto&& q2) { count = Compose<Mutations..., GetCount>{*this}(q2); });
    rt->end_read();
    return count;
}
} // anonymous namespace

// Test that queries properly bind to their tables and columns by constructing
// a query, maybe copying it in one of several ways, inserting a column at the
// beginning of the table, and then rerunning the query
TEST(Query_TableInitialization)
{
    SHARED_GROUP_TEST_PATH(path);

    auto repl = make_in_realm_history(path);
    DBRef sg = DB::create(*repl, DBOptions(DBOptions::Durability::MemOnly));
    auto wt = sg->start_write();

    DB::VersionID initial_version, extra_col_version;

    Table& table = *wt->add_table("table");
    // The columns are ordered to avoid having types which are backed by the
    // same implementation column type next to each other so that being
    // off-by-one doesn't work by coincidence
    ColKey col_dummy = table.add_column(type_Double, "dummy");
    ColKey col_int = table.add_column(type_Int, "int");
    ColKey col_float = table.add_column(type_Float, "float");
    ColKey col_bool = table.add_column(type_Bool, "bool");
    ColKey col_link = table.add_column_link(type_Link, "link", table);
    ColKey col_string_enum = table.add_column(type_String, "string enum");
    // FIXME table.optimize();
    ColKey col_double = table.add_column(type_Double, "double");
    ColKey col_string = table.add_column(type_String, "string");
    ColKey col_list = table.add_column_link(type_LinkList, "list", table);
    ColKey col_binary = table.add_column(type_Binary, "binary");
    ColKey col_timestamp = table.add_column(type_Timestamp, "timestamp");
    ColKey col_string_indexed = table.add_column(type_String, "indexed string");

    ColKey col_int_null = table.add_column(type_Int, "int_null", true);
    ColKey col_float_null = table.add_column(type_Float, "float_null", true);
    ColKey col_bool_null = table.add_column(type_Bool, "bool_null", true);
    ColKey col_double_null = table.add_column(type_Double, "double_null", true);
    ColKey col_string_null = table.add_column(type_String, "string_null", true);
    ColKey col_binary_null = table.add_column(type_Binary, "binary_null", true);
    ColKey col_timestamp_null = table.add_column(type_Timestamp, "timestamp_null", true);

    ColKey col_list_int = table.add_column_list(type_Int, "integers");

    std::string str(5, 'z');
    std::vector<ObjKey> keys;
    table.create_objects(20, keys);
    for (size_t i = 0; i < 10; ++i) {
        Obj obj = table.get_object(keys[i]);
        obj.set(col_binary, BinaryData(str), false);
        obj.set(col_link, keys[i]);
        obj.set(col_timestamp, Timestamp{int(i), int(i)});
        obj.get_linklist(col_list).add(keys[i]);
        obj.get_list<Int>(col_list_int).add(i);
    }
    Obj obj0 = table.get_object(keys[0]);
    wt->commit_and_continue_as_read();

    // Save this version so we can go back to it before every test
    initial_version = wt->get_version_of_current_transaction();
    auto dummy = wt->duplicate(); // Pin version

    // Create a second version which has the extra column at the beginning
    // of the table removed, so that anything which relies on stable column
    // numbers will use the wrong column after advancing
    wt->promote_to_write();
    table.remove_column(col_dummy);
    wt->commit_and_continue_as_read();

    extra_col_version = wt->get_version_of_current_transaction();
    wt->end_read();

    QueryInitHelper helper{test_context, sg, {}, initial_version, extra_col_version};

    // links_to
    helper([&](Query& q, auto&& test) { test(q.links_to(col_link, keys[0])); });
    helper([&](Query& q, auto&& test) { test(q.links_to(col_list, keys[0])); });
    helper([&](Query& q, auto&& test) { test(q.Not().links_to(col_link, keys[0])); });
    helper([&](Query& q, auto&& test) {
        auto it = q.get_table()->begin();
        ObjKey k0 = it->get_key();
        ++it;
        ObjKey k1 = it->get_key();
        test(q.links_to(col_link, k0).Or().links_to(col_link, k1));
    });

    // compare to null
    helper([&](Query& q, auto&& test) { test(q.equal(col_int_null, null{})); });
    helper([&](Query& q, auto&& test) { test(q.equal(col_float_null, null{})); });
    helper([&](Query& q, auto&& test) { test(q.equal(col_bool_null, null{})); });
    helper([&](Query& q, auto&& test) { test(q.equal(col_double_null, null{})); });
    helper([&](Query& q, auto&& test) { test(q.equal(col_string_null, null{})); });
    helper([&](Query& q, auto&& test) { test(q.equal(col_binary_null, null{})); });
    helper([&](Query& q, auto&& test) { test(q.equal(col_timestamp_null, null{})); });

    helper([&](Query& q, auto&& test) { test(q.not_equal(col_int_null, null{})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_float_null, null{})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_bool_null, null{})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_double_null, null{})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_string_null, null{})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_binary_null, null{})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_timestamp_null, null{})); });

    // Conditions: int64_t
    helper([&](Query& q, auto&& test) { test(q.equal(col_int, int64_t{})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_int, int64_t{})); });
    helper([&](Query& q, auto&& test) { test(q.greater(col_int, int64_t{})); });
    helper([&](Query& q, auto&& test) { test(q.greater_equal(col_int, int64_t{})); });
    helper([&](Query& q, auto&& test) { test(q.less(col_int, int64_t{})); });
    helper([&](Query& q, auto&& test) { test(q.less_equal(col_int, int64_t{})); });
    helper([&](Query& q, auto&& test) { test(q.between(col_int, int64_t{}, {})); });

    // Conditions: int
    helper([&](Query& q, auto&& test) { test(q.equal(col_int, int{})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_int, int{})); });
    helper([&](Query& q, auto&& test) { test(q.greater(col_int, int{})); });
    helper([&](Query& q, auto&& test) { test(q.greater_equal(col_int, int{})); });
    helper([&](Query& q, auto&& test) { test(q.less(col_int, int{})); });
    helper([&](Query& q, auto&& test) { test(q.less_equal(col_int, int{})); });
    helper([&](Query& q, auto&& test) { test(q.between(col_int, int{}, {})); });

    // Conditions: 2 int columns
    helper([&](Query& q, auto&& test) { test(q.equal_int(col_int, col_int)); });
    helper([&](Query& q, auto&& test) { test(q.not_equal_int(col_int, col_int)); });
    helper([&](Query& q, auto&& test) { test(q.greater_int(col_int, col_int)); });
    helper([&](Query& q, auto&& test) { test(q.less_int(col_int, col_int)); });
    helper([&](Query& q, auto&& test) { test(q.greater_equal_int(col_int, col_int)); });
    helper([&](Query& q, auto&& test) { test(q.less_equal_int(col_int, col_int)); });

    // Conditions: float
    helper([&](Query& q, auto&& test) { test(q.equal(col_float, float{})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_float, float{})); });
    helper([&](Query& q, auto&& test) { test(q.greater(col_float, float{})); });
    helper([&](Query& q, auto&& test) { test(q.greater_equal(col_float, float{})); });
    helper([&](Query& q, auto&& test) { test(q.less(col_float, float{})); });
    helper([&](Query& q, auto&& test) { test(q.less_equal(col_float, float{})); });
    helper([&](Query& q, auto&& test) { test(q.between(col_float, float{}, {})); });

    // Conditions: 2 float columns
    helper([&](Query& q, auto&& test) { test(q.equal_float(col_float, col_float)); });
    helper([&](Query& q, auto&& test) { test(q.not_equal_float(col_float, col_float)); });
    helper([&](Query& q, auto&& test) { test(q.greater_float(col_float, col_float)); });
    helper([&](Query& q, auto&& test) { test(q.greater_equal_float(col_float, col_float)); });
    helper([&](Query& q, auto&& test) { test(q.less_float(col_float, col_float)); });
    helper([&](Query& q, auto&& test) { test(q.less_equal_float(col_float, col_float)); });

    // Conditions: double
    helper([&](Query& q, auto&& test) { test(q.equal(col_double, double{})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_double, double{})); });
    helper([&](Query& q, auto&& test) { test(q.greater(col_double, double{})); });
    helper([&](Query& q, auto&& test) { test(q.greater_equal(col_double, double{})); });
    helper([&](Query& q, auto&& test) { test(q.less(col_double, double{})); });
    helper([&](Query& q, auto&& test) { test(q.less_equal(col_double, double{})); });
    helper([&](Query& q, auto&& test) { test(q.between(col_double, double{}, {})); });

    // Conditions: 2 double columns
    helper([&](Query& q, auto&& test) { test(q.equal_double(col_double, col_double)); });
    helper([&](Query& q, auto&& test) { test(q.not_equal_double(col_double, col_double)); });
    helper([&](Query& q, auto&& test) { test(q.greater_double(col_double, col_double)); });
    helper([&](Query& q, auto&& test) { test(q.greater_equal_double(col_double, col_double)); });
    helper([&](Query& q, auto&& test) { test(q.less_double(col_double, col_double)); });
    helper([&](Query& q, auto&& test) { test(q.less_equal_double(col_double, col_double)); });

    // Conditions: timestamp
    helper([&](Query& q, auto&& test) { test(q.equal(col_timestamp, Timestamp{5, 5})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_timestamp, Timestamp{5, 5})); });
    helper([&](Query& q, auto&& test) { test(q.greater(col_timestamp, Timestamp{5, 5})); });
    helper([&](Query& q, auto&& test) { test(q.greater_equal(col_timestamp, Timestamp{5, 5})); });
    helper([&](Query& q, auto&& test) { test(q.less_equal(col_timestamp, Timestamp{5, 5})); });
    helper([&](Query& q, auto&& test) { test(q.less(col_timestamp, Timestamp{5, 5})); });

    // Conditions: bool
    helper([&](Query& q, auto&& test) { test(q.equal(col_bool, bool{})); });

    // Conditions: strings
    helper([&](Query& q, auto&& test) { test(q.equal(col_string, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_string, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.begins_with(col_string, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.ends_with(col_string, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.contains(col_string, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.like(col_string, StringData{})); });

    helper([&](Query& q, auto&& test) { test(q.equal(col_string, StringData{}, false)); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_string, StringData{}, false)); });
    helper([&](Query& q, auto&& test) { test(q.begins_with(col_string, StringData{}, false)); });
    helper([&](Query& q, auto&& test) { test(q.ends_with(col_string, StringData{}, false)); });
    helper([&](Query& q, auto&& test) { test(q.contains(col_string, StringData{}, false)); });
    helper([&](Query& q, auto&& test) { test(q.like(col_string, StringData{}, false)); });

    helper([&](Query& q, auto&& test) { test(q.equal(col_string_enum, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_string_enum, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.begins_with(col_string_enum, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.ends_with(col_string_enum, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.contains(col_string_enum, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.like(col_string_enum, StringData{})); });

    helper([&](Query& q, auto&& test) { test(q.equal(col_string_indexed, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_string_indexed, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.begins_with(col_string_indexed, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.ends_with(col_string_indexed, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.contains(col_string_indexed, StringData{})); });
    helper([&](Query& q, auto&& test) { test(q.like(col_string_indexed, StringData{})); });

    // Conditions: binary data
    helper([&](Query& q, auto&& test) { test(q.equal(col_binary, BinaryData{})); });
    helper([&](Query& q, auto&& test) { test(q.not_equal(col_binary, BinaryData{})); });
    helper([&](Query& q, auto&& test) { test(q.begins_with(col_binary, BinaryData{})); });
    helper([&](Query& q, auto&& test) { test(q.ends_with(col_binary, BinaryData{})); });
    helper([&](Query& q, auto&& test) { test(q.contains(col_binary, BinaryData{})); });

    enum class Mode { Direct, Link, LinkList };

    // note: using std::function<> rather than auto&& here for the sake of compilation speed
    auto test_query_expression = [&](std::function<Table&()> get_table, Mode mode) {
        auto test_operator = [&](auto&& op, auto&& column, auto&& v) {
            if (mode != Mode::LinkList)
                helper([&](Query&, auto&& test) { test(op(column(), column())); });
            helper([&](Query&, auto&& test) { test(op(column(), v)); });
        };
        auto test_numeric = [&](auto value, ColKey col, ColKey null_col) {
            using Type = decltype(value);
            auto get_column = [&] { return get_table().template column<Type>(col); };
            test_operator(std::equal_to<>(), get_column, value);
            test_operator(std::not_equal_to<>(), get_column, value);
            test_operator(std::greater<>(), get_column, value);
            test_operator(std::less<>(), get_column, value);
            test_operator(std::greater_equal<>(), get_column, value);
            test_operator(std::less_equal<>(), get_column, value);

            auto get_null_column = [&] { return get_table().template column<Type>(null_col); };
            test_operator(std::equal_to<>(), get_null_column, null{});
            test_operator(std::not_equal_to<>(), get_null_column, null{});
        };
        auto test_bool = [&](auto value, ColKey col, ColKey null_col) {
            using Type = decltype(value);
            auto get_column = [&] { return get_table().template column<Type>(col); };
            test_operator(std::equal_to<>(), get_column, value);
            test_operator(std::not_equal_to<>(), get_column, value);

            auto get_null_column = [&] { return get_table().template column<Type>(null_col); };
            test_operator(std::equal_to<>(), get_null_column, null{});
            test_operator(std::not_equal_to<>(), get_null_column, null{});
        };

        test_numeric(Int(), col_int, col_int_null);
        test_numeric(Float(), col_float, col_float_null);
        test_bool(Bool(), col_bool, col_bool_null);
        test_numeric(Double(), col_double, col_double_null);
        test_numeric(Timestamp(), col_timestamp, col_timestamp_null);

        auto string_col = [&] { return get_table().template column<String>(col_string); };
        test_operator(std::equal_to<>(), string_col, StringData());
        test_operator(std::not_equal_to<>(), string_col, StringData());
        test_operator([](auto&& a, auto&& b) { return a.begins_with(b); }, string_col, StringData());
        test_operator([](auto&& a, auto&& b) { return a.ends_with(b); }, string_col, StringData());
        test_operator([](auto&& a, auto&& b) { return a.contains(b); }, string_col, StringData());
        test_operator([](auto&& a, auto&& b) { return a.like(b); }, string_col, StringData());

        test_operator([](auto&& a, auto&& b) { return a.equal(b, false); }, string_col, StringData());
        test_operator([](auto&& a, auto&& b) { return a.not_equal(b, false); }, string_col, StringData());
        test_operator([](auto&& a, auto&& b) { return a.begins_with(b, false); }, string_col, StringData());
        test_operator([](auto&& a, auto&& b) { return a.ends_with(b, false); }, string_col, StringData());
        test_operator([](auto&& a, auto&& b) { return a.contains(b, false); }, string_col, StringData());
        test_operator([](auto&& a, auto&& b) { return a.like(b, false); }, string_col, StringData());

        auto null_string_col = [&] { return get_table().template column<String>(col_string_null); };
        test_operator(std::equal_to<>(), null_string_col, null());
        test_operator(std::not_equal_to<>(), null_string_col, null());

        auto binary_col = [&] { return get_table().template column<Binary>(col_binary); };
        helper([&](Query&, auto&& test) { test(binary_col() == BinaryData()); });
        helper([&](Query&, auto&& test) { test(binary_col() != BinaryData()); });
        helper([&](Query&, auto&& test) { test(binary_col().size() != 0); });

        auto link_col = [&] { return get_table().template column<Link>(col_link); };
        auto list_col = [&] { return get_table().template column<Link>(col_list); };

        if (mode == Mode::Direct) { // link equality over links isn't implemented
            helper([&](Query&, auto&& test) { test(link_col().is_null()); });
            helper([&](Query&, auto&& test) { test(link_col().is_not_null()); });
            helper([&](Query&, auto&& test) { test(link_col() == obj0); });
            helper([&](Query&, auto&& test) { test(link_col() != obj0); });

            helper([&](Query&, auto&& test) { test(list_col() == obj0); });
            helper([&](Query&, auto&& test) { test(list_col() != obj0); });
        }

        helper([&](Query&, auto&& test) { test(list_col().count() == 1); });
        helper([&](Query&, auto&& test) { test(list_col().column<Int>(col_int).max() > 0); });
        helper([&](Query&, auto&& test) { test(list_col().column<Int>(col_int).min() > 0); });
        helper([&](Query&, auto&& test) { test(list_col().column<Int>(col_int).sum() > 0); });
        helper([&](Query&, auto&& test) { test(list_col().column<Int>(col_int).average() > 0); });

        auto list_int = [&] { return get_table().template column<Lst<Int>>(col_list_int); };

        helper([&](Query&, auto&& test) { test(list_int().size() == 1); });
        helper([&](Query&, auto&& test) { test(list_int() > 0); });
        helper([&](Query&, auto&& test) { test(list_int().max() > 0); });
        helper([&](Query&, auto&& test) { test(list_int().min() > 0); });
        helper([&](Query&, auto&& test) { test(list_int().sum() > 0); });
        helper([&](Query&, auto&& test) { test(list_int().average() > 0); });
    };

    // Test all of the query expressions directly, over a link, over a backlink
    // over a linklist, and over two links
    test_query_expression([&]() -> Table& { return helper.get_table(); }, Mode::Direct);
    test_query_expression(
        [&]() -> Table& {
            Table& t = helper.get_table();
            t.link(col_link);
            return t;
        },
        Mode::Link);
    test_query_expression(
        [&]() -> Table& {
            Table& t = helper.get_table();
            t.backlink(t, col_link);
            return t;
        },
        Mode::LinkList);
    test_query_expression(
        [&]() -> Table& {
            Table& t = helper.get_table();
            t.link(col_list);
            return t;
        },
        Mode::LinkList);
    test_query_expression(
        [&]() -> Table& {
            Table& t = helper.get_table();
            t.link(col_link);
            t.link(col_list);
            return t;
        },
        Mode::LinkList);

    helper([&](Query& q, auto&& test) {
        test(helper.get_table().column<Link>(col_list, q.equal(col_int, 0)).count() > 0);
    });
}

#endif
