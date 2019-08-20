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
#include <set>
#include <sstream>

#include <realm.hpp>
#include <realm/query_expression.hpp> // only needed to compile on v2.6.0
#include <realm/string_data.hpp>
#include <realm/util/file.hpp>

#include "compatibility.hpp"
#include "../util/timer.hpp"
#include "../util/random.hpp"
#include "../util/benchmark_results.hpp"
#include "../util/test_path.hpp"
#include "../util/unit_test.hpp"
#if REALM_ENABLE_ENCRYPTION
#include "../util/crypt_key.hpp"
#endif

using namespace compatibility;
using namespace realm;
using namespace realm::util;
using namespace realm::test_util;

namespace {
#define BASE_SIZE 36000

/**
  This bechmark suite represents a number of common use cases,
  from the perspective of the bindings. It does *not* benchmark
  the type-safe C++ API, but only the things that language bindings
  are likely to use internally.

  This has the following implications:
  - All access is done with a DB in transactions.
  - The DB has full durability (is backed by a file).
    (but all benchmarks are also run with MemOnly durability for comparison)
  - Cases have been derived from:
    https://github.com/realm/realm-java/blob/bp-performance-test/realm/src/androidTest/java/io/realm/RealmPerformanceTest.java
*/

const size_t min_repetitions = 10;
const size_t max_repetitions = 1000;
const double min_duration_s = 0.1;
const double min_warmup_time_s = 0.05;

const char* to_lead_cstr(RealmDurability level);
const char* to_ident_cstr(RealmDurability level);

#ifdef REALM_CLUSTER_IF
#define KEY(x) ObjKey(x)
#else
#define KEY(x) x
using ColKey = size_t;
#endif


struct Benchmark {
    Benchmark()
    {
    }
    virtual ~Benchmark()
    {
    }
    virtual const char* name() const = 0;
    virtual void before_all(DBRef)
    {
    }
    virtual void after_all(DBRef)
    {
    }
    virtual void before_each(DBRef)
    {
    }
    virtual void after_each(DBRef)
    {
#ifdef REALM_CLUSTER_IF
        m_keys.clear();
#endif
    }
    virtual void operator()(DBRef) = 0;
    RealmDurability m_durability = RealmDurability::Full;
    const char* m_encryption_key = nullptr;
#ifdef REALM_CLUSTER_IF
    std::vector<ObjKey> m_keys;
#endif
};

struct BenchmarkUnorderedTableViewClear : Benchmark {
    const char* name() const
    {
        return "UnorderedTableViewClear";
    }

    void operator()(DBRef group)
    {
        const size_t rows = 10000;
        WriteTransaction tr(group);
        TableRef tbl = tr.add_table(name());
        auto col = tbl->add_column(type_String, "s", true);
#ifdef REALM_CLUSTER_IF
        tbl->create_objects(rows, m_keys);
#else
        tbl->add_empty_row(rows);
#endif
        tbl->add_search_index(col);

        for (size_t t = 0; t < rows / 3; t += 3) {
#ifdef REALM_CLUSTER_IF
            tbl->get_object(m_keys[t + 0]).set(col, StringData("foo"));
            tbl->get_object(m_keys[t + 1]).set(col, StringData("bar"));
            tbl->get_object(m_keys[t + 2]).set(col, StringData("hello"));
#else
            tbl->set_string(col, t + 0, StringData("foo"));
            tbl->set_string(col, t + 1, StringData("bar"));
            tbl->set_string(col, t + 2, StringData("hello"));
#endif
        }

        TableView tv = (tbl->column<String>(col) == "foo").find_all();
        tv.clear();
    }
};

struct AddTable : Benchmark {
    const char* name() const
    {
        return "AddTable";
    }

    void operator()(DBRef group)
    {
        WriteTransaction tr(group);
        TableRef t = tr.add_table(name());
        t->add_column(type_String, "first");
        t->add_column(type_Int, "second");
        t->add_column(type_Float, "third");
        tr.commit();
    }

    void after_each(DBRef group)
    {
        TransactionRef tr = group->start_write();
        tr->remove_table(name());
        tr->commit();
    }
};

struct BenchmarkWithStringsTable : Benchmark {
    void before_all(DBRef group)
    {
        WriteTransaction tr(group);
        TableRef t = tr.add_table("StringOnly");
        m_col = t->add_column(type_String, "chars");
        tr.commit();
    }

    void after_all(DBRef group)
    {
        TransactionRef tr = group->start_write();
        tr->remove_table("StringOnly");
        tr->commit();
    }
    ColKey m_col;
};

struct BenchmarkWithStrings : BenchmarkWithStringsTable {
    void before_all(DBRef group)
    {
        BenchmarkWithStringsTable::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table("StringOnly");

        for (size_t i = 0; i < BASE_SIZE * 4; ++i) {
            std::stringstream ss;
            ss << rand();
            auto s = ss.str();
#ifdef REALM_CLUSTER_IF
            Obj obj = t->create_object();
            obj.set<StringData>(m_col, s);
            m_keys.push_back(obj.get_key());
#else
            auto r = t->add_empty_row();
            t->set_string(m_col, r, s);
#endif
        }
        tr.commit();
    }
};

struct BenchmarkWithStringsFewDup : BenchmarkWithStringsTable {
    void before_all(DBRef group)
    {
        BenchmarkWithStringsTable::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table("StringOnly");

        Random r;
        for (size_t i = 0; i < BASE_SIZE * 4; ++i) {
            std::stringstream ss;
            ss << r.draw_int(0, BASE_SIZE * 2);
            auto s = ss.str();
#ifdef REALM_CLUSTER_IF
            Obj obj = t->create_object();
            obj.set<StringData>(m_col, s);
            m_keys.push_back(obj.get_key());
#else
            auto row = t->add_empty_row();
            t->set_string(m_col, row, s);
#endif
        }
        t->add_search_index(m_col);
        tr.commit();
    }
};

struct BenchmarkWithStringsManyDup : BenchmarkWithStringsTable {
    void before_all(DBRef group)
    {
        BenchmarkWithStringsTable::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table("StringOnly");
        Random r;
        for (size_t i = 0; i < BASE_SIZE * 4; ++i) {
            std::stringstream ss;
            ss << r.draw_int(0, 100);
            auto s = ss.str();
#ifdef REALM_CLUSTER_IF
            Obj obj = t->create_object();
            obj.set<StringData>(m_col, s);
            m_keys.push_back(obj.get_key());
#else
            auto row = t->add_empty_row();
            t->set_string(m_col, row, s);
#endif
        }
        t->add_search_index(m_col);
        tr.commit();
    }
};

struct BenchmarkDistinctStringFewDupes : BenchmarkWithStringsFewDup {
    const char* name() const
    {
        return "DistinctStringFewDupes";
    }

    void operator()(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("StringOnly");
        ConstTableView view = table->get_distinct_view(m_col);
    }
};

struct BenchmarkDistinctStringManyDupes : BenchmarkWithStringsManyDup {
    const char* name() const
    {
        return "DistinctStringManyDupes";
    }

    void operator()(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("StringOnly");
        ConstTableView view = table->get_distinct_view(m_col);
    }
};

struct BenchmarkFindAllStringFewDupes : BenchmarkWithStringsFewDup {
    const char* name() const
    {
        return "FindAllStringFewDupes";
    }

    void operator()(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("StringOnly");
        ConstTableView view = table->where().equal(m_col, StringData("10", 2)).find_all();
    }
};

struct BenchmarkFindAllStringManyDupes : BenchmarkWithStringsManyDup {
    const char* name() const
    {
        return "FindAllStringManyDupes";
    }

    void operator()(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("StringOnly");
        ConstTableView view = table->where().equal(m_col, StringData("10", 2)).find_all();
    }
};

struct BenchmarkFindFirstStringFewDupes : BenchmarkWithStringsFewDup {
    const char* name() const
    {
        return "FindFirstStringFewDupes";
    }

    void operator()(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("StringOnly");
        std::vector<std::string> strs = {
            "10", "20", "30", "40", "50", "60", "70", "80", "90", "100",
        };
        for (auto s : strs) {
            table->where().equal(m_col, StringData(s)).find();
        }
    }
};

struct BenchmarkFindFirstStringManyDupes : BenchmarkWithStringsManyDup {
    const char* name() const
    {
        return "FindFirstStringManyDupes";
    }

    void operator()(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("StringOnly");
        std::vector<std::string> strs = {
            "10", "20", "30", "40", "50", "60", "70", "80", "90", "100",
        };
        for (auto s : strs) {
            table->where().equal(m_col, StringData(s)).find();
        }
    }
};

struct BenchmarkWithLongStrings : BenchmarkWithStrings {
    void before_all(DBRef group)
    {
        BenchmarkWithStrings::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table("StringOnly");

        // This should be enough to upgrade the entire array:
        static std::string really_long_string = "A really long string, longer than 63 bytes at least, I guess......";
#ifdef REALM_CLUSTER_IF
        t->get_object(m_keys[0]).set<StringData>(m_col, really_long_string);
        t->get_object(m_keys[BASE_SIZE]).set<StringData>(m_col, really_long_string);
        t->get_object(m_keys[BASE_SIZE * 2]).set<StringData>(m_col, really_long_string);
        t->get_object(m_keys[BASE_SIZE * 3]).set<StringData>(m_col, really_long_string);
#else
        t->insert_empty_row(0);
        t->set_string(m_col, 0, really_long_string);
        t->set_string(m_col, BASE_SIZE, really_long_string);
        t->set_string(m_col, BASE_SIZE * 2, really_long_string);
        t->set_string(m_col, BASE_SIZE * 3, really_long_string);
#endif
        tr.commit();
    }
};

struct BenchmarkWithTimestamps : Benchmark {
    std::multiset<Timestamp> values;
    Timestamp needle;
    size_t num_results_to_needle;
    size_t num_nulls_added = 0;
    double percent_results_to_needle = 0.5;
    double percent_chance_of_null = 0.0;
    void before_all(DBRef group)
    {
        WriteTransaction tr(group);
        TableRef t = tr.add_table("Timestamps");
        auto col = t->add_column(type_Timestamp, "timestamps", true);
        Random r;
        for (size_t i = 0; i < BASE_SIZE * 10; ++i) {
            Timestamp time{r.draw_int<int64_t>(0, 1000000), r.draw_int<int32_t>(0, 1000000)};
            if (r.draw_int<int64_t>(0, 100) / 100.0 < percent_chance_of_null) {
                time = Timestamp{};
                ++num_nulls_added;
            } else {
                values.insert(time);
            }
#ifdef REALM_CLUSTER_IF
            t->create_object().set(col, time);
#else
            t->set_timestamp(col, t->add_empty_row(), time);
#endif
        }
        tr.commit();
        // simulate a work load where this percent of random results match
        num_results_to_needle = size_t(values.size() * percent_results_to_needle);
        // this relies on values being stored in sorted order by std::multiset
        auto it = values.begin();
        for (size_t i = 0; i < num_results_to_needle; ++i) {
            ++it;
        }
        needle = *it;
    }

    void after_all(DBRef group)
    {
        WriteTransaction tr(group);
        tr.get_group().remove_table("Timestamps");
        tr.commit();
    }
};

struct BenchmarkQueryTimestampGreater : BenchmarkWithTimestamps {
    void before_all(DBRef group) {
        percent_chance_of_null = 0.10f;
        percent_results_to_needle = 0.80f;
        BenchmarkWithTimestamps::before_all(group);
    }
    const char* name() const
    {
        return "QueryTimestampGreater";
    }

    void operator()(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("Timestamps");
        auto col = table->get_column_key("timestamps");
        Query query = table->where().greater(col, needle);
        TableView results = query.find_all();
        REALM_ASSERT_EX(results.size() == values.size() - num_results_to_needle - 1, results.size(), num_results_to_needle, values.size());
        static_cast<void>(results);
    }
};

struct BenchmarkQueryTimestampGreaterEqual : BenchmarkWithTimestamps {
    void before_all(DBRef group) {
        percent_chance_of_null = 0.10f;
        percent_results_to_needle = 0.80f;
        BenchmarkWithTimestamps::before_all(group);
    }
    const char* name() const
    {
        return "QueryTimestampGreaterEqual";
    }

    void operator()(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("Timestamps");
        auto col = table->get_column_key("timestamps");
        Query query = table->where().greater_equal(col, needle);
        TableView results = query.find_all();
        REALM_ASSERT_EX(results.size() == values.size() - num_results_to_needle, results.size(), num_results_to_needle, values.size());
        static_cast<void>(results);
    }
};


struct BenchmarkQueryTimestampLess : BenchmarkWithTimestamps {
    void before_all(DBRef group) {
        percent_chance_of_null = 0.10f;
        percent_results_to_needle = 0.20f;
        BenchmarkWithTimestamps::before_all(group);
    }
    const char* name() const
    {
        return "QueryTimestampLess";
    }

    void operator()(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("Timestamps");
        auto col = table->get_column_key("timestamps");
        Query query = table->where().less(col, needle);
        TableView results = query.find_all();
        REALM_ASSERT_EX(results.size() == num_results_to_needle, results.size(), num_results_to_needle, values.size());
        static_cast<void>(results);
    }
};

struct BenchmarkQueryTimestampLessEqual : BenchmarkWithTimestamps {
    void before_all(DBRef group) {
        percent_chance_of_null = 0.10f;
        percent_results_to_needle = 0.20f;
        BenchmarkWithTimestamps::before_all(group);
    }
    const char* name() const
    {
        return "QueryTimestampLessEqual";
    }

    void operator()(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("Timestamps");
        auto col = table->get_column_key("timestamps");
        Query query = table->where().less_equal(col, needle);
        TableView results = query.find_all();
        REALM_ASSERT_EX(results.size() == num_results_to_needle + 1, results.size(), num_results_to_needle, values.size());
        static_cast<void>(results);
    }
};


struct BenchmarkQueryTimestampEqual : BenchmarkWithTimestamps {
    void before_all(DBRef group) {
        percent_chance_of_null = 0.10f;
        percent_results_to_needle = 0.33f;
        BenchmarkWithTimestamps::before_all(group);
    }
    const char* name() const
    {
        return "QueryTimestampEqual";
    }

    void operator()(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("Timestamps");
        auto col = table->get_column_key("timestamps");
        Query query = table->where().equal(col, needle);
        TableView results = query.find_all();
        REALM_ASSERT_EX(results.size() == values.count(needle), results.size(), num_results_to_needle, values.count(needle), values.size());
        static_cast<void>(results);
    }
};

struct BenchmarkQueryTimestampNotEqual : BenchmarkWithTimestamps {
    void before_all(DBRef group) {
        percent_chance_of_null = 0.60f;
        percent_results_to_needle = 0.10f;
        BenchmarkWithTimestamps::before_all(group);
    }
    const char* name() const
    {
        return "QueryTimestampNotEqual";
    }

    void operator()(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("Timestamps");
        auto col = table->get_column_key("timestamps");
        Query query = table->where().not_equal(col, needle);
        TableView results = query.find_all();
        REALM_ASSERT_EX(results.size() == values.size() - values.count(needle) + num_nulls_added, results.size(), values.size(), values.count(needle));
        static_cast<void>(results);
    }
};

struct BenchmarkQueryTimestampNotNull : BenchmarkWithTimestamps {
    void before_all(DBRef group) {
        percent_chance_of_null = 0.60f;
        percent_results_to_needle = 0.0;
        BenchmarkWithTimestamps::before_all(group);
        needle = Timestamp{};
    }
    const char* name() const
    {
        return "QueryTimestampNotNull";
    }

    void operator()(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("Timestamps");
        auto col = table->get_column_key("timestamps");
        Query query = table->where().not_equal(col, realm::null());
        TableView results = query.find_all();
        REALM_ASSERT_EX(results.size() == values.size(), results.size(), num_nulls_added, num_results_to_needle, values.size());
        static_cast<void>(results);
    }
};

struct BenchmarkQueryTimestampEqualNull : BenchmarkWithTimestamps {
    void before_all(DBRef group) {
        percent_chance_of_null = 0.10;
        percent_results_to_needle = 0.0;
        BenchmarkWithTimestamps::before_all(group);
        needle = Timestamp{};
    }
    const char* name() const
    {
        return "QueryTimestampEqualNull";
    }
    void operator()(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("Timestamps");
        auto col = table->get_column_key("timestamps");
        Query query = table->where().equal(col, realm::null());
        TableView results = query.find_all();
        REALM_ASSERT_EX(results.size() == num_nulls_added, results.size(), num_nulls_added, values.size());
        static_cast<void>(results);
    }
};

struct BenchmarkWithIntsTable : Benchmark {
    void before_all(DBRef group)
    {
        WriteTransaction tr(group);
        TableRef t = tr.add_table("IntOnly");
        m_col = t->add_column(type_Int, "ints");
        tr.commit();
    }

    void after_all(DBRef group)
    {
        TransactionRef tr = group->start_write();
        tr->remove_table("IntOnly");
        tr->commit();
    }
    ColKey m_col;
};

struct BenchmarkWithInts : BenchmarkWithIntsTable {
    void before_all(DBRef group)
    {
        BenchmarkWithIntsTable::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table("IntOnly");

        Random r;
        for (size_t i = 0; i < BASE_SIZE * 4; ++i) {
            int64_t val = r.draw_int<int64_t>();
#ifdef REALM_CLUSTER_IF
            Obj obj = t->create_object();
            obj.set(m_col, val);
            m_keys.push_back(obj.get_key());
#else
            auto row = t->add_empty_row();
            t->set_int(m_col, row, val);
#endif
        }
        tr.commit();
    }
};

struct BenchmarkQueryChainedOrInts : BenchmarkWithIntsTable {
    const size_t num_queried_matches = 1000;
    const size_t num_rows = 100000;
    std::vector<int64_t> values_to_query;
    const char* name() const
    {
        return "QueryChainedOrInts";
    }

    void before_all(DBRef group)
    {
        BenchmarkWithIntsTable::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table("IntOnly");
        std::vector<ObjKey> keys;
        t->create_objects(num_rows, keys);
        REALM_ASSERT(num_rows > num_queried_matches);
        Random r;
        size_t i = 0;
        for (auto e : *t) {
            e.set<Int>(m_col, i);
            ++i;
        }
        for (i = 0; i < num_queried_matches; ++i) {
            size_t ndx_to_match = (num_rows / num_queried_matches) * i;
            values_to_query.push_back(t->get_object(ndx_to_match).get<Int>(m_col));
        }
        tr.commit();
    }

    void operator()(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("IntOnly");
        Query query = table->where();
        for (size_t i = 0; i < values_to_query.size(); ++i) {
            query.Or().equal(m_col, values_to_query[i]);
        }
        TableView results = query.find_all();
        REALM_ASSERT_EX(results.size() == num_queried_matches, results.size(), num_queried_matches,
                        values_to_query.size());
        static_cast<void>(results);
    }
};

struct BenchmarkQueryChainedOrIntsIndexed : BenchmarkQueryChainedOrInts {
    const char* name() const
    {
        return "QueryChainedOrIntsIndexed";
    }
    void before_all(DBRef group)
    {
        BenchmarkQueryChainedOrInts::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table("IntOnly");
        t->add_search_index(m_col);
        tr.commit();
    }
};


struct BenchmarkQueryIntEquality : BenchmarkQueryChainedOrInts {
    const char* name() const
    {
        return "QueryIntEquality";
    }

    void operator()(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("IntOnly");
        Query query = table->where().equal(m_col, 0);
        TableView results = query.find_all();
        REALM_ASSERT_EX(results.size() == 1, results.size(), 1);
        static_cast<void>(results);
    }
};

struct BenchmarkQueryIntEqualityIndexed : BenchmarkQueryIntEquality {
    const char* name() const
    {
        return "QueryIntEqualityIndexed";
    }
    void before_all(DBRef group)
    {
        BenchmarkQueryIntEquality::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table("IntOnly");
        t->add_search_index(m_col);
        tr.commit();
    }
};

struct BenchmarkQuery : BenchmarkWithStrings {
    const char* name() const
    {
        return "Query";
    }

    void operator()(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("StringOnly");
        ConstTableView view = table->find_all_string(m_col, "200");
    }
};

struct BenchmarkQueryChainedOrStrings : BenchmarkWithStringsTable {
    const size_t num_queried_matches = 1000;
    const size_t num_rows = 100000;
    std::vector<std::string> values_to_query;
    const char* name() const
    {
        return "QueryChainedOrStrings";
    }

    void before_all(DBRef group)
    {
        BenchmarkWithStringsTable::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table("StringOnly");
        REALM_ASSERT(num_rows > num_queried_matches);
        for (size_t i = 0; i < num_rows; ++i) {
            std::stringstream ss;
            ss << i;
            auto s = ss.str();
            t->create_object().set(m_col, s);
        }
        // t->add_search_index(0);
        for (size_t i = 0; i < num_queried_matches; ++i) {
            size_t ndx_to_match = (num_rows / num_queried_matches) * i;
            auto obj = t->get_object(ndx_to_match);
            values_to_query.push_back(obj.get<String>(m_col));
        }
        tr.commit();
    }

    void operator()(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("StringOnly");
        Query query = table->where();
        for (size_t i = 0; i < values_to_query.size(); ++i) {
            query.Or().equal(m_col, values_to_query[i]);
        }
        TableView results = query.find_all();
        REALM_ASSERT_EX(results.size() == num_queried_matches, results.size(), num_queried_matches,
                        values_to_query.size());
        static_cast<void>(results);
    }
};

struct BenchmarkSize : BenchmarkWithStrings {
    const char* name() const
    {
        return "Size";
    }

    void operator()(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("StringOnly");
        volatile size_t dummy = table->size();
        static_cast<void>(dummy);
    }
};

struct BenchmarkSort : BenchmarkWithStrings {
    const char* name() const
    {
        return "Sort";
    }

    void operator()(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("StringOnly");
        ConstTableView view = table->get_sorted_view(m_col);
    }
};

struct BenchmarkEmptyCommit : Benchmark {
    const char* name() const
    {
        return "EmptyCommit";
    }

    void operator()(DBRef group)
    {
        WriteTransaction tr(group);
        tr.commit();
    }
};

struct BenchmarkSortInt : BenchmarkWithInts {
    const char* name() const
    {
        return "SortInt";
    }

    void operator()(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("IntOnly");
        ConstTableView view = table->get_sorted_view(m_col);
    }
};

struct BenchmarkDistinctIntFewDupes : BenchmarkWithIntsTable {
    const char* name() const
    {
        return "DistinctIntNoDupes";
    }

    void before_all(DBRef group)
    {
        BenchmarkWithIntsTable::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table("IntOnly");
        Random r;
        for (size_t i = 0; i < BASE_SIZE * 4; ++i) {
            int64_t val = r.draw_int(0, BASE_SIZE * 2);
#ifdef REALM_CLUSTER_IF
            Obj obj = t->create_object();
            obj.set(m_col, val);
            m_keys.push_back(obj.get_key());
#else
            auto row = t->add_empty_row();
            t->set_int(m_col, row, val);
#endif
        }
        t->add_search_index(m_col);
        tr.commit();
    }

    void operator()(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("IntOnly");
        ConstTableView view = table->get_distinct_view(m_col);
    }
};

struct BenchmarkDistinctIntManyDupes : BenchmarkWithIntsTable {
    const char* name() const
    {
        return "DistinctIntManyDupes";
    }

    void before_all(DBRef group)
    {
        BenchmarkWithIntsTable::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table("IntOnly");
        Random r;
        for (size_t i = 0; i < BASE_SIZE * 4; ++i) {
            int64_t val = r.draw_int(0, 10);
#ifdef REALM_CLUSTER_IF
            Obj obj = t->create_object();
            obj.set(m_col, val);
            m_keys.push_back(obj.get_key());
#else
            auto row = t->add_empty_row();
            t->set_int(m_col, row, val);
#endif
        }
        t->add_search_index(m_col);
        tr.commit();
    }

    void operator()(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("IntOnly");
        ConstTableView view = table->get_distinct_view(m_col);
    }
};

struct BenchmarkInsert : BenchmarkWithStringsTable {
    const char* name() const
    {
        return "Insert";
    }

    void operator()(DBRef group)
    {
        WriteTransaction tr(group);
        TableRef t = tr.get_table("StringOnly");

        for (size_t i = 0; i < 10000; ++i) {
#ifdef REALM_CLUSTER_IF
            Obj obj = t->create_object();
            obj.set(m_col, "a");
            m_keys.push_back(obj.get_key());
#else
            auto row = t->add_empty_row();
            t->set_string(m_col, row, "a");
#endif
        }
        tr.commit();
    }
};

struct BenchmarkGetString : BenchmarkWithStrings {
    const char* name() const
    {
        return "GetString";
    }

    void operator()(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("StringOnly");

        volatile int dummy = 0;
#ifdef REALM_CLUSTER_IF
        for (auto obj : *table) {
            StringData str = obj.get<String>(m_col);
            dummy += str[0]; // to avoid over-optimization
        }
#else
        size_t len = table->size();
        for (size_t i = 0; i < len; ++i) {
            StringData str = table->get_string(m_col, i);
            dummy += str[0]; // to avoid over-optimization
        }
#endif
    }
};

struct BenchmarkSetString : BenchmarkWithStrings {
    const char* name() const
    {
        return "SetString";
    }

    void operator()(DBRef group)
    {
        WriteTransaction tr(group);
        TableRef table = tr.get_table("StringOnly");

#ifdef REALM_CLUSTER_IF
        for (auto obj : *table) {
            obj.set<String>(m_col, "c");
        }
#else
        size_t len = table->size();
        for (size_t i = 0; i < len; ++i) {
            table->set_string(m_col, i, "c");
        }
#endif
        tr.commit();
    }
};

struct BenchmarkCreateIndex : BenchmarkWithStrings {
    const char* name() const
    {
        return "CreateIndex";
    }
    void operator()(DBRef group)
    {
        WriteTransaction tr(group);
        TableRef table = tr.get_table("StringOnly");
        table->add_search_index(m_col);
        tr.commit();
    }
};

struct BenchmarkGetLongString : BenchmarkWithLongStrings {
    const char* name() const
    {
        return "GetLongString";
    }

    void operator()(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("StringOnly");
        volatile int dummy = 0;
#ifdef REALM_CLUSTER_IF
        for (auto obj : *table) {
            StringData str = obj.get<String>(m_col);
            dummy += str[0]; // to avoid over-optimization
        }
#else
        size_t len = table->size();
        for (size_t i = 0; i < len; ++i) {
            StringData str = table->get_string(m_col, i);
            dummy += str[0]; // to avoid over-optimization
        }
#endif
    }
};

struct BenchmarkQueryLongString : BenchmarkWithStrings {
    static constexpr const char* long_string = "This is some other long string, that takes a lot of time to find";
    bool ok;

    const char* name() const
    {
        return "QueryLongString";
    }

    void before_all(DBRef group)
    {
        BenchmarkWithStrings::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table("StringOnly");
#ifdef REALM_CLUSTER_IF
        auto it = t->begin();
        it->set<String>(m_col, "Some random string");
        ++it;
        it->set<String>(m_col, long_string);
#else
        t->set_string(m_col, 0, "Some random string");
        t->set_string(m_col, 1, long_string);
#endif
        tr.commit();
    }

    void operator()(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("StringOnly");
        StringData str(long_string);
        ok = true;
        auto q = table->where().equal(m_col, str);
        for (size_t ndx = 0; ndx < 1000; ndx++) {
            auto res = q.find();
            if (res != KEY(1)) {
                ok = false;
            }
        }
    }
};

struct BenchmarkQueryInsensitiveString : BenchmarkWithStringsTable {
    const char* name() const
    {
        return "QueryInsensitiveString";
    }

    std::string gen_random_case_string(size_t length)
    {
        std::stringstream ss;
        for (size_t c = 0; c < length; ++c) {
            bool lowercase = (rand() % 2) == 0;
            // choose characters from a-z or A-Z
            ss << char((rand() % 26) + (lowercase ? 97 : 65));
        }
        return ss.str();
    }

    std::string shuffle_case(std::string str)
    {
        for (size_t i = 0; i < str.size(); ++i) {
            char c = str[i];
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')) {
                bool change_case = (rand() % 2) == 0;
                c ^= change_case ? 0x20 : 0;
            }
            str[i] = c;
        }
        return str;
    }

    size_t rand() {
        return seeded_rand.draw_int<size_t>();
    }

    void before_all(DBRef group)
    {
        BenchmarkWithStringsTable::before_all(group);

        // chosen by fair dice roll, guaranteed to be random
        static const unsigned long seed = 4;
        seeded_rand.seed(seed);

        WriteTransaction tr(group);
        TableRef t = tr.get_table("StringOnly");

        const size_t max_chars_in_string = 100;

        for (size_t i = 0; i < BASE_SIZE * 4; ++i) {
            size_t num_chars = rand() % max_chars_in_string;
            std::string randomly_cased_string = gen_random_case_string(num_chars);
#ifdef REALM_CLUSTER_IF
            Obj obj = t->create_object();
            obj.set<String>(m_col, randomly_cased_string);
            m_keys.push_back(obj.get_key());
#else
            auto row = t->add_empty_row();
            t->set_string(m_col, row, randomly_cased_string);
#endif
        }
        tr.commit();
    }
    std::string needle;
    bool successful = false;
    Random seeded_rand;

    void before_each(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("StringOnly");
        size_t target_row = rand() % table->size();
#ifdef REALM_CLUSTER_IF
        ConstObj obj = table->get_object(m_keys[target_row]);
        StringData target_str = obj.get<String>(m_col);
#else
        StringData target_str = table->get_string(0, target_row);
#endif
        needle = shuffle_case(target_str.data());
    }

    void operator()(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table("StringOnly");
        StringData str(needle);
        Query q = table->where().equal(m_col, str, false);
        TableView res = q.find_all();
        successful = res.size() > 0;
    }
};

struct BenchmarkQueryInsensitiveStringIndexed : BenchmarkQueryInsensitiveString {
    const char* name() const
    {
        return "QueryInsensitiveStringIndexed";
    }
    void before_all(DBRef group)
    {
        BenchmarkQueryInsensitiveString::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table("StringOnly");
        t->add_search_index(m_col);
        tr.commit();
    }
};

struct BenchmarkSetLongString : BenchmarkWithLongStrings {
    const char* name() const
    {
        return "SetLongString";
    }

    void operator()(DBRef group)
    {
        WriteTransaction tr(group);
        TableRef table = tr.get_table("StringOnly");
#ifdef REALM_CLUSTER_IF
        Obj obj = table->create_object();
        obj.set<String>(m_col, "c");
        m_keys.push_back(obj.get_key());
#else
        size_t len = table->size();
        for (size_t i = 0; i < len; ++i) {
            table->set_string(m_col, i, "c");
        }
#endif
        tr.commit();
    }
};

struct BenchmarkQueryNot : Benchmark {
    const char* name() const
    {
        return "QueryNot";
    }

    void before_all(DBRef group)
    {
        WriteTransaction tr(group);
        TableRef table = tr.add_table(name());
        m_col = table->add_column(type_Int, "first");
#ifdef REALM_CLUSTER_IF
        for (size_t i = 0; i < 1000; ++i) {
            table->create_object().set(m_col, 1);
        }
#else
        table->add_empty_row(1000);
        for (size_t i = 0; i < 1000; ++i) {
            table->set_int(m_col, i, 1);
        }
#endif
        tr.commit();
    }

    void operator()(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table(name());
        Query q = table->where();
        q.not_equal(m_col, 2); // never found, = worst case
        TableView results = q.find_all();
        results.size();
    }

    void after_all(DBRef group)
    {
        TransactionRef tr = group->start_write();
        tr->remove_table(name());
        tr->commit();
    }

    ColKey m_col;
};

struct BenchmarkGetLinkList : Benchmark {
    const char* name() const
    {
        return "GetLinkList";
    }
    static const size_t rows = 10000;

    void before_all(DBRef group)
    {
        WriteTransaction tr(group);
        std::string n = std::string(name()) + "_Destination";
        TableRef destination_table = tr.add_table(n);
        TableRef table = tr.add_table(name());
        m_col_link = table->add_column_link(type_LinkList, "linklist", *destination_table);
#ifdef REALM_CLUSTER_IF
        table->create_objects(rows, m_keys);
#else
        table->add_empty_row(rows);
#endif
        tr.commit();
    }

    void operator()(DBRef group)
    {
        ReadTransaction tr(group);
        ConstTableRef table = tr.get_table(name());
#ifdef REALM_CLUSTER_IF
        std::vector<ConstLnkLstPtr> linklists(rows);
        for (size_t i = 0; i < rows; ++i) {
            auto obj = table->get_object(m_keys[i]);
            linklists[i] = obj.get_linklist_ptr(m_col_link);
        }
        for (size_t i = 0; i < rows; ++i) {
            auto obj = table->get_object(m_keys[i]);
            obj.get_linklist_ptr(m_col_link);
        }
        for (size_t i = 0; i < rows; ++i) {
            linklists[i].reset();
        }
#else
        std::vector<ConstLinkViewRef> linklists(rows);
        for (size_t i = 0; i < rows; ++i) {
            linklists[i] = table->get_linklist(m_col_link, i);
        }
        for (size_t i = 0; i < rows; ++i) {
            table->get_linklist(m_col_link, i);
        }
        for (size_t i = 0; i < rows; ++i) {
            linklists[i].reset();
        }
#endif
    }

    void after_all(DBRef group)
    {
        TransactionRef tr = group->start_write();
        tr->remove_table(name());
        auto n = std::string(name()) + "_Destination";
        tr->remove_table(n);
        tr->commit();
    }

    ColKey m_col_link;
};

struct BenchmarkNonInitatorOpen : Benchmark {
    const char* name() const
    {
        return "NonInitiatorOpen";
    }
    // the shared realm will be removed after the benchmark finishes
    std::unique_ptr<realm::test_util::SharedGroupTestPathGuard> path;
    DBRef initiator;

    DBRef do_open()
    {
        const std::string realm_path = *path;
        return create_new_shared_group(realm_path, m_durability, m_encryption_key);
    }

    void before_all(DBRef)
    {
        // Generate the benchmark result texts:
        std::stringstream ident_ss;
        ident_ss << "BenchmarkCommonTasks_" << this->name()
        << "_" << to_ident_cstr(m_durability);
        std::string ident = ident_ss.str();

        realm::test_util::unit_test::TestDetails test_details;
        test_details.suite_name = "BenchmarkCommonTasks";
        test_details.test_name = ident.c_str();
        test_details.file_name = __FILE__;
        test_details.line_number = __LINE__;

        path = std::unique_ptr<realm::test_util::SharedGroupTestPathGuard>(new realm::test_util::SharedGroupTestPathGuard(ident));

        // open once - session initiation
        initiator = do_open();
    }

    void operator()(DBRef)
    {
        // use groups of 10 to get higher times
        for (size_t i = 0; i < 10; ++i) {
            do_open();
            // let it close, otherwise we get error: too many open files
        }
    }
};


const char* to_lead_cstr(RealmDurability level)
{
    switch (level) {
        case RealmDurability::Full:
            return "Full   ";
        case RealmDurability::MemOnly:
            return "MemOnly";
#ifndef _WIN32
        case RealmDurability::Async:
            return "Async  ";
#endif
    }
    return nullptr;
}

const char* to_ident_cstr(RealmDurability level)
{
    switch (level) {
        case RealmDurability::Full:
            return "Full";
        case RealmDurability::MemOnly:
            return "MemOnly";
#ifndef _WIN32
        case RealmDurability::Async:
            return "Async";
#endif
    }
    return nullptr;
}

void run_benchmark_once(Benchmark& benchmark, DBRef sg, Timer& timer)
{
    timer.pause();
    benchmark.before_each(sg);
    timer.unpause();

    benchmark(sg);

    timer.pause();
    benchmark.after_each(sg);
    timer.unpause();
}


/// This little piece of likely over-engineering runs the benchmark a number of times,
/// with each durability setting, and reports the results for each run.
template<typename B>
void run_benchmark(BenchmarkResults& results)
{
    typedef std::pair<RealmDurability, const char*> config_pair;
    std::vector<config_pair> configs;

    configs.push_back(config_pair(RealmDurability::MemOnly, nullptr));
#if REALM_ENABLE_ENCRYPTION
    configs.push_back(config_pair(RealmDurability::MemOnly, crypt_key(true)));
#endif

    configs.push_back(config_pair(RealmDurability::Full, nullptr));

#if REALM_ENABLE_ENCRYPTION
    configs.push_back(config_pair(RealmDurability::Full, crypt_key(true)));
#endif

    Timer timer(Timer::type_UserTime);

    for (auto it = configs.begin(); it != configs.end(); ++it) {
        RealmDurability level = it->first;
        const char* key = it->second;
        B benchmark;
        benchmark.m_durability = level;
        benchmark.m_encryption_key = key;

        // Generate the benchmark result texts:
        std::stringstream lead_text_ss;
        std::stringstream ident_ss;
        lead_text_ss << benchmark.name() << " (" << to_lead_cstr(level) << ", "
                     << (key == nullptr ? "EncryptionOff" : "EncryptionOn") << ")";
        ident_ss << benchmark.name() << "_" << to_ident_cstr(level)
                 << (key == nullptr ? "_EncryptionOff" : "_EncryptionOn");
        std::string ident = ident_ss.str();

        realm::test_util::unit_test::TestDetails test_details;
        test_details.suite_name = "BenchmarkCommonTasks";
        test_details.test_name = ident.c_str();
        test_details.file_name = __FILE__;
        test_details.line_number = __LINE__;

        // Open a SharedGroup:
        realm::test_util::SharedGroupTestPathGuard realm_path("benchmark_common_tasks" + ident);
        DBRef group;
        group = create_new_shared_group(realm_path, level, key);
        benchmark.before_all(group);

        // Warm-up and initial measuring:
        size_t num_warmup_reps = 1;
        double time_to_execute_warmup_reps = 0;
        while (time_to_execute_warmup_reps < min_warmup_time_s && num_warmup_reps < max_repetitions) {
            num_warmup_reps *= 10;
            Timer t(Timer::type_UserTime);
            for (size_t i = 0; i < num_warmup_reps; ++i) {
                run_benchmark_once(benchmark, group, t);
            }
            time_to_execute_warmup_reps = t.get_elapsed_time();
        }

        size_t required_reps = size_t(min_duration_s / (time_to_execute_warmup_reps / num_warmup_reps));
        if (required_reps < min_repetitions) {
            required_reps = min_repetitions;
        }
        if (required_reps > max_repetitions) {
            required_reps = max_repetitions;
        }

        for (size_t rep = 0; rep < required_reps; ++rep) {
            Timer t;
            run_benchmark_once(benchmark, group, t);
            double s = t.get_elapsed_time();
            results.submit(ident.c_str(), s);
        }

        benchmark.after_all(group);

        results.finish(ident, lead_text_ss.str());
    }
    std::cout << std::endl;
}

} // anonymous namespace

extern "C" int benchmark_common_tasks_main();

int benchmark_common_tasks_main()
{
    std::string results_file_stem = realm::test_util::get_test_path_prefix() + "results";
    BenchmarkResults results(40, results_file_stem.c_str());

#define BENCH(B) run_benchmark<B>(results)
    BENCH(BenchmarkUnorderedTableViewClear);
    BENCH(BenchmarkEmptyCommit);
    BENCH(AddTable);
    BENCH(BenchmarkQuery);
    BENCH(BenchmarkQueryNot);
    BENCH(BenchmarkSize);
    BENCH(BenchmarkSort);
    BENCH(BenchmarkSortInt);

    BENCH(BenchmarkDistinctIntFewDupes);
    BENCH(BenchmarkDistinctIntManyDupes);
    BENCH(BenchmarkDistinctStringFewDupes);
    BENCH(BenchmarkDistinctStringManyDupes);

    BENCH(BenchmarkFindAllStringFewDupes);
    BENCH(BenchmarkFindAllStringManyDupes);
    BENCH(BenchmarkFindFirstStringFewDupes);
    BENCH(BenchmarkFindFirstStringManyDupes);

    BENCH(BenchmarkInsert);
    BENCH(BenchmarkGetString);
    BENCH(BenchmarkSetString);
    BENCH(BenchmarkCreateIndex);
    BENCH(BenchmarkGetLongString);
    BENCH(BenchmarkQueryLongString);
    BENCH(BenchmarkSetLongString);
    BENCH(BenchmarkGetLinkList);
    BENCH(BenchmarkQueryInsensitiveString);
    BENCH(BenchmarkQueryInsensitiveStringIndexed);
    BENCH(BenchmarkNonInitatorOpen);
    BENCH(BenchmarkQueryChainedOrStrings);
    BENCH(BenchmarkQueryChainedOrInts);
    BENCH(BenchmarkQueryChainedOrIntsIndexed);
    BENCH(BenchmarkQueryIntEquality);
    BENCH(BenchmarkQueryIntEqualityIndexed);
    BENCH(BenchmarkQueryTimestampGreater);
    BENCH(BenchmarkQueryTimestampGreaterEqual);
    BENCH(BenchmarkQueryTimestampLess);
    BENCH(BenchmarkQueryTimestampLessEqual);
    BENCH(BenchmarkQueryTimestampEqual);
    BENCH(BenchmarkQueryTimestampNotEqual);
    BENCH(BenchmarkQueryTimestampNotNull);
    BENCH(BenchmarkQueryTimestampEqualNull);

#undef BENCH
    return 0;
}

#if !REALM_IOS
int main(int, const char**)
{
    return benchmark_common_tasks_main();
}
#endif
