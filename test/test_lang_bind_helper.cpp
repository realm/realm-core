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

#include <map>
#include <sstream>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <thread>
#include "testsettings.hpp"
#ifdef TEST_LANG_BIND_HELPER

#include <realm.hpp>
#include <realm/util/encrypted_file_mapping.hpp>
#include <realm/util/to_string.hpp>
#include <realm/replication.hpp>
#include <realm/history.hpp>

// Need fork() and waitpid() for Shared_RobustAgainstDeathDuringWrite
#ifndef _WIN32
#include <unistd.h>
#include <sys/wait.h>
#define ENABLE_ROBUST_AGAINST_DEATH_DURING_WRITE
#else
#include <windows.h>
#endif


#include "test.hpp"
#include "test_table_helper.hpp"

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;
using unit_test::TestContext;

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

void work_on_frozen(TestContext& test_context, TransactionRef frozen)
{
    CHECK_THROW(frozen->promote_to_write(), LogicError);
    auto table = frozen->get_table("my_table");
    auto col = table->get_column_key("my_col_1");
    int64_t sum = 0;
    for (auto i : *table) {
        sum += i.get<int64_t>(col);
    }
    CHECK_EQUAL(sum, 1000 / 2 * 999);
}

TEST(Transactions_Frozen)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef db = DB::create(*hist_w);
    TransactionRef frozen;
    {
        auto wt = db->start_write();
        auto table = wt->add_table("my_table");
        table->add_column(type_Int, "my_col_1");
        for (int j = 0; j < 1000; ++j) {
            table->create_object().set_all(j);
        }
        wt->commit_and_continue_as_read();
        frozen = wt->freeze();
    }
    // create multiple threads, all doing read-only work on Frozen
    const int num_threads = 100;
    std::thread frozen_workers[num_threads];
    for (int j = 0; j < num_threads; ++j)
        frozen_workers[j] = std::thread([&] { work_on_frozen(test_context, frozen); });
    for (int j = 0; j < num_threads; ++j)
        frozen_workers[j].join();
}

class MyHistory : public _impl::History {
public:
    std::vector<char> m_incoming_changeset;
    version_type m_incoming_version;
    struct ChangeSet {
        std::vector<char> changes;
        bool finalized = false;
    };
    std::map<uint_fast64_t, ChangeSet> m_changesets;

    void update_from_ref(ref_type, version_type) override
    {
        // No-op
    }
    version_type add_changeset(const char* data, size_t size, version_type orig_version)
    {
        m_incoming_changeset.assign(data, data + size); // Throws
        version_type new_version = orig_version + 1;
        m_incoming_version = new_version;
        // Allocate space for the new changeset in m_changesets such that we can
        // be sure no exception will be thrown whan adding the changeset in
        // finalize_changeset().
        m_changesets[new_version]; // Throws
        return new_version;
    }
    void finalize()
    {
        // The following operation will not throw due to the space reservation
        // carried out in prepare_new_changeset().
        m_changesets[m_incoming_version].changes = std::move(m_incoming_changeset);
        m_changesets[m_incoming_version].finalized = true;
    }
    void get_changesets(version_type begin_version, version_type end_version, BinaryIterator* buffer) const
        noexcept override
    {
        size_t n = size_t(end_version - begin_version);
        for (size_t i = 0; i < n; ++i) {
            uint_fast64_t version = begin_version + i + 1;
            auto j = m_changesets.find(version);
            REALM_ASSERT(j != m_changesets.end());
            const ChangeSet& changeset = j->second;
            REALM_ASSERT(changeset.finalized); // Must have been finalized
            buffer[i] = BinaryData(changeset.changes.data(), changeset.changes.size());
        }
    }
    void set_oldest_bound_version(version_type) override
    {
        // No-op
    }

    void verify() const override
    {
        // No-op
    }
};

class ShortCircuitHistory : public TrivialReplication {
public:
    using version_type = _impl::History::version_type;

    ShortCircuitHistory(const std::string& database_file)
        : TrivialReplication(database_file)
    {
    }

    void initiate_session(version_type) override
    {
        // No-op
    }

    void terminate_session() noexcept override
    {
        // No-op
    }

    version_type prepare_changeset(const char* data, size_t size, version_type orig_version) override
    {
        return m_history.add_changeset(data, size, orig_version); // Throws
    }

    void finalize_changeset() noexcept override
    {
        m_history.finalize();
    }

    HistoryType get_history_type() const noexcept override
    {
        return hist_InRealm;
    }

    _impl::History* get_history_write() override
    {
        return &m_history;
    }

    std::unique_ptr<_impl::History> get_history_read() override
    {
        auto hist = std::make_unique<MyHistory>();
        *hist = m_history;
        return std::move(hist);
    }

    int get_history_schema_version() const noexcept override
    {
        return 0;
    }

    bool is_upgradable_history_schema(int) const noexcept override
    {
        REALM_ASSERT(false);
        return false;
    }

    void upgrade_history_schema(int) override
    {
        REALM_ASSERT(false);
    }


private:
    MyHistory m_history;
};

} // anonymous namespace


TEST(LangBindHelper_AdvanceReadTransact_Basics)
{
    SHARED_GROUP_TEST_PATH(path);
    ShortCircuitHistory hist(path);
    DBRef sg = DB::create(hist, DBOptions(crypt_key()));
    DBRef sg_w = DB::create(hist, DBOptions(crypt_key()));

    // Start a read transaction (to be repeatedly advanced)
    TransactionRef rt = sg->start_read();
    CHECK_EQUAL(0, rt->size());

    // Try to advance without anything having happened
    rt->advance_read();
    rt->verify();
    CHECK_EQUAL(0, rt->size());

    // Try to advance after an empty write transaction
    {
        WriteTransaction wt(sg_w);
        wt.commit();
    }
    rt->advance_read();
    rt->verify();
    CHECK_EQUAL(0, rt->size());

    // Try to advance after a superfluous rollback
    {
        WriteTransaction wt(sg_w);
        // Implicit rollback
    }
    rt->advance_read();
    rt->verify();
    CHECK_EQUAL(0, rt->size());

    // Try to advance after a propper rollback
    {
        WriteTransaction wt(sg_w);
        TableRef foo_w = wt.add_table("bad");
        // Implicit rollback
    }
    rt->advance_read();
    rt->verify();
    CHECK_EQUAL(0, rt->size());

    // Create a table via the other SharedGroup
    ObjKey k0;
    {
        WriteTransaction wt(sg_w);
        TableRef foo_w = wt.add_table("foo");
        foo_w->add_column(type_Int, "i");
        k0 = foo_w->create_object().get_key();
        wt.commit();
    }

    rt->advance_read();
    rt->verify();
    CHECK_EQUAL(1, rt->size());
    ConstTableRef foo = rt->get_table("foo");
    CHECK_EQUAL(1, foo->get_column_count());
    auto cols = foo->get_column_keys();
    CHECK_EQUAL(type_Int, foo->get_column_type(cols[0]));
    CHECK_EQUAL(1, foo->size());
    CHECK_EQUAL(0, foo->get_object(k0).get<int64_t>(cols[0]));
    uint_fast64_t version = foo->get_content_version();

    // Modify the table via the other SharedGroup
    ObjKey k1;
    {
        WriteTransaction wt(sg_w);
        TableRef foo_w = wt.get_table("foo");
        foo_w->add_column(type_String, "s");
        cols = foo_w->get_column_keys();
        k1 = foo_w->create_object().get_key();
        auto obj0 = foo_w->get_object(k0);
        auto obj1 = foo_w->get_object(k1);
        obj0.set<int>(cols[0], 1);
        obj1.set<int>(cols[0], 2);
        obj0.set<StringData>(cols[1], "a");
        obj1.set<StringData>(cols[1], "b");
        wt.commit();
    }
    rt->advance_read();
    CHECK(version != foo->get_content_version());
    rt->verify();
    CHECK_EQUAL(2, foo->get_column_count());
    CHECK_EQUAL(type_Int, foo->get_column_type(cols[0]));
    CHECK_EQUAL(type_String, foo->get_column_type(cols[1]));
    CHECK_EQUAL(2, foo->size());
    auto obj0 = foo->get_object(k0);
    auto obj1 = foo->get_object(k1);
    CHECK_EQUAL(1, obj0.get<int64_t>(cols[0]));
    CHECK_EQUAL(2, obj1.get<int64_t>(cols[0]));
    CHECK_EQUAL("a", obj0.get<StringData>(cols[1]));
    CHECK_EQUAL("b", obj1.get<StringData>(cols[1]));
    CHECK_EQUAL(foo, rt->get_table("foo"));

    // Again, with no change
    rt->advance_read();
    rt->verify();
    CHECK_EQUAL(2, foo->get_column_count());
    CHECK_EQUAL(type_Int, foo->get_column_type(cols[0]));
    CHECK_EQUAL(type_String, foo->get_column_type(cols[1]));
    CHECK_EQUAL(2, foo->size());
    CHECK_EQUAL(1, obj0.get<int64_t>(cols[0]));
    CHECK_EQUAL(2, obj1.get<int64_t>(cols[0]));
    CHECK_EQUAL("a", obj0.get<StringData>(cols[1]));
    CHECK_EQUAL("b", obj1.get<StringData>(cols[1]));
    CHECK_EQUAL(foo, rt->get_table("foo"));

    // Perform several write transactions before advancing the read transaction
    {
        WriteTransaction wt(sg_w);
        TableRef bar_w = wt.add_table("bar");
        bar_w->add_column(type_Int, "a");
        wt.commit();
    }
    {
        WriteTransaction wt(sg_w);
        wt.commit();
    }
    {
        WriteTransaction wt(sg_w);
        TableRef bar_w = wt.get_table("bar");
        bar_w->add_column(type_Float, "b");
        wt.commit();
    }
    {
        WriteTransaction wt(sg_w);
        // Implicit rollback
    }
    {
        WriteTransaction wt(sg_w);
        TableRef bar_w = wt.get_table("bar");
        bar_w->add_column(type_Double, "c");
        wt.commit();
    }

    rt->advance_read();
    rt->verify();
    CHECK_EQUAL(2, rt->size());
    CHECK_EQUAL(2, foo->get_column_count());
    cols = foo->get_column_keys();
    CHECK_EQUAL(type_Int, foo->get_column_type(cols[0]));
    CHECK_EQUAL(type_String, foo->get_column_type(cols[1]));
    CHECK_EQUAL(2, foo->size());
    CHECK_EQUAL(1, obj0.get<int64_t>(cols[0]));
    CHECK_EQUAL(2, obj1.get<int64_t>(cols[0]));
    CHECK_EQUAL("a", obj0.get<StringData>(cols[1]));
    CHECK_EQUAL("b", obj1.get<StringData>(cols[1]));
    CHECK_EQUAL(foo, rt->get_table("foo"));
    ConstTableRef bar = rt->get_table("bar");
    cols = bar->get_column_keys();
    CHECK_EQUAL(3, bar->get_column_count());
    CHECK_EQUAL(type_Int, bar->get_column_type(cols[0]));
    CHECK_EQUAL(type_Float, bar->get_column_type(cols[1]));
    CHECK_EQUAL(type_Double, bar->get_column_type(cols[2]));

    // Clear tables - not supported before backlinks work again
    {
        WriteTransaction wt(sg_w);
        TableRef foo_w = wt.get_table("foo");
        foo_w->clear();
        TableRef bar_w = wt.get_table("bar");
        bar_w->clear();
        wt.commit();
    }
    rt->advance_read();
    rt->verify();
    CHECK_EQUAL(2, rt->size());
    CHECK(foo);
    CHECK_EQUAL(2, foo->get_column_count());
    CHECK_EQUAL(type_Int, foo->get_column_type(cols[0]));
    CHECK_EQUAL(type_String, foo->get_column_type(cols[1]));
    CHECK_EQUAL(0, foo->size());
    CHECK(bar);
    CHECK_EQUAL(3, bar->get_column_count());
    CHECK_EQUAL(type_Int, bar->get_column_type(cols[0]));
    CHECK_EQUAL(type_Float, bar->get_column_type(cols[1]));
    CHECK_EQUAL(type_Double, bar->get_column_type(cols[2]));
    CHECK_EQUAL(0, bar->size());
    CHECK_EQUAL(foo, rt->get_table("foo"));
    CHECK_EQUAL(bar, rt->get_table("bar"));
}

TEST(LangBindHelper_AdvanceReadTransact_AddTableWithFreshSharedGroup)
{
    SHARED_GROUP_TEST_PATH(path);

    // Testing that a foreign transaction, that adds a table, can be applied to
    // a freshly created SharedGroup, even when another table existed in the
    // group prior to the one being added in the mentioned transaction. This
    // test is relevant because of the way table accesors are created and
    // managed inside a SharedGroup, in particular because table accessors are
    // created lazily, and will therefore not be present in a freshly created
    // SharedGroup instance.

    // Add the first table
    {
        std::unique_ptr<Replication> hist_w(realm::make_in_realm_history(path));
        DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
        WriteTransaction wt(sg_w);
        wt.add_table("table_1");
        wt.commit();
    }

    // Create a SharedGroup to which we can apply a foreign transaction
    std::unique_ptr<Replication> hist(realm::make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    TransactionRef rt = sg->start_read();

    // Add the second table in a "foreign" transaction
    {
        std::unique_ptr<Replication> hist_w(realm::make_in_realm_history(path));
        DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
        WriteTransaction wt(sg_w);
        wt.add_table("table_2");
        wt.commit();
    }

    rt->advance_read();
}


TEST(LangBindHelper_AdvanceReadTransact_RemoveTableWithFreshSharedGroup)
{
    SHARED_GROUP_TEST_PATH(path);

    // Testing that a foreign transaction, that removes a table, can be applied
    // to a freshly created Sharedrt-> This test is relevant because of the
    // way table accesors are created and managed inside a SharedGroup, in
    // particular because table accessors are created lazily, and will therefore
    // not be present in a freshly created SharedGroup instance.

    // Add the table
    {
        std::unique_ptr<Replication> hist_w(realm::make_in_realm_history(path));
        DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
        WriteTransaction wt(sg_w);
        wt.add_table("table");
        wt.commit();
    }

    // Create a SharedGroup to which we can apply a foreign transaction
    std::unique_ptr<Replication> hist(realm::make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    TransactionRef rt = sg->start_read();

    // remove the table in a "foreign" transaction
    {
        std::unique_ptr<Replication> hist_w(realm::make_in_realm_history(path));
        DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
        WriteTransaction wt(sg_w);
        wt.get_group().remove_table("table");
        wt.commit();
    }

    rt->advance_read();
}


TEST(LangBindHelper_AdvanceReadTransact_CreateManyTables)
{
    SHARED_GROUP_TEST_PATH(path);

    {
        std::unique_ptr<Replication> hist_w(realm::make_in_realm_history(path));
        DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
        WriteTransaction wt(sg_w);
        wt.add_table("table");
        wt.commit();
    }

    std::unique_ptr<Replication> hist(realm::make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    TransactionRef rt = sg->start_read();

    {
        std::unique_ptr<Replication> hist_w(realm::make_in_realm_history(path));
        DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));

        WriteTransaction wt(sg_w);
        for (int i = 0; i < 16; ++i) {
            std::stringstream ss;
            ss << "table_" << i;
            std::string str(ss.str());
            wt.add_table(str);
        }
        wt.commit();
    }

    rt->advance_read();
}


TEST(LangBindHelper_AdvanceReadTransact_InsertTable)
{
    SHARED_GROUP_TEST_PATH(path);

    {
        std::unique_ptr<Replication> hist_w(realm::make_in_realm_history(path));
        DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
        WriteTransaction wt(sg_w);

        TableRef table = wt.add_table("table1");
        table->add_column(type_Int, "col");

        table = wt.add_table("table2");
        table->add_column(type_Float, "col1");
        table->add_column(type_Float, "col2");

        wt.commit();
    }

    std::unique_ptr<Replication> hist(realm::make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    TransactionRef rt = sg->start_read();

    ConstTableRef table1 = rt->get_table("table1");
    ConstTableRef table2 = rt->get_table("table2");

    {
        std::unique_ptr<Replication> hist_w(realm::make_in_realm_history(path));
        DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));

        WriteTransaction wt(sg_w);
        wt.get_group().add_table("new table");

        wt.get_table("table1")->create_object();
        wt.get_table("table2")->create_object();
        wt.get_table("table2")->create_object();

        wt.commit();
    }

    rt->advance_read();

    CHECK_EQUAL(table1->size(), 1);
    CHECK_EQUAL(table2->size(), 2);
    CHECK_EQUAL(rt->get_table("new table")->size(), 0);
}

TEST(LangBindHelper_AdvanceReadTransact_LinkColumnInNewTable)
{
    // Verify that the table accessor of a link-opposite table is refreshed even
    // when the origin table is created in the same transaction as the link
    // column is added to it. This case is slightly involved, as there is a rule
    // that requires the two opposite table accessors of a link column (origin
    // and target sides) to either both exist or both not exist. On the other
    // hand, tables accessors are normally not created during
    // Group::advance_transact() for newly created tables.

    SHARED_GROUP_TEST_PATH(path);
    ShortCircuitHistory hist(path);
    DBRef sg = DB::create(hist, DBOptions(crypt_key()));
    DBRef sg_w = DB::create(hist, DBOptions(crypt_key()));
    {
        WriteTransaction wt(sg_w);
        TableRef a = wt.get_or_add_table("a");
        wt.commit();
    }

    TransactionRef rt = sg->start_read();
    ConstTableRef a_r = rt->get_table("a");

    {
        WriteTransaction wt(sg_w);
        TableRef a_w = wt.get_table("a");
        TableRef b_w = wt.get_or_add_table("b");
        b_w->add_column_link(type_Link, "foo", *a_w);
        wt.commit();
    }
    rt->advance_read();
    rt->verify();
}


TEST(LangBindHelper_AdvanceReadTransact_EnumeratedStrings)
{
    SHARED_GROUP_TEST_PATH(path);
    ShortCircuitHistory hist(path);
    DBRef sg = DB::create(hist, DBOptions(crypt_key()));
    ColKey c0, c1, c2;

    // Start a read transaction (to be repeatedly advanced)
    auto rt = sg->start_read();
    CHECK_EQUAL(0, rt->size());

    // Create 3 string columns, one primed for conversion to "unique string
    // enumeration" representation
    {
        WriteTransaction wt(sg);
        TableRef table_w = wt.add_table("t");
        c0 = table_w->add_column(type_String, "a");
        c1 = table_w->add_column(type_String, "b");
        c2 = table_w->add_column(type_String, "c");
        for (int i = 0; i < 1000; ++i) {
            std::ostringstream out;
            out << i;
            std::string str = out.str();
            table_w->create_object({}, {{c0, str}, {c1, "foo"}, {c2, str}});
        }
        wt.commit();
    }
    rt->advance_read();
    rt->verify();
    ConstTableRef table = rt->get_table("t");
    CHECK_EQUAL(0, table->get_num_unique_values(c0));
    CHECK_EQUAL(0, table->get_num_unique_values(c1)); // Not yet "optimized"
    CHECK_EQUAL(0, table->get_num_unique_values(c2));

    // Optimize
    {
        WriteTransaction wt(sg);
        TableRef table_w = wt.get_table("t");
        table_w->enumerate_string_column(c1);
        wt.commit();
    }
    rt->advance_read();
    rt->verify();
    CHECK_EQUAL(0, table->get_num_unique_values(c0));
    CHECK_NOT_EQUAL(0, table->get_num_unique_values(c1)); // Must be "optimized" now
    CHECK_EQUAL(0, table->get_num_unique_values(c2));
}

TEST(LangBindHelper_AdvanceReadTransact_SearchIndex)
{
    SHARED_GROUP_TEST_PATH(path);
    ShortCircuitHistory hist(path);
    DBRef sg = DB::create(hist, DBOptions(crypt_key()));
    DBRef sg_w = DB::create(hist, DBOptions(crypt_key()));
    ColKey col_int;
    ColKey col_str1;
    ColKey col_str2;
    ColKey col_int3;
    ColKey col_int4;

    // Start a read transaction (to be repeatedly advanced)
    TransactionRef rt = sg->start_read();
    CHECK_EQUAL(0, rt->size());
    std::vector<ObjKey> keys;

    // Create 5 columns, and make 3 of them indexed
    {
        WriteTransaction wt(sg_w);
        TableRef table_w = wt.add_table("t");
        col_int = table_w->add_column(type_Int, "i0");
        col_str1 = table_w->add_column(type_String, "s1");
        col_str2 = table_w->add_column(type_String, "s2");
        col_int3 = table_w->add_column(type_Int, "i3");
        col_int4 = table_w->add_column(type_Int, "i4");
        table_w->add_search_index(col_int);
        table_w->add_search_index(col_str2);
        table_w->add_search_index(col_int4);
        table_w->create_objects(8, keys);
        wt.commit();
    }
    rt->advance_read();
    rt->verify();
    ConstTableRef table = rt->get_table("t");
    CHECK(table->has_search_index(col_int));
    CHECK_NOT(table->has_search_index(col_str1));
    CHECK(table->has_search_index(col_str2));
    CHECK_NOT(table->has_search_index(col_int3));
    CHECK(table->has_search_index(col_int4));

    // Remove the previous search indexes and add 2 new ones
    {
        WriteTransaction wt(sg_w);
        TableRef table_w = wt.get_table("t");
        table_w->create_objects(8, keys);
        table_w->remove_search_index(col_str2);
        table_w->add_search_index(col_int3);
        table_w->remove_search_index(col_int);
        table_w->add_search_index(col_str1);
        table_w->remove_search_index(col_int4);
        wt.commit();
    }
    rt->advance_read();
    rt->verify();
    CHECK_NOT(table->has_search_index(col_int));
    CHECK(table->has_search_index(col_str1));
    CHECK_NOT(table->has_search_index(col_str2));
    CHECK(table->has_search_index(col_int3));
    CHECK_NOT(table->has_search_index(col_int4));

    // Add some searchable contents
    {
        WriteTransaction wt(sg_w);
        TableRef table_w = wt.get_table("t");
        int_fast64_t v = 7;
        for (auto obj : *table_w) {
            std::string out(util::to_string(v));
            obj.set(col_str1, StringData(out));
            obj.set(col_int3, v);
            v = (v + 1581757577LL) % 1000;
        }
        wt.commit();
    }
    rt->advance_read();
    rt->verify();

    CHECK_NOT(table->has_search_index(col_int));
    CHECK(table->has_search_index(col_str1));
    CHECK_NOT(table->has_search_index(col_str2));
    CHECK(table->has_search_index(col_int3));
    CHECK_NOT(table->has_search_index(col_int4));
    CHECK_EQUAL(ObjKey(12), table->find_first_string(col_str1, "931"));
    CHECK_EQUAL(ObjKey(4), table->find_first_int(col_int3, 315));
    CHECK_EQUAL(ObjKey(13), table->find_first_int(col_int3, 508));

    // Move the indexed columns by removal
    {
        WriteTransaction wt(sg_w);
        TableRef table_w = wt.get_table("t");
        table_w->remove_column(col_int);
        table_w->remove_column(col_str2);
        wt.commit();
    }
    rt->advance_read();
    rt->verify();
    CHECK(table->has_search_index(col_str1));
    CHECK(table->has_search_index(col_int3));
    CHECK_NOT(table->has_search_index(col_int4));
    CHECK_EQUAL(ObjKey(3), table->find_first_string(col_str1, "738"));
    CHECK_EQUAL(ObjKey(13), table->find_first_int(col_int3, 508));
}

TEST(LangBindHelper_AdvanceReadTransact_LinkView)
{
    SHARED_GROUP_TEST_PATH(path);
    ShortCircuitHistory hist(path);
    DBRef sg = DB::create(hist, DBOptions(crypt_key()));
    DBRef sg_w = DB::create(hist, DBOptions(crypt_key()));
    DBRef sg_q = DB::create(hist, DBOptions(crypt_key()));

    // Start a continuous read transaction
    TransactionRef rt = sg->start_read();

    // Add some tables and rows.
    {
        WriteTransaction wt(sg_w);
        TableRef origin = wt.add_table("origin");
        TableRef target = wt.add_table("target");
        target->add_column(type_Int, "value");
        auto col = origin->add_column_link(type_LinkList, "list", *target);
        // origin->add_search_index(0);
        std::vector<ObjKey> keys;
        target->create_objects(10, keys);

        Obj o0 = origin->create_object(ObjKey(0));
        Obj o1 = origin->create_object(ObjKey(1));

        o0.get_linklist(col).add(keys[1]);
        o1.get_linklist(col).add(keys[2]);
        // state:
        // origin[0].ll[0] -> target[1]
        // origin[1].ll[0] -> target[2]
        wt.commit();
    }
    rt->advance_read();
    rt->verify();

    // Grab references to the LinkViews
    auto origin = rt->get_table("origin");
    auto col_link = origin->get_column_key("list");
    ConstObj obj0 = origin->get_object(ObjKey(0));
    ConstObj obj1 = origin->get_object(ObjKey(1));

    auto ll1 = obj0.get_linklist(col_link); // lv1[0] -> target[1]
    auto ll2 = obj1.get_linklist(col_link); // lv2[0] -> target[2]
    CHECK_EQUAL(ll1.size(), 1);
    CHECK_EQUAL(ll2.size(), 1);
}

namespace {

template <typename T>
class ConcurrentQueue {
public:
    ConcurrentQueue(size_t size)
        : sz(size)
    {
        data.reset(new T[sz]);
    }
    inline bool is_full()
    {
        return writer - reader == sz;
    }
    inline bool is_empty()
    {
        return writer - reader == 0;
    }
    void put(T& e)
    {
        std::unique_lock<std::mutex> lock(mutex);
        while (is_full())
            not_full.wait(lock);
        if (is_empty())
            not_empty_or_closed.notify_all();
        data[writer++ % sz] = std::move(e);
    }

    bool get(T& e)
    {
        std::unique_lock<std::mutex> lock(mutex);
        while (is_empty() && !closed)
            not_empty_or_closed.wait(lock);
        if (closed)
            return false;
        if (is_full())
            not_full.notify_all();
        e = std::move(data[reader++ % sz]);
        return true;
    }

    void reopen()
    {
        // no concurrent access allowed here
        closed = false;
    }

    void close()
    {
        std::unique_lock<std::mutex> lock(mutex);
        closed = true;
        not_empty_or_closed.notify_all();
    }

private:
    std::mutex mutex;
    std::condition_variable not_full;
    std::condition_variable not_empty_or_closed;
    size_t reader = 0;
    size_t writer = 0;
    bool closed = false;
    size_t sz;
    std::unique_ptr<T[]> data;
};

// Background thread for test below.
void deleter_thread(ConcurrentQueue<LnkLstPtr>& queue)
{
    Random random(random_int<unsigned long>());
    bool closed = false;
    while (!closed) {
        LnkLstPtr r;
        // prevent the compiler from eliminating a loop:
        volatile int delay = random.draw_int_mod(10000);
        closed = !queue.get(r);
        // random delay goes *after* get(), so that it comes
        // after the potentially synchronizing locking
        // operation inside queue.get()
        while (delay > 0)
            delay--;
        // just let 'r' die
    }
}
}

TEST(LangBindHelper_ConcurrentLinkViewDeletes)
{
    // This tests checks concurrent deletion of LinkViews.
    // It is structured as a mutator which creates and uses
    // LinkView accessors, and a background deleter which
    // consumes LinkViewRefs and makes them go out of scope
    // concurrently with the new references being created.

    // Number of table entries (and hence, max number of accessors)
    const int table_size = 1000;

    // Number of references produced (some will refer to the same
    // accessor)
    const int max_refs = 50000;

    // Frequency of references that are used to change the
    // database during the test.
    const int change_frequency_per_mill = 50000; // 5pct changes

    // Number of references that may be buffered for communication
    // between main thread and deleter thread. Should be large enough
    // to allow considerable overlap.
    const int buffer_size = 2000;

    Random random(random_int<unsigned long>());

    // setup two tables with empty linklists inside
    SHARED_GROUP_TEST_PATH(path);
    ShortCircuitHistory hist(path);
    DBRef sg = DB::create(hist, DBOptions(crypt_key()));

    // Start a read transaction (to be repeatedly advanced)
    std::vector<ObjKey> o_keys;
    std::vector<ObjKey> t_keys;
    ColKey ck;
    auto rt = sg->start_read();
    {
        // setup tables with empty linklists
        WriteTransaction wt(sg);
        TableRef origin = wt.add_table("origin");
        TableRef target = wt.add_table("target");
        ck = origin->add_column_link(type_LinkList, "ll", *target);
        origin->create_objects(table_size, o_keys);
        target->create_objects(table_size, t_keys);
        wt.commit();
    }
    rt->advance_read();

    // Create accessors for random entries in the table.
    // occasionally modify the database through the accessor.
    // feed the accessor refs to the background thread for
    // later deletion.
    util::Thread deleter;
    ConcurrentQueue<LnkLstPtr> queue(buffer_size);
    deleter.start([&] { deleter_thread(queue); });
    for (int i = 0; i < max_refs; ++i) {
        TableRef origin = rt->get_table("origin");
        TableRef target = rt->get_table("target");
        int ndx = random.draw_int_mod(table_size);
        Obj o = origin->get_object(o_keys[ndx]);
        LnkLstPtr lw = o.get_linklist_ptr(ck);
        bool will_add = change_frequency_per_mill > random.draw_int_mod(1000000);
        if (will_add) {
            rt->promote_to_write();
            lw->add(t_keys[ndx]);
            rt->commit_and_continue_as_read();
        }
        queue.put(lw);
    }
    queue.close();
    deleter.join();
}

TEST(LangBindHelper_AdvanceReadTransact_InsertLink)
{
    // This test checks that Table::insert_link() works across transaction
    // boundaries (advance transaction).

    SHARED_GROUP_TEST_PATH(path);
    ShortCircuitHistory hist(path);
    DBRef sg = DB::create(hist, DBOptions(crypt_key()));

    // Start a read transaction (to be repeatedly advanced)
    TransactionRef rt = sg->start_read();
    CHECK_EQUAL(0, rt->size());
    ColKey col;
    ObjKey target_key;
    {
        WriteTransaction wt(sg);
        TableRef origin_w = wt.add_table("origin");
        TableRef target_w = wt.add_table("target");
        col = origin_w->add_column_link(type_Link, "", *target_w);
        target_w->add_column(type_Int, "");
        target_key = target_w->create_object().get_key();
        wt.commit();
    }
    rt->advance_read();
    rt->verify();
    ConstTableRef origin = rt->get_table("origin");
    ConstTableRef target = rt->get_table("target");
    {
        WriteTransaction wt(sg);
        TableRef origin_w = wt.get_table("origin");
        auto obj = origin_w->create_object();
        obj.set(col, target_key);
        wt.commit();
    }
    rt->advance_read();
    rt->verify();
}


TEST(LangBindHelper_AdvanceReadTransact_LinkToNeighbour)
{
    // This test checks that you can insert a link to an object that resides
    // in the same cluster as the origin object.

    SHARED_GROUP_TEST_PATH(path);
    ShortCircuitHistory hist(path);
    DBRef sg = DB::create(hist, DBOptions(crypt_key()));

    // Start a read transaction (to be repeatedly advanced)
    TransactionRef rt = sg->start_read();
    CHECK_EQUAL(0, rt->size());
    ColKey col;
    std::vector<ObjKey> keys;
    {
        WriteTransaction wt(sg);
        TableRef table = wt.add_table("table");
        table->add_column(type_Int, "integers");
        col = table->add_column_link(type_Link, "links", *table);
        table->create_objects(10, keys);
        wt.commit();
    }
    rt->advance_read();
    rt->verify();
    {
        WriteTransaction wt(sg);
        TableRef table = wt.get_table("table");
        table->get_object(keys[0]).set(col, keys[1]);
        table->get_object(keys[1]).set(col, keys[2]);
        wt.commit();
    }
    rt->advance_read();
    rt->verify();
}


TEST(LangBindHelper_AdvanceReadTransact_RemoveTableWithColumns)
{
    SHARED_GROUP_TEST_PATH(path);
    ShortCircuitHistory hist(path);
    DBRef sg = DB::create(hist, DBOptions(crypt_key()));
    DBRef sg_w = DB::create(hist, DBOptions(crypt_key()));

    // Start a read transaction (to be repeatedly advanced)
    TransactionRef rt = sg->start_read();
    CHECK_EQUAL(0, rt->size());

    {
        WriteTransaction wt(sg_w);
        TableRef alpha_w = wt.add_table("alpha");
        TableRef beta_w = wt.add_table("beta");
        TableRef gamma_w = wt.add_table("gamma");
        TableRef delta_w = wt.add_table("delta");
        TableRef epsilon_w = wt.add_table("epsilon");
        alpha_w->add_column(type_Int, "alpha-1");
        beta_w->add_column_link(type_Link, "beta-1", *delta_w);
        gamma_w->add_column_link(type_Link, "gamma-1", *gamma_w);
        delta_w->add_column(type_Int, "delta-1");
        epsilon_w->add_column_link(type_Link, "epsilon-1", *delta_w);
        wt.commit();
    }
    rt->advance_read();
    rt->verify();

    CHECK_EQUAL(5, rt->size());
    ConstTableRef alpha = rt->get_table("alpha");
    ConstTableRef beta = rt->get_table("beta");
    ConstTableRef gamma = rt->get_table("gamma");
    ConstTableRef delta = rt->get_table("delta");
    ConstTableRef epsilon = rt->get_table("epsilon");

    // Remove table with columns, but no link columns, and table is not a link
    // target.
    {
        WriteTransaction wt(sg_w);
        wt.get_group().remove_table("alpha");
        wt.commit();
    }
    rt->advance_read();
    rt->verify();

    CHECK_EQUAL(4, rt->size());
    CHECK_NOT(alpha);
    CHECK(beta);
    CHECK(gamma);
    CHECK(delta);
    CHECK(epsilon);

    // Remove table with link column, and table is not a link target.
    {
        WriteTransaction wt(sg_w);
        wt.get_group().remove_table("beta");
        wt.commit();
    }
    rt->advance_read();
    rt->verify();

    CHECK_EQUAL(3, rt->size());
    CHECK_NOT(beta);
    CHECK(gamma);
    CHECK(delta);
    CHECK(epsilon);

    // Remove table with self-link column, and table is not a target of link
    // columns of other tables.
    {
        WriteTransaction wt(sg_w);
        wt.get_group().remove_table("gamma");
        wt.commit();
    }
    rt->advance_read();
    rt->verify();

    CHECK_EQUAL(2, rt->size());
    CHECK_NOT(gamma);
    CHECK(delta);
    CHECK(epsilon);

    // Try, but fail to remove table which is a target of link columns of other
    // tables.
    {
        WriteTransaction wt(sg_w);
        CHECK_THROW(wt.get_group().remove_table("delta"), CrossTableLinkTarget);
        wt.commit();
    }
    rt->advance_read();
    rt->verify();

    CHECK_EQUAL(2, rt->size());
    CHECK(delta);
    CHECK(epsilon);
}

TEST(LangBindHelper_AdvanceReadTransact_CascadeRemove_ColumnLink)
{
    SHARED_GROUP_TEST_PATH(path);
    ShortCircuitHistory hist(path);
    DBRef sg = DB::create(hist, DBOptions(crypt_key()));

    ColKey col;
    {
        WriteTransaction wt(sg);
        auto origin = wt.add_table("origin");
        auto target = wt.add_table("target");
        col = origin->add_column_link(type_Link, "o_1", *target, link_Strong);
        target->add_column(type_Int, "t_1");
        wt.commit();
    }

    // Start a read transaction (to be repeatedly advanced)
    auto rt = sg->start_read();
    auto target = rt->get_table("target");

    ObjKey target_key0, target_key1;
    ConstObj target_obj0, target_obj1;

    auto perform_change = [&](std::function<void(Table&)> func) {
        // Ensure there are two rows in each table, with each row in `origin`
        // pointing to the corresponding row in `target`
        {
            WriteTransaction wt(sg);
            auto origin_w = wt.get_table("origin");
            auto target_w = wt.get_table("target");

            origin_w->clear();
            target_w->clear();
            auto o0 = origin_w->create_object();
            auto o1 = origin_w->create_object();
            auto t0 = target_w->create_object();
            auto t1 = target_w->create_object();
            target_key0 = t0.get_key();
            target_key1 = t1.get_key();
            o0.set(col, target_key0);
            o1.set(col, target_key1);
            wt.commit();
        }

        // Grab the row accessors before applying the modification being tested
        rt->advance_read();
        rt->verify();
        target_obj0 = target->get_object(target_key0);
        target_obj1 = target->get_object(target_key1);

        // Perform the modification
        {
            WriteTransaction wt(sg);
            func(*wt.get_table("origin"));
            wt.commit();
        }

        rt->advance_read();
        rt->verify();
        // Leave `group` and the target accessors in a state which can be tested
        // with the changes applied
    };

    // Break link by clearing table
    perform_change([](Table& origin) { origin.clear(); });
    CHECK(!target_obj0.is_valid());
    CHECK(!target_obj1.is_valid());
    CHECK_EQUAL(target->size(), 0);

    // Break link by nullifying
    perform_change([&](Table& origin) { origin.get_object(1).set_null(col); });
    CHECK(target_obj0.is_valid());
    CHECK(!target_obj1.is_valid());
    CHECK_EQUAL(target->size(), 1);

    // Break link by reassign
    perform_change([&](Table& origin) { origin.get_object(1).set(col, target_key0); });
    CHECK(target_obj0.is_valid());
    CHECK(!target_obj1.is_valid());
    CHECK_EQUAL(target->size(), 1);

    // Avoid breaking link by reassigning self
    perform_change([&](Table& origin) { origin.get_object(1).set(col, target_key1); });
    // Should not delete anything
    CHECK(target_obj0.is_valid());
    CHECK(target_obj1.is_valid());
    CHECK_EQUAL(target->size(), 2);
}

// to be ported
TEST(LangBindHelper_AdvanceReadTransact_CascadeRemove_ColumnLinkList)
{
    /*
    SHARED_GROUP_TEST_PATH(path);
    ShortCircuitHistory hist(path);
    DBRef sg = DB::create(hist, DBOptions(crypt_key()));
    DBRef sg_w = DB::create(hist, DBOptions(crypt_key()));

    {
        WriteTransaction wt(sg_w);
        Table& origin = *wt.add_table("origin");
        Table& target = *wt.add_table("target");
        origin.add_column_link(type_LinkList, "o_1", target, link_Strong);
        target.add_column(type_Int, "t_1");
        wt.commit();
    }

    // Start a read transaction (to be repeatedly advanced)
    TransactionRef rt = sg->start_read() const Group& group = rt;
    const Table& target = *rt->get_table("target");

    ConstRow target_row_0, target_row_1;

    auto perform_change = [&](std::function<void(Table&)> func) {
        // Ensure there are two rows in each table, with the first row in `origin`
        // linking to the first row in `target`, and the second row in `origin`
        // linking to both rows in `target`
        {
            WriteTransaction wt(sg_w);
            Table& origin_w = *wt.get_table("origin");
            Table& target_w = *wt.get_table("target");

            origin_w.clear();
            target_w.clear();
            origin_w.add_empty_row(2);
            target_w.add_empty_row(2);
            origin_w[0].get_linklist(0)->add(0);
            origin_w[1].get_linklist(0)->add(0);
            origin_w[1].get_linklist(0)->add(1);


            wt.commit();
        }

        // Grab the row accessors before applying the modification being tested
        rt->advance_read();
        rt->verify();
        target_row_0 = target.get(0);
        target_row_1 = target.get(1);

        // Perform the modification
        {
            WriteTransaction wt(sg_w);
            func(*wt.get_table("origin"));
            wt.commit();
        }

        rt->advance_read();
        rt->verify();
        // Leave `group` and the target accessors in a state which can be tested
        // with the changes applied
    };
*/
    SHARED_GROUP_TEST_PATH(path);
    ShortCircuitHistory hist(path);
    DBRef sg = DB::create(hist, DBOptions(crypt_key()));

    ColKey col;
    {
        WriteTransaction wt(sg);
        auto origin = wt.add_table("origin");
        auto target = wt.add_table("target");
        col = origin->add_column_link(type_LinkList, "o_1", *target, link_Strong);
        target->add_column(type_Int, "t_1");
        wt.commit();
    }

    // Start a read transaction (to be repeatedly advanced)
    auto rt = sg->start_read();
    auto target = rt->get_table("target");

    ObjKey target_key0, target_key1;
    ConstObj target_obj0, target_obj1;

    auto perform_change = [&](std::function<void(Table&)> func) {
        // Ensure there are two rows in each table, with each row in `origin`
        // pointing to the corresponding row in `target`
        {
            WriteTransaction wt(sg);
            auto origin_w = wt.get_table("origin");
            auto target_w = wt.get_table("target");

            origin_w->clear();
            target_w->clear();
            auto o0 = origin_w->create_object();
            auto o1 = origin_w->create_object();
            auto t0 = target_w->create_object();
            auto t1 = target_w->create_object();
            target_key0 = t0.get_key();
            target_key1 = t1.get_key();
            o0.get_linklist(col).add(target_key0);
            o1.get_linklist(col).add(target_key0);
            o1.get_linklist(col).add(target_key1);
            wt.commit();
        }

        // Grab the row accessors before applying the modification being tested
        rt->advance_read();
        rt->verify();
        target_obj0 = target->get_object(target_key0);
        target_obj1 = target->get_object(target_key1);

        // Perform the modification
        {
            WriteTransaction wt(sg);
            func(*wt.get_table("origin"));
            wt.commit();
        }

        rt->advance_read();
        rt->verify();
        // Leave `group` and the target accessors in a state which can be tested
        // with the changes applied
    };


    // Break link by clearing list
    perform_change([=](Table& origin) { origin.get_object(1).get_linklist(col).clear(); });
    CHECK(target_obj0.is_valid() && !target_obj1.is_valid());
    CHECK_EQUAL(target->size(), 1);

    // Break link by removal from list
    perform_change([=](Table& origin) { origin.get_object(1).get_linklist(col).remove(1); });
    CHECK(target_obj0.is_valid() && !target_obj1.is_valid());
    CHECK_EQUAL(target->size(), 1);

    // Break link by reassign
    perform_change([=](Table& origin) { origin.get_object(1).get_linklist(col).set(1, target_key0); });
    CHECK(target_obj0.is_valid() && !target_obj1.is_valid());
    CHECK_EQUAL(target->size(), 1);

    // Avoid breaking link by reassigning self
    perform_change([=](Table& origin) { origin.get_object(1).get_linklist(col).set(1, target_key1); });
    // Should not delete anything
    CHECK(target_obj0.is_valid() && target_obj1.is_valid());
    CHECK_EQUAL(target->size(), 2);

    // Break link by clearing table
    perform_change([](Table& origin) { origin.clear(); });
    CHECK(!target_obj0.is_valid() && !target_obj1.is_valid());
    CHECK_EQUAL(target->size(), 0);
}


TEST(LangBindHelper_AdvanceReadTransact_IntIndex)
{
    SHARED_GROUP_TEST_PATH(path);

    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    auto g = sg->start_read();
    g->promote_to_write();

    TableRef target = g->add_table("target");
    auto col = target->add_column(type_Int, "pk");
    target->add_search_index(col);

    std::vector<ObjKey> obj_keys;
    target->create_objects(REALM_MAX_BPNODE_SIZE + 1, obj_keys);

    g->commit_and_continue_as_read();

    // open a second copy that'll be advanced over the write
    auto g_r = sg->start_read();
    TableRef t_r = g_r->get_table("target");

    g->promote_to_write();

    // Ensure that the index has a different bptree layout so that failing to
    // refresh it will do bad things
    int i = 0;
    for (auto it = target->begin(); it != target->end(); ++it)
        it->set(col, i++);

    g->commit_and_continue_as_read();

    g_r->promote_to_write();
    // Crashes if index has an invalid parent ref
    t_r->clear();
}

TEST(LangBindHelper_AdvanceReadTransact_TableClear)
{
    SHARED_GROUP_TEST_PATH(path);

    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    ColKey col;
    {
        WriteTransaction wt(sg);
        TableRef table = wt.add_table("table");
        col = table->add_column(type_Int, "col");
        table->create_object();
        wt.commit();
    }

    auto reader = sg->start_read();
    auto table = reader->get_table("table");
    TableView tv = table->where().find_all();
    auto obj = *table->begin();
    CHECK(obj.is_valid());

    {
        std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
        DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));

        WriteTransaction wt(sg_w);
        wt.get_table("table")->clear();
        wt.commit();
    }

    reader->advance_read();

    CHECK(!obj.is_valid());

    CHECK_EQUAL(tv.size(), 1);
    CHECK(!tv.is_in_sync());
    // key is still there...
    CHECK(tv.get_key(0));
    // but no obj for that key...
    CHECK_THROW(tv.get(0), realm::InvalidKey);

    tv.sync_if_needed();
    CHECK_EQUAL(tv.size(), 0);
}

TEST(LangBindHelper_AdvanceReadTransact_UnorderedTableViewClear)
{
    SHARED_GROUP_TEST_PATH(path);

    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    ObjKey first_obj, last_obj;
    ColKey col;
    {
        WriteTransaction wt(sg);
        TableRef table = wt.add_table("table");
        col = table->add_column(type_Int, "col");
        first_obj = table->create_object().set_all(0).get_key();
        table->create_object().set_all(1);
        last_obj = table->create_object().set_all(2).get_key();
        wt.commit();
    }

    auto reader = sg->start_read();
    auto table = reader->get_table("table");
    auto obj = table->get_object(last_obj);
    CHECK_EQUAL(obj.get<int64_t>(col), 2);

    {
        // Remove the first row via unordered removal, resulting in the '2' row
        // moving to index 0 (with ordered removal it would instead move to index 1)
        WriteTransaction wt(sg);
        wt.get_table("table")->where().equal(col, 0).find_all().clear();
        wt.commit();
    }

    reader->advance_read();

    CHECK(obj.is_valid());
    CHECK_EQUAL(obj.get<int64_t>(col), 2);
}

namespace {
// A base class for transaction log parsers so that tests which want to test
// just a single part of the transaction log handling don't have to implement
// the entire interface
class NoOpTransactionLogParser {
public:
    NoOpTransactionLogParser(TestContext& context)
        : test_context(context)
    {
    }

    TableKey get_current_table() const
    {
        return m_current_table;
    }

    std::pair<ColKey, ObjKey> get_current_linkview() const
    {
        return {m_current_linkview_col, m_current_linkview_row};
    }

protected:
    TestContext& test_context;

private:
    TableKey m_current_table;
    ColKey m_current_linkview_col;
    ObjKey m_current_linkview_row;

public:
    void parse_complete()
    {
    }

    bool select_table(TableKey t)
    {
        m_current_table = t;
        return true;
    }

    bool select_link_list(ColKey col_key, ObjKey obj_key, size_t)
    {
        m_current_linkview_col = col_key;
        m_current_linkview_row = obj_key;
        return true;
    }

    bool select_list(ColKey col_key, ObjKey obj_key)
    {
        m_current_linkview_col = col_key;
        m_current_linkview_row = obj_key;
        return true;
    }

    // subtables not supported
    bool select_descriptor(int, const size_t*)
    {
        return false;
    }

    // Default no-op implmentations of all of the mutation instructions
    bool insert_group_level_table(TableKey)
    {
        return false;
    }
    bool erase_group_level_table(TableKey)
    {
        return false;
    }
    bool rename_group_level_table(TableKey)
    {
        return false;
    }
    bool insert_column(ColKey)
    {
        return false;
    }
    bool insert_link_column(ColKey, DataType, StringData, size_t, size_t)
    {
        return false;
    }
    bool erase_column(ColKey)
    {
        return false;
    }
    bool erase_link_column(size_t, size_t, size_t)
    {
        return false;
    }
    bool rename_column(ColKey)
    {
        return false;
    }
    bool add_search_index(size_t)
    {
        return false;
    }
    bool remove_search_index(size_t)
    {
        return false;
    }
    bool add_primary_key(size_t)
    {
        return false;
    }
    bool remove_primary_key()
    {
        return false;
    }
    bool set_link_type(ColKey)
    {
        return false;
    }
    bool create_object(ObjKey)
    {
        return false;
    }
    bool add_row_with_key(size_t, size_t, size_t, int64_t)
    {
        return false;
    }
    bool remove_object(ObjKey)
    {
        return false;
    }
    bool swap_rows(size_t, size_t)
    {
        return false;
    }
    bool move_row(size_t, size_t)
    {
        return false;
    }
    bool clear_table(size_t) noexcept
    {
        return false;
    }
    bool list_set(size_t)
    {
        return false;
    }
    bool list_clear(size_t)
    {
        return false;
    }
    bool list_erase(size_t)
    {
        return false;
    }
    bool link_list_nullify(size_t, size_t)
    {
        return false;
    }
    bool list_insert(size_t)
    {
        return false;
    }
    bool list_move(size_t, size_t)
    {
        return false;
    }
    bool list_swap(size_t, size_t)
    {
        return false;
    }
    bool modify_object(ColKey, ObjKey)
    {
        return false;
    }
    bool add_int(size_t, size_t, int_fast64_t)
    {
        return false;
    }
    bool set_bool(size_t, size_t, bool, _impl::Instruction)
    {
        return false;
    }
    bool set_float(size_t, size_t, float, _impl::Instruction)
    {
        return false;
    }
    bool set_double(size_t, size_t, double, _impl::Instruction)
    {
        return false;
    }
    bool set_string(size_t, size_t, StringData, _impl::Instruction, size_t)
    {
        return false;
    }
    bool set_binary(size_t, size_t, BinaryData, _impl::Instruction)
    {
        return false;
    }
    bool set_timestamp(size_t, size_t, Timestamp, _impl::Instruction)
    {
        return false;
    }
    bool set_table(size_t, size_t, _impl::Instruction)
    {
        return false;
    }
    bool set_mixed(size_t, size_t, const Mixed&, _impl::Instruction)
    {
        return false;
    }
    bool set_link(size_t, size_t, size_t, size_t, _impl::Instruction)
    {
        return false;
    }
    bool set_null(size_t, size_t, _impl::Instruction, size_t)
    {
        return false;
    }
    bool nullify_link(size_t, size_t, size_t)
    {
        return false;
    }
    bool insert_substring(size_t, size_t, size_t, StringData)
    {
        return false;
    }
    bool erase_substring(size_t, size_t, size_t, size_t)
    {
        return false;
    }
    bool optimize_table()
    {
        return false;
    }
};

struct AdvanceReadTransact {
    template <typename Func>
    static void call(TransactionRef tr, Func* func)
    {
        tr->advance_read(func);
    }
};

struct PromoteThenRollback {
    template <typename Func>
    static void call(TransactionRef tr, Func* func)
    {
        tr->promote_to_write(func);
        tr->rollback_and_continue_as_read();
    }
};

} // unnamed namespace

TEST_TYPES(LangBindHelper_AdvanceReadTransact_TransactLog, AdvanceReadTransact, PromoteThenRollback)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    ColKey c0, c1;
    {
        WriteTransaction wt(sg);
        c0 = wt.add_table("table 1")->add_column(type_Int, "int");
        c1 = wt.add_table("table 2")->add_column(type_Int, "int");
        wt.commit();
    }

    auto tr = sg->start_read();

    {
        // With no changes, the handler should not be called at all
        struct : NoOpTransactionLogParser {
            using NoOpTransactionLogParser::NoOpTransactionLogParser;
            void parse_complete()
            {
                CHECK(false);
            }
        } parser(test_context);
        TEST_TYPE::call(tr, &parser);
    }

    {
        // With an empty change, parse_complete() and nothing else should be called
        auto wt = sg->start_write();
        wt->commit();

        struct foo : NoOpTransactionLogParser {
            using NoOpTransactionLogParser::NoOpTransactionLogParser;

            bool called = false;
            void parse_complete()
            {
                called = true;
            }
        } parser(test_context);
        TEST_TYPE::call(tr, &parser);
        CHECK(parser.called);
    }
    ObjKey o0, o1;
    {
        // Make a simple modification and verify that the appropriate handler is called
        struct foo : NoOpTransactionLogParser {
            using NoOpTransactionLogParser::NoOpTransactionLogParser;

            size_t expected_table = 0;
            TableKey t1;
            TableKey t2;

            bool create_object(ObjKey)
            {
                CHECK_EQUAL(expected_table ? t2 : t1, get_current_table());
                ++expected_table;

                return true;
            }
        } parser(test_context);

        WriteTransaction wt(sg);
        parser.t1 = wt.get_table("table 1")->get_key();
        parser.t2 = wt.get_table("table 2")->get_key();
        o0 = wt.get_table("table 1")->create_object().get_key();
        o1 = wt.get_table("table 2")->create_object().get_key();
        wt.commit();

        TEST_TYPE::call(tr, &parser);
        CHECK_EQUAL(2, parser.expected_table);
    }
    ColKey c2, c3;
    ObjKey okey;
    {
        // Add a table with some links
        WriteTransaction wt(sg);
        TableRef table = wt.add_table("link origin");
        c2 = table->add_column_link(type_Link, "link", *wt.get_table("table 1"));
        c3 = table->add_column_link(type_LinkList, "linklist", *wt.get_table("table 2"));
        Obj o = table->create_object();
        o.set(c2, o.get_key());
        o.get_linklist(c3).add(o.get_key());
        okey = o.get_key();
        wt.commit();

        tr->advance_read();
    }
    {
        // Verify that deleting the targets of the links logs link nullifications
        WriteTransaction wt(sg);
        wt.get_table("table 1")->remove_object(o0);
        wt.get_table("table 2")->remove_object(o1);
        wt.commit();

        struct : NoOpTransactionLogParser {
            using NoOpTransactionLogParser::NoOpTransactionLogParser;

            bool remove_object(ObjKey o)
            {
                CHECK(o == o1 || o == o0);
                return true;
            }
            bool select_link_list(ColKey col, ObjKey o)
            {
                CHECK(col == link_list_col);
                CHECK(o == okey);
                return true;
            }
            bool list_erase(size_t ndx)
            {
                CHECK(ndx == 0);
                return true;
            }

            bool modify_object(ColKey col, ObjKey obj)
            {
                CHECK(col == link_col && obj == okey);
                return true;
            }
            ObjKey o0, o1, okey;
            ColKey link_col, link_list_col;
        } parser(test_context);
        parser.o1 = o1;
        parser.o0 = o0;
        parser.okey = okey;
        parser.link_col = c2;
        parser.link_list_col = c3;
        TEST_TYPE::call(tr, &parser);
    }
    {
        // Verify that clear() logs the correct rows
        WriteTransaction wt(sg);
        std::vector<ObjKey> keys;
        wt.get_table("table 2")->create_objects(10, keys);

        auto lv = wt.get_table("link origin")->begin()->get_linklist(c3);
        lv.add(keys[1]);
        lv.add(keys[3]);
        lv.add(keys[5]);

        wt.commit();
        tr->advance_read();
    }
    {
        WriteTransaction wt(sg);
        wt.get_table("link origin")->begin()->get_linklist(c3).clear();
        wt.commit();
        struct : NoOpTransactionLogParser {
            using NoOpTransactionLogParser::NoOpTransactionLogParser;

            bool list_clear(size_t old_size) const
            {
                CHECK_EQUAL(3, old_size);
                return true;
            }
        } parser(test_context);
        TEST_TYPE::call(tr, &parser);
    }
}


TEST(LangBindHelper_AdvanceReadTransact_ErrorInObserver)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    ColKey col;
    Obj obj;
    // Add some initial data and then begin a read transaction at that version
    {
        WriteTransaction wt(sg);
        TableRef table = wt.add_table("Table");
        col = table->add_column(type_Int, "int");
        obj = table->create_object().set_all(10);
        wt.commit();
    }
    auto g = sg->start_read();

    // Modify the data with a different SG so that we can determine which version
    // the read transaction is using
    {
        auto wt = sg->start_write();
        Obj o2 = wt->import_copy_of(obj);
        o2.set<int64_t>(col, 20);
        wt->commit();
    }

    struct ObserverError {
    };
    try {
        struct : NoOpTransactionLogParser {
            using NoOpTransactionLogParser::NoOpTransactionLogParser;

            bool modify_object(ColKey, ObjKey) const
            {
                throw ObserverError();
            }
        } parser(test_context);
        g->advance_read(&parser);
        CHECK(false); // Should not be reached
    }
    catch (ObserverError) {
    }

    // Should still see data from old version
    auto o = g->import_copy_of(obj);
    CHECK_EQUAL(10, o.get<int64_t>(col));

    // Should be able to advance to the new version still
    g->advance_read();

    // And see that version's data
    CHECK_EQUAL(20, o.get<int64_t>(col));
}


TEST(LangBindHelper_ImplicitTransactions)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    ObjKey o;
    ColKey col;
    {
        WriteTransaction wt(sg);
        auto table = wt.add_table("table");
        col = table->add_column(type_Int, "first");
        table->add_column(type_Int, "second");
        table->add_column(type_Bool, "third");
        table->add_column(type_String, "fourth");
        o = table->create_object().get_key();
        wt.commit();
    }
    auto g = sg->start_read();
    auto table = g->get_table("table");
    for (int i = 0; i < 100; i++) {
        {
            // change table in other context
            WriteTransaction wt(sg);
            wt.get_table("table")->get_object(o).add_int(col, 100);
            wt.commit();
        }
        // verify we can't see the update
        CHECK_EQUAL(i, table->get_object(o).get<int64_t>(col));
        g->advance_read();
        // now we CAN see it, and through the same accessor
        CHECK(table);
        CHECK_EQUAL(i + 100, table->get_object(o).get<int64_t>(col));
        {
            // change table in other context
            WriteTransaction wt(sg);
            wt.get_table("table")->get_object(o).add_int(col, 10000);
            wt.commit();
        }
        // can't see it:
        CHECK_EQUAL(i + 100, table->get_object(o).get<int64_t>(col));
        g->promote_to_write();
        // CAN see it:
        CHECK(table);
        CHECK_EQUAL(i + 10100, table->get_object(o).get<int64_t>(col));
        table->get_object(o).add_int(col, -10100);
        table->get_object(o).add_int(col, 1);
        g->commit_and_continue_as_read();
        CHECK(table);
        CHECK_EQUAL(i + 1, table->get_object(o).get<int64_t>(col));
    }
    g->end_read();
}


TEST(LangBindHelper_RollbackAndContinueAsRead)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    {
        ObjKey key;
        ColKey col;
        auto group = sg->start_read();
        {
            group->promote_to_write();
            TableRef origin = group->get_or_add_table("origin");
            col = origin->add_column(type_Int, "");
            key = origin->create_object().set_all(42).get_key();
            group->commit_and_continue_as_read();
        }
        group->verify();
        {
            // rollback of group level table insertion
            group->promote_to_write();
            TableRef o = group->get_or_add_table("nullermand");
            TableRef o2 = group->get_table("nullermand");
            REALM_ASSERT(o2);
            group->rollback_and_continue_as_read();
            TableRef o3 = group->get_table("nullermand");
            REALM_ASSERT(!o3);
            REALM_ASSERT(!o2);
        }

        TableRef origin = group->get_table("origin");
        Obj row = origin->get_object(key);
        CHECK_EQUAL(42, row.get<int64_t>(col));

        {
            group->promote_to_write();
            auto row2 = origin->create_object().set_all(5746);
            CHECK_EQUAL(42, row.get<int64_t>(col));
            CHECK_EQUAL(5746, row2.get<int64_t>(col));
            CHECK_EQUAL(2, origin->size());
            group->verify();
            group->rollback_and_continue_as_read();
        }
        CHECK_EQUAL(1, origin->size());
        group->verify();
        CHECK_EQUAL(42, row.get<int64_t>(col));
        Obj row2;
        {
            group->promote_to_write();
            row2 = origin->create_object().set_all(42);
            group->commit_and_continue_as_read();
        }
        CHECK_EQUAL(2, origin->size());
        group->verify();
        CHECK_EQUAL(42, row2.get<int64_t>(col));
        group->end_read();
    }
}


TEST(LangBindHelper_RollbackAndContinueAsReadGroupLevelTableRemoval)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    auto reader = sg->start_read();
    {
        reader->promote_to_write();
        TableRef origin = reader->get_or_add_table("a_table");
        reader->commit_and_continue_as_read();
    }
    reader->verify();
    {
        // rollback of group level table delete
        reader->promote_to_write();
        TableRef o2 = reader->get_table("a_table");
        REALM_ASSERT(o2);
        reader->remove_table("a_table");
        TableRef o3 = reader->get_table("a_table");
        REALM_ASSERT(!o3);
        reader->rollback_and_continue_as_read();
        TableRef o4 = reader->get_table("a_table");
        REALM_ASSERT(o4);
    }
    reader->verify();
}

TEST(LangBindHelper_RollbackCircularReferenceRemoval)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    ColKey ca, cb;
    auto group = sg->start_read();
    {
        group->promote_to_write();
        TableRef alpha = group->get_or_add_table("alpha");
        TableRef beta = group->get_or_add_table("beta");
        ca = alpha->add_column_link(type_Link, "beta-1", *beta);
        cb = beta->add_column_link(type_Link, "alpha-1", *alpha);
        group->commit_and_continue_as_read();
    }
    group->verify();
    {
        group->promote_to_write();
        CHECK_EQUAL(2, group->size());
        TableRef alpha = group->get_table("alpha");
        TableRef beta = group->get_table("beta");

        CHECK_THROW(group->remove_table("alpha"), CrossTableLinkTarget);
        beta->remove_column(cb);
        alpha->remove_column(ca);
        group->remove_table("beta");
        CHECK_NOT(group->has_table("beta"));

        // Version 1: This crashes
        group->rollback_and_continue_as_read();
        CHECK_EQUAL(2, group->size());

        //        // Version 2: This works
        //        LangBindHelper::commit_and_continue_as_read(sg);
        //        CHECK_EQUAL(1, group->size());
    }
    group->verify();
}


TEST(LangBindHelper_RollbackAndContinueAsReadColumnAdd)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    auto group = sg->start_read();
    TableRef t;
    {
        group->promote_to_write();
        t = group->get_or_add_table("a_table");
        t->add_column(type_Int, "lorelei");
        t->create_object().set_all(43);
        CHECK_EQUAL(1, t->get_column_count());
        group->commit_and_continue_as_read();
    }
    group->verify();
    {
        // add a column and regret it again
        group->promote_to_write();
        auto col = t->add_column(type_Int, "riget");
        t->begin()->set(col, 44);
        CHECK_EQUAL(2, t->get_column_count());
        group->verify();
        group->rollback_and_continue_as_read();
        group->verify();
        CHECK_EQUAL(1, t->get_column_count());
    }
    group->verify();
}


// This issue was uncovered while looking into the RollbackCircularReferenceRemoval issue
TEST(LangBindHelper_TableLinkingRemovalIssue)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    auto group = sg->start_read();
    {
        group->promote_to_write();
        TableRef t1 = group->get_or_add_table("t1");
        TableRef t2 = group->get_or_add_table("t2");
        TableRef t3 = group->get_or_add_table("t3");
        TableRef t4 = group->get_or_add_table("t4");
        t1->add_column_link(type_Link, "l12", *t2);
        t2->add_column_link(type_Link, "l23", *t3);
        t3->add_column_link(type_Link, "l34", *t4);
        group->commit_and_continue_as_read();
    }
    group->verify();
    {
        group->promote_to_write();
        CHECK_EQUAL(4, group->size());

        group->remove_table("t1");
        group->remove_table("t2");
        group->remove_table("t3"); // CRASHES HERE
        group->remove_table("t4");

        group->rollback_and_continue_as_read();
        CHECK_EQUAL(4, group->size());
    }
    group->verify();
}


// This issue was uncovered while looking into the RollbackCircularReferenceRemoval issue
TEST(LangBindHelper_RollbackTableRemove)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    auto group = sg->start_read();
    {
        group->promote_to_write();
        TableRef alpha = group->get_or_add_table("alpha");
        TableRef beta = group->get_or_add_table("beta");
        beta->add_column_link(type_Link, "alpha-1", *alpha);
        group->commit_and_continue_as_read();
    }
    group->verify();
    {
        group->promote_to_write();
        CHECK_EQUAL(2, group->size());
        TableRef alpha = group->get_table("alpha");
        TableRef beta = group->get_table("beta");
        group->remove_table("beta");
        CHECK_NOT(group->has_table("beta"));
        group->rollback_and_continue_as_read();
        CHECK_EQUAL(2, group->size());
    }
    group->verify();
}

TEST(LangBindHelper_RollbackTableRemove2)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    auto group = sg->start_read();
    {
        group->promote_to_write();
        TableRef a = group->get_or_add_table("a");
        TableRef b = group->get_or_add_table("b");
        TableRef c = group->get_or_add_table("c");
        TableRef d = group->get_or_add_table("d");
        c->add_column_link(type_Link, "a", *a);
        d->add_column_link(type_Link, "b", *b);
        group->commit_and_continue_as_read();
    }
    group->verify();
    {
        group->promote_to_write();
        CHECK_EQUAL(4, group->size());
        group->remove_table("c");
        CHECK_NOT(group->has_table("c"));
        group->verify();
        group->rollback_and_continue_as_read();
        CHECK_EQUAL(4, group->size());
    }
    group->verify();
}

TEST(LangBindHelper_ContinuousTransactions_RollbackTableRemoval)
{
    // Test that it is possible to modify a table, then remove it from the
    // group, and then rollback the transaction.

    // This triggered a bug in the instruction reverser which would incorrectly
    // associate the table removal instruction with the table selection
    // instruction induced by the modification, causing the latter to occur in
    // the reverse log at a point where the selected table does not yet
    // exist. The filler table is there to avoid an early-out in
    // Group::TransactAdvancer::select_table() due to a misinterpretation of the
    // reason for the missing table accessor entry.

    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    auto group = sg->start_read();
    group->promote_to_write();
    TableRef filler = group->get_or_add_table("filler");
    TableRef table = group->get_or_add_table("table");
    auto col = table->add_column(type_Int, "i");
    Obj o = table->create_object();
    group->commit_and_continue_as_read();
    group->promote_to_write();
    o.set<int>(col, 0);
    group->remove_table("table");
    group->rollback_and_continue_as_read();
}

TEST(LangBindHelper_RollbackAndContinueAsReadLinkColumnRemove)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    auto group = sg->start_read();
    TableRef t, t2;
    ColKey col;
    {
        // add a column
        group->promote_to_write();
        t = group->get_or_add_table("a_table");
        t2 = group->get_or_add_table("b_table");
        col = t->add_column_link(type_Link, "bruno", *t2);
        CHECK_EQUAL(1, t->get_column_count());
        group->commit_and_continue_as_read();
    }
    group->verify();
    {
        // ... but then regret it
        group->promote_to_write();
        t->remove_column(col);
        CHECK_EQUAL(0, t->get_column_count());
        group->rollback_and_continue_as_read();
    }
}


TEST(LangBindHelper_RollbackAndContinueAsReadColumnRemove)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    auto group = sg->start_read();
    TableRef t;
    ColKey col;
    {
        group->promote_to_write();
        t = group->get_or_add_table("a_table");
        col = t->add_column(type_Int, "lorelei");
        t->add_column(type_Int, "riget");
        t->create_object().set_all(43, 44);
        CHECK_EQUAL(2, t->get_column_count());
        group->commit_and_continue_as_read();
    }
    group->verify();
    {
        // remove a column but regret it
        group->promote_to_write();
        CHECK_EQUAL(2, t->get_column_count());
        t->remove_column(col);
        group->verify();
        group->rollback_and_continue_as_read();
        group->verify();
        CHECK_EQUAL(2, t->get_column_count());
    }
    group->verify();
}


TEST(LangBindHelper_RollbackAndContinueAsReadLinkList)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    auto group = sg->start_read();
    group->promote_to_write();
    TableRef origin = group->add_table("origin");
    TableRef target = group->add_table("target");
    auto col0 = origin->add_column_link(type_LinkList, "", *target);
    target->add_column(type_Int, "");
    auto o0 = origin->create_object();
    auto t0 = target->create_object();
    auto t1 = target->create_object();
    auto t2 = target->create_object();

    auto link_list = o0.get_linklist(col0);
    link_list.add(t0.get_key());
    group->commit_and_continue_as_read();
    CHECK_EQUAL(1, link_list.size());
    group->verify();
    // now change a link in link list and roll back the change
    group->promote_to_write();
    link_list.add(t1.get_key());
    link_list.add(t2.get_key());
    CHECK_EQUAL(3, link_list.size());
    group->rollback_and_continue_as_read();
    CHECK_EQUAL(1, link_list.size());
    group->promote_to_write();
    link_list.remove(0);
    CHECK_EQUAL(0, link_list.size());
    group->rollback_and_continue_as_read();
    CHECK_EQUAL(1, link_list.size());
}


TEST(LangBindHelper_RollbackAndContinueAsRead_Links)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    auto group = sg->start_read();
    group->promote_to_write();
    TableRef origin = group->add_table("origin");
    TableRef target = group->add_table("target");
    auto col0 = origin->add_column_link(type_Link, "", *target);
    target->add_column(type_Int, "");
    auto o0 = origin->create_object();
    auto t0 = target->create_object();
    auto t1 = target->create_object();
    auto t2 = target->create_object();

    o0.set(col0, t2.get_key());
    CHECK_EQUAL(t2.get_key(), o0.get<ObjKey>(col0));
    group->commit_and_continue_as_read();

    // verify that we can revert a link change:
    group->promote_to_write();
    o0.set(col0, t1.get_key());
    CHECK_EQUAL(t1.get_key(), o0.get<ObjKey>(col0));
    group->rollback_and_continue_as_read();
    CHECK_EQUAL(t2.get_key(), o0.get<ObjKey>(col0));
    // verify that we can revert addition of a row in target table
    group->promote_to_write();
    target->create_object();
    CHECK_EQUAL(t2.get_key(), o0.get<ObjKey>(col0));
    group->rollback_and_continue_as_read();
    CHECK_EQUAL(t2.get_key(), o0.get<ObjKey>(col0));
}


TEST(LangBindHelper_RollbackAndContinueAsRead_LinkLists)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    auto group = sg->start_read();
    group->promote_to_write();
    TableRef origin = group->add_table("origin");
    TableRef target = group->add_table("target");
    auto col0 = origin->add_column_link(type_LinkList, "", *target);
    target->add_column(type_Int, "");
    auto o0 = origin->create_object();
    auto t0 = target->create_object();
    auto t1 = target->create_object();
    auto t2 = target->create_object();

    auto link_list = o0.get_linklist(col0);
    link_list.add(t0.get_key());
    link_list.add(t1.get_key());
    link_list.add(t2.get_key());
    link_list.add(t0.get_key());
    link_list.add(t2.get_key());
    group->commit_and_continue_as_read();
    // verify that we can reverse a LinkView::move()
    CHECK_EQUAL(5, link_list.size());
    CHECK_EQUAL(t0.get_key(), link_list.get(0));
    CHECK_EQUAL(t1.get_key(), link_list.get(1));
    CHECK_EQUAL(t2.get_key(), link_list.get(2));
    CHECK_EQUAL(t0.get_key(), link_list.get(3));
    CHECK_EQUAL(t2.get_key(), link_list.get(4));
    group->promote_to_write();
    link_list.move(1, 3);
    CHECK_EQUAL(5, link_list.size());
    CHECK_EQUAL(t0.get_key(), link_list.get(0));
    CHECK_EQUAL(t2.get_key(), link_list.get(1));
    CHECK_EQUAL(t0.get_key(), link_list.get(2));
    CHECK_EQUAL(t1.get_key(), link_list.get(3));
    CHECK_EQUAL(t2.get_key(), link_list.get(4));
    group->rollback_and_continue_as_read();
    CHECK_EQUAL(5, link_list.size());
    CHECK_EQUAL(t0.get_key(), link_list.get(0));
    CHECK_EQUAL(t1.get_key(), link_list.get(1));
    CHECK_EQUAL(t2.get_key(), link_list.get(2));
    CHECK_EQUAL(t0.get_key(), link_list.get(3));
    CHECK_EQUAL(t2.get_key(), link_list.get(4));
}


TEST(LangBindHelper_RollbackAndContinueAsRead_TableClear)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    auto group = sg->start_read();

    group->promote_to_write();
    TableRef origin = group->add_table("origin");
    TableRef target = group->add_table("target");

    target->add_column(type_Int, "int");
    auto c1 = origin->add_column_link(type_LinkList, "linklist", *target);
    auto c2 = origin->add_column_link(type_Link, "link", *target);

    Obj t = target->create_object();
    Obj o = origin->create_object();
    o.set(c2, t.get_key());
    LnkLst l = o.get_linklist(c1);
    l.add(t.get_key());
    group->commit_and_continue_as_read();

    group->promote_to_write();
    CHECK_EQUAL(1, l.size());
    target->clear();
    CHECK_EQUAL(0, l.size());

    group->rollback_and_continue_as_read();
    CHECK_EQUAL(1, l.size());
}

TEST(LangBindHelper_RollbackAndContinueAsRead_IntIndex)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    auto g = sg->start_read();
    g->promote_to_write();

    TableRef target = g->add_table("target");
    ColKey col = target->add_column(type_Int, "pk");
    target->add_search_index(col);

    std::vector<ObjKey> keys;
    target->create_objects(REALM_MAX_BPNODE_SIZE + 1, keys);
    g->commit_and_continue_as_read();
    g->promote_to_write();

    // Ensure that the index has a different bptree layout so that failing to
    // refresh it will do bad things
    auto it = target->begin();
    for (int i = 0; i < REALM_MAX_BPNODE_SIZE + 1; ++i) {
        it->set<int64_t>(col, i);
        ++it;
    }

    g->rollback_and_continue_as_read();
    g->promote_to_write();

    // Crashes if index has an invalid parent ref
    target->clear();
}


TEST(LangBindHelper_RollbackAndContinueAsRead_TransactLog)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    ColKey c0, c1;
    {
        WriteTransaction wt(sg);
        c0 = wt.add_table("table 1")->add_column(type_Int, "int");
        c1 = wt.add_table("table 2")->add_column(type_Int, "int");
        wt.commit();
    }

    auto tr = sg->start_read();

    ObjKey o0, o1;
    {
        WriteTransaction wt(sg);
        wt.get_table("table 1")->get_key();
        wt.get_table("table 2")->get_key();
        o0 = wt.get_table("table 1")->create_object().get_key();
        o1 = wt.get_table("table 2")->create_object().get_key();
        wt.commit();
    }
    ColKey c2, c3;
    ObjKey okey;
    {
        // Add a table with some links
        WriteTransaction wt(sg);
        TableRef table = wt.add_table("link origin");
        c2 = table->add_column_link(type_Link, "link", *wt.get_table("table 1"));
        c3 = table->add_column_link(type_LinkList, "linklist", *wt.get_table("table 2"));
        Obj o = table->create_object();
        o.set(c2, o.get_key());
        o.get_linklist(c3).add(o.get_key());
        okey = o.get_key();
        wt.commit();

        tr->advance_read();
    }
    {
        // Verify that rolling back deletion restores links correctly
        auto wt = sg->start_write();
        wt->get_table("table 1")->remove_object(o0);
        wt->get_table("table 2")->remove_object(o1);

        struct : NoOpTransactionLogParser {
            using NoOpTransactionLogParser::NoOpTransactionLogParser;

            bool create_object(ObjKey o)
            {
                CHECK(o == o1 || o == o0);
                return true;
            }
            bool select_link_list(ColKey col, ObjKey o)
            {
                CHECK(col == link_list_col);
                CHECK(o == okey);
                return true;
            }
            bool list_insert(size_t ndx)
            {
                CHECK(ndx == 0);
                return true;
            }

            bool modify_object(ColKey col, ObjKey obj)
            {
                CHECK(col == link_col && obj == okey);
                return true;
            }
            ObjKey o0, o1, okey;
            ColKey link_col, link_list_col;
        } parser(test_context);
        parser.o1 = o1;
        parser.o0 = o0;
        parser.okey = okey;
        parser.link_col = c2;
        parser.link_list_col = c3;
        wt->rollback_and_continue_as_read(&parser);
    }
    // Verify that clear() is rolled back appropriately
    tr->promote_to_write();
    tr->get_table("link origin")->begin()->get_linklist(c3).clear();

    {
        struct foo : NoOpTransactionLogParser {
            using NoOpTransactionLogParser::NoOpTransactionLogParser;

            size_t list_ndx = 0;

            bool list_insert(size_t ndx)
            {
                CHECK_EQUAL(list_ndx, ndx);
                ++list_ndx;
                return true;
            }
        } parser(test_context);
        tr->rollback_and_continue_as_read(&parser);
        CHECK(parser.list_ndx == 1);
    }
}

TEST(LangBindHelper_ImplicitTransactions_OverSharedGroupDestruction)
{
    SHARED_GROUP_TEST_PATH(path);
    // we hold on to write log collector and registry across a complete
    // shutdown/initialization of shared rt->
    std::unique_ptr<Replication> hist1(make_in_realm_history(path));
    {
        DBRef sg = DB::create(*hist1, DBOptions(crypt_key()));
        {
            WriteTransaction wt(sg);
            TableRef tr = wt.add_table("table");
            tr->add_column(type_Int, "first");
            for (int i = 0; i < 20; i++)
                tr->create_object();
            wt.commit();
        }
        // no valid shared group anymore
    }
    {
        std::unique_ptr<Replication> hist2(make_in_realm_history(path));
        DBRef sg = DB::create(*hist2, DBOptions(crypt_key()));
        {
            WriteTransaction wt(sg);
            TableRef tr = wt.get_table("table");
            for (int i = 0; i < 20; i++)
                tr->create_object();
            wt.commit();
        }
    }
}

TEST(LangBindHelper_ImplicitTransactions_LinkList)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    auto group = sg->start_write();
    TableRef origin = group->add_table("origin");
    TableRef target = group->add_table("target");
    auto col = origin->add_column_link(type_LinkList, "", *target);
    target->add_column(type_Int, "");
    auto O0 = origin->create_object();
    auto T0 = target->create_object();
    auto link_list = O0.get_linklist(col);
    link_list.add(T0.get_key());
    group->commit_and_continue_as_read();
    group->verify();
}


TEST(LangBindHelper_ImplicitTransactions_StringIndex)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    auto group = sg->start_write();
    TableRef table = group->add_table("a");
    auto col = table->add_column(type_String, "b");
    table->add_search_index(col);
    group->verify();
    group->commit_and_continue_as_read();
    group->verify();
}


namespace {

// Test that multiple trackers (of changes) always see a consistent picture.
// This is done by having multiple writers update a table A, query it, and store
// the count of the query in another table, B. Multiple readers then track the
// changes and verify that the count on their query is consistent with the count
// they read from table B. Terminate verification by signalling through a third
// table, C, after a chosen number of yields, which should allow readers to
// run their verification.
void multiple_trackers_writer_thread(DBRef db)
{
    // insert new random values
    Random random(random_int<unsigned long>());
    for (int i = 0; i < 10; ++i) {
        WriteTransaction wt(db);
        auto ta = wt.get_table("A");
        auto col = ta->get_column_keys()[0];
        for (auto it = ta->begin(); it != ta->end(); ++it) {
            auto e = *it;
            e.set(col, random.draw_int_mod(200));
        }
        auto tb = wt.get_table("B");
        auto count = ta->where().greater(col, 100).count();
        tb->begin()->set<int64_t>(tb->get_column_keys()[0], count);
        wt.commit();
    }
}

void multiple_trackers_reader_thread(TestContext& test_context, DBRef db)
{
    // verify that consistency is maintained as we advance_read through a
    // stream of transactions
    auto g = db->start_read();
    auto ta = g->get_table("A");
    auto tb = g->get_table("B");
    auto tc = g->get_table("C");
    auto col = ta->get_column_keys()[0];
    auto b_col = tb->get_column_keys()[0];
    TableView tv = ta->where().greater(col, 100).find_all();
    while (tc->size() == 0) {
        auto count = tb->begin()->get<int64_t>(b_col);
        tv.sync_if_needed();
        CHECK_EQUAL(tv.size(), count);
        std::this_thread::yield();
        g->advance_read();
    }
}

} // anonymous namespace


TEST(LangBindHelper_ImplicitTransactions_MultipleTrackers)
{
    const int write_thread_count = 7;
    const int read_thread_count = 3;

    SHARED_GROUP_TEST_PATH(path);

    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    ColKey col;
    {
        // initialize table with 200 entries holding 0..200
        WriteTransaction wt(sg);
        TableRef tr = wt.add_table("A");
        col = tr->add_column(type_Int, "first");
        for (int j = 0; j < 200; j++) {
            tr->create_object().set_all(j);
        }
        auto table_b = wt.add_table("B");
        table_b->add_column(type_Int, "bussemand");
        table_b->create_object().set_all(99);
        auto table_c = wt.add_table("C");
        wt.commit();
    }
    // FIXME: Use separate arrays for reader and writer threads for safety and readability.
    Thread threads[write_thread_count + read_thread_count];
    for (int i = 0; i < write_thread_count; ++i)
        threads[i].start([&] { multiple_trackers_writer_thread(sg); });
    std::this_thread::yield();
    for (int i = 0; i < read_thread_count; ++i) {
        threads[write_thread_count + i].start([&] { multiple_trackers_reader_thread(test_context, sg); });
    }

    // Wait for all writer threads to complete
    for (int i = 0; i < write_thread_count; ++i)
        threads[i].join();

    // Allow readers time to catch up
    for (int k = 0; k < 100; ++k)
        std::this_thread::yield();

    // signal to all readers to complete
    {
        WriteTransaction wt(sg);
        TableRef tr = wt.get_table("C");
        tr->create_object();
        wt.commit();
    }
    // Wait for all reader threads to complete
    for (int i = 0; i < read_thread_count; ++i)
        threads[write_thread_count + i].join();
}

#ifndef _WIN32

#if !REALM_ENABLE_ENCRYPTION
// Interprocess communication does not work with encryption enabled

#if !REALM_ANDROID && !REALM_IOS
// fork should not be used on android or ios.

/*
This unit test has been disabled as it occasionally gets itself into a hang
(which has plauged the testing process for a long time). It is unknown to me
(Kristian) whether this is due to a bug in Core or a bug in this test.
*/

#if 0
// #ifdef LEGACY_TESTS
// ^just a marker, that this disabled unittest will need porting
// if we ever try to re-enable it.
TEST(LangBindHelper_ImplicitTransactions_InterProcess)
{
    const int write_process_count = 7;
    const int read_process_count = 3;

    int readpids[read_process_count];
    int writepids[write_process_count];
    SHARED_GROUP_TEST_PATH(path);

    int pid = fork();
    if (pid == 0) {
        std::unique_ptr<Replication> hist(make_in_realm_history(path));
        DBRef sg = DB::create(*hist);
        {
            WriteTransaction wt(sg);
            TableRef tr = wt.add_table("table");
            tr->add_column(type_Int, "first");
            for (int i = 0; i < 200; ++i)
                tr->add_empty_row();
            tr->set_int(0, 100, 42);
            wt.commit();
        }
        exit(0);
    }
    else {
        int status;
        waitpid(pid, &status, 0);
    }

    // intialization complete. Start writers:
    for (int i = 0; i < write_process_count; ++i) {
        writepids[i] = fork();
        if (writepids[i] == 0) {
            multiple_trackers_writer_thread(std::string(path));
            exit(0);
        }
    }
    sched_yield();
    // then start readers:
    for (int i = 0; i < read_process_count; ++i) {
        readpids[i] = fork();
        if (readpids[i] == 0) {
            multiple_trackers_reader_thread(test_context, path);
            exit(0);
        }
    }

    // Wait for all writer threads to complete
    for (int i = 0; i < write_process_count; ++i) {
        int status = 0;
        waitpid(writepids[i], &status, 0);
    }

    // Wait for all reader threads to find and lock onto value '42'
    {
        std::unique_ptr<Replication> hist(make_in_realm_history(path));
        DBRef sg = DB::create(*hist);
        for (;;) {
            TransactionRef rt = sg->start_read()
            ConstTableRef tr = rt.get_table("table");
            if (tr->get_int(0, 0) == read_process_count) break;
            sched_yield();
        }
    }

    // signal to all readers to complete
    {
        std::unique_ptr<Replication> hist(make_in_realm_history(path));
        DBRef sg = DB::create(*hist);
        WriteTransaction wt(sg);
        TableRef tr = wt.get_table("table");
        Query q = tr->where().equal(0, 42);
        int idx = q.find();
        tr->set_int(0, idx, 43);
        wt.commit();
    }

    // Wait for all reader threads to complete
    for (int i = 0; i < read_process_count; ++i) {
        int status;
        waitpid(readpids[i], &status, 0);
    }

}

// #endif // LEGACY_TESTS
#endif // 0
#endif // !REALM_ANDROID && !REALM_IOS
#endif // not REALM_ENABLE_ENCRYPTION
#endif // not defined _WIN32

TEST(LangBindHelper_ImplicitTransactions_NoExtremeFileSpaceLeaks)
{
    SHARED_GROUP_TEST_PATH(path);

    for (int i = 0; i < 100; ++i) {
        std::unique_ptr<Replication> hist(make_in_realm_history(path));
        DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
        auto trans = sg->start_read();
        trans->promote_to_write();
        trans->commit_and_continue_as_read();
    }

// the miminum filesize (after a commit) is one or two pages, depending on the
// page size.
#if REALM_ENABLE_ENCRYPTION
    if (crypt_key())
        // Encrypted files are always at least a 4096 byte header plus payload
        CHECK_LESS_EQUAL(File(path).get_size(), 2 * page_size() + 4096);
    else
        CHECK_LESS_EQUAL(File(path).get_size(), 2 * page_size());
#else
    CHECK_LESS_EQUAL(File(path).get_size(), 2 * page_size());
#endif // REALM_ENABLE_ENCRYPTION
}


TEST(LangBindHelper_ImplicitTransactions_ContinuedUseOfTable)
{
    SHARED_GROUP_TEST_PATH(path);

    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    auto group = sg->start_read();
    auto group_w = sg->start_write();

    TableRef table_w = group_w->add_table("table");
    auto col = table_w->add_column(type_Int, "");
    auto obj = table_w->create_object();
    group_w->commit_and_continue_as_read();
    group_w->verify();

    group->advance_read();
    ConstTableRef table = group->get_table("table");
    CHECK_EQUAL(1, table->size());
    group->verify();

    group_w->promote_to_write();
    obj.set<int64_t>(col, 1);
    group_w->commit_and_continue_as_read();
    group_w->verify();

    group->advance_read();
    auto obj2 = group->import_copy_of(obj);
    CHECK_EQUAL(1, obj2.get<int64_t>(col));
    group->verify();
}


TEST(LangBindHelper_ImplicitTransactions_ContinuedUseOfLinkList)
{
    SHARED_GROUP_TEST_PATH(path);

    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    auto group = sg->start_read();
    auto group_w = sg->start_write();

    TableRef table_w = group_w->add_table("table");
    auto col = table_w->add_column_link(type_LinkList, "flubber", *table_w);
    auto obj = table_w->create_object();
    auto link_list_w = obj.get_linklist(col);
    link_list_w.add(obj.get_key());
    // CHECK_EQUAL(1, link_list_w.size()); // avoid this, it hides missing updates
    group_w->commit_and_continue_as_read();
    group_w->verify();

    group->advance_read();
    ConstTableRef table = group->get_table("table");
    auto link_list = obj.get_linklist(col);
    CHECK_EQUAL(1, link_list.size());
    group->verify();

    group_w->promote_to_write();
    // CHECK_EQUAL(1, link_list_w.size()); // avoid this, it hides missing updates
    link_list_w.add(obj.get_key());
    CHECK_EQUAL(2, link_list_w.size());
    group_w->commit_and_continue_as_read();
    group_w->verify();

    group->advance_read();
    CHECK_EQUAL(2, link_list.size());
    group->verify();
}


TEST(LangBindHelper_MemOnly)
{
    SHARED_GROUP_TEST_PATH(path);
    ShortCircuitHistory hist(path);
    DBRef sg = DB::create(hist, DBOptions(DBOptions::Durability::MemOnly));

    // Verify that the db is empty after populating and then re-opening a file
    {
        WriteTransaction wt(sg);
        wt.add_table("table");
        wt.commit();
    }
    {
        TransactionRef rt = sg->start_read();
        CHECK(!rt->is_empty());
    }
    sg->close();
    sg = DB::create(hist, DBOptions(DBOptions::Durability::MemOnly));

    // Verify that basic replication functionality works
    auto rt = sg->start_read();
    {
        WriteTransaction wt(sg);
        wt.add_table("table");
        wt.commit();
    }

    CHECK(rt->is_empty());
    rt->advance_read();
    CHECK(!rt->is_empty());
}

TEST(LangBindHelper_ImplicitTransactions_SearchIndex)
{
    SHARED_GROUP_TEST_PATH(path);

    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    auto rt = sg->start_read();
    auto group_w = sg->start_read();

    // Add initial data
    group_w->promote_to_write();
    TableRef table_w = group_w->add_table("table");
    auto c0 = table_w->add_column(type_Int, "int1");
    auto c1 = table_w->add_column(type_String, "str");
    auto c2 = table_w->add_column(type_Int, "int2");
    auto ok = table_w->create_object({}, {{c1, "2"}, {c0, 1}, {c2, 3}}).get_key();
    group_w->commit_and_continue_as_read();
    group_w->verify();

    rt->advance_read();
    ConstTableRef table = rt->get_table("table");
    auto obj = table->get_object(ok);
    CHECK_EQUAL(1, obj.get<int64_t>(c0));
    CHECK_EQUAL("2", obj.get<StringData>(c1));
    CHECK_EQUAL(3, obj.get<int64_t>(c2));
    rt->verify();

    // Add search index and re-verify
    group_w->promote_to_write();
    table_w->add_search_index(c1);
    group_w->commit_and_continue_as_read();
    group_w->verify();

    rt->advance_read();
    CHECK_EQUAL(1, obj.get<int64_t>(c0));
    CHECK_EQUAL("2", obj.get<StringData>(c1));
    CHECK_EQUAL(3, obj.get<int64_t>(c2));
    CHECK(table->has_search_index(c1));
    rt->verify();

    // Remove search index and re-verify
    group_w->promote_to_write();
    table_w->remove_search_index(c1);
    group_w->commit_and_continue_as_read();
    group_w->verify();

    rt->advance_read();
    CHECK_EQUAL(1, obj.get<int64_t>(c0));
    CHECK_EQUAL("2", obj.get<StringData>(c1));
    CHECK_EQUAL(3, obj.get<int64_t>(c2));
    CHECK(!table->has_search_index(c1));
    rt->verify();
}

TEST(LangBindHelper_HandoverQuery)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    TransactionRef rt = sg->start_read();
    {
        WriteTransaction wt(sg);
        Group& group_w = wt.get_group();
        TableRef t = group_w.add_table("table2");
        t->add_column(type_String, "first");
        auto int_col = t->add_column(type_Int, "second");
        for (int i = 0; i < 100; ++i) {
            t->create_object().set(int_col, i);
        }
        wt.commit();
    }
    rt->advance_read();
    auto table = rt->get_table("table2");
    auto int_col = table->get_column_key("second");
    Query query = table->column<Int>(int_col) < 50;
    size_t count = query.count();
    // CHECK(query.is_in_sync());
    auto vtrans = rt->duplicate();
    std::unique_ptr<Query> q2 = vtrans->import_copy_of(query, PayloadPolicy::Move);
    CHECK_EQUAL(count, 50);
    {
        // Delete first column. This alters the index of 'second' column
        WriteTransaction wt(sg);
        Group& group_w = wt.get_group();
        TableRef t = group_w.get_table("table2");
        auto str_col = table->get_column_key("first");
        t->remove_column(str_col);
        wt.commit();
    }
    rt->advance_read();
    count = query.count();
    CHECK_EQUAL(count, 50);
    count = q2->count();
    CHECK_EQUAL(count, 50);
}

TEST(LangBindHelper_SubqueryHandoverQueryCreatedFromDeletedLinkView)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    TransactionRef reader;
    auto writer = sg->start_write();
    {
        TableView tv1;
        auto table = writer->add_table("table");
        auto table2 = writer->add_table("table2");
        table2->add_column(type_Int, "int");
        auto key = table2->create_object().set_all(42).get_key();

        auto col = table->add_column_link(type_LinkList, "first", *table2);
        auto obj = table->create_object();
        auto link_view = obj.get_linklist(col);

        link_view.add(key);
        writer->commit_and_continue_as_read();

        Query qq = table2->where(link_view);
        CHECK_EQUAL(qq.count(), 1);
        writer->promote_to_write();
        table->clear();
        writer->commit_and_continue_as_read();
        CHECK_EQUAL(link_view.size(), 0);
        CHECK_EQUAL(qq.count(), 0);

        reader = writer->duplicate();
#ifdef OLD_CORE_BEHAVIOR
        // FIXME: Old core would allow the code below, but new core will throw.
        //
        // Why should a query still be valid after a change, when it would not be possible
        // to reconstruct the query from new after said change?
        //
        // In this specific case, the query is constructed from a linkview on an object
        // which is destroyed. After the object is destroyed, the linkview obviously
        // cannot be constructed, and hence the query can also not be constructed.
        auto lv2 = reader->import_copy_of(link_view);
        auto rq = reader->import_copy_of(qq, PayloadPolicy::Copy);
        writer->close();
        auto tv = rq->find_all();

        CHECK(tv.is_in_sync());
        CHECK(tv.is_attached());
        CHECK_EQUAL(0, tv.size());
#endif
    }
}


TEST(LangBindHelper_SubqueryHandoverDependentViews)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    std::unique_ptr<Query> qq2;
    TransactionRef reader;
    ColKey col1;
    {
        {
            TableView tv1;
            auto writer = sg->start_write();
            TableRef table = writer->add_table("table2");
            auto col0 = table->add_column(type_Int, "first");
            col1 = table->add_column(type_Bool, "even");
            for (int i = 0; i < 100; ++i) {
                auto obj = table->create_object();
                obj.set<int>(col0, i);
                bool isEven = ((i % 2) == 0);
                obj.set<bool>(col1, isEven);
            }
            writer->commit_and_continue_as_read();
            tv1 = table->where().less_equal(col0, 50).find_all();
            Query qq = tv1.get_parent().where(&tv1);
            reader = writer->duplicate();
            qq2 = reader->import_copy_of(qq, PayloadPolicy::Copy);
            CHECK(tv1.is_attached());
            CHECK_EQUAL(51, tv1.size());
        }
        {
            realm::TableView tv = qq2->equal(col1, true).find_all();

            CHECK(tv.is_in_sync());
            CHECK(tv.is_attached());
            CHECK_EQUAL(26, tv.size()); // BOOM! fail with 50
        }
    }
}

TEST(LangBindHelper_HandoverPartialQuery)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    std::unique_ptr<Query> qq2;
    TransactionRef reader;
    ColKey col0;
    {
        {
            TableView tv1;
            auto writer = sg->start_write();
            TableRef table = writer->add_table("table2");
            col0 = table->add_column(type_Int, "first");
            auto col1 = table->add_column(type_Bool, "even");
            for (int i = 0; i < 100; ++i) {
                auto obj = table->create_object();
                obj.set<int>(col0, i);
                bool isEven = ((i % 2) == 0);
                obj.set<bool>(col1, isEven);
            }
            writer->commit_and_continue_as_read();
            tv1 = table->where().less_equal(col0, 50).find_all();
            Query qq = tv1.get_parent().where(&tv1);
            reader = writer->duplicate();
            qq2 = reader->import_copy_of(qq, PayloadPolicy::Copy);
            CHECK(tv1.is_attached());
            CHECK_EQUAL(51, tv1.size());
        }
        {
            TableView tv = qq2->greater(col0, 48).find_all();
            CHECK(tv.is_attached());
            CHECK_EQUAL(2, tv.size());
            auto obj = tv.get(0);
            CHECK_EQUAL(49, obj.get<int64_t>(col0));
            obj = tv.get(1);
            CHECK_EQUAL(50, obj.get<int64_t>(col0));
        }
    }
}


// Verify that an in-sync TableView backed by a Query that is restricted to a TableView
// remains in sync when handed-over using a mutable payload.
TEST(LangBindHelper_HandoverNestedTableViews)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    {
        TransactionRef reader;
        std::unique_ptr<ConstTableView> tv;
        {
            auto writer = sg->start_write();
            TableRef table = writer->add_table("table2");
            auto col = table->add_column(type_Int, "first");
            for (int i = 0; i < 100; ++i) {
                table->create_object().set_all(i);
            }
            writer->commit_and_continue_as_read();
            // Create a TableView tv2 that is backed by a Query that is restricted to rows from TableView tv1.
            TableView tv1 = table->where().less_equal(col, 50).find_all();
            TableView tv2 = tv1.get_parent().where(&tv1).find_all();
            CHECK(tv2.is_in_sync());
            reader = writer->duplicate();
            tv = reader->import_copy_of(tv2, PayloadPolicy::Move);
        }
        CHECK(tv->is_in_sync());
        CHECK(tv->is_attached());
        CHECK_EQUAL(51, tv->size());
    }
}


TEST(LangBindHelper_HandoverAccessors)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    TransactionRef reader;
    ColKey col;
    std::unique_ptr<ConstTableView> tv2;
    std::unique_ptr<ConstTableView> tv3;
    std::unique_ptr<ConstTableView> tv4;
    std::unique_ptr<ConstTableView> tv5;
    std::unique_ptr<ConstTableView> tv6;
    std::unique_ptr<ConstTableView> tv7;
    {
        TableView tv;
        auto writer = sg->start_write();
        TableRef table = writer->add_table("table2");
        col = table->add_column(type_Int, "first");
        for (int i = 0; i < 100; ++i) {
            table->create_object().set_all(i);
        }
        writer->commit_and_continue_as_read();

        tv = table->where().find_all();
        CHECK(tv.is_attached());
        CHECK_EQUAL(100, tv.size());
        for (int i = 0; i < 100; ++i)
            CHECK_EQUAL(i, tv.get(i).get<Int>(col));

        reader = writer->duplicate();
        tv2 = reader->import_copy_of(tv, PayloadPolicy::Copy);
        CHECK(tv.is_attached());
        CHECK(tv.is_in_sync());

        tv3 = reader->import_copy_of(tv, PayloadPolicy::Stay);
        CHECK(tv.is_attached());
        CHECK(tv.is_in_sync());

        tv4 = reader->import_copy_of(tv, PayloadPolicy::Move);
        CHECK(tv.is_attached());
        CHECK(!tv.is_in_sync());

        // and again, but this time with the source out of sync:
        tv5 = reader->import_copy_of(tv, PayloadPolicy::Copy);
        CHECK(tv.is_attached());
        CHECK(!tv.is_in_sync());

        tv6 = reader->import_copy_of(tv, PayloadPolicy::Stay);
        CHECK(tv.is_attached());
        CHECK(!tv.is_in_sync());

        tv7 = reader->import_copy_of(tv, PayloadPolicy::Move);
        CHECK(tv.is_attached());
        CHECK(!tv.is_in_sync());

        // and verify, that even though it was out of sync, we can bring it in sync again
        tv.sync_if_needed();
        CHECK(tv.is_in_sync());

        // Obj handover tested elsewhere
    }
    {
        // now examining stuff handed over to other transaction
        // with payload:
        CHECK(tv2->is_attached());
        CHECK(tv2->is_in_sync());
        CHECK_EQUAL(100, tv2->size());
        for (int i = 0; i < 100; ++i)
            CHECK_EQUAL(i, tv2->get_object(i).get<Int>(col));
        // importing one without payload:
        CHECK(tv3->is_attached());
        CHECK(!tv3->is_in_sync());
        tv3->sync_if_needed();
        CHECK_EQUAL(100, tv3->size());
        for (int i = 0; i < 100; ++i)
            CHECK_EQUAL(i, tv3->get_object(i).get<Int>(col));

        // one with payload:
        CHECK(tv4->is_attached());
        CHECK(tv4->is_in_sync());
        CHECK_EQUAL(100, tv4->size());
        for (int i = 0; i < 100; ++i)
            CHECK_EQUAL(i, tv4->get_object(i).get<Int>(col));

        // verify that subsequent imports are all without payload:
        CHECK(tv5->is_attached());
        CHECK(!tv5->is_in_sync());

        CHECK(tv6->is_attached());
        CHECK(!tv6->is_in_sync());

        CHECK(tv7->is_attached());
        CHECK(!tv7->is_in_sync());
    }
}

TEST(LangBindHelper_TableViewAndTransactionBoundaries)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    ColKey col;
    {
        WriteTransaction wt(sg);
        auto table = wt.add_table("myTable");
        col = table->add_column(type_Int, "myColumn");
        table->create_object().set_all(42);
        wt.commit();
    }
    auto rt = sg->start_read();
    auto tv = rt->get_table("myTable")->where().greater(col, 40).find_all();
    CHECK(tv.is_in_sync());
    {
        WriteTransaction wt(sg);
        wt.commit();
    }
    rt->advance_read();
    CHECK(tv.is_in_sync());
    {
        WriteTransaction wt(sg);
        wt.commit();
    }
    rt->promote_to_write();
    CHECK(tv.is_in_sync());
    rt->commit_and_continue_as_read();
    CHECK(tv.is_in_sync());
    {
        WriteTransaction wt(sg);
        auto table = wt.get_table("myTable");
        table->begin()->set_all(41);
        wt.commit();
    }
    rt->advance_read();
    CHECK(!tv.is_in_sync());
    tv.sync_if_needed();
    CHECK(tv.is_in_sync());
    rt->advance_read();
    CHECK(tv.is_in_sync());
}

namespace {
// support threads for handover test. The setup is as follows:
// thread A writes a stream of updates to the database,
// thread B listens and continously does advance_read to see the updates.
// thread B also has a table view, which it continuosly keeps in sync in response
// to the updates. It then hands over the result to thread C.
// thread C continuously recieves copies of the results obtained in thead B and
// verifies them (by comparing with its own local, but identical query)

template <typename T>
struct HandoverControl {
    Mutex m_lock;
    CondVar m_changed;
    std::unique_ptr<T> m_handover;
    bool m_has_feedback = false;
    void put(std::unique_ptr<T> h)
    {
        LockGuard lg(m_lock);
        // std::cout << "put " << h << std::endl;
        while (m_handover != nullptr)
            m_changed.wait(lg);
        // std::cout << " -- put " << h << std::endl;
        m_handover = move(h);
        m_changed.notify_all();
    }
    void get(std::unique_ptr<T>& h)
    {
        LockGuard lg(m_lock);
        // std::cout << "get " << std::endl;
        while (m_handover == nullptr)
            m_changed.wait(lg);
        // std::cout << " -- get " << m_handover << std::endl;
        h = move(m_handover);
        m_handover = nullptr;
        m_changed.notify_all();
    }
    bool try_get(std::unique_ptr<T>& h)
    {
        LockGuard lg(m_lock);
        if (m_handover == nullptr)
            return false;
        h = move(m_handover);
        m_handover = nullptr;
        m_changed.notify_all();
        return true;
    }
    void signal_feedback()
    {
        LockGuard lg(m_lock);
        m_has_feedback = true;
        m_changed.notify_all();
    }
    void wait_feedback()
    {
        LockGuard lg(m_lock);
        while (!m_has_feedback)
            m_changed.wait(lg);
        m_has_feedback = false;
    }
    HandoverControl(const HandoverControl&) = delete;
    HandoverControl()
    {
    }
};

void handover_writer(DBRef db)
{
    //    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    //    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    auto g = db->start_read();
    auto table = g->get_table("table");
    Random random(random_int<unsigned long>());
    for (int i = 1; i < 5000; ++i) {
        g->promote_to_write();
        // table holds random numbers >= 1, until the writing process
        // finishes, after n new entry with value 0 is added to signal termination
        table->create_object().set_all(1 + random.draw_int_mod(100));
        g->commit_and_continue_as_read();
        // improve chance of consumers running concurrently with
        // new writes:
        for (int n = 0; n < 10; ++n)
            std::this_thread::yield();
    }
    g->promote_to_write();
    table->create_object().set_all(0); // <---- signals other threads to stop
    g->commit();
}

struct Work {
    TransactionRef tr;
    std::unique_ptr<TableView> tv;
};

void handover_querier(HandoverControl<Work>* control, TestContext& test_context, DBRef db)
{
    // We need to ensure that the initial version observed is *before* the final
    // one written by the writer thread. We do this (simplisticly) by locking on
    // to the initial version before even starting the writer.
    auto g = db->start_read();
    Thread writer;
    writer.start([&] { handover_writer(db); });
    TableRef table = g->get_table("table");
    ColKeys cols = table->get_column_keys();
    TableView tv = table->where().greater(cols[0], 50).find_all();
    for (;;) {
        // wait here for writer to change the database. Kind of wasteful, but wait_for_change()
        // is not available on osx.
        if (!db->has_changed(g)) {
            std::this_thread::yield();
            continue;
        }

        g->advance_read();
        CHECK(!tv.is_in_sync());
        tv.sync_if_needed();
        CHECK(tv.is_in_sync());
        auto ref = g->duplicate();
        std::unique_ptr<Work> h = std::make_unique<Work>();
        h->tr = ref;
        h->tv = ref->import_copy_of(tv, PayloadPolicy::Move);
        control->put(std::move(h));

        // here we need to allow the reciever to get hold on the proper version before
        // we go through the loop again and advance_read().
        control->wait_feedback();
        std::this_thread::yield();

        if (table->where().equal(cols[0], 0).count() >= 1)
            break;
    }
    g->end_read();
    writer.join();
}

void handover_verifier(HandoverControl<Work>* control, TestContext& test_context)
{
    bool not_done = true;
    while (not_done) {
        std::unique_ptr<Work> work;
        control->get(work);

        auto g = work->tr;
        control->signal_feedback();
        TableRef table = g->get_table("table");
        ColKeys cols = table->get_column_keys();
        TableView tv = table->where().greater(cols[0], 50).find_all();
        CHECK(tv.is_in_sync());
        std::unique_ptr<TableView> tv2 = std::move(work->tv);
        CHECK(tv.is_in_sync());
        CHECK(tv2->is_in_sync());
        CHECK_EQUAL(tv.size(), tv2->size());
        for (size_t k = 0; k < tv.size(); ++k) {
            auto o = tv.get(k);
            auto o2 = tv2->get(k);
            CHECK_EQUAL(o.get<int64_t>(cols[0]), o2.get<int64_t>(cols[0]));
        }
        if (table->where().equal(cols[0], 0).count() >= 1)
            not_done = false;
        g->close();
    }
}

} // anonymous namespace

namespace {

void attacher(std::string path, ColKey col)
{
    // Creating a new DB in each attacher is on purpose, since we're
    // testing races in the attachment process, and that only takes place
    // during creation of the DB object.
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    for (int i = 0; i < 100; ++i) {
        auto g = sg->start_read();
        g->verify();
        auto table = g->get_table("table");
        g->promote_to_write();
        auto o = table->get_object(ObjKey(i));
        auto o2 = table->get_object(ObjKey(i * 10));
        o.set<int64_t>(col, 1 + o2.get<int64_t>(col));
        g->commit_and_continue_as_read();
        g->verify();
        g->end_read();
    }
}
} // anonymous namespace


TEST(LangBindHelper_RacingAttachers)
{
    const int num_attachers = 10;
    SHARED_GROUP_TEST_PATH(path);
    ColKey col;
    {
        std::unique_ptr<Replication> hist(make_in_realm_history(path));
        DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
        auto g = sg->start_write();
        auto table = g->add_table("table");
        col = table->add_column(type_Int, "first");
        for (int i = 0; i < 1000; ++i)
            table->create_object(ObjKey(i));
        g->commit();
    }
    Thread attachers[num_attachers];
    for (int i = 0; i < num_attachers; ++i) {
        attachers[i].start([&] { attacher(path, col); });
    }
    for (int i = 0; i < num_attachers; ++i) {
        attachers[i].join();
    }
}

TEST(LangBindHelper_HandoverBetweenThreads)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    auto g = sg->start_write();
    auto table = g->add_table("table");
    table->add_column(type_Int, "first");
    g->commit();
    g = sg->start_read();
    table = g->get_table("table");
    CHECK(bool(table));
    g->end_read();

    HandoverControl<Work> control;
    Thread querier, verifier;
    querier.start([&] { handover_querier(&control, test_context, sg); });
    verifier.start([&] { handover_verifier(&control, test_context); });
    querier.join();
    verifier.join();
}


TEST(LangBindHelper_HandoverDependentViews)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef db = DB::create(*hist, DBOptions(crypt_key()));
    TransactionRef tr;
    std::unique_ptr<TableView> tv_ov;
    ColKey col;
    {
        // Untyped interface
        {
            TableView tv1;
            TableView tv2;
            auto group_w = db->start_write();
            TableRef table = group_w->add_table("table2");
            col = table->add_column(type_Int, "first");
            for (int i = 0; i < 100; ++i) {
                table->create_object().set_all(i);
            }
            group_w->commit_and_continue_as_read();
            tv1 = table->where().find_all();
            tv2 = table->where(&tv1).find_all();
            CHECK(tv1.is_attached());
            CHECK(tv2.is_attached());
            CHECK_EQUAL(100, tv1.size());
            for (int i = 0; i < 100; ++i) {
                auto o = tv1.get(i);
                CHECK_EQUAL(i, o.get<int64_t>(col));
            }
            CHECK_EQUAL(100, tv2.size());
            for (int i = 0; i < 100; ++i) {
                auto o = tv2.get(i);
                CHECK_EQUAL(i, o.get<int64_t>(col));
            }
            tr = group_w->duplicate();
            tv_ov = tr->import_copy_of(tv2, PayloadPolicy::Copy);
            CHECK(tv1.is_attached());
            CHECK(tv2.is_attached());
        }
        {
            CHECK(tv_ov->is_in_sync());
            // CHECK(tv1.is_attached());
            CHECK(tv_ov->is_attached());
            CHECK_EQUAL(100, tv_ov->size());
            for (int i = 0; i < 100; ++i) {
                auto o = tv_ov->get(i);
                CHECK_EQUAL(i, o.get<int64_t>(col));
            }
        }
    }
}


TEST(LangBindHelper_HandoverTableViewWithLinkView)
{
    // First iteration hands-over a normal valid attached LinkView. Second
    // iteration hands-over a detached LinkView.
    for (int detached = 0; detached < 2; detached++) {
        SHARED_GROUP_TEST_PATH(path);
        std::unique_ptr<Replication> hist(make_in_realm_history(path));
        DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
        ColKey col_link2, col0;
        ObjKey ok0, ok1, ok2;
        TransactionRef tr;
        std::unique_ptr<TableView> tv2;
        {
            TableView tv;
            auto group_w = sg->start_write();

            TableRef table1 = group_w->add_table("table1");
            TableRef table2 = group_w->add_table("table2");

            // add some more columns to table1 and table2
            col0 = table1->add_column(type_Int, "col1");
            table1->add_column(type_String, "str1");

            // add some rows
            ok0 = table1->create_object().set_all(300, "delta").get_key();
            ok1 = table1->create_object().set_all(100, "alfa").get_key();
            ok2 = table1->create_object().set_all(200, "beta").get_key();

            col_link2 = table2->add_column_link(type_LinkList, "linklist", *table1);

            auto o = table2->create_object();
            auto lvr = o.get_linklist(col_link2);
            lvr.clear();
            lvr.add(ok0);
            lvr.add(ok1);
            lvr.add(ok2);

            // Return all rows of table1 (the linked-to-table) that match the criteria and is in the LinkList

            // q.m_table = table1
            // q.m_view = lvr
            Query q = table1->where(lvr).and_query(table1->column<Int>(col0) > 100);

            // Remove the LinkList that the query depends on, to see if a detached LinkView can be handed over
            // correctly
            if (detached == 1)
                table2->remove_object(o.get_key());

            tv = q.find_all(); // tv = { 0, 2 }
            CHECK(tv.is_in_sync());
            group_w->commit_and_continue_as_read();
            tr = group_w->duplicate();
            CHECK(tv.is_in_sync());
            if (detached == 1) { // import will fail
                CHECK_THROW(tr->import_copy_of(tv, PayloadPolicy::Copy), InvalidKey);
            }
            else {
                tv2 = tr->import_copy_of(tv, PayloadPolicy::Copy);
            }
        }
        {
            if (detached == 0) {
                CHECK(tv2->is_in_sync());
                CHECK_EQUAL(2, tv2->size());
                CHECK_EQUAL(ok0, tv2->get_key(0));
                CHECK_EQUAL(ok2, tv2->get_key(1));
            }
            tr->close();
        }
    }
}


#ifdef LEGACY_TESTS
namespace {

void do_write_work(std::string path, size_t id, size_t num_rows) {
    const size_t num_iterations = 5000000; // this makes it run for a loooong time
    const size_t payload_length_small = 10;
    const size_t payload_length_large = 5000; // > 4096 == page_size
    Random random(random_int<unsigned long>()); // Seed from slow global generator
    const char* key = crypt_key(true);
    for (size_t rep = 0; rep < num_iterations; ++rep) {
        std::unique_ptr<Replication> hist(make_in_realm_history(path));
        DBRef sg = DB::create(*hist, DBOptions(key));

        TransactionRef rt = sg->start_read() LangBindHelper::promote_to_write(sg);
        Group& group = const_cast<Group&>(rt.get_group());
        TableRef t = rt->get_table(0);

        for (size_t i = 0; i < num_rows; ++i) {
            const size_t payload_length = i % 10 == 0 ? payload_length_large : payload_length_small;
            const char payload_char = 'a' + static_cast<char>((id + rep + i) % 26);
            std::string std_payload(payload_length, payload_char);
            StringData payload(std_payload);

            t->set_int(0, i, payload.size());
            t->set_string(1, i, StringData(std_payload.c_str(), 1));
            t->set_string(2, i, payload);
        }
        LangBindHelper::commit_and_continue_as_read(sg);
    }
}

void do_read_verify(std::string path) {
    Random random(random_int<unsigned long>()); // Seed from slow global generator
    const char* key = crypt_key(true);
    while (true) {
        std::unique_ptr<Replication> hist(make_in_realm_history(path));
        DBRef sg = DB::create(*hist, DBOptions(key));
        TransactionRef rt =
            sg->start_read() if (rt.get_version() <= 2) continue; // let the writers make some initial data
        Group& group = const_cast<Group&>(rt.get_group());
        ConstTableRef t = rt->get_table(0);
        size_t num_rows = t->size();
        for (size_t r = 0; r < num_rows; ++r) {
            int64_t num_chars = t->get_int(0, r);
            StringData c = t->get_string(1, r);
            if (c == "stop reading") {
                return;
            } else {
                REALM_ASSERT_EX(c.size() == 1, c.size());
            }
            REALM_ASSERT_EX(t->get_name() == StringData("class_Table_Emulation_Name"), t->get_name().data());
            REALM_ASSERT_EX(t->get_column_name(0) == StringData("count"), t->get_column_name(0).data());
            REALM_ASSERT_EX(t->get_column_name(1) == StringData("char"), t->get_column_name(1).data());
            REALM_ASSERT_EX(t->get_column_name(2) == StringData("payload"), t->get_column_name(2).data());
            std::string std_validator(static_cast<unsigned int>(num_chars), c[0]);
            StringData validator(std_validator);
            StringData s = t->get_string(2, r);
            REALM_ASSERT_EX(s.size() == validator.size(), r, s.size(), validator.size());
            for (size_t i = 0; i < s.size(); ++i) {
                REALM_ASSERT_EX(s[i] == validator[i], r, i, s[i], validator[i]);
            }
            REALM_ASSERT_EX(s == validator, r, s.size(), validator.size());
        }
    }
}

} // end anonymous namespace


// The following test is long running to try to catch race conditions
// in with many reader writer threads on an encrypted realm and it is
// not suited to automated testing.
TEST_IF(Thread_AsynchronousIODataConsistency, false)
{
    SHARED_GROUP_TEST_PATH(path);
    const int num_writer_threads = 2;
    const int num_reader_threads = 2;
    const int num_rows = 200; //2 + REALM_MAX_BPNODE_SIZE;
    const char* key = crypt_key(true);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(key));
    {
        WriteTransaction wt(sg);
        Group& group = wt.get_group();
        TableRef t = rt->add_table("class_Table_Emulation_Name");
        // add a column for each thread to write to
        t->add_column(type_Int, "count", true);
        t->add_column(type_String, "char", true);
        t->add_column(type_String, "payload", true);
        t->add_empty_row(num_rows);
        wt.commit();
    }

    Thread writer_threads[num_writer_threads];
    for (int i = 0; i < num_writer_threads; ++i) {
        writer_threads[i].start(std::bind(do_write_work, std::string(path), i, num_rows));
    }
    Thread reader_threads[num_reader_threads];
    for (int i = 0; i < num_reader_threads; ++i) {
        reader_threads[i].start(std::bind(do_read_verify, std::string(path)));
    }
    for (int i = 0; i < num_writer_threads; ++i) {
        writer_threads[i].join();
    }

    {
        WriteTransaction wt(sg);
        Group &group = wt.get_group();
        TableRef t = rt->get_table("class_Table_Emulation_Name");
        t->set_string(1, 0, "stop reading");
        wt.commit();
    }

    for (int i = 0; i < num_reader_threads; ++i) {
        reader_threads[i].join();
    }
}
#endif

#ifdef LEGACY_TESTS
TEST(Query_ListOfPrimitivesHandover)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    auto& group = sg.begin_read();

    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
    Group& group_w = const_cast<Group&>(sg_w.begin_read());
    DB::VersionID vid;

    std::unique_ptr<DB::Handover<TableView>> table_view_handover;
    {
        LangBindHelper::promote_to_write(sg_w);

        TableRef t = group_w.add_table("table");
        DescriptorRef subdesc;
        size_t int_col = t->add_column(type_Table, "integers", false, &subdesc);
        subdesc->add_column(type_Int, "list", nullptr, true);

        t->add_empty_row(10);

        auto set_list = [](TableRef subtable, const std::vector<int64_t>& value_list) {
            size_t sz = value_list.size();
            subtable->clear();
            subtable->add_empty_row(sz);
            for (size_t i = 0; i < sz; i++) {
                subtable->set_int(0, i, value_list[i]);
            }
        };

        set_list(t->get_subtable(int_col, 0), std::vector<int64_t>({1, 2, 3}));
        set_list(t->get_subtable(int_col, 1), std::vector<int64_t>({1, 3, 5, 7}));
        set_list(t->get_subtable(int_col, 2), std::vector<int64_t>({100, 400, 200, 500, 300}));

        auto query = t->get_subtable(int_col, 2)->column<Int>(0) > 225;
        auto tv = query.find_all();

        LangBindHelper::commit_and_continue_as_read(sg_w);
        vid = sg_w.get_version_of_current_transaction();
        table_view_handover = sg_w.export_for_handover(tv, ConstSourcePayload::Stay);
    }

    LangBindHelper::advance_read(sg, vid);
    auto table_view = sg.import_from_handover(std::move(table_view_handover));
    table_view->sync_if_needed();
    CHECK_EQUAL(table_view->size(), 3);
    CHECK_EQUAL(table_view->get_int(0, 0), 400);

    {
        LangBindHelper::promote_to_write(sg_w);

        TableRef t = group_w.get_or_add_table("table");
        auto sub = t->get_subtable(0, 2);
        sub->insert_empty_row(0);
        sub->set_int(0, 0, 600);
        t->remove(0);
        // table_view is now associated with row 1

        LangBindHelper::commit_and_continue_as_read(sg_w);
    }

    rt->advance_read();
    table_view->sync_if_needed();
    CHECK_EQUAL(table_view->size(), 4);
    CHECK_EQUAL(table_view->get_int(0, 0), 600);
    auto subtable = rt->get_table("table")->get_subtable(0, 0);
    auto query = subtable->where();
    auto sum = query.sum_int(0);
    CHECK_EQUAL(sum, 16);

    {
        LangBindHelper::promote_to_write(sg_w);

        TableRef t = group_w.get_or_add_table("table");
        // Remove the row, table_view is associated with
        t->remove(1);

        // Create a view based on a degenerate table
        auto q = t->get_subtable(0, 2)->column<Int>(0) > 225;
        auto tv = q.find_all();

        LangBindHelper::commit_and_continue_as_read(sg_w);
        table_view_handover = sg_w.export_for_handover(tv, ConstSourcePayload::Stay);
    }
    rt->advance_read();
    CHECK(!table_view->is_attached());

    table_view = sg.import_from_handover(std::move(table_view_handover));
    table_view->sync_if_needed();
    CHECK_EQUAL(table_view->size(), 0);

    {
        LangBindHelper::promote_to_write(sg_w);

        TableRef t = group_w.get_or_add_table("table");
        // Remove the row, g is associated with
        t->remove(0);

        LangBindHelper::commit_and_continue_as_read(sg_w);
    }
    rt->advance_read();
    sum = 0;
    CHECK_LOGIC_ERROR(sum = query.sum_int(0), LogicError::detached_accessor);
    CHECK_EQUAL(sum, 0);
}
#endif

TEST(LangBindHelper_HandoverTableRef)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    TransactionRef reader;
    TableRef table;
    {
        auto writer = sg->start_write();
        TableRef table1 = writer->add_table("table1");
        writer->commit_and_continue_as_read();
        auto vid = writer->get_version_of_current_transaction();
        reader = sg->start_read(vid);
        table = reader->import_copy_of(table1);
    }
    CHECK(bool(table));
    CHECK(table->size() == 0);
}

TEST(LangBindHelper_HandoverLinkView)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    TransactionRef reader;
    ColKey col1;

    auto writer = sg->start_write();

    TableRef table1 = writer->add_table("table1");
    TableRef table2 = writer->add_table("table2");

    // add some more columns to table1 and table2
    col1 = table1->add_column(type_Int, "col1");
    table1->add_column(type_String, "str1");

    // add some rows
    auto to1 = table1->create_object().set_all(300, "delta");
    auto to2 = table1->create_object().set_all(100, "alfa");
    auto to3 = table1->create_object().set_all(200, "beta");

    ColKey col_link2 = table2->add_column_link(type_LinkList, "linklist", *table1);

    auto o1 = table2->create_object();
    auto o2 = table2->create_object();
    LnkLstPtr lvr = o1.get_linklist_ptr(col_link2);
    lvr->clear();
    lvr->add(to1.get_key());
    lvr->add(to2.get_key());
    lvr->add(to3.get_key());
    writer->commit_and_continue_as_read();
    reader = writer->duplicate();
    auto ll = reader->import_copy_of(lvr);
    {
        // validate inside reader transaction
        // Return all rows of table1 (the linked-to-table) that match the criteria and is in the LinkList

        // q.m_table = table1
        // q.m_view = lvr
        TableRef table1b = reader->get_table("table1");
        Query q = table1b->where(*ll).and_query(table1b->column<Int>(col1) > 100);

        // tv.m_table == table1
        TableView tv = q.find_all(); // tv = { 0, 2 }


        CHECK_EQUAL(2, tv.size());
        CHECK_EQUAL(to1.get_key(), tv.get_key(0));
        CHECK_EQUAL(to3.get_key(), tv.get_key(1));
    }
    {
        // Change table1 and verify that the change does not propagate through the handed-over linkview
        writer->promote_to_write();
        to1.set<int64_t>(col1, 50);
        writer->commit_and_continue_as_read();
    }
    {
        TableRef table1b = reader->get_table("table1");
        Query q = table1b->where(*ll).and_query(table1b->column<Int>(col1) > 100);

        // tv.m_table == table1
        TableView tv = q.find_all(); // tv = { 0, 2 }


        CHECK_EQUAL(2, tv.size());
        CHECK_EQUAL(to1.get_key(), tv.get_key(0));
        CHECK_EQUAL(to3.get_key(), tv.get_key(1));
    }
}

TEST(LangBindHelper_HandoverDistinctView)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    TransactionRef reader;
    std::unique_ptr<ConstTableView> tv2;
    ConstObj obj2b;
    {
        {
            TableView tv1;
            auto writer = sg->start_write();
            TableRef table = writer->add_table("table2");
            auto col = table->add_column(type_Int, "first");
            auto obj1 = table->create_object().set_all(100);
            auto obj2 = table->create_object().set_all(100);

            writer->commit_and_continue_as_read();
            tv1 = table->where().find_all();
            tv1.distinct(col);
            CHECK(tv1.size() == 1);
            CHECK(tv1.get_key(0) == obj1.get_key());
            CHECK(tv1.is_attached());

            reader = writer->duplicate();
            tv2 = reader->import_copy_of(tv1, PayloadPolicy::Copy);
            obj2b = reader->import_copy_of(obj1);
            CHECK(tv1.is_attached());
        }
        {
            // importing side: working in the context of "reader"
            CHECK(tv2->is_in_sync());
            CHECK(tv2->is_attached());

            CHECK_EQUAL(tv2->size(), 1);
            CHECK_EQUAL(tv2->get_key(0), obj2b.get_key());

            // distinct property must remain through handover such that second row is kept being omitted
            // after sync_if_needed()
            tv2->sync_if_needed();
            CHECK_EQUAL(tv2->size(), 1);
            CHECK_EQUAL(tv2->get_key(0), obj2b.get_key());
        }
    }
}


TEST(LangBindHelper_HandoverWithReverseDependency)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    auto trans = sg->start_read();
    {
        // Untyped interface
        TableView tv1;
        TableView tv2;
        ColKey ck;
        {
            trans->promote_to_write();
            TableRef table = trans->add_table("table2");
            ck = table->add_column(type_Int, "first");
            for (int i = 0; i < 100; ++i) {
                table->create_object().set_all(i);
            }
            trans->commit_and_continue_as_read();
            tv1 = table->where().find_all();
            tv2 = table->where(&tv1).find_all();
            CHECK(tv1.is_attached());
            CHECK(tv2.is_attached());
            CHECK_EQUAL(100, tv1.size());
            for (int i = 0; i < 100; ++i)
                CHECK_EQUAL(i, tv1.get_object(i).get<int64_t>(ck));
            CHECK_EQUAL(100, tv2.size());
            for (int i = 0; i < 100; ++i)
                CHECK_EQUAL(i, tv1.get_object(i).get<int64_t>(ck));
            auto dummy_trans = trans->duplicate();
            auto dummy_tv = dummy_trans->import_copy_of(tv1, PayloadPolicy::Copy);
            CHECK(tv1.is_attached());
            CHECK(tv2.is_attached());
        }
    }
}

TEST(LangBindHelper_HandoverTableViewFromBacklink)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    auto group_w = sg->start_write();

    TableRef source = group_w->add_table("source");
    source->add_column(type_Int, "int");

    TableRef links = group_w->add_table("links");
    ColKey col = links->add_column_link(type_Link, "link", *source);

    std::vector<ObjKey> dummies;
    source->create_objects(100, dummies);
    links->create_objects(100, dummies);
    auto source_it = source->begin();
    auto links_it = links->begin();
    for (int i = 0; i < 100; ++i) {
        auto obj = source_it->set_all(i);
        links_it->set(col, obj.get_key());
        ++source_it;
        ++links_it;
    }
    group_w->commit_and_continue_as_read();

    for (int i = 0; i < 100; ++i) {
        TableView tv = source->get_object(i).get_backlink_view(links, col);
        CHECK(tv.is_attached());
        CHECK_EQUAL(1, tv.size());
        ObjKey o_key = source->get_object(i).get_key();
        CHECK_EQUAL(o_key, tv.get_key(0));
        auto group = group_w->duplicate();
        auto tv2 = group->import_copy_of(tv, PayloadPolicy::Copy);
        CHECK(tv.is_attached());
        CHECK(tv2->is_attached());
        CHECK_EQUAL(1, tv2->size());
        CHECK_EQUAL(o_key, tv2->get_key(0));
    }
}

// Verify that handing over an out-of-sync TableView that represents backlinks
// to a deleted row results in a TableView that can be brought back into sync.
TEST(LangBindHelper_HandoverOutOfSyncTableViewFromBacklinksToDeletedRow)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    auto group_w = sg->start_write();

    TableRef target = group_w->add_table("target");
    target->add_column(type_Int, "int");

    TableRef links = group_w->add_table("links");
    auto col = links->add_column_link(type_Link, "link", *target);

    auto obj_t = target->create_object().set_all(0);

    links->create_object().set_all(obj_t.get_key());

    TableView tv = obj_t.get_backlink_view(links, col);
    CHECK_EQUAL(true, tv.is_attached());
    CHECK_EQUAL(true, tv.is_in_sync());
    CHECK_EQUAL(false, tv.depends_on_deleted_object());
    CHECK_EQUAL(1, tv.size());

    // Bring the view out of sync, and have it depend on a deleted row.
    target->remove_object(obj_t.get_key());
    CHECK_EQUAL(true, tv.is_attached());
    CHECK_EQUAL(false, tv.is_in_sync());
    CHECK_EQUAL(true, tv.depends_on_deleted_object());
    CHECK_EQUAL(1, tv.size());
    tv.sync_if_needed();
    CHECK_EQUAL(0, tv.size());
    group_w->commit_and_continue_as_read();
    auto group = group_w->duplicate();
    auto tv2 = group->import_copy_of(tv, PayloadPolicy::Copy);
    CHECK_EQUAL(true, tv2->depends_on_deleted_object());
    CHECK_EQUAL(0, tv2->size());
}

// Test that we can handover a query involving links, and that after the
// handover export, the handover is completely decoupled from later changes
// done on accessors belonging to the exporting shared group
TEST(LangBindHelper_HandoverWithLinkQueries)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef db = DB::create(*hist, DBOptions(crypt_key()));
    auto group_w = db->start_write();
    // First setup data so that we can do a query on links
    TableRef table1 = group_w->add_table("table1");
    TableRef table2 = group_w->add_table("table2");
    // add some more columns to table1 and table2
    table1->add_column(type_Int, "col1");
    table1->add_column(type_String, "str1");

    table2->add_column(type_Int, "col1");
    auto col_str = table2->add_column(type_String, "str2");

    // add some rows
    auto o10 = table1->create_object().set_all(100, "foo");
    auto o11 = table1->create_object().set_all(200, "!");
    auto o12 = table1->create_object().set_all(300, "bar");
    auto o20 = table2->create_object().set_all(400, "hello");
    auto o21 = table2->create_object().set_all(500, "world");
    auto o22 = table2->create_object().set_all(600, "!");

    ColKey col_link2 = table1->add_column_link(type_LinkList, "link", *table2);

    // set some links
    auto links1 = o10.get_linklist(col_link2);
    CHECK(links1.is_attached());
    links1.add(o21.get_key());

    auto links2 = o11.get_linklist(col_link2);
    CHECK(links2.is_attached());
    links2.add(o21.get_key());
    links2.add(o22.get_key());
    group_w->commit_and_continue_as_read();

    // Do a query (which will have zero results) and export it twice.
    // To test separation, we'll later modify state at the exporting side,
    // and verify that the two different imports still get identical results
    realm::Query query = table1->link(col_link2).column<String>(col_str) == "nabil";
    realm::TableView tv4 = query.find_all();

    auto rec1 = group_w->duplicate();
    auto q1 = rec1->import_copy_of(query, PayloadPolicy::Copy);
    auto rec2 = group_w->duplicate();
    auto q2 = rec2->import_copy_of(query, PayloadPolicy::Copy);

    {
        realm::TableView tv = q1->find_all();
        CHECK_EQUAL(0, tv.size());
    }

    // On the exporting side, change the data such that the query will now have
    // non-zero results if evaluated in that context.
    group_w->promote_to_write();
    auto o23 = table2->create_object().set_all(700, "nabil");
    links1.add(o23.get_key());
    group_w->commit_and_continue_as_read();
    CHECK_EQUAL(1, query.count());
    {
        // Import query and evaluate in the old context. This should *not* be
        // affected by the change done above on the exporting side.
        realm::TableView tv2 = q2->find_all();
        CHECK_EQUAL(0, tv2.size());
    }
}


TEST(LangBindHelper_HandoverQueryLinksTo)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));

    TransactionRef reader;
    std::unique_ptr<Query> query;
    std::unique_ptr<Query> queryOr;
    std::unique_ptr<Query> queryAnd;
    std::unique_ptr<Query> queryNot;
    std::unique_ptr<Query> queryAndAndOr;
    std::unique_ptr<Query> queryWithExpression;
    std::unique_ptr<Query> queryLinksToDetached;
    {
        auto group_w = sg->start_write();
        TableRef source = group_w->add_table("source");
        TableRef target = group_w->add_table("target");

        ColKey col_link = source->add_column_link(type_Link, "link", *target);
        ColKey col_name = target->add_column(type_String, "name");

        std::vector<ObjKey> keys;
        target->create_objects(4, keys);
        target->get_object(0).set(col_name, "A");
        target->get_object(1).set(col_name, "B");
        target->get_object(2).set(col_name, "C");
        target->get_object(3).set(col_name, "D");

        source->create_object().set_all(keys[0]);
        source->create_object().set_all(keys[1]);
        source->create_object().set_all(keys[2]);

        Obj detached_row = target->get_object(3);
        target->remove_object(detached_row.get_key());

        group_w->commit_and_continue_as_read();

        Query _query = source->column<Link>(col_link) == target->get_object(0);
        Query _queryOr = source->column<Link>(col_link) == target->get_object(0) ||
                         source->column<Link>(col_link) == target->get_object(1);
        Query _queryAnd = source->column<Link>(col_link) == target->get_object(0) &&
                          source->column<Link>(col_link) == target->get_object(0);
        Query _queryNot = !(source->column<Link>(col_link) == target->get_object(0)) &&
                          source->column<Link>(col_link) == target->get_object(1);
        Query _queryAndAndOr = source->where().group().and_query(_queryOr).end_group().and_query(_queryAnd);
        Query _queryWithExpression = source->column<Link>(col_link).is_not_null() && _query;
        Query _queryLinksToDetached = source->where().links_to(col_link, detached_row.get_key());

        // handover:
        reader = group_w->duplicate();
        query = reader->import_copy_of(_query, PayloadPolicy::Copy);
        queryOr = reader->import_copy_of(_queryOr, PayloadPolicy::Copy);
        queryAnd = reader->import_copy_of(_queryAnd, PayloadPolicy::Copy);
        queryNot = reader->import_copy_of(_queryNot, PayloadPolicy::Copy);
        queryAndAndOr = reader->import_copy_of(_queryAndAndOr, PayloadPolicy::Copy);
        queryWithExpression = reader->import_copy_of(_queryWithExpression, PayloadPolicy::Copy);
        queryLinksToDetached = reader->import_copy_of(_queryLinksToDetached, PayloadPolicy::Copy);

        CHECK_EQUAL(1, _query.count());
        CHECK_EQUAL(2, _queryOr.count());
        CHECK_EQUAL(1, _queryAnd.count());
        CHECK_EQUAL(1, _queryNot.count());
        CHECK_EQUAL(1, _queryAndAndOr.count());
        CHECK_EQUAL(1, _queryWithExpression.count());
        CHECK_EQUAL(0, _queryLinksToDetached.count());
    }
    {
        CHECK_EQUAL(1, query->count());
        CHECK_EQUAL(2, queryOr->count());
        CHECK_EQUAL(1, queryAnd->count());
        CHECK_EQUAL(1, queryNot->count());
        CHECK_EQUAL(1, queryAndAndOr->count());
        CHECK_EQUAL(1, queryWithExpression->count());
        CHECK_EQUAL(0, queryLinksToDetached->count());


        // Remove the linked-to row.
        {
            auto group_w = sg->start_write();
            TableRef target = group_w->get_table("target");
            target->remove_object(target->begin()->get_key());
            group_w->commit();
        }

        // Verify that the queries against the read-only shared group gives the same results.
        CHECK_EQUAL(1, query->count());
        CHECK_EQUAL(2, queryOr->count());
        CHECK_EQUAL(1, queryAnd->count());
        CHECK_EQUAL(1, queryNot->count());
        CHECK_EQUAL(1, queryAndAndOr->count());
        CHECK_EQUAL(1, queryWithExpression->count());
        CHECK_EQUAL(0, queryLinksToDetached->count());
    }
}


TEST(LangBindHelper_HandoverQuerySubQuery)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));

    TransactionRef reader;
    std::unique_ptr<Query> query;
    {
        auto group_w = sg->start_write();

        TableRef source = group_w->add_table("source");
        TableRef target = group_w->add_table("target");

        ColKey col_link = source->add_column_link(type_Link, "link", *target);
        ColKey col_name = target->add_column(type_String, "name");

        std::vector<ObjKey> keys;
        target->create_objects(3, keys);
        target->get_object(keys[0]).set(col_name, "A");
        target->get_object(keys[1]).set(col_name, "B");
        target->get_object(keys[2]).set(col_name, "C");

        source->create_object().set_all(keys[0]);
        source->create_object().set_all(keys[1]);
        source->create_object().set_all(keys[2]);

        group_w->commit_and_continue_as_read();

        realm::Query query_2 = source->column<Link>(col_link, target->column<String>(col_name) == "C").count() == 1;
        reader = group_w->duplicate();
        query = reader->import_copy_of(query_2, PayloadPolicy::Copy);
    }

    CHECK_EQUAL(1, query->count());

    // Remove the linked-to row.
    {
        auto group_w = sg->start_write();

        TableRef target = group_w->get_table("target");
        target->clear();
        group_w->commit_and_continue_as_read();
    }

    // Verify that the queries against the read-only shared group gives the same results.
    CHECK_EQUAL(1, query->count());
}

TEST(LangBindHelper_VersionControl)
{
    Random random(random_int<unsigned long>());

    const int num_versions = 10;
    const int num_random_tests = 100;
    DB::VersionID versions[num_versions];
    SHARED_GROUP_TEST_PATH(path);
    {
        // Create a new shared db
        std::unique_ptr<Replication> hist(make_in_realm_history(path));
        DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
        // first create 'num_version' versions
        ColKey col;
        auto reader = sg->start_read();
        {
            WriteTransaction wt(sg);
            col = wt.get_or_add_table("test")->add_column(type_Int, "a");
            wt.commit();
        }
        for (int i = 0; i < num_versions; ++i) {
            {
                WriteTransaction wt(sg);
                auto t = wt.get_table("test");
                t->create_object().set_all(i);
                wt.commit();
            }
            {
                auto rt = sg->start_read();
                versions[i] = rt->get_version_of_current_transaction();
            }
        }

        // do steps of increasing size from the first version to the last,
        // including a "step on the spot" (from version 0 to 0)
        {
            for (int k = 0; k < num_versions; ++k) {
                // std::cerr << "Advancing from initial version to version " << k << std::endl;
                auto g = sg->start_read(versions[0]);
                auto t = g->get_table("test");
                CHECK(versions[k] >= versions[0]);
                g->verify();
                g->advance_read(versions[k]);
                g->verify();
                auto o = *(t->begin() + k);
                CHECK_EQUAL(k, o.get<int64_t>(col));
            }
        }

        // step through the versions backward:
        for (int i = num_versions - 1; i >= 0; --i) {
            // std::cerr << "Jumping directly to version " << i << std::endl;

            auto g = sg->start_read(versions[i]);
            g->verify();
            auto t = g->get_table("test");
            auto o = *(t->begin() + i);
            CHECK_EQUAL(i, o.get<int64_t>(col));
        }

        // then advance through the versions going forward
        {
            auto g = sg->start_read(versions[0]);
            g->verify();
            auto t = g->get_table("test");
            for (int k = 0; k < num_versions; ++k) {
                // std::cerr << "Advancing to version " << k << std::endl;
                CHECK(k == 0 || versions[k] >= versions[k - 1]);

                g->advance_read(versions[k]);
                g->verify();
                auto o = *(t->begin() + k);
                CHECK_EQUAL(k, o.get<int64_t>(col));
            }
        }
        // sync to a randomly selected version - use advance_read when going
        // forward in time, but begin_read when going back in time
        int old_version = 0;
        auto g = sg->start_read(versions[old_version]);
        auto t = g->get_table("test");
        for (int k = num_random_tests; k; --k) {
            int new_version = random.draw_int_mod(num_versions);
            // std::cerr << "Random jump: version " << old_version << " -> " << new_version << std::endl;
            if (new_version < old_version) {
                CHECK(versions[new_version] < versions[old_version]);
                g->end_read();
                g = sg->start_read(versions[new_version]);
                g->verify();
                t = g->get_table("test");
                auto o = *(t->begin() + new_version);
                CHECK_EQUAL(new_version, o.get<int64_t>(col));
            }
            else {
                CHECK(versions[new_version] >= versions[old_version]);
                g->verify();
                g->advance_read(versions[new_version]);
                g->verify();
                auto o = *(t->begin() + new_version);
                CHECK_EQUAL(new_version, o.get<int64_t>(col));
            }
            old_version = new_version;
        }
        g->end_read();
        // release the first readlock and commit something to force a cleanup
        // we need to commit twice, because cleanup is done before the actual
        // commit, so during the first commit, the last of the previous versions
        // will still be kept. To get rid of it, we must commit once more.
        reader->end_read();
        g = sg->start_write();
        g->commit();
        g = sg->start_write();
        g->commit();

        // Validate that all the versions are now unreachable
        for (int i = 0; i < num_versions; ++i)
            CHECK_THROW(sg->start_read(versions[i]), DB::BadVersion);
    }
}

TEST(LangBindHelper_RollbackToInitialState1)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
    auto trans = sg_w->start_read();
    trans->promote_to_write();
    trans->rollback_and_continue_as_read();
}


TEST(LangBindHelper_RollbackToInitialState2)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
    auto trans = sg_w->start_write();
    trans->rollback();
}

TEST(LangBindHelper_Compact)
{
    SHARED_GROUP_TEST_PATH(path);
    size_t N = 100;

    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    {
        WriteTransaction w(sg);
        TableRef table = w.get_or_add_table("test");
        table->add_column(type_Int, "int");
        for (size_t i = 0; i < N; ++i) {
            table->create_object().set_all(static_cast<signed>(i));
        }
        w.commit();
    }
    {
        ReadTransaction r(sg);
        ConstTableRef table = r.get_table("test");
        CHECK_EQUAL(N, table->size());
    }
    {
        CHECK_EQUAL(true, sg->compact());
    }
    {
        ReadTransaction r(sg);
        ConstTableRef table = r.get_table("test");
        CHECK_EQUAL(N, table->size());
    }
    {
        WriteTransaction w(sg);
        TableRef table = w.get_or_add_table("test");
        table->create_object().set_all(0);
        w.commit();
    }
    {
        CHECK_EQUAL(true, sg->compact());
    }
}

TEST(LangBindHelper_CompactLargeEncryptedFile)
{
    SHARED_GROUP_TEST_PATH(path);

    std::vector<char> data(realm::util::page_size());
    const size_t N = 32;

    {
        std::unique_ptr<Replication> hist(make_in_realm_history(path));
        DBRef sg = DB::create(*hist, DBOptions(crypt_key(true)));
        WriteTransaction wt(sg);
        TableRef table = wt.get_or_add_table("test");
        table->add_column(type_String, "string");
        for (size_t i = 0; i < N; ++i) {
            table->create_object().set_all(StringData(data.data(), data.size()));
        }
        wt.commit();

        CHECK_EQUAL(true, sg->compact());

        sg->close();
    }

    {
        std::unique_ptr<Replication> hist(make_in_realm_history(path));
        DBRef sg = DB::create(*hist, DBOptions(crypt_key(true)));
        ReadTransaction r(sg);
        ConstTableRef table = r.get_table("test");
        CHECK_EQUAL(N, table->size());
    }
}

TEST(LangBindHelper_TableViewAggregateAfterAdvanceRead)
{
    SHARED_GROUP_TEST_PATH(path);

    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
    ColKey col;
    {
        WriteTransaction w(sg_w);
        TableRef table = w.add_table("test");
        col = table->add_column(type_Double, "double");
        table->create_object().set_all(1234.0);
        table->create_object().set_all(-5678.0);
        table->create_object().set_all(1000.0);
        w.commit();
    }

    auto reader = sg_w->start_read();
    auto table_r = reader->get_table("test");

    // Create a table view with all refs detached.
    TableView view = table_r->where().find_all();
    {
        WriteTransaction w(sg_w);
        w.get_table("test")->clear();
        w.commit();
    }
    reader->advance_read();

    // Verify that an aggregate on the view with detached refs gives the expected result.
    CHECK_EQUAL(false, view.is_in_sync());
    ObjKey res;
    double min = view.minimum_double(col, &res);
    CHECK_EQUAL(0, min);
    CHECK_EQUAL(ObjKey(), res);

    // Sync the view to discard the detached refs.
    view.sync_if_needed();

    // Verify that an aggregate on the view still gives the expected result.
    res = ObjKey();
    min = view.minimum_double(col, &res);
    CHECK_EQUAL(0, min);
    CHECK_EQUAL(ObjKey(), res);
}


#ifdef LEGACY_TESTS
// Tests handover of a Query. Especially it tests if next-gen-syntax nodes are deep copied correctly by
// executing an imported query multiple times in parallel
TEST(LangBindHelper_HandoverFuzzyTest)
{
    SHARED_GROUP_TEST_PATH(path);

    const size_t threads = 5;

    size_t numberOfOwner = 100;
    size_t numberOfDogsPerOwner = 20;

    std::vector<DB::VersionID> vids;
    std::vector<std::unique_ptr<DB::Handover<Query>>> qs;
    std::mutex vector_mutex;

    std::atomic<bool> end_signal(false);

    {
        std::unique_ptr<Replication> hist(make_in_realm_history(path));
        DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
        sg.begin_read();

        std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
        DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
        Group& group_w = const_cast<Group&>(sg_w.begin_read());

        // First setup data so that we can do a query on links
        LangBindHelper::promote_to_write(sg_w);

        TableRef owner = group_w.add_table("Owner");
        TableRef dog = group_w.add_table("Dog");

        owner->add_column(type_String, "name");
        owner->add_column_link(type_LinkList, "link", *dog);

        dog->add_column(type_String, "name");
        dog->add_column_link(type_Link, "link", *owner);

        for (size_t i = 0; i < numberOfOwner; i++) {

            size_t r_i = owner->add_empty_row();
            std::string owner_str(std::string("owner") + to_string(i));
            owner->set_string(0, r_i, owner_str);

            for (size_t j = 0; j < numberOfDogsPerOwner; j++) {
                size_t r_j = dog->add_empty_row();
                std::string dog_str(std::string("dog") + to_string(i * numberOfOwner + j));
                dog->set_string(0, r_j, dog_str);
                dog->set_link(1, r_j, i);
                LinkViewRef ll = owner->get_linklist(1, i);
                ll->add(r_j);
            }
        }

        LangBindHelper::commit_and_continue_as_read(sg_w);
    }

    auto async = [&]() {
        // Async thread
        //************************************************************************************************
        std::unique_ptr<Replication> hist(make_in_realm_history(path));
        DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
        sg.begin_read();

        while (!end_signal) {
            millisleep(10);

            vector_mutex.lock();
            if (qs.size() > 0) {

                DB::VersionID v = std::move(vids[0]);
                vids.erase(vids.begin());
                std::unique_ptr<DB::Handover<Query>> qptr = move(qs[0]);
                qs.erase(qs.begin());
                vector_mutex.unlock();

                // We cannot advance backwards compared to our initial begin_read() outside the while loop
                if (v >= sg.get_version_of_current_transaction()) {
                    LangBindHelper::advance_read(sg, v);
                    std::unique_ptr<Query> q(sg.import_from_handover(move(qptr)));
                    realm::TableView tv = q->find_all();
                }
            }
            else {
                vector_mutex.unlock();
            }
        }
        //************************************************************************************************
    };

    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    Group& group = const_cast<Group&>(sg.begin_read());

    // Create and export query
    TableRef owner = rt->get_table("Owner");
    TableRef dog = rt->get_table("Dog");

    realm::Query query = dog->link(1).column<String>(0) == "owner" + to_string(rand() % numberOfOwner);

    Thread slaves[threads];
    for (int i = 0; i != threads; ++i) {
        slaves[i].start([=] { async(); });
    }

    // Main thread
    //************************************************************************************************
    for (size_t iter = 0; iter < 20 + TEST_DURATION * TEST_DURATION * 500; iter++) {
        vector_mutex.lock();
        LangBindHelper::promote_to_write(sg);
        LangBindHelper::commit_and_continue_as_read(sg);
        if (qs.size() < 100) {
            for (size_t t = 0; t < 5; t++) {
                qs.push_back(sg.export_for_handover(query, MutableSourcePayload::Move));
                vids.push_back(sg.get_version_of_current_transaction());
            }
        }
        vector_mutex.unlock();

        millisleep(100);
    }
    //************************************************************************************************

    end_signal = true;
    for (int i = 0; i != threads; ++i)
        slaves[i].join();
}
#endif


// TableView::clear() was originally reported to be slow when table was indexed and had links, but performance
// has now doubled. This test is just a short sanity test that clear() still works.
TEST(LangBindHelper_TableViewClear)
{
    SHARED_GROUP_TEST_PATH(path);

    int64_t number_of_history = 1000;
    int64_t number_of_line = 18;

    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg = DB::create(*hist_w, DBOptions(crypt_key()));
    TransactionRef tr;
    ColKey col0, col1, col2, colA, colB;
    // set up tables:
    // history : ["id" (int), "parent" (int), "lines" (list(line))]
    // line    : ["id" (int), "parent" (int)]
    {
        tr = sg->start_write();

        TableRef history = tr->add_table("history");
        TableRef line = tr->add_table("line");

        col0 = history->add_column(type_Int, "id");
        col1 = history->add_column(type_Int, "parent");
        col2 = history->add_column_link(type_LinkList, "lines", *line);
        history->add_search_index(col1);

        colA = line->add_column(type_Int, "id");
        colB = line->add_column(type_Int, "parent");
        line->add_search_index(colB);
        tr->commit_and_continue_as_read();
    }

    {
        tr->promote_to_write();

        TableRef history = tr->get_table("history");
        TableRef line = tr->get_table("line");

        auto obj = history->create_object();
        obj.set(col0, 1);
        auto ll = obj.get_linklist(col2);
        for (int64_t j = 0; j < number_of_line; ++j) {
            Obj o = line->create_object().set_all(j, 0);
            ll.add(o.get_key());
        }

        for (int64_t i = 1; i < number_of_history; ++i) {
            history->create_object().set_all(i, i + 1);
            int64_t rj = i * number_of_line;
            for (int64_t j = 1; j <= number_of_line; ++j) {
                line->create_object().set_all(rj, j);
                ++rj;
            }
        }
        tr->commit_and_continue_as_read();
        CHECK_EQUAL(number_of_history, history->size());
        CHECK_EQUAL(number_of_history * number_of_line, line->size());
    }

    // query and delete
    {
        tr->promote_to_write();

        TableRef history = tr->get_table("history");
        TableRef line = tr->get_table("line");

        //    number_of_line = 2;
        for (int64_t i = 1; i <= number_of_line; ++i) {
            TableView tv = (line->column<Int>(colB) == i).find_all();
            tv.clear();
        }
        tr->commit_and_continue_as_read();
    }

    {
        TableRef history = tr->get_table("history");
        TableRef line = tr->get_table("line");

        CHECK_EQUAL(number_of_history, history->size());
        CHECK_EQUAL(number_of_line, line->size());
    }
}


TEST(LangBindHelper_SessionHistoryConsistency)
{
    // Check that we can reliably detect inconsist history
    // types across concurrent session participants.

    // Errors of this kind are considered as incorrect API usage, and will lead
    // to throwing of LogicError exceptions.

    SHARED_GROUP_TEST_PATH(path);

    // When starting with an empty Realm, all history types are allowed, but all
    // session participants must still agree
    {
        // No history
        DBRef sg = DB::create(path, false, DBOptions(crypt_key()));

        // Out-of-Realm history
        std::unique_ptr<Replication> hist = realm::make_in_realm_history(path);
        CHECK_LOGIC_ERROR(DB::create(*hist, DBOptions(crypt_key())), LogicError::mixed_history_type);
    }
}


TEST(LangBindHelper_InRealmHistory_Upgrade)
{
    SHARED_GROUP_TEST_PATH(path_1);
    {
        // Out-of-Realm history
        std::unique_ptr<Replication> hist = make_in_realm_history(path_1);
        DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
        WriteTransaction wt(sg);
        wt.commit();
    }
    {
        // In-Realm history
        std::unique_ptr<Replication> hist = make_in_realm_history(path_1);
        DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
        WriteTransaction wt(sg);
        wt.commit();
    }
    SHARED_GROUP_TEST_PATH(path_2);
    {
        // No history
        DBRef sg = DB::create(path_2, false, DBOptions(crypt_key()));
        WriteTransaction wt(sg);
        wt.commit();
    }
    {
        // In-Realm history
        std::unique_ptr<Replication> hist = make_in_realm_history(path_2);
        DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
        WriteTransaction wt(sg);
        wt.commit();
    }
}

TEST(LangBindHelper_InRealmHistory_Downgrade)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        // In-Realm history
        std::unique_ptr<Replication> hist = make_in_realm_history(path);
        DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
        WriteTransaction wt(sg);
        wt.commit();
    }
    {
        // No history
        CHECK_THROW(DB::create(path, false, DBOptions(crypt_key())), IncompatibleHistories);
    }
}

// Trigger erase_rows with num_rows == 0 by inserting zero rows
// and then rolling back the transaction. There was a problem
// where accessors were not updated correctly in this case because
// of an early out when num_rows_to_erase is zero.
TEST(LangBindHelper_RollbackInsertZeroRows)
{
    SHARED_GROUP_TEST_PATH(path)
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg = DB::create(*hist_w, DBOptions(crypt_key()));
    auto g = sg->start_write();

    auto t0 = g->add_table("t0");
    auto t1 = g->add_table("t1");

    auto col = t0->add_column_link(type_Link, "t0_link_to_t1", *t1);
    auto o0 = t0->create_object();
    auto o1 = t0->create_object();
    auto v0 = t1->create_object();
    auto v1 = t1->create_object();
    o1.set(col, v1.get_key());

    CHECK_EQUAL(t0->size(), 2);
    CHECK_EQUAL(t1->size(), 2);
    CHECK_EQUAL(o1.get<ObjKey>(col), v1.get_key());

    g->commit_and_continue_as_read();
    g->promote_to_write();

    std::vector<ObjKey> keys;
    t1->create_objects(0, keys); // Insert zero rows

    CHECK_EQUAL(t0->size(), 2);
    CHECK_EQUAL(t1->size(), 2);
    CHECK_EQUAL(o1.get<ObjKey>(col), v1.get_key());

    g->rollback_and_continue_as_read();
    g->verify();

    CHECK_EQUAL(t0->size(), 2);
    CHECK_EQUAL(t1->size(), 2);
    CHECK_EQUAL(o1.get<ObjKey>(col), v1.get_key());
}


TEST(LangBindHelper_RollbackRemoveZeroRows)
{
    SHARED_GROUP_TEST_PATH(path)
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg = DB::create(*hist_w, DBOptions(crypt_key()));
    auto g = sg->start_write();

    auto t0 = g->add_table("t0");
    auto t1 = g->add_table("t1");

    auto col = t0->add_column_link(type_Link, "t0_link_to_t1", *t1);
    auto o0 = t0->create_object();
    auto o1 = t0->create_object();
    auto v0 = t1->create_object();
    auto v1 = t1->create_object();
    o1.set(col, v1.get_key());

    CHECK_EQUAL(t0->size(), 2);
    CHECK_EQUAL(t1->size(), 2);
    CHECK_EQUAL(o1.get<ObjKey>(col), v1.get_key());

    g->commit_and_continue_as_read();
    g->promote_to_write();

    t1->clear();

    CHECK_EQUAL(t0->size(), 2);
    CHECK_EQUAL(t1->size(), 0);
    CHECK_EQUAL(o1.get<ObjKey>(col), ObjKey());

    g->rollback_and_continue_as_read();
    g->verify();

    CHECK_EQUAL(t0->size(), 2);
    CHECK_EQUAL(t1->size(), 2);
    CHECK_EQUAL(o1.get<ObjKey>(col), v1.get_key());
}

// Bug found by AFL during development of TimestampColumn
TEST_TYPES(LangBindHelper_AddEmptyRowsAndRollBackTimestamp, std::true_type, std::false_type)
{
    constexpr bool nullable_toggle = TEST_TYPE::value;
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
    auto g = sg_w->start_write();
    TableRef t = g->add_table("");
    t->add_column(type_Int, "", nullable_toggle);
    t->add_column(type_Timestamp, "gnyf", nullable_toggle);
    g->commit_and_continue_as_read();
    g->promote_to_write();
    std::vector<ObjKey> keys;
    t->create_objects(224, keys);
    g->rollback_and_continue_as_read();
    g->verify();
}

// Another bug found by AFL during development of TimestampColumn
TEST_TYPES(LangBindHelper_EmptyWrites, std::true_type, std::false_type)
{
    constexpr bool nullable_toggle = TEST_TYPE::value;
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
    auto g = sg_w->start_write();
    TableRef t = g->add_table("");
    t->add_column(type_Timestamp, "gnyf", nullable_toggle);

    for (int i = 0; i < 27; ++i) {
        g->commit_and_continue_as_read();
        g->promote_to_write();
    }

    t->create_object();
}


// Found by AFL
TEST_TYPES(LangBindHelper_SetTimestampRollback, std::true_type, std::false_type)
{
    constexpr bool nullable_toggle = TEST_TYPE::value;
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg = DB::create(*hist_w, DBOptions(crypt_key()));
    auto g = sg->start_write();
    auto table = g->add_table("");
    table->add_column(type_Timestamp, "gnyf", nullable_toggle);
    table->create_object().set_all(Timestamp(-1, -1));
    g->rollback_and_continue_as_read();
    g->verify();
}


// Found by AFL, probably related to the rollback version above
TEST_TYPES(LangBindHelper_SetTimestampAdvanceRead, std::true_type, std::false_type)
{
    constexpr bool nullable_toggle = TEST_TYPE::value;
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    auto g_r = sg->start_read();
    auto g_w = sg->start_write();
    auto table = g_w->add_table("");
    table->add_column(type_Timestamp, "gnyf", nullable_toggle);
    table->create_object().set_all(Timestamp(-1, -1));
    g_w->commit_and_continue_as_read();
    g_w->verify();
    g_r->advance_read();
    g_r->verify();
}


// Found by AFL.
TEST(LangbindHelper_BoolSearchIndexCommitPromote)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    auto g = sg->start_write();
    auto t = g->add_table("");
    auto col = t->add_column(type_Bool, "gnyf", true);
    std::vector<ObjKey> keys;
    t->create_objects(5, keys);
    t->get_object(keys[0]).set(col, false);
    t->add_search_index(col);
    g->commit_and_continue_as_read();
    g->promote_to_write();
    t->create_objects(5, keys);
    t->remove_object(keys[8]);
}


// Found by AFL.
TEST(LangbindHelper_GroupWriter_EdgeCaseAssert)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    auto g_r = sg->start_read();
    auto g_w = sg->start_write();

    auto t1 = g_w->add_table("dgrpnpgmjbchktdgagmqlihjckcdhpjccsjhnqlcjnbterse");
    auto t2 = g_w->add_table("pknglaqnckqbffehqfgjnrepcfohoedkhiqsiedlotmaqitm");
    t1->add_column(type_Double, "ggotpkoshbrcrmmqbagbfjetajlrrlbpjhhqrngfgdteilmj", true);
    t2->add_column_link(type_LinkList, "dtkiipajqdsfglbptieibknaoeeohqdlhftqmlriphobspjr", *t1);
    std::vector<ObjKey> keys;
    t1->create_objects(375, keys);
    auto t3 = g_w->add_table("pnsidlijqeddnsgaesiijrrqedkdktmfekftogjccerhpeil");
    g_r->close();
    g_w->commit();
    REALM_ASSERT_RELEASE(sg->compact());
    g_w = sg->start_write();
    g_r = sg->start_read();
    g_r->verify();
    g_w->add_table("citdgiaclkfbbksfaqegcfiqcserceaqmttkilnlbknoadtb");
    g_w->add_table("tqtnnikpggeakeqcqhfqtshmimtjqkchgbnmbpttbetlahfi");
    g_w->add_table("hkesaecjqbkemmmkffctacsnskekjbtqmpoetjnqkpactenf");
    g_r->close();
    g_w->commit();
}

TEST(LangBindHelper_Bug2321)
{
    SHARED_GROUP_TEST_PATH(path);
    ShortCircuitHistory hist(path);
    DBRef sg = DB::create(hist, DBOptions(crypt_key()));
    int i;
    std::vector<ObjKey> target_keys;
    std::vector<ObjKey> origin_keys;
    ColKey col;
    {
        WriteTransaction wt(sg);
        Group& group = wt.get_group();
        TableRef target = group.add_table("target");
        target->add_column(type_Int, "data");
        target->create_objects(REALM_MAX_BPNODE_SIZE + 2, target_keys);
        TableRef origin = group.add_table("origin");
        col = origin->add_column_link(type_LinkList, "_link", *target);
        origin->create_objects(2, origin_keys);
        wt.commit();
    }

    {
        WriteTransaction wt(sg);
        Group& group = wt.get_group();
        TableRef origin = group.get_table("origin");
        auto lv0 = origin->begin()->get_linklist(col);
        for (i = 0; i < (REALM_MAX_BPNODE_SIZE - 1); i++) {
            lv0.add(target_keys[i]);
        }
        wt.commit();
    }

    auto reader = sg->start_read();
    auto lv1 = reader->get_table("origin")->begin()->get_linklist(col);
    {
        WriteTransaction wt(sg);
        Group& group = wt.get_group();
        TableRef origin = group.get_table("origin");
        auto lv0 = origin->begin()->get_linklist(col);
        lv0.add(target_keys[i++]);
        lv0.add(target_keys[i++]);
        wt.commit();
    }

    // If MAX_BPNODE_SIZE is 4 and we run in debug mode, then the LinkView
    // accessor was not refreshed correctly. It would still be a leaf class,
    // but the header flags would tell it is a node.
    reader->advance_read();
    CHECK_EQUAL(lv1.size(), i);
}

TEST(LangBindHelper_Bug2295)
{
    SHARED_GROUP_TEST_PATH(path);
    ShortCircuitHistory hist(path);
    DBRef sg = DB::create(hist, DBOptions(crypt_key()));
    int i;
    std::vector<ObjKey> target_keys;
    std::vector<ObjKey> origin_keys;
    ColKey col;
    {
        WriteTransaction wt(sg);
        Group& group = wt.get_group();
        TableRef target = group.add_table("target");
        target->add_column(type_Int, "data");
        target->create_objects(REALM_MAX_BPNODE_SIZE + 2, target_keys);
        TableRef origin = group.add_table("origin");
        col = origin->add_column_link(type_LinkList, "_link", *target);
        origin->create_objects(2, origin_keys);
        wt.commit();
    }

    {
        WriteTransaction wt(sg);
        Group& group = wt.get_group();
        TableRef origin = group.get_table("origin");
        auto lv0 = origin->begin()->get_linklist(col);
        for (i = 0; i < (REALM_MAX_BPNODE_SIZE - 1); i++) {
            lv0.add(target_keys[i]);
        }
        wt.commit();
    }

    auto reader = sg->start_read();
    auto lv1 = reader->get_table("origin")->begin()->get_linklist(col);
    CHECK_EQUAL(lv1.size(), i);
    {
        WriteTransaction wt(sg);
        Group& group = wt.get_group();
        TableRef origin = group.get_table("origin");
        // With the error present, this will cause some areas to be freed
        // that has already been freed in the above transaction
        auto lv0 = origin->begin()->get_linklist(col);
        lv0.add(target_keys[i++]);
        wt.commit();
    }
    reader->promote_to_write();
    // Here we write the duplicates to the free list
    reader->commit_and_continue_as_read();
    reader->verify();
    CHECK_EQUAL(lv1.size(), i);
}

#ifdef LEGACY_TESTS
// FIXME: Requires get_at() method to be available on ConstObj.
ONLY(LangBindHelper_BigBinary)
{
    SHARED_GROUP_TEST_PATH(path);
    ShortCircuitHistory hist(path);
    DBRef sg = DB::create(hist);
    std::string big_data(0x1000000, 'x');
    auto rt = sg->start_read();
    auto wt = sg->start_write();

    std::string data(16777362, 'y');
    TableRef target = wt->add_table("big");
    auto col = target->add_column(type_Binary, "data");
    target->create_object().set(col, BinaryData(data.data(), data.size()));
    wt->commit();
    rt->advance_read();
    {
        WriteTransaction wt(sg);
        TableRef t = wt.get_table("big");
        t->begin()->set(col, BinaryData(big_data.data(), big_data.size()));
        wt.get_group().verify();
        wt.commit();
    }
    rt->advance_read();
    auto t = rt->get_table("big");
    size_t pos = 0;
    BinaryData bin = t->begin()->get_at(col, pos); // <---- not there yet?
    CHECK_EQUAL(memcmp(big_data.data(), bin.data(), bin.size()), 0);
}
#endif

TEST(LangBindHelper_CopyOnWriteOverflow)
{
    SHARED_GROUP_TEST_PATH(path);
    ShortCircuitHistory hist(path);
    DBRef sg = DB::create(hist);
    auto g = sg->start_write();
    auto table = g->add_table("big");
    auto obj = table->create_object();
    auto col = table->add_column(type_Binary, "data");
    std::string data(0xfffff0, 'x');
    obj.set(col, BinaryData(data.data(), data.size()));
    g->commit();
    g = sg->start_write();
    g->get_table("big")->begin()->set(col, BinaryData{"Hello", 5});
    g->verify();
    g->commit();
}


TEST(LangBindHelper_RollbackOptimize)
{
    SHARED_GROUP_TEST_PATH(path);
    const char* key = crypt_key();
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(key));
    auto g = sg_w->start_write();

    auto table = g->add_table("t0");
    auto col = table->add_column(type_String, "str_col_0", true);
    g->commit_and_continue_as_read();
    g->verify();
    g->promote_to_write();
    g->verify();
    std::vector<ObjKey> keys;
    table->create_objects(198, keys);
    table->enumerate_string_column(col);
    g->rollback_and_continue_as_read();
    g->verify();
}


TEST(LangBindHelper_BinaryReallocOverMax)
{
    SHARED_GROUP_TEST_PATH(path);
    const char* key = crypt_key();
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(key));
    auto g = sg_w->start_write();
    auto table = g->add_table("table");
    auto col = table->add_column(type_Binary, "binary_col", false);
    auto obj = table->create_object();

    // The sizes of these binaries were found with AFL. Essentially we must hit
    // the case where doubling the allocated memory goes above max_array_payload
    // and hits the condition to clamp to the maximum.
    std::string blob1(8877637, static_cast<unsigned char>(133));
    std::string blob2(15994373, static_cast<unsigned char>(133));
    BinaryData dataAlloc(blob1);
    BinaryData dataRealloc(blob2);

    obj.set(col, dataAlloc);
    obj.set(col, dataRealloc);
    g->verify();
}


// This test verifies that small unencrypted files are treated correctly if
// opened as encrypted.
#if REALM_ENABLE_ENCRYPTION
TEST(LangBindHelper_OpenAsEncrypted)
{
    SHARED_GROUP_TEST_PATH(path);
    {
        ShortCircuitHistory hist(path);
        DBRef sg_clear = DB::create(hist);

        {
            WriteTransaction wt(sg_clear);
            TableRef target = wt.add_table("table");
            target->add_column(type_String, "mixed_col");
            target->create_object();
            wt.commit();
        }
    }
    {
        const char* key = crypt_key(true);
        std::unique_ptr<Replication> hist_encrypt(make_in_realm_history(path));
        bool is_okay = false;
        try {
            DBRef sg_encrypt = DB::create(*hist_encrypt, DBOptions(key));
        } catch (std::runtime_error&) {
            is_okay = true;
        }
        CHECK(is_okay);
    }
}
#endif


// Test case generated in [realm-core-4.0.4] on Mon Dec 18 13:33:24 2017.
// Adding 0 rows to a StringEnumColumn would add the default value to the keys
// but not the indexes creating an inconsistency.
TEST(LangBindHelper_EnumColumnAddZeroRows)
{
    SHARED_GROUP_TEST_PATH(path);
    const char* key = nullptr;
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(key));
    auto g = sg->start_write();
    auto g_r = sg->start_read();
    auto table = g->add_table("");

    auto col = table->add_column(DataType(2), "table", false);
    table->enumerate_string_column(col);
    g->commit_and_continue_as_read();
    g->verify();
    g->promote_to_write();
    g->verify();
    table->create_object();
    g->commit_and_continue_as_read();
    g_r->advance_read();
    g_r->verify();
    g->verify();
}


TEST(LangBindHelper_NonsharedAccessToRealmWithHistory)
{
    // Create a Realm file with a history (history_type !=
    // Reaplication::hist_None).
    SHARED_GROUP_TEST_PATH(path);
    {
        std::unique_ptr<Replication> history(make_in_realm_history(path));
        DBRef sg = DB::create(*history);
        WriteTransaction wt{sg};
        wt.add_table("foo");
        wt.commit();
    }

    // Since the stored history type is now Replication::hist_InRealm, it should
    // now be impossible to open in shared mode with no replication plugin
    // (Replication::hist_None).
    CHECK_THROW(DB::create(path), IncompatibleHistories);

    // Now modify the file in nonshared mode, which will discard the history (as
    // nonshared mode does not understand how to update it correctly).
    {
        const char* crypt_key = nullptr;
        Group group{path, crypt_key, Group::mode_ReadWriteNoCreate};
        group.commit();
    }

    // Check the the history was actually discarded (reset to
    // Replication::hist_None).
    DBRef sg = DB::create(path);
    ReadTransaction rt{sg};
    CHECK(rt.has_table("foo"));
}

TEST(LangBindHelper_RemoveObject)
{
    SHARED_GROUP_TEST_PATH(path);
    ShortCircuitHistory hist(path);
    DBRef sg = DB::create(hist);
    ColKey col;
    auto rt = sg->start_read();
    {
        auto wt = sg->start_write();
        TableRef t = wt->add_table("Foo");
        col = t->add_column(type_Int, "int");
        t->create_object(ObjKey(123)).set(col, 1);
        t->create_object(ObjKey(456)).set(col, 2);
        wt->commit();
    }

    rt->advance_read();
    auto table = rt->get_table("Foo");
    ConstObj o1 = table->get_object(ObjKey(123));
    ConstObj o2 = table->get_object(ObjKey(456));
    CHECK_EQUAL(o1.get<int64_t>(col), 1);
    CHECK_EQUAL(o2.get<int64_t>(col), 2);

    {
        auto wt = sg->start_write();
        TableRef t = wt->get_table("Foo");
        t->remove_object(ObjKey(123));
        wt->commit();
    }
    rt->advance_read();
    CHECK_THROW(o1.get<int64_t>(col), InvalidKey);
    CHECK_EQUAL(o2.get<int64_t>(col), 2);
}

TEST(LangBindHelper_callWithLock)
{
    SHARED_GROUP_TEST_PATH(path);
    DB::CallbackWithLock callback = [this, &path](const std::string& realm_path) {
        CHECK(realm_path.compare(path) == 0);
    };

    DB::CallbackWithLock callback_not_called = [=](const std::string&) { CHECK(false); };

    // call_with_lock should run the callback if the lock file doesn't exist.
    CHECK_NOT(File::exists(path.get_lock_path()));
    CHECK(DB::call_with_lock(path, callback));
    CHECK(File::exists(path.get_lock_path()));

    {
        std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
        DBRef sg_w = DB::create(*hist_w);
        WriteTransaction wt(sg_w);
        CHECK_NOT(DB::call_with_lock(path, callback_not_called));
        wt.commit();
        CHECK_NOT(DB::call_with_lock(path, callback_not_called));
    }
    CHECK(DB::call_with_lock(path, callback));
}

TEST(LangBindHelper_getCoreFiles)
{
    TEST_DIR(dir);
    std::string realm_path = std::string(dir) + "/test.realm";

    {
        std::unique_ptr<Replication> hist_w(make_in_realm_history(realm_path));
        DBRef sg_w = DB::create(*hist_w);
        WriteTransaction wt(sg_w);
        wt.commit();
    }

    auto core_files = DB::get_core_files(realm_path);
    CHECK(core_files.size() > 0);

    std::string file;
    DirScanner scaner(dir);
    while (scaner.next(file)) {
        const std::string lock_suffix = ".lock";
        if (file.size() >= lock_suffix.size() &&
            file.compare(file.size() - lock_suffix.size(), lock_suffix.size(), lock_suffix) == 0) {
            continue;
        }
        std::string path(std::string(dir) + "/" + file);
        auto file_pair = std::make_pair(path, File::is_dir(path));
        CHECK(core_files.size() != 0);
        core_files.erase(std::remove(core_files.begin(), core_files.end(), file_pair), core_files.end());
    }

    CHECK(core_files.size() == 0);
}

TEST(LangBindHelper_AdvanceReadCluster)
{
    SHARED_GROUP_TEST_PATH(path);
    ShortCircuitHistory hist(path);
    DBRef sg = DB::create(hist);

    auto rt = sg->start_read();
    {
        auto wt = sg->start_write();
        TableRef t = wt->add_table("Foo");
        auto int_col = t->add_column(type_Int, "int");
        for (int64_t i = 0; i < 100; i++) {
            t->create_object(ObjKey(i)).set(int_col, i);
        }
        wt->commit();
    }

    rt->advance_read();
    auto table = rt->get_table("Foo");
    auto col = table->get_column_key("int");
    for (int64_t i = 0; i < 100; i++) {
        ConstObj o = table->get_object(ObjKey(i));
        CHECK_EQUAL(o.get<int64_t>(col), i);
    }
}

#endif
