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
            table_w->create_object().set_all(str, "foo", str);
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

#ifdef LEGACY_TESTS
// to be ported
TEST(LangBindHelper_AdvanceReadTransact_CascadeRemove_ColumnLink)
{
    SHARED_GROUP_TEST_PATH(path);
    ShortCircuitHistory hist(path);
    DBRef sg = DB::create(hist, DBOptions(crypt_key()));
    DBRef sg_w = DB::create(hist, DBOptions(crypt_key()));

    {
        WriteTransaction wt(sg_w);
        Table& origin = *wt.add_table("origin");
        Table& target = *wt.add_table("target");
        origin.add_column_link(type_Link, "o_1", target, link_Strong);
        target.add_column(type_Int, "t_1");
        wt.commit();
    }

    // Start a read transaction (to be repeatedly advanced)
    TransactionRef rt = sg->start_read() const Group& group = rt;
    const Table& target = *rt->get_table("target");

    ConstRow target_row_0, target_row_1;

    auto perform_change = [&](std::function<void(Table&)> func) {
        // Ensure there are two rows in each table, with each row in `origin`
        // pointing to the corresponding row in `target`
        {
            WriteTransaction wt(sg_w);
            Table& origin_w = *wt.get_table("origin");
            Table& target_w = *wt.get_table("target");

            origin_w.clear();
            target_w.clear();
            origin_w.add_empty_row(2);
            target_w.add_empty_row(2);
            origin_w[0].set_link(0, 0);
            origin_w[1].set_link(0, 1);

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

    // Break link by nullifying
    perform_change([](Table& origin) { origin[1].nullify_link(0); });
    CHECK(target_row_0 && !target_row_1);
    CHECK_EQUAL(target.size(), 1);

    // Break link by reassign
    perform_change([](Table& origin) { origin[1].set_link(0, 0); });
    CHECK(target_row_0 && !target_row_1);
    CHECK_EQUAL(target.size(), 1);

    // Avoid breaking link by reassigning self
    perform_change([](Table& origin) { origin[1].set_link(0, 1); });
    // Should not delete anything
    CHECK(target_row_0 && target_row_1);
    CHECK_EQUAL(target.size(), 2);

    // Break link by explicit row removal
    perform_change([](Table& origin) { origin[1].move_last_over(); });
    CHECK(target_row_0 && !target_row_1);
    CHECK_EQUAL(target.size(), 1);

    // Break link by clearing table
    perform_change([](Table& origin) { origin.clear(); });
    CHECK(!target_row_0 && !target_row_1);
    CHECK_EQUAL(target.size(), 0);
}

// to be ported
TEST(LangBindHelper_AdvanceReadTransact_CascadeRemove_ColumnLinkList)
{
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

    // Break link by clearing list
    perform_change([](Table& origin) { origin[1].get_linklist(0)->clear(); });
    CHECK(target_row_0 && !target_row_1);
    CHECK_EQUAL(target.size(), 1);

    // Break link by removal from list
    perform_change([](Table& origin) { origin[1].get_linklist(0)->remove(1); });
    CHECK(target_row_0 && !target_row_1);
    CHECK_EQUAL(target.size(), 1);

    // Break link by reassign
    perform_change([](Table& origin) { origin[1].get_linklist(0)->set(1, 0); });
    CHECK(target_row_0 && !target_row_1);
    CHECK_EQUAL(target.size(), 1);

    // Avoid breaking link by reassigning self
    perform_change([](Table& origin) { origin[1].get_linklist(0)->set(1, 1); });
    // Should not delete anything
    CHECK(target_row_0 && target_row_1);
    CHECK_EQUAL(target.size(), 2);

    // Break link by explicit row removal
    perform_change([](Table& origin) { origin[1].move_last_over(); });
    CHECK(target_row_0 && !target_row_1);
    CHECK_EQUAL(target.size(), 1);

    // Break link by clearing table
    perform_change([](Table& origin) { origin.clear(); });
    CHECK(!target_row_0 && !target_row_1);
    CHECK_EQUAL(target.size(), 0);
}
#endif

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
    bool modify_object(ColKey, ObjKey)
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
    bool set_int(size_t, size_t, int_fast64_t, _impl::Instruction, size_t)
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
#ifdef LEGACY_TESTS
    ObjKey o0, o1;
    {
        // Make a simple modification and verify that the appropriate handler is called
        WriteTransaction wt(sg);
        o0 = wt.get_table("table 1")->create_object().get_key();
        o1 = wt.get_table("table 2")->create_object().get_key();
        wt.commit();

        struct foo : NoOpTransactionLogParser {
            using NoOpTransactionLogParser::NoOpTransactionLogParser;

            size_t expected_table = 0;

            bool create_object(ObjKey ok)
            {
                CHECK_EQUAL(expected_table ? o1.get_table().get_key() : o0.get_table().get_key(),
                            get_current_table());
                ++expected_table;

                CHECK_EQUAL(0, row_ndx);
                CHECK_EQUAL(1, num_rows_to_insert);
                CHECK_EQUAL(0, prior_num_rows);
                CHECK(!unordered);

                return true;
            }
        } parser(test_context);
        TEST_TYPE::call(tr, &parser);
        CHECK_EQUAL(2, parser.expected_table);
    }
    ColKey c2, c3;
    {
        // Add a table with some links
        WriteTransaction wt(sg);
        TableRef table = wt.add_table("link origin");
        c2 = table->add_column_link(type_Link, "link", *wt.get_table("table 1"));
        c3 = table->add_column_link(type_LinkList, "linklist", *wt.get_table("table 2"));
        Obj o = table->create_object();
        o.set(c2, o.get_key());
        o.get_linklist(c3).add(o.get_key());
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

            bool erase_rows(size_t row_ndx, size_t num_rows_to_erase, size_t prior_num_rows, bool unordered)
            {
                CHECK_EQUAL(0, row_ndx);
                CHECK_EQUAL(1, num_rows_to_erase);
                CHECK_EQUAL(1, prior_num_rows);
                CHECK(unordered);
                return true;
            }

            bool link_list_nullify(size_t ndx, size_t)
            {
                CHECK_EQUAL(2, get_current_table());
                CHECK_EQUAL(1, get_current_linkview().first);
                CHECK_EQUAL(0, get_current_linkview().second);

                CHECK_EQUAL(0, ndx);
                return true;
            }

            bool nullify_link(size_t col_ndx, size_t row_ndx, size_t)
            {
                CHECK_EQUAL(2, get_current_table());
                CHECK_EQUAL(0, col_ndx);
                CHECK_EQUAL(0, row_ndx);
                return true;
            }
        } parser(test_context);
        TEST_TYPE::call(tr, &parser);
    }
    {
        // Verify that clear() logs the correct rows
        WriteTransaction wt(sg_w);
        wt.get_table("table 2")->add_empty_row(10);

        LinkViewRef lv = wt.get_table("link origin")->get_linklist(1, 0);
        lv->add(1);
        lv->add(3);
        lv->add(5);

        wt.commit();
        rt->advance_read();
    }

    {
        WriteTransaction wt(sg_w);
        wt.get_table("link origin")->get_linklist(1, 0)->clear();
        wt.commit();

        struct : NoOpTransactionLogParser {
            using NoOpTransactionLogParser::NoOpTransactionLogParser;

            bool link_list_clear(size_t old_list_size) const
            {
                CHECK_EQUAL(2, get_current_table());
                CHECK_EQUAL(1, get_current_linkview().first);
                CHECK_EQUAL(0, get_current_linkview().second);

                CHECK_EQUAL(3, old_list_size);
                return true;
            }
        } parser(test_context);
        TEST_TYPE::call(sg, parser);
    }
#endif
}
#ifdef LEGACY_TESTS
#endif

#ifdef LEGACY_TESTS
TEST(LangBindHelper_AdvanceReadTransact_ErrorInObserver)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));

    // Add some initial data and then begin a read transaction at that version
    {
        WriteTransaction wt(sg);
        TableRef table = wt.add_table("Table");
        table->add_column(type_Int, "int");
        table->add_empty_row();
        table->set_int(0, 0, 10);
        wt.commit();
    }
    const Group& g = sg.begin_read();

    // Modify the data with a different SG so that we can determine which version
    // the read transaction is using
    {
        std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
        DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
        WriteTransaction wt(sg_w);
        wt.get_table("Table")->set_int(0, 0, 20);
        wt.commit();
    }

    struct ObserverError {
    };
    try {
        struct : NoOpTransactionLogParser {
            using NoOpTransactionLogParser::NoOpTransactionLogParser;

            bool set_int(size_t, size_t, int_fast64_t, _impl::Instruction, size_t) const
            {
                throw ObserverError();
            }
        } parser(test_context);

        LangBindHelper::advance_read(sg, parser);
        CHECK(false); // Should not be reached
    }
    catch (ObserverError) {
    }

    // Should still see data from old version
    CHECK_EQUAL(10, g.get_table("Table")->get_int(0, 0));

    // Should be able to advance to the new version still
    rt->advance_read();

    // And see that version's data
    CHECK_EQUAL(20, g.get_table("Table")->get_int(0, 0));
}
#endif


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


#ifdef LEGACY_TESTS
// to be ported
TEST(LangBindHelper_RollbackAndContinueAsRead)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    {
        Group* group = const_cast<Group*>(&sg.begin_read());
        {
            LangBindHelper::promote_to_write(sg);
            TableRef origin = group->get_or_add_table("origin");
            origin->add_column(type_Int, "");
            origin->add_empty_row();
            origin->set_int(0, 0, 42);
            LangBindHelper::commit_and_continue_as_read(sg);
        }
        group->verify();
        {
            // rollback of group level table insertion
            LangBindHelper::promote_to_write(sg);
            TableRef o = group->get_or_add_table("nullermand");
            TableRef o2 = group->get_table("nullermand");
            REALM_ASSERT(o2);
            LangBindHelper::rollback_and_continue_as_read(sg);
            TableRef o3 = group->get_table("nullermand");
            REALM_ASSERT(!o3);
            REALM_ASSERT(o2->is_attached() == false);
        }

        TableRef origin = group->get_table("origin");
        Row row = origin->get(0);
        CHECK_EQUAL(42, origin->get_int(0, 0));

        {
            LangBindHelper::promote_to_write(sg);
            origin->insert_empty_row(0);
            origin->set_int(0, 0, 5746);
            CHECK_EQUAL(42, origin->get_int(0, 1));
            CHECK_EQUAL(5746, origin->get_int(0, 0));
            CHECK_EQUAL(42, row.get_int(0));
            CHECK_EQUAL(2, origin->size());
            group->verify();
            LangBindHelper::rollback_and_continue_as_read(sg);
        }
        CHECK_EQUAL(1, origin->size());
        group->verify();
        CHECK_EQUAL(42, origin->get_int(0, 0));
        CHECK_EQUAL(42, row.get_int(0));

        {
            LangBindHelper::promote_to_write(sg);
            origin->add_empty_row();
            origin->set_int(0, 1, 42);
            LangBindHelper::commit_and_continue_as_read(sg);
        }
        Row row2 = origin->get(1);
        CHECK_EQUAL(2, origin->size());

        {
            LangBindHelper::promote_to_write(sg);
            origin->move_last_over(0);
            CHECK_EQUAL(1, origin->size());
            CHECK_EQUAL(42, row2.get_int(0));
            CHECK_EQUAL(42, origin->get_int(0, 0));
            group->verify();
            LangBindHelper::rollback_and_continue_as_read(sg);
        }
        CHECK_EQUAL(2, origin->size());
        group->verify();
        CHECK_EQUAL(42, row2.get_int(0));
        CHECK_EQUAL(42, origin->get_int(0, 1));
        sg.end_read();
    }
}
#endif

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


#ifdef LEGACY_TESTS
TEST(LangBindHelper_RollbackAndContinueAsRead_TransactLog)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));

    {
        WriteTransaction wt(sg);
        wt.add_table("table 1")->add_column(type_Int, "int");
        wt.add_table("table 2")->add_column(type_Int, "int");
        wt.commit();
    }

    Group& g = const_cast<Group&>(sg.begin_read());
    TableRef table1 = g.get_table("table 1");
    TableRef table2 = g.get_table("table 2");

    {
        // With no changes, the handler should not be called at all
        struct : NoOpTransactionLogParser {
            using NoOpTransactionLogParser::NoOpTransactionLogParser;
            void parse_complete()
            {
                CHECK(false);
            }
        } parser(test_context);
        LangBindHelper::promote_to_write(sg);
        LangBindHelper::rollback_and_continue_as_read(sg, parser);
    }

    // Make a simple modification and verify that the appropriate handler is called
    LangBindHelper::promote_to_write(sg);
    table1->add_empty_row();
    table2->add_empty_row();

    {
        struct foo : NoOpTransactionLogParser {
            using NoOpTransactionLogParser::NoOpTransactionLogParser;

            size_t expected_table = 1;

            bool erase_rows(size_t row_ndx, size_t num_rows_to_erase, size_t prior_num_rows, bool unordered)
            {
                CHECK_EQUAL(expected_table, get_current_table());
                --expected_table;

                CHECK_EQUAL(0, row_ndx);
                CHECK_EQUAL(1, num_rows_to_erase);
                CHECK_EQUAL(1, prior_num_rows);
                CHECK_NOT(unordered);

                return true;
            }
        } parser(test_context);
        LangBindHelper::rollback_and_continue_as_read(sg, parser);
        CHECK_EQUAL(0, parser.expected_table + 1);
    }

    // Add a table with some links
    LangBindHelper::promote_to_write(sg);
    table1->add_empty_row();
    table2->add_empty_row();

    TableRef link_table = g.add_table("link origin");
    link_table->add_column_link(type_Link, "link", *table1);
    link_table->add_column_link(type_LinkList, "linklist", *table2);
    link_table->add_empty_row();
    link_table->set_link(0, 0, 0);
    link_table->get_linklist(1, 0)->add(0);

    LangBindHelper::commit_and_continue_as_read(sg);

    // Verify that link nullification is rolled back appropriately
    LangBindHelper::promote_to_write(sg);
    table1->move_last_over(0);
    table2->move_last_over(0);

    {
        struct foo : NoOpTransactionLogParser {
            using NoOpTransactionLogParser::NoOpTransactionLogParser;

            size_t expected_table = 1;
            bool link_list_insert_called = false;
            bool set_link_called = false;

            bool insert_empty_rows(size_t row_ndx, size_t num_rows_to_insert, size_t prior_num_rows, bool unordered)
            {
                CHECK_EQUAL(expected_table, get_current_table());
                --expected_table;

                CHECK_EQUAL(0, row_ndx);
                CHECK_EQUAL(1, num_rows_to_insert);
                CHECK_EQUAL(0, prior_num_rows);
                CHECK(unordered);
                return true;
            }

            bool link_list_insert(size_t ndx, size_t value, size_t)
            {
                CHECK_EQUAL(2, get_current_table());
                CHECK_EQUAL(1, get_current_linkview().first);
                CHECK_EQUAL(0, get_current_linkview().second);

                CHECK_EQUAL(0, ndx);
                CHECK_EQUAL(0, value);

                link_list_insert_called = true;
                return true;
            }

            bool set_link(size_t col_ndx, size_t row_ndx, size_t value, size_t, _impl::Instruction)
            {
                CHECK_EQUAL(2, get_current_table());
                CHECK_EQUAL(0, col_ndx);
                CHECK_EQUAL(0, row_ndx);
                CHECK_EQUAL(0, value);

                set_link_called = true;
                return true;
            }
        } parser(test_context);
        LangBindHelper::rollback_and_continue_as_read(sg, parser);
        CHECK_EQUAL(0, parser.expected_table + 1);
        CHECK(parser.link_list_insert_called);
        CHECK(parser.set_link_called);
    }

    // Verify that clear() is rolled back appropriately
    LangBindHelper::promote_to_write(sg);
    table2->add_empty_row(10);

    LinkViewRef lv = link_table->get_linklist(1, 0);
    lv->clear();
    lv->add(1);
    lv->add(3);
    lv->add(5);

    LangBindHelper::commit_and_continue_as_read(sg);


    LangBindHelper::promote_to_write(sg);
    link_table->get_linklist(1, 0)->clear();

    {
        struct foo : NoOpTransactionLogParser {
            using NoOpTransactionLogParser::NoOpTransactionLogParser;

            size_t list_ndx = 0;

            bool link_list_insert(size_t ndx, size_t, size_t)
            {
                CHECK_EQUAL(2, get_current_table());
                CHECK_EQUAL(1, get_current_linkview().first);
                CHECK_EQUAL(0, get_current_linkview().second);

                CHECK_EQUAL(list_ndx, ndx);
                ++list_ndx;
                return true;
            }
        } parser(test_context);
        LangBindHelper::rollback_and_continue_as_read(sg, parser);
        CHECK_EQUAL(parser.list_ndx, 3);
    }
}
#endif

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

#ifdef LEGACY_TESTS
TEST(LangBindHelper_ImplicitTransactions_LinkList)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    Group* group = const_cast<Group*>(&sg.begin_read());
    LangBindHelper::promote_to_write(sg);
    TableRef origin = group->add_table("origin");
    TableRef target = group->add_table("target");
    origin->add_column_link(type_LinkList, "", *target);
    target->add_column(type_Int, "");
    origin->add_empty_row();
    target->add_empty_row();
    LinkViewRef link_list = origin->get_linklist(0, 0);
    link_list->add(0);
    LangBindHelper::commit_and_continue_as_read(sg);
    group->verify();
}


TEST(LangBindHelper_ImplicitTransactions_StringIndex)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    Group* group = const_cast<Group*>(&sg.begin_read());
    LangBindHelper::promote_to_write(sg);
    TableRef table = group->add_table("a");
    table->add_column(type_String, "b");
    table->add_search_index(0);
    group->verify();
    LangBindHelper::commit_and_continue_as_read(sg);
    group->verify();
}


namespace {

void multiple_trackers_writer_thread(std::string path)
{
    // Insert up to 10 rows at random positions through 10 separate
    // transactions, then quit. No waiting.
    Random random(random_int<unsigned long>());
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    for (int i = 0; i < 10; ++i) {
        WriteTransaction wt(sg);
        auto tr = wt.get_table("table");
        size_t idx = 1 + random.draw_int_mod(tr->size() - 1);

        if (tr->get_int(0, idx) == 42) {
            // do nothing
        }
        else {
            insert(tr, idx, 0);
        }
        wt.commit();
        std::this_thread::yield();
    }
}

void multiple_trackers_reader_thread(TestContext& test_context, std::string path)
{
    Random random(random_int<unsigned long>());

    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    Group& g = const_cast<Group&>(sg.begin_read());
    TableRef tr = g.get_table("table");
    Query q = tr->where().equal(0, 42);
    size_t row_ndx = q.find();
    Row row = tr->get(row_ndx);
    TableView tv = q.find_all();
    LangBindHelper::promote_to_write(sg);
    tr->set_int(0, 0, 1 + tr->get_int(0, 0));
    LangBindHelper::commit_and_continue_as_read(sg);
    for (;;) {
        int_fast64_t val = row.get_int(0);
        tv.sync_if_needed();
        if (val == 43)
            break;
        CHECK_EQUAL(42, val);
        CHECK_EQUAL(1, tv.size());
        CHECK_EQUAL(42, tv.get_int(0, 0));
        while (!sg.has_changed())
            std::this_thread::yield();
        rt->advance_read();
    }
    CHECK_EQUAL(0, tv.size());
    sg.end_read();
}

} // anonymous namespace


TEST(LangBindHelper_ImplicitTransactions_MultipleTrackers)
{
    const int write_thread_count = 7;
    const int read_thread_count = 3; // must be less than 42 for correct operation

    SHARED_GROUP_TEST_PATH(path);

    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    {
        WriteTransaction wt(sg);
        TableRef tr = wt.add_table("table");
        tr->add_column(type_Int, "first");
        tr->add_empty_row(200); // use first entry in table to count readers which have locked on
        tr->set_int(0, 100, 42);
        wt.commit();
    }
    // FIXME: Use separate arrays for reader and writer threads for safety and readability.
    Thread threads[write_thread_count + read_thread_count];
    for (int i = 0; i < write_thread_count; ++i)
        threads[i].start([&] { multiple_trackers_writer_thread(path); });
    std::this_thread::yield();
    for (int i = 0; i < read_thread_count; ++i) {
        threads[write_thread_count + i].start([&] { multiple_trackers_reader_thread(test_context, path); });
    }

    // Wait for all writer threads to complete
    for (int i = 0; i < write_thread_count; ++i)
        threads[i].join();

    // Busy-wait for all reader threads to find and lock onto value '42'
    for (;;) {
        TransactionRef rt = sg->start_read() ConstTableRef tr = rt.get_table("table");
        if (tr->get_int(0, 0) == read_thread_count)
            break;
        std::this_thread::yield();
    }
    // signal to all readers to complete
    {
        WriteTransaction wt(sg);
        TableRef tr = wt.get_table("table");
        Query q = tr->where().equal(0, 42);
        size_t idx = q.find();
        tr->set_int(0, idx, 43);
        wt.commit();
    }
    // Wait for all reader threads to complete
    for (int i = 0; i < read_thread_count; ++i)
        threads[write_thread_count + i].join();

    // cleanup
    sg.end_read(); // FIXME: What cleanup? This seems out of place!?
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
        sg.begin_read();
        LangBindHelper::promote_to_write(sg);
        LangBindHelper::commit_and_continue_as_read(sg);
        sg.end_read();
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
    const Group& group = sg.begin_read();
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
    Group& group_w = const_cast<Group&>(sg_w.begin_read());

    LangBindHelper::promote_to_write(sg_w);
    TableRef table_w = group_w.add_table("table");
    table_w->add_column(type_Int, "");
    table_w->add_empty_row();
    LangBindHelper::commit_and_continue_as_read(sg_w);
    group_w.verify();

    rt->advance_read();
    ConstTableRef table = rt->get_table("table");
    CHECK_EQUAL(0, table->get_int(0, 0));
    rt->verify();

    LangBindHelper::promote_to_write(sg_w);
    table_w->set_int(0, 0, 1);
    LangBindHelper::commit_and_continue_as_read(sg_w);
    group_w.verify();

    rt->advance_read();
    CHECK_EQUAL(1, table->get_int(0, 0));
    rt->verify();

    sg.end_read();
    sg_w.end_read();
}


TEST(LangBindHelper_ImplicitTransactions_ContinuedUseOfLinkList)
{
    SHARED_GROUP_TEST_PATH(path);

    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    const Group& group = sg.begin_read();

    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
    Group& group_w = const_cast<Group&>(sg_w.begin_read());

    LangBindHelper::promote_to_write(sg_w);
    TableRef table_w = group_w.add_table("table");
    table_w->add_column_link(type_LinkList, "", *table_w);
    table_w->add_empty_row();
    LinkViewRef link_list_w = table_w->get_linklist(0, 0);
    link_list_w->add(0);
    LangBindHelper::commit_and_continue_as_read(sg_w);
    group_w.verify();

    rt->advance_read();
    ConstTableRef table = rt->get_table("table");
    ConstLinkViewRef link_list = table->get_linklist(0, 0);
    CHECK_EQUAL(1, link_list->size());
    rt->verify();

    LangBindHelper::promote_to_write(sg_w);
    link_list_w->add(0);
    LangBindHelper::commit_and_continue_as_read(sg_w);
    group_w.verify();

    rt->advance_read();
    CHECK_EQUAL(2, link_list->size());
    rt->verify();

    sg.end_read();
    sg_w.end_read();
}
#endif


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
    auto ok = table_w->create_object().set_all(1, "2", 3).get_key();
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
#ifdef LEGACY_TESTS
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

#ifdef LEGACY_TESTS
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
    DB::VersionID m_version;
    std::unique_ptr<T> m_handover;
    bool m_has_feedback = false;
    void put(std::unique_ptr<T> h, DB::VersionID v)
    {
        LockGuard lg(m_lock);
        // std::cout << "put " << h << std::endl;
        while (m_handover != nullptr)
            m_changed.wait(lg);
        // std::cout << " -- put " << h << std::endl;
        m_handover = move(h);
        m_version = v;
        m_changed.notify_all();
    }
    void get(std::unique_ptr<T>& h, DB::VersionID& v)
    {
        LockGuard lg(m_lock);
        // std::cout << "get " << std::endl;
        while (m_handover == nullptr)
            m_changed.wait(lg);
        // std::cout << " -- get " << m_handover << std::endl;
        h = move(m_handover);
        v = m_version;
        m_handover = nullptr;
        m_changed.notify_all();
    }
    bool try_get(std::unique_ptr<T>& h, DB::VersionID& v)
    {
        LockGuard lg(m_lock);
        if (m_handover == nullptr)
            return false;
        h = move(m_handover);
        v = m_version;
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

void handover_writer(std::string path)
{
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    Group& g = const_cast<Group&>(sg.begin_read());
    auto table = g.get_table("table");
    Random random(random_int<unsigned long>());
    for (int i = 1; i < 5000; ++i) {
        LangBindHelper::promote_to_write(sg);
        // table holds random numbers >= 1, until the writing process
        // finishes, after which table[0] is set to 0 to signal termination
        add(table, 1 + random.draw_int_mod(100));
        LangBindHelper::commit_and_continue_as_read(sg);
        // improve chance of consumers running concurrently with
        // new writes:
        for (int n = 0; n < 10; ++n)
            std::this_thread::yield();
    }
    LangBindHelper::promote_to_write(sg);
    table->set_int(0, 0, 0); // <---- signals other threads to stop
    LangBindHelper::commit_and_continue_as_read(sg);
    sg.end_read();
}


void handover_querier(HandoverControl<DB::Handover<TableView>>* control, TestContext& test_context, std::string path)
{
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    // We need to ensure that the initial version observed is *before* the final
    // one written by the writer thread. We do this (simplisticly) by locking on
    // to the initial version before even starting the writer.
    Group& g = const_cast<Group&>(sg.begin_read());
    Thread writer;
    writer.start([&] { handover_writer(path); });
    TableRef table = g.get_table("table");
    TableView tv = table->where().greater(0, 50).find_all();
    for (;;) {
        // wait here for writer to change the database. Kind of wasteful, but wait_for_change()
        // is not available on osx.
        if (!sg.has_changed()) {
            std::this_thread::yield();
            continue;
        }
        rt->advance_read();
        CHECK(!tv.is_in_sync());
        tv.sync_if_needed();
        CHECK(tv.is_in_sync());
        control->put(sg.export_for_handover(tv, MutableSourcePayload::Move), sg.get_version_of_current_transaction());

        // here we need to allow the reciever to get hold on the proper version before
        // we go through the loop again and advance_read().
        control->wait_feedback();
        std::this_thread::yield();

        if (table->size() > 0 && table->get_int(0, 0) == 0)
            break;
    }
    sg.end_read();
    writer.join();
}

void handover_verifier(HandoverControl<DB::Handover<TableView>>* control, TestContext& test_context, std::string path)
{
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    for (;;) {
        std::unique_ptr<DB::Handover<TableView>> handover;
        DB::VersionID version;
        control->get(handover, version);
        CHECK_EQUAL(version.version, handover->version.version);
        CHECK(version == handover->version);
        Group& g = const_cast<Group&>(sg.begin_read(version));
        CHECK_EQUAL(version.version, sg.get_version_of_current_transaction().version);
        CHECK(version == sg.get_version_of_current_transaction());
        control->signal_feedback();
        TableRef table = g.get_table("table");
        TableView tv = table->where().greater(0, 50).find_all();
        CHECK(tv.is_in_sync());
        std::unique_ptr<TableView> tv2 = sg.import_from_handover(move(handover));
        CHECK(tv.is_in_sync());
        CHECK(tv2->is_in_sync());
        CHECK_EQUAL(tv.size(), tv2->size());
        for (size_t k = 0; k < tv.size(); ++k)
            CHECK_EQUAL(tv.get_int(0, k), tv2->get_int(0, k));
        if (table->size() > 0 && table->get_int(0, 0) == 0)
            break;
        sg.end_read();
    }
}

} // anonymous namespace
#endif

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

#ifdef LEGACY_TESTS
TEST(LangBindHelper_HandoverBetweenThreads)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    Group& g = sg.begin_write();
    auto table = g.add_table("table");
    table->add_column(type_Int, "first");
    sg.commit();
    sg.begin_read();
    table = g.get_table("table");
    CHECK(bool(table));
    sg.end_read();

    HandoverControl<DB::Handover<TableView>> control;
    Thread querier, verifier;
    querier.start([&] { handover_querier(&control, test_context, path); });
    verifier.start([&] { handover_verifier(&control, test_context, path); });
    querier.join();
    verifier.join();
}


TEST(LangBindHelper_HandoverDependentViews)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    sg.begin_read();

    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
    Group& group_w = const_cast<Group&>(sg_w.begin_read());

    DB::VersionID vid;
    {
        // Untyped interface
        std::unique_ptr<DB::Handover<TableView>> handover1;
        std::unique_ptr<DB::Handover<TableView>> handover2;
        {
            TableView tv1;
            TableView tv2;
            LangBindHelper::promote_to_write(sg_w);
            TableRef table = group_w.add_table("table2");
            table->add_column(type_Int, "first");
            for (int i = 0; i < 100; ++i) {
                table->add_empty_row();
                table->set_int(0, i, i);
            }
            LangBindHelper::commit_and_continue_as_read(sg_w);
            vid = sg_w.get_version_of_current_transaction();
            tv1 = table->where().find_all();
            tv2 = table->where(&tv1).find_all();
            CHECK(tv1.is_attached());
            CHECK(tv2.is_attached());
            CHECK_EQUAL(100, tv1.size());
            for (int i = 0; i < 100; ++i)
                CHECK_EQUAL(i, tv1.get_int(0, i));
            CHECK_EQUAL(100, tv2.size());
            for (int i = 0; i < 100; ++i)
                CHECK_EQUAL(i, tv2.get_int(0, i));
            handover2 = sg_w.export_for_handover(tv2, ConstSourcePayload::Copy);
            CHECK(tv1.is_attached());
            CHECK(tv2.is_attached());
        }
        {
            LangBindHelper::advance_read(sg, vid);
            sg_w.close();
            // importing tv:
            std::unique_ptr<TableView> tv2(sg.import_from_handover(move(handover2)));
            // CHECK(tv1.is_in_sync()); -- not possible, tv1 is now owned by tv2 and not reachable
            CHECK(tv2->is_in_sync());
            // CHECK(tv1.is_attached());
            CHECK(tv2->is_attached());
            CHECK_EQUAL(100, tv2->size());
            for (int i = 0; i < 100; ++i)
                CHECK_EQUAL(i, tv2->get_int(0, i));
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
        sg.begin_read();

        std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
        DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
        Group& group_w = const_cast<Group&>(sg_w.begin_read());
        std::unique_ptr<DB::Handover<TableView>> handover;
        DB::VersionID vid;

        {
            TableView tv;
            LangBindHelper::promote_to_write(sg_w);

            TableRef table1 = group_w.add_table("table1");
            TableRef table2 = group_w.add_table("table2");

            // add some more columns to table1 and table2
            table1->add_column(type_Int, "col1");
            table1->add_column(type_String, "str1");

            // add some rows
            table1->add_empty_row();
            table1->set_int(0, 0, 300);
            table1->set_string(1, 0, "delta");

            table1->add_empty_row();
            table1->set_int(0, 1, 100);
            table1->set_string(1, 1, "alfa");

            table1->add_empty_row();
            table1->set_int(0, 2, 200);
            table1->set_string(1, 2, "beta");

            size_t col_link2 = table2->add_column_link(type_LinkList, "linklist", *table1);

            table2->add_empty_row();
            table2->add_empty_row();

            LinkViewRef lvr;

            lvr = table2->get_linklist(col_link2, 0);
            lvr->clear();
            lvr->add(0);
            lvr->add(1);
            lvr->add(2);

            // Return all rows of table1 (the linked-to-table) that match the criteria and is in the LinkList

            // q.m_table = table1
            // q.m_view = lvr
            Query q = table1->where(lvr).and_query(table1->column<Int>(0) > 100);

            // Remove the LinkList that the query depends on, to see if a detached LinkView can be handed over
            // correctly
            if (detached == 1)
                table2->remove(0);

            // tv.m_table == table1
            tv = q.find_all(); // tv = { 0, 2 }
            CHECK(tv.is_in_sync());

            // TableView tv2 = lvr->get_sorted_view(0);
            LangBindHelper::commit_and_continue_as_read(sg_w);
            vid = sg_w.get_version_of_current_transaction();
            handover = sg_w.export_for_handover(tv, ConstSourcePayload::Copy);
        }
        {
            LangBindHelper::advance_read(sg, vid);
            sg_w.close();
            std::unique_ptr<TableView> tv(sg.import_from_handover(move(handover))); // <-- import tv

            CHECK(tv->is_in_sync());
            if (detached == 1) {
                CHECK_EQUAL(0, tv->size());
            }
            else {
                CHECK_EQUAL(2, tv->size());
                CHECK_EQUAL(0, tv->get_source_ndx(0));
                CHECK_EQUAL(2, tv->get_source_ndx(1));
            }
        }
    }
}


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

#ifdef LEGACY_TESTS
TEST(LangBindHelper_HandoverTableViewFromBacklink)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));

    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
    Group& group_w = const_cast<Group&>(sg_w.begin_read());

    LangBindHelper::promote_to_write(sg_w);

    TableRef source = group_w.add_table("source");
    source->add_column(type_Int, "int");

    TableRef links = group_w.add_table("links");
    links->add_column_link(type_Link, "link", *source);

    source->add_empty_row(100);
    links->add_empty_row(100);
    for (int i = 0; i < 100; ++i) {
        source->set_int(0, i, i);
        links->set_link(0, i, i);
    }
    LangBindHelper::commit_and_continue_as_read(sg_w);
    auto vid = sg_w.get_version_of_current_transaction();

    for (int i = 0; i < 100; ++i) {
        TableView tv = source->get_backlink_view(i, links.get(), 0);
        CHECK(tv.is_attached());
        CHECK_EQUAL(1, tv.size());
        CHECK_EQUAL(i, tv.get_link(0, 0));
        auto handover1 = sg_w.export_for_handover(tv, ConstSourcePayload::Copy);
        CHECK(tv.is_attached());

        sg.begin_read(vid);
        auto tv2 = sg.import_from_handover(std::move(handover1));
        CHECK(tv2->is_attached());
        CHECK_EQUAL(1, tv2->size());
        CHECK_EQUAL(i, tv2->get_link(0, 0));
        sg.end_read();
    }
}

// Verify that handing over an out-of-sync TableView that represents backlinks
// to a deleted row results in a TableView that can be brought back into sync.
TEST(LangBindHelper_HandoverOutOfSyncTableViewFromBacklinksToDeletedRow)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));

    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
    Group& group_w = const_cast<Group&>(sg_w.begin_read());

    LangBindHelper::promote_to_write(sg_w);

    TableRef target = group_w.add_table("target");
    target->add_column(type_Int, "int");

    TableRef links = group_w.add_table("links");
    links->add_column_link(type_Link, "link", *target);

    target->add_empty_row();
    target->set_int(0, 0, 0);

    links->add_empty_row();
    links->set_link(0, 0, 0);

    TableView tv = target->get_backlink_view(0, links.get(), 0);
    CHECK_EQUAL(true, tv.is_attached());
    CHECK_EQUAL(true, tv.is_in_sync());
    CHECK_EQUAL(false, tv.depends_on_deleted_object());
    CHECK_EQUAL(1, tv.size());

    // Bring the view out of sync, and have it depend on a deleted row.
    target->move_last_over(0);
    CHECK_EQUAL(true, tv.is_attached());
    CHECK_EQUAL(false, tv.is_in_sync());
    CHECK_EQUAL(true, tv.depends_on_deleted_object());
    CHECK_EQUAL(1, tv.size());

    LangBindHelper::commit_and_continue_as_read(sg_w);
    DB::VersionID vid = sg_w.get_version_of_current_transaction();

    auto handover = sg_w.export_for_handover(tv, ConstSourcePayload::Copy);
    CHECK(tv.is_attached());
    sg.begin_read(vid);

    // The imported TableView should have the same state as the exported one.
    auto tv2 = sg.import_from_handover(std::move(handover));
    CHECK_EQUAL(true, tv2->is_attached());
    CHECK_EQUAL(false, tv2->is_in_sync());
    CHECK_EQUAL(true, tv.depends_on_deleted_object());
    CHECK_EQUAL(1, tv2->size());

    // Syncing the TableView should bring it into sync, and cause it to reflect
    // that its source row was deleted.
    tv2->sync_if_needed();
    CHECK_EQUAL(true, tv2->is_attached());
    CHECK_EQUAL(true, tv2->is_in_sync());
    CHECK_EQUAL(true, tv.depends_on_deleted_object());
    CHECK_EQUAL(0, tv2->size());
}

// Test that we can handover a query involving links, and that after the
// handover export, the handover is completely decoupled from later changes
// done on accessors belonging to the exporting shared group
TEST(LangBindHelper_HandoverWithLinkQueries)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    sg.begin_read();

    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
    Group& group_w = const_cast<Group&>(sg_w.begin_read());

    // First setup data so that we can do a query on links
    LangBindHelper::promote_to_write(sg_w);
    TableRef table1 = group_w.add_table("table1");
    TableRef table2 = group_w.add_table("table2");
    // add some more columns to table1 and table2
    table1->add_column(type_Int, "col1");
    table1->add_column(type_String, "str1");

    table2->add_column(type_Int, "col1");
    table2->add_column(type_String, "str2");

    // add some rows
    table1->add_empty_row();
    table1->set_int(0, 0, 100);
    table1->set_string(1, 0, "foo");
    table1->add_empty_row();
    table1->set_int(0, 1, 200);
    table1->set_string(1, 1, "!");
    table1->add_empty_row();
    table1->set_int(0, 2, 300);
    table1->set_string(1, 2, "bar");

    table2->add_empty_row();
    table2->set_int(0, 0, 400);
    table2->set_string(1, 0, "hello");
    table2->add_empty_row();
    table2->set_int(0, 1, 500);
    table2->set_string(1, 1, "world");
    table2->add_empty_row();
    table2->set_int(0, 2, 600);
    table2->set_string(1, 2, "!");


    size_t col_link2 = table1->add_column_link(type_LinkList, "link", *table2);

    // set some links
    LinkViewRef links1;

    links1 = table1->get_linklist(col_link2, 0);
    links1->add(1);

    links1 = table1->get_linklist(col_link2, 1);
    links1->add(1);
    links1->add(2);
    LangBindHelper::commit_and_continue_as_read(sg_w);

    size_t match;

    std::unique_ptr<DB::Handover<Query>> handoverQuery;
    std::unique_ptr<DB::Handover<Query>> handoverQuery2;
    std::unique_ptr<DB::Handover<Query>> handoverQuery_int;


    {
        // Do a query (which will have zero results) and export it twice.
        // To test separation, we'll later modify state at the exporting side,
        // and verify that the two different imports still get identical results
        realm::Query query = table1->link(col_link2).column<String>(1) == "nabil";
        realm::TableView tv4 = query.find_all();

        handoverQuery = sg_w.export_for_handover(query, ConstSourcePayload::Copy);
        handoverQuery2 = sg_w.export_for_handover(query, ConstSourcePayload::Copy);
    }

    DB::VersionID vid = sg_w.get_version_of_current_transaction(); // vid == 2
    {
        LangBindHelper::advance_read(sg, vid);
        std::unique_ptr<Query> q(sg.import_from_handover(move(handoverQuery)));
        realm::TableView tv = q->find_all();
        match = tv.size();
        CHECK_EQUAL(0, match);
    }

    // On the exporting side, change the data such that the query will now have
    // non-zero results if evaluated in that context.
    LangBindHelper::promote_to_write(sg_w);
    table2->add_empty_row();
    table2->set_int(0, 3, 700);
    table2->set_string(1, 3, "nabil");
    links1 = table1->get_linklist(col_link2, 2);
    links1->add(3);
    LangBindHelper::commit_and_continue_as_read(sg_w);

    {
        // Import query and evaluate in the old context. This should *not* be
        // affected by the change done above on the exporting side.
        std::unique_ptr<Query> q2(sg.import_from_handover(move(handoverQuery2)));
        realm::TableView tv2 = q2->find_all();
        match = tv2.size();
        CHECK_EQUAL(0, match);
    }
}


TEST(LangBindHelper_HandoverQueryLinksTo)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    sg.begin_read();

    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
    Group& group_w = const_cast<Group&>(sg_w.begin_read());

    std::unique_ptr<DB::Handover<Query>> handoverQuery;
    std::unique_ptr<DB::Handover<Query>> handoverQueryOr;
    std::unique_ptr<DB::Handover<Query>> handoverQueryAnd;
    std::unique_ptr<DB::Handover<Query>> handoverQueryNot;
    std::unique_ptr<DB::Handover<Query>> handoverQueryAndAndOr;
    std::unique_ptr<DB::Handover<Query>> handoverQueryWithExpression;
    std::unique_ptr<DB::Handover<Query>> handoverQueryLinksToDetached;

    {
        LangBindHelper::promote_to_write(sg_w);

        TableRef source = group_w.add_table("source");
        TableRef target = group_w.add_table("target");

        size_t col_link = source->add_column_link(type_Link, "link", *target);
        size_t col_name = target->add_column(type_String, "name");

        target->add_empty_row(4);
        target->set_string(col_name, 0, "A");
        target->set_string(col_name, 1, "B");
        target->set_string(col_name, 2, "C");
        target->set_string(col_name, 3, "D");

        source->add_empty_row(3);
        source->set_link(col_link, 0, 0);
        source->set_link(col_link, 1, 1);
        source->set_link(col_link, 2, 2);

        Row detached_row = target->get(3);
        target->move_last_over(3);

        LangBindHelper::commit_and_continue_as_read(sg_w);

        Query query = source->column<Link>(col_link) == target->get(0);
        handoverQuery = sg_w.export_for_handover(query, ConstSourcePayload::Copy);

        Query queryOr =
            source->column<Link>(col_link) == target->get(0) || source->column<Link>(col_link) == target->get(1);
        handoverQueryOr = sg_w.export_for_handover(queryOr, ConstSourcePayload::Copy);

        Query queryAnd =
            source->column<Link>(col_link) == target->get(0) && source->column<Link>(col_link) == target->get(0);
        handoverQueryAnd = sg_w.export_for_handover(queryAnd, ConstSourcePayload::Copy);

        Query queryNot =
            !(source->column<Link>(col_link) == target->get(0)) && source->column<Link>(col_link) == target->get(1);
        handoverQueryNot = sg_w.export_for_handover(queryNot, ConstSourcePayload::Copy);

        Query queryAndAndOr = source->where().group().and_query(queryOr).end_group().and_query(queryAnd);
        handoverQueryAndAndOr = sg_w.export_for_handover(queryAndAndOr, ConstSourcePayload::Copy);

        Query queryWithExpression = source->column<Link>(col_link).is_not_null() && query;
        handoverQueryWithExpression = sg_w.export_for_handover(queryWithExpression, ConstSourcePayload::Copy);

        Query queryLinksToDetached = source->where().links_to(col_link, detached_row);
        handoverQueryLinksToDetached = sg_w.export_for_handover(queryLinksToDetached, ConstSourcePayload::Copy);
    }

    DB::VersionID vid = sg_w.get_version_of_current_transaction(); // vid == 2
    {
        // Import the queries into the read-only shared rt->
        LangBindHelper::advance_read(sg, vid);
        std::unique_ptr<Query> query(sg.import_from_handover(move(handoverQuery)));
        std::unique_ptr<Query> queryOr(sg.import_from_handover(move(handoverQueryOr)));
        std::unique_ptr<Query> queryAnd(sg.import_from_handover(move(handoverQueryAnd)));
        std::unique_ptr<Query> queryNot(sg.import_from_handover(move(handoverQueryNot)));
        std::unique_ptr<Query> queryAndAndOr(sg.import_from_handover(move(handoverQueryAndAndOr)));
        std::unique_ptr<Query> queryWithExpression(sg.import_from_handover(move(handoverQueryWithExpression)));
        std::unique_ptr<Query> queryLinksToDetached(sg.import_from_handover(move(handoverQueryLinksToDetached)));

        CHECK_EQUAL(1, query->count());
        CHECK_EQUAL(2, queryOr->count());
        CHECK_EQUAL(1, queryAnd->count());
        CHECK_EQUAL(1, queryNot->count());
        CHECK_EQUAL(1, queryAndAndOr->count());
        CHECK_EQUAL(1, queryWithExpression->count());
        CHECK_EQUAL(0, queryLinksToDetached->count());


        // Remove the linked-to row.
        {
            LangBindHelper::promote_to_write(sg_w);

            TableRef target = group_w.get_table("target");
            target->move_last_over(0);

            LangBindHelper::commit_and_continue_as_read(sg_w);
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
    sg.begin_read();

    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
    Group& group_w = const_cast<Group&>(sg_w.begin_read());

    std::unique_ptr<DB::Handover<Query>> handoverQuery;

    {
        LangBindHelper::promote_to_write(sg_w);

        TableRef source = group_w.add_table("source");
        TableRef target = group_w.add_table("target");

        size_t col_link = source->add_column_link(type_Link, "link", *target);
        size_t col_name = target->add_column(type_String, "name");

        target->add_empty_row(3);
        target->set_string(col_name, 0, "A");
        target->set_string(col_name, 1, "B");
        target->set_string(col_name, 2, "C");

        source->add_empty_row(3);
        source->set_link(col_link, 0, 0);
        source->set_link(col_link, 1, 1);
        source->set_link(col_link, 2, 2);

        LangBindHelper::commit_and_continue_as_read(sg_w);

        realm::Query query = source->column<Link>(col_link, target->column<String>(col_name) == "C").count() == 1;
        handoverQuery = sg_w.export_for_handover(query, ConstSourcePayload::Copy);
    }

    DB::VersionID vid = sg_w.get_version_of_current_transaction(); // vid == 2
    {
        // Import the queries into the read-only shared rt->
        LangBindHelper::advance_read(sg, vid);
        std::unique_ptr<Query> query(sg.import_from_handover(move(handoverQuery)));

        CHECK_EQUAL(1, query->count());

        // Remove the linked-to row.
        {
            LangBindHelper::promote_to_write(sg_w);

            TableRef target = group_w.get_table("target");
            target->move_last_over(2);

            LangBindHelper::commit_and_continue_as_read(sg_w);
        }

        // Verify that the queries against the read-only shared group gives the same results.
        CHECK_EQUAL(1, query->count());
    }
}
#endif

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

#ifdef LEGACY_TESTS
TEST(LangBindHelper_TableViewAggregateAfterAdvanceRead)
{
    SHARED_GROUP_TEST_PATH(path);

    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
    {
        WriteTransaction w(sg_w);
        TableRef table = w.add_table("test");
        table->add_column(type_Double, "double");
        table->add_empty_row(3);
        table->set_double(0, 0, 1234);
        table->set_double(0, 1, -5678);
        table->set_double(0, 2, 1000);
        w.commit();
    }

    std::unique_ptr<Replication> hist_r(make_in_realm_history(path));
    DB sg_r(*hist_r, DBOptions(crypt_key()));
    ReadTransaction r(sg_r);
    ConstTableRef table_r = r.get_table("test");

    // Create a table view with all refs detached.
    TableView view = table_r->where().find_all();
    {
        WriteTransaction w(sg_w);
        w.get_table("test")->clear();
        w.commit();
    }
    LangBindHelper::advance_read(sg_r);

    // Verify that an aggregate on the view with detached refs gives the expected result.
    CHECK_EQUAL(false, view.is_in_sync());
    size_t ndx = not_found;
    double min = view.minimum_double(0, &ndx);
    CHECK_EQUAL(0, min);
    CHECK_EQUAL(not_found, ndx);

    // Sync the view to discard the detached refs.
    view.sync_if_needed();

    // Verify that an aggregate on the view still gives the expected result.
    ndx = not_found;
    min = view.minimum_double(0, &ndx);
    CHECK_EQUAL(0, min);
    CHECK_EQUAL(not_found, ndx);
}


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


// TableView::clear() was originally reported to be slow when table was indexed and had links, but performance
// has now doubled. This test is just a short sanity test that clear() still works.
TEST(LangBindHelper_TableViewClear)
{
    SHARED_GROUP_TEST_PATH(path);

    size_t number_of_history = 1000;
    size_t number_of_line = 18;

    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
    Group& group_w = const_cast<Group&>(sg_w.begin_read());

    // set up tables:
    // history : ["id" (int), "parent" (int), "lines" (list(line))]
    // line    : ["id" (int), "parent" (int)]
    {
        LangBindHelper::promote_to_write(sg_w);
        TableRef history = group_w.add_table("history");
        TableRef line = group_w.add_table("line");

        history->add_column(type_Int, "id");
        history->add_column(type_Int, "parent");
        history->add_column_link(type_LinkList, "lines", *line);
        history->add_search_index(1);

        line->add_column(type_Int, "id");
        line->add_column(type_Int, "parent");
        line->add_search_index(1);

        LangBindHelper::commit_and_continue_as_read(sg_w);
    }

    {
        LangBindHelper::promote_to_write(sg_w);

        TableRef history = group_w.get_table("history");
        TableRef line = group_w.get_table("line");

        history->add_empty_row();
        history->set_int(0, 0, 1);
        LinkViewRef ll = history->get_linklist(2, 0);
        for (size_t j = 0; j < number_of_line; ++j) {
            size_t r = line->add_empty_row();
            line->set_int(0, r, j + 1);
            ll->add(r);
        }

        for (size_t i = 1; i < number_of_history; ++i) {
            size_t ri = history->add_empty_row();
            history->set_int(0, ri, i + 1);
            history->set_int(1, ri, 1);
            for (size_t j = 1; j <= number_of_line; ++j) {
                size_t rj = line->add_empty_row();
                line->set_int(0, rj, rj + 1);
                line->set_int(1, rj, j);
            }
        }

        LangBindHelper::commit_and_continue_as_read(sg_w);

        CHECK_EQUAL(number_of_history, history->size());
        CHECK_EQUAL(number_of_history * number_of_line, line->size());
    }

    // query and delete
    {
        LangBindHelper::promote_to_write(sg_w);

        TableRef history = group_w.get_table("history");
        TableRef line = group_w.get_table("line");

        //    number_of_line = 2;
        for (size_t i = 1; i <= number_of_line; ++i) {
            TableView tv = (line->column<Int>(1) == int64_t(i)).find_all();
            tv.clear(RemoveMode::unordered);
        }
        LangBindHelper::commit_and_continue_as_read(sg_w);
    }

    {
        TableRef history = group_w.get_table("history");
        TableRef line = group_w.get_table("line");

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
        CHECK_LOGIC_ERROR(DB(*hist, DBOptions(crypt_key())), LogicError::mixed_history_type);
    }
}

TEST(LangBindHelper_InRealmHistory_Basics)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist = make_in_realm_history(path);
    std::unique_ptr<Replication> hist_w = make_in_realm_history(path);
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));

    // Start a read transaction (to be repeatedly advanced)
    TransactionRef rt = sg->start_read() const Group& group = rt;
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
    {
        WriteTransaction wt(sg_w);
        TableRef foo_w = wt.add_table("foo");
        foo_w->add_column(type_Int, "i");
        foo_w->add_empty_row();
        wt.commit();
    }

    rt->advance_read();
    rt->verify();
    CHECK_EQUAL(1, rt->size());
    ConstTableRef foo = rt->get_table("foo");
    CHECK_EQUAL(1, foo->get_column_count());
    CHECK_EQUAL(type_Int, foo->get_column_type(0));
    CHECK_EQUAL(1, foo->size());
    CHECK_EQUAL(0, foo->get_int(0, 0));
    uint_fast64_t version = foo->get_version_counter();

    // Modify the table via the other SharedGroup
    {
        WriteTransaction wt(sg_w);
        TableRef foo_w = wt.get_table("foo");
        foo_w->add_column(type_String, "s");
        foo_w->add_empty_row();
        foo_w->set_int(0, 0, 1);
        foo_w->set_int(0, 1, 2);
        foo_w->set_string(1, 0, "a");
        foo_w->set_string(1, 1, "b");
        wt.commit();
    }
    rt->advance_read();
    CHECK(version != foo->get_version_counter());
    rt->verify();
    CHECK_EQUAL(2, foo->get_column_count());
    CHECK_EQUAL(type_Int, foo->get_column_type(0));
    CHECK_EQUAL(type_String, foo->get_column_type(1));
    CHECK_EQUAL(2, foo->size());
    CHECK_EQUAL(1, foo->get_int(0, 0));
    CHECK_EQUAL(2, foo->get_int(0, 1));
    CHECK_EQUAL("a", foo->get_string(1, 0));
    CHECK_EQUAL("b", foo->get_string(1, 1));
    CHECK_EQUAL(foo, rt->get_table("foo"));

    // Again, with no change
    rt->advance_read();
    rt->verify();
    CHECK_EQUAL(2, foo->get_column_count());
    CHECK_EQUAL(type_Int, foo->get_column_type(0));
    CHECK_EQUAL(type_String, foo->get_column_type(1));
    CHECK_EQUAL(2, foo->size());
    CHECK_EQUAL(1, foo->get_int(0, 0));
    CHECK_EQUAL(2, foo->get_int(0, 1));
    CHECK_EQUAL("a", foo->get_string(1, 0));
    CHECK_EQUAL("b", foo->get_string(1, 1));
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
    CHECK_EQUAL(type_Int, foo->get_column_type(0));
    CHECK_EQUAL(type_String, foo->get_column_type(1));
    CHECK_EQUAL(2, foo->size());
    CHECK_EQUAL(1, foo->get_int(0, 0));
    CHECK_EQUAL(2, foo->get_int(0, 1));
    CHECK_EQUAL("a", foo->get_string(1, 0));
    CHECK_EQUAL("b", foo->get_string(1, 1));
    CHECK_EQUAL(foo, rt->get_table("foo"));
    ConstTableRef bar = rt->get_table("bar");
    CHECK_EQUAL(3, bar->get_column_count());
    CHECK_EQUAL(type_Int, bar->get_column_type(0));
    CHECK_EQUAL(type_Float, bar->get_column_type(1));
    CHECK_EQUAL(type_Double, bar->get_column_type(2));

    // Clear tables
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
    CHECK(foo->is_attached());
    CHECK_EQUAL(2, foo->get_column_count());
    CHECK_EQUAL(type_Int, foo->get_column_type(0));
    CHECK_EQUAL(type_String, foo->get_column_type(1));
    CHECK_EQUAL(0, foo->size());
    CHECK(bar->is_attached());
    CHECK_EQUAL(3, bar->get_column_count());
    CHECK_EQUAL(type_Int, bar->get_column_type(0));
    CHECK_EQUAL(type_Float, bar->get_column_type(1));
    CHECK_EQUAL(type_Double, bar->get_column_type(2));
    CHECK_EQUAL(0, bar->size());
    CHECK_EQUAL(foo, rt->get_table("foo"));
    CHECK_EQUAL(bar, rt->get_table("bar"));
}


TEST(LangBindHelper_AdvanceReadTransact_BigCommit)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist = make_in_realm_history(path);
    std::unique_ptr<Replication> hist_w = make_in_realm_history(path);
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));

    TransactionRef rt = sg->start_read() const Group& group = rt;
    CHECK_EQUAL(0, rt->size());

    {
        WriteTransaction wt(sg_w);
        TableRef foo_w = wt.add_table("foo");
        foo_w->add_column(type_Binary, "bin");
        wt.commit();
    }

    rt->advance_read();
    auto foo_table = rt->get_table("foo");

    CHECK_EQUAL(foo_table->size(), 0);
    {
        WriteTransaction wt(sg_w);
        TableRef foo_w = wt.get_table("foo");
        foo_w->add_empty_row(20);
        std::vector<char> big_binary(1024 * 1024); // 1 M
        for (unsigned i = 0; i < 20; i++) {
            foo_w->set_binary(0, i, BinaryData(big_binary.data(), big_binary.size()));
        }
        // this will result in a change set of around 20 M
        wt.commit();
    }

    rt->advance_read();
    CHECK_EQUAL(foo_table->size(), 20);
}


TEST(LangBindHelper_InRealmHistory_RollbackAndContinueAsRead)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    {
        Group* group = const_cast<Group*>(&sg.begin_read());
        {
            LangBindHelper::promote_to_write(sg);
            TableRef origin = group->get_or_add_table("origin");
            origin->add_column(type_Int, "");
            origin->add_empty_row();
            origin->set_int(0, 0, 42);
            LangBindHelper::commit_and_continue_as_read(sg);
        }
        group->verify();
        {
            // rollback of group level table insertion
            LangBindHelper::promote_to_write(sg);
            TableRef o = group->get_or_add_table("nullermand");
            TableRef o2 = group->get_table("nullermand");
            REALM_ASSERT(o2);
            LangBindHelper::rollback_and_continue_as_read(sg);
            TableRef o3 = group->get_table("nullermand");
            REALM_ASSERT(!o3);
            REALM_ASSERT(o2->is_attached() == false);
        }

        TableRef origin = group->get_table("origin");
        Row row = origin->get(0);
        CHECK_EQUAL(42, origin->get_int(0, 0));

        {
            LangBindHelper::promote_to_write(sg);
            origin->insert_empty_row(0);
            origin->set_int(0, 0, 5746);
            CHECK_EQUAL(42, origin->get_int(0, 1));
            CHECK_EQUAL(5746, origin->get_int(0, 0));
            CHECK_EQUAL(42, row.get_int(0));
            CHECK_EQUAL(2, origin->size());
            group->verify();
            LangBindHelper::rollback_and_continue_as_read(sg);
        }
        CHECK_EQUAL(1, origin->size());
        group->verify();
        CHECK_EQUAL(42, origin->get_int(0, 0));
        CHECK_EQUAL(42, row.get_int(0));

        {
            LangBindHelper::promote_to_write(sg);
            origin->add_empty_row();
            origin->set_int(0, 1, 42);
            LangBindHelper::commit_and_continue_as_read(sg);
        }
        Row row2 = origin->get(1);
        CHECK_EQUAL(2, origin->size());

        {
            LangBindHelper::promote_to_write(sg);
            origin->move_last_over(0);
            CHECK_EQUAL(1, origin->size());
            CHECK_EQUAL(42, row2.get_int(0));
            CHECK_EQUAL(42, origin->get_int(0, 0));
            group->verify();
            LangBindHelper::rollback_and_continue_as_read(sg);
        }
        CHECK_EQUAL(2, origin->size());
        group->verify();
        CHECK_EQUAL(42, row2.get_int(0));
        CHECK_EQUAL(42, origin->get_int(0, 1));
        sg.end_read();
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
        CHECK_THROW(DB(path, false, DBOptions(crypt_key())), IncompatibleHistories);
    }
}


TEST(LangBindHelper_InRealmHistory_SessionConsistency)
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

        // In-Realm history
        std::unique_ptr<Replication> hist = make_in_realm_history(path);
        CHECK_LOGIC_ERROR(DB(*hist, DBOptions(crypt_key())), LogicError::mixed_history_type);
    }
}


// Check that rollback of a transaction which deletes a table
// containing a link will insert the associated backlink into
// the correct index in the associated (linked) table. In this
// case, backlink columns should not be appended (rather they
// should be inserted into the previously used index).
TEST(LangBindHelper_RollBackAfterRemovalOfTable)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(realm::make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist, DBOptions(crypt_key()));
    Group& group_w = const_cast<Group&>(sg_w.begin_read());

    LangBindHelper::promote_to_write(sg_w);

    TableRef source_a = group_w.add_table("source_a");
    TableRef source_b = group_w.add_table("source_b");
    TableRef target_b = group_w.add_table("target_b");

    source_a->add_column_link(type_LinkList, "b", *target_b);
    source_b->add_column_link(type_LinkList, "b", *target_b);

    LangBindHelper::commit_and_continue_as_read(sg_w);

    {
        LangBindHelper::promote_to_write(sg_w);

        group_w.remove_table("source_a");
        LangBindHelper::rollback_and_continue_as_read(sg_w);
    }
    using tf = _impl::TableFriend;
    CHECK_EQUAL(group_w.size(), 3);
    CHECK_EQUAL(group_w.get_table_name(0), StringData("source_a"));
    CHECK_EQUAL(group_w.get_table(0)->get_column_count(), 1);
    CHECK_EQUAL(group_w.get_table(0)->get_link_target(0), target_b);
    CHECK_EQUAL(group_w.get_table(1)->get_link_target(0), target_b);
    // backlink column index in target_b from source_a should be index 0
    CHECK_EQUAL(tf::get_spec(*target_b).find_backlink_column(0, 0), 0);
    // backlink column index in target_b from source_b should be index 1
    CHECK_EQUAL(tf::get_spec(*target_b).find_backlink_column(1, 0), 1);
}


// Trigger erase_rows with num_rows == 0 by inserting zero rows
// and then rolling back the transaction. There was a problem
// where accessors were not updated correctly in this case because
// of an early out when num_rows_to_erase is zero.
TEST(LangBindHelper_RollbackInsertZeroRows)
{
    SHARED_GROUP_TEST_PATH(shared_path)
    std::unique_ptr<Replication> hist_w(make_in_realm_history(shared_path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
    Group& g = const_cast<Group&>(sg_w.begin_read());
    LangBindHelper::promote_to_write(sg_w);

    g.add_table("t0");
    g.insert_table(1, "t1");

    g.get_table(0)->add_column_link(type_Link, "t0_link_to_t1", *g.get_table(1));
    g.get_table(0)->add_empty_row(2);
    g.get_table(1)->add_empty_row(2);
    g.get_table(0)->set_link(0, 1, 1);

    CHECK_EQUAL(g.get_table(0)->size(), 2);
    CHECK_EQUAL(g.get_table(1)->size(), 2);
    CHECK_EQUAL(g.get_table(0)->get_link(0, 1), 1);

    LangBindHelper::commit_and_continue_as_read(sg_w);
    LangBindHelper::promote_to_write(sg_w);

    g.get_table(1)->insert_empty_row(1, 0); // Insert zero rows

    CHECK_EQUAL(g.get_table(0)->size(), 2);
    CHECK_EQUAL(g.get_table(1)->size(), 2);
    CHECK_EQUAL(g.get_table(0)->get_link(0, 1), 1);

    LangBindHelper::rollback_and_continue_as_read(sg_w);
    g.verify();

    CHECK_EQUAL(g.get_table(0)->size(), 2);
    CHECK_EQUAL(g.get_table(1)->size(), 2);
    CHECK_EQUAL(g.get_table(0)->get_link(0, 1), 1);
}


TEST(LangBindHelper_IsRowAttachedAfterClear)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_r(make_in_realm_history(path));
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DB sg_r(*hist_r, DBOptions(crypt_key()));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
    Group& g = const_cast<Group&>(sg_w.begin_write());
    Group& g_r = const_cast<Group&>(sg_r.begin_read());

    TableRef t = g.add_table("t");
    TableRef t2 = g.add_table("t2");
    size_t col_id = t->add_column(type_Int, "id");
    size_t link_col_id = t2->add_column_link(type_Link, "link", *t);

    t->add_empty_row(2);
    t->set_int(col_id, 0, 0);
    t->set_int(col_id, 1, 1);
    t2->add_empty_row(2);
    t2->set_link(link_col_id, 0, 0);
    t2->set_link(link_col_id, 1, 1);

    LangBindHelper::commit_and_continue_as_read(sg_w);
    g.verify();
    LangBindHelper::advance_read(sg_r);
    g_r.verify();

    TableView tv = t->where().find_all();
    TableView tv_r = g_r.get_table(0)->where().find_all();
    TableView tv_r2 = g_r.get_table(1)->where().find_all();

    CHECK_EQUAL(2, tv.size());
    CHECK(tv.is_row_attached(0));
    CHECK(tv.is_row_attached(1));
    CHECK_EQUAL(2, tv_r.size());
    CHECK(tv_r.is_row_attached(0));
    CHECK(tv_r.is_row_attached(1));
    CHECK_EQUAL(tv_r2.get_link(link_col_id, 0), 0);
    CHECK_EQUAL(tv_r2.get_link(link_col_id, 1), 1);

    LangBindHelper::promote_to_write(sg_w);
    t->move_last_over(1);
    LangBindHelper::commit_and_continue_as_read(sg_w);
    g.verify();
    LangBindHelper::advance_read(sg_r);
    g_r.verify();

    CHECK_EQUAL(2, tv.size());
    CHECK(tv.is_row_attached(0));
    CHECK(!tv.is_row_attached(1));
    CHECK_EQUAL(2, tv_r.size());
    CHECK(tv_r.is_row_attached(0));
    CHECK(!tv_r.is_row_attached(1));
    CHECK_EQUAL(tv_r2.get_link(link_col_id, 0), 0);
    CHECK_EQUAL(tv_r2.get_link(link_col_id, 1), realm::npos);

    LangBindHelper::promote_to_write(sg_w);
    t->clear();
    LangBindHelper::commit_and_continue_as_read(sg_w);
    g.verify();
    LangBindHelper::advance_read(sg_r);
    g_r.verify();

    CHECK_EQUAL(2, tv.size());
    CHECK(!tv.is_row_attached(0));
    CHECK(!tv.is_row_attached(1));
    CHECK_EQUAL(2, tv_r.size());
    CHECK(!tv_r.is_row_attached(0));
    CHECK(!tv_r.is_row_attached(1));
    CHECK_EQUAL(tv_r2.get_link(link_col_id, 0), realm::npos);
    CHECK_EQUAL(tv_r2.get_link(link_col_id, 1), realm::npos);
}



TEST(LangBindHelper_RollbackRemoveZeroRows)
{
    SHARED_GROUP_TEST_PATH(path)
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
    Group& g = const_cast<Group&>(sg_w.begin_read());
    LangBindHelper::promote_to_write(sg_w);

    g.add_table("t0");
    g.insert_table(1, "t1");

    g.get_table(0)->add_column_link(type_Link, "t0_link_to_t1", *g.get_table(1));
    g.get_table(0)->add_empty_row(2);
    g.get_table(1)->add_empty_row(2);
    g.get_table(0)->set_link(0, 1, 1);

    CHECK_EQUAL(g.get_table(0)->size(), 2);
    CHECK_EQUAL(g.get_table(1)->size(), 2);
    CHECK_EQUAL(g.get_table(0)->get_link(0, 1), 1);

    LangBindHelper::commit_and_continue_as_read(sg_w);
    LangBindHelper::promote_to_write(sg_w);

    g.get_table(1)->clear();

    CHECK_EQUAL(g.get_table(0)->size(), 2);
    CHECK_EQUAL(g.get_table(1)->size(), 0);
    CHECK_EQUAL(g.get_table(0)->get_link(0, 0), realm::npos);

    LangBindHelper::rollback_and_continue_as_read(sg_w);
    g.verify();

    CHECK_EQUAL(g.get_table(0)->size(), 2);
    CHECK_EQUAL(g.get_table(1)->size(), 2);
    CHECK_EQUAL(g.get_table(0)->get_link(0, 1), 1);
}


// Bug found by AFL during development of TimestampColumn
TEST_TYPES(LangBindHelper_AddEmptyRowsAndRollBackTimestamp, std::true_type, std::false_type)
{
    constexpr bool nullable_toggle = TEST_TYPE::value;
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
    Group& g = const_cast<Group&>(sg_w.begin_read());
    LangBindHelper::promote_to_write(sg_w);
    TableRef t = g.insert_table(0, "");
    t->insert_column(0, type_Int, "", nullable_toggle);
    t->insert_column(1, type_Timestamp, "", nullable_toggle);
    LangBindHelper::commit_and_continue_as_read(sg_w);
    LangBindHelper::promote_to_write(sg_w);
    t->insert_empty_row(0, 224);
    LangBindHelper::rollback_and_continue_as_read(sg_w);
    g.verify();
}


// Another bug found by AFL during development of TimestampColumn
TEST_TYPES(LangBindHelper_EmptyWrites, std::true_type, std::false_type)
{
    constexpr bool nullable_toggle = TEST_TYPE::value;
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
    Group& g = const_cast<Group&>(sg_w.begin_read());
    LangBindHelper::promote_to_write(sg_w);

    TableRef t = g.add_table("");
    t->add_column(type_Timestamp, "", nullable_toggle);

    for (int i = 0; i < 27; ++i) {
        LangBindHelper::commit_and_continue_as_read(sg_w);
        LangBindHelper::promote_to_write(sg_w);
    }

    t->insert_empty_row(0, 1);
}


// Found by AFL
TEST_TYPES(LangBindHelper_SetTimestampRollback, std::true_type, std::false_type)
{
    constexpr bool nullable_toggle = TEST_TYPE::value;
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
    Group& g = const_cast<Group&>(sg_w.begin_write());

    TableRef t = g.add_table("");
    t->add_column(type_Timestamp, "", nullable_toggle);
    t->add_empty_row();
    t->set_timestamp(0, 0, Timestamp(-1, -1));
    LangBindHelper::rollback_and_continue_as_read(sg_w);
    g.verify();
}


// Found by AFL, probably related to the rollback version above
TEST_TYPES(LangBindHelper_SetTimestampAdvanceRead, std::true_type, std::false_type)
{
    constexpr bool nullable_toggle = TEST_TYPE::value;
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_r(make_in_realm_history(path));
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DB sg_r(*hist_r, DBOptions(crypt_key()));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
    Group& g = const_cast<Group&>(sg_w.begin_write());
    Group& g_r = const_cast<Group&>(sg_r.begin_read());

    TableRef t = g.add_table("");
    t->insert_column(0, type_Timestamp, "", nullable_toggle);
    t->add_empty_row();
    t->set_timestamp(0, 0, Timestamp(-1, -1));
    LangBindHelper::commit_and_continue_as_read(sg_w);
    g.verify();
    LangBindHelper::advance_read(sg_r);
    g_r.verify();
}


// Found by AFL.
TEST(LangbindHelper_BoolSearchIndexCommitPromote)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_r(make_in_realm_history(path));
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DB sg_r(*hist_r, DBOptions(crypt_key()));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
    Group& g = const_cast<Group&>(sg_w.begin_write());

    TableRef t = g.add_table("");
    t->insert_column(0, type_Bool, "", true);
    t->add_empty_row(5);
    t->set_bool(0, 0, false);
    t->add_search_index(0);
    LangBindHelper::commit_and_continue_as_read(sg_w);
    LangBindHelper::promote_to_write(sg_w);
    t->add_empty_row(5);
    t->remove(8);
}


TEST(LangbindHelper_GetDataTypeName)
{
    CHECK_EQUAL(0, strcmp(LangBindHelper::get_data_type_name(type_Int), "int"));
    CHECK_EQUAL(0, strcmp(LangBindHelper::get_data_type_name(type_Bool), "bool"));
    CHECK_EQUAL(0, strcmp(LangBindHelper::get_data_type_name(type_Float), "float"));
    CHECK_EQUAL(0, strcmp(LangBindHelper::get_data_type_name(type_Double), "double"));
    CHECK_EQUAL(0, strcmp(LangBindHelper::get_data_type_name(type_String), "string"));
    CHECK_EQUAL(0, strcmp(LangBindHelper::get_data_type_name(type_Binary), "binary"));
    CHECK_EQUAL(0, strcmp(LangBindHelper::get_data_type_name(type_OldDateTime), "date"));
    CHECK_EQUAL(0, strcmp(LangBindHelper::get_data_type_name(type_Timestamp), "timestamp"));
    CHECK_EQUAL(0, strcmp(LangBindHelper::get_data_type_name(type_Table), "table"));
    CHECK_EQUAL(0, strcmp(LangBindHelper::get_data_type_name(type_Mixed), "mixed"));
    CHECK_EQUAL(0, strcmp(LangBindHelper::get_data_type_name(type_Link), "link"));
    CHECK_EQUAL(0, strcmp(LangBindHelper::get_data_type_name(type_LinkList), "linklist"));
    CHECK_EQUAL(0, strcmp(LangBindHelper::get_data_type_name(static_cast<DataType>(42)), "unknown"));
}


// Found by AFL.
TEST(LangbindHelper_GroupWriter_EdgeCaseAssert)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_r(make_in_realm_history(path));
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DB sg_r(*hist_r, DBOptions(crypt_key()));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
    Group& g = const_cast<Group&>(sg_w.begin_write());
    Group& g_r = const_cast<Group&>(sg_r.begin_read());

    g.add_table("dgrpnpgmjbchktdgagmqlihjckcdhpjccsjhnqlcjnbterse");
    g.add_table("pknglaqnckqbffehqfgjnrepcfohoedkhiqsiedlotmaqitm");
    g.get_table(0)->add_column(type_Double, "ggotpkoshbrcrmmqbagbfjetajlrrlbpjhhqrngfgdteilmj", true);
    g.get_table(1)->add_column_link(type_LinkList, "dtkiipajqdsfglbptieibknaoeeohqdlhftqmlriphobspjr",
                                    *g.get_table(0));
    g.get_table(0)->add_empty_row(375);
    g.add_table("pnsidlijqeddnsgaesiijrrqedkdktmfekftogjccerhpeil");
    sg_r.close();
    sg_w.commit();
    REALM_ASSERT_RELEASE(sg_w.compact());
    sg_w.begin_write();
    sg_r.open(path, true, DBOptions(crypt_key()));
    sg_r.begin_read();
    g_r.verify();
    g.add_table("citdgiaclkfbbksfaqegcfiqcserceaqmttkilnlbknoadtb");
    g.add_table("tqtnnikpggeakeqcqhfqtshmimtjqkchgbnmbpttbetlahfi");
    g.add_table("hkesaecjqbkemmmkffctacsnskekjbtqmpoetjnqkpactenf");
    sg_r.close();
    sg_w.commit();
}

// Found by AFL
TEST(LangBindHelper_SwapSimple)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist_r(make_in_realm_history(path));
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DB sg_r(*hist_r, DBOptions(crypt_key()));
    DBRef sg_w = DB::create(*hist_w, DBOptions(crypt_key()));
    Group& g = const_cast<Group&>(sg_w.begin_write());
    Group& g_r = const_cast<Group&>(sg_r.begin_read());

    TableRef t = g.add_table("t0");
    t->add_column(type_Int, "t_int");
    t->add_column_link(type_Link, "t_link", *t);
    const size_t num_rows = 10;
    t->add_empty_row(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        t->set_int(0, i, i);
    }
    LangBindHelper::advance_read(sg_r);
    g_r.verify();
    LangBindHelper::commit_and_continue_as_read(sg_w);
    g.verify();
    LangBindHelper::promote_to_write(sg_w);
    g.verify();
    for (size_t i = 0; i < num_rows; ++i) {
        CHECK_EQUAL(t->get_int(0, i), i);
    }
    t->swap_rows(7, 4);
    CHECK_EQUAL(t->get_int(0, 4), 7);
    CHECK_EQUAL(t->get_int(0, 7), 4);
    g.remove_table(0);

    LangBindHelper::rollback_and_continue_as_read(sg_w);

    LangBindHelper::advance_read(sg_r);
    g_r.verify();

    TableRef tw = g.get_table(0);
    TableRef tr = g_r.get_table(0);

    CHECK_EQUAL(tw->get_int(0, 4), 4);
    CHECK_EQUAL(tw->get_int(0, 7), 7);
    CHECK_EQUAL(tr->get_int(0, 4), 4);
    CHECK_EQUAL(tr->get_int(0, 7), 7);

    LangBindHelper::promote_to_write(sg_w);
    tw->swap_rows(7, 4);
    LangBindHelper::commit_and_continue_as_read(sg_w);
    LangBindHelper::advance_read(sg_r);
    g_r.verify();

    CHECK_EQUAL(tw->get_int(0, 4), 7);
    CHECK_EQUAL(tw->get_int(0, 7), 4);
    CHECK_EQUAL(tr->get_int(0, 4), 7);
    CHECK_EQUAL(tr->get_int(0, 7), 4);
}


TEST(LangBindHelper_Bug2321)
{
    SHARED_GROUP_TEST_PATH(path);
    ShortCircuitHistory hist(path);
    DB sg_r(hist, DBOptions(crypt_key()));
    DBRef sg_w = DB::create(hist, DBOptions(crypt_key()));
    int i;

    {
        WriteTransaction wt(sg_w);
        Group& group = wt.get_group();
        TableRef target = rt->add_table("target");
        target->add_column(type_Int, "data");
        target->add_empty_row(REALM_MAX_BPNODE_SIZE + 2);
        TableRef origin = rt->add_table("origin");
        origin->add_column_link(type_LinkList, "_link", *target);
        origin->add_empty_row(2);
        wt.commit();
    }

    {
        WriteTransaction wt(sg_w);
        Group& group = wt.get_group();
        TableRef origin = rt->get_table("origin");
        LinkViewRef lv0 = origin->get_linklist(0, 0);
        for (i = 0; i < (REALM_MAX_BPNODE_SIZE - 1); i++) {
            lv0->add(i);
        }
        wt.commit();
    }

    ReadTransactionRef rt(sg_r);
    ConstTableRef origin_read = rt.get_group().get_table("origin");
    ConstLinkViewRef lv1 = origin_read->get_linklist(0, 0);

    {
        WriteTransaction wt(sg_w);
        Group& group = wt.get_group();
        TableRef origin = rt->get_table("origin");
        LinkViewRef lv0 = origin->get_linklist(0, 0);
        lv0->add(i++);
        lv0->add(i++);
        wt.commit();
    }

    // If MAX_BPNODE_SIZE is 4 and we run in debug mode, then the LinkView
    // accessor was not refreshed correctly. It would still be a leaf class,
    // but the header flags would tell it is a node.
    LangBindHelper::advance_read(sg_r);
    CHECK_EQUAL(lv1->size(), i);
}

TEST(LangBindHelper_Bug2295)
{
    SHARED_GROUP_TEST_PATH(path);
    ShortCircuitHistory hist(path);
    DBRef sg_w = DB::create(hist);
    DB sg_r(hist);
    int i;

    {
        WriteTransaction wt(sg_w);
        Group& group = wt.get_group();
        TableRef target = rt->add_table("target");
        target->add_column(type_Int, "data");
        target->add_empty_row(REALM_MAX_BPNODE_SIZE + 2);
        TableRef origin = rt->add_table("origin");
        origin->add_column_link(type_LinkList, "_link", *target);
        origin->add_empty_row(2);
        wt.commit();
    }

    {
        WriteTransaction wt(sg_w);
        Group& group = wt.get_group();
        TableRef origin = rt->get_table("origin");
        LinkViewRef lv0 = origin->get_linklist(0, 0);
        for (i = 0; i < (REALM_MAX_BPNODE_SIZE + 1); i++) {
            lv0->add(i);
        }
        wt.commit();
    }

    ReadTransactionRef rt(sg_r);
    ConstTableRef origin_read = rt.get_group().get_table("origin");
    ConstLinkViewRef lv1 = origin_read->get_linklist(0, 0);

    CHECK_EQUAL(lv1->size(), i);

    {
        WriteTransaction wt(sg_w);
        Group& group = wt.get_group();
        TableRef origin = rt->get_table("origin");
        // With the error present, this will cause some areas to be freed
        // that has already been freed in the above transaction
        LinkViewRef lv0 = origin->get_linklist(0, 0);
        lv0->add(i++);
        wt.commit();
    }

    LangBindHelper::promote_to_write(sg_r);
    // Here we write the duplicates to the free list
    LangBindHelper::commit_and_continue_as_read(sg_r);
    rt.get_group().verify();

    CHECK_EQUAL(lv1->size(), i);
}

TEST(LangBindHelper_BigBinary)
{
    SHARED_GROUP_TEST_PATH(path);
    ShortCircuitHistory hist(path);
    DBRef sg_w = DB::create(hist);
    DB sg_r(hist);
    std::string big_data(0x1000000, 'x');

    ReadTransactionRef rt(sg_r);
    {
        std::string data(16777362, 'y');
        WriteTransaction wt(sg_w);
        Group& group = wt.get_group();
        TableRef target = rt->add_table("big");
        target->add_column(type_Binary, "data");
        target->add_empty_row();
        target->set_binary_big(0, 0, BinaryData(data.data(), 16777362));
        wt.commit();
    }

    LangBindHelper::advance_read(sg_r);
    {
        WriteTransaction wt(sg_w);
        Group& group = wt.get_group();
        TableRef target = rt->get_table("big");
        target->set_binary_big(0, 0, BinaryData(big_data.data(), 0x1000000));
        rt->verify();
        wt.commit();
    }
    LangBindHelper::advance_read(sg_r);
    const Group& g = rt.get_group();
    auto t = g.get_table("big");
    size_t pos = 0;
    BinaryData bin = t->get_binary_at(0, 0, pos);
    CHECK_EQUAL(memcmp(big_data.data(), bin.data(), bin.size()), 0);
}

TEST(LangBindHelper_CopyOnWriteOverflow)
{
    SHARED_GROUP_TEST_PATH(path);
    ShortCircuitHistory hist(path);
    DBRef sg_w = DB::create(hist);

    {
        WriteTransaction wt(sg_w);
        Group& group = wt.get_group();
        TableRef target = rt->add_table("big");
        target->add_column(type_Binary, "data");
        target->add_empty_row();
        {
            std::string data(0xfffff0, 'x');
            target->set_binary(0, 0, BinaryData(data.data(), 0xfffff0));
        }
        wt.commit();
    }

    {
        WriteTransaction wt(sg_w);
        Group& group = wt.get_group();
        rt->get_table(0)->set_binary(0, 0, BinaryData{"Hello", 5});
        rt->verify();
        wt.commit();
    }
}


TEST(LangBindHelper_MixedStringRollback)
{
    SHARED_GROUP_TEST_PATH(path);
    const char* key = crypt_key();
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(key));
    Group& g = const_cast<Group&>(sg_w.begin_write());

    TableRef t = g.add_table("table");
    t->add_column(type_Mixed, "mixed_column", false);
    t->add_empty_row();
    LangBindHelper::commit_and_continue_as_read(sg_w);

    // try with string
    LangBindHelper::promote_to_write(sg_w);
    t->set_mixed(0, 0, StringData("any string data"));
    LangBindHelper::rollback_and_continue_as_read(sg_w);
    g.verify();

    // do the same with binary data
    LangBindHelper::promote_to_write(sg_w);
    t->set_mixed(0, 0, BinaryData("any binary data"));
    LangBindHelper::rollback_and_continue_as_read(sg_w);
    g.verify();
}


TEST(LangBindHelper_RollbackOptimize)
{
    SHARED_GROUP_TEST_PATH(path);
    const char* key = crypt_key();
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(key));
    Group& g = sg_w.begin_write();

    g.insert_table(0, "t0");
    g.get_table(0)->add_column(type_String, "str_col_0", true);
    LangBindHelper::commit_and_continue_as_read(sg_w);
    g.verify();
    LangBindHelper::promote_to_write(sg_w);
    g.verify();
    g.get_table(0)->add_empty_row(198);
    g.get_table(0)->optimize(true);
    LangBindHelper::rollback_and_continue_as_read(sg_w);
}


TEST(LangBindHelper_BinaryReallocOverMax)
{
    SHARED_GROUP_TEST_PATH(path);
    const char* key = crypt_key();
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(key));
    Group& g = const_cast<Group&>(sg_w.begin_write());

    g.add_table("table");
    g.get_table(0)->add_column(type_Binary, "binary_col", false);
    g.get_table(0)->insert_empty_row(0, 1);

    // The sizes of these binaries were found with AFL. Essentially we must hit
    // the case where doubling the allocated memory goes above max_array_payload
    // and hits the condition to clamp to the maximum.
    std::string blob1(8877637, static_cast<unsigned char>(133));
    std::string blob2(15994373, static_cast<unsigned char>(133));
    BinaryData dataAlloc(blob1);
    BinaryData dataRealloc(blob2);

    g.get_table(0)->set_binary(0, 0, dataAlloc);
    g.get_table(0)->set_binary(0, 0, dataRealloc);
    g.verify();
}


TEST(LangBindHelper_MixedTimestampTransaction)
{
    SHARED_GROUP_TEST_PATH(path);
    ShortCircuitHistory hist(path);
    DBRef sg_w = DB::create(hist);
    DB sg_r(hist);

    // the seconds part is constructed to test 64 bit integer reads
    Timestamp time(68451041280, 29);
    // also check that a negative time comes through the transaction intact
    Timestamp neg_time(-57, -23);

    ReadTransactionRef rt(sg_r);
    {
        WriteTransaction wt(sg_w);
        Group& group = wt.get_group();
        TableRef target = rt->add_table("table");
        target->add_column(type_Mixed, "mixed_col");
        target->add_empty_row(2);
        wt.commit();
    }

    LangBindHelper::advance_read(sg_r);
    {
        WriteTransaction wt(sg_w);
        Group& group = wt.get_group();
        TableRef target = rt->get_table("table");
        target->set_mixed(0, 0, Mixed(time));
        target->set_mixed(0, 1, Mixed(neg_time));
        rt->verify();
        wt.commit();
    }
    LangBindHelper::advance_read(sg_r);
    const Group& g = rt.get_group();
    g.verify();
    ConstTableRef t = g.get_table("table");
    CHECK(t->get_mixed(0, 0) == time);
    CHECK(t->get_mixed(0, 1) == neg_time);
}


// This test verifies that small unencrypted files are treated correctly if
// opened as encrypted.
#if REALM_ENABLE_ENCRYPTION
TEST(LangBindHelper_OpenAsEncrypted)
{

    {
        SHARED_GROUP_TEST_PATH(path);
        ShortCircuitHistory hist(path);
        DB sg_clear(hist);

        {
            WriteTransaction wt(sg_clear);
            Group& group = wt.get_group();
            TableRef target = rt->add_table("table");
            target->add_column(type_String, "mixed_col");
            target->add_empty_row();
            wt.commit();
        }

        sg_clear.close();

        const char* key = crypt_key(true);
        std::unique_ptr<Replication> hist_encrypt(make_in_realm_history(path));
        bool is_okay = false;
        try {
            DB sg_encrypt(*hist_encrypt, DBOptions(key));
        } catch (std::runtime_error&) {
            is_okay = true;
        }
        CHECK(is_okay);
    }
}
#endif


TEST(LangBindHelper_IndexedStringEnumColumnSwapRows)
{
    // Test case generated in [realm-core-2.8.6] on Wed Jul 26 17:33:36 2017.
    // The problem was that StringEnumColumn must override the default
    // implementation of Column::swap_rows()
    SHARED_GROUP_TEST_PATH(path);
    const char* key = nullptr;
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(key));
    Group& g = sg_w.begin_write();
    try {
        g.insert_table(0, "t0");
    }
    catch (const TableNameInUse&) {
    }
    g.get_table(0)->insert_column(0, DataType(2), "", true);
    g.get_table(0)->add_search_index(0);
    g.get_table(0)->optimize(true);
    g.get_table(0)->insert_empty_row(0, 128);
    g.verify();
    g.get_table(0)->swap_rows(127, 30);
    g.get_table(0)->insert_empty_row(95, 5);
    g.get_table(0)->remove(30);
    g.verify();
}


TEST(LangBindHelper_IndexedStringEnumColumnSwapRowsWithValue)
{
    // Test case generated in [realm-core-2.9.0] on Fri Aug 11 14:40:03 2017.
    SHARED_GROUP_TEST_PATH(path);
    const char* key = crypt_key();
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    DBRef sg_w = DB::create(*hist_w, DBOptions(key));
    Group& g = sg_w.begin_write();

    try {
        g.add_table("table");
    }
    catch (const TableNameInUse&) {
    }
    g.get_table(0)->add_column(type_String, "str_col", true);
    g.get_table(0)->add_search_index(0);
    g.get_table(0)->insert_empty_row(0, 16);
    g.get_table(0)->optimize(true);
    g.get_table(0)->set_string(0, 2, "some string payload");
    g.get_table(0)->swap_rows(2, 6);
    g.verify();
}


// Test case generated in [realm-core-4.0.4] on Mon Dec 18 13:33:24 2017.
// Adding 0 rows to a StringEnumColumn would add the default value to the keys
// but not the indexes creating an inconsistency.
TEST(LangBindHelper_EnumColumnAddZeroRows)
{
    SHARED_GROUP_TEST_PATH(path);
    const char* key = nullptr;
    std::unique_ptr<Replication> hist_r(make_in_realm_history(path));
    std::unique_ptr<Replication> hist_w(make_in_realm_history(path));
    SharedGroup sg_r(*hist_r, DBOptions(key));
    SharedGroup sg_w(*hist_w, DBOptions(key));
    Group& g = sg_w.begin_write();
    Group& g_r = const_cast<Group&>(sg_r.begin_read());

    try {
        g.insert_table(0, "");
    }
    catch (const TableNameInUse&) {
    }
    g.get_table(0)->add_column(DataType(2), "table", false);
    g.get_table(0)->optimize(true);
    LangBindHelper::commit_and_continue_as_read(sg_w);
    g.verify();
    LangBindHelper::promote_to_write(sg_w);
    g.verify();
    g.get_table(0)->add_empty_row(0);
    LangBindHelper::commit_and_continue_as_read(sg_w);
    LangBindHelper::advance_read(sg_r);
    g_r.verify();
    g.verify();
}


TEST(LangBindHelper_NonsharedAccessToRealmWithHistory)
{
    // Create a Realm file with a history (history_type !=
    // Reaplication::hist_None).
    SHARED_GROUP_TEST_PATH(path);
    {
        std::unique_ptr<Replication> history(make_in_realm_history(path));
        DB sg{*history};
        WriteTransaction wt{sg};
        wt.add_table("foo");
        wt.commit();
    }

    // Since the stored history type is now Replication::hist_InRealm, it should
    // now be impossible to open in shared mode with no replication plugin
    // (Replication::hist_None).
    CHECK_THROW(DB{path}, IncompatibleHistories);

    // Now modify the file in nonshared mode, which will discard the history (as
    // nonshared mode does not understand how to update it correctly).
    {
        const char* crypt_key = nullptr;
        Group group{path, crypt_key, Group::mode_ReadWriteNoCreate};
        rt->commit();
    }

    // Check the the history was actually discarded (reset to
    // Replication::hist_None).
    DB sg{path};
    ReadTransactionRef rt{sg};
    CHECK(rt.has_table("foo"));
}
#endif

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
