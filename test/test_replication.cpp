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
#ifdef TEST_REPLICATION

#include <algorithm>
#include <memory>

#include <realm.hpp>
#include <realm/util/features.h>
#include <realm/util/file.hpp>
#include <realm/replication.hpp>

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
class ReplSyncClient : public Replication {
public:
    ReplSyncClient(int history_schema_version, uint64_t file_ident = 0)
        : m_file_ident(file_ident)
        , m_history_schema_version(history_schema_version)
    {
    }

    version_type prepare_changeset(const char*, size_t, version_type version) override
    {
        if (!m_arr) {
            using gf = _impl::GroupFriend;
            Allocator& alloc = gf::get_alloc(*m_group);
            m_arr = std::make_unique<BinaryColumn>(alloc);
            gf::prepare_history_parent(*m_group, *m_arr, hist_SyncClient, m_history_schema_version, 0);
            m_arr->create();
        }
        return version + 1;
    }

    bool is_upgraded() const
    {
        return m_upgraded;
    }

    bool is_upgradable_history_schema(int) const noexcept override
    {
        return true;
    }

    void upgrade_history_schema(int) override
    {
        m_group->set_sync_file_id(m_file_ident);
        m_upgraded = true;
    }

    HistoryType get_history_type() const noexcept override
    {
        return hist_SyncClient;
    }

    int get_history_schema_version() const noexcept override
    {
        return m_history_schema_version;
    }

    std::unique_ptr<_impl::History> _create_history_read() override
    {
        return {};
    }

private:
    Group* m_group;
    std::unique_ptr<BinaryColumn> m_arr;
    uint64_t m_file_ident;
    int m_history_schema_version;
    bool m_upgraded = false;

    void do_initiate_transact(Group& group, version_type version, bool hist_updated) override
    {
        Replication::do_initiate_transact(group, version, hist_updated);
        m_group = &group;
    }
};

TEST(Replication_HistorySchemaVersionNormal)
{
    SHARED_GROUP_TEST_PATH(path);
    ReplSyncClient repl(1);
    DBRef sg_1 = DB::create(repl, path);
    // it should be possible to have two open shared groups on the same thread
    // without any read/write transactions in between
    DBRef sg_2 = DB::create(repl, path);
}

TEST(Replication_HistorySchemaVersionDuringWT)
{
    SHARED_GROUP_TEST_PATH(path);

    ReplSyncClient repl(1);
    DBRef sg_1 = DB::create(repl, path);
    {
        // Do an empty commit to force the file format version to be established.
        WriteTransaction wt(sg_1);
        wt.commit();
    }

    auto wt = sg_1->start_write();
    wt->set_sync_file_id(2);

    // It should be possible to open a second db at the same path
    // while a WriteTransaction is active via another SharedGroup.
    DBRef sg_2 = DB::create(repl, path);
    wt->commit();

    auto rt = sg_2->start_read();
    CHECK_EQUAL(rt->get_sync_file_id(), 2);
}


// This is to test that the exported file has no memory leaks
TEST(Replication_GroupWriteWithoutHistory)
{
    SHARED_GROUP_TEST_PATH(path);
    SHARED_GROUP_TEST_PATH(out1);
    SHARED_GROUP_TEST_PATH(out2);

    ReplSyncClient repl(1);
    DBRef sg_1 = DB::create(repl, path);
    {
        WriteTransaction wt(sg_1);
        auto table = wt.add_table("Table");
        auto col = table->add_column(type_String, "strings");
        auto obj = table->create_object();
        obj.set(col, "Hello");
        wt.commit();
    }
    {
        ReadTransaction rt(sg_1);
        // Export file without history
        rt.get_group().write(out1);
    }

    {
        // Open without history
        DBRef sg_2 = DB::create(out1);
        ReadTransaction rt(sg_2);
        rt.get_group().verify();
    }

    {
        ReadTransaction rt(sg_1);
        // Export file with history
        rt.get_group().write(out2, nullptr, 1);
    }

    {
        // Open with history
        ReplSyncClient repl2(1);
        DBRef sg_2 = DB::create(repl2, out2);
        ReadTransaction rt(sg_2);
        rt.get_group().verify();
    }
}

TEST(Replication_HistorySchemaVersionUpgrade)
{
    SHARED_GROUP_TEST_PATH(path);

    {
        ReplSyncClient repl(1);
        DBRef sg = DB::create(repl, path);
        {
            // Do an empty commit to force the file format version to be established.
            WriteTransaction wt(sg);
            wt.commit();
        }
    }

    ReplSyncClient repl(2);
    DBRef sg_1 = DB::create(repl, path); // This will be the session initiator
    CHECK(repl.is_upgraded());
    WriteTransaction wt(sg_1);
    // When this one is opened, the file should have been upgraded
    // If this was not the case we would have triggered another upgrade
    // and the test would hang
    DBRef sg_2 = DB::create(repl, path);
}

TEST(Replication_WriteWithoutHistory)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);

    ReplSyncClient repl(1);
    DBRef sg = DB::create(repl, path_1);
    {
        // Do an empty commit to force the file format version to be established.
        WriteTransaction wt(sg);
        wt.add_table("Table");
        wt.commit();
    }

    {
        ReadTransaction rt(sg);
        rt.get_group().write(path_2, nullptr, rt.get_version(), false);
    }
    // Make sure the realm can be opened without history
    DBRef sg_2 = DB::create(path_2);
    {
        WriteTransaction wt(sg_2);
        auto table = wt.get_table("Table");
        CHECK(table);
        table->add_column(type_Int, "int");
        wt.commit();
    }
}

struct ObjectMutationObserver : _impl::NoOpTransactionLogParser {
    unit_test::TestContext& test_context;
    std::set<std::pair<TableKey, ObjKey>> expected_creations;
    std::set<std::tuple<TableKey, ObjKey, ColKey>> expected_modifications;

    ObjectMutationObserver(unit_test::TestContext& test_context,
                           std::initializer_list<std::pair<TableKey, int64_t>> creations,
                           std::initializer_list<std::tuple<TableKey, int64_t, ColKey>> modifications)
        : test_context(test_context)
    {
        for (auto [tk, ok] : creations) {
            expected_creations.emplace(tk, ObjKey(ok));
        }
        for (auto [tk, ok, ck] : modifications) {
            expected_modifications.emplace(tk, ObjKey(ok), ck);
        }
    }

    ObjectMutationObserver& operator=(ObjectMutationObserver&& obs) noexcept
    {
        expected_creations.swap(obs.expected_creations);
        expected_modifications.swap(obs.expected_modifications);
        return *this;
    }

    bool create_object(ObjKey obj_key)
    {
        CHECK(expected_creations.erase(std::pair(get_current_table(), obj_key)));
        return true;
    }
    bool modify_object(ColKey col, ObjKey obj)
    {
        CHECK(expected_modifications.erase(std::tuple(get_current_table(), obj, col)));
        return true;
    }
    bool remove_object(ObjKey)
    {
        return true;
    }

    void check()
    {
        CHECK(expected_creations.empty());
        CHECK(expected_modifications.empty());
    }
};

template <typename Fn>
void expect(DBRef db, ObjectMutationObserver& observer, Fn&& write)
{
    auto read = db->start_read();
    {
        auto tr = db->start_write();
        write(*tr);
        tr->commit();
    }
    read->advance_read(&observer);
    observer.check();
}

TEST(Replication_MutationsOnNewlyCreatedObject)
{
    SHARED_GROUP_TEST_PATH(path);
    auto db = DB::create(make_in_realm_history(), path);

    TableKey tk;
    ColKey col;
    {
        auto tr = db->start_write();
        auto table = tr->add_table("table");
        tk = table->get_key();
        col = table->add_column(type_Int, "value");
        tr->commit();
    }

    // Object creations with immediate mutations should report creations only
    auto obs = ObjectMutationObserver(test_context, {{tk, 0}, {tk, 1}}, {});
    expect(db, obs, [](auto& tr) {
        auto table = tr.get_table("table");
        table->create_object().set_all(1);
        table->create_object().set_all(1);
    });

    // Mutating existing objects should report modifications
    obs = ObjectMutationObserver(test_context, {}, {{tk, 0, col}, {tk, 1, col}});
    expect(db, obs, [](auto& tr) {
        auto table = tr.get_table("table");
        table->get_object(0).set_all(1);
        table->get_object(1).set_all(1);
    });

    // Create two objects and then mutate them. We only track the most recently
    // created object, so this emits a mutation for the first object but not
    // the second.
    obs = ObjectMutationObserver(test_context, {{tk, 2}, {tk, 3}}, {{tk, 2, col}});
    expect(db, obs, [](auto& tr) {
        auto table = tr.get_table("table");
        auto obj1 = table->create_object();
        auto obj2 = table->create_object();
        obj1.set_all(1);
        obj2.set_all(1);
    });

    TableKey tk2;
    ColKey col2;
    {
        auto tr = db->start_write();
        auto table = tr->add_table("table 2");
        tk2 = table->get_key();
        col2 = table->add_column(type_Int, "value");
        tr->commit();
    }

    // Creating an object in one table and then modifying the object with the
    // same ObjKey in a different table
    obs = ObjectMutationObserver(test_context, {{tk2, 0}}, {{tk, 0, col}});
    expect(db, obs, [&](auto& tr) {
        auto table1 = tr.get_table(tk);
        auto table2 = tr.get_table(tk2);
        auto obj1 = table1->get_object(0);
        auto obj2 = table2->create_object();
        CHECK_EQUAL(obj1.get_key(), obj2.get_key());
        obj1.set_all(1);
        obj2.set_all(1);
    });

    // Mutating an object whose Table has an index in group greater than the
    // higest of any created object after creating an object, which has to clear
    // the is-new-object flag
    obs = ObjectMutationObserver(test_context, {{tk, 4}}, {{tk2, 0, col2}});
    expect(db, obs, [&](auto& tr) {
        auto table1 = tr.get_table(tk);
        auto table2 = tr.get_table(tk2);
        auto obj1 = table1->create_object();
        auto obj2 = table2->get_object(0);
        obj1.set_all(1);
        obj2.set_all(1);
    });

    // Splitting object creation and mutation over two different writes with the
    // same transaction object should produce mutation instructions
    obs = ObjectMutationObserver(test_context, {{tk, 5}}, {{tk, 5, col}});
    {
        auto read = db->start_read();
        auto tr = db->start_write();
        auto table = tr->get_table(tk);
        auto obj = table->create_object();
        tr->commit_and_continue_as_read();
        tr->promote_to_write();
        obj.set_all(1);
        tr->commit_and_continue_as_read();
        read->advance_read(&obs);
        obs.check();
    }
}

TEST(Replication_MutationsOnNewlyCreatedObject_Link)
{
    SHARED_GROUP_TEST_PATH(path);
    auto db = DB::create(make_in_realm_history(), path);
    auto tr = db->start_write();

    auto target_table = tr->add_table("target table");
    auto tk_target = target_table->get_key();
    auto ck_target_value = target_table->add_column(type_Int, "value");
    auto embedded_table = tr->add_table("embedded table", Table::Type::Embedded);
    embedded_table->add_column(type_Int, "value");

    auto table = tr->add_table("table");
    auto tk = table->get_key();
    ColKey ck_link_1 = table->add_column(*target_table, "link 1");
    ColKey ck_link_2 = table->add_column(*target_table, "link 2");
    ColKey ck_embedded_1 = table->add_column(*embedded_table, "embedded 1");
    ColKey ck_embedded_2 = table->add_column(*embedded_table, "embedded 2");
    tr->commit();

    // Each top-level object creation is reported along with the mutation on
    // target_1 due to that both target objects are created before the mutations.
    // Nothing is reported for embedded objects
    auto obs = ObjectMutationObserver(test_context, {{tk, 0}, {tk_target, 0}, {tk_target, 1}},
                                      {{tk_target, 0, ck_target_value}});
    expect(db, obs, [&](auto& tr) {
        auto table = tr.get_table(tk);
        auto target_table = tr.get_table(tk_target);
        Obj obj = table->create_object();
        Obj target_1 = target_table->create_object();
        Obj target_2 = target_table->create_object();

        obj.set(ck_link_1, target_1.get_key());
        obj.set(ck_link_2, target_2.get_key());
        target_1.set_all(1);
        target_2.set_all(1);

        obj.create_and_set_linked_object(ck_embedded_1).set_all(1);
        obj.create_and_set_linked_object(ck_embedded_2).set_all(1);
    });

    // Nullifying links via object deletions in both new and pre-existing objects
    // only reports the mutation in the pre-existing object
    obs = ObjectMutationObserver(test_context, {{tk, 1}}, {{tk, 0, ck_link_1}});
    expect(db, obs, [&](auto& tr) {
        auto table = tr.get_table(tk);
        auto target_table = tr.get_table(tk_target);
        Obj obj = table->create_object();
        obj.set(ck_link_1, target_table->get_object(0).get_key());
        obj.set(ck_link_2, target_table->get_object(1).get_key());

        target_table->get_object(0).remove();
    });
}

TEST(Replication_MutationsOnNewlyCreatedObject_Collections)
{
    SHARED_GROUP_TEST_PATH(path);
    auto db = DB::create(make_in_realm_history(), path);
    auto tr = db->start_write();

    auto table = tr->add_table("table");
    auto tk = table->get_key();
    ColKey ck_value = table->add_column(type_Int, "value");
    ColKey ck_value_set = table->add_column_set(type_Int, "value set");
    ColKey ck_value_list = table->add_column_list(type_Int, "value list");
    ColKey ck_value_dictionary = table->add_column_dictionary(type_Int, "value dictionary");

    auto target_table = tr->add_table("target table");
    auto tk_target = target_table->get_key();
    auto ck_target_value = target_table->add_column(type_Int, "value");
    ColKey ck_obj_set = table->add_column_set(*target_table, "obj set");
    ColKey ck_obj_list = table->add_column_list(*target_table, "obj list");
    ColKey ck_obj_dictionary = table->add_column_dictionary(*target_table, "obj dictionary");

    auto embedded_table = tr->add_table("embedded table", Table::Type::Embedded);
    auto ck_embedded_value = embedded_table->add_column(type_Int, "value");
    ColKey ck_embedded_list = table->add_column_list(*embedded_table, "embedded list");
    ColKey ck_embedded_dictionary = table->add_column_dictionary(*embedded_table, "embedded dictionary");

    tr->commit();

    auto obs = ObjectMutationObserver(test_context, {{tk, 0}, {tk_target, 0}}, {});
    expect(db, obs, [&](auto& tr) {
        // Should report object creation but none of these mutations
        auto table = tr.get_table(tk);
        Obj obj = table->create_object();
        obj.set<int64_t>(ck_value, 1);
        obj.get_set<int64_t>(ck_value_set).insert(1);
        obj.get_list<int64_t>(ck_value_list).add(1);
        obj.get_dictionary(ck_value_dictionary).insert("a", 1);

        // Should report the object creation but not the mutations on either object,
        // as they're both the most recently created object in each table
        auto target_table = tr.get_table(tk_target);
        Obj target_obj = target_table->create_object();
        target_obj.set<int64_t>(ck_target_value, 1);
        obj.get_linkset(ck_obj_set).insert(target_obj.get_key());
        obj.get_linklist(ck_obj_list).add(target_obj.get_key());
        obj.get_dictionary(ck_obj_dictionary).insert("a", target_obj.get_key());

        // Should not produce any instructions: embedded object creations aren't
        // replicated (as you can't observe embedded tables directly), and the
        // mutations are on the newest object for each table
        obj.get_linklist(ck_embedded_list).create_and_insert_linked_object(0).set(ck_embedded_value, 1);
        obj.get_dictionary(ck_embedded_dictionary).create_and_insert_linked_object("a").set(ck_embedded_value, 1);
    });
}

} // anonymous namespace

#endif // TEST_REPLICATION
