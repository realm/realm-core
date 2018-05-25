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
#include <realm/history.hpp>

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

class MyTrivialReplication : public TrivialReplication {
public:
    MyTrivialReplication(const std::string& path)
        : TrivialReplication(path)
    {
    }

    void replay_transacts(DB& target, util::Logger& replay_logger)
    {
        for (const Buffer<char>& changeset : m_changesets)
            apply_changeset(changeset.data(), changeset.size(), target, &replay_logger);
        m_changesets.clear();
    }

    void initiate_session(version_type) override
    {
        // No-op
    }

    void terminate_session() noexcept override
    {
        // No-op
    }

    HistoryType get_history_type() const noexcept override
    {
        return hist_None;
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

    _impl::History* get_history_write() override
    {
        return nullptr;
    }

    std::unique_ptr<_impl::History> get_history_read() override
    {
        return nullptr;
    }

private:
    version_type prepare_changeset(const char* data, size_t size, version_type orig_version) override
    {
        m_incoming_changeset = Buffer<char>(size); // Throws
        std::copy(data, data + size, m_incoming_changeset.data());
        // Make space for the new changeset in m_changesets such that we can be
        // sure no exception will be thrown whan adding the changeset in
        // finalize_changeset().
        m_changesets.reserve(m_changesets.size() + 1); // Throws
        return orig_version + 1;
    }

    void finalize_changeset() noexcept override
    {
        // The following operation will not throw due to the space reservation
        // carried out in prepare_new_changeset().
        m_changesets.push_back(std::move(m_incoming_changeset));
    }

    Buffer<char> m_incoming_changeset;
    std::vector<Buffer<char>> m_changesets;
};

class ReplSyncClient : public MyTrivialReplication {
public:
    ReplSyncClient(const std::string& path, int history_schema_version)
        : MyTrivialReplication(path)
        , m_history_schema_version(history_schema_version)
    {
    }

    void initialize(DB& sg) override
    {
        TrivialReplication::initialize(sg);
    }

    version_type prepare_changeset(const char*, size_t, version_type) override
    {
        if (!m_arr) {
            using gf = _impl::GroupFriend;
            Allocator& alloc = gf::get_alloc(*m_group);
            m_arr = std::make_unique<BinaryColumn>(alloc);
            m_arr->create();
            gf::prepare_history_parent(*m_group, *m_arr, hist_SyncClient, m_history_schema_version);
            // m_arr->update_parent(); // Throws
        }
        return 1;
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

private:
    int m_history_schema_version;
    bool m_upgraded = false;
    std::unique_ptr<BinaryColumn> m_arr;
};

#ifdef LEGACY_TESTS
void check(TestContext& test_context, DB& sg_1, const ReadTransaction& rt_2)
{
    ReadTransaction rt_1(sg_1);
    rt_1.get_group().verify();
    rt_2.get_group().verify();
    CHECK(rt_1.get_group() == rt_2.get_group());
}
#endif
void check(TestContext&, DBRef sg_1, const ReadTransaction& rt_2)
{
    ReadTransaction rt_1(sg_1);
    rt_1.get_group().verify();
    rt_2.get_group().verify();
    // CHECK(rt_1.get_group() == rt_2.get_group());
}
} // anonymous namespace


namespace {

void my_table_add_columns(TableRef t)
{
    t->add_column(type_Int, "my_int");
    t->add_column(type_Bool, "my_bool");
    t->add_column(type_Float, "my_float");
    t->add_column(type_Double, "my_double");
    t->add_column(type_String, "my_string");
    t->add_column(type_Binary, "my_binary");
}
}

TEST(Replication_General)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);

    CHECK(Version::has_feature(Feature::feature_Replication));

    MyTrivialReplication repl(path_1);
    DBRef sg_1 = DB::create(repl);
    {
        WriteTransaction wt(sg_1);
        TableRef table = wt.add_table("my_table");
        my_table_add_columns(table);
        table->create_object(ObjKey(0));
        wt.commit();
    }
    {
        WriteTransaction wt(sg_1);
        TableRef table = wt.get_table("my_table");
        char buf[] = {'1'};
        BinaryData bin(buf);
        Obj obj = table->get_object(ObjKey(0));
        obj.set_all(2, true, 2.0f, 2.0, "xx", bin);
        table->create_object(ObjKey(1)).set_all(3, true, 3.0f, 3.0, "xxx", bin);
        table->create_object(ObjKey(2)).set_all(1, true, 1.0f, 1.0, "x", bin);

        table->create_object(ObjKey(3)).set_all(3, true, 3.0f, 0.0, "", bin); // empty string
        table->create_object(ObjKey(4)).set_all(8, true, 3.0f, 1.0, "", bin); // empty string
        wt.commit();
    }
    {
        WriteTransaction wt(sg_1);
        TableRef table = wt.get_table("my_table");
        auto col_id = table->get_column_key("my_int");
        Obj obj = table->get_object(ObjKey(0)).set(col_id, 9);
        table->add_search_index(col_id);
        wt.commit();
    }
    {
        WriteTransaction wt(sg_1);
        TableRef table = wt.get_table("my_table");
        auto col_id = table->get_column_key("my_int");
        table->get_object(ObjKey(0)).add_int(col_id, 5);
        table->get_object(ObjKey(3)).set(col_id, 2);
        wt.commit();
    }
    {
        WriteTransaction wt(sg_1);
        TableRef table = wt.get_table("my_table");
        char buf[] = {'9'};
        BinaryData bin(buf);
        table->create_object(ObjKey(100)).set_all(8, false, 8.0f, 8.0, "y8", bin);
        wt.commit();
    }
    {
        WriteTransaction wt(sg_1);
        TableRef table = wt.get_table("my_table");
        table->remove_object(ObjKey(100));
        wt.commit();
    }

    util::Logger& replay_logger = test_context.logger;
    DBRef sg_2 = DB::create(path_2);
    repl.replay_transacts(*sg_2, replay_logger);

    {
        ReadTransaction rt_1(sg_1);
        ReadTransaction rt_2(sg_2);
        rt_1.get_group().verify();
        rt_2.get_group().verify();
        CHECK(rt_1.get_group() == rt_2.get_group());
        auto table = rt_2.get_table("my_table");
        auto col_id = table->get_column_key("my_int");
        auto col_st = table->get_column_key("my_string");

        CHECK_EQUAL(5, table->size());

        CHECK(table->has_search_index(col_id));
        CHECK_EQUAL(14, table->get_object(ObjKey(0)).get<Int>(col_id));
        CHECK_EQUAL(3, table->get_object(ObjKey(1)).get<Int>(col_id));
        CHECK_EQUAL(2, table->get_object(ObjKey(3)).get<Int>(col_id));
        CHECK_EQUAL(8, table->get_object(ObjKey(4)).get<Int>(col_id));

        StringData sd1 = table->get_object(ObjKey(4)).get<String>(col_st);

        CHECK(!sd1.is_null());
    }
    {
        WriteTransaction wt(sg_1);
        DisableReplication disable(wt);
        TableRef table = wt.get_table("my_table");
        table->create_object(ObjKey(100));
        wt.commit();
    }
    {
        WriteTransaction wt(sg_1);
        TableRef table = wt.get_table("my_table");
        auto col_id = table->get_column_key("my_int");
        table->create_object(ObjKey(200));
        table->remove_search_index(col_id);
        wt.commit();
    }
    repl.replay_transacts(*sg_2, replay_logger);
    {
        ReadTransaction rt_1(sg_1);
        ReadTransaction rt_2(sg_2);
        rt_1.get_group().verify();
        rt_2.get_group().verify();
        CHECK(rt_1.get_group() != rt_2.get_group());
        auto table = rt_2.get_table("my_table");
        auto col_id = table->get_column_key("my_int");
        CHECK_NOT(table->is_valid(ObjKey(100)));
        CHECK(table->is_valid(ObjKey(200)));
        CHECK_NOT(table->has_search_index(col_id));
    }
    // Clear table
    {
        WriteTransaction wt(sg_1);
        TableRef table = wt.get_table("my_table");
        table->clear();
        wt.commit();
    }
    repl.replay_transacts(*sg_2, replay_logger);
    {
        ReadTransaction rt_2(sg_2);
        rt_2.get_group().verify();
        auto table = rt_2.get_table("my_table");
        CHECK_EQUAL(table->size(), 0);
    }
}


TEST(Replication_Timestamp)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);

    MyTrivialReplication repl(path_1);
    DBRef sg_1 = DB::create(repl);
    ObjKey k1;
    ObjKey k2;
    {
        WriteTransaction wt(sg_1);
        TableRef table = wt.add_table("t");

        // Add nullable Timestamp column
        table->add_column(type_Timestamp, "ts", true);

        wt.commit();
    }
    {
        WriteTransaction wt(sg_1);
        TableRef table = wt.get_table("t");
        auto col = table->get_column_key("ts");

        Obj obj = table->create_object();
        CHECK(obj.get<Timestamp>(col).is_null());

        k1 = table->create_object().set(col, Timestamp(5, 6)).get_key();
        k2 = table->create_object().set(col, Timestamp(1, 2)).get_key();
        wt.commit();
    }
    {
        WriteTransaction wt(sg_1);
        TableRef table = wt.get_table("t");
        auto col = table->get_column_key("ts");

        // Overwrite non-null with null to test that
        // TransactLogParser::parse_one(InstructionHandler& handler) correctly will see a set_null instruction
        // and not a set_new_date instruction
        table->get_object(k1).set(col, Timestamp{});

        // Overwrite non-null with other non-null
        table->get_object(k2).set(col, Timestamp{3, 4});
        wt.commit();
    }
    {
        // Delete object
        WriteTransaction wt(sg_1);
        TableRef table = wt.get_table("t");
        table->begin()->remove();
        wt.commit();
    }

    util::Logger& replay_logger = test_context.logger;
    DBRef sg_2 = DB::create(path_2);
    repl.replay_transacts(*sg_2, replay_logger);
    {
        ReadTransaction rt_1(sg_1);
        rt_1.get_group().verify();
        ConstTableRef table = rt_1.get_table("t");
        auto col = table->get_column_key("ts");
        CHECK_EQUAL(2, table->size());
        CHECK(table->get_object(k1).is_null(col));
        CHECK_EQUAL(table->get_object(k2).get<Timestamp>(col), Timestamp(3, 4));
    }
}

TEST(Replication_Links)
{
    // This test checks that all the links-related stuff works through
    // replication. It does that in a chained manner where the output of one
    // test acts as the input of the next one. This is to save boilerplate code,
    // and to make the test scenarios slightly more varied and realistic.
    //
    // The following operations are covered (for cyclic stuff, see
    // Replication_LinkCycles):
    //
    // - add_empty_row to origin table
    // - add_empty_row to target table
    // - insert link + link list
    // - change link
    // - nullify link
    // - insert link into list
    // - remove link from list
    // - move link inside list
    // - clear link list
    // - move_last_over on origin table
    // - move_last_over on target table
    // - clear origin table
    // - clear target table
    // - insert and remove non-link-type columns in origin table
    // - Insert and remove link-type columns in origin table
    // - Insert and remove columns in target table

    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);

    util::Logger& replay_logger = test_context.logger;

    MyTrivialReplication repl(path_1);
    DBRef sg_1 = DB::create(repl);
    DBRef sg_2 = DB::create(path_2);
    std::vector<ObjKey> origin_1_keys{ObjKey(0), ObjKey(1)};
    std::vector<ObjKey> origin_2_keys{ObjKey(10), ObjKey(11)};
    const std::vector<ObjKey> target_1_keys{ObjKey(20), ObjKey(21)};
    const std::vector<ObjKey> target_2_keys{ObjKey(30), ObjKey(31)};

    // First create two target tables
    {
        WriteTransaction wt(sg_1);
        TableRef target_1 = wt.add_table("target_1");
        TableRef target_2 = wt.add_table("target_2");
        target_1->add_column(type_Int, "t_1");
        target_2->add_column(type_Int, "t_2");
        target_1->create_objects(target_1_keys);
        target_2->create_objects(target_2_keys);
        wt.commit();
    }
    repl.replay_transacts(*sg_2, replay_logger);
    {
        ReadTransaction rt(sg_2);
        check(test_context, sg_1, rt);
    }
    // Then create two origin tables
    {
        WriteTransaction wt(sg_1);
        TableRef origin_1 = wt.add_table("origin_1");
        TableRef origin_2 = wt.add_table("origin_2");
        TableRef target_1 = wt.get_table("target_1");
        TableRef target_2 = wt.get_table("target_2");
        origin_1->add_column_link(type_LinkList, "o_1_ll_1", *target_1);
        origin_1->add_column_list(type_Int, "o_1_f_2");
        origin_1->add_column_link(type_Link, "o_1_l_3", *target_1);
        origin_1->add_column_link(type_Link, "o_1_l_4", *target_2);
        origin_2->add_column_list(type_Int, "o_2_f_1");
        origin_2->add_column_link(type_Link, "o_2_l_2", *target_1);
        origin_2->add_column_link(type_LinkList, "o_2_ll_3", *target_2);
        origin_2->add_column_link(type_Link, "o_2_l_4", *target_2);

        origin_1->create_objects(origin_1_keys);
        origin_2->create_objects(origin_2_keys);
        wt.commit();
    }
    repl.replay_transacts(*sg_2, replay_logger);
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // null       null       null                   null       null                   null
    // null       null       null                   null       null                   null
    {
        ReadTransaction rt(sg_2);
        check(test_context, sg_1, rt);
        CHECK_EQUAL(4, rt.get_group().size());
        ConstTableRef origin_1 = rt.get_table("origin_1");
        ConstTableRef origin_2 = rt.get_table("origin_2");
        ConstTableRef target_1 = rt.get_table("target_1");
        ConstTableRef target_2 = rt.get_table("target_2");
        CHECK(origin_1);
        CHECK(origin_2);
        CHECK(target_1);
        CHECK(target_2);
        CHECK_EQUAL(2, origin_1->size());
        CHECK_EQUAL(2, origin_2->size());
        CHECK_EQUAL(2, target_1->size());
        CHECK_EQUAL(2, target_2->size());
        CHECK_EQUAL(4, origin_1->get_column_count());
        CHECK_EQUAL(4, origin_2->get_column_count());
        CHECK_EQUAL(1, target_1->get_column_count());
        CHECK_EQUAL(1, target_2->get_column_count());

        auto o_1_ll_1 = origin_1->get_column_key("o_1_ll_1");
        auto o_1_f_2 = origin_1->get_column_key("o_1_f_2");
        auto o_1_l_3 = origin_1->get_column_key("o_1_l_3");
        auto o_1_l_4 = origin_1->get_column_key("o_1_l_4");

        auto o_2_f_1 = origin_2->get_column_key("o_2_f_1");
        auto o_2_l_2 = origin_2->get_column_key("o_2_l_2");
        auto o_2_ll_3 = origin_2->get_column_key("o_2_ll_3");
        auto o_2_l_4 = origin_2->get_column_key("o_2_l_4");

        CHECK_EQUAL(type_LinkList, origin_1->get_column_type(o_1_ll_1));
        CHECK_EQUAL(type_Int, origin_1->get_column_type(o_1_f_2));
        CHECK_EQUAL(type_Link, origin_1->get_column_type(o_1_l_3));
        CHECK_EQUAL(type_Link, origin_1->get_column_type(o_1_l_4));

        CHECK_EQUAL(type_Int, origin_2->get_column_type(o_2_f_1));
        CHECK_EQUAL(type_Link, origin_2->get_column_type(o_2_l_2));
        CHECK_EQUAL(type_LinkList, origin_2->get_column_type(o_2_ll_3));
        CHECK_EQUAL(type_Link, origin_2->get_column_type(o_2_l_4));

        CHECK_EQUAL(target_1, origin_1->get_link_target(o_1_ll_1));
        CHECK_EQUAL(target_2, origin_1->get_link_target(o_1_l_4));
        CHECK_EQUAL(target_1, origin_2->get_link_target(o_2_l_2));
        CHECK_EQUAL(target_2, origin_2->get_link_target(o_2_l_4));

        ConstObj o_1_0 = origin_1->get_object(origin_1_keys[0]);
        ConstObj o_1_1 = origin_1->get_object(origin_1_keys[1]);
        ConstObj o_2_0 = origin_2->get_object(origin_2_keys[0]);
        ConstObj o_2_1 = origin_2->get_object(origin_2_keys[1]);

        CHECK(!o_1_0.is_null(o_1_ll_1));
        CHECK(o_1_0.get_linklist(o_1_ll_1).is_empty());
        CHECK(!o_1_0.is_null(o_1_f_2));
        CHECK(o_1_0.get_linklist(o_1_f_2).is_empty());
        CHECK(o_1_0.is_null(o_1_l_3));
        CHECK(o_1_0.is_null(o_1_l_4));

        CHECK(!o_1_1.is_null(o_1_ll_1));
        CHECK(o_1_1.get_linklist(o_1_ll_1).is_empty());
        CHECK(!o_1_1.is_null(o_1_f_2));
        CHECK(o_1_1.get_linklist(o_1_f_2).is_empty());
        CHECK(o_1_1.is_null(o_1_l_3));
        CHECK(o_1_1.is_null(o_1_l_4));

        CHECK(o_2_0.is_null(o_2_l_2));
        CHECK(!o_2_0.is_null(o_2_ll_3));
        CHECK(o_2_0.get_linklist(o_2_ll_3).is_empty());
        CHECK(o_2_0.is_null(o_2_l_4));

        CHECK(o_2_1.is_null(o_2_l_2));
        CHECK(!o_2_1.is_null(o_2_ll_3));
        CHECK(o_2_1.get_linklist(o_2_ll_3).is_empty());
        CHECK(o_2_1.is_null(o_2_l_4));

        ConstObj t_1_0 = target_1->get_object(target_1_keys[0]);
        ConstObj t_1_1 = target_1->get_object(target_1_keys[1]);
        ConstObj t_2_0 = target_2->get_object(target_2_keys[0]);
        ConstObj t_2_1 = target_2->get_object(target_2_keys[1]);

        CHECK_EQUAL(0, t_1_0.get_backlink_count(*origin_1, o_1_l_3));
        CHECK_EQUAL(0, t_1_0.get_backlink_count(*origin_1, o_1_ll_1));
        CHECK_EQUAL(0, t_1_0.get_backlink_count(*origin_2, o_2_l_2));
        CHECK_EQUAL(0, t_1_1.get_backlink_count(*origin_1, o_1_l_3));
        CHECK_EQUAL(0, t_1_1.get_backlink_count(*origin_1, o_1_ll_1));
        CHECK_EQUAL(0, t_1_1.get_backlink_count(*origin_2, o_2_l_2));

        CHECK_EQUAL(0, t_2_0.get_backlink_count(*origin_1, o_1_l_4));
        CHECK_EQUAL(0, t_2_0.get_backlink_count(*origin_2, o_2_ll_3));
        CHECK_EQUAL(0, t_2_0.get_backlink_count(*origin_2, o_2_l_4));
        CHECK_EQUAL(0, t_2_1.get_backlink_count(*origin_1, o_1_l_4));
        CHECK_EQUAL(0, t_2_1.get_backlink_count(*origin_2, o_2_ll_3));
        CHECK_EQUAL(0, t_2_1.get_backlink_count(*origin_2, o_2_l_4));
    }
    // Set some links
    {
        WriteTransaction wt(sg_1);
        TableRef origin_1 = wt.get_table("origin_1");
        TableRef origin_2 = wt.get_table("origin_2");
        auto o_1_l_3 = origin_1->get_column_key("o_1_l_3");
        auto o_1_l_4 = origin_1->get_column_key("o_1_l_4");
        auto o_2_l_2 = origin_2->get_column_key("o_2_l_2");
        auto o_2_l_4 = origin_2->get_column_key("o_2_l_4");
        Obj o_1_0 = origin_1->get_object(origin_1_keys[0]);
        Obj o_1_1 = origin_1->get_object(origin_1_keys[1]);
        Obj o_2_0 = origin_2->get_object(origin_2_keys[0]);
        Obj o_2_1 = origin_2->get_object(origin_2_keys[1]);

        o_1_0.set(o_1_l_4, target_2_keys[0]); // O_1_L_4[0] -> T_2[0]
        o_1_1.set(o_1_l_3, target_1_keys[0]); // O_1_L_3[1] -> T_1[0]
        o_1_1.set(o_1_l_4, target_2_keys[1]); // O_1_L_4[1] -> T_2[1]
        o_2_0.set(o_2_l_2, target_1_keys[1]); // O_2_L_2[0] -> T_1[1]
        o_2_0.set(o_2_l_4, target_2_keys[1]); // O_2_L_4[0] -> T_2[1]
        o_2_1.set(o_2_l_4, target_2_keys[0]); // O_2_L_4[1] -> T_2[0]
        wt.commit();
    }
    repl.replay_transacts(*sg_2, replay_logger);
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // null       T_2[0]     null                   T_1[1]     null                   T_2[1]
    // T_1[0]     T_2[1]     null                   null       null                   T_2[0]
    {
        ReadTransaction rt(sg_2);
        check(test_context, sg_1, rt);
        CHECK_EQUAL(4, rt.get_group().size());
        ConstTableRef origin_1 = rt.get_table("origin_1");
        ConstTableRef origin_2 = rt.get_table("origin_2");
        ConstTableRef target_1 = rt.get_table("target_1");
        ConstTableRef target_2 = rt.get_table("target_2");

        auto o_1_l_3 = origin_1->get_column_key("o_1_l_3");
        auto o_1_l_4 = origin_1->get_column_key("o_1_l_4");

        auto o_2_l_2 = origin_2->get_column_key("o_2_l_2");
        auto o_2_l_4 = origin_2->get_column_key("o_2_l_4");

        ConstObj o_1_0 = origin_1->get_object(origin_1_keys[0]);
        ConstObj o_1_1 = origin_1->get_object(origin_1_keys[1]);
        ConstObj o_2_0 = origin_2->get_object(origin_2_keys[0]);
        ConstObj o_2_1 = origin_2->get_object(origin_2_keys[1]);

        CHECK_EQUAL(target_1_keys[0], o_1_1.get<ObjKey>(o_1_l_3));
        CHECK_EQUAL(target_2_keys[0], o_1_0.get<ObjKey>(o_1_l_4));
        CHECK_EQUAL(target_2_keys[1], o_1_1.get<ObjKey>(o_1_l_4));
        CHECK_EQUAL(target_1_keys[1], o_2_0.get<ObjKey>(o_2_l_2));
        CHECK_EQUAL(target_2_keys[1], o_2_0.get<ObjKey>(o_2_l_4));
        CHECK_EQUAL(target_2_keys[0], o_2_1.get<ObjKey>(o_2_l_4));

        ConstObj t_1_0 = target_1->get_object(target_1_keys[0]);
        ConstObj t_1_1 = target_1->get_object(target_1_keys[1]);
        ConstObj t_2_0 = target_2->get_object(target_2_keys[0]);
        ConstObj t_2_1 = target_2->get_object(target_2_keys[1]);

        CHECK_EQUAL(1, t_1_0.get_backlink_count(*origin_1, o_1_l_3));
        CHECK_EQUAL(0, t_1_0.get_backlink_count(*origin_2, o_2_l_2));
        CHECK_EQUAL(0, t_1_1.get_backlink_count(*origin_1, o_1_l_3));
        CHECK_EQUAL(1, t_1_1.get_backlink_count(*origin_2, o_2_l_2));

        CHECK_EQUAL(1, t_2_0.get_backlink_count(*origin_1, o_1_l_4));
        CHECK_EQUAL(1, t_2_0.get_backlink_count(*origin_2, o_2_l_4));
        CHECK_EQUAL(1, t_2_1.get_backlink_count(*origin_1, o_1_l_4));
        CHECK_EQUAL(1, t_2_1.get_backlink_count(*origin_2, o_2_l_4));
    }
    // Add to link lists
    {
        WriteTransaction wt(sg_1);
        TableRef origin_1 = wt.get_table("origin_1");
        TableRef origin_2 = wt.get_table("origin_2");
        auto o_1_ll_1 = origin_1->get_column_key("o_1_ll_1");
        auto o_2_ll_3 = origin_2->get_column_key("o_2_ll_3");
        Obj o_1_0 = origin_1->get_object(origin_1_keys[0]);
        Obj o_1_1 = origin_1->get_object(origin_1_keys[1]);
        Obj o_2_0 = origin_2->get_object(origin_2_keys[0]);
        Obj o_2_1 = origin_2->get_object(origin_2_keys[1]);
        o_1_0.get_linklist(o_1_ll_1).clear();               // O_1_LL_1[0] -> []
        o_1_1.get_linklist(o_1_ll_1).add(target_1_keys[0]); // O_1_LL_1[1] -> T_1[0]
        o_2_0.get_linklist(o_2_ll_3).add(target_2_keys[1]); // O_2_LL_3[0] -> T_2[1]
        o_2_1.get_linklist(o_2_ll_3).add(target_2_keys[0]); // O_2_LL_3[1] -> T_2[0]
        o_2_1.get_linklist(o_2_ll_3).add(target_2_keys[1]); // O_2_LL_3[1] -> T_2[1]
        wt.commit();
    }
    repl.replay_transacts(*sg_2, replay_logger);
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // null       T_2[0]     []                     T_1[1]     [ T_2[1] ]             T_2[1]
    // T_1[0]     T_2[1]     [ T_1[0] ]             null       [ T_2[0] T_2[1] ]      T_2[0]
    {
        ReadTransaction rt(sg_2);
        check(test_context, sg_1, rt);
        CHECK_EQUAL(4, rt.get_group().size());
        ConstTableRef origin_1 = rt.get_table("origin_1");
        ConstTableRef origin_2 = rt.get_table("origin_2");
        ConstTableRef target_1 = rt.get_table("target_1");
        ConstTableRef target_2 = rt.get_table("target_2");

        auto o_1_ll_1 = origin_1->get_column_key("o_1_ll_1");
        auto o_1_l_3 = origin_1->get_column_key("o_1_l_3");
        auto o_1_l_4 = origin_1->get_column_key("o_1_l_4");

        auto o_2_l_2 = origin_2->get_column_key("o_2_l_2");
        auto o_2_ll_3 = origin_2->get_column_key("o_2_ll_3");
        auto o_2_l_4 = origin_2->get_column_key("o_2_l_4");

        ConstObj o_1_0 = origin_1->get_object(origin_1_keys[0]);
        ConstObj o_1_1 = origin_1->get_object(origin_1_keys[1]);
        ConstObj o_2_0 = origin_2->get_object(origin_2_keys[0]);
        ConstObj o_2_1 = origin_2->get_object(origin_2_keys[1]);

        CHECK_NOT(o_1_0.is_null(o_1_ll_1));
        CHECK_EQUAL(0, o_1_0.get_linklist(o_1_ll_1).size());
        CHECK_EQUAL(1, o_1_1.get_linklist(o_1_ll_1).size());
        CHECK_EQUAL(target_1_keys[0], o_1_1.get_linklist(o_1_ll_1).get(0));
        CHECK_EQUAL(1, o_2_0.get_linklist(o_2_ll_3).size());
        CHECK_EQUAL(target_2_keys[1], o_2_0.get_linklist(o_2_ll_3).get(0));
        CHECK_EQUAL(2, o_2_1.get_linklist(o_2_ll_3).size());
        CHECK_EQUAL(target_2_keys[0], o_2_1.get_linklist(o_2_ll_3).get(0));
        CHECK_EQUAL(target_2_keys[1], o_2_1.get_linklist(o_2_ll_3).get(1));

        ConstObj t_1_0 = target_1->get_object(target_1_keys[0]);
        ConstObj t_1_1 = target_1->get_object(target_1_keys[1]);
        ConstObj t_2_0 = target_2->get_object(target_2_keys[0]);
        ConstObj t_2_1 = target_2->get_object(target_2_keys[1]);

        CHECK_EQUAL(1, t_1_0.get_backlink_count(*origin_1, o_1_l_3));
        CHECK_EQUAL(1, t_1_0.get_backlink_count(*origin_1, o_1_ll_1));
        CHECK_EQUAL(0, t_1_0.get_backlink_count(*origin_2, o_2_l_2));
        CHECK_EQUAL(0, t_1_1.get_backlink_count(*origin_1, o_1_l_3));
        CHECK_EQUAL(0, t_1_1.get_backlink_count(*origin_1, o_1_ll_1));
        CHECK_EQUAL(1, t_1_1.get_backlink_count(*origin_2, o_2_l_2));

        CHECK_EQUAL(1, t_2_0.get_backlink_count(*origin_1, o_1_l_4));
        CHECK_EQUAL(1, t_2_0.get_backlink_count(*origin_2, o_2_ll_3));
        CHECK_EQUAL(1, t_2_0.get_backlink_count(*origin_2, o_2_l_4));
        CHECK_EQUAL(1, t_2_1.get_backlink_count(*origin_1, o_1_l_4));
        CHECK_EQUAL(2, t_2_1.get_backlink_count(*origin_2, o_2_ll_3));
        CHECK_EQUAL(1, t_2_1.get_backlink_count(*origin_2, o_2_l_4));
    }
    // Check that a non-empty row can be added to an origin table
    {
        WriteTransaction wt(sg_1);
        TableRef origin_2 = wt.get_table("origin_2");
        auto o_2_l_2 = origin_2->get_column_key("o_2_l_2");
        auto o_2_l_4 = origin_2->get_column_key("o_2_l_4");

        origin_2->create_objects(1, origin_2_keys);
        Obj o_2_2 = origin_2->get_object(origin_2_keys[2]);
        o_2_2.set(o_2_l_2, target_1_keys[1]); // O_2_L_2[2] -> T_1[1]
        o_2_2.set(o_2_l_4, target_2_keys[0]); // O_2_L_4[2] -> T_2[0]

        wt.commit();
    }
    repl.replay_transacts(*sg_2, replay_logger);
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // null       T_2[0]     []                     T_1[1]     [ T_2[1] ]             T_2[1]
    // T_1[0]     T_2[1]     [ T_1[0] ]             null       [ T_2[0] T_2[1] ]      T_2[0]
    //                                              T_1[1]     []                     T_2[0]
    {
        ReadTransaction rt(sg_2);
        check(test_context, sg_1, rt);
        ConstTableRef origin_1 = rt.get_table("origin_1");
        ConstTableRef origin_2 = rt.get_table("origin_2");
        ConstTableRef target_1 = rt.get_table("target_1");
        ConstTableRef target_2 = rt.get_table("target_2");
        CHECK_EQUAL(2, origin_1->size());
        CHECK_EQUAL(3, origin_2->size());

        auto o_2_l_2 = origin_2->get_column_key("o_2_l_2");
        auto o_2_ll_3 = origin_2->get_column_key("o_2_ll_3");
        auto o_2_l_4 = origin_2->get_column_key("o_2_l_4");

        ConstObj o_2_2 = origin_2->get_object(origin_2_keys[2]);
        CHECK_EQUAL(target_1_keys[1], o_2_2.get<ObjKey>(o_2_l_2));
        CHECK_EQUAL(0, o_2_2.get_linklist(o_2_ll_3).size());
        CHECK_EQUAL(target_2_keys[0], o_2_2.get<ObjKey>(o_2_l_4));

        ConstObj t_1_1 = target_1->get_object(target_1_keys[1]);
        ConstObj t_2_0 = target_2->get_object(target_2_keys[0]);

        CHECK_EQUAL(2, t_1_1.get_backlink_count(*origin_2, o_2_l_2));
        CHECK_EQUAL(2, t_2_0.get_backlink_count(*origin_2, o_2_l_4));
    }

    // Check that a link can be changed
    {
        WriteTransaction wt(sg_1);
        TableRef origin_1 = wt.get_table("origin_1");
        TableRef origin_2 = wt.get_table("origin_2");
        auto o_1_l_3 = origin_1->get_column_key("o_1_l_3");
        auto o_2_l_2 = origin_2->get_column_key("o_2_l_2");
        auto o_2_l_4 = origin_2->get_column_key("o_2_l_4");

        Obj o_1_0 = origin_1->get_object(origin_1_keys[0]);
        Obj o_2_2 = origin_2->get_object(origin_2_keys[2]);
        o_1_0.set(o_1_l_3, target_1_keys[1]); // null -> T_1[1]
        o_2_2.set(o_2_l_2, null_key);         // O_2_L_2[2] -> null
        o_2_2.set(o_2_l_4, target_2_keys[1]); // T_2[0] -> T_2[1]

        wt.commit();
    }
    repl.replay_transacts(*sg_2, replay_logger);
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // T_1[1]     T_2[0]     []                     T_1[1]     [ T_2[1] ]             T_2[1]
    // T_1[0]     T_2[1]     [ T_1[0] ]             null       [ T_2[0], T_2[1] ]     T_2[0]
    //                                              null       []                     T_2[1]
    {
        ReadTransaction rt(sg_2);
        check(test_context, sg_1, rt);
        ConstTableRef origin_1 = rt.get_table("origin_1");
        ConstTableRef origin_2 = rt.get_table("origin_2");
        ConstTableRef target_1 = rt.get_table("target_1");
        ConstTableRef target_2 = rt.get_table("target_2");

        auto o_1_l_3 = origin_1->get_column_key("o_1_l_3");
        auto o_2_l_2 = origin_2->get_column_key("o_2_l_2");
        auto o_2_ll_3 = origin_2->get_column_key("o_2_ll_3");
        auto o_2_l_4 = origin_2->get_column_key("o_2_l_4");

        ConstObj o_1_0 = origin_1->get_object(origin_1_keys[0]);
        CHECK_EQUAL(target_1_keys[1], o_1_0.get<ObjKey>(o_1_l_3));

        ConstObj o_2_2 = origin_2->get_object(origin_2_keys[2]);
        CHECK(o_2_2.is_null(o_2_l_2));
        CHECK_EQUAL(0, o_2_2.get_linklist(o_2_ll_3).size());
        CHECK_EQUAL(target_2_keys[1], o_2_2.get<ObjKey>(o_2_l_4));

        ConstObj t_1_1 = target_1->get_object(target_1_keys[1]);
        ConstObj t_2_0 = target_2->get_object(target_2_keys[0]);
        ConstObj t_2_1 = target_2->get_object(target_2_keys[1]);

        CHECK_EQUAL(1, t_1_1.get_backlink_count(*origin_1, o_1_l_3));
        CHECK_EQUAL(1, t_1_1.get_backlink_count(*origin_2, o_2_l_2));
        CHECK_EQUAL(1, t_2_0.get_backlink_count(*origin_2, o_2_l_4));
        CHECK_EQUAL(2, t_2_1.get_backlink_count(*origin_2, o_2_l_4));
    }

    // Check that a link can be inserted in a list / removed from a list
    {
        WriteTransaction wt(sg_1);
        TableRef origin_2 = wt.get_table("origin_2");
        auto o_2_ll_3 = origin_2->get_column_key("o_2_ll_3");

        Obj o_2_1 = origin_2->get_object(origin_2_keys[1]);
        Obj o_2_2 = origin_2->get_object(origin_2_keys[2]);
        o_2_1.get_linklist(o_2_ll_3).remove(0);                   // O_2_LL_3] -> [ T_2[1] ]
        o_2_2.get_linklist(o_2_ll_3).insert(0, target_2_keys[1]); // O_2_LL_3] -> [ T_2[1] ]
        o_2_2.get_linklist(o_2_ll_3).insert(0, target_2_keys[0]); // O_2_LL_3] -> [ T_2[0], T_2[1] ]

        wt.commit();
    }
    repl.replay_transacts(*sg_2, replay_logger);
    // O_1_L_3    O_1_L_4    O_1_LL_1               O_2_L_2    O_2_LL_3               O_2_L_4
    // ----------------------------------------------------------------------------------------
    // T_1[1]     T_2[0]     []                     T_1[1]     [ T_2[1] ]             T_2[1]
    // T_1[0]     T_2[1]     [ T_1[0] ]             null       [ T_2[1] ]             T_2[0]
    //                                              null       [ T_2[0], T_2[1] ]     T_2[1]
    {
        ReadTransaction rt(sg_2);
        check(test_context, sg_1, rt);
        ConstTableRef origin_1 = rt.get_table("origin_1");
        ConstTableRef origin_2 = rt.get_table("origin_2");
        ConstTableRef target_1 = rt.get_table("target_1");
        ConstTableRef target_2 = rt.get_table("target_2");

        auto o_2_ll_3 = origin_2->get_column_key("o_2_ll_3");

        ConstObj o_2_1 = origin_2->get_object(origin_2_keys[1]);
        CHECK_EQUAL(1, o_2_1.get_linklist(o_2_ll_3).size());
        CHECK_EQUAL(target_2_keys[1], o_2_1.get_linklist(o_2_ll_3).get(0));

        ConstObj o_2_2 = origin_2->get_object(origin_2_keys[2]);
        CHECK_EQUAL(2, o_2_2.get_linklist(o_2_ll_3).size());
        CHECK_EQUAL(target_2_keys[0], o_2_2.get_linklist(o_2_ll_3).get(0));
        CHECK_EQUAL(target_2_keys[1], o_2_2.get_linklist(o_2_ll_3).get(1));

        ConstObj t_2_0 = target_2->get_object(target_2_keys[0]);
        ConstObj t_2_1 = target_2->get_object(target_2_keys[1]);

        CHECK_EQUAL(1, t_2_0.get_backlink_count(*origin_2, o_2_ll_3));
        CHECK_EQUAL(3, t_2_1.get_backlink_count(*origin_2, o_2_ll_3));
    }
    // Check that an array of integers can be created
    {
        WriteTransaction wt(sg_1);
        TableRef origin_1 = wt.get_table("origin_1");
        auto o_1_f_2 = origin_1->get_column_key("o_1_f_2");

        Obj o_1_0 = origin_1->get_object(origin_1_keys[0]);
        Obj o_1_1 = origin_1->get_object(origin_1_keys[1]);
        o_1_0.get_list<Int>(o_1_f_2).add(7);     // O_1_F_2] -> [ 7 ]
        auto arr = o_1_1.get_list<Int>(o_1_f_2); // O_1_F_2] -> [ 5, 10, 15 ]
        arr.add(5);
        arr.add(10);
        arr.add(15);

        wt.commit();
    }
    repl.replay_transacts(*sg_2, replay_logger);
    // O_1_L_3    O_1_L_4    O_1_LL_1     O_1_F_2           O_2_L_2    O_2_LL_3               O_2_L_4
    // ------------------------------------------------------------------------------------------------
    // T_1[1]     T_2[0]     []           [ 7 ]             T_1[1]     [ T_2[1] ]             T_2[1]
    // T_1[0]     T_2[1]     [ T_1[0] ]   [ 5, 10, 15 ]     null       [ T_2[1] ]             T_2[0]
    //                                                      null       [ T_2[0], T_2[1] ]     T_2[1]
    {
        ReadTransaction rt(sg_2);
        check(test_context, sg_1, rt);
        ConstTableRef origin_1 = rt.get_table("origin_1");

        auto o_1_f_2 = origin_1->get_column_key("o_1_f_2");

        ConstObj o_1_0 = origin_1->get_object(origin_1_keys[0]);
        ConstObj o_1_1 = origin_1->get_object(origin_1_keys[1]);

        CHECK_EQUAL(1, o_1_0.get_list<Int>(o_1_f_2).size());
        CHECK_EQUAL(7, o_1_0.get_list<Int>(o_1_f_2)[0]);
        CHECK_EQUAL(3, o_1_1.get_list<Int>(o_1_f_2).size());
        CHECK_EQUAL(5, o_1_1.get_list<Int>(o_1_f_2)[0]);
        CHECK_EQUAL(10, o_1_1.get_list<Int>(o_1_f_2)[1]);
        CHECK_EQUAL(15, o_1_1.get_list<Int>(o_1_f_2)[2]);
    }

    // Check that a link list can be cleared, and that a value can be moved
    // inside a list
    {
        WriteTransaction wt(sg_1);
        TableRef origin_1 = wt.get_table("origin_1");
        TableRef origin_2 = wt.get_table("origin_2");
        auto o_1_ll_1 = origin_1->get_column_key("o_1_ll_1");
        auto o_1_f_2 = origin_1->get_column_key("o_1_f_2");
        auto o_2_ll_3 = origin_2->get_column_key("o_2_ll_3");

        Obj o_1_1 = origin_1->get_object(origin_1_keys[1]);
        Obj o_2_2 = origin_2->get_object(origin_2_keys[2]);
        o_1_1.get_linklist(o_1_ll_1).clear();    // O_1_LL_1 -> [ ]
        o_1_1.get_list<Int>(o_1_f_2).move(2, 1); // O_1_F_2 -> [ 5, 15, 10 ]
        o_2_2.get_linklist(o_2_ll_3).move(0, 1); // O_2_LL_3 -> [ T_2[1], T_2[0] ]

        wt.commit();
    }
    repl.replay_transacts(*sg_2, replay_logger);
    // O_1_L_3    O_1_L_4    O_1_LL_1     O_1_F_2           O_2_L_2    O_2_LL_3               O_2_L_4
    // ------------------------------------------------------------------------------------------------
    // T_1[1]     T_2[0]     []           [ 7 ]             T_1[1]     [ T_2[1] ]             T_2[1]
    // T_1[0]     T_2[1]     []           [ 5, 15, 10 ]     null       [ T_2[1] ]             T_2[0]
    //                                                      null       [ T_2[1], T_2[0] ]     T_2[1]
    {
        ReadTransaction rt(sg_2);
        check(test_context, sg_1, rt);
        ConstTableRef origin_1 = rt.get_table("origin_1");
        ConstTableRef origin_2 = rt.get_table("origin_2");
        ConstTableRef target_1 = rt.get_table("target_1");
        ConstTableRef target_2 = rt.get_table("target_2");

        auto o_1_ll_1 = origin_1->get_column_key("o_1_ll_1");
        auto o_1_f_2 = origin_1->get_column_key("o_1_f_2");
        auto o_2_ll_3 = origin_2->get_column_key("o_2_ll_3");

        ConstObj o_1_1 = origin_1->get_object(origin_1_keys[1]);
        CHECK_EQUAL(0, o_1_1.get_linklist(o_1_ll_1).size());
        CHECK_EQUAL(3, o_1_1.get_list<Int>(o_1_f_2).size());
        CHECK_EQUAL(5, o_1_1.get_list<Int>(o_1_f_2)[0]);
        CHECK_EQUAL(15, o_1_1.get_list<Int>(o_1_f_2)[1]);
        CHECK_EQUAL(10, o_1_1.get_list<Int>(o_1_f_2)[2]);

        ConstObj o_2_2 = origin_2->get_object(origin_2_keys[2]);
        CHECK_EQUAL(2, o_2_2.get_linklist(o_2_ll_3).size());
        CHECK_EQUAL(target_2_keys[1], o_2_2.get_linklist(o_2_ll_3).get(0));
        CHECK_EQUAL(target_2_keys[0], o_2_2.get_linklist(o_2_ll_3).get(1));

        ConstObj t_1_0 = target_1->get_object(target_1_keys[0]);
        ConstObj t_2_0 = target_2->get_object(target_2_keys[0]);
        ConstObj t_2_1 = target_2->get_object(target_2_keys[1]);

        CHECK_EQUAL(0, t_1_0.get_backlink_count(*origin_1, o_1_ll_1));
        CHECK_EQUAL(1, t_2_0.get_backlink_count(*origin_2, o_2_ll_3));
        CHECK_EQUAL(3, t_2_1.get_backlink_count(*origin_2, o_2_ll_3));
    }

    // Check that a list can have members swapped
    {
        WriteTransaction wt(sg_1);
        TableRef origin_1 = wt.get_table("origin_1");
        TableRef origin_2 = wt.get_table("origin_2");
        auto o_1_f_2 = origin_1->get_column_key("o_1_f_2");
        auto o_2_ll_3 = origin_2->get_column_key("o_2_ll_3");

        Obj o_1_1 = origin_1->get_object(origin_1_keys[1]);
        Obj o_2_2 = origin_2->get_object(origin_2_keys[2]);
        o_1_1.get_list<Int>(o_1_f_2).swap(2, 0); // O_1_F_2 -> [ 5, 15, 10 ]
        o_2_2.get_linklist(o_2_ll_3).swap(0, 1); // O_2_LL_3 -> [ T_2[1], T_2[0] ]

        wt.commit();
    }
    repl.replay_transacts(*sg_2, replay_logger);
    // O_1_L_3    O_1_L_4    O_1_LL_1     O_1_F_2           O_2_L_2    O_2_LL_3               O_2_L_4
    // ------------------------------------------------------------------------------------------------
    // T_1[1]     T_2[0]     []           [ 7 ]             T_1[1]     [ T_2[1] ]             T_2[1]
    // T_1[0]     T_2[1]     []           [ 10, 15, 5 ]     null       [ T_2[1] ]             T_2[0]
    //                                                      null       [ T_2[0], T_2[1] ]     T_2[1]
    {
        ReadTransaction rt(sg_2);
        check(test_context, sg_1, rt);
        ConstTableRef origin_1 = rt.get_table("origin_1");
        ConstTableRef origin_2 = rt.get_table("origin_2");
        ConstTableRef target_1 = rt.get_table("target_1");
        ConstTableRef target_2 = rt.get_table("target_2");

        auto o_1_f_2 = origin_1->get_column_key("o_1_f_2");
        auto o_2_ll_3 = origin_2->get_column_key("o_2_ll_3");

        ConstObj o_1_1 = origin_1->get_object(origin_1_keys[1]);
        CHECK_EQUAL(3, o_1_1.get_list<Int>(o_1_f_2).size());
        CHECK_EQUAL(10, o_1_1.get_list<Int>(o_1_f_2)[0]);
        CHECK_EQUAL(15, o_1_1.get_list<Int>(o_1_f_2)[1]);
        CHECK_EQUAL(5, o_1_1.get_list<Int>(o_1_f_2)[2]);

        ConstObj o_2_2 = origin_2->get_object(origin_2_keys[2]);
        CHECK_EQUAL(2, o_2_2.get_linklist(o_2_ll_3).size());
        CHECK_EQUAL(target_2_keys[0], o_2_2.get_linklist(o_2_ll_3).get(0));
        CHECK_EQUAL(target_2_keys[1], o_2_2.get_linklist(o_2_ll_3).get(1));

        ConstObj t_1_0 = target_1->get_object(target_1_keys[0]);
        ConstObj t_2_0 = target_2->get_object(target_2_keys[0]);
        ConstObj t_2_1 = target_2->get_object(target_2_keys[1]);

        CHECK_EQUAL(1, t_2_0.get_backlink_count(*origin_2, o_2_ll_3));
        CHECK_EQUAL(3, t_2_1.get_backlink_count(*origin_2, o_2_ll_3));
    }
}


TEST(Replication_ListOfPrimitives)
{
    /*
     * In order to get full coverage of the list code we just need to
     * check that the set and clear operation on all types work. All the
     * other operations are tested in the test above.
     */
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);

    util::Logger& replay_logger = test_context.logger;

    MyTrivialReplication repl(path_1);
    DBRef sg_1 = DB::create(repl);
    DBRef sg_2 = DB::create(path_2);
    ColKey col_int;
    ColKey col_boo;
    ColKey col_flo;
    ColKey col_dou;
    ColKey col_str;
    ColKey col_bin;
    ColKey col_tim;

    // Create table
    {
        WriteTransaction wt(sg_1);
        TableRef table = wt.add_table("table");
        col_int = table->add_column_list(type_Int, "integers");
        col_boo = table->add_column_list(type_Bool, "booleans");
        col_flo = table->add_column_list(type_Float, "floats");
        col_dou = table->add_column_list(type_Double, "doubles");
        col_str = table->add_column_list(type_String, "strings");
        col_bin = table->add_column_list(type_Binary, "binaries");
        col_tim = table->add_column_list(type_Timestamp, "dates");
        table->create_object(ObjKey(0));
        wt.commit();
    }
    repl.replay_transacts(*sg_2, replay_logger);
    {
        ReadTransaction rt(sg_2);
        check(test_context, sg_1, rt);

        ConstTableRef table = rt.get_table("table");
        CHECK(table);
        CHECK_EQUAL(1, table->size());

        ConstObj obj = table->get_object(ObjKey(0));

        CHECK(!obj.is_null(col_int));
        CHECK(!obj.is_null(col_boo));
        CHECK(!obj.is_null(col_flo));
        CHECK(!obj.is_null(col_dou));
        CHECK(!obj.is_null(col_str));
        CHECK(!obj.is_null(col_bin));
        CHECK(!obj.is_null(col_tim));
        CHECK(obj.get_linklist(col_int).is_empty());
        CHECK(obj.get_linklist(col_boo).is_empty());
        CHECK(obj.get_linklist(col_flo).is_empty());
        CHECK(obj.get_linklist(col_dou).is_empty());
        CHECK(obj.get_linklist(col_str).is_empty());
        CHECK(obj.get_linklist(col_bin).is_empty());
        CHECK(obj.get_linklist(col_tim).is_empty());
    }
    // Add valuest to lists
    {
        WriteTransaction wt(sg_1);
        TableRef table = wt.get_table("table");

        char buf[10];
        memset(buf, 'A', sizeof(buf));
        Obj obj = table->get_object(ObjKey(0));
        obj.get_list<Int>(col_int).add(100);
        obj.get_list<Bool>(col_boo).add(true);
        obj.get_list<Float>(col_flo).add(100.f);
        obj.get_list<Double>(col_dou).add(100.);
        obj.get_list<String>(col_str).add(StringData("Hello"));
        obj.get_list<Binary>(col_bin).add(BinaryData(buf, sizeof(buf)));
        obj.get_list<Timestamp>(col_tim).add(Timestamp(100, 0));

        wt.commit();
    }
    repl.replay_transacts(*sg_2, replay_logger);
    {
        ReadTransaction rt(sg_2);
        check(test_context, sg_1, rt);

        ConstTableRef table = rt.get_table("table");
        CHECK(table);
        CHECK_EQUAL(1, table->size());

        ConstObj obj = table->get_object(ObjKey(0));

        char buf[10];
        memset(buf, 'A', sizeof(buf));
        BinaryData bin(buf, sizeof(buf));
        CHECK_EQUAL(obj.get_list<Int>(col_int)[0], 100);
        CHECK_EQUAL(obj.get_list<Bool>(col_boo)[0], true);
        CHECK_EQUAL(obj.get_list<Float>(col_flo)[0], 100.f);
        CHECK_EQUAL(obj.get_list<Double>(col_dou)[0], 100.);
        CHECK_EQUAL(obj.get_list<String>(col_str)[0], "Hello");
        CHECK_EQUAL(obj.get_list<Binary>(col_bin)[0], bin);
        CHECK_EQUAL(obj.get_list<Timestamp>(col_tim)[0], Timestamp(100, 0));
    }
    // Modify values
    {
        WriteTransaction wt(sg_1);
        TableRef table = wt.get_table("table");

        char buf[10];
        memset(buf, 'B', sizeof(buf));
        Obj obj = table->get_object(ObjKey(0));
        obj.get_list<Int>(col_int).set(0, 200);
        obj.get_list<Bool>(col_boo).set(0, false);
        obj.get_list<Float>(col_flo).set(0, 200.f);
        obj.get_list<Double>(col_dou).set(0, 200.);
        obj.get_list<String>(col_str).set(0, StringData("World"));
        obj.get_list<Binary>(col_bin).set(0, BinaryData(buf, sizeof(buf)));
        obj.get_list<Timestamp>(col_tim).set(0, Timestamp(200, 0));

        wt.commit();
    }
    repl.replay_transacts(*sg_2, replay_logger);
    {
        ReadTransaction rt(sg_2);
        check(test_context, sg_1, rt);

        ConstTableRef table = rt.get_table("table");
        CHECK(table);
        CHECK_EQUAL(1, table->size());

        ConstObj obj = table->get_object(ObjKey(0));

        char buf[10];
        memset(buf, 'B', sizeof(buf));
        BinaryData bin(buf, sizeof(buf));
        CHECK_EQUAL(obj.get_list<Int>(col_int)[0], 200);
        CHECK_EQUAL(obj.get_list<Bool>(col_boo)[0], false);
        CHECK_EQUAL(obj.get_list<Float>(col_flo)[0], 200.f);
        CHECK_EQUAL(obj.get_list<Double>(col_dou)[0], 200.);
        CHECK_EQUAL(obj.get_list<String>(col_str)[0], "World");
        CHECK_EQUAL(obj.get_list<Binary>(col_bin)[0], bin);
        CHECK_EQUAL(obj.get_list<Timestamp>(col_tim)[0], Timestamp(200, 0));
    }
    // Clear/erase lists
    {
        WriteTransaction wt(sg_1);
        TableRef table = wt.get_table("table");

        char buf[10];
        memset(buf, 'A', sizeof(buf));
        Obj obj = table->get_object(ObjKey(0));
        obj.get_list<Int>(col_int).clear();
        obj.get_list<Bool>(col_boo).clear();
        obj.get_list<Float>(col_flo).clear();
        obj.get_list<Double>(col_dou).remove(0);
        obj.get_list<String>(col_str).remove(0);
        obj.get_list<Binary>(col_bin).remove(0);
        obj.get_list<Timestamp>(col_tim).remove(0);

        wt.commit();
    }
    repl.replay_transacts(*sg_2, replay_logger);
    {
        ReadTransaction rt(sg_2);
        check(test_context, sg_1, rt);

        ConstTableRef table = rt.get_table("table");
        CHECK(table);
        CHECK_EQUAL(1, table->size());

        ConstObj obj = table->get_object(ObjKey(0));

        char buf[10];
        memset(buf, 'A', sizeof(buf));
        BinaryData bin(buf, sizeof(buf));
        CHECK_EQUAL(obj.get_list<Int>(col_int).size(), 0);
        CHECK_EQUAL(obj.get_list<Bool>(col_boo).size(), 0);
        CHECK_EQUAL(obj.get_list<Float>(col_flo).size(), 0);
        CHECK_EQUAL(obj.get_list<Double>(col_dou).size(), 0);
        CHECK_EQUAL(obj.get_list<String>(col_str).size(), 0);
        CHECK_EQUAL(obj.get_list<Binary>(col_bin).size(), 0);
        CHECK_EQUAL(obj.get_list<Timestamp>(col_tim).size(), 0);
    }
}

#ifdef LEGACY_TESTS
TEST(Replication_CascadeRemove_ColumnLink)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);

    util::Logger& replay_logger = test_context.logger;

    DB sg(path_1);
    MyTrivialReplication repl(path_2);
    DB sg_w(repl);

    {
        WriteTransaction wt(sg_w);
        Table& origin = *wt.add_table("origin");
        Table& target = *wt.add_table("target");
        origin.add_column_link(type_Link, "o_1", target, link_Strong);
        target.add_column(type_Int, "t_1");
        wt.commit();
    }

    // perform_change expects sg to be in a read transaction
    sg.begin_read();

    ConstTableRef target;
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

        // Perform the modification
        {
            WriteTransaction wt(sg_w);
            func(*wt.get_table("origin"));
            wt.commit();
        }

        // Apply the changes to sg via replication
        sg.end_read();
        repl.replay_transacts(sg, replay_logger);
        const Group& group = sg.begin_read();
        group.verify();

        target = group.get_table("target");
        if (target->size() > 0)
            target_row_0 = target->get(0);
        if (target->size() > 1)
            target_row_1 = target->get(1);
        // Leave `group` and the target accessors in a state which can be tested
        // with the changes applied
    };

    // Break link by nullifying
    perform_change([](Table& origin) { origin[1].nullify_link(0); });
    CHECK(target_row_0 && !target_row_1);
    CHECK_EQUAL(target->size(), 1);

    // Break link by reassign
    perform_change([](Table& origin) { origin[1].set_link(0, 0); });
    CHECK(target_row_0 && !target_row_1);
    CHECK_EQUAL(target->size(), 1);

    // Avoid breaking link by reassigning self
    perform_change([](Table& origin) { origin[1].set_link(0, 1); });
    // Should not delete anything
    CHECK(target_row_0 && target_row_1);
    CHECK_EQUAL(target->size(), 2);

    // Break link by explicit row removal
    perform_change([](Table& origin) { origin[1].move_last_over(); });
    CHECK(target_row_0 && !target_row_1);
    CHECK_EQUAL(target->size(), 1);

    // Break link by clearing table
    perform_change([](Table& origin) { origin.clear(); });
    CHECK(!target_row_0 && !target_row_1);
    CHECK_EQUAL(target->size(), 0);
}

TEST(Replication_AdvanceReadTransact_CascadeRemove_ColumnLinkList)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);

    util::Logger& replay_logger = test_context.logger;

    DB sg(path_1);
    MyTrivialReplication repl(path_2);
    DB sg_w(repl);

    {
        WriteTransaction wt(sg_w);
        Table& origin = *wt.add_table("origin");
        Table& target = *wt.add_table("target");
        origin.add_column_link(type_LinkList, "o_1", target, link_Strong);
        target.add_column(type_Int, "t_1");
        wt.commit();
    }

    // perform_change expects sg to be in a read transaction
    sg.begin_read();

    ConstTableRef target;
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
            origin_w[0].get_linklist(0)->add(0);
            origin_w[1].get_linklist(0)->add(0);
            origin_w[1].get_linklist(0)->add(1);

            wt.commit();
        }

        // Perform the modification
        {
            WriteTransaction wt(sg_w);
            func(*wt.get_table("origin"));
            wt.commit();
        }

        // Apply the changes to sg via replication
        sg.end_read();
        repl.replay_transacts(sg, replay_logger);
        const Group& group = sg.begin_read();
        group.verify();

        target = group.get_table("target");
        if (target->size() > 0)
            target_row_0 = target->get(0);
        if (target->size() > 1)
            target_row_1 = target->get(1);
        // Leave `group` and the target accessors in a state which can be tested
        // with the changes applied
    };

    // Break link by clearing list
    perform_change([](Table& origin) { origin[1].get_linklist(0)->clear(); });
    CHECK(target_row_0 && !target_row_1);
    CHECK_EQUAL(target->size(), 1);

    // Break link by removal from list
    perform_change([](Table& origin) { origin[1].get_linklist(0)->remove(1); });
    CHECK(target_row_0 && !target_row_1);
    CHECK_EQUAL(target->size(), 1);

    // Break link by reassign
    perform_change([](Table& origin) { origin[1].get_linklist(0)->set(1, 0); });
    CHECK(target_row_0 && !target_row_1);
    CHECK_EQUAL(target->size(), 1);

    // Avoid breaking link by reassigning self
    perform_change([](Table& origin) { origin[1].get_linklist(0)->set(1, 1); });
    // Should not delete anything
    CHECK(target_row_0 && target_row_1);
    CHECK_EQUAL(target->size(), 2);

    // Break link by explicit row removal
    perform_change([](Table& origin) { origin[1].move_last_over(); });
    CHECK(target_row_0 && !target_row_1);
    CHECK_EQUAL(target->size(), 1);

    // Break link by clearing table
    perform_change([](Table& origin) { origin.clear(); });
    CHECK(!target_row_0 && !target_row_1);
    CHECK_EQUAL(target->size(), 0);
}
#endif

TEST(Replication_LinkListSelfLinkNullification)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);

    MyTrivialReplication repl(path_1);
    DBRef sg_1 = DB::create(repl);
    DBRef sg_2 = DB::create(path_2);

    util::Logger& replay_logger = test_context.logger;

    {
        WriteTransaction wt(sg_1);
        TableRef t = wt.add_table("t");
        auto col = t->add_column_link(type_LinkList, "l", *t);
        Obj obj0 = t->create_object();
        Obj obj1 = t->create_object();
        auto ll = obj1.get_linklist(col);
        ll.add(obj1.get_key());
        ll.add(obj1.get_key());
        ll.add(obj0.get_key());
        auto ll2 = obj0.get_linklist(col);
        ll2.add(obj0.get_key());
        ll2.add(obj1.get_key());
        wt.commit();
    }
    repl.replay_transacts(*sg_2, replay_logger);

    {
        WriteTransaction wt(sg_1);
        TableRef t = wt.get_table("t");
        t->begin()->remove();
        wt.commit();
    }
    repl.replay_transacts(*sg_2, replay_logger);
    ReadTransaction rt_2{sg_2};
    check(test_context, sg_1, rt_2);
}


TEST(Replication_NullStrings)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);

    util::Logger& replay_logger = test_context.logger;

    MyTrivialReplication repl(path_1);
    DBRef sg_1 = DB::create(repl);
    DBRef sg_2 = DB::create(path_2);

    {
        WriteTransaction wt(sg_1);
        TableRef table1 = wt.add_table("table");
        auto col_string = table1->add_column(type_String, "c1", true);
        auto col_binary = table1->add_column(type_Binary, "b1", true);

        Obj obj0 = table1->create_object(ObjKey(0));
        Obj obj1 = table1->create_object(ObjKey(1));
        Obj obj2 = table1->create_object(ObjKey(2));

        obj1.set(col_string, StringData("")); // empty string
        obj2.set(col_string, StringData());   // null

        obj1.set(col_binary, BinaryData("")); // empty string
        obj2.set(col_binary, BinaryData());   // null

        CHECK(obj0.get<String>(col_string).is_null());
        CHECK(!obj1.get<String>(col_string).is_null());
        CHECK(obj2.get<String>(col_string).is_null());

        CHECK(obj0.get<Binary>(col_binary).is_null());
        CHECK(!obj1.get<Binary>(col_binary).is_null());
        CHECK(obj2.get<Binary>(col_binary).is_null());

        wt.commit();
    }
    repl.replay_transacts(*sg_2, replay_logger);
    {
        ReadTransaction rt(sg_2);
        ConstTableRef table2 = rt.get_table("table");
        auto col_string = table2->get_column_key("c1");
        auto col_binary = table2->get_column_key("b1");

        ConstObj obj0 = table2->get_object(ObjKey(0));
        ConstObj obj1 = table2->get_object(ObjKey(1));
        ConstObj obj2 = table2->get_object(ObjKey(2));

        CHECK(obj0.get<String>(col_string).is_null());
        CHECK(!obj1.get<String>(col_string).is_null());
        CHECK(obj2.get<String>(col_string).is_null());

        CHECK(obj0.get<Binary>(col_binary).is_null());
        CHECK(!obj1.get<Binary>(col_binary).is_null());
        CHECK(obj2.get<Binary>(col_binary).is_null());
    }
}


TEST(Replication_NullInteger)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);

    util::Logger& replay_logger = test_context.logger;

    MyTrivialReplication repl(path_1);
    DBRef sg_1 = DB::create(repl);
    DBRef sg_2 = DB::create(path_2);

    {
        WriteTransaction wt(sg_1);
        TableRef table1 = wt.add_table("table");
        auto col_int = table1->add_column(type_Int, "c1", true);

        Obj obj0 = table1->create_object(ObjKey(0));
        Obj obj1 = table1->create_object(ObjKey(1));
        Obj obj2 = table1->create_object(ObjKey(2));

        obj1.set(col_int, 0);
        obj2.set_null(col_int);

        CHECK(obj0.is_null(col_int));
        CHECK_NOT(obj1.is_null(col_int));
        CHECK(obj2.is_null(col_int));

        wt.commit();
    }
    repl.replay_transacts(*sg_2, replay_logger);
    {
        ReadTransaction rt(sg_2);
        ConstTableRef table2 = rt.get_table("table");
        auto col_int = table2->get_column_key("c1");

        ConstObj obj0 = table2->get_object(ObjKey(0));
        ConstObj obj1 = table2->get_object(ObjKey(1));
        ConstObj obj2 = table2->get_object(ObjKey(2));

        CHECK(obj0.is_null(col_int));
        CHECK_NOT(obj1.is_null(col_int));
        CHECK(obj2.is_null(col_int));
    }
}


TEST(Replication_RenameGroupLevelTable_RenameColumn)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);

    util::Logger& replay_logger = test_context.logger;

    MyTrivialReplication repl(path_1);
    DBRef sg_1 = DB::create(repl);
    DBRef sg_2 = DB::create(path_2);

    {
        WriteTransaction wt(sg_1);
        TableRef table1 = wt.add_table("foo");
        table1->add_column(type_Int, "a");
        table1->add_column(type_Int, "c");
        TableRef table2 = wt.add_table("foo2");
        wt.commit();
    }
    {
        WriteTransaction wt(sg_1);
        wt.get_group().rename_table("foo", "bar");
        auto bar = wt.get_table("bar");
        auto col_a = bar->get_column_key("a");
        bar->rename_column(col_a, "b");
        wt.commit();
    }
    repl.replay_transacts(*sg_2, replay_logger);
    {
        ReadTransaction rt(sg_2);
        ConstTableRef foo = rt.get_table("foo");
        CHECK(!foo);
        ConstTableRef bar = rt.get_table("bar");
        CHECK(bar);
        CHECK(bar->get_column_key("b"));
    }
}

TEST(Replication_RemoveGroupLevelTable_RemoveColumn)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);

    util::Logger& replay_logger = test_context.logger;

    MyTrivialReplication repl(path_1);
    DBRef sg_1 = DB::create(repl);
    DBRef sg_2 = DB::create(path_2);

    {
        WriteTransaction wt(sg_1);
        TableRef table1 = wt.add_table("foo");
        TableRef table2 = wt.add_table("bar");
        table1->add_column(type_Int, "a");
        table2->add_column(type_Int, "c");
        wt.commit();
    }
    repl.replay_transacts(*sg_2, replay_logger);
    {
        ReadTransaction rt(sg_2);
        ConstTableRef foo = rt.get_table("foo");
        ConstTableRef bar = rt.get_table("bar");
        CHECK(foo->get_column_key("a"));
        CHECK(bar->get_column_key("c"));
    }
    {
        WriteTransaction wt(sg_1);
        wt.get_group().remove_table("foo");
        auto bar = wt.get_table("bar");
        auto col_c = bar->get_column_key("c");
        bar->remove_column(col_c);
        wt.commit();
    }
    repl.replay_transacts(*sg_2, replay_logger);
    {
        ReadTransaction rt(sg_2);
        CHECK_NOT(rt.get_table("foo"));
        ConstTableRef bar = rt.get_table("bar");
        CHECK(bar);
        CHECK_NOT(bar->get_column_key("c"));
    }
}

#ifdef LEGACY_TESTS
TEST(Replication_LinkListNullifyThroughTableView)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);

    util::Logger& replay_logger = test_context.logger;

    MyTrivialReplication repl(path_1);
    DBRef sg_1 = DB::create(repl);
    DBRef sg_2 = DB::create(path_2);

    {
        WriteTransaction wt(sg_1);
        TableRef t0 = wt.add_table("t0");
        TableRef t1 = wt.add_table("t1");
        t0->add_column_link(type_LinkList, "l", *t1);
        t1->add_column(type_Int, "i");
        t1->add_empty_row();
        t0->add_empty_row();
        t0->get_linklist(0, 0)->add(0);

        // Create a TableView for the table and remove the rows through that.
        auto tv = t1->where().find_all();
        tv.clear(RemoveMode::unordered);

        wt.commit();
    }
    repl.replay_transacts(sg_2, replay_logger);
    {
        ReadTransaction rt1(sg_1);
        ReadTransaction rt2(sg_2);

        CHECK(rt1.get_group() == rt2.get_group());
        CHECK_EQUAL(rt1.get_table(0)->size(), 1);
        CHECK_EQUAL(rt1.get_table(1)->size(), 0);
        CHECK_EQUAL(rt1.get_table(0)->get_linklist(0, 0)->size(), 0);
    }
}

TEST(Replication_Substrings)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);

    util::Logger& replay_logger = test_context.logger;

    MyTrivialReplication repl(path_1);
    DBRef sg_1 = DB::create(repl);
    DBRef sg_2 = DB::create(path_2);

    {
        WriteTransaction wt(sg_1);
        TableRef table = wt.add_table("table");
        table->add_column(type_String, "string");
        table->add_empty_row();
        table->set_string(0, 0, "Hello, World!");
        wt.commit();
    }
    {
        WriteTransaction wt(sg_1);
        TableRef table = wt.get_table("table");
        table->remove_substring(0, 0, 0, 6);
        table->insert_substring(0, 0, 0, "Goodbye, Cruel");
        wt.commit();
    }
    repl.replay_transacts(sg_2, replay_logger);
    {
        ReadTransaction rt(sg_2);
        auto table = rt.get_table("table");
        CHECK_EQUAL("Goodbye, Cruel World!", table->get_string(0, 0));
    }
}


TEST(Replication_MoveSelectedLinkView)
{
    // 1st: Create table with two rows
    // 2nd: Select link list via 2nd row
    // 3rd: Delete first row by move last over (which moves the row of the selected link list)
    // 4th: Modify the selected link list.
    // 5th: Replay changeset on different Realm

    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);

    util::Logger& replay_logger = test_context.logger;

    MyTrivialReplication repl(path_1);
    DBRef sg_1 = DB::create(repl);
    DBRef sg_2 = DB::create(path_2);

    {
        WriteTransaction wt(sg_1);
        TableRef origin = wt.add_table("origin");
        TableRef target = wt.add_table("target");
        origin->add_column_link(type_LinkList, "", *target);
        target->add_column(type_Int, "");
        origin->add_empty_row(2);
        target->add_empty_row(2);
        wt.commit();
    }
    repl.replay_transacts(sg_2, replay_logger);
    {
        ReadTransaction rt(sg_2);
        rt.get_group().verify();
    }

    {
        WriteTransaction wt(sg_1);
        TableRef origin = wt.get_table("origin");
        LinkViewRef link_list = origin->get_linklist(0, 1);
        link_list->add(0);         // Select the link list of the 2nd row
        origin->move_last_over(0); // Move that link list
        link_list->add(1);         // Now modify it again
        wt.commit();
    }
    repl.replay_transacts(sg_2, replay_logger);
    {
        ReadTransaction rt(sg_2);
        rt.get_group().verify();
        ConstTableRef origin = rt.get_table("origin");
        ConstLinkViewRef link_list = origin->get_linklist(0, 0);
        CHECK_EQUAL(2, link_list->size());
    }

    // FIXME: Redo the test with all other table-level operations that move the
    // link list to a new row or column index.
}
#endif

TEST(Replication_HistorySchemaVersionNormal)
{
    SHARED_GROUP_TEST_PATH(path);
    ReplSyncClient repl(path, 1);
    DBRef sg_1 = DB::create(repl);
    // it should be possible to have two open shared groups on the same thread
    // without any read/write transactions in between
    DBRef sg_2 = DB::create(repl);
}

TEST(Replication_HistorySchemaVersionDuringWT)
{
    SHARED_GROUP_TEST_PATH(path);

    ReplSyncClient repl(path, 1);
    DBRef sg_1 = DB::create(repl);
    {
        // Do an empty commit to force the file format version to be established.
        WriteTransaction wt(sg_1);
        wt.commit();
    }

    WriteTransaction wt(sg_1);

    // It should be possible to open a second SharedGroup at the same path
    // while a WriteTransaction is active via another SharedGroup.
    DBRef sg_2 = DB::create(repl);
}

TEST(Replication_HistorySchemaVersionUpgrade)
{
    SHARED_GROUP_TEST_PATH(path);

    {
        ReplSyncClient repl(path, 1);
        DBRef sg = DB::create(repl);
        {
            // Do an empty commit to force the file format version to be established.
            WriteTransaction wt(sg);
            wt.commit();
        }
    }

    ReplSyncClient repl(path, 2);
    DBRef sg_1 = DB::create(repl); // This will be the session initiater
    CHECK(repl.is_upgraded());
    WriteTransaction wt(sg_1);
    // When this one is opened, the file should have been upgraded
    // If this was not the case we would have triggered another upgrade
    // and the test would hang
    DBRef sg_2 = DB::create(repl);
}

TEST(Replication_CreateAndRemoveObject)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);

    util::Logger& replay_logger = test_context.logger;

    MyTrivialReplication repl(path_1);
    DBRef sg_1 = DB::create(repl);
    DBRef sg_2 = DB::create(path_2);
    ColKey c0;
    {
        WriteTransaction wt(sg_1);
        TableRef table1 = wt.add_table("table");
        c0 = table1->add_column(type_Int, "int1");
        table1->create_object(ObjKey(123)).set(c0, 0);
        table1->create_object(ObjKey(456)).set(c0, 1);
        CHECK_EQUAL(table1->size(), 2);
        wt.commit();
    }
    repl.replay_transacts(*sg_2, replay_logger);
    {
        ReadTransaction rt(sg_2);
        ConstTableRef table2 = rt.get_table("table");

        CHECK_EQUAL(table2->get_object(ObjKey(123)).get<int64_t>(c0), 0);
        CHECK_EQUAL(table2->get_object(ObjKey(456)).get<int64_t>(c0), 1);
    }
    {
        WriteTransaction wt(sg_1);
        TableRef table1 = wt.get_table("table");
        table1->remove_object(ObjKey(123));
        table1->get_object(ObjKey(456)).set(c0, 7);
        CHECK_EQUAL(table1->size(), 1);
        wt.commit();
    }
    repl.replay_transacts(*sg_2, replay_logger);
    {
        ReadTransaction rt(sg_2);
        ConstTableRef table2 = rt.get_table("table");
        CHECK_THROW(table2->get_object(ObjKey(123)), InvalidKey);
        CHECK_EQUAL(table2->get_object(ObjKey(456)).get<int64_t>(c0), 7);
    }
}

#endif // TEST_REPLICATION
