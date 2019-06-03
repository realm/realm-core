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

    _impl::History* _get_history_write() override
    {
        return nullptr;
    }

    std::unique_ptr<_impl::History> _create_history_read() override
    {
        return {};
    }

    void do_initiate_transact(Group& group, version_type version, bool hist_updated) override
    {
        TrivialReplication::do_initiate_transact(group, version, hist_updated);
        m_group = &group;
    }

protected:
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
    Group* m_group;
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

    version_type prepare_changeset(const char*, size_t, version_type version) override
    {
        if (!m_arr) {
            using gf = _impl::GroupFriend;
            Allocator& alloc = gf::get_alloc(*m_group);
            m_arr = std::make_unique<BinaryColumn>(alloc);
            gf::prepare_history_parent(*m_group, *m_arr, hist_SyncClient, m_history_schema_version);
            m_arr->create();
            m_arr->add(BinaryData("Changeset"));
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

} // anonymous namespace

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

    // It should be possible to open a second db at the same path
    // while a WriteTransaction is active via another SharedGroup.
    DBRef sg_2 = DB::create(repl);
}


// This is to test that the exported file has no memory leaks
TEST(Replication_GroupWriteWithoutHistory)
{
    SHARED_GROUP_TEST_PATH(path);
    SHARED_GROUP_TEST_PATH(out1);
    SHARED_GROUP_TEST_PATH(out2);

    ReplSyncClient repl(path, 1);
    DBRef sg_1 = DB::create(repl);
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
        ReplSyncClient repl2(out2, 1);
        DBRef sg_2 = DB::create(repl2);
        ReadTransaction rt(sg_2);
        rt.get_group().verify();
    }
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
    DBRef sg_1 = DB::create(repl); // This will be the session initiator
    CHECK(repl.is_upgraded());
    WriteTransaction wt(sg_1);
    // When this one is opened, the file should have been upgraded
    // If this was not the case we would have triggered another upgrade
    // and the test would hang
    DBRef sg_2 = DB::create(repl);
}

TEST(Replication_WriteWithoutHistory)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);

    ReplSyncClient repl(path_1, 1);
    DBRef sg = DB::create(repl);
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

#endif // TEST_REPLICATION
