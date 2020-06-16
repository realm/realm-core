#include <memory>
#include <atomic>
#include <string>

#include <realm/util/features.h>
#include <realm/db.hpp>
#include <realm/sync/history.hpp>
#include <realm/sync/protocol.hpp>
#include <realm/sync/changeset_cooker.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/server_history.hpp>

#include "sync_fixtures.hpp"

#include "test.hpp"

using namespace std::literals::chrono_literals;
using namespace realm;
using namespace realm::test_util;
using namespace realm::fixtures;


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
// environment variable `UNITTEST_FILTER`. See `README.md` for more on
// this.
//
// Another way to debug a particular test, is to copy that test into
// `experiments/testcase.cpp` and then run `sh build.sh
// check-testcase` (or one of its friends) from the command line.


namespace {

TEST(Sync_CookedHistory_Basics)
{
    TEST_DIR(server_dir);
    SHARED_GROUP_TEST_PATH(client_path_1);
    SHARED_GROUP_TEST_PATH(client_path_2);
    SHARED_GROUP_TEST_PATH(client_path_3);

    using ChangesetCooker = ClientReplication::ChangesetCooker;
    std::shared_ptr<ChangesetCooker> cooker = std::make_shared<TrivialChangesetCooker>();

    // Produce a changeset
    {
        std::unique_ptr<ClientReplication> history = sync::make_client_replication(client_path_1);
        DBRef sg = DB::create(*history);
        WriteTransaction wt{sg};
        sync::create_table(wt, "class_Table");
        wt.commit();
    }

    // Check that the cooked progress starts out as zero
    {
        std::unique_ptr<ClientReplication> history = sync::make_client_replication(client_path_1);
        DBRef sg = DB::create(*history);
        auto progress = history->get_cooked_progress();
        CHECK_EQUAL(0, progress.changeset_index);
        CHECK_EQUAL(0, progress.intrachangeset_progress);
    }
    {
        std::unique_ptr<ClientReplication> history = sync::make_client_replication(client_path_2);
        DBRef sg = DB::create(*history);
        auto progress = history->get_cooked_progress();
        CHECK_EQUAL(0, progress.changeset_index);
        CHECK_EQUAL(0, progress.intrachangeset_progress);
    }
    {
        std::unique_ptr<ClientReplication> history = sync::make_client_replication(client_path_3);
        DBRef sg = DB::create(*history);
        auto progress = history->get_cooked_progress();
        CHECK_EQUAL(0, progress.changeset_index);
        CHECK_EQUAL(0, progress.intrachangeset_progress);
    }

    // Check that there are no cooked changesets available yet
    {
        std::unique_ptr<ClientReplication> history = sync::make_client_replication(client_path_1);
        DBRef sg = DB::create(*history);
        CHECK_EQUAL(0, history->get_num_cooked_changesets());
    }
    {
        std::unique_ptr<ClientReplication> history = sync::make_client_replication(client_path_2);
        DBRef sg = DB::create(*history);
        CHECK_EQUAL(0, history->get_num_cooked_changesets());
    }
    {
        std::unique_ptr<ClientReplication> history = sync::make_client_replication(client_path_3);
        DBRef sg = DB::create(*history);
        CHECK_EQUAL(0, history->get_num_cooked_changesets());
    }

    // Download without a changeset cooker, such that no cooked changesets are
    // produced
    {
        ClientServerFixture fixture{server_dir, test_context};
        fixture.start();

        // Upload from client file #1 to server
        {
            Session::Config config;
            config.changeset_cooker = cooker;
            Session session = fixture.make_bound_session(client_path_1, "/test", config);
            session.wait_for_upload_complete_or_client_stopped();
        }

        // Download from server to client file #2 not using a cooker
        {
            Session session = fixture.make_bound_session(client_path_2, "/test");
            session.wait_for_download_complete_or_client_stopped();
        }
    }

    // Check that there are still no cooked changesets avaialble
    {
        std::unique_ptr<ClientReplication> history = sync::make_client_replication(client_path_2);
        DBRef sg = DB::create(*history);
        CHECK_EQUAL(0, history->get_num_cooked_changesets());
    }

    // Produce another changeset
    {
        std::unique_ptr<ClientReplication> history = sync::make_client_replication(client_path_1);
        DBRef sg = DB::create(*history);
        WriteTransaction wt{sg};
        sync::create_table(wt, "class_Table2");
        wt.commit();
    }

    // Produce a cooked changeset in client file #3
    {
        ClientServerFixture fixture{server_dir, test_context};
        fixture.start();

        // Upload from client file #1 to server
        {
            Session::Config config;
            config.changeset_cooker = cooker;
            Session session = fixture.make_bound_session(client_path_1, "/test", config);
            session.wait_for_upload_complete_or_client_stopped();
        }

        // Download from server to client file #3
        {
            Session::Config config;
            config.changeset_cooker = cooker;
            Session session = fixture.make_bound_session(client_path_3, "/test", config);
            session.wait_for_download_complete_or_client_stopped();
        }
    }

    // Check that the cooked progress is still zero, since we didn't change it
    {
        std::unique_ptr<ClientReplication> history = sync::make_client_replication(client_path_1);
        DBRef sg = DB::create(*history);
        auto progress = history->get_cooked_progress();
        CHECK_EQUAL(0, progress.changeset_index);
        CHECK_EQUAL(0, progress.intrachangeset_progress);
    }
    {
        std::unique_ptr<ClientReplication> history = sync::make_client_replication(client_path_3);
        DBRef sg = DB::create(*history);
        auto progress = history->get_cooked_progress();
        CHECK_EQUAL(0, progress.changeset_index);
        CHECK_EQUAL(0, progress.intrachangeset_progress);
    }

    // Check that exactly two cooked changeset were produced, and advance the
    // point of progress of the cooked consumption
    {
        std::unique_ptr<ClientReplication> history = sync::make_client_replication(client_path_3);
        DBRef sg = DB::create(*history);
        auto progress = history->get_cooked_progress();
        auto n = history->get_num_cooked_changesets();
        if (CHECK_EQUAL(2, n) && CHECK_EQUAL(0, progress.changeset_index)) {
            ClientReplication::CookedProgress progress;
            progress.changeset_index = 2;
            history->set_cooked_progress(progress);
        }
    }

    // Check that there are still no cooked changesets available through client file #1
    {
        std::unique_ptr<ClientReplication> history = sync::make_client_replication(client_path_1);
        DBRef sg = DB::create(*history);
        auto progress = history->get_cooked_progress();
        auto n = history->get_num_cooked_changesets();
        CHECK_EQUAL(0, n - progress.changeset_index);
    }

    // Produce and cook a 3rd changeset
    {
        std::unique_ptr<ClientReplication> history = sync::make_client_replication(client_path_1);
        DBRef sg = DB::create(*history);
        WriteTransaction wt{sg};
        sync::create_table(wt, "class_Table3");
        wt.commit();
    }
    {
        ClientServerFixture fixture{server_dir, test_context};
        fixture.start();

        // Upload from client file #1 to server
        {
            Session::Config config;
            config.changeset_cooker = cooker;
            Session session = fixture.make_bound_session(client_path_1, "/test", config);
            session.wait_for_upload_complete_or_client_stopped();
        }

        // Download from server to client file #3
        {
            Session::Config config;
            config.changeset_cooker = cooker;
            Session session = fixture.make_bound_session(client_path_3, "/test", config);
            session.wait_for_download_complete_or_client_stopped();
        }
    }

    // Check that exactly one new cooked changeset was produced
    {
        std::unique_ptr<ClientReplication> history = sync::make_client_replication(client_path_3);
        DBRef sg = DB::create(*history);
        auto progress = history->get_cooked_progress();
        auto n = history->get_num_cooked_changesets();
        CHECK_EQUAL(3, n);
        CHECK_EQUAL(2, progress.changeset_index);
    }

    // Produce a 4th changeset via client file #3
    {
        std::unique_ptr<ClientReplication> history = sync::make_client_replication(client_path_3);
        DBRef sg = DB::create(*history);
        WriteTransaction wt{sg};
        sync::create_table(wt, "class_Table4");
        wt.commit();
    }

    // Check that there is no new cooked changeset
    {
        std::unique_ptr<ClientReplication> history = sync::make_client_replication(client_path_3);
        DBRef sg = DB::create(*history);
        auto progress = history->get_cooked_progress();
        auto n = history->get_num_cooked_changesets();
        CHECK_EQUAL(3, n);
        CHECK_EQUAL(2, progress.changeset_index);
    }

    // Produce a cooked changeset in client file #1
    {
        ClientServerFixture fixture{server_dir, test_context};
        fixture.start();

        // Upload from client file #3 to server
        {
            Session::Config config;
            config.changeset_cooker = cooker;
            Session session = fixture.make_bound_session(client_path_3, "/test", config);
            session.wait_for_upload_complete_or_client_stopped();
        }

        // Download from server to client file #1
        {
            Session::Config config;
            config.changeset_cooker = cooker;
            Session session = fixture.make_bound_session(client_path_1, "/test", config);
            session.wait_for_download_complete_or_client_stopped();
        }
    }

    // Check that exactly one cooked changeset was produced
    {
        std::unique_ptr<ClientReplication> history = sync::make_client_replication(client_path_1);
        DBRef sg = DB::create(*history);
        auto progress = history->get_cooked_progress();
        auto n = history->get_num_cooked_changesets();
        CHECK_EQUAL(1, n);
        CHECK_EQUAL(0, progress.changeset_index);
    }
}


TEST(Sync_CookedHistory_2)
{
    TEST_DIR(server_dir);
    SHARED_GROUP_TEST_PATH(client_path_1);
    SHARED_GROUP_TEST_PATH(client_path_2);

    ClientServerFixture fixture{server_dir, test_context};
    fixture.start();

    using ChangesetCooker = ClientReplication::ChangesetCooker;
    std::shared_ptr<ChangesetCooker> cooker = std::make_shared<TrivialChangesetCooker>();

    {
        std::unique_ptr<ClientReplication> history = sync::make_client_replication(client_path_1);
        DBRef sg = DB::create(*history);
        WriteTransaction wt_1{sg};
        sync::create_table(wt_1, "class_Table1");
        wt_1.commit();
        WriteTransaction wt_2{sg};
        sync::create_table(wt_2, "class_Table2");
        wt_2.commit();
        WriteTransaction wt_3{sg};
        sync::create_table(wt_3, "class_Table3");
        wt_3.commit();
        WriteTransaction wt_4{sg};
        sync::create_table(wt_4, "class_Table4");
        wt_4.commit();

        Session::Config config;
        Session session = fixture.make_session(client_path_1, config);
        fixture.bind_session(session, "/test");
        session.wait_for_upload_complete_or_client_stopped();
    }

    {
        Session::Config config;
        config.changeset_cooker = cooker;
        Session session = fixture.make_bound_session(client_path_2, "/test", config);
        session.wait_for_download_complete_or_client_stopped();
    }

    {
        std::unique_ptr<ClientReplication> history = sync::make_client_replication(client_path_1);
        DBRef sg = DB::create(*history);

        WriteTransaction wt_5{sg};
        sync::create_table(wt_5, "class_Table5");
        wt_5.commit();
        WriteTransaction wt_6{sg};
        sync::create_table(wt_6, "class_Table6");
        wt_6.commit();

        Session::Config config;
        Session session = fixture.make_session(client_path_1, config);
        fixture.bind_session(session, "/test");
        session.wait_for_upload_complete_or_client_stopped();
    }

    {
        Session::Config config;
        config.changeset_cooker = cooker;
        Session session = fixture.make_bound_session(client_path_2, "/test", config);
        session.wait_for_download_complete_or_client_stopped();
    }
}

#ifndef REALM_PLATFORM_WIN32

TEST(Sync_CookedHistory_LargeChangeset)
{
    TEST_DIR(server_dir);
    SHARED_GROUP_TEST_PATH(client_path_1);
    SHARED_GROUP_TEST_PATH(client_path_2);

    ClientServerFixture fixture{server_dir, test_context};
    fixture.start();

    using ChangesetCooker = ClientReplication::ChangesetCooker;
    std::shared_ptr<ChangesetCooker> cooker = std::make_shared<TrivialChangesetCooker>();

    // Create enough data that our changeset cannot be stored contiguously by BinaryColumn (> 16MB).
    constexpr size_t data_size = 8 * 1024 * 1024;
    constexpr size_t data_count = 4;
    constexpr size_t total_data_size = data_size * data_count;

    {
        std::unique_ptr<ClientReplication> history = sync::make_client_replication(client_path_1);
        DBRef sg = DB::create(*history);
        WriteTransaction wt{sg};
        auto table = sync::create_table(wt, "class_Table");
        auto col_data = table->add_column(type_Binary, "data");

        {
            std::string data(data_size, '\0');
            for (size_t row = 0; row < data_count; ++row) {
                table->create_object().set(col_data, BinaryData{data.data(), data.size()});
            }
        }

        wt.commit();

        Session::Config config;
        Session session = fixture.make_session(client_path_1, config);
        fixture.bind_session(session, "/test");
        session.wait_for_upload_complete_or_client_stopped();
    }

    {
        Session::Config config;
        config.changeset_cooker = cooker;
        Session session = fixture.make_bound_session(client_path_2, "/test", config);
        session.wait_for_download_complete_or_client_stopped();
    }

    {
        std::unique_ptr<ClientReplication> history = sync::make_client_replication(client_path_2);
        DBRef sg = DB::create(*history);
        auto progress = history->get_cooked_progress();
        CHECK_EQUAL(0, progress.changeset_index);
        CHECK_EQUAL(0, progress.intrachangeset_progress);

        auto available = history->get_num_cooked_changesets();
        CHECK_EQUAL(1, available);

        util::AppendBuffer<char> changeset;
        version_type server_version = 0;
        history->get_cooked_changeset(0, changeset, server_version);

        // The changeset we receive must be at least as large as the size of the
        // data we stored.
        CHECK_GREATER(changeset.size(), total_data_size);

        // A version produced by a changeset can never be zero
        CHECK_GREATER(server_version, 0);
    }
}

#endif // REALM_PLATFORM_WIN32


TEST(Sync_CookedHistory_RestrictsServerSideHistoryCompaction)
{
    TEST_DIR(server_dir);
    SHARED_GROUP_TEST_PATH(client_path_1);
    SHARED_GROUP_TEST_PATH(client_path_2);

    std::string virt_path = "/test";

    class ServerHistoryContext : public _impl::ServerHistory::Context {
    public:
        bool owner_is_sync_server() const noexcept override
        {
            return true;
        }
        std::mt19937_64& server_history_get_random() noexcept override
        {
            return m_random;
        }

    private:
        std::mt19937_64 m_random;
    };

    ServerHistoryContext server_history_context;
    _impl::ServerHistory::DummyCompactionControl compaction_control;
    FakeClock compaction_clock;
    ClientServerFixture::Config fixture_config;
    fixture_config.history_ttl = 60s;
    fixture_config.history_compaction_interval = 1s;
    fixture_config.disable_upload_compaction = true;
    fixture_config.history_compaction_clock = &compaction_clock;

    using ChangesetCooker = ClientReplication::ChangesetCooker;
    std::shared_ptr<ChangesetCooker> cooker = std::make_shared<TrivialChangesetCooker>();

    auto transact = [](DBRef sg) {
        WriteTransaction wt{sg};
        TableRef table = wt.get_table("class_Foo");
        if (!table) {
            table = sync::create_table(wt, "class_Foo");
            table->add_column(type_Int, "i");
        }
        table->create_object();
        wt.commit();
    };

    auto produce_changeset_to_be_cooked = [&] {
        _impl::ClientHistoryImpl history{client_path_1};
        DBRef sg = DB::create(history);
        transact(sg);
        ClientServerFixture fixture{server_dir, test_context, fixture_config};
        fixture.start();
        Session session = fixture.make_bound_session(client_path_1, virt_path);
        session.wait_for_upload_complete_or_client_stopped();
    };

    auto pull_changesets_from_server_and_cook = [&] {
        ClientServerFixture fixture{server_dir, test_context, fixture_config};
        fixture.start();
        Session::Config config;
        config.changeset_cooker = cooker;
        Session session = fixture.make_bound_session(client_path_2, virt_path, config);
        session.wait_for_download_complete_or_client_stopped();
        _impl::ClientHistoryImpl history{client_path_2};
        DBRef sg = DB::create(history);
        return history.get_num_cooked_changesets();
    };

    auto push_cooked_progress_to_server = [&] {
        _impl::ClientHistoryImpl history{client_path_2};
        DBRef sg = DB::create(history);
        transact(sg);
        compaction_clock.add_time(2s);
        ClientServerFixture fixture{server_dir, test_context, fixture_config};
        fixture.start();
        Session::Config config;
        config.changeset_cooker = cooker;
        Session session = fixture.make_bound_session(client_path_2, virt_path, config);
        session.wait_for_upload_complete_or_client_stopped();
    };

    auto advance_cooked_progress = [&](std::int_fast64_t changeset_index) {
        _impl::ClientHistoryImpl history{client_path_2};
        DBRef sg = DB::create(history);
        ClientReplication::CookedProgress progress = history.get_cooked_progress();
        REALM_ASSERT(changeset_index > progress.changeset_index);
        util::AppendBuffer<char> buffer; // Dummy
        version_type released_server_version;
        history.get_cooked_changeset(changeset_index - 1, buffer, released_server_version);
        progress.changeset_index = changeset_index;
        history.set_cooked_progress(progress);
        return released_server_version;
    };

    auto get_compacted_until = [&] {
        ClientServerFixture fixture{server_dir, test_context, fixture_config};
        std::string real_path = fixture.map_virtual_to_real_path(virt_path);
        _impl::ServerHistory history{real_path, server_history_context, compaction_control};
        DBRef sg = DB::create(history);
        return history.get_compacted_until_version();
    };

    CHECK_EQUAL(0, get_compacted_until());
    produce_changeset_to_be_cooked();
    produce_changeset_to_be_cooked();
    std::int_fast64_t num_cooked_changesets_1 = pull_changesets_from_server_and_cook();
    CHECK_EQUAL(2, num_cooked_changesets_1);
    push_cooked_progress_to_server();
    CHECK_EQUAL(0, get_compacted_until());
    std::int_fast64_t num_cooked_changesets_3 = pull_changesets_from_server_and_cook();
    CHECK_EQUAL(2, num_cooked_changesets_3);
    version_type released_server_version_1 = advance_cooked_progress(1);
    push_cooked_progress_to_server();
    CHECK_EQUAL(released_server_version_1, get_compacted_until());
    produce_changeset_to_be_cooked();
    produce_changeset_to_be_cooked();
    CHECK_EQUAL(released_server_version_1, get_compacted_until());
    std::int_fast64_t num_cooked_changesets_2 = pull_changesets_from_server_and_cook();
    CHECK_EQUAL(4, num_cooked_changesets_2);
    CHECK_EQUAL(released_server_version_1, get_compacted_until());
    version_type released_server_version_2 = advance_cooked_progress(2);
    push_cooked_progress_to_server();
    CHECK_EQUAL(released_server_version_2, get_compacted_until());
    version_type released_server_version_3 = advance_cooked_progress(3);
    push_cooked_progress_to_server();
    CHECK_EQUAL(released_server_version_3, get_compacted_until());
}


// FIXME: Disabled due to migration bug in Core re: embedded objects.
TEST_IF(Sync_CookedHistory_MigrationFromSchemaVersion1, false)
{
    SHARED_GROUP_TEST_PATH(client_path);
    TEST_DIR(server_dir);

    std::string virtual_path = "/test";
    std::string server_path;
    {
        fixtures::ClientServerFixture fixture{server_dir, test_context};
        server_path = fixture.map_virtual_to_real_path(virtual_path);
    }

    std::string resources_dir = "resources";
    std::string resources_subdir = util::File::resolve("cooked_migration", resources_dir);

    std::string origin_client_path = util::File::resolve("client_schema_version_1.realm", resources_subdir);

    std::string origin_server_path = util::File::resolve("server.realm", resources_subdir);

    util::File::copy(origin_client_path, client_path);
    util::File::copy(origin_server_path, server_path);

    // Verify that client file uses schema version 1
    {
        /*
         *      In core-6 you cannot open a file without upgrading it.
         *      so opening in RO mode is not possible
         *
                Group group{client_path};
                using gf = _impl::GroupFriend;
                Allocator& alloc = gf::get_alloc(group);
                ref_type top_ref = gf::get_top_ref(group);
                _impl::History::version_type version; // Dummy
                int history_type = 0;
                int history_schema_version = 0;
                gf::get_version_and_history_info(alloc, top_ref, version, history_type,
                                                 history_schema_version);
                REALM_ASSERT(history_type == Replication::hist_SyncClient);
                REALM_ASSERT(history_schema_version == 1);
        */
    }

    // Migrate client file, and verify constitution of cooked history
    sync::SyncProgress sync_progress;
    {
        _impl::ClientHistoryImpl history{client_path};
        DBRef sg = DB::create(history);
        {
            ReadTransaction rt{sg};
            rt.get_group().verify();
        }
        sync::version_type server_version = 0;
        std::int_fast64_t num_changesets = 0;
        sync::ClientReplication::CookedProgress progress;
        std::int_fast64_t num_skipped_changesets = 0;
        history.get_cooked_status(server_version, num_changesets, progress, num_skipped_changesets);
        CHECK_EQUAL(4, num_changesets);
        CHECK_EQUAL(2, progress.changeset_index);
        CHECK_EQUAL(5, progress.intrachangeset_progress);
        CHECK_EQUAL(0, num_skipped_changesets);
        CHECK_EQUAL(num_changesets, history.get_num_cooked_changesets());
        sync::ClientReplication::CookedProgress progress_2 = history.get_cooked_progress();
        CHECK_EQUAL(progress.changeset_index, progress_2.changeset_index);
        CHECK_EQUAL(progress.intrachangeset_progress, progress_2.intrachangeset_progress);

        // Try skip until the server version that is set as base server version
        // during migration and see that it is still the case that nothing is
        // skipped
        sync::version_type current_client_version; // Dummy
        sync::SaltedFileIdent client_file_ident;   // Dummy
        history.get_status(current_client_version, client_file_ident, sync_progress);
        server_version = sync_progress.download.server_version;
        history.get_cooked_status(server_version, num_changesets, progress, num_skipped_changesets);
        CHECK_EQUAL(4, num_changesets);
        CHECK_EQUAL(2, progress.changeset_index);
        CHECK_EQUAL(5, progress.intrachangeset_progress);
        CHECK_EQUAL(0, num_skipped_changesets);

        // Check that migration stores zero for server version
        util::AppendBuffer<char> buffer;
        history.get_cooked_changeset(2, buffer, server_version);
        CHECK_EQUAL(0, server_version);
        history.get_cooked_changeset(3, buffer, server_version);
        CHECK_EQUAL(0, server_version);

        // Consume one cooked changeset, then confirm cooked status
        progress.changeset_index = 3;
        progress.intrachangeset_progress = 1;
        history.set_cooked_progress(progress);
        server_version = 0;
        history.get_cooked_status(server_version, num_changesets, progress, num_skipped_changesets);
        CHECK_EQUAL(4, num_changesets);
        CHECK_EQUAL(3, progress.changeset_index);
        CHECK_EQUAL(1, progress.intrachangeset_progress);
        CHECK_EQUAL(0, num_skipped_changesets);
        server_version = sync_progress.download.server_version;
        history.get_cooked_status(server_version, num_changesets, progress, num_skipped_changesets);
        CHECK_EQUAL(4, num_changesets);
        CHECK_EQUAL(3, progress.changeset_index);
        CHECK_EQUAL(1, progress.intrachangeset_progress);
        CHECK_EQUAL(0, num_skipped_changesets);
    }

    // Produce two new cooked changesets, and verify constitution of cooked
    // history
    auto transact = [](DBRef sg) {
        WriteTransaction wt{sg};
        TableRef table = wt.get_table("class_Foo");
        if (!table) {
            table = sync::create_table(wt, "class_Foo");
            table->add_column(type_Int, "i");
        }
        table->create_object();
        return wt.commit();
    };
    {
        fixtures::ClientServerFixture fixture{server_dir, test_context};
        fixture.start();
        Session::Config config;
        config.changeset_cooker = std::make_shared<TrivialChangesetCooker>();
        Session cooking_session = fixture.make_bound_session(client_path, virtual_path, config);
        SHARED_GROUP_TEST_PATH(other_client_path);
        Session other_session = fixture.make_bound_session(other_client_path, "/test");
        _impl::ClientHistoryImpl history{other_client_path};
        DBRef sg = DB::create(history);
        for (int i = 0; i < 2; ++i) {
            version_type version = transact(sg);
            other_session.nonsync_transact_notify(version);
            other_session.wait_for_upload_complete_or_client_stopped();
            cooking_session.wait_for_download_complete_or_client_stopped();
        }
    }
    {
        _impl::ClientHistoryImpl history{client_path};
        DBRef sg = DB::create(history);
        {
            ReadTransaction rt{sg};
            rt.get_group().verify();
        }
        sync::version_type server_version = 0;
        std::int_fast64_t num_changesets = 0;
        sync::ClientReplication::CookedProgress progress;
        std::int_fast64_t num_skipped_changesets = 0;
        history.get_cooked_status(server_version, num_changesets, progress, num_skipped_changesets);
        CHECK_EQUAL(6, num_changesets);
        CHECK_EQUAL(3, progress.changeset_index);
        CHECK_EQUAL(1, progress.intrachangeset_progress);
        CHECK_EQUAL(0, num_skipped_changesets);
        server_version = sync_progress.download.server_version;
        history.get_cooked_status(server_version, num_changesets, progress, num_skipped_changesets);
        CHECK_EQUAL(6, num_changesets);
        CHECK_EQUAL(3, progress.changeset_index);
        CHECK_EQUAL(1, progress.intrachangeset_progress);
        CHECK_EQUAL(0, num_skipped_changesets);

        // Consume the last old cooked changeset, then confirm cooked status
        progress.changeset_index = 4;
        progress.intrachangeset_progress = 9;
        history.set_cooked_progress(progress);
        server_version = 0;
        history.get_cooked_status(server_version, num_changesets, progress, num_skipped_changesets);
        CHECK_EQUAL(6, num_changesets);
        CHECK_EQUAL(4, progress.changeset_index);
        CHECK_EQUAL(9, progress.intrachangeset_progress);
        CHECK_EQUAL(0, num_skipped_changesets);
        server_version = sync_progress.download.server_version;
        history.get_cooked_status(server_version, num_changesets, progress, num_skipped_changesets);
        CHECK_EQUAL(6, num_changesets);
        CHECK_EQUAL(4, progress.changeset_index);
        CHECK_EQUAL(9, progress.intrachangeset_progress);
        CHECK_EQUAL(0, num_skipped_changesets);

        // Verify that new changesets specify nonzero server version
        version_type server_version_1 = 0, server_version_2 = 0;
        util::AppendBuffer<char> buffer;
        history.get_cooked_changeset(4, buffer, server_version_1);
        history.get_cooked_changeset(5, buffer, server_version_2);
        CHECK_GREATER(server_version_1, sync_progress.download.server_version);
        CHECK_GREATER(server_version_2, server_version_1);

        // Confirm that one cannot specify a server version that was never
        // associated with a cooked changeset
        server_version = server_version_2 + 1;
        CHECK_THROW(history.get_cooked_status(server_version, num_changesets, progress, num_skipped_changesets),
                    sync::BadCookedServerVersion);

        // Try skip one, then two unconsumed changesets
        server_version = server_version_1;
        history.get_cooked_status(server_version, num_changesets, progress, num_skipped_changesets);
        CHECK_EQUAL(6, num_changesets);
        CHECK_EQUAL(5, progress.changeset_index);
        CHECK_EQUAL(0, progress.intrachangeset_progress);
        CHECK_EQUAL(1, num_skipped_changesets);
        server_version = server_version_2;
        history.get_cooked_status(server_version, num_changesets, progress, num_skipped_changesets);
        CHECK_EQUAL(6, num_changesets);
        CHECK_EQUAL(6, progress.changeset_index);
        CHECK_EQUAL(0, progress.intrachangeset_progress);
        CHECK_EQUAL(2, num_skipped_changesets);

        // Consume one newly cooked changeset, then confirm cooked status
        progress.changeset_index = 5;
        progress.intrachangeset_progress = 2;
        history.set_cooked_progress(progress);
        server_version = 0;
        history.get_cooked_status(server_version, num_changesets, progress, num_skipped_changesets);
        CHECK_EQUAL(6, num_changesets);
        CHECK_EQUAL(5, progress.changeset_index);
        CHECK_EQUAL(2, progress.intrachangeset_progress);
        CHECK_EQUAL(0, num_skipped_changesets);
        server_version = server_version_1;
        history.get_cooked_status(server_version, num_changesets, progress, num_skipped_changesets);
        CHECK_EQUAL(6, num_changesets);
        CHECK_EQUAL(5, progress.changeset_index);
        CHECK_EQUAL(2, progress.intrachangeset_progress);
        CHECK_EQUAL(0, num_skipped_changesets);

        // Confirm that one cannot specify a server version earlier than the one
        // associated with the last consumed cooked changeset
        server_version = sync_progress.download.server_version;
        CHECK_THROW(history.get_cooked_status(server_version, num_changesets, progress, num_skipped_changesets),
                    sync::BadCookedServerVersion);
    }
}

} // unnamed namespace
