#include <thread>

#include <realm/sync/object.hpp>
#include <realm/sync/noinst/server_history.hpp>

#include "test.hpp"

#include "util/semaphore.hpp"
#include "sync_fixtures.hpp"


using namespace realm;
using namespace realm::sync;
using namespace realm::test_util;
using namespace realm::fixtures;

using namespace std::literals::chrono_literals;

namespace {

class ServerHistoryContext : public _impl::ServerHistory::Context {
public:
    std::mt19937_64& server_history_get_random() noexcept override
    {
        return m_random;
    }

private:
    std::mt19937_64 m_random;
};


TEST(Sync_ServerHistoryCompaction_Basic)
{
    TEST_DIR(dir);

    FakeClock clock{1s};

    ClientServerFixture::Config config;
    config.history_ttl = 15s;
    config.history_compaction_interval = 1s;
    config.history_compaction_clock = &clock;

    ClientServerFixture fixture{dir, test_context, config};

    fixture.start();

    SHARED_GROUP_TEST_PATH(client_1_path);
    SHARED_GROUP_TEST_PATH(client_2_path);
    auto sg_1 = DB::create(make_client_replication(client_1_path));
    auto sg_2 = DB::create(make_client_replication(client_2_path));

    std::atomic<bool> did_fail{false};
    std::atomic<bool> did_expire{false};

    auto handler = [&](std::error_code ec, bool, const std::string&) {
        did_fail = true;
        if (ec == ProtocolError::client_file_expired)
            did_expire = true;
        fixture.stop();
    };
    fixture.set_client_side_error_handler(handler);

    // Use client 1 to introduce the first entry into the server-side
    // history.
    {
        WriteTransaction wt{sg_1};
        sync::create_table(wt, "class_Foo");
        wt.commit();
        Session session = fixture.make_bound_session(sg_1, "/test");
        session.wait_for_upload_complete_or_client_stopped();
        session.wait_for_download_complete_or_client_stopped();
    }

    // Make client 2 upload a changeset. This leaves client 2's
    // synchronization progress at server version 1 or 2 depending on timing,
    // and its last seen timestamp at 1s.
    {
        WriteTransaction wt{sg_2};
        sync::create_table(wt, "class_Bar");
        wt.commit();
        Session session = fixture.make_bound_session(sg_2, "/test");
        session.wait_for_upload_complete_or_client_stopped();
        session.wait_for_download_complete_or_client_stopped();
    }

    // Use client 1 to flush out any remining server-side activity pertaining to
    // client 2 before advancing the clock.
    {
        WriteTransaction wt{sg_1};
        TableRef foo = wt.get_table("class_Foo");
        foo->create_object();
        wt.commit();
        Session session = fixture.make_bound_session(sg_1, "/test");
        session.wait_for_upload_complete_or_client_stopped();
    }

    // Use client 1 to introduce a new changeset, such that we can make
    // compaction progress beyond client 2's position. Also start advancing the
    // clock. Note that it cannot be advanced enough in one step to expire
    // client 2, because that would also expire client 1.
    clock.add_time(10s); // -> 1s -> 11s
    {
        WriteTransaction wt{sg_1};
        TableRef foo = wt.get_table("class_Foo");
        foo->create_object();
        wt.commit();
        Session session = fixture.make_bound_session(sg_1, "/test");
        session.wait_for_upload_complete_or_client_stopped();
    }

    // Advance the clock enough to expire client 2, and trigger compaction by
    // using client 1 to upload another changeset.
    clock.add_time(10s); // -> 11s -> 21s
    {
        WriteTransaction wt{sg_1};
        TableRef foo = wt.get_table("class_Foo");
        foo->create_object();
        wt.commit();
        Session session = fixture.make_bound_session(sg_1, "/test");
        session.wait_for_upload_complete_or_client_stopped();
    }

    CHECK(!did_fail);
    CHECK(!did_expire);

    // Attempt to reconnect with client 2, and thereby trigger a failure due to
    // expired client file entry in server-side file.
    {
        Session session = fixture.make_bound_session(sg_2, "/test");
        session.wait_for_upload_complete_or_client_stopped();
    }

    CHECK(did_fail);
    CHECK(did_expire);
}


TEST(Sync_ServerHistoryCompaction_ExpiredAtDownloadTime)
{
    TEST_DIR(dir);
    SHARED_GROUP_TEST_PATH(client_1_path);
    SHARED_GROUP_TEST_PATH(client_2_path);

    bool did_fail = false;
    bool did_expire = false;
    {
        FakeClock clock{1s};
        ClientServerFixture::Config fixture_config;
        fixture_config.history_ttl = 15s;
        fixture_config.history_compaction_interval = 1s;
        fixture_config.history_compaction_clock = &clock;

        ClientServerFixture fixture{dir, test_context, fixture_config};
        fixture.start();

        using ConnectionState = Session::ConnectionState;
        using ErrorInfo = Session::ErrorInfo;
        auto listener_1 = [&](ConnectionState, const ErrorInfo* info) {
            if (info) {
                did_fail = true;
                if (info->error_code == ProtocolError::client_file_expired)
                    did_expire = true;
                fixture.stop();
            }
        };
        auto listener_2 = [&](ConnectionState, const ErrorInfo*) {};

        // Set up client 1 for continuous download
        Session::Config session_config;
        session_config.disable_upload = true;
        Session session_1 = fixture.make_session(client_1_path, session_config);
        session_1.set_connection_state_change_listener(listener_1);
        fixture.bind_session(session_1, "/test");
        session_1.wait_for_download_complete_or_client_stopped();

        // Use client 2 to push changeset that expires client 1
        {
            auto history = make_client_replication(client_2_path);
            auto sg = DB::create(*history);
            WriteTransaction wt{sg};
            sync::create_table(wt, "class_Foo");
            wt.commit();
        }
        clock.add_time(100s);
        Session session_2 = fixture.make_session(client_2_path);
        session_2.set_connection_state_change_listener(listener_2);
        fixture.bind_session(session_2, "/test");
        session_2.wait_for_upload_complete_or_client_stopped();

        // Wait for the failure to occur
        session_1.wait_for_download_complete_or_client_stopped();
    }
    CHECK(did_fail);
    CHECK(did_expire);
}


TEST(Sync_ServerHistoryCompaction_ExpiredAtUploadTime)
{
    TEST_DIR(dir);
    SHARED_GROUP_TEST_PATH(client_1_path);
    SHARED_GROUP_TEST_PATH(client_2_path);

    FakeClock clock{1s};
    ClientServerFixture::Config fixture_config;
    fixture_config.history_ttl = 15s;
    fixture_config.history_compaction_interval = 1s;
    fixture_config.history_compaction_clock = &clock;
    fixture_config.disable_upload_activation_delay = true;

    // Register client 1 with server and retreive its client file identifier
    file_ident_type client_1_file_ident;
    {
        ClientServerFixture fixture{dir, test_context, fixture_config};
        fixture.start();
        auto history = make_client_replication(client_1_path);
        auto& history_local = *history;
        auto sg = DB::create(std::move(history));
        Session session = fixture.make_bound_session(sg, "/test");
        session.wait_for_download_complete_or_client_stopped();
        version_type current_client_version;
        SaltedFileIdent client_file_ident;
        SyncProgress progress;
        history_local.get_status(current_client_version, client_file_ident, progress);
        client_1_file_ident = client_file_ident.ident;
    }

    BowlOfStonesSemaphore bowl;
    fixture_config.server_disable_download_for = {client_1_file_ident};
    fixture_config.server_session_bootstrap_callback = [&](util::StringView virt_path,
                                                           file_ident_type client_file_ident) {
        if (virt_path == "/test" && client_file_ident == client_1_file_ident)
            bowl.add_stone();
    };
    ClientServerFixture fixture{dir, test_context, fixture_config};
    fixture.start();

    std::atomic<bool> did_fail{false};
    std::atomic<bool> did_expire{false};

    using ConnectionState = Session::ConnectionState;
    using ErrorInfo = Session::ErrorInfo;
    auto listener_1 = [&](ConnectionState, const ErrorInfo* info) {
        if (info) {
            did_fail = true;
            if (info->error_code == ProtocolError::client_file_expired)
                did_expire = true;
            fixture.stop();
        }
    };

    Session::Config session_config;
    session_config.disable_empty_upload = true;
    Session session_1 = fixture.make_session(client_1_path, session_config);
    session_1.set_connection_state_change_listener(listener_1);
    fixture.bind_session(session_1, "/test");

    // Wait for client 1 to have been bootstrapped on the server
    bowl.get_stone();

    CHECK(!did_fail);
    CHECK(!did_expire);

    // Use client 2 to push a changeset that expires client 1
    {
        auto sg = DB::create(make_client_replication(client_2_path));
        WriteTransaction wt{sg};
        sync::create_table(wt, "class_Foo");
        wt.commit();
    }
    clock.add_time(100s);
    Session session_2 = fixture.make_session(client_2_path);
    auto listener_2 = [&](ConnectionState, const ErrorInfo*) {};
    session_2.set_connection_state_change_listener(listener_2);
    fixture.bind_session(session_2, "/test");
    session_2.wait_for_upload_complete_or_client_stopped();

    CHECK(!did_fail);
    CHECK(!did_expire);

    // Now use client 1 to trigger an error due to expiration at upload time
    {
        auto sg = DB::create(make_client_replication(client_1_path));
        WriteTransaction wt{sg};
        sync::create_table(wt, "class_Bar");
        version_type version = wt.commit();
        session_1.nonsync_transact_notify(version);
    }
    // The client never figures out that upload has completed, because donwload
    // is disabled, the expiration error will cause the client to be stopped.
    session_1.wait_for_upload_complete_or_client_stopped();

    CHECK(did_fail);
    CHECK(did_expire);
}


TEST(Sync_ServerHistoryCompaction_Old)
{
    TEST_DIR(dir);

    FakeClock clock;

    ClientServerFixture::Config config;
    config.history_ttl = 1s;
    config.history_compaction_interval = 1s;
    config.disable_upload_compaction = true;
    config.history_compaction_clock = &clock;

    ClientServerFixture fixture{dir, test_context, config};

    fixture.start();

    SHARED_GROUP_TEST_PATH(client_1_path);
    SHARED_GROUP_TEST_PATH(client_2_path);

    bool client_reset_did_happen = false;
    fixture.set_client_side_error_handler([&](std::error_code ec, bool fatal, const std::string& message) {
        static_cast<void>(fatal);
        static_cast<void>(message);
        if (ec == ProtocolError::client_file_expired) {
            client_reset_did_happen = true;
        }
        fixture.stop();
    });

    DBRef sg_1 = DB::create(make_client_replication(client_1_path));
    Session client_1 = fixture.make_bound_session(sg_1, "/test");
    {
        WriteTransaction wt{sg_1};
        sync::create_table(wt, "class_Foo");
        version_type version = wt.commit();
        client_1.nonsync_transact_notify(version);
    }
    client_1.wait_for_upload_complete_or_client_stopped();

    client_1.wait_for_download_complete_or_client_stopped();
    {
        WriteTransaction wt{sg_1};
        TableRef foos = wt.get_table("class_Foo");
        foos->create_object().remove();
        version_type version = wt.commit();
        client_1.nonsync_transact_notify(version);
    }
    client_1.wait_for_upload_complete_or_client_stopped();

    // Download changes to client2, then stop.
    DBRef sg_2 = DB::create(make_client_replication(client_2_path));
    {
        Session expired_client = fixture.make_bound_session(sg_2, "/test");
        expired_client.wait_for_download_complete_or_client_stopped();
    }

    // Make a change in client2 while offline.
    {
        WriteTransaction wt{sg_2};
        TableRef foos = wt.get_table("class_Foo");
        foos->create_object();
        wt.commit();
    }

    // Wait until history_ttl is expired.
    clock.add_time(config.history_ttl * 2);

    // Make some changes in client_1, causing the server to compact history.
    {
        WriteTransaction wt{sg_1};
        TableRef foos = wt.get_table("class_Foo");
        foos->create_object();
        version_type version = wt.commit();
        client_1.nonsync_transact_notify(version);
    }
    client_1.wait_for_upload_complete_or_client_stopped();
    client_1.wait_for_download_complete_or_client_stopped();
    clock.add_time(config.history_ttl * 2);
    {
        WriteTransaction wt{sg_1};
        TableRef foos = wt.get_table("class_Foo");
        foos->create_object();
        version_type version = wt.commit();
        client_1.nonsync_transact_notify(version);
    }
    client_1.wait_for_upload_complete_or_client_stopped();

    // Attempt to reconnect with client2, which has changes based on the
    // now-compacted history.
    {
        Session expired_client = fixture.make_bound_session(sg_2, "/test");
        expired_client.wait_for_upload_complete_or_client_stopped();
    }

    CHECK(client_reset_did_happen);
}


// Check that a read-only client will properly relinquish its lock on the
// server-side history such that in-place history compaction can proceed.
TEST(Sync_ServerHistoryCompaction_ReadOnlyClients)
{
    FakeClock clock;

    TEST_DIR(dir);
    ClientServerFixture::Config config;
    config.history_compaction_clock = &clock;
    config.history_compaction_interval = 1s;
    config.one_connection_per_session = false; // See comment below
    ClientServerFixture fixture{dir, test_context, config};
    fixture.start();

    std::string vpath = "/test";
    std::string server_path = fixture.map_virtual_to_real_path(vpath);
    ServerHistoryContext context;
    _impl::ServerHistory::DummyCompactionControl compaction_control;
    _impl::ServerHistory server_history{server_path, context, compaction_control};
    DBRef server_realm = DB::create(server_history);

    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);
    RealmFixture client_1{fixture, path_1, vpath}; // Downloader (read-only client)
    RealmFixture client_2{fixture, path_2, vpath}; // Uploader

    for (int i = 0; i < 100; ++i) {
        client_1.wait_for_download_complete_or_client_stopped();
        version_type version = client_1.get_last_integrated_server_version();
        // Because client 1 and client 2 share a single network connection, the
        // pushing of a new changeset via client 2 is enough to ensure that the
        // empty UPLOAD message generated by client 1 at download completion
        // will have been received and processed by the server. Additionally,
        // because the clock is bumped, compaction will be activated on the
        // server during, or after the reception of that empty UPLOAD message,
        // but no later than during the processing of the new changeset pushed
        // by client 2. The reasoning here relies on the assumption (currently a
        // fact) that when two sessions share a network connection, they will be
        // allowed to send a message in the other that they express a desire to
        // do so.
        clock.add_time(2s);
        client_2.nonempty_transact();
        client_2.wait_for_upload_complete_or_client_stopped();
        CHECK_GREATER_EQUAL(server_history.get_compacted_until_version(), version);
    }

    // Verify the server file.
    ReadTransaction rt{server_realm};
    rt.get_group().verify();
}

TEST_IF(Sync_ServerHistoryCompaction_Benchmark, false)
{
    using seconds = std::chrono::seconds;
    auto run_benchmark = [this](bool enabled, seconds history_ttl, seconds compaction_interval) {
        TEST_DIR(dir);
        FakeClock clock;
        {
            ClientServerFixture::Config config;
            config.disable_history_compaction = !enabled;
            config.history_ttl = history_ttl;
            config.history_compaction_interval = compaction_interval;
            config.history_compaction_clock = &clock;
            ClientServerFixture fixture{dir, test_context, config};
            fixture.start();

            std::string large_string(10000, 'a');

            for (size_t i = 0; i < 10; ++i) {
                std::cout << "Producing data on client " << i << "\n";
                SHARED_GROUP_TEST_PATH(path);
                DBRef sg = DB::create(make_client_replication(path));
                Session session = fixture.make_bound_session(sg, "/test");
                {
                    WriteTransaction wt{sg};
                    TableRef foo = sync::create_table_with_primary_key(wt, "class_Foo", type_Int, "pk");
                    ColKey col = foo->add_column(type_String, "large");
                    for (int_fast64_t j = 0; j < 100; ++j) {
                        foo->create_object_with_primary_key(j).set(col, large_string);
                    }

                    session.nonsync_transact_notify(wt.commit());
                    session.wait_for_upload_complete_or_client_stopped();
                    session.wait_for_download_complete_or_client_stopped();
                }
                {
                    // Create another non-empty transaction such that the last
                    // rh_base_version on the server is > 0.
                    WriteTransaction wt{sg};
                    TableRef foo = wt.get_table("class_Foo");
                    foo->create_object_with_primary_key(int(i) + 100);
                    session.nonsync_transact_notify(wt.commit());
                    session.wait_for_upload_complete_or_client_stopped();
                }

                clock.add_time(history_ttl);
            }

            // Make a client that uploads 1 changeset to trigger compaction.
            SHARED_GROUP_TEST_PATH(path);
            DBRef sg = DB::create(make_client_replication(path));
            Session session = fixture.make_bound_session(sg, "/test");
            session.wait_for_download_complete_or_client_stopped();
            WriteTransaction wt{sg};
            TableRef foo = wt.get_table("class_Foo");
            foo->create_object_with_primary_key(1);
            session.nonsync_transact_notify(wt.commit());
            session.wait_for_upload_complete_or_client_stopped();
            fixture.stop();
        }

        std::string server_realm_path = std::string{dir} + "/test.realm";
        {
            util::File server_realm{server_realm_path, util::File::mode_Read};
            auto file_size = server_realm.get_size();
            std::cout << "Server Realm Size: " << file_size
                      << " "
                         "(compaction_enabled = "
                      << enabled
                      << ", "
                         "history_ttl = "
                      << history_ttl.count()
                      << ", "
                         "compaction_interval = "
                      << compaction_interval.count() << ")\n";
        }

        {
            // Compact the file and see what difference it makes:
            ServerHistoryContext context;
            _impl::ServerHistory::DummyCompactionControl compaction_control;
            _impl::ServerHistory server_history{server_realm_path, context, compaction_control};
            DBRef sg = DB::create(server_history);
            CHECK(sg->compact());
            sg->close();

            util::File server_realm{server_realm_path, util::File::mode_Read};
            auto file_size = server_realm.get_size();
            std::cout << "Server Realm Size after compact(): " << file_size << "\n";
        }
    };

    run_benchmark(false, 0s, 0s);
    run_benchmark(true, 1s, 0s);
}

} // unnamed namespace
