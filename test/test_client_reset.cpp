#include <string>
#include <thread>

#include <realm/util/random.hpp>
#include <realm/db.hpp>
#include <realm/list.hpp>

#include "test.hpp"
#include "sync_fixtures.hpp"
#include "util/semaphore.hpp"
#include "util/compare_groups.hpp"

using namespace realm;
using namespace realm::sync;
using namespace realm::test_util;
using namespace realm::fixtures;

namespace {

using ErrorInfo = Session::ErrorInfo;

TEST(ClientReset_NoLocalChanges)
{
    TEST_DIR(dir_1);                // The original server dir.
    TEST_DIR(dir_2);                // The backup dir.
    SHARED_GROUP_TEST_PATH(path_1); // The writer.
    SHARED_GROUP_TEST_PATH(path_2); // The resetting client.

    util::Logger& logger = test_context.logger;

    const std::string server_path = "/data";

    std::string real_path_1, real_path_2;

    // First we make a changeset and upload it
    {
        ClientServerFixture fixture(dir_1, test_context);
        fixture.start();
        real_path_1 = fixture.map_virtual_to_real_path(server_path);

        DBRef sg = DB::create(make_client_replication(), path_1);
        Session session = fixture.make_bound_session(sg, server_path);

        WriteTransaction wt{sg};
        TableRef table = wt.get_group().add_table_with_primary_key("class_table", type_Int, "int_pk");
        table->create_object_with_primary_key(int64_t(123));
        session.nonsync_transact_notify(wt.commit());
        session.wait_for_upload_complete_or_client_stopped();
    }

    // Get the real path of the backup.
    {
        ClientServerFixture fixture(dir_2, test_context);
        fixture.start();
        real_path_2 = fixture.map_virtual_to_real_path(server_path);
    }

    // The server is shut down. We make a backup of the server Realm.
    logger.debug("real_path_1 = %1, real_path_2 = %2", real_path_1, real_path_2);
    util::File::copy(real_path_1, real_path_2);

    // Make the second changeset in the original and have a client download it
    // all.
    {
        ClientServerFixture fixture(dir_1, test_context);
        fixture.start();
        real_path_1 = fixture.map_virtual_to_real_path(server_path);

        DBRef sg = DB::create(make_client_replication(), path_1);
        Session session = fixture.make_bound_session(sg, server_path);

        WriteTransaction wt{sg};
        TableRef table = wt.get_table("class_table");
        table->create_object_with_primary_key(int64_t(456));
        session.nonsync_transact_notify(wt.commit());
        session.wait_for_upload_complete_or_client_stopped();

        Session session_2 = fixture.make_session(path_2);
        fixture.bind_session(session_2, server_path);
        session_2.wait_for_download_complete_or_client_stopped();
    }

    // Check the content in path_2.
    {
        DBRef sg = DB::create(make_client_replication(), path_2);
        ReadTransaction rt{sg};
        const Group& group = rt.get_group();
        ConstTableRef table = group.get_table("class_table");
        auto col = table->get_primary_key_column();
        CHECK(table);
        CHECK_EQUAL(table->size(), 2);
        CHECK(table->find_first_int(col, 123));
        CHECK(table->find_first_int(col, 456));
    }

    // Start the server from dir_2 and connect with the client 2.
    // We expect an error of type 209, "Bad server version".
    {
        ClientServerFixture fixture(dir_2, test_context);
        fixture.start();

        // The session that receives an error.
        {
            BowlOfStonesSemaphore bowl;
            auto listener = [&](ConnectionState state, const ErrorInfo* error_info) {
                if (state != ConnectionState::disconnected)
                    return;
                REALM_ASSERT(error_info);
                std::error_code ec = error_info->error_code;
                CHECK_EQUAL(ec, sync::ProtocolError::bad_server_version);
                bowl.add_stone();
            };

            Session session = fixture.make_session(path_2);
            session.set_connection_state_change_listener(listener);
            fixture.bind_session(session, server_path);
            bowl.get_stone();
        }

        // get a fresh copy from the server to reset against
        SHARED_GROUP_TEST_PATH(path_fresh);
        {
            Session session_fresh = fixture.make_session(path_fresh);
            fixture.bind_session(session_fresh, server_path);
            session_fresh.wait_for_download_complete_or_client_stopped();
        }
        DBRef sg_fresh = DB::create(make_client_replication(), path_fresh);

        // The session that performs client reset.
        // The Realm will be opened by a user while the reset takes place.
        {
            DBRef sg = DB::create(make_client_replication(), path_2);
            ReadTransaction rt{sg};
            const Group& group = rt.get_group();
            ConstTableRef table = group.get_table("class_table");
            CHECK_EQUAL(table->size(), 2);

            bool sync_transact_callback_called = false;
            auto sync_transact_callback = [&](VersionID old_version, VersionID new_version) {
                logger.debug("sync_transact_callback, old_version.version = %1, "
                             "old_version.index = %2, new_version.version = %3, "
                             "new_version.index = %4",
                             old_version.version, old_version.index, new_version.version, new_version.index);
                CHECK_LESS(old_version.version, new_version.version);
                sync_transact_callback_called = true;
            };

            Session::Config session_config;
            {
                Session::Config::ClientReset client_reset_config;
                client_reset_config.discard_local = true;
                client_reset_config.fresh_copy = std::move(sg_fresh);
                session_config.client_reset_config = std::move(client_reset_config);
            }
            Session session = fixture.make_session(sg, std::move(session_config));
            session.set_sync_transact_callback(std::move(sync_transact_callback));
            fixture.bind_session(session, server_path);
            session.wait_for_download_complete_or_client_stopped();
            CHECK(sync_transact_callback_called);
        }
    }

    // Check the content in path_2. There should only be one row now.
    {
        DBRef sg = DB::create(make_client_replication(), path_2);
        ReadTransaction rt{sg};
        const Group& group = rt.get_group();
        ConstTableRef table = group.get_table("class_table");
        auto col = table->get_primary_key_column();
        CHECK(table);
        CHECK_EQUAL(table->size(), 1);
        CHECK_EQUAL(table->begin()->get<Int>(col), 123);
    }
}

TEST(ClientReset_InitialLocalChanges)
{
    TEST_DIR(dir);
    SHARED_GROUP_TEST_PATH(path_1); // The writer.
    SHARED_GROUP_TEST_PATH(path_2); // The resetting client.

    const std::string server_path = "/data";

    ClientServerFixture fixture(dir, test_context);
    fixture.start();

    Session session_1 = fixture.make_session(path_1);
    fixture.bind_session(session_1, server_path);

    // First we make a changeset and upload it
    {
        DBRef sg = DB::create(make_client_replication(), path_1);

        WriteTransaction wt{sg};
        TableRef table = wt.get_group().add_table_with_primary_key("class_table", type_Int, "int");
        table->create_object_with_primary_key(int64_t(123));
        session_1.nonsync_transact_notify(wt.commit());
    }
    session_1.wait_for_upload_complete_or_client_stopped();

    // The local changes.
    {
        DBRef sg = DB::create(make_client_replication(), path_2);

        WriteTransaction wt{sg};
        TableRef table = wt.get_group().add_table_with_primary_key("class_table", type_Int, "int");
        table->create_object_with_primary_key(int64_t(456));
        wt.commit();
    }

    // get a fresh copy from the server to reset against
    SHARED_GROUP_TEST_PATH(path_fresh);
    {
        Session session_fresh = fixture.make_session(path_fresh);
        fixture.bind_session(session_fresh, server_path);
        session_fresh.wait_for_download_complete_or_client_stopped();
    }
    DBRef sg_fresh = DB::create(make_client_replication(), path_fresh);

    // Start a client reset. There is no need for a reset, but we can do it.
    Session::Config session_config_2;
    {
        Session::Config::ClientReset client_reset_config;
        client_reset_config.discard_local = true;
        client_reset_config.fresh_copy = std::move(sg_fresh);
        session_config_2.client_reset_config = std::move(client_reset_config);
    }
    Session session_2 = fixture.make_session(path_2, std::move(session_config_2));
    fixture.bind_session(session_2, server_path);
    session_2.wait_for_upload_complete_or_client_stopped();
    session_2.wait_for_download_complete_or_client_stopped();

    session_1.wait_for_download_complete_or_client_stopped();

    // Check the content in path_2. There should be two rows now.
    {
        DBRef sg_1 = DB::create(make_client_replication(), path_1);
        DBRef sg_2 = DB::create(make_client_replication(), path_2);

        ReadTransaction rt_1(sg_1);
        ReadTransaction rt_2(sg_2);
        CHECK(compare_groups(rt_1, rt_2));

        const Group& group = rt_2.get_group();
        ConstTableRef table = group.get_table("class_table");
        auto col = table->get_column_key("int");
        CHECK(table);
        CHECK_EQUAL(table->size(), 1);
        auto it = table->begin();
        int_fast64_t val_0 = it->get<Int>(col);
        CHECK(val_0 == 123);
    }

    // Make more changes in path_1.
    {
        DBRef sg = DB::create(make_client_replication(), path_1);

        WriteTransaction wt{sg};
        TableRef table = wt.get_table("class_table");
        table->create_object_with_primary_key(int64_t(1000));
        session_1.nonsync_transact_notify(wt.commit());
    }
    // Make more changes in path_2.
    {
        DBRef sg = DB::create(make_client_replication(), path_2);

        WriteTransaction wt{sg};
        TableRef table = wt.get_table("class_table");
        table->create_object_with_primary_key(int64_t(2000));
        session_2.nonsync_transact_notify(wt.commit());
    }
    session_1.wait_for_upload_complete_or_client_stopped();
    session_2.wait_for_upload_complete_or_client_stopped();
    session_1.wait_for_download_complete_or_client_stopped();
    session_2.wait_for_download_complete_or_client_stopped();

    {
        DBRef sg_1 = DB::create(make_client_replication(), path_1);
        DBRef sg_2 = DB::create(make_client_replication(), path_2);

        ReadTransaction rt_1(sg_1);
        ReadTransaction rt_2(sg_2);
        CHECK(compare_groups(rt_1, rt_2));
    }
}

TEST(ClientReset_LocalChangesWhenOffline)
{
    TEST_DIR(dir);
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);
    ColKey col_int;

    const std::string server_path = "/data";

    ClientServerFixture fixture(dir, test_context);
    fixture.start();

    DBRef sg = DB::create(make_client_replication(), path_1);

    // First we make a changeset and upload it
    {
        // We force an async open. This will have the effect that the state file will be empty
        Session::Config session_config_1;
        session_config_1.client_reset_config = Session::Config::ClientReset{};
        Session session_1 = fixture.make_session(sg, std::move(session_config_1));
        fixture.bind_session(session_1, server_path);
        session_1.wait_for_download_complete_or_client_stopped();

        WriteTransaction wt{sg};
        TableRef table = wt.add_table("class_table");
        col_int = table->add_column(type_Int, "int");
        table->create_object().set(col_int, 123);
        session_1.nonsync_transact_notify(wt.commit());
        session_1.wait_for_upload_complete_or_client_stopped();
        session_1.wait_for_download_complete_or_client_stopped();
    }

    DBRef sg_2 = DB::create(make_client_replication(), path_2);
    Session session_2 = fixture.make_session(sg_2);
    fixture.bind_session(session_2, server_path);
    session_2.wait_for_upload_complete_or_client_stopped();
    session_2.wait_for_download_complete_or_client_stopped();

    {
        ReadTransaction rt(sg_2);
        auto table = rt.get_table("class_table");
        CHECK(table);
        if (table) {
            CHECK_EQUAL(table->size(), 1);
        }
    }

    // The local changes.
    {
        WriteTransaction wt{sg};
        TableRef table = wt.get_table("class_table");
        table->create_object().set(col_int, 456);
        wt.commit();
    }

    Session::Config session_config_3;
    session_config_3.client_reset_config = Session::Config::ClientReset{};
    Session session_3 = fixture.make_session(sg, std::move(session_config_3));
    fixture.bind_session(session_3, server_path);
    session_3.wait_for_upload_complete_or_client_stopped();
    session_3.wait_for_download_complete_or_client_stopped();

    session_2.wait_for_upload_complete_or_client_stopped();
    session_2.wait_for_download_complete_or_client_stopped();

    {
        ReadTransaction rt(sg_2);
        auto table = rt.get_table("class_table");
        CHECK(table);
        if (table) {
            CHECK_EQUAL(table->size(), 1);
            CHECK_EQUAL(table->begin()->get<Int>(col_int), 123);
        }
    }
}

// In this test, two clients create multiple changesets and upload them.
// At some point, the server recovers from a backup. The client keeps making
// changes. Both clients will experience a client reset and upload their local
// changes. The client make even more changes and upload them.
// In the end, a third client performs async open.
// It is checked that the clients and server work without errors and that the
// clients converge in the end.
TEST(ClientReset_ThreeClients)
{
    TEST_DIR(dir_1); // The server.
    TEST_DIR(dir_2); // The backup server.
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);
    SHARED_GROUP_TEST_PATH(path_3);

    util::Logger& logger = test_context.logger;

    const std::string server_path = "/data";

    std::string real_path_1, real_path_2;

    auto create_schema = [&](Transaction& group) {
        TableRef table_0 = group.add_table_with_primary_key("class_table_0", type_Int, "pk_int");
        table_0->add_column(type_Int, "int");
        table_0->add_column(type_Bool, "bool");
        table_0->add_column(type_Float, "float");
        table_0->add_column(type_Double, "double");
        table_0->add_column(type_Timestamp, "timestamp");

        TableRef table_1 = group.add_table_with_primary_key("class_table_1", type_Int, "pk_int");
        table_1->add_column(type_String, "String");

        TableRef table_2 = group.add_table_with_primary_key("class_table_2", type_String, "pk_string");
        table_2->add_column_list(type_String, "array_string");
    };

    // First we make changesets. Then we upload them.
    {
        ClientServerFixture fixture(dir_1, test_context);
        fixture.start();
        real_path_1 = fixture.map_virtual_to_real_path(server_path);

        {
            DBRef sg = DB::create(make_client_replication(), path_1);
            WriteTransaction wt{sg};
            create_schema(wt);
            wt.commit();
        }
        {
            DBRef sg = DB::create(make_client_replication(), path_2);
            WriteTransaction wt{sg};
            create_schema(wt);

            TableRef table_2 = wt.get_table("class_table_2");
            auto col = table_2->get_column_key("array_string");
            auto list_string = table_2->create_object_with_primary_key("aaa").get_list<String>(col);
            list_string.add("a");
            list_string.add("b");

            wt.commit();
        }

        Session session_1 = fixture.make_session(path_1);
        fixture.bind_session(session_1, server_path);
        Session session_2 = fixture.make_session(path_2);
        fixture.bind_session(session_2, server_path);

        session_1.wait_for_upload_complete_or_client_stopped();
        session_2.wait_for_upload_complete_or_client_stopped();
        // Download completion is not important.
    }

    // Get the real path of the backup.
    {
        ClientServerFixture fixture(dir_2, test_context);
        fixture.start();
        real_path_2 = fixture.map_virtual_to_real_path(server_path);
    }

    // The server is shut down. We make a backup of the server Realm.
    logger.debug("real_path_1 = %1, real_path_2 = %2", real_path_1, real_path_2);
    util::File::copy(real_path_1, real_path_2);

    // Continue uploading changes to the original server.
    {
        ClientServerFixture fixture(dir_1, test_context);
        fixture.start();

        DBRef db_1 = DB::create(make_client_replication(), path_1);
        DBRef db_2 = DB::create(make_client_replication(), path_2);

        {
            WriteTransaction wt{db_1};
            TableRef table_0 = wt.get_table("class_table_0");
            CHECK(table_0);
            table_0->create_object_with_primary_key(int64_t(0)).set_all(111, true);

            TableRef table_2 = wt.get_table("class_table_2");
            CHECK(table_2);
            {
                auto col = table_2->get_column_key("array_string");
                Obj obj = table_2->create_object_with_primary_key("aaa"); // get or create
                auto list_string = obj.get_list<String>(col);
                list_string.add("c");
                list_string.add("d");
            }

            wt.commit();
        }
        {
            WriteTransaction wt{db_2};
            TableRef table = wt.get_table("class_table_0");
            CHECK(table);
            table->create_object_with_primary_key(int64_t(1)).set_all(222, false);
            wt.commit();
        }

        Session session_1 = fixture.make_bound_session(db_1, server_path);
        Session session_2 = fixture.make_bound_session(db_2, server_path);

        session_1.wait_for_upload_complete_or_client_stopped();
        session_2.wait_for_upload_complete_or_client_stopped();
    }

    // Start the backup server from dir_2.
    {
        // client 1 and 2 will receive session errors.

        ClientServerFixture fixture(dir_2, test_context);
        fixture.start();

        // The two clients add changes.
        {
            DBRef sg = DB::create(make_client_replication(), path_1);
            WriteTransaction wt{sg};
            TableRef table_0 = wt.get_table("class_table_0");
            CHECK(table_0);
            table_0->create_object_with_primary_key(int64_t(3)).set_all(333);

            TableRef table_2 = wt.get_table("class_table_2");
            CHECK(table_2);
            {
                auto col = table_2->get_column_key("array_string");
                Obj obj = table_2->get_object_with_primary_key("aaa");
                CHECK(obj.is_valid());
                auto list_string = obj.get_list<String>(col);
                list_string.insert(0, "e");
                list_string.insert(1, "f");
            }
            wt.commit();
        }
        {
            DBRef sg = DB::create(make_client_replication(), path_2);
            WriteTransaction wt{sg};
            TableRef table_0 = wt.get_table("class_table_0");
            CHECK(table_0);
            table_0->create_object_with_primary_key(int64_t(4)).set_all(444);

            TableRef table_2 = wt.get_table("class_table_2");
            CHECK(table_2);
            {
                Obj obj = table_2->get_object_with_primary_key("aaa");
                CHECK(obj.is_valid());
                table_2->remove_object(obj.get_key());
            }

            wt.commit();
        }

        // The clients get session errors.
        {
            BowlOfStonesSemaphore bowl;
            auto listener = [&](ConnectionState state, const ErrorInfo* error_info) {
                if (state != ConnectionState::disconnected)
                    return;
                REALM_ASSERT(error_info);
                std::error_code ec = error_info->error_code;
                CHECK_EQUAL(ec, sync::ProtocolError::bad_server_version);
                bowl.add_stone();
            };

            Session session_1 = fixture.make_session(path_1);
            session_1.set_connection_state_change_listener(listener);
            fixture.bind_session(session_1, server_path);
            Session session_2 = fixture.make_session(path_2);
            session_2.set_connection_state_change_listener(listener);
            fixture.bind_session(session_2, server_path);
            bowl.get_stone();
            bowl.get_stone();
        }

        // get a fresh copy from the server to reset against
        SHARED_GROUP_TEST_PATH(path_fresh1);
        SHARED_GROUP_TEST_PATH(path_fresh2);
        {
            Session session4 = fixture.make_session(path_fresh1);
            fixture.bind_session(session4, server_path);
            session4.wait_for_download_complete_or_client_stopped();
        }
        DBRef sg_fresh1 = DB::create(make_client_replication(), path_fresh1);

        {
            Session session4 = fixture.make_session(path_fresh2);
            fixture.bind_session(session4, server_path);
            session4.wait_for_download_complete_or_client_stopped();
        }
        DBRef sg_fresh2 = DB::create(make_client_replication(), path_fresh2);

        // Perform client resets on the two clients.
        {
            Session::Config session_config_1;
            {
                Session::Config::ClientReset client_reset_config;
                client_reset_config.discard_local = true;
                client_reset_config.fresh_copy = std::move(sg_fresh1);
                session_config_1.client_reset_config = std::move(client_reset_config);
            }
            Session::Config session_config_2;
            {
                Session::Config::ClientReset client_reset_config;
                client_reset_config.discard_local = true;
                client_reset_config.fresh_copy = std::move(sg_fresh2);
                session_config_2.client_reset_config = std::move(client_reset_config);
            }
            Session session_1 = fixture.make_session(path_1, std::move(session_config_1));
            fixture.bind_session(session_1, server_path);
            Session session_2 = fixture.make_session(path_2, std::move(session_config_2));
            fixture.bind_session(session_2, server_path);

            session_1.wait_for_download_complete_or_client_stopped();
            session_2.wait_for_download_complete_or_client_stopped();
        }

        // More local changes
        {
            DBRef sg = DB::create(make_client_replication(), path_1);
            WriteTransaction wt{sg};
            TableRef table = wt.get_table("class_table_0");
            CHECK(table);
            table->create_object_with_primary_key(int64_t(5)).set_all(555);
            wt.commit();
        }
        {
            DBRef sg = DB::create(make_client_replication(), path_2);
            WriteTransaction wt{sg};
            TableRef table = wt.get_table("class_table_0");
            CHECK(table);
            table->create_object_with_primary_key(int64_t(6)).set_all(666);
            wt.commit();
        }

        // Upload and download complete the clients.
        Session session_1 = fixture.make_session(path_1);
        fixture.bind_session(session_1, server_path);
        Session session_2 = fixture.make_session(path_2);
        fixture.bind_session(session_2, server_path);

        session_1.wait_for_upload_complete_or_client_stopped();
        session_2.wait_for_upload_complete_or_client_stopped();
        session_1.wait_for_download_complete_or_client_stopped();
        session_2.wait_for_download_complete_or_client_stopped();

        std::this_thread::sleep_for(std::chrono::milliseconds{1000});

        // A third client makes async open
        {
            Session::Config session_config;
            session_config.client_reset_config = Session::Config::ClientReset{};
            Session session = fixture.make_session(path_3, std::move(session_config));
            fixture.bind_session(session, server_path);
            session.wait_for_download_complete_or_client_stopped();
        }
    }

    // Check convergence
    {
        DBRef sg_1 = DB::create(make_client_replication(), path_1);
        DBRef sg_2 = DB::create(make_client_replication(), path_2);
        DBRef sg_3 = DB::create(make_client_replication(), path_3);

        ReadTransaction rt_1(sg_1);
        ReadTransaction rt_2(sg_2);
        ReadTransaction rt_3(sg_3);
        CHECK(compare_groups(rt_1, rt_2));
        CHECK(compare_groups(rt_1, rt_3));
        CHECK(compare_groups(rt_2, rt_3));
    }
}

TEST(ClientReset_DoNotRecoverSchema)
{
    TEST_DIR(dir);
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);

    const std::string server_path_1 = "/data_1";
    const std::string server_path_2 = "/data_2";

    ClientServerFixture fixture(dir, test_context);
    fixture.start();

    // Insert data into path_1/server_path_1 and upload it.
    {
        DBRef sg = DB::create(make_client_replication(), path_1);
        WriteTransaction wt{sg};
        std::string table_name = "class_table1";
        TableRef table = wt.get_group().add_table_with_primary_key(table_name, type_Int, "int_pk");
        table->create_object_with_primary_key(int64_t(123));
        wt.commit();
        Session session = fixture.make_bound_session(sg, server_path_1);
        session.wait_for_upload_complete_or_client_stopped();
    }
    // Insert a different table into path_2/server_path_2
    {
        DBRef sg = DB::create(make_client_replication(), path_2);
        WriteTransaction wt{sg};
        std::string table_name = "class_table2";
        TableRef table = wt.get_group().add_table_with_primary_key(table_name, type_String, "string_pk");
        table->create_object_with_primary_key("pk_0");
        wt.commit();
        Session session = fixture.make_bound_session(sg, server_path_2);
        session.wait_for_upload_complete_or_client_stopped();
    }

    // get a fresh copy from the server to reset against
    SHARED_GROUP_TEST_PATH(path_fresh1);
    {
        Session session_fresh = fixture.make_session(path_fresh1);
        fixture.bind_session(session_fresh, server_path_2);
        session_fresh.wait_for_download_complete_or_client_stopped();
    }
    DBRef sg_fresh1 = DB::create(make_client_replication(), path_fresh1);

    // Perform client reset for path_1 against server_path_2.
    // This attempts to remove the added class and this destructive
    // schema change is not allowed and so fails with a client reset error.
    {
        Session::Config session_config;
        {
            Session::Config::ClientReset client_reset_config;
            client_reset_config.discard_local = true;
            client_reset_config.fresh_copy = std::move(sg_fresh1);
            session_config.client_reset_config = std::move(client_reset_config);
        }
        Session session = fixture.make_session(path_1, std::move(session_config));
        BowlOfStonesSemaphore bowl;
        session.set_connection_state_change_listener([&](ConnectionState state, const ErrorInfo* error_info) {
            if (state != ConnectionState::disconnected)
                return;
            REALM_ASSERT(error_info);
            std::error_code ec = error_info->error_code;
            CHECK_EQUAL(ec, sync::Client::Error::auto_client_reset_failure);
            bowl.add_stone();
        });
        fixture.bind_session(session, server_path_2);
        bowl.get_stone();
    }

    {
        DBRef sg_1 = DB::create(make_client_replication(), path_1);
        DBRef sg_2 = DB::create(make_client_replication(), path_2);

        ReadTransaction rt_1(sg_1);
        ReadTransaction rt_2(sg_2);
        CHECK(!compare_groups(rt_1, rt_2));

        const Group& group = rt_1.get_group();
        CHECK_EQUAL(group.size(), 1);
        CHECK(group.get_table("class_table1"));
        CHECK_NOT(group.get_table("class_table2"));
        const Group& group2 = rt_2.get_group();
        CHECK_EQUAL(group2.size(), 1);
        CHECK_NOT(group2.get_table("class_table1"));
        CHECK(group2.get_table("class_table2"));
    }
}

TEST(ClientReset_PinnedVersion)
{
    TEST_DIR(dir);
    SHARED_GROUP_TEST_PATH(path_1);

    const std::string server_path_1 = "/data_1";
    const std::string server_path_2 = "/data_2";
    const std::string table_name = "class_table";

    ClientServerFixture fixture(dir, test_context);
    fixture.start();

    DBRef sg = DB::create(make_client_replication(), path_1);

    // Create and upload the initial version
    {
        WriteTransaction wt{sg};
        TableRef table = wt.add_table(table_name);
        table->add_column(type_Float, "float");
        table->create_object();
        wt.commit();

        Session session = fixture.make_bound_session(sg, server_path_1);
        session.wait_for_upload_complete_or_client_stopped();
    }

    // Pin this current version so that the history can't be trimmed
    auto pin_rt = sg->start_read();

    // Add another version locally
    {
        WriteTransaction wt{sg};
        wt.get_table(table_name)->create_object();
        wt.commit();
    }

    // Trigger a client reset by syncing with a different server URL
    {
        Session::Config session_config;
        session_config.client_reset_config = Session::Config::ClientReset{};
        Session session = fixture.make_bound_session(sg, server_path_2, std::move(session_config));
        session.wait_for_download_complete_or_client_stopped();
    }
}

} // unnamed namespace
