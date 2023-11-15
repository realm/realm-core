#include <realm/db.hpp>
#include <realm/list.hpp>
#include <realm/object_converter.hpp>
#include <realm/sync/noinst/client_reset.hpp>
#include <realm/sync/noinst/client_reset_operation.hpp>
#include <realm/sync/subscriptions.hpp>
#include <realm/table_view.hpp>
#include <realm/util/random.hpp>

#include "test.hpp"
#include "sync_fixtures.hpp"
#include "util/semaphore.hpp"
#include "util/compare_groups.hpp"

#include <string>
#include <thread>

using namespace realm;
using namespace realm::sync;
using namespace realm::test_util;
using namespace realm::fixtures;

namespace {

using ErrorInfo = SessionErrorInfo;

TEST(ClientReset_TransferGroupWithDanglingLinks)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);

    auto setup_realm = [](auto& path) {
        DBRef sg = DB::create(make_client_replication(), path);

        auto wt = sg->start_write();

        // The ordering of creating the tables matters here. The bug this test is verifying depends
        // on tablekeys being created such that the table that links come from is transferred before
        // the table that links are linking to.
        auto table = wt->add_table_with_primary_key("class_table", type_String, "_id");
        auto target = wt->add_table_with_primary_key("class_target", type_Int, "_id");
        table->add_column_list(*target, "list");
        auto obj = table->create_object_with_primary_key(Mixed{"the_object"});
        auto lst = obj.get_linklist("list");
        for (int64_t i = 0; i < 10; ++i) {
            target->create_object_with_primary_key(i);
            lst.add(target->create_object_with_primary_key(i).get_key());
        }
        wt->commit();

        return sg;
    };

    auto sg_1 = setup_realm(path_1);
    auto sg_2 = setup_realm(path_2);

    auto rt = sg_1->start_read();
    auto wt = sg_2->start_write();

    auto target_2 = wt->get_table("class_target");
    auto obj = target_2->get_object_with_primary_key(Mixed{5});
    obj.invalidate();

    wt->commit_and_continue_writing();
    constexpr bool allow_schema_additions = false;
    _impl::client_reset::transfer_group(*rt, *wt, *test_context.logger, allow_schema_additions);
}

TEST(ClientReset_NoLocalChanges)
{
    TEST_DIR(dir_1);                // The original server dir.
    TEST_DIR(dir_2);                // The backup dir.
    SHARED_GROUP_TEST_PATH(path_1); // The writer.
    SHARED_GROUP_TEST_PATH(path_2); // The resetting client.

    auto& logger = *test_context.logger;

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
        wt.commit();
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
        wt.commit();
        session.wait_for_upload_complete_or_client_stopped();

        Session session_2 = fixture.make_session(path_2, server_path);
        session_2.bind();
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
            auto listener = [&](ConnectionState state, util::Optional<ErrorInfo> error_info) {
                if (state != ConnectionState::disconnected)
                    return;
                REALM_ASSERT(error_info);
                CHECK_EQUAL(error_info->status, ErrorCodes::SyncClientResetRequired);
                CHECK_EQUAL(static_cast<ProtocolError>(error_info->raw_error_code),
                            ProtocolError::bad_server_version);
                bowl.add_stone();
            };

            Session session = fixture.make_session(path_2, server_path);
            session.set_connection_state_change_listener(listener);
            session.bind();
            bowl.get_stone();
        }

        // get a fresh copy from the server to reset against
        SHARED_GROUP_TEST_PATH(path_fresh);
        {
            Session session_fresh = fixture.make_session(path_fresh, server_path);
            session_fresh.bind();
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

            Session::Config session_config;
            {
                Session::Config::ClientReset client_reset_config;
                client_reset_config.mode = ClientResyncMode::DiscardLocal;
                client_reset_config.fresh_copy = std::move(sg_fresh);
                session_config.client_reset_config = std::move(client_reset_config);
            }
            Session session = fixture.make_session(sg, server_path, std::move(session_config));
            session.bind();
            session.wait_for_download_complete_or_client_stopped();
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

    DBRef db_1 = DB::create(make_client_replication(), path_1);
    DBRef db_2 = DB::create(make_client_replication(), path_2);

    Session session_1 = fixture.make_session(db_1, server_path);
    session_1.bind();

    // First we make a changeset and upload it
    {
        WriteTransaction wt{db_1};
        TableRef table = wt.get_group().add_table_with_primary_key("class_table", type_Int, "int");
        table->create_object_with_primary_key(int64_t(123));
        wt.commit();
    }
    session_1.wait_for_upload_complete_or_client_stopped();

    // The local changes.
    {
        WriteTransaction wt{db_2};
        TableRef table = wt.get_group().add_table_with_primary_key("class_table", type_Int, "int");
        table->create_object_with_primary_key(int64_t(456));
        wt.commit();
    }

    // get a fresh copy from the server to reset against
    SHARED_GROUP_TEST_PATH(path_fresh);
    {
        Session session_fresh = fixture.make_session(path_fresh, server_path);
        session_fresh.bind();
        session_fresh.wait_for_download_complete_or_client_stopped();
    }
    DBRef sg_fresh = DB::create(make_client_replication(), path_fresh);

    // Start a client reset. There is no need for a reset, but we can do it.
    Session::Config session_config_2;
    {
        Session::Config::ClientReset client_reset_config;
        client_reset_config.mode = ClientResyncMode::DiscardLocal;
        client_reset_config.fresh_copy = std::move(sg_fresh);
        session_config_2.client_reset_config = std::move(client_reset_config);
    }
    Session session_2 = fixture.make_session(db_2, server_path, std::move(session_config_2));
    session_2.bind();
    session_2.wait_for_upload_complete_or_client_stopped();
    session_2.wait_for_download_complete_or_client_stopped();

    session_1.wait_for_download_complete_or_client_stopped();

    // Check the content in path_2. There should be two rows now.
    {
        ReadTransaction rt_1(db_1);
        ReadTransaction rt_2(db_2);
        CHECK(compare_groups(rt_1, rt_2, *test_context.logger));

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
        WriteTransaction wt{db_1};
        TableRef table = wt.get_table("class_table");
        table->create_object_with_primary_key(int64_t(1000));
        wt.commit();
    }
    // Make more changes in path_2.
    {
        WriteTransaction wt{db_2};
        TableRef table = wt.get_table("class_table");
        table->create_object_with_primary_key(int64_t(2000));
        wt.commit();
    }
    session_1.wait_for_upload_complete_or_client_stopped();
    session_2.wait_for_upload_complete_or_client_stopped();
    session_1.wait_for_download_complete_or_client_stopped();
    session_2.wait_for_download_complete_or_client_stopped();

    {
        ReadTransaction rt_1(db_1);
        ReadTransaction rt_2(db_2);
        CHECK(compare_groups(rt_1, rt_2, *test_context.logger));
    }
}

TEST_TYPES(ClientReset_LocalChangesWhenOffline, std::true_type, std::false_type)
{
    constexpr bool recover = TEST_TYPE::value;
    TEST_DIR(dir);
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);

    const std::string server_path = "/data";

    ClientServerFixture fixture(dir, test_context);
    fixture.start();

    DBRef sg = DB::create(make_client_replication(), path_1);

    // First we make a changeset and upload it
    {
        // Download a new Realm. The state is empty.
        Session::Config session_config_1;
        Session session_1 = fixture.make_session(sg, server_path, std::move(session_config_1));
        session_1.bind();
        session_1.wait_for_download_complete_or_client_stopped();

        WriteTransaction wt{sg};
        TableRef table = ((Transaction&)wt).add_table_with_primary_key("class_table", type_Int, "_id");
        table->create_object_with_primary_key(123);
        wt.commit();
        session_1.wait_for_upload_complete_or_client_stopped();
        session_1.wait_for_download_complete_or_client_stopped();
    }

    DBRef sg_2 = DB::create(make_client_replication(), path_2);
    Session session_2 = fixture.make_session(sg_2, server_path);
    session_2.bind();
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
        table->create_object_with_primary_key(456);
        wt.commit();
    }

    // get a fresh copy from the server to reset against
    SHARED_GROUP_TEST_PATH(path_fresh1);
    {
        Session session4 = fixture.make_session(path_fresh1, server_path);
        session4.bind();
        session4.wait_for_download_complete_or_client_stopped();
    }
    DBRef sg_fresh1 = DB::create(make_client_replication(), path_fresh1);

    Session::Config session_config_3;
    session_config_3.client_reset_config = Session::Config::ClientReset{};
    session_config_3.client_reset_config->mode = recover ? ClientResyncMode::Recover : ClientResyncMode::DiscardLocal;
    session_config_3.client_reset_config->fresh_copy = std::move(sg_fresh1);
    Session session_3 = fixture.make_session(sg, server_path, std::move(session_config_3));
    session_3.bind();
    session_3.wait_for_upload_complete_or_client_stopped();
    session_3.wait_for_download_complete_or_client_stopped();

    session_2.wait_for_upload_complete_or_client_stopped();
    session_2.wait_for_download_complete_or_client_stopped();

    {
        ReadTransaction rt(sg_2);
        auto table = rt.get_table("class_table");
        CHECK(table);
        if (table) {
            if (recover) {
                CHECK_EQUAL(table->size(), 2);
                TableView sorted = table->get_sorted_view(table->get_primary_key_column());
                CHECK_EQUAL(sorted.size(), 2);
                CHECK_EQUAL(sorted.get_object(0).get_primary_key().get_int(), 123);
                CHECK_EQUAL(sorted.get_object(1).get_primary_key().get_int(), 456);
            }
            else {
                // discard local changes
                CHECK_EQUAL(table->size(), 1);
                CHECK_EQUAL(table->begin()->get_primary_key().get_int(), 123);
            }
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

    auto& logger = *test_context.logger;

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

        Session session_1 = fixture.make_session(path_1, server_path);
        session_1.bind();
        Session session_2 = fixture.make_session(path_2, server_path);
        session_2.bind();

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
            auto listener = [&](ConnectionState state, util::Optional<ErrorInfo> error_info) {
                if (state != ConnectionState::disconnected)
                    return;
                REALM_ASSERT(error_info);
                CHECK_EQUAL(error_info->status, ErrorCodes::SyncClientResetRequired);
                CHECK_EQUAL(static_cast<ProtocolError>(error_info->raw_error_code),
                            ProtocolError::bad_server_version);
                bowl.add_stone();
            };

            Session session_1 = fixture.make_session(path_1, server_path);
            session_1.set_connection_state_change_listener(listener);
            session_1.bind();
            Session session_2 = fixture.make_session(path_2, server_path);
            session_2.set_connection_state_change_listener(listener);
            session_2.bind();
            bowl.get_stone();
            bowl.get_stone();
        }

        // get a fresh copy from the server to reset against
        SHARED_GROUP_TEST_PATH(path_fresh1);
        SHARED_GROUP_TEST_PATH(path_fresh2);
        {
            Session session4 = fixture.make_session(path_fresh1, server_path);
            session4.bind();
            session4.wait_for_download_complete_or_client_stopped();
        }
        DBRef sg_fresh1 = DB::create(make_client_replication(), path_fresh1);

        {
            Session session4 = fixture.make_session(path_fresh2, server_path);
            session4.bind();
            session4.wait_for_download_complete_or_client_stopped();
        }
        DBRef sg_fresh2 = DB::create(make_client_replication(), path_fresh2);

        // Perform client resets on the two clients.
        {
            Session::Config session_config_1;
            {
                Session::Config::ClientReset client_reset_config;
                client_reset_config.mode = ClientResyncMode::DiscardLocal;
                client_reset_config.fresh_copy = std::move(sg_fresh1);
                session_config_1.client_reset_config = std::move(client_reset_config);
            }
            Session::Config session_config_2;
            {
                Session::Config::ClientReset client_reset_config;
                client_reset_config.mode = ClientResyncMode::DiscardLocal;
                client_reset_config.fresh_copy = std::move(sg_fresh2);
                session_config_2.client_reset_config = std::move(client_reset_config);
            }
            Session session_1 = fixture.make_session(path_1, server_path, std::move(session_config_1));
            session_1.bind();
            Session session_2 = fixture.make_session(path_2, server_path, std::move(session_config_2));
            session_2.bind();

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
        Session session_1 = fixture.make_session(path_1, server_path);
        session_1.bind();
        Session session_2 = fixture.make_session(path_2, server_path);
        session_2.bind();

        session_1.wait_for_upload_complete_or_client_stopped();
        session_2.wait_for_upload_complete_or_client_stopped();
        session_1.wait_for_download_complete_or_client_stopped();
        session_2.wait_for_download_complete_or_client_stopped();

        std::this_thread::sleep_for(std::chrono::milliseconds{1000});

        // A third client downloads the state
        {
            Session::Config session_config;
            Session session = fixture.make_session(path_3, server_path, std::move(session_config));
            session.bind();
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
        CHECK(compare_groups(rt_1, rt_2, *test_context.logger));
        CHECK(compare_groups(rt_1, rt_3, *test_context.logger));
        CHECK(compare_groups(rt_2, rt_3, *test_context.logger));
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
        Session session_fresh = fixture.make_session(path_fresh1, server_path_2);
        session_fresh.bind();
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
            client_reset_config.mode = ClientResyncMode::DiscardLocal;
            client_reset_config.fresh_copy = std::move(sg_fresh1);
            session_config.client_reset_config = std::move(client_reset_config);
        }
        Session session = fixture.make_session(path_1, server_path_2, std::move(session_config));
        BowlOfStonesSemaphore bowl;
        session.set_connection_state_change_listener(
            [&](ConnectionState state, util::Optional<ErrorInfo> error_info) {
                if (state != ConnectionState::disconnected)
                    return;
                REALM_ASSERT(error_info);
                CHECK_EQUAL(error_info->status, ErrorCodes::AutoClientResetFailed);
                bowl.add_stone();
            });
        session.bind();
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
        TableRef table = ((Transaction&)wt).add_table_with_primary_key(table_name, type_Int, "_id");
        table->create_object_with_primary_key(123);
        wt.commit();

        Session session = fixture.make_bound_session(sg, server_path_1);
        session.wait_for_upload_complete_or_client_stopped();
    }

    // Pin this current version so that the history can't be trimmed
    auto pin_rt = sg->start_read();

    // Add another version locally
    {
        WriteTransaction wt{sg};
        wt.get_table(table_name)->create_object_with_primary_key(456);
        wt.commit();
    }

    // Trigger a client reset
    {
        // get a fresh copy from the server to reset against
        SHARED_GROUP_TEST_PATH(path_fresh);
        {
            Session session_fresh = fixture.make_session(path_fresh, server_path_1);
            session_fresh.bind();
            session_fresh.wait_for_download_complete_or_client_stopped();
        }
        DBRef sg_fresh = DB::create(make_client_replication(), path_fresh);

        Session::Config session_config;
        {
            session_config.client_reset_config = Session::Config::ClientReset{};
            session_config.client_reset_config->mode = ClientResyncMode::DiscardLocal;
            session_config.client_reset_config->fresh_copy = std::move(sg_fresh);
        }

        Session session = fixture.make_bound_session(sg, server_path_1, std::move(session_config));
        session.wait_for_download_complete_or_client_stopped();
    }
}

void mark_as_synchronized(DB& db)
{
    auto& history = static_cast<ClientReplication*>(db.get_replication())->get_history();
    sync::version_type current_version;
    sync::SaltedFileIdent file_ident;
    sync::SyncProgress progress;
    history.get_status(current_version, file_ident, progress);
    progress.download.last_integrated_client_version = current_version;
    progress.upload.client_version = current_version;
    progress.upload.last_integrated_server_version = current_version;
    sync::VersionInfo info_out;
    history.set_sync_progress(progress, nullptr, info_out);
}

void expect_reset(unit_test::TestContext& test_context, DB& target, DB& fresh, ClientResyncMode mode,
                  bool allow_recovery = true)
{
    auto db_version = target.get_version_of_latest_snapshot();
    auto fresh_path = fresh.get_path();
    bool did_reset = _impl::client_reset::perform_client_reset(
        *test_context.logger, target, fresh, mode, nullptr, nullptr, {100, 200}, nullptr, [](int64_t) {},
        allow_recovery);
    CHECK(did_reset);

    // Should have closed and deleted the fresh realm
    CHECK_NOT(fresh.is_attached());
    CHECK_NOT(util::File::exists(fresh_path));

    // Should have performed exactly one write on the target DB
    CHECK_EQUAL(target.get_version_of_latest_snapshot(), db_version + 1);

    // Should have set the client file ident
    CHECK_EQUAL(target.start_read()->get_sync_file_id(), 100);

    // Client resets aren't marked as complete until the server has acknowledged
    // sync completion to avoid reset cycles
    {
        auto wt = target.start_write();
        _impl::client_reset::remove_pending_client_resets(*wt);
        wt->commit();
    }
}

void expect_reset(unit_test::TestContext& test_context, DB& target, DB& fresh, ClientResyncMode mode,
                  SubscriptionStore* sub_store)
{
    auto db_version = target.get_version_of_latest_snapshot();
    auto fresh_path = fresh.get_path();
    bool did_reset = _impl::client_reset::perform_client_reset(
        *test_context.logger, target, fresh, mode, nullptr, nullptr, {100, 200}, sub_store, [](int64_t) {}, true);
    CHECK(did_reset);

    // Should have closed and deleted the fresh realm
    CHECK_NOT(fresh.is_attached());
    CHECK_NOT(util::File::exists(fresh_path));

    // Should have performed exactly one write on the target DB
    CHECK_EQUAL(target.get_version_of_latest_snapshot(), db_version + 1);

    // Should have set the client file ident
    CHECK_EQUAL(target.start_read()->get_sync_file_id(), 100);

    // Client resets aren't marked as complete until the server has acknowledged
    // sync completion to avoid reset cycles
    {
        auto wt = target.start_write();
        _impl::client_reset::remove_pending_client_resets(*wt);
        wt->commit();
    }
}

std::pair<DBRef, DBRef> prepare_db(const std::string& path, const std::string& copy_path,
                                   util::FunctionRef<void(Transaction&)> fn)
{
    DBRef db = DB::create(make_client_replication(), path);
    {
        auto wt = db->start_write();
        fn(*wt);
        wt->commit();
    }
    mark_as_synchronized(*db);
    db->write_copy(copy_path, nullptr);
    auto db_2 = DB::create(make_client_replication(), copy_path);
    return {db, db_2};
}

TEST(ClientReset_UninitializedFile)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);
    SHARED_GROUP_TEST_PATH(path_3);

    auto [db, db_fresh] = prepare_db(path_1, path_2, [](Transaction& tr) {
        tr.add_table_with_primary_key("class_table", type_Int, "pk");
    });

    auto db_empty = DB::create(make_client_replication(), path_3);
    // Should not perform a client reset because the target file has never been
    // written to
    bool did_reset = _impl::client_reset::perform_client_reset(
        *test_context.logger, *db_empty, *db_fresh, ClientResyncMode::Recover, nullptr, nullptr, {100, 200}, nullptr,
        [](int64_t) {}, true);
    CHECK_NOT(did_reset);

    // Should still have closed and deleted the fresh realm
    CHECK_NOT(db_fresh->is_attached());
    CHECK_NOT(util::File::exists(path_2));
}

TEST(ClientReset_NoChanges)
{
    SHARED_GROUP_TEST_PATH(path);
    SHARED_GROUP_TEST_PATH(path_fresh);
    SHARED_GROUP_TEST_PATH(path_backup);

    DBRef db = DB::create(make_client_replication(), path);
    {
        auto wt = db->start_write();
        auto table = wt->add_table_with_primary_key("class_table", type_Int, "pk");
        table->create_object_with_primary_key(1);
        table->create_object_with_primary_key(2);
        table->create_object_with_primary_key(3);
        wt->commit();
    }
    mark_as_synchronized(*db);

    // Write a copy of the pre-reset state to compare against
    db->write_copy(path_backup, nullptr);
    DBOptions options;
    options.is_immutable = true;
    auto backup_db = DB::create(path_backup, true, options);

    const ClientResyncMode modes[] = {ClientResyncMode::Recover, ClientResyncMode::DiscardLocal,
                                      ClientResyncMode::RecoverOrDiscard};
    for (auto mode : modes) {
        // Perform a reset with a fresh Realm that exactly matches the current
        // one, which shouldn't result in any changes regardless of mode
        db->write_copy(path_fresh, nullptr);
        expect_reset(test_context, *db, *DB::create(make_client_replication(), path_fresh), mode);

        // End state should exactly match the pre-reset state
        CHECK_OR_RETURN(compare_groups(*db->start_read(), *backup_db->start_read()));
    }
}

TEST(ClientReset_SimpleNonconflictingChanges)
{
    const std::pair<ClientResyncMode, bool> modes[] = {
        {ClientResyncMode::Recover, true},
        {ClientResyncMode::RecoverOrDiscard, true},
        {ClientResyncMode::RecoverOrDiscard, false},
        {ClientResyncMode::DiscardLocal, false},
    };
    for (auto [mode, allow_recovery] : modes) {
        SHARED_GROUP_TEST_PATH(path_1);
        SHARED_GROUP_TEST_PATH(path_2);

        auto [db, db_fresh] = prepare_db(path_1, path_2, [](Transaction& tr) {
            auto table = tr.add_table_with_primary_key("class_table", type_Int, "pk");
            table->create_object_with_primary_key(1);
            table->create_object_with_primary_key(2);
            table->create_object_with_primary_key(3);
        });

        for (int i = 0; i < 5; ++i) {
            auto wt = db->start_write();
            auto table = wt->get_table("class_table");
            table->create_object_with_primary_key(4 + i);
            wt->commit();
        }

        {
            auto wt = db_fresh->start_write();
            auto table = wt->get_table("class_table");
            for (int i = 0; i < 5; ++i) {
                table->create_object_with_primary_key(10 + i);
            }
            wt->commit();
        }

        expect_reset(test_context, *db, *db_fresh, mode, allow_recovery);

        if (allow_recovery) {
            // Should have both the objects created locally and from the reset realm
            auto tr = db->start_read();
            auto table = tr->get_table("class_table");
            CHECK_EQUAL(table->size(), 13);
        }
        else {
            // Should only have the objects from the fresh realm
            auto tr = db->start_read();
            auto table = tr->get_table("class_table");
            CHECK_EQUAL(table->size(), 8);
            CHECK(table->get_object_with_primary_key(10));
            CHECK_NOT(table->get_object_with_primary_key(4));
        }
    }
}

TEST(ClientReset_SimpleConflictingWrites)
{
    const std::pair<ClientResyncMode, bool> modes[] = {
        {ClientResyncMode::Recover, true},
        {ClientResyncMode::RecoverOrDiscard, true},
        {ClientResyncMode::RecoverOrDiscard, false},
        {ClientResyncMode::DiscardLocal, false},
    };
    for (auto [mode, allow_recovery] : modes) {
        SHARED_GROUP_TEST_PATH(path_1);
        SHARED_GROUP_TEST_PATH(path_2);

        auto [db, db_fresh] = prepare_db(path_1, path_2, [](Transaction& tr) {
            auto table = tr.add_table_with_primary_key("class_table", type_Int, "pk");
            table->add_column(type_Int, "value");
            table->create_object_with_primary_key(1).set_all(1);
            table->create_object_with_primary_key(2).set_all(2);
            table->create_object_with_primary_key(3).set_all(3);
        });

        {
            auto wt = db->start_write();
            auto table = wt->get_table("class_table");
            for (auto&& obj : *table) {
                obj.set_all(obj.get<int64_t>("value") + 10);
            }
            wt->commit();
        }

        {
            auto wt = db_fresh->start_write();
            auto table = wt->get_table("class_table");
            for (auto&& obj : *table) {
                obj.set_all(0);
            }
            wt->commit();
        }

        expect_reset(test_context, *db, *db_fresh, mode, allow_recovery);

        auto tr = db->start_read();
        auto table = tr->get_table("class_table");
        CHECK_EQUAL(table->size(), 3);
        if (allow_recovery) {
            CHECK_EQUAL(table->get_object_with_primary_key(1).get<int64_t>("value"), 11);
            CHECK_EQUAL(table->get_object_with_primary_key(2).get<int64_t>("value"), 12);
            CHECK_EQUAL(table->get_object_with_primary_key(3).get<int64_t>("value"), 13);
        }
        else {
            CHECK_EQUAL(table->get_object_with_primary_key(1).get<int64_t>("value"), 0);
            CHECK_EQUAL(table->get_object_with_primary_key(2).get<int64_t>("value"), 0);
            CHECK_EQUAL(table->get_object_with_primary_key(3).get<int64_t>("value"), 0);
        }
    }
}

TEST(ClientReset_Recover_RecoveryDisabled)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);

    auto dbs = prepare_db(path_1, path_2, [](Transaction& tr) {
        tr.add_table_with_primary_key("class_table", type_Int, "pk");
    });
    CHECK_THROW((_impl::client_reset::perform_client_reset(
                    *test_context.logger, *dbs.first, *dbs.second, ClientResyncMode::Recover, nullptr, nullptr,
                    {100, 200}, nullptr, [](int64_t) {}, false)),
                _impl::client_reset::ClientResetFailed);
    CHECK_NOT(_impl::client_reset::has_pending_reset(*dbs.first->start_read()));
}

TEST(ClientReset_Recover_ModificationsOnDeletedObject)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);

    ColKey col;
    auto [db, db_fresh] = prepare_db(path_1, path_2, [&](Transaction& tr) {
        auto table = tr.add_table_with_primary_key("class_table", type_Int, "pk");
        col = table->add_column(type_Int, "value");
        table->create_object_with_primary_key(1).set_all(1);
        table->create_object_with_primary_key(2).set_all(2);
        table->create_object_with_primary_key(3).set_all(3);
    });

    {
        auto wt = db->start_write();
        auto table = wt->get_table("class_table");
        table->get_object(0).set<int64_t>(col, 11);
        table->get_object(1).add_int(col, 10);
        table->get_object(2).set<int64_t>(col, 13);
        wt->commit();
    }
    {
        auto wt = db_fresh->start_write();
        auto table = wt->get_table("class_table");
        table->get_object(0).remove();
        table->get_object(0).remove();
        wt->commit();
    }

    expect_reset(test_context, *db, *db_fresh, ClientResyncMode::Recover);

    auto tr = db->start_read();
    auto table = tr->get_table("class_table");
    CHECK_EQUAL(table->size(), 1);
    CHECK_EQUAL(table->get_object_with_primary_key(3).get<int64_t>("value"), 13);
}

SubscriptionSet add_subscription(SubscriptionStore& sub_store, const std::string& name, const Query& q,
                                 std::optional<SubscriptionSet::State> state = std::nullopt)
{
    auto mut = sub_store.get_latest().make_mutable_copy();
    mut.insert_or_assign(name, q);
    if (state) {
        mut.update_state(*state);
    }
    return mut.commit();
}

TEST(ClientReset_DiscardLocal_DiscardsPendingSubscriptions)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);
    auto [db, db_fresh] = prepare_db(path_1, path_2, [&](Transaction& tr) {
        tr.add_table_with_primary_key("class_table", type_Int, "pk");
    });

    auto tr = db->start_read();
    Query query = tr->get_table("class_table")->where();
    auto sub_store = SubscriptionStore::create(db);
    add_subscription(*sub_store, "complete", query, SubscriptionSet::State::Complete);

    std::vector<SubscriptionSet> pending_sets;
    std::vector<util::Future<SubscriptionSet::State>> futures;
    for (int i = 0; i < 3; ++i) {
        auto set = add_subscription(*sub_store, util::format("pending %1", i), query);
        futures.push_back(set.get_state_change_notification(SubscriptionSet::State::Complete));
        pending_sets.push_back(std::move(set));
    }

    expect_reset(test_context, *db, *db_fresh, ClientResyncMode::DiscardLocal, sub_store.get());

    CHECK(sub_store->get_pending_subscriptions().empty());
    auto subs = sub_store->get_latest();
    CHECK_EQUAL(subs.state(), SubscriptionSet::State::Complete);
    CHECK_EQUAL(subs.size(), 1);
    CHECK_EQUAL(subs.at(0).name, "complete");

    for (auto& fut : futures) {
        CHECK_EQUAL(fut.get(), SubscriptionSet::State::Superseded);
    }
    for (auto& set : pending_sets) {
        CHECK_EQUAL(set.state(), SubscriptionSet::State::Pending);
        set.refresh();
        CHECK_EQUAL(set.state(), SubscriptionSet::State::Superseded);
    }
}

TEST(ClientReset_DiscardLocal_MakesAwaitingMarkActiveSubscriptionsComplete)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);
    auto [db, db_fresh] = prepare_db(path_1, path_2, [&](Transaction& tr) {
        tr.add_table_with_primary_key("class_table", type_Int, "pk");
    });

    auto tr = db->start_read();
    Query query = tr->get_table("class_table")->where();
    auto sub_store = SubscriptionStore::create(db);
    auto set = add_subscription(*sub_store, "complete", query, SubscriptionSet::State::AwaitingMark);
    auto future = set.get_state_change_notification(SubscriptionSet::State::Complete);

    expect_reset(test_context, *db, *db_fresh, ClientResyncMode::DiscardLocal, sub_store.get());

    CHECK_EQUAL(future.get(), SubscriptionSet::State::Complete);
    CHECK_EQUAL(set.state(), SubscriptionSet::State::AwaitingMark);
    set.refresh();
    CHECK_EQUAL(set.state(), SubscriptionSet::State::Complete);
}

} // unnamed namespace
