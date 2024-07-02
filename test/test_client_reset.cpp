#include <realm/db.hpp>
#include <realm/list.hpp>
#include <realm/object_converter.hpp>
#include <realm/sync/noinst/client_reset.hpp>
#include <realm/sync/noinst/client_reset_operation.hpp>
#include <realm/sync/noinst/pending_reset_store.hpp>
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

#if !REALM_MOBILE
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
            Session::Config config;
            config.connection_state_change_listener = [&](ConnectionState state,
                                                          util::Optional<ErrorInfo> error_info) {
                if (state != ConnectionState::disconnected)
                    return;
                REALM_ASSERT(error_info);
                CHECK_EQUAL(error_info->status, ErrorCodes::SyncClientResetRequired);
                CHECK_EQUAL(static_cast<ProtocolError>(error_info->raw_error_code),
                            ProtocolError::bad_server_version);
                bowl.add_stone();
            };

            Session session = fixture.make_session(path_2, server_path, std::move(config));
            bowl.get_stone();
        }

        // get a fresh copy from the server to reset against
        SHARED_GROUP_TEST_PATH(path_fresh);
        {
            Session session_fresh = fixture.make_session(path_fresh, server_path);
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
                Session::Config::ClientReset cr_config{
                    ClientResyncMode::DiscardLocal,
                    sg_fresh,
                    {ErrorCodes::SyncClientResetRequired, "Bad client file identifier (IDENT)"}};
                session_config.client_reset_config = std::move(cr_config);
            }
            Session session = fixture.make_session(sg, server_path, std::move(session_config));
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
        session_fresh.wait_for_download_complete_or_client_stopped();
    }
    DBRef sg_fresh = DB::create(make_client_replication(), path_fresh);

    // Start a client reset. There is no need for a reset, but we can do it.
    Session::Config session_config_2;
    Session::Config::ClientReset cr_config{
        ClientResyncMode::DiscardLocal,
        sg_fresh,
        {ErrorCodes::SyncClientResetRequired, "Bad client file identifier (IDENT)"}};
    session_config_2.client_reset_config = std::move(cr_config);
    Session session_2 = fixture.make_session(db_2, server_path, std::move(session_config_2));
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
        session4.wait_for_download_complete_or_client_stopped();
    }
    DBRef sg_fresh1 = DB::create(make_client_replication(), path_fresh1);

    Session::Config::ClientReset cr_config{
        recover ? ClientResyncMode::Recover : ClientResyncMode::DiscardLocal,
        sg_fresh1,
        {ErrorCodes::SyncClientResetRequired, "Bad client file identifier (IDENT)"},
        recover ? sync::ProtocolErrorInfo::Action::ClientReset
                : sync::ProtocolErrorInfo::Action::ClientResetNoRecovery};
    Session::Config session_config_3;
    session_config_3.client_reset_config = std::move(cr_config);
    Session session_3 = fixture.make_session(sg, server_path, std::move(session_config_3));
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
        Session session_2 = fixture.make_session(path_2, server_path);

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
            auto config = [&] {
                Session::Config config;
                config.connection_state_change_listener = [&](ConnectionState state,
                                                              util::Optional<ErrorInfo> error_info) {
                    if (state != ConnectionState::disconnected)
                        return;
                    REALM_ASSERT(error_info);
                    CHECK_EQUAL(error_info->status, ErrorCodes::SyncClientResetRequired);
                    CHECK_EQUAL(static_cast<ProtocolError>(error_info->raw_error_code),
                                ProtocolError::bad_server_version);
                    bowl.add_stone();
                };
                return config;
            };

            Session session_1 = fixture.make_session(path_1, server_path, config());
            Session session_2 = fixture.make_session(path_2, server_path, config());
            bowl.get_stone();
            bowl.get_stone();
        }

        // get a fresh copy from the server to reset against
        SHARED_GROUP_TEST_PATH(path_fresh1);
        SHARED_GROUP_TEST_PATH(path_fresh2);
        {
            Session session4 = fixture.make_session(path_fresh1, server_path);
            session4.wait_for_download_complete_or_client_stopped();
        }
        DBRef sg_fresh1 = DB::create(make_client_replication(), path_fresh1);

        {
            Session session4 = fixture.make_session(path_fresh2, server_path);
            session4.wait_for_download_complete_or_client_stopped();
        }
        DBRef sg_fresh2 = DB::create(make_client_replication(), path_fresh2);

        // Perform client resets on the two clients.
        {
            Session::Config session_config_1;
            {
                Session::Config::ClientReset cr_config{
                    ClientResyncMode::DiscardLocal,
                    sg_fresh1,
                    {ErrorCodes::SyncClientResetRequired, "Bad client file identifier (IDENT)"}};
                session_config_1.client_reset_config = std::move(cr_config);
            }
            Session::Config session_config_2;
            {
                Session::Config::ClientReset cr_config{
                    ClientResyncMode::DiscardLocal,
                    sg_fresh2,
                    {ErrorCodes::SyncClientResetRequired, "Bad client file identifier (IDENT)"}};
                session_config_2.client_reset_config = std::move(cr_config);
            }
            Session session_1 = fixture.make_session(path_1, server_path, std::move(session_config_1));
            Session session_2 = fixture.make_session(path_2, server_path, std::move(session_config_2));

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
        Session session_2 = fixture.make_session(path_2, server_path);

        session_1.wait_for_upload_complete_or_client_stopped();
        session_2.wait_for_upload_complete_or_client_stopped();
        session_1.wait_for_download_complete_or_client_stopped();
        session_2.wait_for_download_complete_or_client_stopped();

        std::this_thread::sleep_for(std::chrono::milliseconds{1000});

        // A third client downloads the state
        {
            Session session = fixture.make_session(path_3, server_path);
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
        session_fresh.wait_for_download_complete_or_client_stopped();
    }
    DBRef sg_fresh1 = DB::create(make_client_replication(), path_fresh1);

    // Perform client reset for path_1 against server_path_2.
    // This attempts to remove the added class and this destructive
    // schema change is not allowed and so fails with a client reset error.
    {
        Session::Config session_config;
        std::string error_msg = "Some bad client file identifier (IDENT)";
        {
            Session::Config::ClientReset cr_config{ClientResyncMode::DiscardLocal,
                                                   sg_fresh1,
                                                   {ErrorCodes::SyncClientResetRequired, error_msg},
                                                   sync::ProtocolErrorInfo::Action::ClientReset};
            session_config.client_reset_config = std::move(cr_config);
        }

        BowlOfStonesSemaphore bowl;
        session_config.connection_state_change_listener = [&](ConnectionState state,
                                                              util::Optional<ErrorInfo> error_info) {
            if (state != ConnectionState::disconnected)
                return;
            REALM_ASSERT(error_info);
            CHECK_EQUAL(error_info->status, ErrorCodes::AutoClientResetFailed);
            CHECK(error_info->status.reason().find(error_msg) != std::string::npos);
            bowl.add_stone();
        };
        Session session = fixture.make_session(path_1, server_path_2, std::move(session_config));
        bowl.get_stone();
    }

    {
        DBRef sg_1 = DB::create(make_client_replication(), path_1);
        DBRef sg_2 = DB::create(make_client_replication(), path_2);

        ReadTransaction rt_1(sg_1);
        ReadTransaction rt_2(sg_2);
        CHECK(!compare_groups(rt_1, rt_2));

        const Group& group = rt_1.get_group();
        CHECK_EQUAL(group.size(), 3);
        CHECK(group.get_table("class_table1"));
        CHECK(group.get_table("client_reset_metadata"));
        CHECK(group.get_table("sync_internal_schemas"));
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
            session_fresh.wait_for_download_complete_or_client_stopped();
        }
        DBRef sg_fresh = DB::create(make_client_replication(), path_fresh);

        Session::Config session_config;
        {
            Session::Config::ClientReset cr_config{
                ClientResyncMode::DiscardLocal,
                sg_fresh,
                {ErrorCodes::SyncClientResetRequired, "Bad client file identifier (IDENT)"}};
            session_config.client_reset_config = std::move(cr_config);
        }

        Session session = fixture.make_bound_session(sg, server_path_1, std::move(session_config));
        session.wait_for_download_complete_or_client_stopped();
    }
}
#endif // !REALM_MOBILE

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
    history.set_sync_progress(progress, 0, info_out);
    history.set_client_file_ident({1, 0}, false);
}

void expect_reset(unit_test::TestContext& test_context, DBRef& target, DBRef& fresh, ClientResyncMode mode,
                  SubscriptionStore* sub_store = nullptr, bool allow_recovery = true)
{
    CHECK(target);
    CHECK(fresh);
    // Ensure the schema is initialized before starting the test
    {
        auto wr_tr = target->start_write();
        PendingResetStore::clear_pending_reset(wr_tr);
        wr_tr->commit();
    }

    auto db_version = target->get_version_of_latest_snapshot();
    auto fresh_path = fresh->get_path();
    Status error{ErrorCodes::SyncClientResetRequired, "Bad client file identifier (IDENT)"};
    auto action = allow_recovery ? sync::ProtocolErrorInfo::Action::ClientReset
                                 : sync::ProtocolErrorInfo::Action::ClientResetNoRecovery;
    // Pending reset store doesn't save RecoverOrDiscard
    auto expected_mode = [](ClientResyncMode mode, bool allow_recovery) {
        if (mode != ClientResyncMode::RecoverOrDiscard)
            return mode;
        if (allow_recovery)
            return ClientResyncMode::Recover;
        return ClientResyncMode::DiscardLocal;
    }(mode, allow_recovery);

    sync::ClientReset cr_config{mode, fresh, error, action};

    bool did_reset = _impl::client_reset::perform_client_reset(*test_context.logger, *target, std::move(cr_config),
                                                               {100, 200}, sub_store, [](int64_t) {});
    CHECK(did_reset);

    // Should have closed and deleted the fresh realm
    CHECK_NOT(fresh->is_attached());
    CHECK_NOT(util::File::exists(fresh_path));

    // Should have performed exactly two writes on the target DB: one to track
    // that we're attempting recovery, and one with the actual reset
    CHECK_EQUAL(target->get_version_of_latest_snapshot(), db_version + 2);

    // Should have set the client file ident
    CHECK_EQUAL(target->start_read()->get_sync_file_id(), 100);

    // Client resets aren't marked as complete until the server has acknowledged
    // sync completion to avoid reset cycles
    {
        auto tr = target->start_read();
        auto pending_reset = PendingResetStore::has_pending_reset(tr);
        CHECK(pending_reset);
        CHECK(pending_reset->action == action);
        CHECK(pending_reset->mode == expected_mode);
        CHECK(pending_reset->error == error);
        tr->promote_to_write();
        PendingResetStore::clear_pending_reset(tr);
        tr->commit_and_continue_as_read();
        CHECK_NOT(PendingResetStore::has_pending_reset(tr));
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

TEST(ClientReset_ConvertResyncMode)
{
    CHECK(PendingResetStore::to_resync_mode(0) == ClientResyncMode::DiscardLocal);
    CHECK(PendingResetStore::to_resync_mode(1) == ClientResyncMode::Recover);
    CHECK_THROW(PendingResetStore::to_resync_mode(2), sync::ClientResetFailed);

    CHECK(PendingResetStore::from_resync_mode(ClientResyncMode::DiscardLocal) == 0);
    CHECK(PendingResetStore::from_resync_mode(ClientResyncMode::RecoverOrDiscard) == 1);
    CHECK(PendingResetStore::from_resync_mode(ClientResyncMode::Recover) == 1);
    CHECK_THROW(PendingResetStore::from_resync_mode(ClientResyncMode::Manual), sync::ClientResetFailed);
}

TEST(ClientReset_ConvertResetAction)
{
    CHECK(PendingResetStore::to_reset_action(0) == sync::ProtocolErrorInfo::Action::NoAction);
    CHECK(PendingResetStore::to_reset_action(1) == sync::ProtocolErrorInfo::Action::ClientReset);
    CHECK(PendingResetStore::to_reset_action(2) == sync::ProtocolErrorInfo::Action::ClientResetNoRecovery);
    CHECK(PendingResetStore::to_reset_action(3) == sync::ProtocolErrorInfo::Action::MigrateToFLX);
    CHECK(PendingResetStore::to_reset_action(4) == sync::ProtocolErrorInfo::Action::RevertToPBS);
    CHECK(PendingResetStore::to_reset_action(5) == sync::ProtocolErrorInfo::Action::NoAction);

    CHECK(PendingResetStore::from_reset_action(sync::ProtocolErrorInfo::Action::ClientReset) == 1);
    CHECK(PendingResetStore::from_reset_action(sync::ProtocolErrorInfo::Action::ClientResetNoRecovery) == 2);
    CHECK(PendingResetStore::from_reset_action(sync::ProtocolErrorInfo::Action::MigrateToFLX) == 3);
    CHECK(PendingResetStore::from_reset_action(sync::ProtocolErrorInfo::Action::RevertToPBS) == 4);
    CHECK_THROW(PendingResetStore::from_reset_action(sync::ProtocolErrorInfo::Action::MigrateSchema),
                sync::ClientResetFailed);
}

DBRef setup_metadata_table_v1(test_util::unit_test::TestContext& test_context, std::string path, Timestamp ts,
                              int64_t type)
{
    DBRef db = DB::create(make_client_replication(), path);
    auto wt = db->start_write();
    auto table = wt->add_table_with_primary_key("client_reset_metadata", type_ObjectId, "id");
    CHECK(table);
    auto version_col = table->add_column(type_Int, "version");
    auto timestamp_col = table->add_column(type_Timestamp, "event_time");
    auto type_col = table->add_column(type_Int, "type_of_reset");
    wt->commit_and_continue_writing();
    auto id = ObjectId::gen();
    table->create_object_with_primary_key(id, {
                                                  {version_col, 1},
                                                  {timestamp_col, ts},
                                                  {type_col, type},
                                              });
    wt->commit_and_continue_as_read();
    table = wt->get_table("client_reset_metadata");
    size_t table_size = table->size();
    CHECK(table_size == 1);
    return db;
}

TEST_TYPES(ClientReset_V1Table, std::integral_constant<ClientResyncMode, ClientResyncMode::DiscardLocal>,
           std::integral_constant<ClientResyncMode, ClientResyncMode::Recover>)
{
    SHARED_GROUP_TEST_PATH(path_v1);
    auto timestamp = Timestamp(std::chrono::system_clock::now());
    auto reset_type = PendingResetStore::from_resync_mode(TEST_TYPE::value);
    DBRef db = setup_metadata_table_v1(test_context, path_v1, timestamp, reset_type);
    auto rd_tr = db->start_read();
    auto reset = PendingResetStore::has_pending_reset(rd_tr);
    CHECK(reset);
    CHECK(reset->time == timestamp);
    CHECK(reset->mode == TEST_TYPE::value);
    if (TEST_TYPE::value == ClientResyncMode::DiscardLocal) {
        CHECK(reset->action == sync::ProtocolErrorInfo::Action::ClientResetNoRecovery);
    }
    else {
        CHECK(reset->action == sync::ProtocolErrorInfo::Action::ClientReset);
    }
}

TEST(ClientReset_TrackReset_V1_EntryExists)
{
    SHARED_GROUP_TEST_PATH(path_v1);
    auto timestamp = Timestamp(std::chrono::system_clock::now());
    auto reset_type = PendingResetStore::from_resync_mode(ClientResyncMode::Recover);
    // Create a previous v1 entry
    DBRef db = setup_metadata_table_v1(test_context, path_v1, timestamp, reset_type);
    auto wr_tr = db->start_write();
    // Should throw an exception, since the table isn't empty
    CHECK_THROW(PendingResetStore::track_reset(wr_tr, ClientResyncMode::DiscardLocal,
                                               sync::ProtocolErrorInfo::Action::RevertToPBS),
                sync::ClientResetFailed);
}

TEST(ClientReset_TrackReset_Existing_empty_V1_table)
{
    SHARED_GROUP_TEST_PATH(path_v1);
    auto timestamp = Timestamp(std::chrono::system_clock::now());
    auto reset_type = PendingResetStore::from_resync_mode(ClientResyncMode::Recover);
    Status error{ErrorCodes::SyncClientResetRequired, "Bad client file ident"};
    DBRef db = setup_metadata_table_v1(test_context, path_v1, timestamp, reset_type);
    auto wr_tr = db->start_write();
    PendingResetStore::clear_pending_reset(wr_tr);
    wr_tr->commit_and_continue_writing();
    PendingResetStore::track_reset(wr_tr, ClientResyncMode::DiscardLocal,
                                   sync::ProtocolErrorInfo::Action::RevertToPBS, error);
    wr_tr->commit_and_continue_as_read();
    auto reset = PendingResetStore::has_pending_reset(wr_tr);
    CHECK(reset);
    CHECK(reset->mode == ClientResyncMode::DiscardLocal);
    CHECK(reset->action == sync::ProtocolErrorInfo::Action::RevertToPBS);
    CHECK(reset->error == error);
    timestamp = Timestamp(std::chrono::system_clock::now());
    // Verify timestamp is at least close to current time
    CHECK(abs(reset->time.get_seconds() - timestamp.get_seconds()) < 5);
}

TEST_TYPES(
    ClientReset_TrackReset_v2,
    std::integral_constant<sync::ProtocolErrorInfo::Action, sync::ProtocolErrorInfo::Action::ClientReset>,
    std::integral_constant<sync::ProtocolErrorInfo::Action, sync::ProtocolErrorInfo::Action::ClientResetNoRecovery>,
    std::integral_constant<sync::ProtocolErrorInfo::Action, sync::ProtocolErrorInfo::Action::RevertToPBS>,
    std::integral_constant<sync::ProtocolErrorInfo::Action, sync::ProtocolErrorInfo::Action::MigrateToFLX>)
{
    SHARED_GROUP_TEST_PATH(test_path);
    DBRef db = DB::create(make_client_replication(), test_path);
    Status error{ErrorCodes::SyncClientResetRequired, "Bad client file ident"};
    sync::ProtocolErrorInfo::Action reset_action = TEST_TYPE::value;
    auto tr = db->start_write();
    PendingResetStore::track_reset(tr, ClientResyncMode::DiscardLocal, reset_action, error);
    tr->commit_and_continue_as_read();
    auto reset = PendingResetStore::has_pending_reset(tr);
    CHECK(reset);
    CHECK(reset->mode == ClientResyncMode::DiscardLocal);
    CHECK(reset->action == reset_action);
    CHECK(reset->error == error);
    auto timestamp = Timestamp(std::chrono::system_clock::now());
    // Verify timestamp is at least close to current time
    CHECK((reset->time.get_seconds() - timestamp.get_seconds() < 5));
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
    sync::ClientReset cr_config{ClientResyncMode::Recover,
                                db_fresh,
                                {ErrorCodes::SyncClientResetRequired, "Bad client file identifier (IDENT)"}};

    // Should not perform a client reset because the target file has never been
    // written to
    bool did_reset = _impl::client_reset::perform_client_reset(*test_context.logger, *db_empty, std::move(cr_config),
                                                               {100, 200}, nullptr, [](int64_t) {});
    CHECK_NOT(did_reset);
    auto rd_tr = db_empty->start_frozen();
    CHECK_NOT(PendingResetStore::has_pending_reset(rd_tr));

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
    options.no_create = true;
    auto backup_db = DB::create(path_backup, options);

    const ClientResyncMode modes[] = {ClientResyncMode::Recover, ClientResyncMode::DiscardLocal,
                                      ClientResyncMode::RecoverOrDiscard};
    for (auto mode : modes) {
        // Perform a reset with a fresh Realm that exactly matches the current
        // one, which shouldn't result in any changes regardless of mode
        db->write_copy(path_fresh, nullptr);
        auto db_fresh = DB::create(make_client_replication(), path_fresh);
        expect_reset(test_context, db, db_fresh, mode);

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

        expect_reset(test_context, db, db_fresh, mode, nullptr, allow_recovery);

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

        expect_reset(test_context, db, db_fresh, mode, nullptr, allow_recovery);

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
    sync::ClientReset cr_config{ClientResyncMode::Recover,
                                dbs.second,
                                {ErrorCodes::SyncClientResetRequired, "Bad client file identifier (IDENT)"},
                                sync::ProtocolErrorInfo::Action::ClientResetNoRecovery};

    CHECK_THROW((_impl::client_reset::perform_client_reset(*test_context.logger, *dbs.first, std::move(cr_config),
                                                           {100, 200}, nullptr, [](int64_t) {})),
                sync::ClientResetFailed);
    auto rd_tr = dbs.first->start_frozen();
    CHECK_NOT(PendingResetStore::has_pending_reset(rd_tr));
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

    expect_reset(test_context, db, db_fresh, ClientResyncMode::Recover);

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
        mut.set_state(*state);
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

    expect_reset(test_context, db, db_fresh, ClientResyncMode::DiscardLocal, sub_store.get());

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

TEST_TYPES(ClientReset_DiscardLocal_MakesAwaitingMarkActiveSubscriptionsComplete,
           std::integral_constant<ClientResyncMode, ClientResyncMode::DiscardLocal>,
           std::integral_constant<ClientResyncMode, ClientResyncMode::Recover>)
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

    expect_reset(test_context, db, db_fresh, TEST_TYPE::value, sub_store.get());

    CHECK_EQUAL(future.get(), SubscriptionSet::State::Complete);
    CHECK_EQUAL(set.state(), SubscriptionSet::State::AwaitingMark);
    set.refresh();
    CHECK_EQUAL(set.state(), SubscriptionSet::State::Complete);
}

TEST(ClientReset_Recover_DoesNotCompletePendingSubscriptions)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);
    auto [db, db_fresh] = prepare_db(path_1, path_2, [&](Transaction& tr) {
        tr.add_table_with_primary_key("class_table", type_Int, "pk");
    });

    auto tr = db->start_read();
    auto sub_store = SubscriptionStore::create(db);
    auto query = tr->get_table("class_table")->where();

    add_subscription(*sub_store, "complete", query, SubscriptionSet::State::Complete);

    std::vector<util::Future<SubscriptionSet::State>> futures;
    for (int i = 0; i < 3; ++i) {
        auto subs = add_subscription(*sub_store, util::format("pending %1", i), query);
        futures.push_back(subs.get_state_change_notification(SubscriptionSet::State::Complete));
    }

    expect_reset(test_context, db, db_fresh, ClientResyncMode::Recover, sub_store.get());

    for (auto& fut : futures) {
        CHECK_NOT(fut.is_ready());
    }

    auto pending = sub_store->get_pending_subscriptions();
    CHECK_EQUAL(pending.size(), 3);
    for (int i = 0; i < 3; ++i) {
        CHECK_EQUAL(pending[i].size(), i + 2);
        CHECK_EQUAL(std::prev(pending[i].end())->name, util::format("pending %1", i));
    }
}

TEST(ClientReset_Recover_UpdatesRemoteServerVersions)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);
    auto [db, db_fresh] = prepare_db(path_1, path_2, [&](Transaction& tr) {
        tr.add_table_with_primary_key("class_table", type_Int, "pk");
    });

    // Create local unsynchronized changes
    for (int i = 0; i < 5; ++i) {
        auto wt = db->start_write();
        auto table = wt->get_table("class_table");
        table->create_object_with_primary_key(i);
        wt->commit();
    }

    // Change the last seen server version for the freshly download DB
    {
        sync::SyncProgress progress;
        // Set to a valid but incorrect client version which should not be
        // copied over by client reset
        auto client_version = db_fresh->get_version_of_latest_snapshot() - 1;
        progress.download.last_integrated_client_version = client_version;
        progress.upload.client_version = client_version;

        // Server versions are opaque increasing values, so they can be whatever.
        // Set to known values that we can verify are used
        progress.latest_server_version.version = 123;
        progress.latest_server_version.salt = 456;
        progress.download.server_version = 123;
        progress.upload.last_integrated_server_version = 789;

        sync::VersionInfo info_out;
        auto& history = static_cast<ClientReplication*>(db_fresh->get_replication())->get_history();
        history.set_sync_progress(progress, 0, info_out);
    }

    expect_reset(test_context, db, db_fresh, ClientResyncMode::Recover, nullptr);

    auto& history = static_cast<ClientReplication*>(db->get_replication())->get_history();
    history.ensure_updated(db->get_version_of_latest_snapshot());

    version_type current_client_version;
    SaltedFileIdent file_ident;
    SyncProgress sync_progress;
    history.get_status(current_client_version, file_ident, sync_progress);

    CHECK_EQUAL(file_ident.ident, 100);
    CHECK_EQUAL(file_ident.salt, 200);
    CHECK_EQUAL(sync_progress.upload.client_version, 0);
    CHECK_EQUAL(sync_progress.download.last_integrated_client_version, 0);
    CHECK_EQUAL(sync_progress.upload.last_integrated_server_version, 123);
    CHECK_EQUAL(sync_progress.download.server_version, 123);

    std::vector<ClientHistory::UploadChangeset> uploadable_changesets;
    version_type locked_server_version;
    history.find_uploadable_changesets(sync_progress.upload, db->get_version_of_latest_snapshot(),
                                       uploadable_changesets, locked_server_version);

    CHECK_EQUAL(uploadable_changesets.size(), 5);
    for (auto& uc : uploadable_changesets) {
        CHECK_EQUAL(uc.progress.last_integrated_server_version, 123);
    }
}

TEST(ClientReset_Recover_UploadableBytes)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);
    auto [db, db_fresh] = prepare_db(path_1, path_2, [&](Transaction& tr) {
        tr.add_table_with_primary_key("class_table", type_Int, "pk");
    });

    // Create local unsynchronized changes
    for (int i = 0; i < 5; ++i) {
        auto wt = db->start_write();
        auto table = wt->get_table("class_table");
        table->create_object_with_primary_key(i);
        wt->commit();
    }

    // Create some of the same objects in the fresh realm so that the post-reset
    // uploadable_bytes should be different from pre-reset (but still not zero)
    {
        auto wt = db_fresh->start_write();
        auto table = wt->get_table("class_table");
        for (int i = 0; i < 3; ++i) {
            table->create_object_with_primary_key(i);
        }
        wt->commit();
    }

    auto& history = static_cast<ClientReplication*>(db->get_replication())->get_history();
    uint_fast64_t unused, pre_reset_uploadable_bytes;
    DownloadableProgress unused_progress;
    version_type unused_version;
    history.get_upload_download_state(*db, unused, unused_progress, unused, pre_reset_uploadable_bytes, unused,
                                      unused_version);
    CHECK_GREATER(pre_reset_uploadable_bytes, 0);

    expect_reset(test_context, db, db_fresh, ClientResyncMode::Recover, nullptr);

    uint_fast64_t post_reset_uploadable_bytes;
    history.get_upload_download_state(*db, unused, unused_progress, unused, post_reset_uploadable_bytes, unused,
                                      unused_version);
    CHECK_GREATER(post_reset_uploadable_bytes, 0);
    CHECK_GREATER(pre_reset_uploadable_bytes, post_reset_uploadable_bytes);
}

TEST(ClientReset_Recover_ListsAreOnlyCopiedOnce)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);
    auto [db, db_fresh] = prepare_db(path_1, path_2, [&](Transaction& tr) {
        auto table = tr.add_table_with_primary_key("class_table", type_Int, "pk");
        auto col = table->add_column_list(type_Int, "list");
        auto list = table->create_object_with_primary_key(0).get_list<Int>(col);
        list.add(0);
        list.add(1);
        list.add(2);
    });

    // Perform some conflicting list writes which aren't recoverable and require
    // a copy
    { // modify local
        auto wt = db->start_write();
        auto list = wt->get_table("class_table")->begin()->get_list<Int>("list");
        list.remove(0);
        list.add(4);
        wt->commit_and_continue_writing();
        list.remove(0);
        list.add(5);
        wt->commit_and_continue_writing();
        list.remove(0);
        list.add(6);
        wt->commit();
    }
    { // modify remote
        auto wt = db_fresh->start_write();
        auto list = wt->get_table("class_table")->begin()->get_list<Int>("list");
        list.clear();
        list.add(7);
        list.add(8);
        list.add(9);
        wt->commit();
    }

    expect_reset(test_context, db, db_fresh, ClientResyncMode::Recover, nullptr);

    // List should match the pre-reset local state
    auto rt = db->start_read();
    auto list = rt->get_table("class_table")->begin()->get_list<Int>("list");
    CHECK_EQUAL(list.size(), 3);
    CHECK_EQUAL(list.get(0), 4);
    CHECK_EQUAL(list.get(1), 5);
    CHECK_EQUAL(list.get(2), 6);

    // The second and third changeset should now be empty and so excluded from
    // get_local_changes()
    auto repl = static_cast<ClientReplication*>(db->get_replication());
    auto changes = repl->get_history().get_local_changes(rt->get_version());
    CHECK_EQUAL(changes.size(), 1);
}

TEST(ClientReset_Recover_RecoverableChangesOnListsAfterUnrecoverableAreNotDuplicated)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);
    auto [db, db_fresh] = prepare_db(path_1, path_2, [&](Transaction& tr) {
        auto table = tr.add_table_with_primary_key("class_table", type_Int, "pk");
        auto col = table->add_column_list(type_Int, "list");
        auto list = table->create_object_with_primary_key(0).get_list<Int>(col);
        list.add(0);
        list.add(1);
    });

    auto sub_store = SubscriptionStore::create(db);
    add_subscription(*sub_store, "complete", db->start_read()->get_table("class_table")->where(),
                     SubscriptionSet::State::Complete);

    { // offline modify local
        auto wt = db->start_write();
        auto list = wt->get_table("class_table")->begin()->get_list<Int>("list");
        // triggers a copy since it's unrecoverable
        list.remove(0);
        list.add(4);
        wt->commit_and_continue_as_read();

        // Pending subscription in between the two writes makes this recovered
        // in a second write, which shouldn't actually do anything as the new
        // element was already added by the copy
        add_subscription(*sub_store, "pending 1", wt->get_table("class_table")->where());
        wt->promote_to_write();
        list.add(5);
        wt->commit();
    }
    { // remote modification that should be discarded
        auto wt = db_fresh->start_write();
        auto list = wt->get_table("class_table")->begin()->get_list<Int>("list");
        list.clear();
        list.add(8);
        wt->commit();
    }

    expect_reset(test_context, db, db_fresh, ClientResyncMode::Recover, sub_store.get());

    // List should match the pre-reset local state
    auto rt = db->start_read();
    auto list = rt->get_table("class_table")->begin()->get_list<Int>("list");
    CHECK_EQUAL(list.size(), 3);
    CHECK_EQUAL(list.get(0), 1);
    CHECK_EQUAL(list.get(1), 4);
    CHECK_EQUAL(list.get(2), 5);

    // The second changeset should now be empty and so excluded from get_local_changes()
    auto repl = static_cast<ClientReplication*>(db->get_replication());
    auto changes = repl->get_history().get_local_changes(rt->get_version());
    CHECK_EQUAL(changes.size(), 1);
}

// Apply uploaded changes in src to dst as if they had been exchanged by sync
void apply_changes(DB& src, DB& dst)
{
    auto& src_history = static_cast<ClientReplication*>(src.get_replication())->get_history();
    auto& dst_history = static_cast<ClientReplication*>(dst.get_replication())->get_history();

    version_type dst_client_version;
    SaltedFileIdent dst_file_ident;
    SyncProgress dst_progress;
    dst_history.get_status(dst_client_version, dst_file_ident, dst_progress);

    std::vector<util::AppendBuffer<char>> decompressed_changesets;
    std::vector<RemoteChangeset> remote_changesets;
    auto local_changes = src_history.get_local_changes(src.get_version_of_latest_snapshot());
    for (auto& change : local_changes) {
        decompressed_changesets.emplace_back();
        auto& buffer = decompressed_changesets.back();
        ChunkedBinaryInputStream is{change.changeset};
        util::compression::decompress_nonportable(is, buffer);

        // Arbitrary non-zero file ident
        file_ident_type file_ident = 2;
        // Treat src's changesets as being "after" dst's
        uint_fast64_t timestamp = -1;
        remote_changesets.emplace_back(change.version, dst_progress.upload.last_integrated_server_version,
                                       BinaryData(buffer.data(), buffer.size()), timestamp, file_ident);
    }

    dst_progress.download.server_version += remote_changesets.size();
    dst_progress.latest_server_version.version += remote_changesets.size();

    util::NullLogger logger;
    VersionInfo new_version;
    dst_history.integrate_server_changesets(dst_progress, 0, remote_changesets, new_version,
                                            DownloadBatchState::SteadyState, logger, dst.start_read());
}

TEST(ClientReset_Recover_ReciprocalListChanges)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);
    auto [db, db_fresh] = prepare_db(path_1, path_2, [&](Transaction& tr) {
        auto table = tr.add_table_with_primary_key("class_table", type_Int, "pk");
        auto col = table->add_column_list(type_Int, "list");
        auto list = table->create_object_with_primary_key(0).get_list<Int>(col);
        for (int i = 0; i < 5; ++i) {
            list.add(i * 10);
        }
    });

    {
        auto wt = db->start_write();
        auto list = wt->get_table("class_table")->begin()->get_list<Int>("list");
        for (int i = 0; i < 5; ++i) {
            list.insert(i * 2 + 1, i * 10 + 1);
        }
        // list is now [0, 1, 10, 11, 20, 21, 30, 31, 40, 41]
        wt->commit();
    }

    {
        auto wt = db_fresh->start_write();
        auto list = wt->get_table("class_table")->begin()->get_list<Int>("list");
        for (int i = 0; i < 5; ++i) {
            list.insert(i * 2 + 1, i * 10 + 2);
        }
        // list is now [0, 2, 10, 12, 20, 22, 30, 32, 40, 42]
        wt->commit();
    }

    // Apply the changes in db_fresh to db as if it was a changeset downloaded
    // from the server. This creates reciprocal history for the unuploaded
    // changeset in db.
    // list is now [0, 1, 2, 10, 11, 12, 20, 21, 22, 30, 31, 32, 40, 41, 42]
    apply_changes(*db_fresh, *db);

    // The local realm is fully up-to-date with the server, so this client reset
    // shouldn't modify the group. However, if it reapplied the original changesets
    // and not the reciprocal history, it'd result in the list being
    // [0, 1, 2, 11, 10, 21, 12, 31, 20, 41, 22, 30, 32, 40, 42]
    expect_reset(test_context, db, db_fresh, ClientResyncMode::Recover, nullptr);

    auto rt = db->start_read();
    auto list = rt->get_table("class_table")->begin()->get_list<Int>("list");
    CHECK_OR_RETURN(list.size() == 15);
    for (int i = 0; i < 5; ++i) {
        CHECK_EQUAL(list[i * 3], i * 10);
        CHECK_EQUAL(list[i * 3 + 1], i * 10 + 1);
        CHECK_EQUAL(list[i * 3 + 2], i * 10 + 2);
    }
}

TEST(ClientReset_Recover_UpdatesReciprocalHistory)
{
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);
    SHARED_GROUP_TEST_PATH(path_3);

    auto [db, db_fresh] = prepare_db(path_1, path_2, [&](Transaction& tr) {
        auto table = tr.add_table_with_primary_key("class_table", type_Int, "pk");
        auto col = table->add_column_list(type_Int, "list");
        table->create_object_with_primary_key(0).get_list<Int>(col).add(0);
    });

    { // local online write that doesn't get uploaded
        auto wt = db->start_write();
        // This instruction is merged with the add in the remote write,
        // generating reciprocal history. It is then discarded when replaying
        // onto the fresh realm in the client reset as the object will no longer
        // exist at that point
        wt->get_table("class_table")->begin()->get_list<Int>("list").add(1);
        // An instruction that won't get discarded when replaying to ensure
        // the changeset remains non-empty
        wt->get_table("class_table")->create_object_with_primary_key(1);
        wt->commit();
    }

    { // remote write which gets sent to the client in a DOWNLOAD
        auto wt = db_fresh->start_write();
        wt->get_table("class_table")->begin()->get_list<Int>("list").add(2);
        wt->commit();
    }

    // db now has a changeset waiting to be uploaded with both a changeset
    // and reciprocal transform
    apply_changes(*db_fresh, *db);

    { // Freshly downloaded client reset realm doesn't have the object
        auto wt = db_fresh->start_write();
        wt->get_table("class_table")->begin()->remove();
        wt->commit();
    }

    // Make a copy as client reset will delete the fresh realm
    mark_as_synchronized(*db_fresh);
    db_fresh->write_copy(path_3, nullptr);

    // client reset will discard the recovered array insertion as the object
    // doesn't exist, but keep the object creation
    expect_reset(test_context, db, db_fresh, ClientResyncMode::Recover, nullptr);

    // Recreate the object and add a different value to the list
    {
        db_fresh = DB::create(make_client_replication(), path_3);
        auto wt = db_fresh->start_write();
        wt->get_table("class_table")->create_object_with_primary_key(0).get_list<Int>("list").add(3);
        wt->commit();
    }

    // If the client failed to discard the old reciprocal transform when performing
    // the client reset this'll merge the ArrayInsert with the discarded ArrayInsert,
    // and then throw an exception because prior_size is now incorrect
    apply_changes(*db_fresh, *db);

    // Sanity check the end state
    auto rt = db->start_read();
    auto table = rt->get_table("class_table");
    CHECK_OR_RETURN(table->size() == 2);
    auto list = table->get_object(1).get_list<Int>("list");
    CHECK_OR_RETURN(list.size() == 1);
    CHECK_EQUAL(list.get(0), 3);
}

} // unnamed namespace
