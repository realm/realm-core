#include <string>
#include <thread>

#include <realm/util/random.hpp>
#include <realm/db.hpp>

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

// FIXME: Rewrite this test without QBS
TEST_IF(ClientResetQueryBased_1, false)
{
    TEST_DIR(dir_1);                // The original server dir.
    TEST_DIR(dir_2);                // The backup dir.
    SHARED_GROUP_TEST_PATH(path_1); // The writer.
    SHARED_GROUP_TEST_PATH(path_2); // The resetting client.
    TEST_DIR(metadata_dir);         // The metadata directory used by the resetting client.

    util::Logger& logger = test_context.logger;

    const std::string ref_path = "/data";
    const std::string partial_path = "/data/__partial/test/1";

    std::string ref_path_1, partial_path_1, ref_path_2, partial_path_2;

    // First we make a changeset and upload it to the reference Realm.
    // we also create a partial client with a query and a query result.
    {
        ClientServerFixture fixture(dir_1, test_context);
        fixture.start();
        ref_path_1 = fixture.map_virtual_to_real_path(ref_path);
        partial_path_1 = fixture.map_virtual_to_real_path(partial_path);

        // Create the data and upload it to the reference Realm.
        {
            std::unique_ptr<ClientReplication> history = make_client_replication();
            DBRef sg = DB::create(*history, path_1);
            Session session = fixture.make_session(path_1);
            fixture.bind_session(session, ref_path);

            WriteTransaction wt{sg};
            TableRef table = create_table(wt, "class_values");
            auto col_ndx = table->add_column(type_Int, "value");
            for (int i = 0; i < 3; ++i) {
                create_object(wt, *table).set(col_ndx, 1000 + i * 100);
            }
            session.nonsync_transact_notify(wt.commit());
            session.wait_for_upload_complete_or_client_stopped();
        }

        // Create a partial client and add a query.
        {
            std::unique_ptr<ClientReplication> history = make_client_replication();
            DBRef sg = DB::create(*history, path_2);
            Session session = fixture.make_session(path_2);
            fixture.bind_session(session, partial_path);

            session.wait_for_download_complete_or_client_stopped();
            {
                WriteTransaction wt{sg};
                Group& group = wt.get_group();
                TableRef table = group.get_table("class_values");
                CHECK(table);

                // Create the query
                TableRef result_sets = wt.get_table(g_partial_sync_result_sets_table_name);
                CHECK(result_sets);
                ColKey col_key_query = result_sets->get_column_key("query");
                ColKey col_key_matches_property = result_sets->get_column_key("matches_property");
                ColKey col_key_status = result_sets->get_column_key("status");
                CHECK(col_key_query);
                CHECK(col_key_matches_property);
                CHECK(col_key_status);
                result_sets->add_column_list(*table, "values");
                Obj res = create_object(wt, *result_sets);
                res.set(col_key_matches_property, "values");
                res.set(col_key_query, "value = 1100");
                session.nonsync_transact_notify(wt.commit());
            }
            session.wait_for_upload_complete_or_client_stopped();
            session.wait_for_download_complete_or_client_stopped();
            {
                ReadTransaction rt{sg};
                const Group& group = rt.get_group();
                ConstTableRef table = group.get_table("class_values");
                CHECK(table);
                CHECK_EQUAL(table->size(), 1);
            }
        }
    }

    // Get the real paths of the backup.
    {
        ClientServerFixture fixture(dir_2, test_context);
        fixture.start();
        ref_path_2 = fixture.map_virtual_to_real_path(ref_path);
        partial_path_2 = fixture.map_virtual_to_real_path(partial_path);
    }

    // The server is shut down. We make a backup of the server Realms.
    logger.debug("ref_path_1 = %1, partial_path_1 = %2, "
                 "ref_path_2 = %3, partial_path_2 = %4",
                 ref_path_1, partial_path_1, ref_path_2, partial_path_2);
    // util::File::copy(ref_path_1, ref_path_2);
    //    util::File::copy(partial_path_1, partial_path_2);


    // Start a server and let the partial client add another query.
    {
    }


    //
    //
    //    // Make the second changeset in the original and have a client download it
    //    // all.
    //    {
    //        ClientServerFixture fixture(dir_1, test_context);
    //        fixture.start();
    //        real_path_1 = fixture.map_virtual_to_real_path(server_path);
    //
    //        std::unique_ptr<ClientReplication> history = make_client_replication();
    //        DBRef sg = DB::create(*history, path_1);
    //        Session session = fixture.make_session(path_1);
    //        fixture.bind_session(session, server_path);
    //
    //        WriteTransaction wt {sg};
    //        Group& group = wt.get_group();
    //        TableRef table = group.get_table("class_table");
    //        size_t row_ndx = create_object(group, *table);
    //        CHECK_EQUAL(row_ndx, 1);
    //        table->set_int(col_ndx, row_ndx, 456);
    //        session.nonsync_transact_notify(wt.commit());
    //        session.wait_for_upload_complete_or_client_stopped();
    //
    //        Session session_2 = fixture.make_session(path_2);
    //        fixture.bind_session(session_2, server_path);
    //        session_2.wait_for_download_complete_or_client_stopped();
    //    }
    //
    //    // Check the content in path_2.
    //    {
    //        std::unique_ptr<ClientReplication> history = make_client_replication();
    //        DBRef sg = DB::create(*history, path_2);
    //        ReadTransaction rt {sg};
    //        const Group& group = rt.get_group();
    //        ConstTableRef table = group.get_table("class_table");
    //        CHECK(table);
    //        CHECK_EQUAL(table->size(), 2);
    //        CHECK_EQUAL(table->get_int(col_ndx, 0), 123);
    //        CHECK_EQUAL(table->get_int(col_ndx, 1), 456);
    //    }
    //
    //    // Start the server from dir_2 and connect with the client 2.
    //    // We expect an error of type 209, "Bad server version".
    //    {
    //        ClientServerFixture fixture(dir_2, test_context);
    //        fixture.start();
    //
    //        // The session that receives an error.
    //        {
    //            BowlOfStonesSemaphore bowl;
    //            auto listener = [&](ConnectionState state, const ErrorInfo* error_info) {
    //                if (state != ConnectionState::disconnected)
    //                    return;
    //                REALM_ASSERT(error_info);
    //                std::error_code ec = error_info->error_code;
    //                CHECK_EQUAL(ec, sync::ProtocolError::bad_server_version);
    //                bowl.add_stone();
    //            };
    //
    //            Session session = fixture.make_session(path_2);
    //            session.set_connection_state_change_listener(listener);
    //            fixture.bind_session(session, server_path);
    //            bowl.get_stone();
    //        }
    //
    //        // The session that performs client reset.
    //        // The Realm will be opened by a user while the reset takes place.
    //        {
    //            std::unique_ptr<ClientReplication> history = make_client_replication();
    //            DBRef sg = DB::create(*history, path_2);
    //            ReadTransaction rt {sg};
    //            const Group& group = rt.get_group();
    //            ConstTableRef table = group.get_table("class_table");
    //            CHECK_EQUAL(table->size(), 2);
    //
    //            Session::Config session_config;
    //            {
    //                Session::Config::ClientReset client_reset_config;
    //                client_reset_config.metadata_dir = std::string(metadata_dir);
    //                session_config.client_reset_config = client_reset_config;
    //            }
    //            Session session = fixture.make_session(path_2, session_config);
    //            fixture.bind_session(session, server_path);
    //            session.wait_for_download_complete_or_client_stopped();
    //        }
    //    }
    //
    //    // Check the content in path_2. There should only be one row now.
    //    {
    //        std::unique_ptr<ClientReplication> history = make_client_replication();
    //        DBRef sg = DB::create(*history, path_2);
    //        ReadTransaction rt {sg};
    //        const Group& group = rt.get_group();
    //        ConstTableRef table = group.get_table("class_table");
    //        CHECK(table);
    //        CHECK_EQUAL(table->size(), 1);
    //        CHECK_EQUAL(table->get_int(col_ndx, 0), 123);
    //    }
}

} // unnamed namespace
