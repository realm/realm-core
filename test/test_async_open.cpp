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

TEST(AsyncOpen_NonExistingRealm)
{
    TEST_DIR(dir);
    SHARED_GROUP_TEST_PATH(path);

    const std::string server_path = "/data";

    ClientServerFixture fixture(dir, test_context);
    fixture.start();

    auto progress_handler = [&](uint_fast64_t downloaded, uint_fast64_t downloadable, uint_fast64_t uploaded,
                                uint_fast64_t uploadable, uint_fast64_t progress, uint_fast64_t snapshot) {
        CHECK_EQUAL(uploaded, 0);
        CHECK_EQUAL(uploadable, 0);
        CHECK_EQUAL(downloaded, 0);
        CHECK_EQUAL(downloadable, 0);
        static_cast<void>(snapshot);
        static_cast<void>(progress);
    };

    // Download the empty state Realm.
    Session::Config session_config;
    session_config.client_reset_config = Session::Config::ClientReset{};
    Session session = fixture.make_session(path, std::move(session_config));
    session.set_progress_handler(progress_handler);
    fixture.bind_session(session, server_path);
    session.wait_for_download_complete_or_client_stopped();

    {
        DBRef sg = DB::create(make_client_replication(), path);
        ReadTransaction rt(sg);
        CHECK(rt.get_group().is_empty());
    }
}


// Note: Since v10, state realms are always disabled on the internal sync server
// implementation.
TEST(AsyncOpen_DisableStateRealms)
{
    TEST_DIR(dir);
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);

    const int number_of_rows = 100;

    util::Logger& logger = test_context.logger;

    ClientServerFixture::Config config;
    ClientServerFixture fixture(dir, test_context, std::move(config));
    fixture.start();

    std::unique_ptr<ClientReplication> history_1 = make_client_replication();
    DBRef sg_1 = DB::create(*history_1, path_1);

    {
        WriteTransaction wt{sg_1};
        TableRef table = wt.get_group().add_table_with_primary_key("class_table", type_Int, "pk_int");
        auto col_ndx = table->add_column(type_Int, "int");
        for (int i = 0; i < number_of_rows; ++i) {
            table->create_object_with_primary_key(i).set(col_ndx, i);
        }
        wt.commit();

        Session session = fixture.make_session(path_1);
        fixture.bind_session(session, "/data");
        session.wait_for_upload_complete_or_client_stopped();
    }

    uint_fast64_t state_downloadable = 0;
    auto progress_handler = [&](uint_fast64_t downloaded, uint_fast64_t downloadable, uint_fast64_t uploaded,
                                uint_fast64_t uploadable, uint_fast64_t progress, uint_fast64_t snapshot) {
        CHECK_EQUAL(uploaded, 0);
        CHECK_EQUAL(uploadable, 0);
        static_cast<void>(downloaded);
        static_cast<void>(snapshot);
        if (progress == 0)
            state_downloadable = downloadable;
    };
    Session::Config session_config;
    session_config.client_reset_config = Session::Config::ClientReset{};
    Session session = fixture.make_session(path_2, std::move(session_config));
    session.set_progress_handler(progress_handler);
    fixture.bind_session(session, "/data");
    session.wait_for_download_complete_or_client_stopped();
    CHECK_EQUAL(state_downloadable, 0);

    {
        std::unique_ptr<ClientReplication> history_2 = make_client_replication();
        DBRef sg_2 = DB::create(*history_2, path_2);
        ReadTransaction rt_1(sg_1);
        ReadTransaction rt_2(sg_2);
        CHECK(compare_groups(rt_1, rt_2, logger));
    }
}


TEST(AsyncOpen_StateRealmManagement)
{
    TEST_DIR(dir);
    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);

    const std::string server_path = "/data";

    ClientServerFixture fixture(dir, test_context);
    fixture.start();

    std::unique_ptr<ClientReplication> history_1 = make_client_replication();
    DBRef sg_1 = DB::create(*history_1, path_1);
    Session session_1 = fixture.make_session(path_1);
    fixture.bind_session(session_1, server_path);

    // Create a large Realm.
    const int num_rows = 1000;
    {
        WriteTransaction wt{sg_1};
        TableRef table = wt.add_table("class_table");
        auto col_ndx_int = table->add_column(type_Int, "int");
        auto col_ndx_string = table->add_column(type_String, "string");
        for (int i = 0; i < num_rows; ++i) {
            std::string str = "something-" + std::to_string(i);
            table->create_object().set(col_ndx_int, i).set(col_ndx_string, str);
        }
        session_1.nonsync_transact_notify(wt.commit());
    }
    session_1.wait_for_upload_complete_or_client_stopped();

    // Async open with a client.
    {
        Session::Config session_config;
        session_config.client_reset_config = Session::Config::ClientReset{};
        Session session = fixture.make_session(path_2, std::move(session_config));
        fixture.bind_session(session, server_path);
        session.wait_for_download_complete_or_client_stopped();
    }
}

} // unnamed namespace
