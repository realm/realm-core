#include "../util/benchmark_results.hpp"
#include "../util/timer.hpp"
#include "../util/test_path.hpp"
#include "../util/unit_test.hpp"
#include "../test_all.hpp"
#include "../sync_fixtures.hpp"

using namespace realm;
using namespace realm::test_util::unit_test;
using namespace realm::fixtures;

namespace bench {

static std::unique_ptr<BenchmarkResults> results;

#define TEST_CLIENT_DB(name)                                                                                         \
    SHARED_GROUP_TEST_PATH(name##_path);                                                                             \
    auto name = DB::create(make_client_replication(), name##_path);

// Two peers have 1000 transactions each with a handful of instructions in
// each (25% transactions contain MoveLastOver). One peer receives and merges
// all transactions from the other (but does not apply them to their
// database).
template <size_t num_transactions>
void transform_transactions(TestContext& test_context)
{
    std::string ident = test_context.test_details.test_name;
    const size_t num_iterations = 3;

    for (size_t i = 0; i < num_iterations; ++i) {
        TEST_CLIENT_DB(db_1);
        TEST_CLIENT_DB(db_2);

        // Produce some mostly realistic transactions on both sides.
        auto make_transactions = [](DBRef& db) {
            ColKey col_ndx;
            {
                WriteTransaction wt(db);
                TableRef t = wt.add_table("class_t");
                col_ndx = t->add_column(type_String, "s");
                wt.commit();
            }

            for (size_t j = 0; j < num_transactions - 1; ++j) {
                WriteTransaction wt(db);
                TableRef t = wt.get_table("class_t");
                t->create_object().set(col_ndx, std::string(500, char('a' + j % 26)));

                // Let 25% of commits contain a MoveLastOver
                if (j % 4 == 0) {
                    t->remove_object(t->begin());
                }
                wt.commit();
            }
        };

        make_transactions(db_1);
        make_transactions(db_2);

        TEST_DIR(dir);

        MultiClientServerFixture::Config config;
        config.server_public_key_path = "";
        MultiClientServerFixture fixture(2, 1, dir, test_context, config);
        Timer t{Timer::type_RealTime};

        Session::Config session_config;
        session_config.on_sync_client_event_hook = [&](const SyncClientHookData& data) {
            CHECK(data.batch_state == sync::DownloadBatchState::SteadyState);
            if (data.num_changesets == 0) {
                return SyncClientHookAction::NoAction;
            }

            switch (data.event) {
                case realm::SyncClientHookEvent::DownloadMessageReceived:
                    t.reset();
                    break;
                case realm::SyncClientHookEvent::DownloadMessageIntegrated:
                    results->submit(ident.c_str(), t.get_elapsed_time());
                    break;
                default:
                    break;
            }

            return SyncClientHookAction::NoAction;
        };

        Session session_1 = fixture.make_session(0, db_1, std::move(session_config));
        fixture.bind_session(session_1, 0, "/test");
        Session session_2 = fixture.make_session(1, db_2);
        fixture.bind_session(session_2, 0, "/test");

        // Start server and upload changes of second client.
        fixture.start_server(0);
        fixture.start_client(1);
        session_2.wait_for_upload_complete_or_client_stopped();
        session_2.wait_for_download_complete_or_client_stopped();
        fixture.stop_client(1);

        // Upload changes of first client and wait to integrate changes from second client.
        fixture.start_client(0);
        session_1.wait_for_upload_complete_or_client_stopped();
        session_1.wait_for_download_complete_or_client_stopped();
    }

    results->finish(ident, ident, "runtime_secs");
}

// Two peers have 1 transaction each with 1000 instructions (8.3% of
// instructions are MoveLastOver). One peer receives and merges the large
// transaction from the other (but does not apply it to their database).
template <size_t num_iterations>
void transform_instructions(TestContext& test_context)
{
    std::string ident = test_context.test_details.test_name;

    for (size_t i = 0; i < 3; ++i) {
        TEST_CLIENT_DB(db_1);
        TEST_CLIENT_DB(db_2);

        // Produce some mostly realistic transactions on both sides.
        auto make_instructions = [](DBRef& db) {
            WriteTransaction wt(db);
            TableRef t = wt.add_table("class_t");
            ColKey col_ndx = t->add_column(type_Int, "i");

            for (size_t j = 0; j < num_iterations; ++j) {
                t->create_object().set(col_ndx, 123);

                // Let 25% of commits contain a MoveLastOver
                if (j % 4 == 0) {
                    t->begin()->remove();
                }
            }
            wt.commit();
        };

        make_instructions(db_1);
        make_instructions(db_2);

        TEST_DIR(dir);

        MultiClientServerFixture::Config config;
        config.server_public_key_path = "";
        MultiClientServerFixture fixture(2, 1, dir, test_context, config);
        Timer t{Timer::type_RealTime};

        Session::Config session_config;
        session_config.on_sync_client_event_hook = [&](const SyncClientHookData& data) {
            CHECK(data.batch_state == sync::DownloadBatchState::SteadyState);
            if (data.num_changesets == 0) {
                return SyncClientHookAction::NoAction;
            }

            switch (data.event) {
                case realm::SyncClientHookEvent::DownloadMessageReceived:
                    t.reset();
                    break;
                case realm::SyncClientHookEvent::DownloadMessageIntegrated:
                    results->submit(ident.c_str(), t.get_elapsed_time());
                    break;
                default:
                    break;
            }

            return SyncClientHookAction::NoAction;
        };
        Session session_1 = fixture.make_session(0, db_1, std::move(session_config));
        fixture.bind_session(session_1, 0, "/test");
        Session session_2 = fixture.make_session(1, db_2);
        fixture.bind_session(session_2, 0, "/test");

        // Start server and upload changes of second client.
        fixture.start_server(0);
        fixture.start_client(1);
        session_2.wait_for_upload_complete_or_client_stopped();
        session_2.wait_for_download_complete_or_client_stopped();
        fixture.stop_client(1);

        // Upload changes of first client and wait to integrate changes from second client.
        fixture.start_client(0);
        session_1.wait_for_upload_complete_or_client_stopped();
        session_1.wait_for_download_complete_or_client_stopped();
    }

    results->finish(ident, ident, "runtime_secs");
}

template <size_t num_iterations>
void connected_objects(TestContext& test_context)
{
    std::string ident = test_context.test_details.test_name;

    for (size_t i = 0; i < 3; ++i) {
        TEST_CLIENT_DB(db_1);
        TEST_CLIENT_DB(db_2);

        // Produce some mostly realistic transactions on both sides.
        auto make_instructions = [](DBRef& db) {
            WriteTransaction wt(db);
            TableRef t = wt.get_group().add_table_with_primary_key("class_t", type_String, "pk");
            ColKey col_key = t->add_column(*t, "l");

            // Everything links to this object!
            auto first_key = t->create_object_with_primary_key("Hello").get_key();

            for (size_t j = 0; j < num_iterations; ++j) {
                std::stringstream ss;
                ss << j;
                std::string pk = ss.str();
                t->create_object_with_primary_key(pk).set(col_key, first_key);
            }
            wt.commit();
        };

        make_instructions(db_1);
        make_instructions(db_2);

        TEST_DIR(dir);

        MultiClientServerFixture::Config config;
        config.server_public_key_path = "";
        MultiClientServerFixture fixture(2, 1, dir, test_context, config);
        Timer t{Timer::type_RealTime};

        Session::Config session_config;
        session_config.on_sync_client_event_hook = [&](const SyncClientHookData& data) {
            CHECK(data.batch_state == sync::DownloadBatchState::SteadyState);
            if (data.num_changesets == 0) {
                return SyncClientHookAction::NoAction;
            }

            switch (data.event) {
                case realm::SyncClientHookEvent::DownloadMessageReceived:
                    t.reset();
                    break;
                case realm::SyncClientHookEvent::DownloadMessageIntegrated:
                    results->submit(ident.c_str(), t.get_elapsed_time());
                    break;
                default:
                    break;
            }

            return SyncClientHookAction::NoAction;
        };
        Session session_1 = fixture.make_session(0, db_1, std::move(session_config));
        fixture.bind_session(session_1, 0, "/test");
        Session session_2 = fixture.make_session(1, db_2);
        fixture.bind_session(session_2, 0, "/test");

        // Start server and upload changes of second client.
        fixture.start_server(0);
        fixture.start_client(1);
        session_2.wait_for_upload_complete_or_client_stopped();
        session_2.wait_for_download_complete_or_client_stopped();
        fixture.stop_client(1);

        // Upload changes of first client and wait to integrate changes from second client.
        fixture.start_client(0);
        session_1.wait_for_upload_complete_or_client_stopped();
        session_1.wait_for_download_complete_or_client_stopped();
    }

    results->finish(ident, ident, "runtime_secs");
}

} // namespace bench

const int max_lead_text_width = 40;

TEST(BenchMerge1000x1000Instructions)
{
    bench::transform_instructions<1000>(test_context);
}

TEST(BenchMerge2000x2000Instructions)
{
    bench::transform_instructions<2000>(test_context);
}

TEST(BenchMerge4000x4000Instructions)
{
    bench::transform_instructions<4000>(test_context);
}

TEST(BenchMerge8000x8000Instructions)
{
    bench::transform_instructions<8000>(test_context);
}

TEST(BenchMerge16000x16000Instructions)
{
    bench::transform_instructions<16000>(test_context);
}

TEST(BenchMerge100x100Transactions)
{
    bench::transform_transactions<100>(test_context);
}

TEST(BenchMerge500x500Transactions)
{
    bench::transform_transactions<500>(test_context);
}

TEST(BenchMerge1000x1000Transactions)
{
    bench::transform_transactions<1000>(test_context);
}

TEST(BenchMerge2000x2000Transactions)
{
    bench::transform_transactions<2000>(test_context);
}

TEST(BenchMerge4000x4000Transactions)
{
    bench::transform_transactions<4000>(test_context);
}

TEST(BenchMerge8000x8000Transactions)
{
    bench::transform_transactions<8000>(test_context);
}

TEST(BenchMerge16000x16000Transactions)
{
    bench::transform_transactions<16000>(test_context);
}

TEST(BenchMergeManyConnectedObjects)
{
    bench::connected_objects<1000>(test_context);
}

#if !REALM_IOS
int main()
{
    std::string results_file_stem = realm::test_util::get_test_path_prefix() + "results";
    bench::results =
        std::make_unique<BenchmarkResults>(max_lead_text_width, "benchmark-sync", results_file_stem.c_str());
    auto exit_status = test_all();
    // Save to file when deallocated.
    bench::results.reset();
    return exit_status;
}
#endif // REALM_IOS
