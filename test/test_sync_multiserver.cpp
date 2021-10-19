#include <memory>

#include <realm/db.hpp>
#include <realm/sync/noinst/server/server_history.hpp>
#include <realm/sync/protocol.hpp>
#include <realm/sync/client.hpp>

#include "util/compare_groups.hpp"

#include "test.hpp"
#include "sync_fixtures.hpp"

using namespace realm;
using namespace realm::sync;
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

TEST(Sync_Multiserver_Replicate)
{
    // Client file 1 -> 2nd tier server 1 -> root server -> 2nd tier server 2 -> Client file 2

    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);
    std::unique_ptr<Replication> history_1 = make_client_replication();
    std::unique_ptr<Replication> history_2 = make_client_replication();
    DBRef sg_1 = DB::create(*history_1, path_1);
    DBRef sg_2 = DB::create(*history_2, path_2);

    int num_transacts = 1000;
    {
        TEST_DIR(dir);
        int num_clients = 1;
        int num_servers = 1 + 2; // One root node + two 2nd tier nodes
        MultiClientServerFixture::Config config;
        config.cluster_topology = MultiClientServerFixture::ClusterTopology::two_tiers;
        MultiClientServerFixture fixture{num_clients, num_servers, dir, test_context, config};
        fixture.start();

        Session session_1 = fixture.make_bound_session(0, path_1, 1 + 0, "/test");
        Session session_2 = fixture.make_bound_session(0, path_2, 1 + 1, "/test");

        // Make sure that both 2nd tier servers have upstream sessions for
        // `/test` such that a full server cluster synchronization can be
        // forformed below.
        session_1.wait_for_download_complete_or_client_stopped();
        session_2.wait_for_download_complete_or_client_stopped();

        // Create schema
        {
            WriteTransaction wt{sg_1};
            TableRef table = sync::create_table(wt, "class_foo");
            table->add_column(type_Int, "i");
            version_type new_version = wt.commit();
            session_1.nonsync_transact_notify(new_version);
        }
        Random random(random_int<unsigned long>()); // Seed from slow global generator
        for (int i = 0; i < num_transacts; ++i) {
            WriteTransaction wt{sg_1};
            TableRef table = wt.get_table("class_foo");
            sync::create_object(wt, *table);
            Obj obj = *(table->begin() + random.draw_int_mod(table->size()));
            obj.set<int64_t>("i", random.draw_int_max(0x7FFFFFFFFFFFFFFF));
            version_type new_version = wt.commit();
            session_1.nonsync_transact_notify(new_version);
        }

        session_1.wait_for_upload_complete_or_client_stopped();
        fixture.wait_for_server_cluster_synchronized();
        session_2.wait_for_download_complete_or_client_stopped();
    }

    ReadTransaction rt_1{sg_1};
    ReadTransaction rt_2{sg_2};
    const Group& group_1 = rt_1.get_group();
    CHECK(compare_groups(rt_1, rt_2, test_context.logger));
    ConstTableRef table = group_1.get_table("class_foo");
    CHECK_EQUAL(num_transacts, table->size());
}


TEST(Sync_Multiserver_Merge)
{
    // Merge changes from clint file 1 and client file 2.

    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);
    std::unique_ptr<Replication> history_1 = make_client_replication();
    std::unique_ptr<Replication> history_2 = make_client_replication();
    DBRef sg_1 = DB::create(*history_1, path_1);
    DBRef sg_2 = DB::create(*history_2, path_2);

    auto create_schema = [](DBRef sg) {
        WriteTransaction wt{sg};
        TableRef table = sync::create_table(wt, "class_foo");
        table->add_column(type_Int, "i");
        wt.commit();
    };
    create_schema(sg_1);
    create_schema(sg_2);

    int num_transacts_per_client = 1000;
    {
        TEST_DIR(dir);
        int num_clients = 2;
        int num_servers = 1 + 2; // One root node + two 2nd tier nodes
        MultiClientServerFixture::Config config;
        config.cluster_topology = MultiClientServerFixture::ClusterTopology::two_tiers;
        MultiClientServerFixture fixture{num_clients, num_servers, dir, test_context, config};
        fixture.start();

        Session session_1 = fixture.make_bound_session(0, path_1, 1 + 0, "/test");
        Session session_2 = fixture.make_bound_session(1, path_2, 1 + 1, "/test");

        // Make sure that both 2nd tier servers have upstream sessions for
        // `/test` such that a full server cluster synchronization can be
        // forformed below.
        session_1.wait_for_download_complete_or_client_stopped();
        session_2.wait_for_download_complete_or_client_stopped();

        auto client_program = [=](DBRef sg, Session& session) {
            Random random(random_int<unsigned long>()); // Seed from slow global generator
            for (int i = 0; i < num_transacts_per_client; ++i) {
                WriteTransaction wt{sg};
                TableRef table = wt.get_table("class_foo");
                sync::create_object(wt, *table);
                Obj obj = *(table->begin() + random.draw_int_mod(table->size()));
                obj.set<int64_t>("i", random.draw_int_max(0x7FFFFFFFFFFFFFFF));
                version_type new_version = wt.commit();
                session.nonsync_transact_notify(new_version);
                if (i % 16 == 0)
                    session.wait_for_upload_complete_or_client_stopped();
            }
        };
        ThreadWrapper thread_1, thread_2;
        thread_1.start([&] {
            client_program(sg_1, session_1);
        });
        thread_2.start([&] {
            client_program(sg_2, session_2);
        });
        CHECK(!thread_1.join());
        CHECK(!thread_2.join());

        session_1.wait_for_upload_complete_or_client_stopped();
        session_2.wait_for_upload_complete_or_client_stopped();
        fixture.wait_for_server_cluster_synchronized();
        session_1.wait_for_download_complete_or_client_stopped();
        session_2.wait_for_download_complete_or_client_stopped();
    }

    ReadTransaction rt_1{sg_1};
    ReadTransaction rt_2{sg_2};
    const Group& group_1 = rt_1.get_group();
    CHECK(compare_groups(rt_1, rt_2, test_context.logger));
    ConstTableRef table = group_1.get_table("class_foo");
    CHECK_EQUAL(2 * num_transacts_per_client, table->size());
}


TEST(Sync_Multiserver_MultipleClientsPer2ndtierServer)
{
    const int num_2ndtier_servers = 3;
    const int num_clients_per_2ndtier_server = 3;
    int num_transacts_per_client = 100;

    TEST_DIR(dir_1);
    MultiClientServerFixture::Config config;
    config.cluster_topology = MultiClientServerFixture::ClusterTopology::two_tiers;
    int num_clients = num_clients_per_2ndtier_server * num_2ndtier_servers;
    int num_servers = 1 + num_2ndtier_servers;
    MultiClientServerFixture fixture{num_clients, num_servers, dir_1, test_context, config};
    fixture.start();

    TEST_DIR(dir_2);
    const int n_1 = num_2ndtier_servers, n_2 = num_clients_per_2ndtier_server;
    std::unique_ptr<ClientReplication> histories[n_1][n_2];
    DBRef shared_groups[n_1][n_2];
    Session sessions[n_1][n_2];
    for (int i = 0; i < num_2ndtier_servers; ++i) {
        for (int j = 0; j < num_clients_per_2ndtier_server; ++j) {
            std::ostringstream out;
            out.imbue(std::locale::classic());
            out << i << "_" << j << ".realm";
            std::string path = util::File::resolve(out.str(), dir_2);
            histories[i][j] = make_client_replication();
            shared_groups[i][j] = DB::create(*histories[i][j], path);
            int client_ndx = i * num_clients_per_2ndtier_server + j;
            int server_ndx = 1 + i;
            sessions[i][j] = fixture.make_bound_session(client_ndx, path, server_ndx, "/test");
        }
    }

    auto client_program = [=](DBRef sg, Session& session) {
        Random random(random_int<unsigned long>()); // Seed from slow global generator
        {
            WriteTransaction wt{sg};
            if (!wt.has_table("class_foo")) {
                TableRef table = sync::create_table(wt, "class_foo");
                table->add_column(type_Int, "i");
                wt.commit();
            }
        }
        for (int i = 0; i < num_transacts_per_client; ++i) {
            WriteTransaction wt{sg};
            TableRef table = wt.get_table("class_foo");
            sync::create_object(wt, *table);
            Obj obj = *(table->begin() + random.draw_int_mod(table->size()));
            obj.set<int64_t>("i", random.draw_int_max(0x0'7FFF'FFFF'FFFF'FFFF));
            version_type new_version = wt.commit();
            session.nonsync_transact_notify(new_version);
            if (i % 16 == 0)
                session.wait_for_upload_complete_or_client_stopped();
        }
    };

    ThreadWrapper threads[n_1][n_2];
    for (int i = 0; i < num_2ndtier_servers; ++i) {
        for (int j = 0; j < num_clients_per_2ndtier_server; ++j) {
            auto run = [&](int i, int j) {
                client_program(shared_groups[i][j], sessions[i][j]);
            };
            threads[i][j].start([=] {
                run(i, j);
            });
        }
    }

    for (int i = 0; i < num_2ndtier_servers; ++i) {
        for (int j = 0; j < num_clients_per_2ndtier_server; ++j)
            CHECK(!threads[i][j].join());
    }
    log("All client programs completed");

    for (int i = 0; i < num_2ndtier_servers; ++i) {
        for (int j = 0; j < num_clients_per_2ndtier_server; ++j)
            sessions[i][j].wait_for_upload_complete_or_client_stopped();
    }
    log("Everything uploaded");

    fixture.wait_for_server_cluster_synchronized();
    log("Server cluster synchronized");

    for (int i = 0; i < num_2ndtier_servers; ++i) {
        for (int j = 0; j < num_clients_per_2ndtier_server; ++j)
            sessions[i][j].wait_for_download_complete_or_client_stopped();
    }
    log("Everything downloaded");

    class ServerHistoryContext : public _impl::ServerHistory::Context {
    public:
        bool owner_is_sync_server() const noexcept override final
        {
            return false;
        }
        std::mt19937_64& server_history_get_random() noexcept override final
        {
            return m_random;
        }

    private:
        std::mt19937_64 m_random;
    };

    std::string path_1 = fixture.map_virtual_to_real_path(0, "/test");
    ServerHistoryContext context;
    _impl::ServerHistory::DummyCompactionControl compaction_control;
    _impl::ServerHistory history_1{context, compaction_control};
    DBRef sg_1 = DB::create(history_1, path_1);
    ReadTransaction rt_1{sg_1};
    for (int i = 0; i < num_2ndtier_servers; ++i) {
        for (int j = 0; j < num_clients_per_2ndtier_server; ++j) {
            ReadTransaction rt_2{shared_groups[i][j]};
            CHECK(compare_groups(rt_1, rt_2, test_context.logger));
        }
    }
}


TEST(Sync_Multiserver_ManyTiers)
{
    const int num_tiers = 4;
    const int num_clients_per_tier = 2;
    int num_transacts_per_client = 100;

    TEST_DIR(dir_1);
    MultiClientServerFixture::Config config;
    config.cluster_topology = MultiClientServerFixture::ClusterTopology::one_node_per_tier;
    int num_clients = num_clients_per_tier * num_tiers;
    int num_servers = num_tiers;
    MultiClientServerFixture fixture{num_clients, num_servers, dir_1, test_context, config};
    fixture.start();

    TEST_DIR(dir_2);
    const int n_1 = num_tiers, n_2 = num_clients_per_tier;
    std::unique_ptr<ClientReplication> histories[n_1][n_2];
    DBRef shared_groups[n_1][n_2];
    Session sessions[n_1][n_2];
    for (int i = 0; i < num_tiers; ++i) {
        for (int j = 0; j < num_clients_per_tier; ++j) {
            std::ostringstream out;
            out.imbue(std::locale::classic());
            out << i << "_" << j << ".realm";
            std::string path = util::File::resolve(out.str(), dir_2);
            histories[i][j] = make_client_replication();
            shared_groups[i][j] = DB::create(*histories[i][j], path);
            int client_ndx = i * num_clients_per_tier + j;
            int server_ndx = i;
            sessions[i][j] = fixture.make_bound_session(client_ndx, path, server_ndx, "/test");
        }
    }

    auto client_program = [=](DBRef sg, Session& session) {
        Random random(random_int<unsigned long>()); // Seed from slow global generator
        {
            WriteTransaction wt{sg};
            if (!wt.has_table("class_foo")) {
                TableRef table = sync::create_table(wt, "class_foo");
                table->add_column(type_Int, "i");
                wt.commit();
            }
        }
        for (int i = 0; i < num_transacts_per_client; ++i) {
            WriteTransaction wt{sg};
            TableRef table = wt.get_table("class_foo");
            sync::create_object(wt, *table);
            Obj obj = *(table->begin() + random.draw_int_mod(table->size()));
            obj.set<int64_t>("i", random.draw_int_max(0x0'7FFF'FFFF'FFFF'FFFF));
            version_type new_version = wt.commit();
            session.nonsync_transact_notify(new_version);
            if (i % 16 == 0)
                session.wait_for_upload_complete_or_client_stopped();
        }
    };

    ThreadWrapper threads[n_1][n_2];
    for (int i = 0; i < num_tiers; ++i) {
        for (int j = 0; j < num_clients_per_tier; ++j) {
            auto run = [&](int i, int j) {
                client_program(shared_groups[i][j], sessions[i][j]);
            };
            threads[i][j].start([=] {
                run(i, j);
            });
        }
    }

    for (int i = 0; i < num_tiers; ++i) {
        for (int j = 0; j < num_clients_per_tier; ++j)
            CHECK(!threads[i][j].join());
    }
    log("All client programs completed");

    for (int i = 0; i < num_tiers; ++i) {
        for (int j = 0; j < num_clients_per_tier; ++j)
            sessions[i][j].wait_for_upload_complete_or_client_stopped();
    }
    log("Everything uploaded");

    fixture.wait_for_server_cluster_synchronized();
    log("Server cluster synchronized");

    for (int i = 0; i < num_tiers; ++i) {
        for (int j = 0; j < num_clients_per_tier; ++j)
            sessions[i][j].wait_for_download_complete_or_client_stopped();
    }
    log("Everything downloaded");

    class ServerHistoryContext : public _impl::ServerHistory::Context {
    public:
        bool owner_is_sync_server() const noexcept override final
        {
            return false;
        }
        std::mt19937_64& server_history_get_random() noexcept override final
        {
            return m_random;
        }

    private:
        std::mt19937_64 m_random;
    };

    std::string path_1 = fixture.map_virtual_to_real_path(0, "/test");
    ServerHistoryContext context;
    _impl::ServerHistory::DummyCompactionControl compaction_control;
    _impl::ServerHistory history_1{context, compaction_control};
    DBRef sg_1 = DB::create(history_1, path_1);
    ReadTransaction rt_1{sg_1};
    for (int i = 0; i < num_tiers; ++i) {
        for (int j = 0; j < num_clients_per_tier; ++j) {
            ReadTransaction rt_2{shared_groups[i][j]};
            CHECK(compare_groups(rt_1, rt_2, test_context.logger));
        }
    }
}


TEST(Sync_Multiserver_PartialSync)
{
    const int num_tiers = 3;
    const int num_clients_per_tier = 2;
    int num_transacts_per_client = 25;

    TEST_DIR(dir_1);
    TEST_DIR(dir_2);
    MultiClientServerFixture::Config config;
    config.cluster_topology = MultiClientServerFixture::ClusterTopology::one_node_per_tier;
    int num_clients = num_clients_per_tier * num_tiers;
    int num_servers = num_tiers;
    MultiClientServerFixture fixture{num_clients, num_servers, dir_1, test_context, config};
    fixture.start();

    const int n_1 = num_tiers, n_2 = num_clients_per_tier;
    std::unique_ptr<ClientReplication> histories[n_1][n_2];
    DBRef shared_groups[n_1][n_2];
    Session sessions[n_1][n_2];
    for (int i = 0; i < num_tiers; ++i) {
        for (int j = 0; j < num_clients_per_tier; ++j) {
            std::ostringstream out;
            out.imbue(std::locale::classic());
            out << i << "_" << j << ".realm";
            std::string path = util::File::resolve(out.str(), dir_2);
            histories[i][j] = make_client_replication();
            shared_groups[i][j] = DB::create(*histories[i][j], path);
            int client_ndx = i * num_clients_per_tier + j;
            int server_ndx = i;
            out.str(std::string{});
            out << "/test/__partial/test/" << client_ndx;
            std::string partial_path = out.str();
            sessions[i][j] = fixture.make_bound_session(client_ndx, path, server_ndx, partial_path);
        }
    }

    StringData table_name_result_sets = g_partial_sync_result_sets_table_name;
    auto client_program = [=](DBRef sg, Session& session) {
        Random random(random_int<unsigned long>()); // Seed from slow global generator
        {
            WriteTransaction wt{sg};
            TableRef table = wt.get_table("class_foo");
            if (!table) {
                table = sync::create_table(wt, "class_foo");
                table->add_column(type_Int, "i");
            }
            TableRef result_sets = create_table(wt, table_name_result_sets);
            ColKey col_ndx_result_set_query = result_sets->get_column_key("query");
            if (!col_ndx_result_set_query)
                col_ndx_result_set_query = result_sets->add_column(type_String, "query");
            ColKey col_ndx_result_set_matches_property = result_sets->get_column_key("matches_property");
            if (!col_ndx_result_set_matches_property)
                col_ndx_result_set_matches_property = result_sets->add_column(type_String, "matches_property");
            // 0 = uninitialized, 1 = initialized, -1 = query parsing failed
            result_sets->add_column_list(*table, "matches");
            Obj result_set = create_object(wt, *result_sets);
            result_set.set(col_ndx_result_set_query, "TRUEPREDICATE");
            result_set.set(col_ndx_result_set_matches_property, "matches");
            wt.commit();
        }
        for (int i = 0; i < num_transacts_per_client; ++i) {
            WriteTransaction wt{sg};
            TableRef table = wt.get_table("class_foo");
            sync::create_object(wt, *table);
            Obj obj = *(table->begin() + random.draw_int_mod(table->size()));
            obj.set<int64_t>("i", random.draw_int_max(0x7FFFFFFFFFFFFFFF));
            version_type new_version = wt.commit();
            session.nonsync_transact_notify(new_version);
            if (i % 16 == 0)
                session.wait_for_upload_complete_or_client_stopped();
        }
    };

    ThreadWrapper threads[n_1][n_2];
    for (int i = 0; i < num_tiers; ++i) {
        for (int j = 0; j < num_clients_per_tier; ++j) {
            auto run = [&](int i, int j) {
                client_program(shared_groups[i][j], sessions[i][j]);
            };
            threads[i][j].start([=] {
                run(i, j);
            });
        }
    }

    for (int i = 0; i < num_tiers; ++i) {
        for (int j = 0; j < num_clients_per_tier; ++j)
            CHECK(!threads[i][j].join());
    }
    log("All client programs completed");

    for (int i = 0; i < num_tiers; ++i) {
        for (int j = 0; j < num_clients_per_tier; ++j)
            sessions[i][j].wait_for_upload_complete_or_client_stopped();
    }
    log("Everything uploaded");

    fixture.wait_for_server_cluster_synchronized();
    log("Server cluster synchronized");

    for (int i = 0; i < num_tiers; ++i) {
        for (int j = 0; j < num_clients_per_tier; ++j)
            sessions[i][j].wait_for_download_complete_or_client_stopped();
    }
    log("Everything downloaded");

    class ServerHistoryContext : public _impl::ServerHistory::Context {
    public:
        bool owner_is_sync_server() const noexcept override final
        {
            return false;
        }
        std::mt19937_64& server_history_get_random() noexcept override final
        {
            return m_random;
        }

    private:
        std::mt19937_64 m_random;
    };

    std::string path_1 = fixture.map_virtual_to_real_path(0, "/test");
    ServerHistoryContext context;
    _impl::ServerHistory::DummyCompactionControl compaction_control;
    _impl::ServerHistory history_1{context, compaction_control};
    DBRef sg_1 = DB::create(history_1, path_1);
    ReadTransaction rt_1{sg_1};
    for (int i = 0; i < num_tiers; ++i) {
        for (int j = 0; j < num_clients_per_tier; ++j) {
            ReadTransaction rt_2{shared_groups[i][j]};
            auto filter = [&](StringData table_name) {
                if (table_name == table_name_result_sets)
                    return false;
                if (table_name == "class___Permission")
                    return false;
                if (table_name == "class___Role")
                    return false;
                if (table_name == "class___Class")
                    return false;
                if (table_name == "class___Realm")
                    return false;
                if (table_name == "class___User")
                    return false;
                return true;
            };
            CHECK(compare_groups(rt_1, rt_2, std::move(filter), test_context.logger));
        }
    }
}


TEST(Sync_Multiserver_ServerSideModify)
{
    const int num_tiers = 3;
    const int num_clients_per_tier = 1;
    int num_transacts_per_server = 100;
    int num_transacts_per_client = 100;

    TEST_DIR(dir_1);
    MultiClientServerFixture::Config config;
    config.cluster_topology = MultiClientServerFixture::ClusterTopology::one_node_per_tier;
    config.integrated_backup = ClientServerFixture::IntegratedBackup::disabled;
    int num_clients = num_clients_per_tier * num_tiers;
    int num_servers = num_tiers;
    MultiClientServerFixture fixture{num_clients, num_servers, dir_1, test_context, config};
    fixture.start();

    class ServerHistoryContext : public _impl::ServerHistory::Context {
    public:
        bool owner_is_sync_server() const noexcept override final
        {
            return false;
        }
        std::mt19937_64& server_history_get_random() noexcept override final
        {
            return m_random;
        }

    private:
        std::mt19937_64 m_random;
    };

    TEST_DIR(dir_2);
    const int n_1 = num_tiers, n_2 = num_clients_per_tier;
    ServerHistoryContext context;
    _impl::ServerHistory::DummyCompactionControl compaction_control;
    std::unique_ptr<_impl::ServerHistory> server_histories[n_1];
    DBRef server_shared_groups[n_1];
    std::unique_ptr<ClientReplication> client_histories[n_1][n_2];
    DBRef client_shared_groups[n_1][n_2];
    Session sessions[n_1][n_2];
    for (int i = 0; i < num_tiers; ++i) {
        std::string server_path = fixture.map_virtual_to_real_path(i, "/test");
        server_histories[i] = std::make_unique<_impl::ServerHistory>(context, compaction_control);
        server_shared_groups[i] = DB::create(*server_histories[i], server_path);
        for (int j = 0; j < num_clients_per_tier; ++j) {
            std::ostringstream out;
            out.imbue(std::locale::classic());
            out << i << "_" << j << ".realm";
            std::string path = util::File::resolve(out.str(), dir_2);
            client_histories[i][j] = make_client_replication();
            client_shared_groups[i][j] = DB::create(*client_histories[i][j], path);
            int client_ndx = i * num_clients_per_tier + j;
            int server_ndx = i;
            sessions[i][j] = fixture.make_bound_session(client_ndx, path, server_ndx, "/test");
        }
    }

    auto server_side_program = [&](DBRef sg, int server_index) {
        Random random(random_int<unsigned long>()); // Seed from slow global generator
        {
            WriteTransaction wt{sg};
            if (!wt.has_table("class_foo")) {
                TableRef table = sync::create_table(wt, "class_foo");
                table->add_column(type_Int, "i");
                wt.commit();
            }
        }
        for (int i = 0; i < num_transacts_per_server; ++i) {
            WriteTransaction wt{sg};
            TableRef table = wt.get_table("class_foo");
            sync::create_object(wt, *table);
            Obj obj = *(table->begin() + random.draw_int_mod(table->size()));
            obj.set<int64_t>("i", random.draw_int_max(0x0'7FFF'FFFF'FFFF'FFFF));
            wt.commit();
            fixture.inform_server_about_external_change(server_index, "/test");
        }
    };

    auto client_side_program = [&](DBRef sg, Session& session) {
        Random random(random_int<unsigned long>()); // Seed from slow global generator
        {
            WriteTransaction wt{sg};
            if (!wt.has_table("class_foo")) {
                TableRef table = sync::create_table(wt, "class_foo");
                table->add_column(type_Int, "i");
                wt.commit();
            }
        }
        for (int i = 0; i < num_transacts_per_client; ++i) {
            WriteTransaction wt{sg};
            TableRef table = wt.get_table("class_foo");
            sync::create_object(wt, *table);
            Obj obj = *(table->begin() + random.draw_int_mod(table->size()));
            obj.set<int64_t>("i", random.draw_int_max(0x0'7FFF'FFFF'FFFF'FFFF));
            version_type new_version = wt.commit();
            session.nonsync_transact_notify(new_version);
            if (i % 32 == 0)
                session.wait_for_upload_complete_or_client_stopped();
        }
    };

    ThreadWrapper server_program_threads[n_1];
    ThreadWrapper client_program_threads[n_1][n_2];
    for (int i = 0; i < num_tiers; ++i) {
        auto run_server_prog = [&](int i) {
            server_side_program(server_shared_groups[i], i);
        };
        server_program_threads[i].start([=] {
            run_server_prog(i);
        });
        for (int j = 0; j < num_clients_per_tier; ++j) {
            auto run_client_prog = [&](int i, int j) {
                client_side_program(client_shared_groups[i][j], sessions[i][j]);
            };
            client_program_threads[i][j].start([=] {
                run_client_prog(i, j);
            });
        }
    }

    for (int i = 0; i < num_tiers; ++i) {
        CHECK(!server_program_threads[i].join());
        for (int j = 0; j < num_clients_per_tier; ++j)
            CHECK(!client_program_threads[i][j].join());
    }
    log("All programs completed");

    for (int i = 0; i < num_tiers; ++i) {
        for (int j = 0; j < num_clients_per_tier; ++j)
            sessions[i][j].wait_for_upload_complete_or_client_stopped();
    }
    log("Everything uploaded");

    fixture.wait_for_server_cluster_synchronized();
    log("Server cluster synchronized");

    for (int i = 0; i < num_tiers; ++i) {
        for (int j = 0; j < num_clients_per_tier; ++j)
            sessions[i][j].wait_for_download_complete_or_client_stopped();
    }
    log("Everything downloaded");

    // Compare server-side Realms
    ReadTransaction rt_root{server_shared_groups[0]};
    for (int i = 1; i < num_tiers; ++i) {
        ReadTransaction rt{server_shared_groups[i]};
        CHECK(compare_groups(rt_root, rt, test_context.logger));
    }

    // Compare client-side Realms with root server's Realm
    for (int i = 0; i < num_tiers; ++i) {
        for (int j = 0; j < num_clients_per_tier; ++j) {
            ReadTransaction rt{client_shared_groups[i][j]};
            CHECK(compare_groups(rt_root, rt, test_context.logger));
        }
    }
}

} // unnamed namespace
