#include <cstddef>
#include <cstdint>
#include <cmath>
#include <cstring>
#include <memory>
#include <tuple>
#include <set>
#include <string>
#include <sstream>
#include <mutex>
#include <condition_variable>
#include <thread>

#include <realm/util/features.h>
#include <realm/util/parent_dir.hpp>
#include <realm/util/uri.hpp>
#include <realm/util/network.hpp>
#include <realm/util/http.hpp>
#include <realm/util/random.hpp>
#include <realm/util/websocket.hpp>
#include <realm/chunked_binary.hpp>
#include <realm/sync/noinst/server/server_history.hpp>
#include <realm/sync/noinst/protocol_codec.hpp>
#include <realm/sync/noinst/server/server_dir.hpp>
#include <realm/impl/simulated_failure.hpp>
#include <realm.hpp>
#include <realm/version.hpp>
#include <realm/sync/transform.hpp>
#include <realm/sync/history.hpp>
#include <realm/sync/protocol.hpp>
#include <realm/sync/client.hpp>
#include <realm/sync/noinst/server/server.hpp>
#include <realm/list.hpp>

#include "sync_fixtures.hpp"

#include "test.hpp"
#include "util/demangle.hpp"
#include "util/semaphore.hpp"
#include "util/thread_wrapper.hpp"
#include "util/mock_metrics.hpp"
#include "util/compare_groups.hpp"

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

class TestServerHistoryContext : public _impl::ServerHistory::Context {
public:
    std::mt19937_64& server_history_get_random() noexcept override final
    {
        return m_random;
    }

private:
    std::mt19937_64 m_random;
};

#define TEST_CLIENT_DB(name)                                                                                         \
    SHARED_GROUP_TEST_PATH(name##_path);                                                                             \
    auto name = DB::create(make_client_replication(), name##_path);

template <typename Function>
void write_transaction_notifying_session(DBRef db, Session& session, Function&& function)
{
    WriteTransaction wt(db);
    function(wt);
    auto new_version = wt.commit();
    session.nonsync_transact_notify(new_version);
}

ClientReplication& get_replication(DBRef db)
{
    auto repl = dynamic_cast<ClientReplication*>(db->get_replication());
    REALM_ASSERT(repl);
    return *repl;
}

ClientHistory& get_history(DBRef db)
{
    return get_replication(db).get_history();
}


TEST(Sync_BadVirtualPath)
{
    // NOTE:  This test is no longer valid after migration to MongoDB Realm
    //  It still passes because it runs against the mock C++ server, but the
    //  MongoDB Realm server will behave differently

    TEST_DIR(dir);
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);
    TEST_CLIENT_DB(db_3);
    ClientServerFixture fixture(dir, test_context);
    fixture.start();

    int nerrors = 0;

    using ErrorInfo = Session::ErrorInfo;
    auto listener = [&](ConnectionState state, const ErrorInfo* error_info) {
        if (state != ConnectionState::disconnected)
            return;
        REALM_ASSERT(error_info);
        std::error_code ec = error_info->error_code;
        bool is_fatal = error_info->is_fatal;
        CHECK_EQUAL(sync::ProtocolError::illegal_realm_path, ec);
        CHECK(is_fatal);
        ++nerrors;
        if (nerrors == 3)
            fixture.stop();
    };

    Session session_1 = fixture.make_session(db_1);
    session_1.set_connection_state_change_listener(listener);
    fixture.bind_session(session_1, "/test.realm");

    Session session_2 = fixture.make_session(db_2);
    session_2.set_connection_state_change_listener(listener);
    fixture.bind_session(session_2, "/../test");

    Session session_3 = fixture.make_session(db_3);
    session_3.set_connection_state_change_listener(listener);
    fixture.bind_session(session_3, "/test%abc ");

    session_1.wait_for_download_complete_or_client_stopped();
    session_2.wait_for_download_complete_or_client_stopped();
    session_3.wait_for_download_complete_or_client_stopped();
    CHECK_EQUAL(nerrors, 3);
}


TEST(Sync_AsyncWaitForUploadCompletion)
{
    TEST_DIR(dir);
    TEST_CLIENT_DB(db);
    ClientServerFixture fixture(dir, test_context);
    fixture.start();

    Session session = fixture.make_bound_session(db, "/test");

    auto wait = [&] {
        BowlOfStonesSemaphore bowl;
        auto handler = [&](std::error_code ec) {
            if (CHECK_NOT(ec))
                bowl.add_stone();
        };
        session.async_wait_for_upload_completion(handler);
        bowl.get_stone();
    };

    // Empty
    wait();

    // Nonempty
    write_transaction_notifying_session(db, session, [](WriteTransaction& wt) {
        wt.add_table("class_foo");
    });
    wait();

    // Already done
    wait();

    // More
    write_transaction_notifying_session(db, session, [](WriteTransaction& wt) {
        wt.add_table("class_bar");
    });
    wait();
}


TEST(Sync_AsyncWaitForDownloadCompletion)
{
    TEST_DIR(dir);
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);
    ClientServerFixture fixture(dir, test_context);
    fixture.start();

    auto wait = [&](Session& session) {
        BowlOfStonesSemaphore bowl;
        auto handler = [&](std::error_code ec) {
            if (CHECK_NOT(ec))
                bowl.add_stone();
        };
        session.async_wait_for_download_completion(handler);
        bowl.get_stone();
    };

    // Nothing to download
    Session session_1 = fixture.make_bound_session(db_1, "/test");
    wait(session_1);

    // Again
    wait(session_1);

    // Upload something via session 2
    Session session_2 = fixture.make_bound_session(db_2, "/test");
    write_transaction_notifying_session(db_2, session_2, [](WriteTransaction& wt) {
        wt.add_table("class_foo");
    });
    session_2.wait_for_upload_complete_or_client_stopped();

    // Wait for session 1 to download it
    wait(session_1);
    {
        ReadTransaction rt_1(db_1);
        ReadTransaction rt_2(db_2);
        CHECK(compare_groups(rt_1, rt_2));
    }

    // Again
    wait(session_1);

    // Wait for session 2 to download nothing
    wait(session_2);

    // Upload something via session 1
    write_transaction_notifying_session(db_1, session_1, [](WriteTransaction& wt) {
        wt.add_table("class_bar");
    });
    session_1.wait_for_upload_complete_or_client_stopped();

    // Wait for session 2 to download it
    wait(session_2);
    {
        ReadTransaction rt_1(db_1);
        ReadTransaction rt_2(db_2);
        CHECK(compare_groups(rt_1, rt_2));
    }
}


TEST(Sync_AsyncWaitForSyncCompletion)
{
    TEST_DIR(dir);
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);
    ClientServerFixture fixture(dir, test_context);
    fixture.start();

    auto wait = [&](Session& session) {
        BowlOfStonesSemaphore bowl;
        auto handler = [&](std::error_code ec) {
            if (CHECK_NOT(ec))
                bowl.add_stone();
        };
        session.async_wait_for_sync_completion(handler);
        bowl.get_stone();
    };

    // Nothing to synchronize
    Session session_1 = fixture.make_bound_session(db_1);
    wait(session_1);

    // Again
    wait(session_1);

    // Generate changes to be downloaded (uploading via session 2)
    Session session_2 = fixture.make_bound_session(db_2);
    write_transaction_notifying_session(db_2, session_2, [](WriteTransaction& wt) {
        wt.add_table("class_foo");
    });
    session_2.wait_for_upload_complete_or_client_stopped();

    // Generate changes to be uploaded
    write_transaction_notifying_session(db_1, session_1, [](WriteTransaction& wt) {
        wt.add_table("class_bar");
    });

    // Nontrivial synchronization (upload and download required)
    wait(session_1);
    wait(session_2);

    {
        ReadTransaction rt_1(db_1);
        ReadTransaction rt_2(db_2);
        CHECK(compare_groups(rt_1, rt_2));
    }
}


TEST(Sync_AsyncWaitCancellation)
{
    TEST_DIR(dir);
    TEST_CLIENT_DB(db);
    ClientServerFixture fixture(dir, test_context);

    BowlOfStonesSemaphore bowl;
    auto upload_completion_handler = [&](std::error_code ec) {
        CHECK_EQUAL(util::error::operation_aborted, ec);
        bowl.add_stone();
    };
    auto download_completion_handler = [&](std::error_code ec) {
        CHECK_EQUAL(util::error::operation_aborted, ec);
        bowl.add_stone();
    };
    auto sync_completion_handler = [&](std::error_code ec) {
        CHECK_EQUAL(util::error::operation_aborted, ec);
        bowl.add_stone();
    };
    {
        Session session = fixture.make_bound_session(db, "/test");
        session.async_wait_for_upload_completion(upload_completion_handler);
        session.async_wait_for_download_completion(download_completion_handler);
        session.async_wait_for_sync_completion(sync_completion_handler);
        // Destruction of session cancels wait operations
    }

    fixture.start();
    bowl.get_stone();
    bowl.get_stone();
    bowl.get_stone();
}


TEST(Sync_WaitForUploadCompletion)
{
    TEST_DIR(dir);
    TEST_CLIENT_DB(db);
    ClientServerFixture fixture{dir, test_context};
    std::string virtual_path = "/test";
    std::string server_path = fixture.map_virtual_to_real_path(virtual_path);
    fixture.start();

    // Empty
    Session session = fixture.make_bound_session(db);
    // Since the Realm is empty, the following wait operation can complete
    // without the client ever having been in contact with the server
    session.wait_for_upload_complete_or_client_stopped();

    // Nonempty
    write_transaction_notifying_session(db, session, [](WriteTransaction& wt) {
        wt.add_table("class_foo");
    });
    // Since the Realm is no longer empty, the following wait operation cannot
    // complete until the client has been in contact with the server, and caused
    // the server to create the server-side file
    session.wait_for_upload_complete_or_client_stopped();
    CHECK(util::File::exists(server_path));

    // Already done
    session.wait_for_upload_complete_or_client_stopped();

    // More changes
    write_transaction_notifying_session(db, session, [](WriteTransaction& wt) {
        wt.add_table("class_bar");
    });
    session.wait_for_upload_complete_or_client_stopped();
}


TEST(Sync_WaitForUploadCompletionAfterEmptyTransaction)
{
    TEST_DIR(dir);
    TEST_CLIENT_DB(db);
    ClientServerFixture fixture(dir, test_context);
    fixture.start();

    Session session = fixture.make_bound_session(db);
    for (int i = 0; i < 100; ++i) {
        WriteTransaction wt(db);
        version_type new_version = wt.commit();
        session.nonsync_transact_notify(new_version);
        session.wait_for_upload_complete_or_client_stopped();
    }
    {
        WriteTransaction wt(db);
        wt.add_table("class_foo");
        version_type new_version = wt.commit();
        session.nonsync_transact_notify(new_version);
        session.wait_for_upload_complete_or_client_stopped();
    }
}


TEST(Sync_WaitForDownloadCompletion)
{
    TEST_DIR(dir);
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);
    ClientServerFixture fixture(dir, test_context);
    fixture.start();

    // Noting to download
    Session session_1 = fixture.make_bound_session(db_1);
    session_1.wait_for_download_complete_or_client_stopped();

    // Again
    session_1.wait_for_download_complete_or_client_stopped();

    // Upload something via session 2
    Session session_2 = fixture.make_bound_session(db_2);
    write_transaction_notifying_session(db_2, session_2, [](WriteTransaction& wt) {
        wt.add_table("class_foo");
    });
    session_2.wait_for_upload_complete_or_client_stopped();

    // Wait for session 1 to download it
    session_1.wait_for_download_complete_or_client_stopped();
    {
        ReadTransaction rt_1(db_1);
        ReadTransaction rt_2(db_2);
        CHECK(compare_groups(rt_1, rt_2));
    }

    // Again
    session_1.wait_for_download_complete_or_client_stopped();

    // Wait for session 2 to download nothing
    session_2.wait_for_download_complete_or_client_stopped();

    // Upload something via session 1
    write_transaction_notifying_session(db_1, session_1, [](WriteTransaction& wt) {
        wt.add_table("class_bar");
    });
    session_1.wait_for_upload_complete_or_client_stopped();

    // Wait for session 2 to download it
    session_2.wait_for_download_complete_or_client_stopped();
    {
        ReadTransaction rt_1(db_1);
        ReadTransaction rt_2(db_2);
        CHECK(compare_groups(rt_1, rt_2));
    }
}


TEST(Sync_WaitForDownloadCompletionAfterEmptyTransaction)
{
    TEST_DIR(dir);
    TEST_CLIENT_DB(db);
    ClientServerFixture fixture(dir, test_context);

    {
        WriteTransaction wt(db);
        wt.commit();
    }
    fixture.start();
    for (int i = 0; i < 8; ++i) {
        Session session = fixture.make_bound_session(db, "/test");
        session.wait_for_download_complete_or_client_stopped();
        session.wait_for_download_complete_or_client_stopped();
        {
            WriteTransaction wt(db);
            wt.commit();
        }
        session.wait_for_download_complete_or_client_stopped();
        session.wait_for_download_complete_or_client_stopped();
    }
}


TEST(Sync_WaitForDownloadCompletionManyConcurrent)
{
    TEST_DIR(dir);
    TEST_CLIENT_DB(db);
    ClientServerFixture fixture(dir, test_context);
    fixture.start();

    Session session = fixture.make_bound_session(db);
    constexpr int num_threads = 8;
    std::thread threads[num_threads];
    for (int i = 0; i < num_threads; ++i) {
        auto handler = [&] {
            session.wait_for_download_complete_or_client_stopped();
        };
        threads[i] = std::thread{handler};
    }
    for (int i = 0; i < num_threads; ++i)
        threads[i].join();
}


TEST(Sync_WaitForSessionTerminations)
{
    TEST_DIR(server_dir);
    TEST_CLIENT_DB(db);

    ClientServerFixture fixture(server_dir, test_context);
    fixture.start();

    Session session = fixture.make_bound_session(db, "/test");
    session.wait_for_download_complete_or_client_stopped();
    // Note: Atomicity would not be needed if
    // Session::async_wait_for_download_completion() was assumed to work.
    std::atomic<bool> called{false};
    auto handler = [&](std::error_code) {
        called = true;
    };
    session.async_wait_for_download_completion(std::move(handler));
    session.detach();
    // The completion handler of an asynchronous wait operation is guaranteed
    // to be called, and no later than at session termination time. Also, any
    // callback function associated with a session on which termination has been
    // initiated, including the completion handler of the asynchronous wait
    // operation, must have finished executing when
    // Client::wait_for_session_terminations_or_client_stopped() returns.
    fixture.wait_for_session_terminations_or_client_stopped();
    CHECK(called);
}


TEST(Sync_AuthFailure)
{
    bool did_fail = false;
    {
        TEST_DIR(dir);
        TEST_CLIENT_DB(db);
        ClientServerFixture fixture(dir, test_context);
        auto error_handler = [&](std::error_code ec, bool, const std::string&) {
            CHECK_EQUAL(ProtocolError::bad_authentication, ec);
            did_fail = true;
            fixture.stop();
        };
        fixture.set_client_side_error_handler(std::move(error_handler));
        fixture.start();

        Session session = fixture.make_session(db);
        std::string wrong_signed_user_token{g_signed_test_user_token};
        wrong_signed_user_token[0] = 'a';
        fixture.bind_session(session, "/test", wrong_signed_user_token);
        write_transaction_notifying_session(db, session, [](WriteTransaction& wt) {
            wt.add_table("class_foo");
        });
        session.wait_for_upload_complete_or_client_stopped();
        session.wait_for_download_complete_or_client_stopped();
    }
    CHECK(did_fail);
}


TEST(Sync_TokenWithoutExpirationAllowed)
{
    bool did_fail = false;
    {
        TEST_DIR(dir);
        TEST_CLIENT_DB(db);
        ClientServerFixture fixture(dir, test_context);

        using ErrorInfo = Session::ErrorInfo;
        auto listener = [&](ConnectionState state, const ErrorInfo* error_info) {
            if (state != ConnectionState::disconnected)
                return;
            REALM_ASSERT(error_info);
            std::error_code ec = error_info->error_code;
            CHECK(ec == sync::ProtocolError::token_expired || ec == sync::ProtocolError::bad_authentication ||
                  ec == sync::ProtocolError::permission_denied);
            did_fail = true;
            fixture.stop();
        };

        fixture.start();

        Session session = fixture.make_session(db);
        session.set_connection_state_change_listener(listener);
        fixture.bind_session(session, "/test", g_signed_test_user_token_expiration_unspecified);
        write_transaction_notifying_session(db, session, [](WriteTransaction& wt) {
            wt.add_table("class_foo");
        });
        session.wait_for_upload_complete_or_client_stopped();
        session.wait_for_download_complete_or_client_stopped();
    }
    CHECK_NOT(did_fail);
}


TEST(Sync_TokenWithNullExpirationAllowed)
{
    bool did_fail = false;
    {
        TEST_DIR(dir);
        TEST_CLIENT_DB(db);
        ClientServerFixture fixture(dir, test_context);
        auto error_handler = [&](std::error_code, bool, const std::string&) {
            fixture.stop();
            did_fail = true;
        };
        fixture.set_client_side_error_handler(error_handler);
        fixture.start();

        Session session = fixture.make_session(db);
        fixture.bind_session(session, "/test", g_signed_test_user_token_expiration_null);
        {
            write_transaction_notifying_session(db, session, [](WriteTransaction& wt) {
                wt.add_table("class_foo");
            });
        }
        session.wait_for_upload_complete_or_client_stopped();
        session.wait_for_download_complete_or_client_stopped();
    }
    CHECK_NOT(did_fail);
}


TEST(Sync_UnexpiredTokenValidAndExpires)
{
    bool did_fail = false;
    {
        TEST_DIR(dir);
        TEST_CLIENT_DB(db);
        ClientServerFixture fixture(dir, test_context);
        auto error_handler = [&](std::error_code ec, bool, const std::string&) {
            CHECK_EQUAL(ProtocolError::token_expired, ec);
            fixture.stop();
            did_fail = true;
        };
        fixture.set_client_side_error_handler(error_handler);
        fixture.start();

        fixture.set_fake_token_expiration_time(2999999999); // One second before the token expiration

        Session session = fixture.make_session(db);
        fixture.bind_session(session, "/test", g_signed_test_user_token_expiration_specified);

        write_transaction_notifying_session(db, session, [](WriteTransaction& wt) {
            wt.add_table("class_foo");
        });
        session.wait_for_upload_complete_or_client_stopped();
        fixture.set_fake_token_expiration_time(3000000001); // One second after the token expiration
        session.wait_for_download_complete_or_client_stopped();
    }
    CHECK(did_fail);
}


TEST(Sync_RefreshExpiredToken)
{
    bool did_fail = false;
    {
        TEST_DIR(dir);
        TEST_CLIENT_DB(db);
        ClientServerFixture fixture(dir, test_context);
        auto error_handler = [&](std::error_code ec, bool, const std::string&) {
            CHECK_EQUAL(ProtocolError::token_expired, ec);
            fixture.stop();
            did_fail = true;
        };
        fixture.set_client_side_error_handler(error_handler);
        fixture.start();

        fixture.set_fake_token_expiration_time(2999999999); // One second before the token expiration

        Session session = fixture.make_session(db);
        fixture.bind_session(session, "/test", g_signed_test_user_token_expiration_specified);

        write_transaction_notifying_session(db, session, [](WriteTransaction& wt) {
            wt.add_table("class_foo");
        });
        session.wait_for_upload_complete_or_client_stopped();
        fixture.set_fake_token_expiration_time(3000000001); // One second after the token expiration
        session.refresh(g_signed_test_user_token_expiration_unspecified);
        session.wait_for_download_complete_or_client_stopped();
    }
    CHECK_NOT(did_fail);
}


TEST(Sync_RefreshChangeUserNotAllowed)
{
    TEST_DIR(dir);
    TEST_CLIENT_DB(db);
    ClientServerFixture fixture(dir, test_context);

    BowlOfStonesSemaphore bowl;
    auto error_handler = [&](std::error_code ec, bool, const std::string&) {
        CHECK_EQUAL(ProtocolError::bad_authentication, ec);
        fixture.stop();
        bowl.add_stone();
    };
    fixture.set_client_side_error_handler(error_handler);
    fixture.start();

    Session session = fixture.make_session(db);
    fixture.bind_session(session, "/test", g_user_0_path_test_token);
    session.wait_for_download_complete_or_client_stopped();

    // Change user
    session.refresh(g_user_1_path_test_token);
    bowl.get_stone();
}


TEST(Sync_CannotBindWithExpiredToken)
{
    bool did_fail = false;
    {
        TEST_DIR(dir);
        TEST_CLIENT_DB(db);
        ClientServerFixture fixture(dir, test_context);
        auto error_handler = [&](std::error_code ec, bool, const std::string&) {
            CHECK_EQUAL(ProtocolError::token_expired, ec);
            fixture.stop();
            did_fail = true;
        };
        fixture.set_client_side_error_handler(error_handler);
        fixture.start();
        fixture.set_fake_token_expiration_time(3000000001); // One second after the token expiration

        Session session = fixture.make_session(db);
        fixture.bind_session(session, "/test", g_signed_test_user_token_expiration_specified);
        write_transaction_notifying_session(db, session, [](WriteTransaction& wt) {
            wt.add_table("class_foo");
        });
        session.wait_for_upload_complete_or_client_stopped();
        session.wait_for_download_complete_or_client_stopped();
    }
    CHECK(did_fail);
}


TEST(Sync_CannotRefreshWithExpiredToken)
{
    bool did_fail = false;
    {
        TEST_DIR(dir);
        TEST_CLIENT_DB(db);
        ClientServerFixture fixture(dir, test_context);
        auto error_handler = [&](std::error_code ec, bool, const std::string&) {
            CHECK_EQUAL(ProtocolError::token_expired, ec);
            fixture.stop();
            did_fail = true;
        };
        fixture.set_client_side_error_handler(error_handler);
        fixture.start();
        fixture.set_fake_token_expiration_time(3000000001); // One second after the token expiration

        Session session = fixture.make_session(db);
        fixture.bind_session(session, "/test", g_signed_test_user_token_expiration_unspecified);
        write_transaction_notifying_session(db, session, [](WriteTransaction& wt) {
            wt.add_table("class_foo");
        });
        session.wait_for_upload_complete_or_client_stopped();
        session.wait_for_download_complete_or_client_stopped();
        session.refresh(g_signed_test_user_token_expiration_specified);
        session.wait_for_download_complete_or_client_stopped();
    }
    CHECK(did_fail);
}


TEST(Sync_CanRefreshTokenAfterExpirationError)
{
    // Note: A failure in this test is expected to cause an indefinite hang in
    // the final call to
    // Session::wait_for_download_complete_or_client_stopped().

    TEST_DIR(dir);
    TEST_CLIENT_DB(db);
    ClientServerFixture fixture(dir, test_context);

    BowlOfStonesSemaphore bowl;
    auto error_handler = [&](std::error_code ec, bool, const std::string&) {
        CHECK_EQUAL(ProtocolError::token_expired, ec);
        bowl.add_stone();
    };

    fixture.set_client_side_error_handler(error_handler);
    fixture.start();

    fixture.set_fake_token_expiration_time(3000000001); // One second after the token expiration

    Session session = fixture.make_session(db);
    fixture.bind_session(session, "/test", g_signed_test_user_token_expiration_specified);
    bowl.get_stone();
    session.refresh(g_signed_test_user_token_expiration_unspecified);
    session.wait_for_download_complete_or_client_stopped();
}


TEST(Sync_Upload)
{
    TEST_DIR(dir);
    TEST_CLIENT_DB(db);
    ClientServerFixture fixture(dir, test_context);
    fixture.start();

    Session session = fixture.make_bound_session(db);

    {
        write_transaction_notifying_session(db, session, [](WriteTransaction& wt) {
            TableRef table = wt.add_table("class_foo");
            table->add_column(type_Int, "i");
        });
        for (int i = 0; i < 100; ++i) {
            WriteTransaction wt(db);
            TableRef table = wt.get_table("class_foo");
            table->create_object();
            version_type new_version = wt.commit();
            session.nonsync_transact_notify(new_version);
        }
    }
    session.wait_for_upload_complete_or_client_stopped();
    session.wait_for_download_complete_or_client_stopped();
}


TEST(Sync_Replication)
{
    // Replicate changes in file 1 to file 2.

    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    {
        TEST_DIR(dir);
        ClientServerFixture fixture(dir, test_context);
        fixture.start();

        version_type sync_transact_callback_version = 0;
        auto sync_transact_callback = [&](VersionID, VersionID new_version) {
            // May be called once or multiple times depending on timing
            sync_transact_callback_version = new_version.version;
        };

        Session session_1 = fixture.make_bound_session(db_1);

        Session session_2 = fixture.make_session(db_2);
        session_2.set_sync_transact_callback(std::move(sync_transact_callback));
        fixture.bind_session(session_2, "/test");

        // Create schema
        write_transaction_notifying_session(db_1, session_1, [](WriteTransaction& wt) {
            TableRef table = wt.add_table("class_foo");
            table->add_column(type_Int, "i");
        });
        Random random(random_int<unsigned long>()); // Seed from slow global generator
        for (int i = 0; i < 100; ++i) {
            WriteTransaction wt(db_1);
            TableRef table = wt.get_table("class_foo");
            table->create_object();
            Obj obj = *(table->begin() + random.draw_int_mod(table->size()));
            obj.set<int64_t>("i", random.draw_int_max(0x7FFFFFFFFFFFFFFF));
            version_type new_version = wt.commit();
            session_1.nonsync_transact_notify(new_version);
        }

        session_1.wait_for_upload_complete_or_client_stopped();
        session_2.wait_for_download_complete_or_client_stopped();

        {
            ReadTransaction rt(db_2);
            CHECK_EQUAL(rt.get_version(), sync_transact_callback_version);
        }
    }

    ReadTransaction rt_1(db_1);
    ReadTransaction rt_2(db_2);
    const Group& group_1 = rt_1;
    const Group& group_2 = rt_2;
    group_1.verify();
    group_2.verify();
    CHECK(compare_groups(rt_1, rt_2));
    ConstTableRef table = group_1.get_table("class_foo");
    CHECK_EQUAL(100, table->size());
}


TEST(Sync_Merge)
{

    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    {
        TEST_DIR(dir);
        MultiClientServerFixture fixture(2, 1, dir, test_context);
        fixture.start();

        Session session_1 = fixture.make_session(0, db_1);
        fixture.bind_session(session_1, 0, "/test");

        Session session_2 = fixture.make_session(1, db_2);
        fixture.bind_session(session_2, 0, "/test");

        // Create schema on both clients.
        auto create_schema = [](Session& sess, DBRef db) {
            WriteTransaction wt(db);
            if (wt.has_table("class_foo"))
                return;
            TableRef table = wt.add_table("class_foo");
            table->add_column(type_Int, "i");
            version_type new_version = wt.commit();
            sess.nonsync_transact_notify(new_version);
        };
        create_schema(session_1, db_1);
        create_schema(session_2, db_2);

        write_transaction_notifying_session(db_1, session_1, [](WriteTransaction& wt) {
            TableRef table = wt.get_table("class_foo");
            table->create_object().set("i", 5);
            table->create_object().set("i", 6);
        });
        write_transaction_notifying_session(db_2, session_2, [](WriteTransaction& wt) {
            TableRef table = wt.get_table("class_foo");
            table->create_object().set("i", 7);
            table->create_object().set("i", 8);
        });

        session_1.wait_for_upload_complete_or_client_stopped();
        session_2.wait_for_upload_complete_or_client_stopped();
        session_1.wait_for_download_complete_or_client_stopped();
        session_2.wait_for_download_complete_or_client_stopped();
    }

    ReadTransaction rt_1(db_1);
    ReadTransaction rt_2(db_2);
    const Group& group_1 = rt_1;
    const Group& group_2 = rt_2;
    group_1.verify();
    group_2.verify();
    CHECK(compare_groups(rt_1, rt_2));
    ConstTableRef table = group_1.get_table("class_foo");
    CHECK_EQUAL(4, table->size());
}


TEST(Sync_DetectSchemaMismatch_ColumnType)
{
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    {
        TEST_DIR(dir);
        MultiClientServerFixture fixture(2, 1, dir, test_context);
        fixture.allow_server_errors(0, 1);
        fixture.start();

        using ErrorInfo = Session::ErrorInfo;
        auto listener = [&](ConnectionState state, const ErrorInfo* error_info) {
            if (state != ConnectionState::disconnected)
                return;
            REALM_ASSERT(error_info);
            std::error_code ec = error_info->error_code;
            bool is_fatal = error_info->is_fatal;
            CHECK(ec == sync::Client::Error::bad_changeset || ec == sync::ProtocolError::invalid_schema_change);
            CHECK(is_fatal);
            // FIXME: Check that the message in the log is user-friendly.
            fixture.stop();
        };

        Session session_1 = fixture.make_session(0, db_1);
        Session session_2 = fixture.make_session(1, db_2);

        session_1.set_connection_state_change_listener(listener);
        session_2.set_connection_state_change_listener(listener);

        fixture.bind_session(session_1, 0, "/test");
        fixture.bind_session(session_2, 0, "/test");

        write_transaction_notifying_session(db_1, session_1, [](WriteTransaction& wt) {
            TableRef table = wt.add_table("class_foo");
            ColKey col_ndx = table->add_column(type_Int, "column");
            table->create_object().set<int64_t>(col_ndx, 123);
        });

        write_transaction_notifying_session(db_2, session_2, [](WriteTransaction& wt) {
            TableRef table = wt.add_table("class_foo");
            ColKey col_ndx = table->add_column(type_String, "column");
            table->create_object().set(col_ndx, "Hello, World!");
        });
        session_1.wait_for_upload_complete_or_client_stopped();
        session_2.wait_for_upload_complete_or_client_stopped();
        session_1.wait_for_download_complete_or_client_stopped();
        session_2.wait_for_download_complete_or_client_stopped();
    }
}


TEST(Sync_DetectSchemaMismatch_Nullability)
{
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    {
        TEST_DIR(dir);
        MultiClientServerFixture fixture(2, 1, dir, test_context);
        fixture.allow_server_errors(0, 1);
        fixture.start();

        using ErrorInfo = Session::ErrorInfo;
        auto listener = [&](ConnectionState state, const ErrorInfo* error_info) {
            if (state != ConnectionState::disconnected)
                return;
            REALM_ASSERT(error_info);
            std::error_code ec = error_info->error_code;
            bool is_fatal = error_info->is_fatal;
            CHECK(ec == sync::Client::Error::bad_changeset || ec == sync::ProtocolError::invalid_schema_change);
            CHECK(is_fatal);
            // FIXME: Check that the message in the log is user-friendly.
            fixture.stop();
        };

        Session session_1 = fixture.make_session(0, db_1);
        Session session_2 = fixture.make_session(1, db_2);

        session_1.set_connection_state_change_listener(listener);
        session_2.set_connection_state_change_listener(listener);

        fixture.bind_session(session_1, 0, "/test");
        fixture.bind_session(session_2, 0, "/test");

        write_transaction_notifying_session(db_1, session_1, [](WriteTransaction& wt) {
            TableRef table = wt.add_table("class_foo");
            bool nullable = false;
            ColKey col_ndx = table->add_column(type_Int, "column", nullable);
            table->create_object().set<int64_t>(col_ndx, 123);
        });

        write_transaction_notifying_session(db_2, session_2, [](WriteTransaction& wt) {
            TableRef table = wt.add_table("class_foo");
            bool nullable = true;
            ColKey col_ndx = table->add_column(type_Int, "column", nullable);
            table->create_object().set<int64_t>(col_ndx, 123);
        });
        session_1.wait_for_upload_complete_or_client_stopped();
        session_2.wait_for_upload_complete_or_client_stopped();
        session_1.wait_for_download_complete_or_client_stopped();
        session_2.wait_for_download_complete_or_client_stopped();
    }
}


TEST(Sync_DetectSchemaMismatch_Links)
{
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    {
        TEST_DIR(dir);
        MultiClientServerFixture fixture(2, 1, dir, test_context);
        fixture.allow_server_errors(0, 1);
        fixture.start();

        using ErrorInfo = Session::ErrorInfo;
        auto listener = [&](ConnectionState state, const ErrorInfo* error_info) {
            if (state != ConnectionState::disconnected)
                return;
            REALM_ASSERT(error_info);
            std::error_code ec = error_info->error_code;
            bool is_fatal = error_info->is_fatal;
            CHECK(ec == sync::Client::Error::bad_changeset || ec == sync::ProtocolError::invalid_schema_change);
            CHECK(is_fatal);
            // FIXME: Check that the message in the log is user-friendly.
            fixture.stop();
        };

        Session session_1 = fixture.make_session(0, db_1);
        Session session_2 = fixture.make_session(1, db_2);

        session_1.set_connection_state_change_listener(listener);
        session_2.set_connection_state_change_listener(listener);

        fixture.bind_session(session_1, 0, "/test");
        fixture.bind_session(session_2, 0, "/test");

        write_transaction_notifying_session(db_1, session_1, [](WriteTransaction& wt) {
            TableRef table = wt.add_table("class_foo");
            TableRef target = wt.add_table("class_bar");
            table->add_column(*target, "column");
        });

        write_transaction_notifying_session(db_2, session_2, [](WriteTransaction& wt) {
            TableRef table = wt.add_table("class_foo");
            TableRef target = wt.add_table("class_baz");
            table->add_column(*target, "column");
        });
        session_1.wait_for_upload_complete_or_client_stopped();
        session_2.wait_for_upload_complete_or_client_stopped();
        session_1.wait_for_download_complete_or_client_stopped();
        session_2.wait_for_download_complete_or_client_stopped();
    }
}


TEST(Sync_DetectSchemaMismatch_PrimaryKeys_Name)
{
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    {
        TEST_DIR(dir);
        MultiClientServerFixture fixture(2, 1, dir, test_context);
        fixture.allow_server_errors(0, 1);
        fixture.start();

        using ErrorInfo = Session::ErrorInfo;
        auto listener = [&](ConnectionState state, const ErrorInfo* error_info) {
            if (state != ConnectionState::disconnected)
                return;
            REALM_ASSERT(error_info);
            std::error_code ec = error_info->error_code;
            bool is_fatal = error_info->is_fatal;
            CHECK(ec == sync::Client::Error::bad_changeset || ec == sync::ProtocolError::invalid_schema_change);
            CHECK(is_fatal);
            // FIXME: Check that the message in the log is user-friendly.
            fixture.stop();
        };

        Session session_1 = fixture.make_session(0, db_1);
        Session session_2 = fixture.make_session(1, db_2);

        session_1.set_connection_state_change_listener(listener);
        session_2.set_connection_state_change_listener(listener);

        fixture.bind_session(session_1, 0, "/test");
        fixture.bind_session(session_2, 0, "/test");

        write_transaction_notifying_session(db_1, session_1, [](WriteTransaction& wt) {
            wt.get_group().add_table_with_primary_key("class_foo", type_Int, "a");
        });

        write_transaction_notifying_session(db_2, session_2, [](WriteTransaction& wt) {
            wt.get_group().add_table_with_primary_key("class_foo", type_Int, "b");
        });
        session_1.wait_for_upload_complete_or_client_stopped();
        session_2.wait_for_upload_complete_or_client_stopped();
        session_1.wait_for_download_complete_or_client_stopped();
        session_2.wait_for_download_complete_or_client_stopped();
    }
}


TEST(Sync_DetectSchemaMismatch_PrimaryKeys_Type)
{
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    {
        TEST_DIR(dir);
        MultiClientServerFixture fixture(2, 1, dir, test_context);
        fixture.allow_server_errors(0, 1);
        fixture.start();

        using ErrorInfo = Session::ErrorInfo;
        auto listener = [&](ConnectionState state, const ErrorInfo* error_info) {
            if (state != ConnectionState::disconnected)
                return;
            REALM_ASSERT(error_info);
            std::error_code ec = error_info->error_code;
            bool is_fatal = error_info->is_fatal;
            CHECK(ec == sync::Client::Error::bad_changeset || ec == sync::ProtocolError::invalid_schema_change);
            CHECK(is_fatal);
            // FIXME: Check that the message in the log is user-friendly.
            fixture.stop();
        };

        Session session_1 = fixture.make_session(0, db_1);
        Session session_2 = fixture.make_session(1, db_2);

        session_1.set_connection_state_change_listener(listener);
        session_2.set_connection_state_change_listener(listener);

        fixture.bind_session(session_1, 0, "/test");
        fixture.bind_session(session_2, 0, "/test");

        write_transaction_notifying_session(db_1, session_1, [](WriteTransaction& wt) {
            wt.get_group().add_table_with_primary_key("class_foo", type_Int, "a");
        });

        write_transaction_notifying_session(db_2, session_2, [](WriteTransaction& wt) {
            wt.get_group().add_table_with_primary_key("class_foo", type_String, "a");
        });
        session_1.wait_for_upload_complete_or_client_stopped();
        session_2.wait_for_upload_complete_or_client_stopped();
        session_1.wait_for_download_complete_or_client_stopped();
        session_2.wait_for_download_complete_or_client_stopped();
    }
}


TEST(Sync_DetectSchemaMismatch_PrimaryKeys_Nullability)
{
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    {
        TEST_DIR(dir);
        MultiClientServerFixture fixture(2, 1, dir, test_context);
        fixture.allow_server_errors(0, 1);
        fixture.start();

        bool error_did_occur = false;

        using ErrorInfo = Session::ErrorInfo;
        auto listener = [&](ConnectionState state, const ErrorInfo* error_info) {
            if (state != ConnectionState::disconnected)
                return;
            REALM_ASSERT(error_info);
            std::error_code ec = error_info->error_code;
            bool is_fatal = error_info->is_fatal;
            CHECK(ec == sync::Client::Error::bad_changeset || ec == sync::ProtocolError::invalid_schema_change);
            CHECK(is_fatal);
            // FIXME: Check that the message in the log is user-friendly.
            error_did_occur = true;
            fixture.stop();
        };

        Session session_1 = fixture.make_session(0, db_1);
        Session session_2 = fixture.make_session(1, db_2);

        session_1.set_connection_state_change_listener(listener);
        session_2.set_connection_state_change_listener(listener);

        fixture.bind_session(session_1, 0, "/test");
        fixture.bind_session(session_2, 0, "/test");

        write_transaction_notifying_session(db_1, session_1, [](WriteTransaction& wt) {
            bool nullable = false;
            wt.get_group().add_table_with_primary_key("class_foo", type_Int, "a", nullable);
        });

        write_transaction_notifying_session(db_2, session_2, [](WriteTransaction& wt) {
            bool nullable = true;
            wt.get_group().add_table_with_primary_key("class_foo", type_Int, "a", nullable);
        });
        session_1.wait_for_upload_complete_or_client_stopped();
        session_2.wait_for_upload_complete_or_client_stopped();
        session_1.wait_for_download_complete_or_client_stopped();
        session_2.wait_for_download_complete_or_client_stopped();
        CHECK(error_did_occur);
    }
}


TEST(Sync_LateBind)
{
    // Test that a session can be initiated at a point in time where the client
    // already has established a connection to the server.

    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    {
        TEST_DIR(dir);
        ClientServerFixture fixture(dir, test_context);
        fixture.start();

        Session session_1 = fixture.make_bound_session(db_1);
        write_transaction_notifying_session(db_1, session_1, [](WriteTransaction& wt) {
            wt.add_table("class_foo");
        });
        session_1.wait_for_upload_complete_or_client_stopped();

        Session session_2 = fixture.make_bound_session(db_2);
        write_transaction_notifying_session(db_2, session_2, [](WriteTransaction& wt) {
            wt.add_table("class_bar");
        });
        session_2.wait_for_upload_complete_or_client_stopped();

        session_1.wait_for_download_complete_or_client_stopped();
        session_2.wait_for_download_complete_or_client_stopped();
    }

    ReadTransaction rt_1(db_1);
    ReadTransaction rt_2(db_2);
    const Group& group_1 = rt_1;
    const Group& group_2 = rt_2;
    group_1.verify();
    group_2.verify();
    CHECK(compare_groups(rt_1, rt_2));
    CHECK_EQUAL(2, group_1.size());
}


TEST(Sync_EarlyUnbind)
{
    // Verify that it is possible to unbind one session while another session
    // keeps the connection to the server open.

    TEST_DIR(dir);
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);
    TEST_CLIENT_DB(db_3);
    ClientServerFixture fixture(dir, test_context);
    fixture.start();

    // Session 1 is here only to keep the connection alive
    Session session_1 = fixture.make_bound_session(db_1, "/dummy");
    {
        Session session_2 = fixture.make_bound_session(db_2);
        write_transaction_notifying_session(db_2, session_2, [](WriteTransaction& wt) {
            wt.add_table("class_foo");
        });
        session_2.wait_for_upload_complete_or_client_stopped();
        // Session 2 is now connected, but will be abandoned at end of scope
    }
    {
        // Starting a new session 3 forces closure of all previously abandoned
        // sessions, in turn forcing session 2 to be enlisted for writing its
        // UNBIND before session 3 is enlisted for writing BIND.
        Session session_3 = fixture.make_bound_session(db_3);
        // We now use MARK messages to wait for a complete unbind of session
        // 2. The client is guaranteed to receive the UNBIND response for session
        // 2 before it receives the MARK response for session 3.
        session_3.wait_for_download_complete_or_client_stopped();
    }
}


TEST(Sync_FastRebind)
{
    // Verify that it is possible to create multiple immediately consecutive
    // sessions for the same Realm file.

    TEST_DIR(dir);
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);
    ClientServerFixture fixture(dir, test_context);
    fixture.start();

    // Session 1 is here only to keep the connection alive
    Session session_1 = fixture.make_bound_session(db_1, "/dummy");
    {
        Session session_2 = fixture.make_bound_session(db_2, "/test");
        WriteTransaction wt(db_2);
        TableRef table = wt.add_table("class_foo");
        table->add_column(type_Int, "i");
        table->create_object();
        version_type new_version = wt.commit();
        session_2.nonsync_transact_notify(new_version);
        session_2.wait_for_upload_complete_or_client_stopped();
    }
    for (int i = 0; i < 100; ++i) {
        Session session_2 = fixture.make_bound_session(db_2, "/test");
        WriteTransaction wt(db_2);
        TableRef table = wt.get_table("class_foo");
        table->begin()->set<int64_t>("i", i);
        version_type new_version = wt.commit();
        session_2.nonsync_transact_notify(new_version);
        session_2.wait_for_upload_complete_or_client_stopped();
    }
}


TEST(Sync_UnbindBeforeActivation)
{
    // This test tries to make it likely that the server receives an UNBIND
    // message for a session that is still not activated, i.e., before the
    // server receives the IDENT message.

    TEST_DIR(dir);
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);
    ClientServerFixture fixture(dir, test_context);
    fixture.start();

    // Session 1 is here only to keep the connection alive
    Session session_1 = fixture.make_bound_session(db_1);
    for (int i = 0; i < 1000; ++i) {
        Session session_2 = fixture.make_bound_session(db_2);
        session_2.wait_for_upload_complete_or_client_stopped();
    }
}


TEST(Sync_AbandonUnboundSessions)
{
    TEST_DIR(dir);
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);
    TEST_CLIENT_DB(db_3);
    ClientServerFixture fixture(dir, test_context);
    fixture.start();

    int n = 32;
    for (int i = 0; i < n; ++i) {
        fixture.make_session(db_1);
        fixture.make_session(db_2);
        fixture.make_session(db_3);
    }

    for (int i = 0; i < n; ++i) {
        fixture.make_session(db_1);
        Session session = fixture.make_session(db_2);
        fixture.make_session(db_3);
        fixture.bind_session(session, "/test");
    }

    for (int i = 0; i < n; ++i) {
        fixture.make_session(db_1);
        Session session = fixture.make_session(db_2);
        fixture.make_session(db_3);
        fixture.bind_session(session, "/test");
        session.wait_for_upload_complete_or_client_stopped();
    }

    for (int i = 0; i < n; ++i) {
        fixture.make_session(db_1);
        Session session = fixture.make_session(db_2);
        fixture.make_session(db_3);
        fixture.bind_session(session, "/test");
        session.wait_for_download_complete_or_client_stopped();
    }
}


#if 0  // FIXME: Disabled because substring operations are not yet supported in Core 6.

// This test illustrates that our instruction set and merge rules
// do not have higher order convergence. The final merge result depends
// on the order with which the changesets reach the server. This example
// employs three clients operating on the same state. The state consists
// of two tables, "source" and "target". "source" has a link list pointing
// to target. Target contains three rows 0, 1, and 2. Source contains one
// row with a link list whose value is 2.
//
// The three clients produce changesets with client 1 having the earliest time
// stamp, client 2 the middle time stamp, and client 3 the latest time stamp.
// The clients produce the following changesets.
//
// client 1: target.move_last_over(0)
// client 2: source.link_list.set(0, 0);
// client 3: source.link_list.set(0, 1);
//
// In part a of the test, the order of the clients reaching the server is
// 1, 2, 3. The result is an empty link list since the merge of client 1 and 2
// produces a nullify link list instruction.
//
// In part b, the order of the clients reaching the server is 3, 1, 2. The
// result is a link list of size 1, since client 3 wins due to having the
// latest time stamp.
//
// If the "natural" peer to peer system of these merge rules were built, the
// transition from server a to server b involves an insert link instruction. In
// other words, the diff between two servers differing in the order of one
// move_last_over and two link_list_set instructions is an insert instruction.
// Repeated application of the pairwise merge rules would never produce this
// result.
//
// The test is not run in general since it just checks that we do not have
// higher order convergence, and the absence of higher order convergence is not
// a desired feature in itself.
TEST_IF(Sync_NonDeterministicMerge, false)
{
    TEST_DIR(dir);
    TEST_CLIENT_DB(db_a1);
    TEST_CLIENT_DB(db_a2);
    TEST_CLIENT_DB(db_a3);
    TEST_CLIENT_DB(db_b1);
    TEST_CLIENT_DB(db_b2);
    TEST_CLIENT_DB(db_b3);

    ClientServerFixture fixture{dir, test_context};
    fixture.start();

    // Part a of the test.
    {
        WriteTransaction wt{db_a1};

        TableRef table_target = wt.add_table("class_target");
        ColKey col_ndx = table_target->add_column(type_Int, "value");
        CHECK_EQUAL(col_ndx, 1);
        Obj row0 = table_target->create_object();
        Obj row1 = table_target->create_object();
        Obj row2 = table_target->create_object();
        row0.set(col_ndx, 123);
        row1.set(col_ndx, 456);
        row2.set(col_ndx, 789);

        TableRef table_source = wt.add_table("class_source");
        col_ndx = table_source->add_column_link(type_LinkList, "target_link",
                                                *table_target);
        CHECK_EQUAL(col_ndx, 1);
        Obj obj = table_source->create_object();
        auto ll = obj.get_linklist(col_ndx);
        ll.insert(0, row2.get_key());
        CHECK_EQUAL(ll.size(), 1);
        wt.commit();
    }

    {
        Session session = fixture.make_bound_session(db_a1, "/server-path-a");
        session.wait_for_upload_complete_or_client_stopped();
    }

    {
        Session session = fixture.make_bound_session(db_a2, "/server-path-a");
        session.wait_for_download_complete_or_client_stopped();
    }

    {
        Session session = fixture.make_bound_session(db_a3, "/server-path-a");
        session.wait_for_download_complete_or_client_stopped();
    }

    {
        WriteTransaction wt{db_a1};
        TableRef table = wt.get_table("class_target");
        table->remove_object(table->begin());
        CHECK_EQUAL(table->size(), 2);
        wt.commit();
    }

    {
        WriteTransaction wt{db_a2};
        TableRef table = wt.get_table("class_source");
        auto ll = table->get_linklist(1, 0);
        CHECK_EQUAL(ll->size(), 1);
        CHECK_EQUAL(ll->get(0).get_int(1), 789);
        ll->set(0, 0);
        CHECK_EQUAL(ll->get(0).get_int(1), 123);
        wt.commit();
    }

    {
        WriteTransaction wt{db_a3};
        TableRef table = wt.get_table("class_source");
        auto ll = table->get_linklist(1, 0);
        CHECK_EQUAL(ll->size(), 1);
        CHECK_EQUAL(ll->get(0).get_int(1), 789);
        ll->set(0, 1);
        CHECK_EQUAL(ll->get(0).get_int(1), 456);
        wt.commit();
    }

    {
        Session session = fixture.make_bound_session(db_a1, "/server-path-a");
        session.wait_for_upload_complete_or_client_stopped();
    }

    {
        Session session = fixture.make_bound_session(db_a2, "/server-path-a");
        session.wait_for_upload_complete_or_client_stopped();
    }

    {
        Session session = fixture.make_bound_session(db_a3, "/server-path-a");
        session.wait_for_upload_complete_or_client_stopped();
    }

    {
        Session session = fixture.make_bound_session(db_a1, "/server-path-a");
        session.wait_for_download_complete_or_client_stopped();
    }

    // Part b of the test.
    {
        WriteTransaction wt{db_b1};

        TableRef table_target = wt.add_table("class_target");
        ColKey col_ndx = table_target->add_column(type_Int, "value");
        CHECK_EQUAL(col_ndx, 1);
        table_target->create_object();
        table_target->create_object();
        table_target->create_object();
        table_target->begin()->set(col_ndx, 123);
        table_target->get_object(1).set(col_ndx, 456);
        table_target->get_object(2).set(col_ndx, 789);

        TableRef table_source = wt.add_table("class_source");
        col_ndx = table_source->add_column_link(type_LinkList, "target_link",
                                                *table_target);
        CHECK_EQUAL(col_ndx, 1);
        table_source->create_object();
        auto ll = table_source->get_linklist(col_ndx, 0);
        ll->insert(0, 2);
        CHECK_EQUAL(ll->size(), 1);
        wt.commit();
    }

    {
        Session session = fixture.make_bound_session(db_b1, "/server-path-b");
        session.wait_for_upload_complete_or_client_stopped();
    }

    {
        Session session = fixture.make_bound_session(db_b2, "/server-path-b");
        session.wait_for_download_complete_or_client_stopped();
    }

    {
        Session session = fixture.make_bound_session(db_b3, "/server-path-b");
        session.wait_for_download_complete_or_client_stopped();
    }

    {
        WriteTransaction wt{db_b1};
        TableRef table = wt.get_table("class_target");
        table->move_last_over(0);
        CHECK_EQUAL(table->size(), 2);
        wt.commit();
    }

    {
        WriteTransaction wt{db_b2};
        TableRef table = wt.get_table("class_source");
        auto ll = table->get_linklist(1, 0);
        CHECK_EQUAL(ll->size(), 1);
        CHECK_EQUAL(ll->get(0).get_int(1), 789);
        ll->set(0, 0);
        CHECK_EQUAL(ll->get(0).get_int(1), 123);
        wt.commit();
    }

    {
        WriteTransaction wt{db_b3};
        TableRef table = wt.get_table("class_source");
        auto ll = table->get_linklist(1, 0);
        CHECK_EQUAL(ll->size(), 1);
        CHECK_EQUAL(ll->get(0).get_int(1), 789);
        ll->set(0, 1);
        CHECK_EQUAL(ll->get(0).get_int(1), 456);
        wt.commit();
    }

    // The crucial difference between part a and b is that client 3
    // uploads it changes first in part b and last in part a.
    {
        Session session = fixture.make_bound_session(db_b3, "/server-path-b");
        session.wait_for_upload_complete_or_client_stopped();
    }

    {
        Session session = fixture.make_bound_session(db_b1, "/server-path-b");
        session.wait_for_upload_complete_or_client_stopped();
    }

    {
        Session session = fixture.make_bound_session(db_b2, "/server-path-b");
        session.wait_for_upload_complete_or_client_stopped();
    }

    {
        Session session = fixture.make_bound_session(db_b1, "/server-path-b");
        session.wait_for_download_complete_or_client_stopped();
    }


    // Check the end result.

    size_t size_link_list_a;
    size_t size_link_list_b;

    {
        ReadTransaction wt{db_a1};
        ConstTableRef table = wt.get_table("class_source");
        auto ll = table->get_linklist(1, 0);
        size_link_list_a = ll->size();
    }

    {
        ReadTransaction wt{db_b1};
        ConstTableRef table = wt.get_table("class_source");
        auto ll = table->get_linklist(1, 0);
        size_link_list_b = ll->size();
        CHECK_EQUAL(ll->size(), 1);
    }

    // The final link list has size 0 in part a and size 1 in part b.
    // These checks confirm that the OT system behaves as expected.
    // The expected behavior is higher order divergence.
    CHECK_EQUAL(size_link_list_a, 0);
    CHECK_EQUAL(size_link_list_b, 1);
    CHECK_NOT_EQUAL(size_link_list_a, size_link_list_b);
}
#endif // 0


TEST(Sync_Randomized)
{
    constexpr size_t num_clients = 7;

    auto client_test_program = [](DBRef db, Session& session) {
        // Create the schema
        write_transaction_notifying_session(db, session, [](WriteTransaction& wt) {
            if (wt.has_table("class_foo"))
                return;
            TableRef table = wt.add_table("class_foo");
            table->add_column(type_Int, "i");
            table->create_object();
        });

        Random random(random_int<unsigned long>()); // Seed from slow global generator
        for (int i = 0; i < 100; ++i) {
            WriteTransaction wt(db);
            if (random.chance(4, 5)) {
                TableRef table = wt.get_table("class_foo");
                if (random.chance(1, 5)) {
                    table->create_object();
                }
                int value = random.draw_int(-32767, 32767);
                size_t row_ndx = random.draw_int_mod(table->size());
                table->get_object(row_ndx).set("i", value);
            }
            version_type new_version = wt.commit();
            session.nonsync_transact_notify(new_version);
        }
    };

    TEST_DIR(dir);
    MultiClientServerFixture fixture(num_clients, 1, dir, test_context);
    fixture.start();

    std::unique_ptr<DBTestPathGuard> client_path_guards[num_clients];
    DBRef client_shared_groups[num_clients];
    for (size_t i = 0; i < num_clients; ++i) {
        std::string suffix = util::format(".client_%1.realm", i);
        std::string test_path = get_test_path(test_context.get_test_name(), suffix);
        client_path_guards[i].reset(new DBTestPathGuard(test_path));
        client_shared_groups[i] = DB::create(make_client_replication(), test_path);
    }

    std::unique_ptr<Session> sessions[num_clients];
    for (size_t i = 0; i < num_clients; ++i) {
        auto db = client_shared_groups[i];
        sessions[i].reset(new Session(fixture.make_session(int(i), db)));
        fixture.bind_session(*sessions[i], 0, "/test");
    }

    auto run_client_test_program = [&](size_t i) {
        try {
            client_test_program(client_shared_groups[i], *sessions[i]);
        }
        catch (...) {
            fixture.stop();
            throw;
        }
    };

    ThreadWrapper client_program_threads[num_clients];
    for (size_t i = 0; i < num_clients; ++i)
        client_program_threads[i].start([=] {
            run_client_test_program(i);
        });

    for (size_t i = 0; i < num_clients; ++i)
        CHECK(!client_program_threads[i].join());

    log("All client programs completed");

    // Wait until all local changes are uploaded, and acknowledged by the
    // server.
    for (size_t i = 0; i < num_clients; ++i)
        sessions[i]->wait_for_upload_complete_or_client_stopped();

    log("Everything uploaded");

    // Now wait for all previously uploaded changes to be downloaded by all
    // others.
    for (size_t i = 0; i < num_clients; ++i)
        sessions[i]->wait_for_download_complete_or_client_stopped();

    log("Everything downloaded");

    REALM_ASSERT(num_clients > 0);
    ReadTransaction rt_0(client_shared_groups[0]);
    rt_0.get_group().verify();
    for (size_t i = 1; i < num_clients; ++i) {
        ReadTransaction rt(client_shared_groups[i]);
        rt.get_group().verify();
        CHECK(compare_groups(rt_0, rt));
    }
}


#ifdef REALM_DEBUG // Failure simulation only works in debug mode

TEST(Sync_ReadFailureSimulation)
{
    TEST_DIR(server_dir);
    TEST_CLIENT_DB(db);

    // Check that read failure simulation works on the client-side
    {
        bool client_side_read_did_fail = false;
        {
            ClientServerFixture fixture(server_dir, test_context);
            fixture.set_client_side_error_rate(1, 1); // 100% chance of failure
            auto error_handler = [&](std::error_code ec, bool is_fatal, const std::string&) {
                CHECK_EQUAL(_impl::SimulatedFailure::sync_client__read_head, ec);
                CHECK_NOT(is_fatal);
                fixture.stop();
                client_side_read_did_fail = true;
            };
            fixture.set_client_side_error_handler(error_handler);
            Session session = fixture.make_bound_session(db, "/test");
            fixture.start();
            session.wait_for_download_complete_or_client_stopped();
        }
        CHECK(client_side_read_did_fail);
    }

    // FIXME: Figure out a way to check that read failure simulation works on
    // the server-side
}

#endif // REALM_DEBUG


TEST(Sync_FailingReadsOnClientSide)
{
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    {
        TEST_DIR(dir);
        ClientServerFixture fixture{dir, test_context};
        fixture.set_client_side_error_rate(5, 100); // 5% chance of failure
        auto error_handler = [&](std::error_code ec, bool, const std::string&) {
            if (CHECK_EQUAL(_impl::SimulatedFailure::sync_client__read_head, ec))
                fixture.cancel_reconnect_delay();
        };
        fixture.set_client_side_error_handler(error_handler);
        fixture.start();

        Session session_1 = fixture.make_bound_session(db_1);

        Session session_2 = fixture.make_bound_session(db_2);

        write_transaction_notifying_session(db_1, session_1, [](WriteTransaction& wt) {
            TableRef table = wt.add_table("class_foo");
            table->add_column(type_Int, "i");
            table->create_object();
        });
        write_transaction_notifying_session(db_2, session_2, [](WriteTransaction& wt) {
            TableRef table = wt.add_table("class_bar");
            table->add_column(type_Int, "i");
            table->create_object();
        });
        for (int i = 0; i < 100; ++i) {
            session_1.wait_for_upload_complete_or_client_stopped();
            session_2.wait_for_upload_complete_or_client_stopped();
            for (int i = 0; i < 10; ++i) {
                write_transaction_notifying_session(db_1, session_1, [=](WriteTransaction& wt) {
                    TableRef table = wt.get_table("class_foo");
                    table->begin()->set("i", i);
                });
                write_transaction_notifying_session(db_2, session_2, [=](WriteTransaction& wt) {
                    TableRef table = wt.get_table("class_bar");
                    table->begin()->set("i", i);
                });
            }
        }
        session_1.wait_for_upload_complete_or_client_stopped();
        session_2.wait_for_upload_complete_or_client_stopped();
        session_1.wait_for_download_complete_or_client_stopped();
        session_2.wait_for_download_complete_or_client_stopped();
    }

    ReadTransaction rt_1(db_1);
    ReadTransaction rt_2(db_2);
    const Group& group_1 = rt_1;
    const Group& group_2 = rt_2;
    group_1.verify();
    group_2.verify();
    CHECK(compare_groups(rt_1, rt_2));
}


TEST(Sync_FailingReadsOnServerSide)
{
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    {
        TEST_DIR(dir);
        ClientServerFixture fixture{dir, test_context};
        fixture.set_server_side_error_rate(5, 100); // 5% chance of failure
        auto error_handler = [&](std::error_code, bool is_fatal, const std::string&) {
            CHECK_NOT(is_fatal);
            fixture.cancel_reconnect_delay();
        };
        fixture.set_client_side_error_handler(error_handler);
        fixture.start();

        Session session_1 = fixture.make_bound_session(db_1);

        Session session_2 = fixture.make_bound_session(db_2);

        write_transaction_notifying_session(db_1, session_1, [](WriteTransaction& wt) {
            TableRef table = wt.add_table("class_foo");
            table->add_column(type_Int, "i");
            table->create_object();
        });
        write_transaction_notifying_session(db_2, session_2, [](WriteTransaction& wt) {
            TableRef table = wt.add_table("class_bar");
            table->add_column(type_Int, "i");
            table->create_object();
        });
        for (int i = 0; i < 100; ++i) {
            session_1.wait_for_upload_complete_or_client_stopped();
            session_2.wait_for_upload_complete_or_client_stopped();
            for (int i = 0; i < 10; ++i) {
                write_transaction_notifying_session(db_1, session_1, [=](WriteTransaction& wt) {
                    TableRef table = wt.get_table("class_foo");
                    table->begin()->set("i", i);
                });
                write_transaction_notifying_session(db_2, session_2, [=](WriteTransaction& wt) {
                    TableRef table = wt.get_table("class_bar");
                    table->begin()->set("i", i);
                });
            }
        }
        session_1.wait_for_upload_complete_or_client_stopped();
        session_2.wait_for_upload_complete_or_client_stopped();
        session_1.wait_for_download_complete_or_client_stopped();
        session_2.wait_for_download_complete_or_client_stopped();
    }

    ReadTransaction rt_1(db_1);
    ReadTransaction rt_2(db_2);
    const Group& group_1 = rt_1;
    const Group& group_2 = rt_2;
    group_1.verify();
    group_2.verify();
    CHECK(compare_groups(rt_1, rt_2));
}


TEST(Sync_ErrorAfterServerRestore_BadClientFileIdent)
{
    TEST_DIR(server_dir);
    TEST_CLIENT_DB(db);

    std::string server_path = "/test";
    std::string server_realm_path;

    // Make a change and synchronize with server
    {
        ClientServerFixture fixture(server_dir, test_context);
        server_realm_path = fixture.map_virtual_to_real_path(server_path);
        Session session = fixture.make_bound_session(db, server_path);
        WriteTransaction wt{db};
        wt.add_table("class_table");
        auto new_version = wt.commit();
        session.nonsync_transact_notify(new_version);
        fixture.start();
        session.wait_for_upload_complete_or_client_stopped();
    }

    // Emulate a server-side restore to before the creation of the Realm
    util::File::remove(server_realm_path);

    // Provoke error by attempting to resynchronize
    bool did_fail = false;
    {
        ClientServerFixture fixture(server_dir, test_context);
        auto error_handler = [&](std::error_code ec, bool is_fatal, const std::string&) {
            CHECK_EQUAL(ProtocolError::bad_server_version, ec);
            CHECK(is_fatal);
            did_fail = true;
            fixture.stop();
        };
        fixture.set_client_side_error_handler(error_handler);
        Session session = fixture.make_bound_session(db, server_path);
        fixture.start();
        session.wait_for_download_complete_or_client_stopped();
    }
    CHECK(did_fail);
}


TEST(Sync_HTTP404NotFound)
{
    TEST_DIR(server_dir);

    util::Logger& logger = test_context.logger;
    util::PrefixLogger server_logger("Server: ", logger);
    std::string server_address = "localhost";

    Server::Config server_config;
    server_config.logger = &server_logger;
    server_config.listen_address = server_address;
    server_config.listen_port = "";
    server_config.tcp_no_delay = true;

    util::Optional<PKey> public_key = PKey::load_public(g_test_server_key_path);
    Server server(server_dir, std::move(public_key), server_config);
    server.start();
    util::network::Endpoint endpoint = server.listen_endpoint();

    ThreadWrapper server_thread;
    server_thread.start([&] {
        server.run();
    });

    util::HTTPRequest request;
    request.path = "/not-found";

    HTTPRequestClient client(logger, endpoint, request);
    client.fetch_response();

    server.stop();

    server_thread.join();

    const util::HTTPResponse& response = client.get_response();

    CHECK(response.status == util::HTTPStatus::NotFound);
    CHECK(response.headers.find("Server")->second == "RealmSync/" REALM_VERSION_STRING);
}


namespace {

class RequestWithContentLength {
public:
    RequestWithContentLength(test_util::unit_test::TestContext& test_context, util::network::Service& service,
                             const util::network::Endpoint& endpoint, const std::string& content_length,
                             const std::string& expected_response_line)
        : test_context{test_context}
        , m_socket{service}
        , m_endpoint{endpoint}
        , m_content_length{content_length}
        , m_expected_response_line{expected_response_line}
    {
        m_request = "POST /does-not-exist-1234 HTTP/1.1\r\n"
                    "Content-Length: " +
                    m_content_length +
                    "\r\n"
                    "\r\n";
    }

    void write_completion_handler(std::error_code ec, size_t nbytes)
    {
        CHECK_NOT(ec);
        CHECK_EQUAL(m_request.size(), nbytes);
        auto handler = [&](std::error_code ec, size_t nbytes) {
            this->read_completion_handler(ec, nbytes);
        };
        m_socket.async_read_until(m_buffer, m_buf_size, '\n', m_read_ahead_buffer, handler);
    }

    void read_completion_handler(std::error_code ec, size_t nbytes)
    {
        CHECK_NOT(ec);
        std::string response_line{m_buffer, nbytes};
        CHECK_EQUAL(response_line, m_expected_response_line);
    }

    void start()
    {
        std::error_code ec;
        m_socket.connect(m_endpoint, ec);
        CHECK_NOT(ec);

        auto handler = [&](std::error_code ec, size_t nbytes) {
            this->write_completion_handler(ec, nbytes);
        };
        m_socket.async_write(m_request.data(), m_request.size(), handler);
    }

private:
    test_util::unit_test::TestContext& test_context;
    util::network::Socket m_socket;
    util::network::ReadAheadBuffer m_read_ahead_buffer;
    static constexpr size_t m_buf_size = 1000;
    char m_buffer[m_buf_size];
    const util::network::Endpoint& m_endpoint;
    const std::string m_content_length;
    std::string m_request;
    const std::string m_expected_response_line;
};

} // namespace

// Test the server's HTTP response to a Content-Length header of zero, empty,
// and a non-number string.
TEST(Sync_HTTP_ContentLength)
{
    TEST_DIR(server_dir);

    util::Logger& logger = test_context.logger;
    util::PrefixLogger server_logger("Server: ", logger);
    std::string server_address = "localhost";

    Server::Config server_config;
    server_config.logger = &server_logger;
    server_config.listen_address = server_address;
    server_config.listen_port = "";
    server_config.tcp_no_delay = true;

    util::Optional<PKey> public_key = PKey::load_public(g_test_server_key_path);
    Server server(server_dir, std::move(public_key), server_config);
    server.start();
    util::network::Endpoint endpoint = server.listen_endpoint();

    ThreadWrapper server_thread;
    server_thread.start([&] {
        server.run();
    });

    util::network::Service service;

    RequestWithContentLength req_0(test_context, service, endpoint, "0", "HTTP/1.1 404 Not Found\r\n");

    RequestWithContentLength req_1(test_context, service, endpoint, "", "HTTP/1.1 404 Not Found\r\n");

    RequestWithContentLength req_2(test_context, service, endpoint, "abc", "HTTP/1.1 400 Bad Request\r\n");

    RequestWithContentLength req_3(test_context, service, endpoint, "5abc", "HTTP/1.1 400 Bad Request\r\n");

    req_0.start();
    req_1.start();
    req_2.start();
    req_3.start();

    service.run();

    server.stop();
    server_thread.join();
}


// The Sync_HttpApiOk sends a HTTP request to a running sync server with url
// prefix /api/ and checks the various api endpoints.
TEST(Sync_HttpApi)
{
    TEST_DIR(server_dir);
    util::Logger& logger = test_context.logger;
    util::PrefixLogger server_logger("Server: ", logger);
    std::string server_address = "localhost";

    Server::Config server_config;
    server_config.logger = &server_logger;
    server_config.listen_address = server_address;
    server_config.listen_port = "";
    server_config.tcp_no_delay = true;

    util::Optional<PKey> public_key = PKey::load_public(g_test_server_key_path);
    Server server(server_dir, std::move(public_key), server_config);
    server.start();

    ThreadWrapper server_thread;
    server_thread.start([&] {
        server.run();
    });

    const util::network::Endpoint& endpoint = server.listen_endpoint();

    // url = /api/ok
    {
        util::HTTPRequest request;
        request.method = util::HTTPMethod::Get;
        request.path = "/api/ok";
        HTTPRequestClient client(logger, endpoint, request);
        client.fetch_response();
        const util::HTTPResponse& response = client.get_response();
        CHECK_EQUAL(response.status, util::HTTPStatus::Ok);
        CHECK(!response.body);
    }

    // url = /api/x
    {
        util::HTTPRequest request;
        request.method = util::HTTPMethod::Get;
        request.path = "/api/x";
        HTTPRequestClient client(logger, endpoint, request);
        client.fetch_response();
        const util::HTTPResponse& response = client.get_response();
        CHECK_EQUAL(response.status, util::HTTPStatus::Forbidden);
        CHECK_EQUAL(response.body, "no access token");
    }

    // url = /api/x with admin access token
    {
        util::HTTPRequest request;
        request.method = util::HTTPMethod::Get;
        request.path = "/api/x";
        request.headers["Authorization"] = _impl::make_authorization_header(g_signed_test_user_token);
        HTTPRequestClient client(logger, endpoint, request);
        client.fetch_response();
        const util::HTTPResponse& response = client.get_response();
        CHECK_EQUAL(response.status, util::HTTPStatus::NotFound);
    }

    // url = /api/info with admin access token
    {
        util::HTTPRequest request;
        request.method = util::HTTPMethod::Get;
        request.path = "/api/info";
        request.headers["Authorization"] = _impl::make_authorization_header(g_signed_test_user_token);
        HTTPRequestClient client(logger, endpoint, request);
        client.fetch_response();
        const util::HTTPResponse& response = client.get_response();
        CHECK_EQUAL(response.status, util::HTTPStatus::Ok);
        CHECK(response.body);
        const char* prefix = "Realm sync server\n\n";
        size_t prefix_len = strlen(prefix);
        CHECK(response.body->length() >= prefix_len && response.body->substr(0, prefix_len) == prefix);
    }

    // url = /api/info with non-admin access token
    {
        util::HTTPRequest request;
        request.method = util::HTTPMethod::Get;
        request.path = "/api/info";
        request.headers["Authorization"] = _impl::make_authorization_header(g_user_0_token);
        HTTPRequestClient client(logger, endpoint, request);
        client.fetch_response();
        const util::HTTPResponse& response = client.get_response();
        CHECK_EQUAL(response.status, util::HTTPStatus::Forbidden);
        CHECK_EQUAL(response.body, "must be admin");
    }

    server.stop();
    server_thread.join();
}


// This test checks that a custom authorization header name
// can be set in the sync server config.
TEST(Sync_HttpApiWithCustomAuthorizationHeaderName)
{
    TEST_DIR(server_dir);
    util::Logger& logger = test_context.logger;
    util::PrefixLogger server_logger("Server: ", logger);
    std::string server_address = "localhost";

    Server::Config server_config;
    server_config.logger = &server_logger;
    server_config.listen_address = server_address;
    server_config.listen_port = "";
    server_config.tcp_no_delay = true;
    server_config.authorization_header_name = "X-Alternative-Name";

    util::Optional<PKey> public_key = PKey::load_public(g_test_server_key_path);
    Server server(server_dir, std::move(public_key), server_config);
    server.start();

    ThreadWrapper server_thread;
    server_thread.start([&] {
        server.run();
    });

    const util::network::Endpoint& endpoint = server.listen_endpoint();

    // Correct authorization header.
    {
        util::HTTPRequest request;
        request.method = util::HTTPMethod::Get;
        request.path = "/api/info";
        request.headers["X-Alternative-Name"] = _impl::make_authorization_header(g_signed_test_user_token);
        HTTPRequestClient client(logger, endpoint, request);
        client.fetch_response();
        const util::HTTPResponse& response = client.get_response();
        CHECK_EQUAL(response.status, util::HTTPStatus::Ok);
        CHECK(response.body);
        const char* prefix = "Realm sync server\n\n";
        size_t prefix_len = strlen(prefix);
        CHECK(response.body->length() >= prefix_len && response.body->substr(0, prefix_len) == prefix);
    }

    // Incorrect authorization header.
    {
        util::HTTPRequest request;
        request.method = util::HTTPMethod::Get;
        request.path = "/api/info";
        request.headers["Authorization"] = _impl::make_authorization_header(g_signed_test_user_token);
        HTTPRequestClient client(logger, endpoint, request);
        client.fetch_response();
        const util::HTTPResponse& response = client.get_response();
        CHECK_EQUAL(response.status, util::HTTPStatus::Forbidden);
        CHECK_EQUAL(response.body, "no access token");
    }

    server.stop();
    server_thread.join();
}


#if 0
// FIXME: This test does not pass always - CHECK_LESS(size_after_1, size_before_1) fails sometimes.
//        Is this test still relevant?
// This test creates a sync server and a sync client. The sync client uploads
// data to two Realms.
//
// The sizes of the Realms are found.  A HTTP request for "/api/compact" is
// sent to the server. It is checked that the Realms are smaller.
//
// Another client is made that downloads the data through full sync and it is
// verified that it ends up with the same data as the uploading client.
//
// This cycle is repeated: Create more data, compact, check sizes, verify
// correctness.
TEST(Sync_HttpApiCompact)
{
    TEST_DIR(server_dir);
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    ClientServerFixture fixture(server_dir, test_context);
    fixture.start();

    Session session_1 = fixture.make_bound_session(db_1, "/db_1");
    std::unique_ptr<Replication> history_1 = make_client_replication();
    auto db_1 = DB::create(*history_1, path_1);

    Session session_2 = fixture.make_bound_session(db_2, "/db_2");
    std::unique_ptr<Replication> history_2 = make_client_replication();
    auto db_2 = DB::create(*history_2, path_2);

    auto create_schema = [](Session& sess, DBRef db) {
        WriteTransaction wt(db);
        TableRef table = wt.get_group().add_table_with_primary_key("class_items", type_String, "a");
        table->add_column(type_Int, "i");
        version_type new_version = wt.commit();
        sess.nonsync_transact_notify(new_version);
    };
    create_schema(session_1, db_1);
    create_schema(session_2, db_2);

    auto insert_objects = [](WriteTransaction& wt, size_t counter, int number_of_objects) {
        TableRef table = wt.get_table("class_items");

        for (int i = 0; i < number_of_objects; ++i) {
            std::string pk_str = std::to_string(counter) + "_" + std::to_string(i);
            StringData pk{pk_str};
            Obj obj = table->create_object_with_primary_key(pk);
            obj.set(table->get_column_key("i"), i);
        }
    };

    size_t counter = 0;

    for (size_t i = 0; i < 1; ++i) {
        WriteTransaction wt{db_1};
        insert_objects(wt, counter, 400);
        version_type new_version = wt.commit();
        session_1.nonsync_transact_notify(new_version);
        ++counter;
    }

    for (size_t i = 0; i < 5; ++i) {
        WriteTransaction wt{db_2};
        insert_objects(wt, counter, 300);
        version_type new_version = wt.commit();
        session_2.nonsync_transact_notify(new_version);
        ++counter;
    }

    session_1.wait_for_upload_complete_or_client_stopped();
    session_2.wait_for_upload_complete_or_client_stopped();

    std::string server_realm_file_1 = fixture.map_virtual_to_real_path("/db_1");
    std::string server_realm_file_2 = fixture.map_virtual_to_real_path("/db_2");
    CHECK(util::File::exists(server_realm_file_1));
    CHECK(util::File::exists(server_realm_file_2));

    size_t size_before_1 = util::File{server_realm_file_1}.get_size();
    size_t size_before_2 = util::File{server_realm_file_2}.get_size();

    // Send a HTTP request to the server to compact all Realms.
    CHECK_EQUAL(util::HTTPStatus::Ok, fixture.send_http_compact_request());

    size_t size_after_1 = util::File{server_realm_file_1}.get_size();
    size_t size_after_2 = util::File{server_realm_file_2}.get_size();

    CHECK_LESS(size_after_1, size_before_1);
    CHECK_LESS(size_after_2, size_before_2);

    auto check_groups = [&](DBRef db_external, const std::string& server_path) {
        TEST_CLIENT_DB(db);
        Session session = fixture.make_bound_session(db, server_path);
        session.wait_for_download_complete_or_client_stopped();

        auto db = DB::create(make_client_replication(), path);
        ReadTransaction rt_1(db);
        ReadTransaction rt_2(db_external);
        CHECK(compare_groups(rt_1, rt_2));
        session.detach();
        fixture.wait_for_session_terminations_or_client_stopped();
    };

    check_groups(db_1, "/db_1");
    check_groups(db_2, "/db_2");

    // First cycle is complete. Repeat. The amount of data is slightly
    // changes.

    for (size_t i = 0; i < 2; ++i) {
        WriteTransaction wt{db_1};
        insert_objects(wt, counter, 700);
        version_type new_version = wt.commit();
        session_1.nonsync_transact_notify(new_version);
        ++counter;
    }

    for (size_t i = 0; i < 5; ++i) {
        WriteTransaction wt{db_2};
        insert_objects(wt, counter, 300);
        version_type new_version = wt.commit();
        session_2.nonsync_transact_notify(new_version);
        ++counter;
    }

    session_1.wait_for_upload_complete_or_client_stopped();
    session_2.wait_for_upload_complete_or_client_stopped();

    size_before_1 = util::File{server_realm_file_1}.get_size();
    size_before_2 = util::File{server_realm_file_2}.get_size();

    // Send a HTTP request to the server to compact all Realms.
    CHECK_EQUAL(util::HTTPStatus::Ok, fixture.send_http_compact_request());

    size_after_1 = util::File{server_realm_file_1}.get_size();
    size_after_2 = util::File{server_realm_file_2}.get_size();

    CHECK_LESS(size_after_1, size_before_1);
    CHECK_LESS(size_after_2, size_before_2);

    check_groups(db_1, "/db_1");
    check_groups(db_2, "/db_2");
}

#endif // _WIN32


// Sync_RealmDeletion creates a client realm, uploads a changeset,
// exercises the Realm deletion HTTP request, and verifies that
// the Realm (including .lock and .management) is gone and that
// the session has been disabled.
// The test also verifies that the Realm isn't deleted if the
// request lacks proper Authorization.
void test_realm_deletion(unit_test::TestContext& test_context, bool disable_state_realms)
{
    REALM_ASSERT(disable_state_realms);

    TEST_DIR(server_dir);
    TEST_CLIENT_DB(db);

    {
        WriteTransaction wt{db};
        wt.add_table("class_table-1");
        wt.commit();
    }

    ClientServerFixture::Config config;
    ClientServerFixture fixture(server_dir, test_context, config);

    std::string server_path = "/test";
    std::string server_realm_file = fixture.map_virtual_to_real_path(server_path);
    std::string server_realm_file_lock = server_realm_file + ".lock";
    std::string server_realm_file_management = server_realm_file + ".management";

    bool session_is_disabled = false;

    auto error_handler = [&](std::error_code ec, bool, const std::string&) {
        CHECK_EQUAL(ProtocolError::server_file_deleted, ec);
        session_is_disabled = true;
        fixture.stop();
    };

    fixture.set_client_side_error_handler(error_handler);
    Session session = fixture.make_bound_session(db, server_path);
    fixture.start();
    session.wait_for_upload_complete_or_client_stopped();

    CHECK(util::File::exists(server_realm_file));
    CHECK(util::File::exists(server_realm_file_lock));
    CHECK(util::File::exists(server_realm_file_management));

    // Send a HTTP request to delete the Realm without Authorization
    CHECK_EQUAL(util::HTTPStatus::Forbidden, fixture.send_http_delete_request(server_path, ""));

    // The server realm is still there
    CHECK(util::File::exists(server_realm_file));
    CHECK(util::File::exists(server_realm_file_lock));
    CHECK(util::File::exists(server_realm_file_management));

    // Send a HTTP request to delete the Realm without Authorization
    CHECK_EQUAL(util::HTTPStatus::Forbidden, fixture.send_http_delete_request(server_path, ""));

    // The server realm is still there
    CHECK(util::File::exists(server_realm_file));
    CHECK(util::File::exists(server_realm_file_lock));
    CHECK(util::File::exists(server_realm_file_management));

    // Send a HTTP request to delete the Realm with Authorization
    // for another Realm.
    CHECK_EQUAL(util::HTTPStatus::Forbidden,
                fixture.send_http_delete_request(server_path, g_signed_test_user_token_for_path));

    // The server realm is still there
    CHECK(util::File::exists(server_realm_file));
    CHECK(util::File::exists(server_realm_file_lock));
    CHECK(util::File::exists(server_realm_file_management));

    // Send a HTTP request to delete the Realm with admin Authorization
    CHECK_EQUAL(util::HTTPStatus::Ok, fixture.send_http_delete_request(server_path));

    // The realm is deleted
    CHECK(!util::File::exists(server_realm_file));
    CHECK(!util::File::exists(server_realm_file_lock));
    CHECK(!util::File::exists(server_realm_file_management));

    write_transaction_notifying_session(db, session, [](WriteTransaction& wt) {
        wt.add_table("class_table-2");
    });

    session.wait_for_upload_complete_or_client_stopped();

    CHECK(session_is_disabled);
    CHECK(!util::File::exists(server_realm_file));
    CHECK(!util::File::exists(server_realm_file_lock));
    CHECK(!util::File::exists(server_realm_file_management));
}


TEST(Sync_RealmDeletionWhenStateRealmsDisabled)
{
    test_realm_deletion(test_context, true);
}


// Sync_RealmDeletionEmptyDir creates a client realm, uploads a changeset,
// exercises the Realm deletion HTTP request, and verifies that
// the Realm (including .lock and .management) and all directories
// made empty by removing the realm are removed as well.
TEST(Sync_RealmDeletionEmptyDir)
{
    TEST_DIR(server_dir);
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    ClientServerFixture fixture(server_dir, test_context);
    fixture.start();

    std::string server_path = "/u/project/task/test";
    std::string server_realm_file = fixture.map_virtual_to_real_path(server_path);
    std::string server_realm_file_lock = server_realm_file + ".lock";
    std::string server_realm_file_management = server_realm_file + ".management";
    std::string server_task_dir = util::parent_dir(server_realm_file);
    std::string server_project_dir = util::parent_dir(server_task_dir);
    std::string server_u_dir = util::parent_dir(server_project_dir);

    // Create the Realm at path = /u/project/task/test. This Realm will be deleted later.
    {
        {
            WriteTransaction wt{db_1};
            wt.add_table("class_table-1");
            wt.commit();
        }

        Session session = fixture.make_bound_session(db_1, server_path);
        session.wait_for_download_complete_or_client_stopped();
    }

    // Create another Realm at path = /u/test. This Realm will not be deleted.
    {
        {
            WriteTransaction wt{db_2};
            wt.add_table("class_table-1");
            wt.commit();
        }

        Session session = fixture.make_bound_session(db_2, "/u/test");
        session.wait_for_download_complete_or_client_stopped();
    }

    CHECK(util::File::exists(server_u_dir));
    CHECK(util::File::exists(server_project_dir));
    CHECK(util::File::exists(server_task_dir));
    CHECK(util::File::exists(server_realm_file));
    CHECK(util::File::exists(server_realm_file_lock));
    CHECK(util::File::exists(server_realm_file_management));

    // Send a HTTP request to delete the Realm with admin Authorization
    CHECK_EQUAL(util::HTTPStatus::Ok, fixture.send_http_delete_request(server_path));

    // server_u_dir should still exist.
    CHECK(util::File::exists(server_u_dir));

    // Check that the realm and the empty parent directories are deleted
    CHECK(!util::File::exists(server_project_dir));
    CHECK(!util::File::exists(server_task_dir));
    CHECK(!util::File::exists(server_realm_file));
    CHECK(!util::File::exists(server_realm_file_lock));
    CHECK(!util::File::exists(server_realm_file_management));
}


TEST(Sync_ErrorAfterServerRestore_BadServerVersion)
{
    TEST_DIR(server_dir);
    TEST_DIR(backup_dir);
    TEST_CLIENT_DB(db);

    std::string server_path = "/test";
    std::string server_realm_path;
    std::string backup_realm_path = util::File::resolve("test.realm", backup_dir);

    // Create schema and synchronize with server
    {
        ClientServerFixture fixture(server_dir, test_context);
        server_realm_path = fixture.map_virtual_to_real_path(server_path);
        Session session = fixture.make_bound_session(db, server_path);
        WriteTransaction wt{db};
        TableRef table = wt.add_table("class_table");
        table->add_column(type_Int, "column");
        auto new_version = wt.commit();
        session.nonsync_transact_notify(new_version);
        fixture.start();
        session.wait_for_upload_complete_or_client_stopped();
    }

    // Save a snapshot of the server-side Realm file
    util::File::copy(server_realm_path, backup_realm_path);

    // Make change in which will be lost when restoring snapshot
    {
        ClientServerFixture fixture(server_dir, test_context);
        Session session = fixture.make_bound_session(db, server_path);
        WriteTransaction wt{db};
        TableRef table = wt.get_table("class_table");
        table->create_object();
        auto new_version = wt.commit();
        session.nonsync_transact_notify(new_version);
        fixture.start();
        session.wait_for_upload_complete_or_client_stopped();
    }

    // Restore the snapshot
    util::File::copy(backup_realm_path, server_realm_path);

    // Provoke error by resynchronizing
    bool did_fail = false;
    {
        ClientServerFixture fixture(server_dir, test_context);
        auto error_handler = [&](std::error_code ec, bool is_fatal, const std::string&) {
            CHECK_EQUAL(ProtocolError::bad_server_version, ec);
            CHECK(is_fatal);
            did_fail = true;
            fixture.stop();
        };
        fixture.set_client_side_error_handler(error_handler);
        Session session = fixture.make_bound_session(db, server_path);
        fixture.start();
        session.wait_for_download_complete_or_client_stopped();
    }
    CHECK(did_fail);
}


TEST(Sync_ErrorAfterServerRestore_BadClientVersion)
{
    TEST_DIR(server_dir);
    TEST_DIR(backup_dir);
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    std::string server_path = "/test";
    std::string server_realm_path;
    std::string backup_realm_path = util::File::resolve("test.realm", backup_dir);

    // Create schema and synchronize client files
    {
        ClientServerFixture fixture(server_dir, test_context);
        server_realm_path = fixture.map_virtual_to_real_path(server_path);
        Session session_1 = fixture.make_bound_session(db_1, server_path);
        Session session_2 = fixture.make_bound_session(db_2, server_path);
        WriteTransaction wt{db_1};
        TableRef table = wt.add_table("class_table");
        table->add_column(type_Int, "column");
        auto new_version = wt.commit();
        session_1.nonsync_transact_notify(new_version);
        fixture.start();
        session_1.wait_for_upload_complete_or_client_stopped();
        session_2.wait_for_download_complete_or_client_stopped();
    }

    // Save a snapshot of the server-side Realm file
    util::File::copy(server_realm_path, backup_realm_path);

    // Make change in 1st file which will be lost when restoring snapshot
    {
        ClientServerFixture fixture(server_dir, test_context);
        Session session = fixture.make_bound_session(db_1, server_path);
        WriteTransaction wt{db_1};
        TableRef table = wt.get_table("class_table");
        table->create_object();
        auto new_version = wt.commit();
        session.nonsync_transact_notify(new_version);
        fixture.start();
        session.wait_for_upload_complete_or_client_stopped();
    }

    // Restore the snapshot
    util::File::copy(backup_realm_path, server_realm_path);

    // Make a conflicting change in 2nd file relative to reverted server state
    {
        ClientServerFixture fixture(server_dir, test_context);
        Session session = fixture.make_bound_session(db_2, server_path);
        WriteTransaction wt{db_2};
        TableRef table = wt.get_table("class_table");
        table->create_object();
        auto new_version = wt.commit();
        session.nonsync_transact_notify(new_version);
        fixture.start();
        session.wait_for_upload_complete_or_client_stopped();
    }

    // Provoke error by synchronizing 1st file
    bool did_fail = false;
    {
        ClientServerFixture fixture(server_dir, test_context);
        auto error_handler = [&](std::error_code ec, bool is_fatal, const std::string&) {
            CHECK_EQUAL(ProtocolError::bad_client_version, ec);
            CHECK(is_fatal);
            did_fail = true;
            fixture.stop();
        };
        fixture.set_client_side_error_handler(error_handler);
        Session session = fixture.make_bound_session(db_1, server_path);
        fixture.start();
        session.wait_for_download_complete_or_client_stopped();
    }
    CHECK(did_fail);
}


TEST(Sync_ErrorAfterServerRestore_BadClientFileIdentSalt)
{
    TEST_DIR(server_dir);
    TEST_DIR(backup_dir);
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);
    TEST_CLIENT_DB(db_3);

    std::string server_path = "/test";
    std::string server_realm_path;
    std::string backup_realm_path = util::File::resolve("test.realm", backup_dir);

    // Register 1st file with server
    {
        ClientServerFixture fixture(server_dir, test_context);
        server_realm_path = fixture.map_virtual_to_real_path(server_path);
        Session session = fixture.make_bound_session(db_1, server_path);
        WriteTransaction wt{db_1};
        TableRef table = wt.add_table("class_table_1");
        table->add_column(type_Int, "column");
        auto new_version = wt.commit();
        session.nonsync_transact_notify(new_version);
        fixture.start();
        session.wait_for_upload_complete_or_client_stopped();
    }

    // Save a snapshot of the server-side Realm file
    util::File::copy(server_realm_path, backup_realm_path);

    // Register 2nd file with server
    {
        ClientServerFixture fixture(server_dir, test_context);
        Session session = fixture.make_bound_session(db_2, server_path);
        fixture.start();
        session.wait_for_download_complete_or_client_stopped();
    }

    // Restore the snapshot
    util::File::copy(backup_realm_path, server_realm_path);

    // Register 3rd conflicting file with server
    {
        ClientServerFixture fixture(server_dir, test_context);
        Session session = fixture.make_bound_session(db_3, server_path);
        fixture.start();
        session.wait_for_download_complete_or_client_stopped();
    }

    // Provoke error by resynchronizing 2nd file
    bool did_fail = false;
    {
        ClientServerFixture fixture(server_dir, test_context);
        auto error_handler = [&](std::error_code ec, bool is_fatal, const std::string&) {
            CHECK_EQUAL(ProtocolError::diverging_histories, ec);
            CHECK(is_fatal);
            did_fail = true;
            fixture.stop();
        };
        fixture.set_client_side_error_handler(error_handler);
        Session session = fixture.make_bound_session(db_2, server_path);
        fixture.start();
        session.wait_for_download_complete_or_client_stopped();
    }
    CHECK(did_fail);
}


TEST(Sync_ErrorAfterServerRestore_BadServerVersionSalt)
{
    TEST_DIR(server_dir);
    TEST_DIR(backup_dir);
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);
    TEST_CLIENT_DB(db_3);

    std::string server_path = "/test";
    std::string server_realm_path;
    std::string backup_realm_path = util::File::resolve("test.realm", backup_dir);

    // Create schema and synchronize client files
    {
        ClientServerFixture fixture(server_dir, test_context);
        server_realm_path = fixture.map_virtual_to_real_path(server_path);
        Session session_1 = fixture.make_bound_session(db_1, server_path);
        Session session_2 = fixture.make_bound_session(db_2, server_path);
        Session session_3 = fixture.make_bound_session(db_3, server_path);
        WriteTransaction wt{db_1};
        TableRef table = wt.add_table("class_table");
        table->add_column(type_Int, "column");
        auto new_version = wt.commit();
        session_1.nonsync_transact_notify(new_version);
        fixture.start();
        session_1.wait_for_upload_complete_or_client_stopped();
        session_2.wait_for_download_complete_or_client_stopped();
        session_3.wait_for_download_complete_or_client_stopped();
    }

    // Save a snapshot of the server-side Realm file
    util::File::copy(server_realm_path, backup_realm_path);

    // Make change in 1st file which will be lost when restoring snapshot, and
    // make 2nd file download it.
    {
        ClientServerFixture fixture(server_dir, test_context);
        Session session_1 = fixture.make_bound_session(db_1, server_path);
        Session session_2 = fixture.make_bound_session(db_2, server_path);
        WriteTransaction wt{db_1};
        TableRef table = wt.get_table("class_table");
        table->create_object();
        auto new_version = wt.commit();
        session_1.nonsync_transact_notify(new_version);
        fixture.start();
        session_1.wait_for_upload_complete_or_client_stopped();
        session_2.wait_for_download_complete_or_client_stopped();
    }

    // Restore the snapshot
    util::File::copy(backup_realm_path, server_realm_path);

    // Make a conflicting change in 3rd file relative to reverted server state
    {
        ClientServerFixture fixture(server_dir, test_context);
        Session session = fixture.make_bound_session(db_3, server_path);
        WriteTransaction wt{db_3};
        TableRef table = wt.get_table("class_table");
        table->create_object();
        auto new_version = wt.commit();
        session.nonsync_transact_notify(new_version);
        fixture.start();
        session.wait_for_upload_complete_or_client_stopped();
    }

    // Provoke error by synchronizing 2nd file
    bool did_fail = false;
    {
        ClientServerFixture fixture(server_dir, test_context);
        auto error_handler = [&](std::error_code ec, bool is_fatal, const std::string&) {
            CHECK_EQUAL(ProtocolError::diverging_histories, ec);
            CHECK(is_fatal);
            did_fail = true;
            fixture.stop();
        };
        fixture.set_client_side_error_handler(error_handler);
        Session session = fixture.make_bound_session(db_2, server_path);
        fixture.start();
        session.wait_for_download_complete_or_client_stopped();
    }
    CHECK(did_fail);
}


TEST(Sync_MultipleServers)
{
    // Check that a client can make lots of connection to lots of servers in a
    // concurrent manner.

    const int num_servers = 2;
    const int num_realms_per_server = 2;
    const int num_files_per_realm = 4;
    const int num_sessions_per_file = 8;
    const int num_transacts_per_session = 2;

    TEST_DIR(dir);
    int num_clients = 1;
    MultiClientServerFixture fixture(num_clients, num_servers, dir, test_context);
    fixture.start();

    TEST_DIR(dir_2);
    auto get_file_path = [&](int server_index, int realm_index, int file_index) {
        std::ostringstream out;
        out << server_index << "_" << realm_index << "_" << file_index << ".realm";
        return util::File::resolve(out.str(), dir_2);
    };

    auto run = [&](int server_index, int realm_index, int file_index) {
        try {
            std::string path = get_file_path(server_index, realm_index, file_index);
            DBRef db = DB::create(make_client_replication(), path);
            {
                WriteTransaction wt(db);
                TableRef table = wt.add_table("class_table");
                table->add_column(type_Int, "server_index");
                table->add_column(type_Int, "realm_index");
                table->add_column(type_Int, "file_index");
                table->add_column(type_Int, "session_index");
                table->add_column(type_Int, "transact_index");
                wt.commit();
            }
            std::string server_path = "/" + std::to_string(realm_index);
            for (int i = 0; i < num_sessions_per_file; ++i) {
                int client_index = 0;
                Session session = fixture.make_session(client_index, db);
                fixture.bind_session(session, server_index, server_path);
                for (int j = 0; j < num_transacts_per_session; ++j) {
                    WriteTransaction wt(db);
                    TableRef table = wt.get_table("class_table");
                    Obj obj = table->create_object();
                    obj.set("server_index", server_index);
                    obj.set("realm_index", realm_index);
                    obj.set("file_index", file_index);
                    obj.set("session_index", i);
                    obj.set("transact_index", j);
                    version_type new_version = wt.commit();
                    session.nonsync_transact_notify(new_version);
                }
                session.wait_for_upload_complete_or_client_stopped();
            }
        }
        catch (...) {
            fixture.stop();
            throw;
        }
    };

    auto finish_download = [&](int server_index, int realm_index, int file_index) {
        try {
            int client_index = 0;
            std::string path = get_file_path(server_index, realm_index, file_index);
            DBRef db = DB::create(make_client_replication(), path);
            std::string server_path = "/" + std::to_string(realm_index);
            Session session = fixture.make_session(client_index, db);
            fixture.bind_session(session, server_index, server_path);
            session.wait_for_download_complete_or_client_stopped();
        }
        catch (...) {
            fixture.stop();
            throw;
        }
    };

    // Make and upload changes
    {
        ThreadWrapper threads[num_servers][num_realms_per_server][num_files_per_realm];
        for (int i = 0; i < num_servers; ++i) {
            for (int j = 0; j < num_realms_per_server; ++j) {
                for (int k = 0; k < num_files_per_realm; ++k)
                    threads[i][j][k].start([=] {
                        run(i, j, k);
                    });
            }
        }
        for (size_t i = 0; i < num_servers; ++i) {
            for (size_t j = 0; j < num_realms_per_server; ++j) {
                for (size_t k = 0; k < num_files_per_realm; ++k)
                    CHECK_NOT(threads[i][j][k].join());
            }
        }
    }

    // Finish downloading
    {
        ThreadWrapper threads[num_servers][num_realms_per_server][num_files_per_realm];
        for (int i = 0; i < num_servers; ++i) {
            for (int j = 0; j < num_realms_per_server; ++j) {
                for (int k = 0; k < num_files_per_realm; ++k)
                    threads[i][j][k].start([=] {
                        finish_download(i, j, k);
                    });
            }
        }
        for (size_t i = 0; i < num_servers; ++i) {
            for (size_t j = 0; j < num_realms_per_server; ++j) {
                for (size_t k = 0; k < num_files_per_realm; ++k)
                    CHECK_NOT(threads[i][j][k].join());
            }
        }
    }

    // Check that all client side Realms have been correctly synchronized
    std::set<std::tuple<int, int, int>> expected_rows;
    for (int i = 0; i < num_files_per_realm; ++i) {
        for (int j = 0; j < num_sessions_per_file; ++j) {
            for (int k = 0; k < num_transacts_per_session; ++k)
                expected_rows.emplace(i, j, k);
        }
    }
    for (size_t i = 0; i < num_servers; ++i) {
        for (size_t j = 0; j < num_realms_per_server; ++j) {
            REALM_ASSERT(num_files_per_realm > 0);
            int file_index_0 = 0;
            std::string path_0 = get_file_path(int(i), int(j), file_index_0);
            std::unique_ptr<Replication> history_0 = make_client_replication();
            DBRef db_0 = DB::create(*history_0, path_0);
            ReadTransaction rt_0(db_0);
            {
                ConstTableRef table = rt_0.get_table("class_table");
                if (CHECK(table)) {
                    std::set<std::tuple<int, int, int>> rows;
                    for (const Obj& obj : *table) {
                        int server_index = int(obj.get<int64_t>("server_index"));
                        int realm_index = int(obj.get<int64_t>("realm_index"));
                        int file_index = int(obj.get<int64_t>("file_index"));
                        int session_index = int(obj.get<int64_t>("session_index"));
                        int transact_index = int(obj.get<int64_t>("transact_index"));
                        CHECK_EQUAL(i, server_index);
                        CHECK_EQUAL(j, realm_index);
                        rows.emplace(file_index, session_index, transact_index);
                    }
                    CHECK(rows == expected_rows);
                }
            }
            for (int k = 1; k < num_files_per_realm; ++k) {
                std::string path = get_file_path(int(i), int(j), k);
                DBRef db = DB::create(make_client_replication(), path);
                ReadTransaction rt(db);
                CHECK(compare_groups(rt_0, rt));
            }
        }
    }
}


TEST_IF(Sync_ReadOnlyClient, false)
{
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    TEST_DIR(server_dir);
    MultiClientServerFixture fixture(2, 1, server_dir, test_context);
    bool did_get_permission_denied = false;
    fixture.set_client_side_error_handler(1, [&](std::error_code ec, bool, const std::string&) {
        CHECK_EQUAL(ProtocolError::permission_denied, ec);
        did_get_permission_denied = true;
        fixture.get_client(1).stop();
    });
    fixture.start();

    // Write some stuff from the client that can upload
    {
        Session session_1 = fixture.make_bound_session(0, db_1, 0, "/test");
        WriteTransaction wt(db_1);
        auto table = wt.add_table("class_foo");
        table->add_column(type_Int, "i");
        table->create_object();
        table->begin()->set("i", 123);
        session_1.nonsync_transact_notify(wt.commit());
        session_1.wait_for_upload_complete_or_client_stopped();
    }

    // Check that the stuff was received on the read-only client
    {
        Session session_2 = fixture.make_bound_session(1, db_2, 0, "/test", g_signed_test_user_token_readonly);
        session_2.wait_for_download_complete_or_client_stopped();
        {
            ReadTransaction rt(db_2);
            auto table = rt.get_table("class_foo");
            CHECK_EQUAL(table->begin()->get<Int>("i"), 123);
        }
        // Try to upload something
        {
            WriteTransaction wt(db_2);
            auto table = wt.get_table("class_foo");
            table->begin()->set("i", 456);
            session_2.nonsync_transact_notify(wt.commit());
        }
        session_2.wait_for_upload_complete_or_client_stopped();
        CHECK(did_get_permission_denied);
    }

    // Check that the original client was unchanged
    {
        Session session_1 = fixture.make_bound_session(0, db_1, 0, "/test");
        session_1.wait_for_download_complete_or_client_stopped();
        ReadTransaction rt(db_1);
        auto table = rt.get_table("class_foo");
        CHECK_EQUAL(table->begin()->get<Int>("i"), 123);
    }
}


// This test is a performance study. A single client keeps creating
// transactions that creates new objects and uploads them. The time to perform
// upload completion is measured and logged at info level.
TEST(Sync_SingleClientUploadForever_CreateObjects)
{
    int_fast32_t number_of_transactions = 100; // Set to low number in ordinary testing.

    util::Logger& logger = test_context.logger;

    logger.info("Sync_SingleClientUploadForever_CreateObjects test. Number of transactions = %1",
                number_of_transactions);

    TEST_DIR(server_dir);
    TEST_CLIENT_DB(db);

    ClientServerFixture fixture(server_dir, test_context);
    fixture.start();

    ColKey col_int;
    ColKey col_str;
    ColKey col_dbl;
    ColKey col_time;

    {
        WriteTransaction wt{db};
        TableRef tr = wt.add_table("class_table");
        col_int = tr->add_column(type_Int, "integer column");
        col_str = tr->add_column(type_String, "string column");
        col_dbl = tr->add_column(type_Double, "double column");
        col_time = tr->add_column(type_Timestamp, "timestamp column");
        wt.commit();
    }

    Session session = fixture.make_bound_session(db);
    session.wait_for_upload_complete_or_client_stopped();

    for (int_fast32_t i = 0; i < number_of_transactions; ++i) {
        WriteTransaction wt{db};
        TableRef tr = wt.get_table("class_table");
        auto obj = tr->create_object();
        int_fast32_t number = i;
        obj.set<Int>(col_int, number);
        std::string str = "str: " + std::to_string(number);
        StringData str_data = StringData(str);
        obj.set(col_str, str_data);
        obj.set(col_dbl, double(number));
        obj.set(col_time, Timestamp{123, 456});
        version_type version = wt.commit();
        auto before_upload = std::chrono::steady_clock::now();
        session.nonsync_transact_notify(version);
        session.wait_for_upload_complete_or_client_stopped();
        auto after_upload = std::chrono::steady_clock::now();

        // We only log the duration every 1000 transactions. The duration is for a single changeset.
        if (i % 1000 == 0) {
            auto duration =
                std::chrono::duration_cast<std::chrono::milliseconds>(after_upload - before_upload).count();
            logger.info("Duration of single changeset upload(%1) = %2 ms", i, duration);
        }
    }
}


// This test is a performance study. A single client keeps creating
// transactions that changes the value of an existing object and uploads them.
// The time to perform upload completion is measured and logged at info level.
TEST(Sync_SingleClientUploadForever_MutateObject)
{
    int_fast32_t number_of_transactions = 100; // Set to low number in ordinary testing.

    util::Logger& logger = test_context.logger;

    logger.info("Sync_SingleClientUploadForever_MutateObject test. Number of transactions = %1",
                number_of_transactions);

    TEST_DIR(server_dir);
    TEST_CLIENT_DB(db);

    ClientServerFixture fixture(server_dir, test_context);
    fixture.start();

    ColKey col_int;
    ColKey col_str;
    ColKey col_dbl;
    ColKey col_time;
    ObjKey obj_key;

    {
        WriteTransaction wt{db};
        TableRef tr = wt.add_table("class_table");
        col_int = tr->add_column(type_Int, "integer column");
        col_str = tr->add_column(type_String, "string column");
        col_dbl = tr->add_column(type_Double, "double column");
        col_time = tr->add_column(type_Timestamp, "timestamp column");
        obj_key = tr->create_object().get_key();
        wt.commit();
    }

    Session session = fixture.make_bound_session(db);
    session.wait_for_upload_complete_or_client_stopped();

    for (int_fast32_t i = 0; i < number_of_transactions; ++i) {
        WriteTransaction wt{db};
        TableRef tr = wt.get_table("class_table");
        int_fast32_t number = i;
        auto obj = tr->get_object(obj_key);
        obj.set<Int>(col_int, number);
        std::string str = "str: " + std::to_string(number);
        StringData str_data = StringData(str);
        obj.set(col_str, str_data);
        obj.set(col_dbl, double(number));
        obj.set(col_time, Timestamp{123, 456});
        version_type version = wt.commit();
        auto before_upload = std::chrono::steady_clock::now();
        session.nonsync_transact_notify(version);
        session.wait_for_upload_complete_or_client_stopped();
        auto after_upload = std::chrono::steady_clock::now();

        // We only log the duration every 1000 transactions. The duration is for a single changeset.
        if (i % 1000 == 0) {
            auto duration =
                std::chrono::duration_cast<std::chrono::milliseconds>(after_upload - before_upload).count();
            logger.info("Duration of single changeset upload(%1) = %2 ms", i, duration);
        }
    }
}


// This test is used to time upload and download.
// The test might be moved to a performance test directory later.
TEST(Sync_LargeUploadDownloadPerformance)
{
    int_fast32_t number_of_transactions = 2;         // Set to low number in ordinary testing.
    int_fast32_t number_of_rows_per_transaction = 5; // Set to low number in ordinary testing.
    int number_of_download_clients = 1;              // Set to low number in ordinary testing
    bool print_durations = false;                    // Set to false in ordinary testing.

    if (print_durations) {
        std::cerr << "Number of transactions = " << number_of_transactions << std::endl;
        std::cerr << "Number of rows per transaction = " << number_of_rows_per_transaction << std::endl;
        std::cerr << "Number of download clients = " << number_of_download_clients << std::endl;
    }

    TEST_DIR(server_dir);
    ClientServerFixture fixture(server_dir, test_context);
    fixture.start();

    TEST_CLIENT_DB(db_upload);

    // Populate path_upload realm with data.
    auto start_data_creation = std::chrono::steady_clock::now();
    {
        {
            WriteTransaction wt{db_upload};
            TableRef tr = wt.add_table("class_table");
            tr->add_column(type_Int, "integer column");
            tr->add_column(type_String, "string column");
            tr->add_column(type_Double, "double column");
            tr->add_column(type_Timestamp, "timestamp column");
            wt.commit();
        }

        for (int_fast32_t i = 0; i < number_of_transactions; ++i) {
            WriteTransaction wt{db_upload};
            TableRef tr = wt.get_table("class_table");
            for (int_fast32_t j = 0; j < number_of_rows_per_transaction; ++j) {
                Obj obj = tr->create_object();
                int_fast32_t number = i * number_of_rows_per_transaction + j;
                obj.set("integer column", number);
                std::string str = "str: " + std::to_string(number);
                StringData str_data = StringData(str);
                obj.set("string column", str_data);
                obj.set("double column", double(number));
                obj.set("timestamp column", Timestamp{123, 456});
            }
            wt.commit();
        }
    }
    auto end_data_creation = std::chrono::steady_clock::now();
    auto duration_data_creation =
        std::chrono::duration_cast<std::chrono::milliseconds>(end_data_creation - start_data_creation).count();
    if (print_durations)
        std::cerr << "Duration of data creation = " << duration_data_creation << " ms" << std::endl;

    // Upload the data.
    auto start_session_upload = std::chrono::steady_clock::now();

    Session session_upload = fixture.make_bound_session(db_upload);
    session_upload.wait_for_upload_complete_or_client_stopped();

    auto end_session_upload = std::chrono::steady_clock::now();
    auto duration_upload =
        std::chrono::duration_cast<std::chrono::milliseconds>(end_session_upload - start_session_upload).count();
    if (print_durations)
        std::cerr << "Duration of uploading = " << duration_upload << " ms" << std::endl;


    // Download the data to the download realms.
    auto start_sesion_download = std::chrono::steady_clock::now();

    std::vector<DBTestPathGuard> shared_group_test_path_guards;
    std::vector<DBRef> dbs;
    std::vector<Session> sessions;

    for (int i = 0; i < number_of_download_clients; ++i) {
        std::string path = get_test_path(test_context.get_test_name(), std::to_string(i));
        shared_group_test_path_guards.emplace_back(path);
        dbs.push_back(DB::create(make_client_replication(), path));
        sessions.push_back(fixture.make_bound_session(dbs.back()));
    }

    // Wait for all Realms to finish. They might finish in another order than
    // started, but calling download_complete on a client after it finished only
    // adds a tiny amount of extra mark messages.
    for (auto& session : sessions)
        session.wait_for_download_complete_or_client_stopped();


    auto end_session_download = std::chrono::steady_clock::now();
    auto duration_download =
        std::chrono::duration_cast<std::chrono::milliseconds>(end_session_download - start_sesion_download).count();
    if (print_durations)
        std::cerr << "Duration of downloading = " << duration_download << " ms" << std::endl;


    // Check convergence.
    for (int i = 0; i < number_of_download_clients; ++i) {
        ReadTransaction rt_1(db_upload);
        ReadTransaction rt_2(dbs[i]);
        CHECK(compare_groups(rt_1, rt_2));
    }
}


// This test creates a changeset that is larger than 4GB, uploads it and downloads it to another client.
// The test checks that compression and other aspects of large changeset handling works.
// The test is disabled since it requires a powerful machine to run.
TEST_IF(Sync_4GB_Messages, false)
{
    // The changeset will be slightly larger.
    const uint64_t approximate_changeset_size = uint64_t(1) << 32;

    TEST_DIR(dir);
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);
    ClientServerFixture fixture(dir, test_context);
    fixture.start();

    Session session_1 = fixture.make_bound_session(db_1);
    session_1.wait_for_download_complete_or_client_stopped();

    Session session_2 = fixture.make_bound_session(db_2);
    session_2.wait_for_download_complete_or_client_stopped();

    const size_t single_object_data_size = size_t(1e7); // 10 MB which is below the 16 MB limit
    const size_t num_objects = size_t(approximate_changeset_size / single_object_data_size + 1);

    const std::string str_a(single_object_data_size, 'a');
    BinaryData bd_a(str_a.data(), single_object_data_size);

    const std::string str_b(single_object_data_size, 'b');
    BinaryData bd_b(str_b.data(), single_object_data_size);

    const std::string str_c(single_object_data_size, 'c');
    BinaryData bd_c(str_c.data(), single_object_data_size);

    {
        WriteTransaction wt{db_1};

        TableRef tr = wt.add_table("class_simple_data");
        auto col_key = tr->add_column(type_Binary, "binary column");
        for (size_t i = 0; i < num_objects; ++i) {
            Obj obj = tr->create_object();
            switch (i % 3) {
                case 0:
                    obj.set(col_key, bd_a);
                    break;
                case 1:
                    obj.set(col_key, bd_b);
                    break;
                default:
                    obj.set(col_key, bd_c);
            }
        }
        version_type new_version = wt.commit();
        session_1.nonsync_transact_notify(new_version);
    }
    session_1.wait_for_upload_complete_or_client_stopped();
    session_2.wait_for_download_complete_or_client_stopped();

    // Check convergence.
    {
        ReadTransaction rt_1(db_1);
        ReadTransaction rt_2(db_2);
        CHECK(compare_groups(rt_1, rt_2));
    }
}


TEST(Sync_RefreshSignedUserToken)
{
    TEST_DIR(dir);
    TEST_CLIENT_DB(db);
    ClientServerFixture fixture(dir, test_context);
    fixture.start();

    Session session = fixture.make_bound_session(db);
    session.wait_for_download_complete_or_client_stopped();
    session.refresh(g_signed_test_user_token);
    session.wait_for_download_complete_or_client_stopped();
}


// This test refreshes the user token multiple times right after binding
// the session. The test tries to achieve a situation where a session is
// enlisted to send after sending BIND but before receiving ALLOC.
// The token is refreshed multiple times to increase the probability that the
// refresh took place after BIND. The check of the test is just the absence of
// errors.
TEST(Sync_RefreshRightAfterBind)
{
    TEST_DIR(dir);
    TEST_CLIENT_DB(db);
    ClientServerFixture fixture(dir, test_context);
    fixture.start();

    Session session = fixture.make_bound_session(db);
    for (int i = 0; i < 50; ++i) {
        session.refresh(g_signed_test_user_token_readonly);
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    session.wait_for_download_complete_or_client_stopped();
}


TEST(Sync_Permissions)
{
    TEST_CLIENT_DB(db_valid);
    TEST_CLIENT_DB(db_invalid);

    bool did_see_error_for_valid = false;
    bool did_see_error_for_invalid = false;

    TEST_DIR(server_dir);

    // FIXME: This could use a single client, but the fixture doesn't really
    // make it easier to deal with session-level errors without disrupting other
    // sessions.
    MultiClientServerFixture fixture{2, 1, server_dir, test_context};
    fixture.set_client_side_error_handler(0, [&](std::error_code, bool, const std::string& message) {
        CHECK_EQUAL("", message);
        did_see_error_for_valid = true;
    });
    fixture.set_client_side_error_handler(1, [&](std::error_code ec, bool, const std::string&) {
        CHECK_EQUAL(ProtocolError::permission_denied, ec);
        did_see_error_for_invalid = true;
        fixture.get_client(1).stop();
    });
    fixture.start();

    Session session_valid = fixture.make_bound_session(0, db_valid, 0, "/valid", g_signed_test_user_token_for_path);
    Session session_invalid =
        fixture.make_bound_session(1, db_invalid, 0, "/invalid", g_signed_test_user_token_for_path);

    // Insert some dummy data
    WriteTransaction wt_valid{db_valid};
    wt_valid.add_table("class_a");
    session_valid.nonsync_transact_notify(wt_valid.commit());
    session_valid.wait_for_upload_complete_or_client_stopped();

    WriteTransaction wt_invalid{db_invalid};
    wt_invalid.add_table("class_b");
    session_invalid.nonsync_transact_notify(wt_invalid.commit());
    session_invalid.wait_for_upload_complete_or_client_stopped();

    CHECK_NOT(did_see_error_for_valid);
    CHECK(did_see_error_for_invalid);
}


// This test checks that a client SSL connection to localhost succeeds when the
// server presents a certificate issued to localhost signed by a CA whose
// certificate the client loads.
TEST(Sync_SSL_Certificate_1)
{
    TEST_DIR(server_dir);
    TEST_CLIENT_DB(db);
    std::string ca_dir = get_test_resource_path() + "../certificate-authority";

    ClientServerFixture::Config config;
    config.enable_server_ssl = true;
    config.server_ssl_certificate_path = ca_dir + "/certs/localhost-chain.crt.pem";
    config.server_ssl_certificate_key_path = ca_dir + "/certs/localhost-server.key.pem";

    ClientServerFixture fixture{server_dir, test_context, config};

    Session::Config session_config;
    session_config.protocol_envelope = ProtocolEnvelope::realms;
    session_config.verify_servers_ssl_certificate = true;
    session_config.ssl_trust_certificate_path = ca_dir + "/root-ca/crt.pem";

    Session session = fixture.make_session(db, std::move(session_config));
    fixture.bind_session(session, "/test", g_signed_test_user_token, ProtocolEnvelope::realms);

    fixture.start();
    session.wait_for_download_complete_or_client_stopped();
}


// This test checks that a client SSL connection to localhost does not succeed
// when the server presents a certificate issued to localhost signed by a CA whose
// certificate does not match the certificate loaded by the client.
TEST(Sync_SSL_Certificate_2)
{
    bool did_fail = false;
    TEST_DIR(server_dir);
    TEST_CLIENT_DB(db);
    std::string ca_dir = get_test_resource_path() + "../certificate-authority";

    ClientServerFixture::Config config;
    config.enable_server_ssl = true;
    config.server_ssl_certificate_path = ca_dir + "/certs/localhost-chain.crt.pem";
    config.server_ssl_certificate_key_path = ca_dir + "/certs/localhost-server.key.pem";

    ClientServerFixture fixture{server_dir, test_context, config};

    Session::Config session_config;
    session_config.protocol_envelope = ProtocolEnvelope::realms;
    session_config.verify_servers_ssl_certificate = true;
    session_config.ssl_trust_certificate_path = ca_dir + "/certs/dns-chain.crt.pem";

    auto error_handler = [&](std::error_code ec, bool, const std::string&) {
        CHECK_EQUAL(ec, Client::Error::ssl_server_cert_rejected);
        did_fail = true;
        fixture.stop();
    };
    fixture.set_client_side_error_handler(std::move(error_handler));

    Session session = fixture.make_bound_session(db, "/test", g_signed_test_user_token, std::move(session_config));
    fixture.start();
    session.wait_for_download_complete_or_client_stopped();
    CHECK(did_fail);
}


// This test checks that a client SSL connection to localhost succeeds
// if verify_servers_ssl_certificate = false, even when
// when the server presents a certificate issued to localhost signed by a CA whose
// certificate does not match the certificate loaded by the client.
// This test is identical to Sync_SSL_Certificate_2 except for
// the value of verify_servers_ssl_certificate.
TEST(Sync_SSL_Certificate_3)
{
    TEST_DIR(server_dir);
    TEST_CLIENT_DB(db);
    std::string ca_dir = get_test_resource_path() + "../certificate-authority";

    ClientServerFixture::Config config;
    config.enable_server_ssl = true;
    config.server_ssl_certificate_path = ca_dir + "/certs/localhost-chain.crt.pem";
    config.server_ssl_certificate_key_path = ca_dir + "/certs/localhost-server.key.pem";

    ClientServerFixture fixture{server_dir, test_context, config};

    Session::Config session_config;
    session_config.protocol_envelope = ProtocolEnvelope::realms;
    session_config.verify_servers_ssl_certificate = false;
    session_config.ssl_trust_certificate_path = ca_dir + "/certs/dns-chain.crt.pem";

    Session session = fixture.make_bound_session(db, "/test", g_signed_test_user_token, std::move(session_config));
    fixture.start();
    session.wait_for_download_complete_or_client_stopped();
}


#if REALM_HAVE_SECURE_TRANSPORT

// This test checks that the client can also use a certificate in DER format.
TEST(Sync_SSL_Certificate_DER)
{
    TEST_DIR(server_dir);
    TEST_CLIENT_DB(db);
    std::string ca_dir = get_test_resource_path() + "../certificate-authority";

    ClientServerFixture::Config config;
    config.enable_server_ssl = true;
    config.server_ssl_certificate_path = ca_dir + "/certs/localhost-chain.crt.pem";
    config.server_ssl_certificate_key_path = ca_dir + "/certs/localhost-server.key.pem";

    ClientServerFixture fixture{server_dir, test_context, config};

    Session::Config session_config;
    session_config.protocol_envelope = ProtocolEnvelope::realms;
    session_config.verify_servers_ssl_certificate = true;
    session_config.ssl_trust_certificate_path = ca_dir + "/certs/localhost-chain.crt.cer";

    Session session = fixture.make_session(db, std::move(session_config));
    fixture.bind_session(session, "/test", g_signed_test_user_token, ProtocolEnvelope::realms);

    fixture.start();
    session.wait_for_download_complete_or_client_stopped();
}

#endif // REALM_HAVE_SECURE_TRANSPORT


#if REALM_HAVE_OPENSSL

// This test checks that the SSL connection is accepted if the verify callback
// always returns true.
TEST(Sync_SSL_Certificate_Verify_Callback_1)
{
    TEST_DIR(server_dir);
    TEST_CLIENT_DB(db);
    std::string ca_dir = get_test_resource_path() + "../certificate-authority";

    Session::port_type server_port_ssl;
    auto ssl_verify_callback = [&](const std::string server_address, Session::port_type server_port, const char*,
                                   size_t, int, int) {
        CHECK_EQUAL(server_address, "localhost");
        server_port_ssl = server_port;
        return true;
    };

    ClientServerFixture::Config config;
    config.enable_server_ssl = true;
    config.server_ssl_certificate_path = ca_dir + "/certs/localhost-chain.crt.pem";
    config.server_ssl_certificate_key_path = ca_dir + "/certs/localhost-server.key.pem";

    ClientServerFixture fixture{server_dir, test_context, config};

    Session::Config session_config;
    session_config.protocol_envelope = ProtocolEnvelope::realms;
    session_config.verify_servers_ssl_certificate = true;
    session_config.ssl_trust_certificate_path = util::none;
    session_config.ssl_verify_callback = ssl_verify_callback;

    Session session = fixture.make_bound_session(db, "/test", g_signed_test_user_token, std::move(session_config));
    fixture.start();
    session.wait_for_download_complete_or_client_stopped();

    Session::port_type server_port_actual = fixture.get_server().listen_endpoint().port();
    CHECK_EQUAL(server_port_ssl, server_port_actual);
}


// This test checks that the SSL connection is rejected if the verify callback
// always returns false. It also checks that preverify_ok and depth have
// the expected values.
TEST(Sync_SSL_Certificate_Verify_Callback_2)
{
    bool did_fail = false;
    TEST_DIR(server_dir);
    TEST_CLIENT_DB(db);
    std::string ca_dir = get_test_resource_path() + "../certificate-authority";

    Session::port_type server_port_ssl;
    auto ssl_verify_callback = [&](const std::string server_address, Session::port_type server_port,
                                   const char* pem_data, size_t pem_size, int preverify_ok, int depth) {
        CHECK_EQUAL(server_address, "localhost");
        server_port_ssl = server_port;
        CHECK_EQUAL(preverify_ok, 0);
        CHECK_EQUAL(depth, 1);
        CHECK_EQUAL(pem_size, 2082);
        std::string pem(pem_data, pem_size);

        std::string expected = "-----BEGIN CERTIFICATE-----\n"
                               "MIIF0zCCA7ugAwIBAgIBBjANBgkqhkiG9w0BAQsFADB1MRIwEAYKCZImiZPyLGQB\n";

        CHECK_EQUAL(expected, pem.substr(0, expected.size()));

        return false;
    };

    ClientServerFixture::Config config;
    config.enable_server_ssl = true;
    config.server_ssl_certificate_path = ca_dir + "/certs/localhost-chain.crt.pem";
    config.server_ssl_certificate_key_path = ca_dir + "/certs/localhost-server.key.pem";

    ClientServerFixture fixture{server_dir, test_context, config};

    auto error_handler = [&](std::error_code ec, bool, const std::string&) {
        CHECK_EQUAL(ec, Client::Error::ssl_server_cert_rejected);
        did_fail = true;
        fixture.stop();
    };
    fixture.set_client_side_error_handler(std::move(error_handler));

    Session::Config session_config;
    session_config.protocol_envelope = ProtocolEnvelope::realms;
    session_config.verify_servers_ssl_certificate = true;
    session_config.ssl_trust_certificate_path = util::none;
    session_config.ssl_verify_callback = ssl_verify_callback;

    Session session = fixture.make_bound_session(db, "/test", g_signed_test_user_token, std::move(session_config));
    fixture.start();
    session.wait_for_download_complete_or_client_stopped();
    CHECK(did_fail);
    Session::port_type server_port_actual = fixture.get_server().listen_endpoint().port();
    CHECK_EQUAL(server_port_ssl, server_port_actual);
}


// This test checks that the verify callback function receives the expected
// certificates.
TEST(Sync_SSL_Certificate_Verify_Callback_3)
{
    TEST_DIR(server_dir);
    TEST_CLIENT_DB(db);
    std::string ca_dir = get_test_resource_path() + "../certificate-authority";

    Session::port_type server_port_ssl = 0;
    auto ssl_verify_callback = [&](const std::string server_address, Session::port_type server_port,
                                   const char* pem_data, size_t pem_size, int preverify_ok, int depth) {
        CHECK_EQUAL(server_address, "localhost");
        server_port_ssl = server_port;

        CHECK(depth == 0 || depth == 1);
        if (depth == 1) {
            CHECK_EQUAL(pem_size, 2082);
            CHECK_EQUAL(pem_data[93], 'G');
        }
        else {
            CHECK_EQUAL(pem_size, 1700);
            CHECK_EQUAL(preverify_ok, 1);
            CHECK_EQUAL(pem_data[1667], 'h');
            CHECK_EQUAL(pem_data[1698], '-');
            CHECK_EQUAL(pem_data[1699], '\n');
        }

        return true;
    };

    ClientServerFixture::Config config;
    config.enable_server_ssl = true;
    config.server_ssl_certificate_path = ca_dir + "/certs/localhost-chain.crt.pem";
    config.server_ssl_certificate_key_path = ca_dir + "/certs/localhost-server.key.pem";

    ClientServerFixture fixture{server_dir, test_context, config};

    Session::Config session_config;
    session_config.protocol_envelope = ProtocolEnvelope::realms;
    session_config.verify_servers_ssl_certificate = true;
    session_config.ssl_trust_certificate_path = util::none;
    session_config.ssl_verify_callback = ssl_verify_callback;

    Session session = fixture.make_bound_session(db, "/test", g_signed_test_user_token, std::move(session_config));
    fixture.start();
    session.wait_for_download_complete_or_client_stopped();
    Session::port_type server_port_actual = fixture.get_server().listen_endpoint().port();
    CHECK_EQUAL(server_port_ssl, server_port_actual);
}


// This test is used to verify the ssl_verify_callback function against an
// external server. The tests should only be used for debugging should normally
// be disabled.
TEST_IF(Sync_SSL_Certificate_Verify_Callback_External, false)
{
    const std::string server_address = "www.writeurl.com";
    Session::port_type port = 443;

    TEST_CLIENT_DB(db);

    util::Logger& logger = test_context.logger;
    util::PrefixLogger client_logger("Client: ", logger);
    Client::Config config;
    config.logger = &client_logger;
    config.reconnect_mode = ReconnectMode::testing;
    config.tcp_no_delay = true;
    Client client(config);

    ThreadWrapper client_thread;
    client_thread.start([&] {
        client.run();
    });

    auto ssl_verify_callback = [&](const std::string server_address, Session::port_type server_port,
                                   const char* pem_data, size_t pem_size, int preverify_ok, int depth) {
        StringData pem{pem_data, pem_size};
        logger.info("server_address = %1, server_port = %2, pem =\n%3\n, "
                    " preverify_ok = %4, depth = %5",
                    server_address, server_port, pem, preverify_ok, depth);
        if (depth == 0)
            client.stop();
        return true;
    };

    Session::Config session_config;
    session_config.server_address = server_address;
    session_config.server_port = port;
    session_config.protocol_envelope = ProtocolEnvelope::realms;
    session_config.verify_servers_ssl_certificate = true;
    session_config.ssl_trust_certificate_path = util::none;
    session_config.ssl_verify_callback = ssl_verify_callback;

    Session session(client, db, std::move(session_config));
    session.bind();
    session.wait_for_download_complete_or_client_stopped();

    client.stop();
    client_thread.join();
}

#endif // REALM_HAVE_OPENSSL


// This test has a single client connected to a server with
// one session.
// The client creates four changesets at various times and
// uploads them to the server. The session has a registered
// progress_handler. It is checked that downloaded_bytes,
// downloadable_bytes, uploaded_bytes, and uploadable_bytes
// are correct. This client does not have any downloaded_bytes
// or downloadable bytes because it created all the changesets
// itself.
TEST(Sync_UploadDownloadProgress_1)
{
    TEST_DIR(server_dir);
    TEST_CLIENT_DB(db);

    uint_fast64_t global_snapshot_version;

    {
        int handler_entry = 0;

        bool cond_var_signaled = false;
        std::mutex mutex;
        std::condition_variable cond_var;

        std::atomic<uint_fast64_t> downloaded_bytes;
        std::atomic<uint_fast64_t> downloadable_bytes;
        std::atomic<uint_fast64_t> uploaded_bytes;
        std::atomic<uint_fast64_t> uploadable_bytes;
        std::atomic<uint_fast64_t> progress_version;
        std::atomic<uint_fast64_t> snapshot_version;

        ClientServerFixture fixture(server_dir, test_context);
        fixture.start();

        Session session = fixture.make_session(db);

        auto progress_handler = [&](uint_fast64_t downloaded, uint_fast64_t downloadable, uint_fast64_t uploaded,
                                    uint_fast64_t uploadable, uint_fast64_t progress, uint_fast64_t snapshot) {
            downloaded_bytes = downloaded;
            downloadable_bytes = downloadable;
            uploaded_bytes = uploaded;
            uploadable_bytes = uploadable;
            progress_version = progress;
            snapshot_version = snapshot;

            if (handler_entry == 0) {
                std::unique_lock<std::mutex> lock(mutex);
                cond_var_signaled = true;
                lock.unlock();
                cond_var.notify_one();
            }
            ++handler_entry;
        };

        std::unique_lock<std::mutex> lock(mutex);
        session.set_progress_handler(progress_handler);
        fixture.bind_session(session, "/test");
        cond_var.wait(lock, [&] {
            return cond_var_signaled;
        });

        CHECK_EQUAL(downloaded_bytes, uint_fast64_t(0));
        CHECK_EQUAL(downloadable_bytes, uint_fast64_t(0));
        CHECK_EQUAL(uploaded_bytes, uint_fast64_t(0));
        CHECK_EQUAL(uploadable_bytes, uint_fast64_t(0));
        CHECK_GREATER_EQUAL(snapshot_version, uint_fast64_t(1));

        uint_fast64_t commit_version;
        {
            WriteTransaction wt{db};
            TableRef tr = wt.add_table("class_table");
            tr->add_column(type_Int, "integer column");
            commit_version = wt.commit();
            session.nonsync_transact_notify(commit_version);
        }

        session.wait_for_upload_complete_or_client_stopped();
        session.wait_for_download_complete_or_client_stopped();

        CHECK_EQUAL(downloaded_bytes, uint_fast64_t(0));
        CHECK_EQUAL(downloadable_bytes, uint_fast64_t(0));
        CHECK_NOT_EQUAL(uploaded_bytes, uint_fast64_t(0));
        CHECK_NOT_EQUAL(uploadable_bytes, uint_fast64_t(0));
        CHECK_GREATER(progress_version, uint_fast64_t(0));
        CHECK_GREATER_EQUAL(snapshot_version, commit_version);

        {
            WriteTransaction wt{db};
            TableRef tr = wt.get_table("class_table");
            tr->create_object().set("integer column", 42);
            commit_version = wt.commit();
            session.nonsync_transact_notify(commit_version);
        }

        session.wait_for_upload_complete_or_client_stopped();
        session.wait_for_download_complete_or_client_stopped();

        CHECK_EQUAL(downloaded_bytes, uint_fast64_t(0));
        CHECK_EQUAL(downloadable_bytes, uint_fast64_t(0));
        CHECK_NOT_EQUAL(uploaded_bytes, uint_fast64_t(0));
        CHECK_NOT_EQUAL(uploadable_bytes, uint_fast64_t(0));
        CHECK_GREATER_EQUAL(snapshot_version, commit_version);

        global_snapshot_version = snapshot_version;
    }

    {
        // Here we check that the progress handler is called
        // after the session is bound, and that the values
        // are the ones stored in the Realm in the previous
        // session.

        bool cond_var_signaled = false;
        std::mutex mutex;
        std::condition_variable cond_var;

        util::Logger& logger = test_context.logger;
        util::PrefixLogger client_logger("Client: ", logger);
        Client::Config config;
        config.logger = &client_logger;
        config.reconnect_mode = ReconnectMode::testing;
        config.tcp_no_delay = true;
        Client client(config);

        ThreadWrapper client_thread;
        client_thread.start([&] {
            client.run();
        });

        Session session(client, db);

        int number_of_handler_calls = 0;

        auto progress_handler = [&](uint_fast64_t downloaded_bytes, uint_fast64_t downloadable_bytes,
                                    uint_fast64_t uploaded_bytes, uint_fast64_t uploadable_bytes,
                                    uint_fast64_t progress_version, uint_fast64_t snapshot_version) {
            CHECK_EQUAL(downloaded_bytes, 0);
            CHECK_EQUAL(downloadable_bytes, 0);
            CHECK_NOT_EQUAL(uploaded_bytes, 0);
            CHECK_NOT_EQUAL(uploadable_bytes, 0);
            CHECK_EQUAL(progress_version, 0);
            CHECK_EQUAL(snapshot_version, global_snapshot_version);
            number_of_handler_calls++;

            std::unique_lock<std::mutex> lock(mutex);
            cond_var_signaled = true;
            lock.unlock();
            cond_var.notify_one();
        };

        std::unique_lock<std::mutex> lock(mutex);
        session.set_progress_handler(progress_handler);
        std::string server_address = "no server";
        Session::port_type server_port = 8000;
        session.bind(server_address, "/test", g_signed_test_user_token, server_port, ProtocolEnvelope::realm);
        cond_var.wait(lock, [&] {
            return cond_var_signaled;
        });

        client.stop();
        client_thread.join();

        CHECK_EQUAL(number_of_handler_calls, 1);
    }
}


// This test creates one server and a client with
// two sessions that synchronizes with the same server Realm.
// The clients generate changesets, uploads and downloads, and
// waits for upload/download completion. Both sessions have a
// progress handler registered, and it is checked that the
// progress handlers report the correct values.
TEST(Sync_UploadDownloadProgress_2)
{
    TEST_DIR(server_dir);
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    ClientServerFixture fixture(server_dir, test_context);
    fixture.start();

    Session session_1 = fixture.make_session(db_1);
    Session session_2 = fixture.make_session(db_2);

    uint_fast64_t downloaded_bytes_1 = 123; // Not zero
    uint_fast64_t downloadable_bytes_1 = 123;
    uint_fast64_t uploaded_bytes_1 = 123;
    uint_fast64_t uploadable_bytes_1 = 123;
    uint_fast64_t progress_version_1 = 123;
    uint_fast64_t snapshot_version_1 = 0;

    auto progress_handler_1 = [&](uint_fast64_t downloaded_bytes, uint_fast64_t downloadable_bytes,
                                  uint_fast64_t uploaded_bytes, uint_fast64_t uploadable_bytes,
                                  uint_fast64_t progress_version, uint_fast64_t snapshot_version) {
        downloaded_bytes_1 = downloaded_bytes;
        downloadable_bytes_1 = downloadable_bytes;
        uploaded_bytes_1 = uploaded_bytes;
        uploadable_bytes_1 = uploadable_bytes;
        progress_version_1 = progress_version;
        snapshot_version_1 = snapshot_version;
    };

    session_1.set_progress_handler(progress_handler_1);

    uint_fast64_t downloaded_bytes_2 = 123;
    uint_fast64_t downloadable_bytes_2 = 123;
    uint_fast64_t uploaded_bytes_2 = 123;
    uint_fast64_t uploadable_bytes_2 = 123;
    uint_fast64_t progress_version_2 = 123;
    uint_fast64_t snapshot_version_2 = 0;

    auto progress_handler_2 = [&](uint_fast64_t downloaded_bytes, uint_fast64_t downloadable_bytes,
                                  uint_fast64_t uploaded_bytes, uint_fast64_t uploadable_bytes,
                                  uint_fast64_t progress_version, uint_fast64_t snapshot_version) {
        downloaded_bytes_2 = downloaded_bytes;
        downloadable_bytes_2 = downloadable_bytes;
        uploaded_bytes_2 = uploaded_bytes;
        uploadable_bytes_2 = uploadable_bytes;
        progress_version_2 = progress_version;
        snapshot_version_2 = snapshot_version;
    };

    session_2.set_progress_handler(progress_handler_2);

    fixture.bind_session(session_1, "/test");
    fixture.bind_session(session_2, "/test");

    session_1.wait_for_upload_complete_or_client_stopped();
    session_2.wait_for_upload_complete_or_client_stopped();
    session_1.wait_for_download_complete_or_client_stopped();
    session_2.wait_for_download_complete_or_client_stopped();

    CHECK_EQUAL(downloaded_bytes_1, downloadable_bytes_1);
    CHECK_EQUAL(downloaded_bytes_2, downloadable_bytes_2);
    CHECK_EQUAL(downloaded_bytes_1, downloaded_bytes_2);
    CHECK_EQUAL(downloadable_bytes_1, 0);
    CHECK_GREATER(progress_version_1, 0);
    CHECK_GREATER(snapshot_version_1, 0);

    CHECK_EQUAL(uploaded_bytes_1, 0);
    CHECK_EQUAL(uploadable_bytes_1, 0);

    CHECK_EQUAL(uploaded_bytes_2, 0);
    CHECK_EQUAL(uploadable_bytes_2, 0);
    CHECK_GREATER(progress_version_2, 0);
    CHECK_GREATER(snapshot_version_2, 0);

    write_transaction_notifying_session(db_1, session_1, [](WriteTransaction& wt) {
        TableRef tr = wt.add_table("class_table");
        tr->add_column(type_Int, "integer column");
    });

    session_1.wait_for_upload_complete_or_client_stopped();
    session_2.wait_for_upload_complete_or_client_stopped();
    session_1.wait_for_download_complete_or_client_stopped();
    session_2.wait_for_download_complete_or_client_stopped();

    CHECK_EQUAL(downloaded_bytes_1, 0);
    CHECK_EQUAL(downloadable_bytes_1, 0);

    CHECK_NOT_EQUAL(downloaded_bytes_2, 0);
    CHECK_NOT_EQUAL(downloadable_bytes_2, 0);

    CHECK_NOT_EQUAL(uploaded_bytes_1, 0);
    CHECK_NOT_EQUAL(uploadable_bytes_1, 0);

    CHECK_EQUAL(uploaded_bytes_2, 0);
    CHECK_EQUAL(uploadable_bytes_2, 0);

    CHECK_GREATER(snapshot_version_1, 1);
    CHECK_GREATER(snapshot_version_2, 1);

    write_transaction_notifying_session(db_1, session_1, [](WriteTransaction& wt) {
        TableRef tr = wt.get_table("class_table");
        tr->create_object().set("integer column", 42);
    });

    write_transaction_notifying_session(db_1, session_1, [](WriteTransaction& wt) {
        TableRef tr = wt.get_table("class_table");
        tr->create_object().set("integer column", 44);
    });

    write_transaction_notifying_session(db_2, session_2, [](WriteTransaction& wt) {
        TableRef tr = wt.get_table("class_table");
        tr->create_object().set("integer column", 43);
    });

    session_1.wait_for_upload_complete_or_client_stopped();
    session_2.wait_for_upload_complete_or_client_stopped();
    session_1.wait_for_download_complete_or_client_stopped();
    session_2.wait_for_download_complete_or_client_stopped();

    CHECK_NOT_EQUAL(downloaded_bytes_1, 0);
    CHECK_NOT_EQUAL(downloadable_bytes_1, 0);

    CHECK_NOT_EQUAL(downloaded_bytes_2, 0);
    CHECK_NOT_EQUAL(downloadable_bytes_2, 0);

    CHECK_NOT_EQUAL(uploaded_bytes_1, 0);
    CHECK_NOT_EQUAL(uploadable_bytes_1, 0);

    CHECK_NOT_EQUAL(uploaded_bytes_2, 0);
    CHECK_NOT_EQUAL(uploadable_bytes_2, 0);

    CHECK_GREATER(snapshot_version_1, 4);
    CHECK_GREATER(snapshot_version_2, 3);

    write_transaction_notifying_session(db_1, session_1, [](WriteTransaction& wt) {
        TableRef tr = wt.get_table("class_table");
        tr->begin()->set("integer column", 101);
    });

    write_transaction_notifying_session(db_2, session_2, [](WriteTransaction& wt) {
        TableRef tr = wt.get_table("class_table");
        tr->begin()->set("integer column", 102);
    });

    session_1.wait_for_upload_complete_or_client_stopped();
    session_2.wait_for_upload_complete_or_client_stopped();
    session_1.wait_for_download_complete_or_client_stopped();
    session_2.wait_for_download_complete_or_client_stopped();

    CHECK_EQUAL(downloaded_bytes_1, downloadable_bytes_1);

    // uncertainty due to merge
    CHECK_NOT_EQUAL(downloaded_bytes_1, 0);

    CHECK_EQUAL(downloaded_bytes_2, downloadable_bytes_2);
    CHECK_NOT_EQUAL(downloaded_bytes_2, 0);

    CHECK_NOT_EQUAL(uploaded_bytes_1, 0);
    CHECK_NOT_EQUAL(uploadable_bytes_1, 0);

    CHECK_NOT_EQUAL(uploaded_bytes_2, 0);
    CHECK_NOT_EQUAL(uploadable_bytes_2, 0);

    CHECK_GREATER(snapshot_version_1, 6);
    CHECK_GREATER(snapshot_version_2, 5);

    CHECK_GREATER(snapshot_version_1, 6);
    CHECK_GREATER(snapshot_version_2, 5);

    // Check convergence.
    {
        ReadTransaction rt_1(db_1);
        ReadTransaction rt_2(db_2);
        CHECK(compare_groups(rt_1, rt_2));
    }
}


// This test creates a server and a client. Initially, the server is not running.
// The client generates changes and binds a session. It is verified that the
// progress_handler() is called and that the four arguments of progress_handler()
// have the correct values. The server is started in the first call to
// progress_handler() and it is checked that after upload and download completion,
// the upload_progress_handler has been called again, and that the four arguments
// have the correct values. After this, the server is stopped and the client produces
// more changes. It is checked that the progress_handler() is called and that the
// final values are correct.
TEST(Sync_UploadDownloadProgress_3)
{
    TEST_DIR(server_dir);
    TEST_CLIENT_DB(db);

    util::Logger& logger = test_context.logger;
    util::PrefixLogger server_logger("Server: ", logger);
    util::PrefixLogger client_logger("Client: ", logger);

    std::string server_address = "localhost";

    Server::Config server_config;
    server_config.logger = &server_logger;
    server_config.listen_address = server_address;
    server_config.listen_port = "";
    server_config.tcp_no_delay = true;

    util::Optional<PKey> public_key = PKey::load_public(g_test_server_key_path);
    Server server(server_dir, std::move(public_key), server_config);
    server.start();
    auto server_port = server.listen_endpoint().port();

    ThreadWrapper server_thread;

    // The server is not running.

    {
        WriteTransaction wt{db};
        TableRef tr = wt.add_table("class_table");
        tr->add_column(type_Int, "integer column");
        wt.commit();
    }

    Client::Config client_config;
    client_config.logger = &client_logger;
    client_config.reconnect_mode = ReconnectMode::testing;
    client_config.tcp_no_delay = true;
    Client client(client_config);

    ThreadWrapper client_thread;
    client_thread.start([&] {
        client.run();
    });

    // when connecting to the C++ server, use URL prefix:
    Session::Config config;
    config.service_identifier = "/realm-sync";

    Session session(client, db, std::move(config));

    // entry is used to count the number of calls to
    // progress_handler. At the first call, the server is
    // not running, and it is started by progress_handler().
    int entry = 0;

    bool should_signal_cond_var = false;
    bool cond_var_signaled = false;
    std::mutex mutex;
    std::condition_variable cond_var;

    uint_fast64_t downloaded_bytes_1 = 123; // Not zero
    uint_fast64_t downloadable_bytes_1 = 123;
    uint_fast64_t uploaded_bytes_1 = 123;
    uint_fast64_t uploadable_bytes_1 = 123;
    uint_fast64_t progress_version_1 = 123;
    uint_fast64_t snapshot_version_1 = 0;

    auto progress_handler = [&](uint_fast64_t downloaded_bytes, uint_fast64_t downloadable_bytes,
                                uint_fast64_t uploaded_bytes, uint_fast64_t uploadable_bytes,
                                uint_fast64_t progress_version, uint_fast64_t snapshot_version) {
        downloaded_bytes_1 = downloaded_bytes;
        downloadable_bytes_1 = downloadable_bytes;
        uploaded_bytes_1 = uploaded_bytes;
        uploadable_bytes_1 = uploadable_bytes;
        progress_version_1 = progress_version;
        snapshot_version_1 = snapshot_version;

        if (entry == 0) {
            CHECK_EQUAL(downloaded_bytes, 0);
            CHECK_EQUAL(downloadable_bytes, 0);
            CHECK_EQUAL(uploaded_bytes, 0);
            CHECK_NOT_EQUAL(uploadable_bytes, 0);
            CHECK_EQUAL(snapshot_version, 2);
        }

        if (entry == 0) {
            server_thread.start([&] {
                server.run();
            });
        }

        if (should_signal_cond_var) {
            std::unique_lock<std::mutex> lock(mutex);
            cond_var_signaled = true;
            lock.unlock();
            cond_var.notify_one();
        }

        entry++;
    };

    session.set_progress_handler(progress_handler);

    session.bind(server_address, "/test", g_signed_test_user_token, server_port, ProtocolEnvelope::realm);

    session.wait_for_upload_complete_or_client_stopped();
    session.wait_for_download_complete_or_client_stopped();

    // Now the server is running.

    CHECK_EQUAL(downloaded_bytes_1, 0);
    CHECK_EQUAL(downloadable_bytes_1, 0);
    CHECK_NOT_EQUAL(uploaded_bytes_1, 0);
    CHECK_NOT_EQUAL(uploadable_bytes_1, 0);
    CHECK_GREATER(progress_version_1, 0);
    CHECK_GREATER_EQUAL(snapshot_version_1, 2);

    server.stop();

    // The server is stopped

    should_signal_cond_var = true;

    uint_fast64_t commited_version;
    {
        WriteTransaction wt{db};
        TableRef tr = wt.get_table("class_table");
        tr->create_object().set("integer column", 42);
        commited_version = wt.commit();
        session.nonsync_transact_notify(commited_version);
    }

    {
        std::unique_lock<std::mutex> lock(mutex);
        cond_var.wait(lock, [&] {
            return cond_var_signaled;
        });
    }

    CHECK_EQUAL(downloaded_bytes_1, 0);
    CHECK_EQUAL(downloadable_bytes_1, 0);
    CHECK_NOT_EQUAL(uploaded_bytes_1, 0);
    CHECK_NOT_EQUAL(uploadable_bytes_1, 0);
    CHECK_EQUAL(snapshot_version_1, commited_version);

    client.stop();

    server_thread.join();
    client_thread.join();
}


// This test creates a server and two clients. The first client uploads two
// large changesets. The other client downloads them. The download messages to
// the second client contains one changeset because the changesets are larger
// than the soft size limit for changesets in the DOWNLOAD message. This implies
// that after receiving the first DOWNLOAD message, the second client will have
// downloaded_bytes < downloadable_bytes.
TEST(Sync_UploadDownloadProgress_4)
{
    TEST_DIR(server_dir);
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    {
        WriteTransaction wt{db_1};
        TableRef tr = wt.add_table("class_table");
        auto col = tr->add_column(type_Binary, "binary column");
        tr->create_object();
        std::string str(size_t(5e5), 'a');
        BinaryData bd(str.data(), str.size());
        tr->begin()->set(col, bd);
        wt.commit();
    }

    {
        WriteTransaction wt{db_1};
        TableRef tr = wt.get_table("class_table");
        auto col = tr->get_column_key("binary column");
        tr->create_object();
        std::string str(size_t(1e6), 'a');
        BinaryData bd(str.data(), str.size());
        tr->begin()->set(col, bd);
        wt.commit();
    }

    ClientServerFixture::Config config;
    config.max_download_size = size_t(1e5);
    ClientServerFixture fixture(server_dir, test_context, config);
    fixture.start();

    Session session_1 = fixture.make_session(db_1);

    int entry_1 = 0;

    auto progress_handler_1 = [&](uint_fast64_t downloaded_bytes, uint_fast64_t downloadable_bytes,
                                  uint_fast64_t uploaded_bytes, uint_fast64_t uploadable_bytes,
                                  uint_fast64_t progress_version, uint_fast64_t snapshot_version) {
        CHECK_EQUAL(downloaded_bytes, 0);
        CHECK_EQUAL(downloadable_bytes, 0);
        CHECK_NOT_EQUAL(uploadable_bytes, 0);

        if (entry_1 == 0) {
            CHECK_EQUAL(progress_version, 0);
            CHECK_EQUAL(uploaded_bytes, 0);
            CHECK_EQUAL(snapshot_version, 3);
        }
        else {
            CHECK_GREATER(progress_version, 0);
            CHECK_GREATER(snapshot_version, 3);
        }

        ++entry_1;
    };

    session_1.set_progress_handler(progress_handler_1);

    fixture.bind_session(session_1, "/test");
    session_1.wait_for_upload_complete_or_client_stopped();
    session_1.wait_for_download_complete_or_client_stopped();

    CHECK_NOT_EQUAL(entry_1, 0);

    Session session_2 = fixture.make_session(db_2);

    int entry_2 = 0;

    auto progress_handler_2 = [&](uint_fast64_t downloaded_bytes, uint_fast64_t downloadable_bytes,
                                  uint_fast64_t uploaded_bytes, uint_fast64_t uploadable_bytes,
                                  uint_fast64_t progress_version, uint_fast64_t snapshot_version) {
        CHECK_EQUAL(uploaded_bytes, 0);
        CHECK_EQUAL(uploadable_bytes, 0);

        if (entry_2 == 0) {
            CHECK_EQUAL(progress_version, 0);
            CHECK_EQUAL(downloaded_bytes, 0);
            CHECK_EQUAL(downloadable_bytes, 0);
            CHECK_EQUAL(snapshot_version, 1);
        }
        else if (entry_2 == 1) {
            CHECK_GREATER(progress_version, 0);
            CHECK_NOT_EQUAL(downloaded_bytes, 0);
            CHECK_NOT_EQUAL(downloadable_bytes, 0);
            CHECK_EQUAL(snapshot_version, 3);
        }
        else if (entry_2 == 2) {
            CHECK_GREATER(progress_version, 0);
            CHECK_NOT_EQUAL(downloaded_bytes, 0);
            CHECK_NOT_EQUAL(downloadable_bytes, 0);
            CHECK_EQUAL(snapshot_version, 4);
        }

        ++entry_2;
    };

    session_2.set_progress_handler(progress_handler_2);

    fixture.bind_session(session_2, "/test");

    session_2.wait_for_upload_complete_or_client_stopped();
    session_2.wait_for_download_complete_or_client_stopped();
}


// This test has a single client connected to a server with one session. The
// client does not create any changesets. The test verifies that the client gets
// a confirmation from the server of downloadable_bytes = 0.
TEST(Sync_UploadDownloadProgress_5)
{
    TEST_DIR(server_dir);
    TEST_CLIENT_DB(db);

    bool cond_var_signaled = false;
    std::mutex mutex;
    std::condition_variable cond_var;

    ClientServerFixture fixture(server_dir, test_context);
    fixture.start();

    Session session = fixture.make_session(db);

    auto progress_handler = [&](uint_fast64_t downloaded_bytes, uint_fast64_t downloadable_bytes,
                                uint_fast64_t uploaded_bytes, uint_fast64_t uploadable_bytes,
                                uint_fast64_t progress_version, uint_fast64_t snapshot_version) {
        CHECK_EQUAL(downloaded_bytes, 0);
        CHECK_EQUAL(downloadable_bytes, 0);
        CHECK_EQUAL(uploaded_bytes, 0);
        CHECK_EQUAL(uploadable_bytes, 0);

        if (progress_version > 0) {
            CHECK_EQUAL(snapshot_version, 3);
            std::unique_lock<std::mutex> lock(mutex);
            cond_var_signaled = true;
            lock.unlock();
            cond_var.notify_one();
        }
    };

    session.set_progress_handler(progress_handler);

    std::unique_lock<std::mutex> lock(mutex);
    fixture.bind_session(session, "/test");
    cond_var.wait(lock, [&] {
        return cond_var_signaled;
    });

    // The check is that we reach this point.
}


// This test has a single client connected to a server with one session.
// The session has a registered progress handler.
TEST(Sync_UploadDownloadProgress_6)
{
    TEST_DIR(server_dir);
    TEST_CLIENT_DB(db);

    util::Logger& logger = test_context.logger;
    util::PrefixLogger server_logger("Server: ", logger);
    util::PrefixLogger client_logger("Client: ", logger);

    Server::Config server_config;
    server_config.logger = &server_logger;
    server_config.listen_address = "localhost";
    server_config.listen_port = "";
    server_config.tcp_no_delay = true;

    util::Optional<PKey> public_key = PKey::load_public(g_test_server_key_path);
    Server server(server_dir, std::move(public_key), server_config);
    server.start();

    auto server_port = server.listen_endpoint().port();

    ThreadWrapper server_thread;
    server_thread.start([&] {
        server.run();
    });

    Client::Config client_config;
    client_config.logger = &client_logger;
    client_config.reconnect_mode = ReconnectMode::testing;
    client_config.one_connection_per_session = false;
    client_config.tcp_no_delay = true;
    Client client(client_config);

    ThreadWrapper client_thread;
    client_thread.start([&] {
        client.run();
    });

    Session::Config session_config;
    session_config.server_address = "localhost";
    session_config.server_port = server_port;
    session_config.realm_identifier = "/test";
    session_config.signed_user_token = g_signed_test_user_token;

    std::unique_ptr<Session> session{new Session{client, db, std::move(session_config)}};

    util::Mutex mutex;

    auto progress_handler = [&](uint_fast64_t downloaded_bytes, uint_fast64_t downloadable_bytes,
                                uint_fast64_t uploaded_bytes, uint_fast64_t uploadable_bytes,
                                uint_fast64_t progress_version, uint_fast64_t snapshot_version) {
        CHECK_EQUAL(downloaded_bytes, 0);
        CHECK_EQUAL(downloadable_bytes, 0);
        CHECK_EQUAL(uploaded_bytes, 0);
        CHECK_EQUAL(uploadable_bytes, 0);
        CHECK_EQUAL(progress_version, 0);
        CHECK_EQUAL(snapshot_version, 1);
        util::LockGuard lock{mutex};
        session.reset();
    };

    session->set_progress_handler(progress_handler);

    {
        util::LockGuard lock{mutex};
        session->bind();
    }

    client.stop();
    server.stop();
    client_thread.join();
    server_thread.join();

    // The check is that we reach this point without deadlocking.
}


TEST(Sync_MultipleSyncAgentsNotAllowed)
{
    // At most one sync agent is allowed to participate in a Realm file access
    // session at any particular point in time. Note that a Realm file access
    // session is a group of temporally overlapping accesses to a Realm file,
    // and that the group of participants is the transitive closure of a
    // particular session participant over the "temporally overlapping access"
    // relation.

    TEST_CLIENT_DB(db);
    Client::Config config;
    config.logger = &test_context.logger;
    config.reconnect_mode = ReconnectMode::testing;
    config.tcp_no_delay = true;
    Client client{config};
    Session session_1{client, db};
    Session session_2{client, db};
    session_1.bind("realm://foo/bar", "blablabla");
    session_2.bind("realm://foo/bar", "blablabla");
    CHECK_THROW(client.run(), MultipleSyncAgents);
}


TEST(Sync_CancelReconnectDelay)
{
    TEST_DIR(server_dir);
    TEST_CLIENT_DB(db);
    TEST_CLIENT_DB(db_x);

    ClientServerFixture::Config fixture_config;
    fixture_config.one_connection_per_session = false;

    // After connection-level error, and at session-level.
    {
        ClientServerFixture fixture{server_dir, test_context, fixture_config};
        fixture.start();

        BowlOfStonesSemaphore bowl;
        auto handler = [&](std::error_code ec, bool, const std::string&) {
            if (CHECK_EQUAL(ec, ProtocolError::connection_closed))
                bowl.add_stone();
        };
        Session session = fixture.make_session(db);
        session.set_error_handler(std::move(handler));
        fixture.bind_session(session, "/test");
        session.wait_for_download_complete_or_client_stopped();
        fixture.close_server_side_connections();
        bowl.get_stone();

        session.cancel_reconnect_delay();
        session.wait_for_download_complete_or_client_stopped();
    }

    // After connection-level error, and at client-level while connection
    // object exists (ConnectionImpl in clinet.cpp).
    {
        ClientServerFixture fixture{server_dir, test_context, fixture_config};
        fixture.start();

        BowlOfStonesSemaphore bowl;
        auto handler = [&](std::error_code ec, bool, const std::string&) {
            if (CHECK_EQUAL(ec, ProtocolError::connection_closed))
                bowl.add_stone();
        };
        Session session = fixture.make_session(db);
        session.set_error_handler(std::move(handler));
        fixture.bind_session(session, "/test");
        session.wait_for_download_complete_or_client_stopped();
        fixture.close_server_side_connections();
        bowl.get_stone();

        fixture.cancel_reconnect_delay();
        session.wait_for_download_complete_or_client_stopped();
    }

    // After connection-level error, and at client-level while connection object
    // does not exist (ConnectionImpl in clinet.cpp).
    {
        ClientServerFixture fixture{server_dir, test_context, fixture_config};
        fixture.start();

        {
            BowlOfStonesSemaphore bowl;
            auto handler = [&](std::error_code ec, bool, const std::string&) {
                if (CHECK_EQUAL(ec, ProtocolError::connection_closed))
                    bowl.add_stone();
            };
            Session session = fixture.make_session(db);
            session.set_error_handler(std::move(handler));
            fixture.bind_session(session, "/test");
            session.wait_for_download_complete_or_client_stopped();
            fixture.close_server_side_connections();
            bowl.get_stone();
        }

        fixture.wait_for_session_terminations_or_client_stopped();
        fixture.wait_for_session_terminations_or_client_stopped();
        // The connection object no longer exists at this time. After the first
        // of the two waits above, the invocation of ConnectionImpl::on_idle()
        // (in client.cpp) has been scheduled. After the second wait, it has
        // been called, and that destroys the connection object.

        fixture.cancel_reconnect_delay();
        {
            Session session = fixture.make_bound_session(db, "/test");
            session.wait_for_download_complete_or_client_stopped();
        }
    }

    // After session-level error, and at session-level.
    {
        ClientServerFixture fixture{server_dir, test_context, fixture_config};
        fixture.start();

        // Add a session for the purpose of keeping the connection open
        Session session_x = fixture.make_bound_session(db_x, "/x");
        session_x.wait_for_download_complete_or_client_stopped();

        BowlOfStonesSemaphore bowl;
        auto handler = [&](std::error_code ec, bool, const std::string&) {
            if (CHECK_EQUAL(ec, ProtocolError::illegal_realm_path))
                bowl.add_stone();
        };
        Session session = fixture.make_session(db);
        session.set_error_handler(std::move(handler));
        fixture.bind_session(session, "/.."); // Illegal virtual path
        bowl.get_stone();

        session.cancel_reconnect_delay();
        bowl.get_stone();
    }

    // After session-level error, and at client-level.
    {
        ClientServerFixture fixture{server_dir, test_context, fixture_config};
        fixture.start();

        // Add a session for the purpose of keeping the connection open
        Session session_x = fixture.make_bound_session(db_x, "/x");
        session_x.wait_for_download_complete_or_client_stopped();

        BowlOfStonesSemaphore bowl;
        auto handler = [&](std::error_code ec, bool, const std::string&) {
            if (CHECK_EQUAL(ec, ProtocolError::illegal_realm_path))
                bowl.add_stone();
        };
        Session session = fixture.make_session(db);
        session.set_error_handler(std::move(handler));
        fixture.bind_session(session, "/.."); // Illegal virtual path
        bowl.get_stone();

        fixture.cancel_reconnect_delay();
        bowl.get_stone();
    }
}


#ifndef REALM_PLATFORM_WIN32

// This test checks that it is possible to create, upload, download, and merge
// changesets larger than 16 MB.
//
// Fails with 'bad alloc' around 1 GB mem usage on 32-bit Windows + 32-bit Linux
TEST_IF(Sync_MergeLargeBinary, !(REALM_ARCHITECTURE_X86_32))
{
    // Two binaries are inserted in each transaction such that the total size
    // of the changeset exceeds 16 MB. A single set_binary operation does not
    // accept a binary larger than 16 MB.
    size_t binary_sizes[] = {
        static_cast<size_t>(8e6), static_cast<size_t>(9e6),  static_cast<size_t>(7e6), static_cast<size_t>(11e6),
        static_cast<size_t>(6e6), static_cast<size_t>(12e6), static_cast<size_t>(5e6), static_cast<size_t>(13e6),
    };

    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    {
        WriteTransaction wt(db_1);
        TableRef table = wt.add_table("class_table name");
        table->add_column(type_Binary, "column name");
        std::string str_1(binary_sizes[0], 'a');
        BinaryData bd_1(str_1.data(), str_1.size());
        std::string str_2(binary_sizes[1], 'b');
        BinaryData bd_2(str_2.data(), str_2.size());
        table->create_object().set("column name", bd_1);
        table->create_object().set("column name", bd_2);
        wt.commit();
    }

    {
        WriteTransaction wt(db_1);
        TableRef table = wt.get_table("class_table name");
        std::string str_1(binary_sizes[2], 'c');
        BinaryData bd_1(str_1.data(), str_1.size());
        std::string str_2(binary_sizes[3], 'd');
        BinaryData bd_2(str_2.data(), str_2.size());
        table->create_object().set("column name", bd_1);
        table->create_object().set("column name", bd_2);
        wt.commit();
    }

    {
        WriteTransaction wt(db_2);
        TableRef table = wt.add_table("class_table name");
        table->add_column(type_Binary, "column name");
        std::string str_1(binary_sizes[4], 'e');
        BinaryData bd_1(str_1.data(), str_1.size());
        std::string str_2(binary_sizes[5], 'f');
        BinaryData bd_2(str_2.data(), str_2.size());
        table->create_object().set("column name", bd_1);
        table->create_object().set("column name", bd_2);
        wt.commit();
    }

    {
        WriteTransaction wt(db_2);
        TableRef table = wt.get_table("class_table name");
        std::string str_1(binary_sizes[6], 'g');
        BinaryData bd_1(str_1.data(), str_1.size());
        std::string str_2(binary_sizes[7], 'h');
        BinaryData bd_2(str_2.data(), str_2.size());
        table->create_object().set("column name", bd_1);
        table->create_object().set("column name", bd_2);
        wt.commit();
    }

    std::uint_fast64_t downloaded_bytes_1 = 0;
    std::uint_fast64_t downloadable_bytes_1 = 0;
    std::uint_fast64_t uploaded_bytes_1 = 0;
    std::uint_fast64_t uploadable_bytes_1 = 0;

    auto progress_handler_1 = [&](std::uint_fast64_t downloaded_bytes, std::uint_fast64_t downloadable_bytes,
                                  std::uint_fast64_t uploaded_bytes, std::uint_fast64_t uploadable_bytes,
                                  std::uint_fast64_t, std::uint_fast64_t) {
        downloaded_bytes_1 = downloaded_bytes;
        downloadable_bytes_1 = downloadable_bytes;
        uploaded_bytes_1 = uploaded_bytes;
        uploadable_bytes_1 = uploadable_bytes;
    };

    std::uint_fast64_t downloaded_bytes_2 = 0;
    std::uint_fast64_t downloadable_bytes_2 = 0;
    std::uint_fast64_t uploaded_bytes_2 = 0;
    std::uint_fast64_t uploadable_bytes_2 = 0;

    auto progress_handler_2 = [&](uint_fast64_t downloaded_bytes, uint_fast64_t downloadable_bytes,
                                  uint_fast64_t uploaded_bytes, uint_fast64_t uploadable_bytes, uint_fast64_t,
                                  uint_fast64_t) {
        downloaded_bytes_2 = downloaded_bytes;
        downloadable_bytes_2 = downloadable_bytes;
        uploaded_bytes_2 = uploaded_bytes;
        uploadable_bytes_2 = uploadable_bytes;
    };

    {
        TEST_DIR(dir);
        MultiClientServerFixture fixture(2, 1, dir, test_context);
        fixture.start();

        {
            Session session_1 = fixture.make_session(0, db_1);
            session_1.set_progress_handler(progress_handler_1);
            fixture.bind_session(session_1, 0, "/test");
            session_1.wait_for_upload_complete_or_client_stopped();
        }

        {
            Session session_2 = fixture.make_session(1, db_2);
            session_2.set_progress_handler(progress_handler_2);
            fixture.bind_session(session_2, 0, "/test");
            session_2.wait_for_download_complete_or_client_stopped();
            session_2.wait_for_upload_complete_or_client_stopped();
        }

        {
            Session session_1 = fixture.make_session(0, db_1);
            session_1.set_progress_handler(progress_handler_1);
            fixture.bind_session(session_1, 0, "/test");
            session_1.wait_for_download_complete_or_client_stopped();
        }
    }

    ReadTransaction read_1(db_1);
    ReadTransaction read_2(db_2);

    const Group& group = read_1;
    CHECK(compare_groups(read_1, read_2));
    ConstTableRef table = group.get_table("class_table name");
    CHECK_EQUAL(table->size(), 8);
    {
        const Obj obj = *table->begin();
        ChunkedBinaryData cb{obj.get<BinaryData>("column name")};
        CHECK((cb.size() == binary_sizes[0] && cb[0] == 'a') || (cb.size() == binary_sizes[4] && cb[0] == 'e'));
    }
    {
        const Obj obj = *(table->begin() + 7);
        ChunkedBinaryData cb{obj.get<BinaryData>("column name")};
        CHECK((cb.size() == binary_sizes[3] && cb[0] == 'd') || (cb.size() == binary_sizes[7] && cb[0] == 'h'));
    }

    CHECK_EQUAL(downloadable_bytes_1, downloaded_bytes_1);
    CHECK_EQUAL(uploadable_bytes_1, uploaded_bytes_1);
    CHECK_NOT_EQUAL(uploaded_bytes_1, 0);

    CHECK_EQUAL(downloadable_bytes_2, downloaded_bytes_2);
    CHECK_EQUAL(uploadable_bytes_2, uploaded_bytes_2);
    CHECK_NOT_EQUAL(uploaded_bytes_2, 0);

    CHECK_EQUAL(uploaded_bytes_1, downloaded_bytes_2);
    CHECK_NOT_EQUAL(downloaded_bytes_1, 0);
}


// This test checks that it is possible to create, upload, download, and merge
// changesets larger than 16 MB. This test uses less memory than
// Sync_MergeLargeBinary.
TEST(Sync_MergeLargeBinaryReducedMemory)
{
    // Two binaries are inserted in a transaction such that the total size
    // of the changeset exceeds 16MB. A single set_binary operation does not
    // accept a binary larger than 16MB. Only one changeset is larger than
    // 16 MB in this test.
    size_t binary_sizes[] = {
        static_cast<size_t>(8e6), static_cast<size_t>(9e6),  static_cast<size_t>(7e4), static_cast<size_t>(11e4),
        static_cast<size_t>(6e4), static_cast<size_t>(12e4), static_cast<size_t>(5e4), static_cast<size_t>(13e4),
    };

    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    {
        WriteTransaction wt(db_1);
        TableRef table = wt.add_table("class_table name");
        table->add_column(type_Binary, "column name");
        std::string str_1(binary_sizes[0], 'a');
        BinaryData bd_1(str_1.data(), str_1.size());
        std::string str_2(binary_sizes[1], 'b');
        BinaryData bd_2(str_2.data(), str_2.size());
        table->create_object().set("column name", bd_1);
        table->create_object().set("column name", bd_2);
        wt.commit();
    }

    {
        WriteTransaction wt(db_1);
        TableRef table = wt.get_table("class_table name");
        std::string str_1(binary_sizes[2], 'c');
        BinaryData bd_1(str_1.data(), str_1.size());
        std::string str_2(binary_sizes[3], 'd');
        BinaryData bd_2(str_2.data(), str_2.size());
        table->create_object().set("column name", bd_1);
        table->create_object().set("column name", bd_2);
        wt.commit();
    }

    {
        WriteTransaction wt(db_2);
        TableRef table = wt.add_table("class_table name");
        table->add_column(type_Binary, "column name");
        std::string str_1(binary_sizes[4], 'e');
        BinaryData bd_1(str_1.data(), str_1.size());
        std::string str_2(binary_sizes[5], 'f');
        BinaryData bd_2(str_2.data(), str_2.size());
        table->create_object().set("column name", bd_1);
        table->create_object().set("column name", bd_2);
        wt.commit();
    }

    {
        WriteTransaction wt(db_2);
        TableRef table = wt.get_table("class_table name");
        std::string str_1(binary_sizes[6], 'g');
        BinaryData bd_1(str_1.data(), str_1.size());
        std::string str_2(binary_sizes[7], 'h');
        BinaryData bd_2(str_2.data(), str_2.size());
        table->create_object().set("column name", bd_1);
        table->create_object().set("column name", bd_2);
        wt.commit();
    }

    uint_fast64_t downloaded_bytes_1 = 0;
    uint_fast64_t downloadable_bytes_1 = 0;
    uint_fast64_t uploaded_bytes_1 = 0;
    uint_fast64_t uploadable_bytes_1 = 0;

    auto progress_handler_1 = [&](uint_fast64_t downloaded_bytes, uint_fast64_t downloadable_bytes,
                                  uint_fast64_t uploaded_bytes, uint_fast64_t uploadable_bytes,
                                  uint_fast64_t /* progress_version */, uint_fast64_t /* snapshot_version */) {
        downloaded_bytes_1 = downloaded_bytes;
        downloadable_bytes_1 = downloadable_bytes;
        uploaded_bytes_1 = uploaded_bytes;
        uploadable_bytes_1 = uploadable_bytes;
    };

    uint_fast64_t downloaded_bytes_2 = 0;
    uint_fast64_t downloadable_bytes_2 = 0;
    uint_fast64_t uploaded_bytes_2 = 0;
    uint_fast64_t uploadable_bytes_2 = 0;

    auto progress_handler_2 = [&](uint_fast64_t downloaded_bytes, uint_fast64_t downloadable_bytes,
                                  uint_fast64_t uploaded_bytes, uint_fast64_t uploadable_bytes,
                                  uint_fast64_t /* progress_version */, uint_fast64_t /* snapshot_version */) {
        downloaded_bytes_2 = downloaded_bytes;
        downloadable_bytes_2 = downloadable_bytes;
        uploaded_bytes_2 = uploaded_bytes;
        uploadable_bytes_2 = uploadable_bytes;
    };

    {
        TEST_DIR(dir);
        MultiClientServerFixture fixture(2, 1, dir, test_context);
        fixture.start();

        {
            Session session_1 = fixture.make_session(0, db_1);
            session_1.set_progress_handler(progress_handler_1);
            fixture.bind_session(session_1, 0, "/test");
            session_1.wait_for_upload_complete_or_client_stopped();
        }

        {
            Session session_2 = fixture.make_session(1, db_2);
            session_2.set_progress_handler(progress_handler_2);
            fixture.bind_session(session_2, 0, "/test");
            session_2.wait_for_download_complete_or_client_stopped();
            session_2.wait_for_upload_complete_or_client_stopped();
        }

        {
            Session session_1 = fixture.make_session(0, db_1);
            session_1.set_progress_handler(progress_handler_1);
            fixture.bind_session(session_1, 0, "/test");
            session_1.wait_for_download_complete_or_client_stopped();
        }
    }

    ReadTransaction read_1(db_1);
    ReadTransaction read_2(db_2);

    const Group& group = read_1;
    CHECK(compare_groups(read_1, read_2));
    ConstTableRef table = group.get_table("class_table name");
    CHECK_EQUAL(table->size(), 8);
    {
        const Obj obj = *table->begin();
        ChunkedBinaryData cb(obj.get<BinaryData>("column name"));
        CHECK((cb.size() == binary_sizes[0] && cb[0] == 'a') || (cb.size() == binary_sizes[4] && cb[0] == 'e'));
    }
    {
        const Obj obj = *(table->begin() + 7);
        ChunkedBinaryData cb(obj.get<BinaryData>("column name"));
        CHECK((cb.size() == binary_sizes[3] && cb[0] == 'd') || (cb.size() == binary_sizes[7] && cb[0] == 'h'));
    }

    CHECK_EQUAL(downloadable_bytes_1, downloaded_bytes_1);
    CHECK_EQUAL(uploadable_bytes_1, uploaded_bytes_1);
    CHECK_NOT_EQUAL(uploaded_bytes_1, 0);

    CHECK_EQUAL(downloadable_bytes_2, downloaded_bytes_2);
    CHECK_EQUAL(uploadable_bytes_2, uploaded_bytes_2);
    CHECK_NOT_EQUAL(uploaded_bytes_2, 0);

    CHECK_EQUAL(uploaded_bytes_1, downloaded_bytes_2);
    CHECK_NOT_EQUAL(downloaded_bytes_1, 0);
}


// This test checks that it is possible to create, upload, download, and merge
// changesets larger than 16MB.
TEST(Sync_MergeLargeChangesets)
{
    constexpr int number_of_rows = 200;

    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    {
        WriteTransaction wt(db_1);
        TableRef table = wt.add_table("class_table name");
        table->add_column(type_Binary, "column name");
        table->add_column(type_Int, "integer column");
        wt.commit();
    }

    {
        WriteTransaction wt(db_2);
        TableRef table = wt.add_table("class_table name");
        table->add_column(type_Binary, "column name");
        table->add_column(type_Int, "integer column");
        wt.commit();
    }

    {
        WriteTransaction wt(db_1);
        TableRef table = wt.get_table("class_table name");
        for (int i = 0; i < number_of_rows; ++i) {
            table->create_object();
        }
        std::string str(100000, 'a');
        BinaryData bd(str.data(), str.size());
        for (int row = 0; row < number_of_rows; ++row) {
            table->get_object(size_t(row)).set("column name", bd);
            table->get_object(size_t(row)).set("integer column", 2 * row);
        }
        wt.commit();
    }

    {
        WriteTransaction wt(db_2);
        TableRef table = wt.get_table("class_table name");
        for (int i = 0; i < number_of_rows; ++i) {
            table->create_object();
        }
        std::string str(100000, 'b');
        BinaryData bd(str.data(), str.size());
        for (int row = 0; row < number_of_rows; ++row) {
            table->get_object(size_t(row)).set("column name", bd);
            table->get_object(size_t(row)).set("integer column", 2 * row + 1);
        }
        wt.commit();
    }

    {
        TEST_DIR(dir);
        MultiClientServerFixture fixture(2, 1, dir, test_context);

        Session session_1 = fixture.make_session(0, db_1);
        fixture.bind_session(session_1, 0, "/test");
        Session session_2 = fixture.make_session(1, db_2);
        fixture.bind_session(session_2, 0, "/test");

        fixture.start();

        session_1.wait_for_upload_complete_or_client_stopped();
        session_2.wait_for_upload_complete_or_client_stopped();
        session_1.wait_for_download_complete_or_client_stopped();
        session_2.wait_for_download_complete_or_client_stopped();
    }

    ReadTransaction read_1(db_1);
    ReadTransaction read_2(db_2);
    const Group& group = read_1;
    CHECK(compare_groups(read_1, read_2));
    ConstTableRef table = group.get_table("class_table name");
    CHECK_EQUAL(table->size(), 2 * number_of_rows);
}

#endif // REALM_PLATFORM_WIN32


TEST(Sync_PingTimesOut)
{
    bool did_fail = false;
    {
        TEST_DIR(dir);
        TEST_CLIENT_DB(db);

        ClientServerFixture::Config config;
        config.client_ping_period = 0;  // send ping immediately
        config.client_pong_timeout = 0; // time out immediately
        ClientServerFixture fixture(dir, test_context, config);

        auto error_handler = [&](std::error_code ec, bool, const std::string&) {
            CHECK_EQUAL(Client::Error::pong_timeout, ec);
            did_fail = true;
            fixture.stop();
        };
        fixture.set_client_side_error_handler(std::move(error_handler));

        fixture.start();

        Session session = fixture.make_bound_session(db);
        session.wait_for_download_complete_or_client_stopped();
    }
    CHECK(did_fail);
}


TEST(Sync_ReconnectAfterPingTimeout)
{
    TEST_DIR(dir);
    TEST_CLIENT_DB(db);

    ClientServerFixture::Config config;
    config.client_ping_period = 0;  // send ping immediately
    config.client_pong_timeout = 0; // time out immediately

    ClientServerFixture fixture(dir, test_context, config);

    BowlOfStonesSemaphore bowl;
    auto error_handler = [&](std::error_code ec, bool, const std::string&) {
        if (CHECK_EQUAL(Client::Error::pong_timeout, ec))
            bowl.add_stone();
    };
    fixture.set_client_side_error_handler(std::move(error_handler));
    fixture.start();

    Session session = fixture.make_bound_session(db, "/test");
    bowl.get_stone();
}


TEST(Sync_UrgentPingIsSent)
{
    bool did_fail = false;
    {
        TEST_DIR(dir);
        TEST_CLIENT_DB(db);

        ClientServerFixture::Config config;
        config.client_pong_timeout = 0; // urgent pings time out immediately

        ClientServerFixture fixture(dir, test_context, config);

        auto error_handler = [&](std::error_code ec, bool, const std::string&) {
            CHECK_EQUAL(Client::Error::pong_timeout, ec);
            did_fail = true;
            fixture.stop();
        };
        fixture.set_client_side_error_handler(std::move(error_handler));

        fixture.start();

        Session session = fixture.make_bound_session(db);
        session.wait_for_download_complete_or_client_stopped(); // ensure connection established
        session.cancel_reconnect_delay();                       // send an urgent ping
        session.wait_for_download_complete_or_client_stopped();
    }
    CHECK(did_fail);
}


TEST(Sync_ServerDiscardDeadConnections)
{
    TEST_DIR(dir);
    TEST_CLIENT_DB(db);

    ClientServerFixture::Config config;
    config.server_connection_reaper_interval = 1; // discard dead connections quickly, FIXME: 0 will not work here :(

    ClientServerFixture fixture(dir, test_context, config);

    BowlOfStonesSemaphore bowl;
    auto error_handler = [&](std::error_code ec, bool, const std::string&) {
        using syserr = util::error::basic_system_errors;
        bool valid_error = (util::MiscExtErrors::end_of_input == ec) ||
                           (util::MiscExtErrors::premature_end_of_input == ec) ||
                           // FIXME: this is the error on Windows. is it correct?
                           (util::make_basic_system_error_code(syserr::connection_reset) == ec) ||
                           (util::make_basic_system_error_code(syserr::connection_aborted) == ec);
        CHECK(valid_error);
        bowl.add_stone();
    };
    fixture.set_client_side_error_handler(std::move(error_handler));
    fixture.start();

    Session session = fixture.make_bound_session(db);
    session.wait_for_download_complete_or_client_stopped(); // ensure connection established
    fixture.set_server_connection_reaper_timeout(0);        // all connections will now be considered dead
    bowl.get_stone();
}


TEST(Sync_Quadratic_Merge)
{
    size_t num_instructions_1 = 100;
    size_t num_instructions_2 = 200;
    REALM_ASSERT(num_instructions_1 >= 3 && num_instructions_2 >= 3);

    TEST_DIR(server_dir);
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    // The schema and data is created with
    // n_operations instructions. The instructions are:
    // create table
    // add column
    // create object
    // n_operations - 3 add_int instructions.
    auto create_data = [](DBRef db, size_t n_operations) {
        WriteTransaction wt(db);
        TableRef table = wt.add_table("class_table");
        table->add_column(type_Int, "i");
        Obj obj = table->create_object();
        for (size_t i = 0; i < n_operations - 3; ++i)
            obj.add_int("i", 1);
        wt.commit();
    };

    create_data(db_1, num_instructions_1);
    create_data(db_2, num_instructions_2);

    int num_clients = 2;
    int num_servers = 1;
    MultiClientServerFixture fixture{num_clients, num_servers, server_dir, test_context};
    fixture.start();

    Session session_1 = fixture.make_session(0, db_1);
    fixture.bind_session(session_1, 0, "/test");
    session_1.wait_for_upload_complete_or_client_stopped();

    Session session_2 = fixture.make_session(1, db_2);
    fixture.bind_session(session_2, 0, "/test");
    session_2.wait_for_upload_complete_or_client_stopped();

    session_1.wait_for_download_complete_or_client_stopped();
    session_2.wait_for_download_complete_or_client_stopped();
}


TEST(Sync_BatchedUploadMessages)
{
    TEST_DIR(server_dir);
    TEST_CLIENT_DB(db);

    ClientServerFixture fixture(server_dir, test_context);
    fixture.start();

    Session session = fixture.make_session(db);

    {
        WriteTransaction wt{db};
        TableRef tr = wt.add_table("class_foo");
        tr->add_column(type_Int, "integer column");
        wt.commit();
    }

    // Create a lot of changesets. We will attempt to check that
    // they are uploaded in a few upload messages.
    for (int i = 0; i < 400; ++i) {
        WriteTransaction wt{db};
        TableRef tr = wt.get_table("class_foo");
        tr->create_object().set("integer column", i);
        wt.commit();
    }

    auto progress_handler = [&](uint_fast64_t downloaded_bytes, uint_fast64_t downloadable_bytes,
                                uint_fast64_t uploaded_bytes, uint_fast64_t uploadable_bytes,
                                uint_fast64_t progress_version, uint_fast64_t snapshot_version) {
        CHECK_GREATER(uploadable_bytes, 1000);

        // This is the important check. If the changesets were not batched,
        // there would be callbacks with partial uploaded_bytes.
        // With batching, all uploadable_bytes are uploaded in the same message.
        CHECK(uploaded_bytes == 0 || uploaded_bytes == uploadable_bytes);
        CHECK_EQUAL(0, downloaded_bytes);
        CHECK_EQUAL(0, downloadable_bytes);
        static_cast<void>(progress_version);
        static_cast<void>(snapshot_version);
    };

    session.set_progress_handler(progress_handler);
    fixture.bind_session(session, "/test");
    session.wait_for_upload_complete_or_client_stopped();
}


TEST(Sync_UploadLogCompactionEnabled)
{
    TEST_DIR(server_dir);
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    ClientServerFixture::Config config;
    config.disable_upload_compaction = false;
    ClientServerFixture fixture(server_dir, test_context, config);
    fixture.start();

    Session session_1 = fixture.make_session(db_1);
    Session session_2 = fixture.make_session(db_2);

    // Create a changeset with lots of overwrites of the
    // same fields.
    {
        WriteTransaction wt{db_1};
        TableRef tr = wt.add_table("class_foo");
        tr->add_column(type_Int, "integer column");
        Obj obj0 = tr->create_object();
        Obj obj1 = tr->create_object();
        for (int i = 0; i < 10000; ++i) {
            obj0.set("integer column", i);
            obj1.set("integer column", 2 * i);
        }
        wt.commit();
    }

    fixture.bind_session(session_1, "/test");
    session_1.wait_for_upload_complete_or_client_stopped();

    auto progress_handler = [&](uint_fast64_t downloaded_bytes, uint_fast64_t downloadable_bytes,
                                uint_fast64_t uploaded_bytes, uint_fast64_t uploadable_bytes,
                                uint_fast64_t progress_version, uint_fast64_t snapshot_version) {
        CHECK_EQUAL(downloaded_bytes, downloadable_bytes);
        CHECK_EQUAL(0, uploaded_bytes);
        CHECK_EQUAL(0, uploadable_bytes);
        static_cast<void>(snapshot_version);
        if (progress_version > 0)
            CHECK_NOT_EQUAL(downloadable_bytes, 0);
    };

    session_2.set_progress_handler(progress_handler);

    fixture.bind_session(session_2, "/test");

    session_2.wait_for_download_complete_or_client_stopped();

    {
        ReadTransaction rt_1(db_1);
        ReadTransaction rt_2(db_2);
        CHECK(compare_groups(rt_1, rt_2));
        ConstTableRef table = rt_1.get_table("class_foo");
        CHECK_EQUAL(2, table->size());
        CHECK_EQUAL(9999, table->begin()->get<Int>("integer column"));
        CHECK_EQUAL(19998, table->get_object(1).get<Int>("integer column"));
    }
}


TEST(Sync_UploadLogCompactionDisabled)
{
    TEST_DIR(server_dir);
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    ClientServerFixture::Config config;
    config.disable_upload_compaction = true;
    config.disable_history_compaction = true;
    ClientServerFixture fixture{server_dir, test_context, config};
    fixture.start();

    // Create a changeset with lots of overwrites of the
    // same fields.
    {
        WriteTransaction wt{db_1};
        TableRef tr = wt.add_table("class_foo");
        auto col_int = tr->add_column(type_Int, "integer column");
        Obj obj0 = tr->create_object();
        Obj obj1 = tr->create_object();
        for (int i = 0; i < 10000; ++i) {
            obj0.set(col_int, i);
            obj1.set(col_int, 2 * i);
        }
        wt.commit();
    }

    Session session_1 = fixture.make_bound_session(db_1, "/test");
    session_1.wait_for_upload_complete_or_client_stopped();

    auto progress_handler = [&](std::uint_fast64_t downloaded_bytes, std::uint_fast64_t downloadable_bytes,
                                std::uint_fast64_t uploaded_bytes, std::uint_fast64_t uploadable_bytes,
                                std::uint_fast64_t progress_version, std::uint_fast64_t snapshot_version) {
        CHECK_EQUAL(downloaded_bytes, downloadable_bytes);
        CHECK_EQUAL(0, uploaded_bytes);
        CHECK_EQUAL(0, uploadable_bytes);
        static_cast<void>(snapshot_version);
        if (progress_version > 0)
            CHECK_NOT_EQUAL(0, downloadable_bytes);
    };

    Session session_2 = fixture.make_session(db_2);
    session_2.set_progress_handler(progress_handler);
    fixture.bind_session(session_2, "/test");
    session_2.wait_for_download_complete_or_client_stopped();

    {
        ReadTransaction rt_1(db_1);
        ReadTransaction rt_2(db_2);
        CHECK(compare_groups(rt_1, rt_2));
        ConstTableRef table = rt_1.get_table("class_foo");
        CHECK_EQUAL(2, table->size());
        CHECK_EQUAL(9999, table->begin()->get<Int>("integer column"));
        CHECK_EQUAL(19998, table->get_object(1).get<Int>("integer column"));
    }
}


TEST(Sync_ReadOnlyClientSideHistoryTrim)
{
    TEST_DIR(dir);
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    ClientServerFixture fixture{dir, test_context};
    fixture.start();

    ColKey col_ndx_blob_data;
    {
        WriteTransaction wt{db_1};
        TableRef blobs = wt.add_table("class_Blob");
        col_ndx_blob_data = blobs->add_column(type_Binary, "data");
        blobs->create_object();
        wt.commit();
    }

    Session session_1 = fixture.make_bound_session(db_1, "/foo");
    Session session_2 = fixture.make_bound_session(db_2, "/foo");

    std::string blob(0x4000, '\0');
    for (long i = 0; i < 1024; ++i) {
        {
            WriteTransaction wt{db_1};
            TableRef blobs = wt.get_table("class_Blob");
            blobs->begin()->set(col_ndx_blob_data, BinaryData{blob});
            version_type new_version = wt.commit();
            session_1.nonsync_transact_notify(new_version);
        }
        session_1.wait_for_upload_complete_or_client_stopped();
        session_2.wait_for_download_complete_or_client_stopped();
    }

    // Check that the file size is less than 4 MiB. If it is, then the history
    // must have been trimmed, as the combined size of all the blobs is at least
    // 16 MiB.
    CHECK_LESS(util::File{db_1_path}.get_size(), 0x400000);
}

#if 0 // FIXME: enable when history and file format upgrade is implemented
TEST(Sync_DownloadLogCompactionClassUnderScorePrefix)
{
    TEST_DIR(server_dir);
    TEST_CLIENT_DB(db);

    std::string virtual_path = "/test";
    std::string origin_server_path =
        util::File::resolve("admin_realm_issue_1794.realm", "resources");
    std::string target_server_path;
    {
        ClientServerFixture fixture{server_dir, test_context};
        target_server_path = fixture.map_virtual_to_real_path(virtual_path);
        fixture.start();
    }
    util::File::copy(origin_server_path, target_server_path);

    // Synchronize a client with the migrated server file
    {
        ClientServerFixture fixture{server_dir, test_context};
        fixture.start();
        Session session = fixture.make_bound_session(client_path, virtual_path);
        session.wait_for_download_complete_or_client_stopped();
    }

    {
        // Verify the migrated server file
        TestServerHistoryContext context;
        _impl::ServerHistory::DummyCompactionControl compaction_control;
        _impl::ServerHistory history{context, compaction_control};
        SharedGroup db{history, target_server_path};
        ReadTransaction rt{db};
        rt.get_group().verify();
    }
}
#endif

// This test creates two objects in a target table and a link list
// in a source table. The first target object is inserted in the link list,
// and later the link is set to the second target object.
// Both the target objects are deleted afterwards. The tests verifies that
// sync works with log compaction turned on.
TEST(Sync_ContainerInsertAndSetLogCompaction)
{
    TEST_DIR(dir);
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);
    ClientServerFixture fixture(dir, test_context);
    fixture.start();

    {
        WriteTransaction wt{db_1};

        TableRef table_target = wt.add_table("class_target");
        ColKey col_ndx = table_target->add_column(type_Int, "value");
        auto k0 = table_target->create_object().set(col_ndx, 123).get_key();
        auto k1 = table_target->create_object().set(col_ndx, 456).get_key();

        TableRef table_source = wt.add_table("class_source");
        col_ndx = table_source->add_column_list(*table_target, "target_link");
        Obj obj = table_source->create_object();
        LnkLst ll = obj.get_linklist(col_ndx);
        ll.insert(0, k0);
        ll.set(0, k1);

        table_target->remove_object(k1);
        table_target->remove_object(k0);

        wt.commit();
    }

    Session session_1 = fixture.make_bound_session(db_1);
    session_1.wait_for_upload_complete_or_client_stopped();

    Session session_2 = fixture.make_bound_session(db_2);
    session_2.wait_for_download_complete_or_client_stopped();

    {
        ReadTransaction rt_1(db_1);
        ReadTransaction rt_2(db_2);
        CHECK(compare_groups(rt_1, rt_2));
    }
}


TEST(Sync_MultipleContainerColumns)
{
    TEST_DIR(dir);
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);
    ClientServerFixture fixture(dir, test_context);
    fixture.start();

    {
        WriteTransaction wt{db_1};

        TableRef table = wt.add_table("class_Table");
        table->add_column_list(type_String, "array1");
        table->add_column_list(type_String, "array2");

        Obj row = table->create_object();
        {
            Lst<StringData> array1 = row.get_list<StringData>("array1");
            array1.clear();
            array1.add("Hello");
        }
        {
            Lst<StringData> array2 = row.get_list<StringData>("array2");
            array2.clear();
            array2.add("World");
        }

        wt.commit();
    }

    Session session_1 = fixture.make_bound_session(db_1);
    session_1.wait_for_upload_complete_or_client_stopped();

    Session session_2 = fixture.make_bound_session(db_2);
    session_2.wait_for_download_complete_or_client_stopped();

    {
        ReadTransaction rt_1(db_1);
        ReadTransaction rt_2(db_2);
        CHECK(compare_groups(rt_1, rt_2));

        ConstTableRef table = rt_1.get_table("class_Table");
        const Obj row = *table->begin();
        auto array1 = row.get_list<StringData>("array1");
        auto array2 = row.get_list<StringData>("array2");
        CHECK_EQUAL(array1.size(), 1);
        CHECK_EQUAL(array2.size(), 1);
        CHECK_EQUAL(array1.get(0), "Hello");
        CHECK_EQUAL(array2.get(0), "World");
    }
}


TEST(Sync_ConnectionStateChange)
{
    TEST_DIR(dir);
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);
    using ErrorInfo = Session::ErrorInfo;
    std::vector<ConnectionState> states_1, states_2;
    {
        ClientServerFixture fixture(dir, test_context);
        fixture.start();

        BowlOfStonesSemaphore bowl_1, bowl_2;
        auto listener_1 = [&](ConnectionState state, const ErrorInfo* error_info) {
            CHECK_EQUAL(state == ConnectionState::disconnected, bool(error_info));
            states_1.push_back(state);
            if (state == ConnectionState::disconnected)
                bowl_1.add_stone();
        };
        auto listener_2 = [&](ConnectionState state, const ErrorInfo* error_info) {
            CHECK_EQUAL(state == ConnectionState::disconnected, bool(error_info));
            states_2.push_back(state);
            if (state == ConnectionState::disconnected)
                bowl_2.add_stone();
        };

        Session session_1 = fixture.make_session(db_1);
        session_1.set_connection_state_change_listener(listener_1);
        fixture.bind_session(session_1, "/test");
        session_1.wait_for_download_complete_or_client_stopped();

        Session session_2 = fixture.make_session(db_2);
        session_2.set_connection_state_change_listener(listener_2);
        fixture.bind_session(session_2, "/test");
        session_2.wait_for_download_complete_or_client_stopped();

        fixture.close_server_side_connections();
        bowl_1.get_stone();
        bowl_2.get_stone();
    }
    std::vector<ConnectionState> reference{ConnectionState::connecting, ConnectionState::connected,
                                           ConnectionState::disconnected};
    CHECK(states_1 == reference);
    CHECK(states_2 == reference);
}


TEST(Sync_ClientErrorHandler)
{
    TEST_DIR(dir);
    TEST_CLIENT_DB(db);
    ClientServerFixture fixture(dir, test_context);
    fixture.start();

    BowlOfStonesSemaphore bowl;
    auto handler = [&](std::error_code, bool, const std::string&) {
        bowl.add_stone();
    };

    Session session = fixture.make_session(db);
    session.set_error_handler(std::move(handler));
    fixture.bind_session(session, "/test");
    session.wait_for_download_complete_or_client_stopped();

    fixture.close_server_side_connections();
    bowl.get_stone();
}


TEST(Sync_VerifyServerHistoryAfterLargeUpload)
{
    TEST_DIR(server_dir);
    TEST_CLIENT_DB(db);

    ClientServerFixture fixture{server_dir, test_context};
    fixture.start();

    {
        WriteTransaction wt{db};
        auto table = wt.add_table("class_table");
        ColKey col = table->add_column(type_Binary, "data");

        // Create enough data that our changeset cannot be stored contiguously
        // by BinaryColumn (> 16MB).
        std::size_t data_size = 8 * 1024 * 1024;
        std::string data(data_size, '\0');
        for (std::size_t i = 0; i < 8; ++i) {
            table->create_object().set(col, BinaryData{data.data(), data.size()});
        }

        wt.commit();

        Session session = fixture.make_session(db);
        fixture.bind_session(session, "/test");
        session.wait_for_upload_complete_or_client_stopped();
    }

    {
        std::string server_path = fixture.map_virtual_to_real_path("/test");
        TestServerHistoryContext context;
        _impl::ServerHistory::DummyCompactionControl compaction_control;
        _impl::ServerHistory history{context, compaction_control};
        DBRef db = DB::create(history, server_path);
        {
            ReadTransaction rt{db};
            rt.get_group().verify();
        }
    }
}


TEST(Sync_ServerSideModify_Randomize)
{
    int num_server_side_transacts = 1200;
    int num_client_side_transacts = 1200;

    TEST_DIR(server_dir);
    TEST_CLIENT_DB(db_2);

    ClientServerFixture::Config config;
    ClientServerFixture fixture{server_dir, test_context, std::move(config)};
    fixture.start();

    Session session = fixture.make_bound_session(db_2, "/test");

    std::string server_path = fixture.map_virtual_to_real_path("/test");
    TestServerHistoryContext context;
    _impl::ServerHistory::DummyCompactionControl compaction_control;
    _impl::ServerHistory history_1{context, compaction_control};
    DBRef db_1 = DB::create(history_1, server_path);

    auto server_side_program = [num_server_side_transacts, &db_1, &fixture, &session] {
        Random random(random_int<unsigned long>()); // Seed from slow global generator
        for (int i = 0; i < num_server_side_transacts; ++i) {
            WriteTransaction wt{db_1};
            TableRef table = wt.get_table("class_foo");
            if (!table) {
                table = wt.add_table("class_foo");
                table->add_column(type_Int, "i");
            }
            if (i % 2 == 0)
                table->create_object();
            Obj obj = *(table->begin() + random.draw_int_mod(table->size()));
            obj.set<int64_t>("i", random.draw_int_max(0x0'7FFF'FFFF'FFFF'FFFF));
            wt.commit();
            fixture.inform_server_about_external_change("/test");
            session.wait_for_download_complete_or_client_stopped();
        }
    };

    auto client_side_program = [num_client_side_transacts, &db_2, &session] {
        Random random(random_int<unsigned long>()); // Seed from slow global generator
        for (int i = 0; i < num_client_side_transacts; ++i) {
            WriteTransaction wt{db_2};
            TableRef table = wt.get_table("class_foo");
            if (!table) {
                table = wt.add_table("class_foo");
                table->add_column(type_Int, "i");
            }
            if (i % 2 == 0)
                table->create_object();
            ;
            Obj obj = *(table->begin() + random.draw_int_mod(table->size()));
            obj.set<int64_t>("i", random.draw_int_max(0x0'7FFF'FFFF'FFFF'FFFF));
            version_type new_version = wt.commit();
            session.nonsync_transact_notify(new_version);
            if (i % 16 == 0)
                session.wait_for_upload_complete_or_client_stopped();
        }
    };

    ThreadWrapper server_program_thread;
    server_program_thread.start(std::move(server_side_program));
    client_side_program();
    CHECK(!server_program_thread.join());

    session.wait_for_upload_complete_or_client_stopped();
    session.wait_for_download_complete_or_client_stopped();

    ReadTransaction rt_1{db_1};
    ReadTransaction rt_2{db_2};
    CHECK(compare_groups(rt_1, rt_2, test_context.logger));
}


// This test connects a sync client to the realm cloud service using a SSL
// connection. The purpose of the test is to check that the server's SSL
// certificate is accepted by the client.  The client will connect with an
// invalid token and get an error code back.  The check is that the error is
// not rejected certificate.  The test should be disabled under normal
// circumstances since it requires network access and cloud availability. The
// test might be enabled during testing of SSL functionality.
TEST_IF(Sync_SSL_Certificates, false)
{
    TEST_CLIENT_DB(db);

    const char* server_address[] = {
        "morten-krogh.us1.cloud.realm.io",
        "fantastic-cotton-shoes.us1.cloud.realm.io",
        "www.realm.io",
        "www.yahoo.com",
        "www.nytimes.com",
        "www.ibm.com",
        "www.ssllabs.com",
    };

    size_t num_servers = sizeof(server_address) / sizeof(server_address[0]);

    util::Logger& logger = test_context.logger;
    util::PrefixLogger client_logger("Client: ", logger);

    for (size_t i = 0; i < num_servers; ++i) {
        Client::Config client_config;
        client_config.logger = &client_logger;
        client_config.reconnect_mode = ReconnectMode::testing;
        Client client(client_config);

        ThreadWrapper client_thread;
        client_thread.start([&] {
            client.run();
        });

        Session::Config session_config;
        session_config.server_address = server_address[i];
        session_config.server_port = 443;
        session_config.realm_identifier = "/anything";
        session_config.protocol_envelope = ProtocolEnvelope::realms;

        // Invalid token for the cloud.
        session_config.signed_user_token = g_signed_test_user_token;

        Session session{client, db, std::move(session_config)};

        auto listener = [&](ConnectionState state, const Session::ErrorInfo* error_info) {
            if (state == ConnectionState::disconnected) {
                CHECK(error_info);
                client_logger.debug(
                    "State change: disconnected, error_code = %1, is_fatal = %2, detailed_message = %3",
                    error_info->error_code, error_info->is_fatal, error_info->detailed_message);
                // We expect to get through the SSL handshake but will hit an error due to the wrong token.
                CHECK_NOT_EQUAL(error_info->error_code, Client::Error::ssl_server_cert_rejected);
                client.stop();
            }
        };

        session.set_connection_state_change_listener(listener);
        session.bind();

        session.wait_for_download_complete_or_client_stopped();
        client.stop();
        client_thread.join();
    }
}


// Testing the custom authorization header name.  The sync protocol does not
// currently use the HTTP Authorization header, so the test is to watch the
// logs and see that the client use the right header name. Proxies and the sync
// server HTTP api use the Authorization header.
TEST(Sync_AuthorizationHeaderName)
{
    TEST_DIR(dir);
    TEST_CLIENT_DB(db);

    ClientServerFixture::Config config;
    config.authorization_header_name = "X-Alternative-Name";
    ClientServerFixture fixture(dir, test_context, config);
    fixture.start();

    Session::Config session_config;
    session_config.authorization_header_name = config.authorization_header_name;

    std::map<std::string, std::string> custom_http_headers;
    custom_http_headers["Header-Name-1"] = "Header-Value-1";
    custom_http_headers["Header-Name-2"] = "Header-Value-2";
    session_config.custom_http_headers = std::move(custom_http_headers);
    Session session = fixture.make_session(db, std::move(session_config));
    fixture.bind_session(session, "/test");

    session.wait_for_download_complete_or_client_stopped();
}


TEST(Sync_BadChangeset)
{
    TEST_DIR(dir);
    TEST_CLIENT_DB(db);

    bool did_fail = false;
    {
        ClientServerFixture::Config config;
        config.disable_upload_compaction = true;
        ClientServerFixture fixture(dir, test_context, config);
        fixture.start();

        {
            Session session = fixture.make_bound_session(db);
            session.wait_for_download_complete_or_client_stopped();
        }

        {
            WriteTransaction wt(db);
            TableRef table = wt.add_table("class_Foo");
            table->add_column(type_Int, "i");
            table->create_object().set_all(123);
            const ChangesetEncoder::Buffer& buffer = get_replication(db).get_instruction_encoder().buffer();
            char bad_instruction = 0x3e;
            const_cast<ChangesetEncoder::Buffer&>(buffer).append(&bad_instruction, 1);
            wt.commit();
        }

        auto listener = [&](ConnectionState state, const Session::ErrorInfo* error_info) {
            if (state != ConnectionState::disconnected)
                return;
            REALM_ASSERT(error_info);
            std::error_code ec = error_info->error_code;
            bool is_fatal = error_info->is_fatal;
            CHECK_EQUAL(sync::ProtocolError::bad_changeset, ec);
            CHECK(is_fatal);
            fixture.stop();
            did_fail = true;
        };

        Session session = fixture.make_session(db);
        session.set_connection_state_change_listener(listener);
        fixture.bind_session(session, "/test");

        session.wait_for_upload_complete_or_client_stopped();
        session.wait_for_download_complete_or_client_stopped();
    }
    CHECK(did_fail);
}


namespace issue2104 {

class IntegrationReporter : public _impl::ServerHistory::IntegrationReporter {
public:
    void on_integration_session_begin() override {}

    void on_changeset_integrated(std::size_t) override {}

    void on_changesets_merged(long) override {}
};

class ServerHistoryContext : public _impl::ServerHistory::Context {
public:
    ServerHistoryContext()
        : m_transformer{make_transformer()}
    {
    }

    std::mt19937_64& server_history_get_random() noexcept override
    {
        return m_random;
    }

    sync::Transformer& get_transformer() override
    {
        return *m_transformer;
    }

    util::Buffer<char>& get_transform_buffer() override
    {
        return m_transform_buffer;
    }

    IntegrationReporter& get_integration_reporter() override
    {
        return m_integration_reporter;
    }

private:
    std::mt19937_64 m_random;
    std::unique_ptr<sync::Transformer> m_transformer;
    util::Buffer<char> m_transform_buffer;
    IntegrationReporter m_integration_reporter;
};

} // namespace issue2104

// This test reproduces a slow merge seen in issue 2104.
// The test uses a user supplied Realm and a changeset
// from a client.
// The test uses a user supplied Realm that is very large
// and not kept in the repo. The realm has checksum 3693867489.
//
// This test might be modified to avoid having a large Realm
// (96 MB uncompressed) in the repo.
TEST_IF(Sync_Issue2104, false)
{
    TEST_DIR(dir);

    util::Logger& logger = test_context.logger;

    // Save a snapshot of the server Realm file.
    std::string realm_path = "issue_2104_server.realm";
    std::string realm_path_copy = util::File::resolve("issue_2104.realm", dir);
    util::File::copy(realm_path, realm_path_copy);

    std::string changeset_hex = "3F 00 07 41 42 43 44 61 74 61 3F 01 02 69 64 3F 02 09 41 6C 69 67 6E 6D 65 6E 74 3F "
                                "03 12 42 65 68 61 76 69 6F 72 4F 63 63 75 72 72 65 6E 63 65 3F 04 0D 42 65 68 61 76 "
                                "69 6F 72 50 68 61 73 65 3F 05 09 43 6F 6C 6C 65 63 74 6F 72 3F 06 09 43 72 69 74 65 "
                                "72 69 6F 6E 3F 07 07 46 65 61 74 75 72 65 3F 08 12 49 6E 73 74 72 75 63 74 69 6F 6E "
                                "61 6C 54 72 69 61 6C 3F 09 14 4D 65 61 73 75 72 65 6D 65 6E 74 50 72 6F 63 65 64 75 "
                                "72 65 3F 0A 07 4D 65 73 73 61 67 65 3F 0B 04 4E 6F 74 65 3F 0C 16 4F 6E 62 6F 61 72 "
                                "64 69 6E 67 54 6F 75 72 50 72 6F 67 72 65 73 73 3F 0D 05 50 68 61 73 65 3F 0E 07 50 "
                                "72 6F 67 72 61 6D 3F 0F 0C 50 72 6F 67 72 61 6D 47 72 6F 75 70 3F 10 0A 50 72 6F 67 "
                                "72 61 6D 52 75 6E 3F 11 0F 50 72 6F 67 72 61 6D 54 65 6D 70 6C 61 74 65 3F 12 0B 52 "
                                "65 61 6C 6D 53 74 72 69 6E 67 3F 13 0B 53 65 73 73 69 6F 6E 4E 6F 74 65 3F 14 07 53 "
                                "74 75 64 65 6E 74 3F 15 06 54 61 72 67 65 74 3F 16 0E 54 61 72 67 65 74 54 65 6D 70 "
                                "6C 61 74 65 3F 17 04 54 61 73 6B 3F 18 05 54 6F 6B 65 6E 3F 19 04 55 73 65 72 3F 1A "
                                "07 5F 5F 43 6C 61 73 73 3F 1B 04 6E 61 6D 65 3F 1C 0C 5F 5F 50 65 72 6D 69 73 73 69 "
                                "6F 6E 3F 1D 07 5F 5F 52 65 61 6C 6D 3F 1E 06 5F 5F 52 6F 6C 65 3F 1F 06 5F 5F 55 73 "
                                "65 72 3F 20 09 63 72 65 61 74 65 64 41 74 3F 21 0A 6D 6F 64 69 66 69 65 64 41 74 3F "
                                "22 09 63 72 65 61 74 65 64 42 79 3F 23 0A 6D 6F 64 69 66 69 65 64 42 79 3F 24 07 70 "
                                "72 6F 67 72 61 6D 3F 25 04 64 61 74 65 3F 26 0A 61 6E 74 65 63 65 64 65 6E 74 3F 27 "
                                "08 62 65 68 61 76 69 6F 72 3F 28 0B 63 6F 6E 73 65 71 75 65 6E 63 65 3F 29 07 73 65 "
                                "74 74 69 6E 67 3F 2A 04 6E 6F 74 65 3F 2B 08 63 61 74 65 67 6F 72 79 3F 2C 05 6C 65 "
                                "76 65 6C 3F 2D 0A 6F 63 63 75 72 72 65 64 41 74 3F 2E 05 70 68 61 73 65 3F 2F 08 64 "
                                "75 72 61 74 69 6F 6E 3F 30 07 6D 61 72 6B 52 61 77 3F 31 09 73 68 6F 72 74 4E 61 6D "
                                "65 3F 32 0A 64 65 66 69 6E 69 74 69 6F 6E 3F 33 06 74 61 72 67 65 74 3F 34 08 74 65 "
                                "6D 70 6C 61 74 65 3F 35 0D 6C 61 62 65 6C 4F 76 65 72 72 69 64 65 3F 36 08 62 61 73 "
                                "65 6C 69 6E 65 3F 37 13 63 6F 6C 6C 65 63 74 69 6F 6E 46 72 65 71 75 65 6E 63 79 3F "
                                "38 0E 61 64 64 69 74 69 6F 6E 61 6C 49 6E 66 6F 3F 39 0D 64 61 79 73 54 6F 49 6E 63 "
                                "6C 75 64 65 3F 3A 0D 64 61 79 73 54 6F 45 78 63 6C 75 64 65 3F 3B 07 74 79 70 65 52 "
                                "61 77 3F 3C 09 66 72 65 71 75 65 6E 63 79 3F 3D 08 69 6E 74 65 72 76 61 6C 3F 3E 0E "
                                "70 6F 69 6E 74 73 41 6E 61 6C 79 7A 65 64 3F 3F 0D 6D 69 6E 50 65 72 63 65 6E 74 61 "
                                "67 65 3F C0 00 04 63 6F 64 65 3F C1 00 06 74 65 61 6D 49 64 3F C2 00 03 75 72 6C 3F "
                                "C3 00 07 73 65 63 74 69 6F 6E 3F C4 00 11 63 72 69 74 65 72 69 6F 6E 44 65 66 61 75 "
                                "6C 74 73 3F C5 00 04 74 61 73 6B 3F C6 00 09 72 65 73 75 6C 74 52 61 77 3F C7 00 09 "
                                "70 72 6F 6D 70 74 52 61 77 3F C8 00 04 74 65 78 74 3F C9 00 0A 70 72 6F 67 72 61 6D "
                                "52 75 6E 3F CA 00 09 72 65 63 69 70 69 65 6E 74 3F CB 00 04 62 6F 64 79 3F CC 00 06 "
                                "61 63 74 69 76 65 3F CD 00 0D 62 65 68 61 76 69 6F 72 50 68 61 73 65 3F CE 00 03 64 "
                                "61 79 3F CF 00 06 74 6F 75 72 49 64 3F D0 00 08 63 6F 6D 70 6C 65 74 65 3F D1 00 05 "
                                "73 74 61 72 74 3F D2 00 03 65 6E 64 3F D3 00 05 74 69 74 6C 65 3F D4 00 12 70 72 6F "
                                "67 72 61 6D 44 65 73 63 72 69 70 74 69 6F 6E 3F D5 00 09 63 72 69 74 65 72 69 6F 6E "
                                "3F D6 00 0E 63 72 69 74 65 72 69 6F 6E 52 75 6C 65 73 3F D7 00 03 73 74 6F 3F D8 00 "
                                "03 6C 74 6F 3F D9 00 18 72 65 69 6E 66 6F 72 63 65 6D 65 6E 74 53 63 68 65 64 75 6C "
                                "65 52 61 77 3F DA 00 0D 72 65 69 6E 66 6F 72 63 65 6D 65 6E 74 3F DB 00 11 72 65 69 "
                                "6E 66 6F 72 63 65 6D 65 6E 74 54 79 70 65 3F DC 00 16 64 69 73 63 72 69 6D 69 6E 61 "
                                "74 69 76 65 53 74 69 6D 75 6C 75 73 3F DD 00 07 74 61 72 67 65 74 73 3F DE 00 05 74 "
                                "61 73 6B 73 3F DF 00 0A 74 61 73 6B 53 74 61 74 65 73 3F E0 00 0C 74 6F 74 61 6C 49 "
                                "54 43 6F 75 6E 74 3F E1 00 0A 73 61 6D 70 6C 65 54 69 6D 65 3F E2 00 10 64 65 66 61 "
                                "75 6C 74 52 65 73 75 6C 74 52 61 77 3F E3 00 0F 76 61 72 69 61 62 6C 65 49 54 43 6F "
                                "75 6E 74 3F E4 00 09 65 72 72 6F 72 6C 65 73 73 3F E5 00 0C 6D 69 6E 41 74 74 65 6D "
                                "70 74 65 64 3F E6 00 10 64 65 66 61 75 6C 74 4D 65 74 68 6F 64 52 61 77 3F E7 00 0A "
                                "73 65 74 74 69 6E 67 52 61 77 3F E8 00 07 73 74 75 64 65 6E 74 3F E9 00 0F 6D 61 73 "
                                "74 65 72 65 64 54 61 72 67 65 74 73 3F EA 00 0D 66 75 74 75 72 65 54 61 72 67 65 74 "
                                "73 3F EB 00 05 67 72 6F 75 70 3F EC 00 06 6C 6F 63 6B 65 64 3F ED 00 0E 6C 61 73 74 "
                                "44 65 63 69 73 69 6F 6E 41 74 3F EE 00 08 61 72 63 68 69 76 65 64 3F EF 00 0E 64 61 "
                                "74 65 73 54 6F 49 6E 63 6C 75 64 65 3F F0 00 0E 64 61 74 65 73 54 6F 45 78 63 6C 75 "
                                "64 65 3F F1 00 09 64 72 61 77 65 72 52 61 77 3F F2 00 0B 63 6F 6D 70 6C 65 74 65 64 "
                                "41 74 3F F3 00 03 49 54 73 3F F4 00 0C 64 69 73 70 6C 61 79 4F 72 64 65 72 3F F5 00 "
                                "0F 63 6F 72 72 65 63 74 4F 76 65 72 72 69 64 65 3F F6 00 11 61 74 74 65 6D 70 74 65 "
                                "64 4F 76 65 72 72 69 64 65 3F F7 00 09 6D 65 74 68 6F 64 52 61 77 3F F8 00 08 73 74 "
                                "61 74 65 52 61 77 3F F9 00 0C 70 6F 69 6E 74 54 79 70 65 52 61 77 3F FA 00 09 61 6C "
                                "69 67 6E 6D 65 6E 74 3F FB 00 08 65 78 61 6D 70 6C 65 73 3F FC 00 0E 67 65 6E 65 72 "
                                "61 6C 69 7A 61 74 69 6F 6E 3F FD 00 09 6D 61 74 65 72 69 61 6C 73 3F FE 00 09 6F 62 "
                                "6A 65 63 74 69 76 65 3F FF 00 0F 72 65 63 6F 6D 6D 65 6E 64 61 74 69 6F 6E 73 3F 80 "
                                "01 08 73 74 69 6D 75 6C 75 73 3F 81 01 0B 74 61 72 67 65 74 4E 6F 74 65 73 3F 82 01 "
                                "11 74 65 61 63 68 69 6E 67 50 72 6F 63 65 64 75 72 65 3F 83 01 0A 76 62 6D 61 70 70 "
                                "54 61 67 73 3F 84 01 08 61 66 6C 73 54 61 67 73 3F 85 01 09 6E 79 73 6C 73 54 61 67 "
                                "73 3F 86 01 06 64 6F 6D 61 69 6E 3F 87 01 04 67 6F 61 6C 3F 88 01 07 73 75 62 6A 65 "
                                "63 74 3F 89 01 0B 6A 6F 62 43 61 74 65 67 6F 72 79 3F 8A 01 13 70 72 6F 6D 70 74 69 "
                                "6E 67 50 72 6F 63 65 64 75 72 65 73 3F 8B 01 10 70 72 65 73 63 68 6F 6F 6C 4D 61 73 "
                                "74 65 72 79 3F 8C 01 0C 61 62 6C 6C 73 4D 61 73 74 65 72 79 3F 8D 01 0D 64 61 74 61 "
                                "52 65 63 6F 72 64 69 6E 67 3F 8E 01 0F 65 72 72 6F 72 43 6F 72 72 65 63 74 69 6F 6E "
                                "3F 8F 01 0B 73 74 72 69 6E 67 56 61 6C 75 65 3F 90 01 06 63 6C 69 65 6E 74 3F 91 01 "
                                "09 74 68 65 72 61 70 69 73 74 3F 92 01 0B 72 65 69 6E 66 6F 72 63 65 72 73 3F 93 01 "
                                "05 6E 6F 74 65 73 3F 94 01 0F 74 61 72 67 65 74 42 65 68 61 76 69 6F 72 73 3F 95 01 "
                                "08 67 6F 61 6C 73 4D 65 74 3F 96 01 0D 74 79 70 65 4F 66 53 65 72 76 69 63 65 3F 97 "
                                "01 0D 70 65 6F 70 6C 65 50 72 65 73 65 6E 74 3F 98 01 08 6C 61 74 69 74 75 64 65 3F "
                                "99 01 09 6C 6F 6E 67 69 74 75 64 65 3F 9A 01 06 61 6C 65 72 74 73 3F 9B 01 03 65 69 "
                                "6E 3F 9C 01 03 64 6F 62 3F 9D 01 0F 70 72 69 6D 61 72 79 47 75 61 72 64 69 61 6E 3F "
                                "9E 01 11 73 65 63 6F 6E 64 61 72 79 47 75 61 72 64 69 61 6E 3F 9F 01 08 69 6D 61 67 "
                                "65 55 72 6C 3F A0 01 0B 64 65 61 63 74 69 76 61 74 65 64 3F A1 01 11 74 61 72 67 65 "
                                "74 44 65 73 63 72 69 70 74 69 6F 6E 3F A2 01 08 6D 61 73 74 65 72 65 64 3F A3 01 0F "
                                "74 61 73 6B 44 65 73 63 72 69 70 74 69 6F 6E 3F A4 01 09 65 78 70 69 72 65 73 41 74 "
                                "3F A5 01 0C 63 6F 6C 6C 65 63 74 6F 72 49 64 73 3F A6 01 08 73 74 75 64 65 6E 74 73 "
                                "3F A7 01 12 6F 6E 62 6F 61 72 64 69 6E 67 50 72 6F 67 72 65 73 73 3F A8 01 05 65 6D "
                                "61 69 6C 3F A9 01 05 70 68 6F 6E 65 3F AA 01 07 72 6F 6C 65 52 61 77 3F AB 01 08 73 "
                                "65 74 74 69 6E 67 73 3F AC 01 0B 70 65 72 6D 69 73 73 69 6F 6E 73 3F AD 01 04 72 6F "
                                "6C 65 3F AE 01 07 63 61 6E 52 65 61 64 3F AF 01 09 63 61 6E 55 70 64 61 74 65 3F B0 "
                                "01 09 63 61 6E 44 65 6C 65 74 65 3F B1 01 11 63 61 6E 53 65 74 50 65 72 6D 69 73 73 "
                                "69 6F 6E 73 3F B2 01 08 63 61 6E 51 75 65 72 79 3F B3 01 09 63 61 6E 43 72 65 61 74 "
                                "65 3F B4 01 0F 63 61 6E 4D 6F 64 69 66 79 53 63 68 65 6D 61 3F B5 01 07 6D 65 6D 62 "
                                "65 72 73 02 00 01 01 02 00 02 02 01 01 02 00 02 03 01 01 02 00 02 04 01 01 02 00 02 "
                                "05 01 01 02 01 02 06 01 01 02 01 02 07 01 01 02 00 02 08 01 01 02 00 02 09 01 01 02 "
                                "00 02 0A 01 01 02 00 02 0B 01 01 02 00 02 0C 01 01 02 00 02 0D 01 01 02 00 02 0E 01 "
                                "01 02 00 02 0F 01 01 02 00 02 10 01 01 02 00 02 11 01 01 02 00 02 12 00 02 13 01 01 "
                                "02 00 02 14 01 01 02 00 02 15 01 01 02 00 02 16 01 01 02 00 02 17 01 01 02 00 02 18 "
                                "01 01 02 00 02 19 01 01 02 00 02 1A 01 1B 02 00 02 1C 00 02 1D 01 01 00 00 02 1E 01 "
                                "1B 02 00 02 1F 01 01 02 00 00 00 0B 20 08 00 00 0B 21 08 00 00 0B 22 0C 00 19 0B 23 "
                                "0C 00 19 0B 24 0C 00 0E 0B 25 08 00 00 0B 26 02 00 01 0B 27 02 00 01 0B 28 02 00 01 "
                                "0B 29 02 00 01 0B 2A 02 00 01 00 02 0B 20 08 00 00 0B 21 08 00 00 0B 2B 02 00 01 0B "
                                "2C 02 00 01 00 03 0B 20 08 00 00 0B 21 08 00 00 0B 2D 08 00 00 0B 22 0C 00 19 0B 23 "
                                "0C 00 19 0B 2E 0C 00 04 0B 2F 0A 00 01 0B 30 02 00 00 00 04 0B 20 08 00 00 0B 21 08 "
                                "00 00 0B 22 0C 00 19 0B 23 0C 00 19 0B 1B 02 00 01 0B 31 02 00 01 0B 32 02 00 01 0B "
                                "33 02 00 01 0B 24 0C 00 0E 0B 34 0C 00 11 0B 35 02 00 01 0B 36 02 00 01 0B 37 02 00 "
                                "01 0B 38 02 00 01 0B 39 08 02 00 0B 3A 08 02 00 0B 3B 02 00 00 00 05 0B 2F 0C 00 04 "
                                "0B 3C 0C 00 04 0B 3D 0C 00 10 00 06 0B 3E 00 00 00 0B 3F 0A 00 00 00 07 0B C0 00 02 "
                                "00 00 0B C1 00 02 00 01 0B C2 00 02 00 01 0B C3 00 02 00 01 0B C4 00 0D 00 06 00 08 "
                                "0B 20 08 00 00 0B 21 08 00 00 0B 22 0C 00 19 0B 23 0C 00 19 0B C5 00 0C 00 17 0B 33 "
                                "0C 00 15 0B C6 00 02 00 00 0B C7 00 02 00 00 00 09 0B C8 00 02 00 01 00 0A 0B 20 08 "
                                "00 00 0B 21 08 00 00 0B 22 0C 00 19 0B 23 0C 00 19 0B C9 00 0C 00 10 0B 24 0C 00 0E "
                                "0B CA 00 0C 00 19 0B CB 00 02 00 00 0B CC 00 01 00 00 0B 3B 02 00 00 00 0B 0B 20 08 "
                                "00 00 0B 21 08 00 00 0B 22 0C 00 19 0B 23 0C 00 19 0B CD 00 0C 00 04 0B CE 00 08 00 "
                                "00 0B CB 00 02 00 00 0B CC 00 01 00 00 00 0C 0B CF 00 02 00 00 0B D0 00 01 00 00 00 "
                                "0D 0B 20 08 00 00 0B 21 08 00 00 0B 22 0C 00 19 0B 23 0C 00 19 0B 24 0C 00 0E 0B D1 "
                                "00 08 00 00 0B D2 00 08 00 01 0B D3 00 02 00 01 0B D4 00 02 00 01 0B 32 02 00 01 0B "
                                "D5 00 02 00 01 0B D6 00 0D 00 06 0B D7 00 02 00 01 0B D8 00 02 00 01 0B 36 02 00 01 "
                                "0B 37 02 00 01 0B 35 02 00 01 0B 38 02 00 01 0B C7 00 02 00 00 0B D9 00 02 00 00 0B "
                                "DA 00 00 00 01 0B DB 00 02 00 01 0B DC 00 02 00 01 0B DD 00 0D 00 15 0B DE 00 0D 00 "
                                "17 0B DF 00 0D 00 12 0B E0 00 00 00 01 0B E1 00 0A 00 01 0B E2 00 02 00 00 0B E3 00 "
                                "01 00 00 0B E4 00 01 00 00 0B E5 00 00 00 00 0B E6 00 02 00 00 0B E7 00 02 00 00 00 "
                                "0E 0B 20 08 00 00 0B 21 08 00 00 0B 22 0C 00 19 0B 23 0C 00 19 0B E8 00 0C 00 14 0B "
                                "E9 00 0D 00 15 0B EA 00 0D 00 15 0B EB 00 0C 00 0F 0B EC 00 01 00 00 0B ED 00 08 00 "
                                "01 0B EE 00 01 00 00 0B 34 0C 00 11 0B EF 00 08 02 00 0B F0 00 08 02 00 0B F1 00 02 "
                                "00 00 00 0F 0B 20 08 00 00 0B 21 08 00 00 0B 22 0C 00 19 0B 23 0C 00 19 00 10 0B 20 "
                                "08 00 00 0B 21 08 00 00 0B F2 00 08 00 01 0B 22 0C 00 19 0B 23 0C 00 19 0B F3 00 0D "
                                "00 08 0B CC 00 01 00 00 0B F4 00 00 00 01 0B F5 00 00 00 01 0B F6 00 00 00 01 0B F7 "
                                "00 02 00 00 0B F8 00 02 00 00 0B F9 00 02 00 00 0B 2E 0C 00 0D 0B 2A 02 00 01 0B EE "
                                "00 01 00 00 00 11 0B 20 08 00 00 0B 21 08 00 00 0B FA 00 0C 00 02 0B 36 02 00 01 0B "
                                "FB 00 02 00 01 0B EA 00 0D 00 16 0B FC 00 02 00 01 0B FD 00 02 00 01 0B 1B 02 00 01 "
                                "0B FE 00 02 00 01 0B FF 00 02 00 01 0B 80 01 02 00 01 0B 81 01 02 00 01 0B 82 01 02 "
                                "00 01 0B 32 02 00 01 0B 83 01 02 00 01 0B 84 01 02 00 01 0B 85 01 02 00 01 0B 86 01 "
                                "02 00 01 0B 87 01 02 00 01 0B 88 01 02 00 01 0B 89 01 02 00 01 0B D8 00 02 00 01 0B "
                                "8A 01 02 00 01 0B 8B 01 02 00 01 0B 8C 01 02 00 01 0B 8D 01 02 00 01 0B 8E 01 02 00 "
                                "01 0B D5 00 0D 00 06 00 12 0B 8F 01 02 00 00 00 13 0B 20 08 00 00 0B 21 08 00 00 0B "
                                "22 0C 00 19 0B 23 0C 00 19 0B 90 01 0C 00 14 0B 91 01 02 00 01 0B 92 01 02 00 01 0B "
                                "93 01 02 00 01 0B 94 01 02 00 01 0B 95 01 02 00 01 0B 96 01 02 00 01 0B 97 01 02 00 "
                                "01 0B D1 00 08 00 01 0B D2 00 08 00 01 0B 98 01 0A 00 01 0B 99 01 0A 00 01 00 14 0B "
                                "20 08 00 00 0B 21 08 00 00 0B 1B 02 00 01 0B 9A 01 02 00 01 0B 9B 01 02 00 01 0B 9C "
                                "01 08 00 01 0B 9D 01 0C 00 19 0B 9E 01 0C 00 19 0B 9F 01 02 00 01 0B A0 01 01 00 00 "
                                "00 15 0B 20 08 00 00 0B 21 08 00 00 0B 22 0C 00 19 0B 23 0C 00 19 0B A1 01 02 00 01 "
                                "0B A2 01 08 00 01 00 16 0B 20 08 00 00 0B 21 08 00 00 0B A1 01 02 00 01 00 17 0B 20 "
                                "08 00 00 0B 21 08 00 00 0B 22 0C 00 19 0B 23 0C 00 19 0B A3 01 02 00 01 0B F8 00 02 "
                                "00 00 00 18 0B A4 01 08 00 00 0B CB 00 02 00 01 00 19 0B 20 08 00 00 0B 21 08 00 00 "
                                "0B A5 01 02 02 00 0B A6 01 0D 00 14 0B A7 01 0D 00 0C 0B 1B 02 00 01 0B A8 01 02 00 "
                                "01 0B A9 01 02 00 01 0B 9F 01 02 00 01 0B AA 01 02 00 00 0B AB 01 02 02 00 00 1A 0B "
                                "AC 01 0D 00 1C 00 1C 0B AD 01 0C 00 1E 0B AE 01 01 00 00 0B AF 01 01 00 00 0B B0 01 "
                                "01 00 00 0B B1 01 01 00 00 0B B2 01 01 00 00 0B B3 01 01 00 00 0B B4 01 01 00 00 00 "
                                "1D 0B AC 01 0D 00 1C 00 1E 0B B5 01 0D 00 1F 00 1F 0B AD 01 0C 00 1E";

    std::vector<char> changeset_vec;
    {
        std::istringstream in{changeset_hex};
        int n;
        in >> std::hex >> n;
        while (in) {
            REALM_ASSERT(n >= 0 && n <= 255);
            changeset_vec.push_back(n);
            in >> std::hex >> n;
        }
    }

    BinaryData changeset_bin{changeset_vec.data(), changeset_vec.size()};

    file_ident_type client_file_ident = 51;
    timestamp_type origin_timestamp = 103573722140;
    file_ident_type origin_file_ident = 0;
    version_type client_version = 2;
    version_type last_integrated_server_version = 0;
    UploadCursor upload_cursor{client_version, last_integrated_server_version};

    _impl::ServerHistory::IntegratableChangeset integratable_changeset{
        client_file_ident, origin_timestamp, origin_file_ident, upload_cursor, changeset_bin};

    _impl::ServerHistory::IntegratableChangesets integratable_changesets;
    integratable_changesets[client_file_ident].changesets.push_back(integratable_changeset);

    issue2104::ServerHistoryContext history_context;
    _impl::ServerHistory::DummyCompactionControl compaction_control;
    _impl::ServerHistory history{history_context, compaction_control};
    DBRef db = DB::create(history, realm_path_copy);

    VersionInfo version_info;
    bool backup_whole_realm;
    _impl::ServerHistory::IntegrationResult result;
    history.integrate_client_changesets(integratable_changesets, version_info, backup_whole_realm, result, logger);
}


TEST(Sync_ConcurrentHttpDeleteAndHttpCompact)
{
    TEST_DIR(server_dir);
    ClientServerFixture::Config config;
    ClientServerFixture fixture(server_dir, test_context, config);
    fixture.start();

    for (int i = 0; i < 64; ++i) {
        std::string virt_path = "/test";
        {
            TEST_CLIENT_DB(db);
            Session session = fixture.make_bound_session(db, virt_path);
            session.wait_for_download_complete_or_client_stopped();
            session.detach();
            fixture.wait_for_session_terminations_or_client_stopped();
        }
        auto run_delete = [&] {
            CHECK_EQUAL(util::HTTPStatus::Ok, fixture.send_http_delete_request(virt_path));
        };
        auto run_compact = [&] {
            CHECK_EQUAL(util::HTTPStatus::Ok, fixture.send_http_compact_request());
        };
        ThreadWrapper delete_thread;
        ThreadWrapper compact_thread;
        delete_thread.start(run_delete);
        compact_thread.start(run_compact);
        delete_thread.join();
        compact_thread.join();
    }
}


TEST(Sync_RunServerWithoutPublicKey)
{
    TEST_CLIENT_DB(db);
    TEST_DIR(server_dir);
    ClientServerFixture::Config config;
    config.server_public_key_path = {};
    ClientServerFixture fixture(server_dir, test_context, config);
    fixture.start();

    // Server must accept an unsigned token when a public key is not passed to
    // it
    {
        Session session = fixture.make_bound_session(db, "/test", g_unsigned_test_user_token);
        session.wait_for_download_complete_or_client_stopped();
    }

    // Server must also accept a signed token when a public key is not passed to
    // it
    {
        Session session = fixture.make_bound_session(db, "/test");
        session.wait_for_download_complete_or_client_stopped();
    }
}


TEST(Sync_ServerSideEncryption)
{
    TEST_CLIENT_DB(db);
    {
        WriteTransaction wt(db);
        wt.add_table("class_Test");
        wt.commit();
    }

    TEST_DIR(server_dir);
    bool always_encrypt = true;
    std::string server_path;
    {
        ClientServerFixture::Config config;
        config.server_encryption_key = crypt_key_2(always_encrypt);
        ClientServerFixture fixture(server_dir, test_context, config);
        fixture.start();

        Session session = fixture.make_bound_session(db, "/test");
        session.wait_for_upload_complete_or_client_stopped();

        server_path = fixture.map_virtual_to_real_path("/test");
    }

    const char* encryption_key = crypt_key(always_encrypt);
    Group group{server_path, encryption_key};
    CHECK(group.has_table("class_Test"));
}


TEST(Sync_ServerSideEncryptionPlusCompact)
{
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    {
        WriteTransaction wt(db_1);
        wt.add_table("class_Test");
        wt.commit();
    }

    TEST_DIR(server_dir);
    ClientServerFixture::Config config;
    bool always_encrypt = true;
    config.server_encryption_key = crypt_key_2(always_encrypt);
    ClientServerFixture fixture(server_dir, test_context, config);
    fixture.start();

    {
        Session session = fixture.make_bound_session(db_1, "/test");
        session.wait_for_upload_complete_or_client_stopped();
    }

    // Send a HTTP request to the server to compact all Realms.
    CHECK_EQUAL(util::HTTPStatus::Ok, fixture.send_http_compact_request());

    {
        Session session = fixture.make_bound_session(db_2, "/test");
        session.wait_for_download_complete_or_client_stopped();
    }

    {
        auto rt = db_2->start_read();
        CHECK(rt->has_table("class_Test"));
    }
}

// This test calls row_for_object_id() for various object ids and tests that
// the right value is returned including that no assertions are hit.
TEST(Sync_RowForGlobalKey)
{
    TEST_CLIENT_DB(db);

    {
        WriteTransaction wt(db);
        TableRef table = wt.add_table("class_foo");
        table->add_column(type_Int, "i");
        wt.commit();
    }

    // Check that various object_ids are not in the table.
    {
        ReadTransaction rt(db);
        ConstTableRef table = rt.get_table("class_foo");
        CHECK(table);

        // Default constructed GlobalKey
        {
            GlobalKey object_id;
            auto row_ndx = table->get_objkey(object_id);
            CHECK_NOT(row_ndx);
        }

        // GlobalKey with small lo and hi values
        {
            GlobalKey object_id{12, 24};
            auto row_ndx = table->get_objkey(object_id);
            CHECK_NOT(row_ndx);
        }

        // GlobalKey with lo and hi values past the 32 bit limit.
        {
            GlobalKey object_id{uint_fast64_t(1) << 50, uint_fast64_t(1) << 52};
            auto row_ndx = table->get_objkey(object_id);
            CHECK_NOT(row_ndx);
        }
    }
}


TEST(Sync_LogCompaction_EraseObject_LinkList)
{
    TEST_DIR(dir);
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);
    ClientServerFixture::Config config;

    // Log comapction is true by default, but we emphasize it.
    config.disable_upload_compaction = false;
    config.disable_download_compaction = false;

    ClientServerFixture fixture(dir, test_context, config);
    fixture.start();

    {
        WriteTransaction wt{db_1};

        TableRef table_source = wt.add_table("class_source");
        TableRef table_target = wt.add_table("class_target");
        auto col_key = table_source->add_column_list(*table_target, "target_link");

        auto k0 = table_target->create_object().get_key();
        auto k1 = table_target->create_object().get_key();

        auto ll = table_source->create_object().get_linklist_ptr(col_key);
        ll->add(k0);
        ll->add(k1);
        CHECK_EQUAL(ll->size(), 2);
        wt.commit();
    }

    {
        Session session_1 = fixture.make_bound_session(db_1);
        Session session_2 = fixture.make_bound_session(db_2);

        session_1.wait_for_upload_complete_or_client_stopped();
        session_2.wait_for_download_complete_or_client_stopped();
    }

    {
        WriteTransaction wt{db_1};

        TableRef table_source = wt.get_table("class_source");
        TableRef table_target = wt.get_table("class_target");

        CHECK_EQUAL(table_source->size(), 1);
        CHECK_EQUAL(table_target->size(), 2);

        table_target->get_object(1).remove();
        table_target->get_object(0).remove();

        table_source->get_object(0).remove();
        wt.commit();
    }

    {
        WriteTransaction wt{db_2};

        TableRef table_source = wt.get_table("class_source");
        TableRef table_target = wt.get_table("class_target");
        auto col_key = table_source->get_column_key("target_link");

        CHECK_EQUAL(table_source->size(), 1);
        CHECK_EQUAL(table_target->size(), 2);

        auto k0 = table_target->begin()->get_key();

        auto ll = table_source->get_object(0).get_linklist_ptr(col_key);
        ll->add(k0);
        wt.commit();
    }

    {
        Session session_1 = fixture.make_bound_session(db_1);
        session_1.wait_for_upload_complete_or_client_stopped();
    }

    {
        Session session_2 = fixture.make_bound_session(db_2);
        session_2.wait_for_upload_complete_or_client_stopped();
        session_2.wait_for_download_complete_or_client_stopped();
    }

    {
        ReadTransaction rt{db_2};

        ConstTableRef table_source = rt.get_group().get_table("class_source");
        ConstTableRef table_target = rt.get_group().get_table("class_target");

        CHECK_EQUAL(table_source->size(), 0);
        CHECK_EQUAL(table_target->size(), 0);
    }
}


TEST(Sync_ClientFileBlacklisting)
{
    TEST_CLIENT_DB(db);
    TEST_DIR(server_dir);

    // Get a client file identifier allocated for the client-side file
    {
        ClientServerFixture fixture(server_dir, test_context);
        fixture.start();
        Session session = fixture.make_bound_session(db, "/test");
        session.wait_for_download_complete_or_client_stopped();
    }
    file_ident_type client_file_ident;
    {
        version_type client_version;
        SaltedFileIdent client_file_ident_2;
        SyncProgress progress;
        get_history(db).get_status(client_version, client_file_ident_2, progress);
        client_file_ident = client_file_ident_2.ident;
    }

    // Check that blacklisting works
    MockMetrics metrics;
    bool did_fail = false;
    {
        ClientServerFixture::Config config;
        config.server_metrics = &metrics;
        config.client_file_blacklists["/test"].push_back(client_file_ident);
        ClientServerFixture fixture(server_dir, test_context, config);
        fixture.start();
        using ConnectionState = ConnectionState;
        using ErrorInfo = Session::ErrorInfo;
        auto listener = [&](ConnectionState state, const ErrorInfo* error_info) {
            if (state != ConnectionState::disconnected)
                return;
            REALM_ASSERT(error_info);
            std::error_code ec = error_info->error_code;
            bool is_fatal = error_info->is_fatal;
            CHECK_EQUAL(sync::ProtocolError::client_file_blacklisted, ec);
            CHECK(is_fatal);
            fixture.stop();
            did_fail = true;
        };
        Session session = fixture.make_session(db);
        session.set_connection_state_change_listener(listener);
        fixture.bind_session(session, "/test");
        session.wait_for_download_complete_or_client_stopped();
    }
    CHECK(did_fail);
    CHECK_EQUAL(1.0, metrics.sum_equal("blacklisted"));
}

// This test could trigger the assertion that the row_for_object_id cache is
// valid before the cache was properly invalidated in the case of a short
// circuited sync replicator.
TEST(Sync_CreateObjects_EraseObjects)
{
    TEST_DIR(dir);
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);
    ClientServerFixture fixture(dir, test_context);
    fixture.start();

    Session session_1 = fixture.make_bound_session(db_1);
    Session session_2 = fixture.make_bound_session(db_2);

    write_transaction_notifying_session(db_1, session_1, [](WriteTransaction& wt) {
        TableRef table = wt.add_table("class_persons");
        table->create_object();
        table->create_object();
    });
    session_1.wait_for_upload_complete_or_client_stopped();
    session_2.wait_for_download_complete_or_client_stopped();

    write_transaction_notifying_session(db_1, session_1, [&](WriteTransaction& wt) {
        TableRef table = wt.get_table("class_persons");
        CHECK_EQUAL(table->size(), 2);
        table->get_object(0).remove();
        table->get_object(0).remove();
    });
    session_1.wait_for_upload_complete_or_client_stopped();
    session_2.wait_for_download_complete_or_client_stopped();
}


TEST(Sync_CreateDeleteCreateTableWithPrimaryKey)
{
    TEST_DIR(dir);
    TEST_CLIENT_DB(db);
    ClientServerFixture fixture(dir, test_context);
    fixture.start();

    Session session = fixture.make_bound_session(db);

    write_transaction_notifying_session(db, session, [](WriteTransaction& wt) {
        TableRef table = wt.get_group().add_table_with_primary_key("class_t", type_Int, "pk");
        wt.get_group().remove_table(table->get_key());
        table = wt.get_group().add_table_with_primary_key("class_t", type_String, "pk");
    });
    session.wait_for_upload_complete_or_client_stopped();
    session.wait_for_download_complete_or_client_stopped();
}


TEST(Sync_ResumeAfterClientSideFailureToIntegrate)
{
    SHARED_GROUP_TEST_PATH(path_1);
    TEST_CLIENT_DB(db_2);

    // Verify that if a client fails to integrate a downloaded changeset, then
    // it will keep failing during future attempts. This test once failed due to
    // https://jira.mongodb.org/browse/RSYNC-48.

    TEST_DIR(dir);
    fixtures::ClientServerFixture fixture{dir, test_context};
    fixture.start();

    // Introduce a changeset into the server-side Realm
    {
        fixtures::RealmFixture realm{fixture, path_1, "/test"};
        realm.nonempty_transact();
        realm.wait_for_upload_complete_or_client_stopped();
    }

    // Launch a client with `simulate_integration_error` set to true, and make
    // it download that changeset. Then check that it fails at least two times.
    bool failed_once = false;
    bool failed_twice = false;
    using ConnectionState = ConnectionState;
    using ErrorInfo = Session::ErrorInfo;
    auto listener = [&](ConnectionState state, const ErrorInfo* error_info) {
        if (state != ConnectionState::disconnected)
            return;
        REALM_ASSERT(error_info);
        std::error_code ec = error_info->error_code;
        bool is_fatal = error_info->is_fatal;
        CHECK_EQUAL(Client::Error::bad_changeset, ec);
        CHECK(is_fatal);
        if (!failed_once) {
            failed_once = true;
            fixture.cancel_reconnect_delay();
        }
        else {
            failed_twice = true;
            fixture.stop();
        }
    };
    Session::Config config;
    config.simulate_integration_error = true;
    Session session = fixture.make_session(db_2, std::move(config));
    session.set_connection_state_change_listener(listener);
    fixture.bind_session(session, "/test");
    session.wait_for_download_complete_or_client_stopped();
    CHECK(failed_twice);
}

template <typename T>
T sequence_next()
{
    REALM_UNREACHABLE();
}

template <>
ObjectId sequence_next()
{
    return ObjectId::gen();
}

template <>
UUID sequence_next()
{
    union {
        struct {
            uint64_t upper;
            uint64_t lower;
        } ints;
        UUID::UUIDBytes bytes;
    } u;
    static uint64_t counter = test_util::random_int(0, 1000);
    u.ints.upper = ++counter;
    u.ints.lower = ++counter;
    return UUID{u.bytes};
}

template <>
Int sequence_next()
{
    static Int count = test_util::random_int(-1000, 1000);
    return ++count;
}

template <>
String sequence_next()
{
    static std::string str;
    static Int sequence = test_util::random_int(-1000, 1000);
    str = util::format("string sequence %1", ++sequence);
    return String(str);
}

NONCONCURRENT_TEST_TYPES(Sync_PrimaryKeyTypes, Int, String, ObjectId, UUID, util::Optional<Int>,
                         util::Optional<ObjectId>, util::Optional<UUID>)
{
    using underlying_type = typename util::RemoveOptional<TEST_TYPE>::type;
    constexpr bool is_optional = !std::is_same_v<underlying_type, TEST_TYPE>;
    DataType type = ColumnTypeTraits<TEST_TYPE>::id;

    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    TEST_DIR(dir);
    fixtures::ClientServerFixture fixture{dir, test_context};
    fixture.start();

    Session session_1 = fixture.make_session(db_1);
    Session session_2 = fixture.make_session(db_2);
    fixture.bind_session(session_1, "/test");
    fixture.bind_session(session_2, "/test");

    TEST_TYPE obj_1_id;
    TEST_TYPE obj_2_id;

    TEST_TYPE default_or_null{};
    if constexpr (std::is_same_v<TEST_TYPE, String>) {
        default_or_null = "";
    }
    if constexpr (is_optional) {
        CHECK(!default_or_null);
    }

    {
        WriteTransaction tr{db_1};
        auto table_1 = tr.get_group().add_table_with_primary_key("class_Table1", type, "id", is_optional);
        auto table_2 = tr.get_group().add_table_with_primary_key("class_Table2", type, "id", is_optional);
        table_1->add_column_list(type, "oids", is_optional);

        auto obj_1 = table_1->create_object_with_primary_key(sequence_next<underlying_type>());
        auto obj_2 = table_2->create_object_with_primary_key(sequence_next<underlying_type>());
        if constexpr (is_optional) {
            auto obj_3 = table_2->create_object_with_primary_key(default_or_null);
        }

        auto list = obj_1.template get_list<TEST_TYPE>("oids");
        obj_1_id = obj_1.template get<TEST_TYPE>("id");
        obj_2_id = obj_2.template get<TEST_TYPE>("id");
        list.insert(0, obj_2_id);
        list.insert(1, default_or_null);
        list.add(default_or_null);
        session_1.nonsync_transact_notify(tr.commit());
    }

    session_1.wait_for_upload_complete_or_client_stopped();
    session_2.wait_for_download_complete_or_client_stopped();

    {
        ReadTransaction tr{db_2};
        auto table_1 = tr.get_table("class_Table1");
        auto table_2 = tr.get_table("class_Table2");
        auto obj_1 = *table_1->begin();
        auto obj_2 = table_2->find_first(table_2->get_column_key("id"), obj_2_id);
        CHECK(obj_2);
        auto list = obj_1.get_list<TEST_TYPE>("oids");
        CHECK_EQUAL(obj_1.template get<TEST_TYPE>("id"), obj_1_id);
        CHECK_EQUAL(list.size(), 3);
        CHECK_NOT(list.is_null(0));
        CHECK_EQUAL(list.get(0), obj_2_id);
        CHECK_EQUAL(list.get(1), default_or_null);
        CHECK_EQUAL(list.get(2), default_or_null);
        if constexpr (is_optional) {
            auto obj_3 = table_2->find_first_null(table_2->get_column_key("id"));
            CHECK(obj_3);
            CHECK(list.is_null(1));
            CHECK(list.is_null(2));
        }
    }
}

TEST(Sync_Mixed)
{
    // Test replication and synchronization of Mixed values and lists.

    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    TEST_DIR(dir);
    fixtures::ClientServerFixture fixture{dir, test_context};
    fixture.start();

    Session session_1 = fixture.make_session(db_1);
    Session session_2 = fixture.make_session(db_2);
    fixture.bind_session(session_1, "/test");
    fixture.bind_session(session_2, "/test");

    {
        WriteTransaction tr{db_1};
        auto& g = tr.get_group();
        auto foos = g.add_table_with_primary_key("class_Foo", type_Int, "id");
        auto bars = g.add_table_with_primary_key("class_Bar", type_String, "id");
        auto fops = g.add_table_with_primary_key("class_Fop", type_Int, "id");
        foos->add_column(type_Mixed, "value", true);
        foos->add_column_list(type_Mixed, "values");

        auto foo = foos->create_object_with_primary_key(123);
        auto bar = bars->create_object_with_primary_key("Hello");
        auto fop = fops->create_object_with_primary_key(456);

        foo.set("value", Mixed(6.2f));
        auto values = foo.get_list<Mixed>("values");
        values.insert(0, StringData("A"));
        values.insert(1, ObjLink{bars->get_key(), bar.get_key()});
        values.insert(2, ObjLink{fops->get_key(), fop.get_key()});
        values.insert(3, 123.f);

        session_1.nonsync_transact_notify(tr.commit());
    }

    session_1.wait_for_upload_complete_or_client_stopped();
    session_2.wait_for_download_complete_or_client_stopped();

    {
        ReadTransaction tr{db_2};

        auto foos = tr.get_table("class_Foo");
        auto bars = tr.get_table("class_Bar");
        auto fops = tr.get_table("class_Fop");

        CHECK_EQUAL(foos->size(), 1);
        CHECK_EQUAL(bars->size(), 1);
        CHECK_EQUAL(fops->size(), 1);

        auto foo = *foos->begin();
        auto value = foo.get<Mixed>("value");
        CHECK_EQUAL(value, Mixed{6.2f});
        auto values = foo.get_list<Mixed>("values");
        CHECK_EQUAL(values.size(), 4);

        auto v0 = values.get(0);
        auto v1 = values.get(1);
        auto v2 = values.get(2);
        auto v3 = values.get(3);

        auto l1 = v1.get_link();
        auto l2 = v2.get_link();

        auto l1_table = tr.get_table(l1.get_table_key());
        auto l2_table = tr.get_table(l2.get_table_key());

        CHECK_EQUAL(v0, Mixed{"A"});
        CHECK_EQUAL(l1_table, bars);
        CHECK_EQUAL(l2_table, fops);
        CHECK_EQUAL(l1.get_obj_key(), bars->begin()->get_key());
        CHECK_EQUAL(l2.get_obj_key(), fops->begin()->get_key());
        CHECK_EQUAL(v3, Mixed{123.f});
    }
}

TEST(Sync_TypedLinks)
{
    // Test replication and synchronization of Mixed values and lists.

    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    TEST_DIR(dir);
    fixtures::ClientServerFixture fixture{dir, test_context};
    fixture.start();

    Session session_1 = fixture.make_session(db_1);
    Session session_2 = fixture.make_session(db_2);
    fixture.bind_session(session_1, "/test");
    fixture.bind_session(session_2, "/test");

    write_transaction_notifying_session(db_1, session_1, [](WriteTransaction& tr) {
        auto& g = tr.get_group();
        auto foos = g.add_table_with_primary_key("class_Foo", type_Int, "id");
        auto bars = g.add_table_with_primary_key("class_Bar", type_String, "id");
        auto fops = g.add_table_with_primary_key("class_Fop", type_Int, "id");
        foos->add_column(type_TypedLink, "link");

        auto foo1 = foos->create_object_with_primary_key(123);
        auto foo2 = foos->create_object_with_primary_key(456);
        auto bar = bars->create_object_with_primary_key("Hello");
        auto fop = fops->create_object_with_primary_key(456);

        foo1.set("link", ObjLink(bars->get_key(), bar.get_key()));
        foo2.set("link", ObjLink(fops->get_key(), fop.get_key()));
    });

    session_1.wait_for_upload_complete_or_client_stopped();
    session_2.wait_for_download_complete_or_client_stopped();

    {
        ReadTransaction tr{db_2};

        auto foos = tr.get_table("class_Foo");
        auto bars = tr.get_table("class_Bar");
        auto fops = tr.get_table("class_Fop");

        CHECK_EQUAL(foos->size(), 2);
        CHECK_EQUAL(bars->size(), 1);
        CHECK_EQUAL(fops->size(), 1);

        auto it = foos->begin();
        auto l1 = it->get<ObjLink>("link");
        ++it;
        auto l2 = it->get<ObjLink>("link");

        auto l1_table = tr.get_table(l1.get_table_key());
        auto l2_table = tr.get_table(l2.get_table_key());

        CHECK_EQUAL(l1_table, bars);
        CHECK_EQUAL(l2_table, fops);
        CHECK_EQUAL(l1.get_obj_key(), bars->begin()->get_key());
        CHECK_EQUAL(l2.get_obj_key(), fops->begin()->get_key());
    }
}

TEST(Sync_Dictionary)
{
    // Test replication and synchronization of Mixed values and lists.

    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    TEST_DIR(dir);
    fixtures::ClientServerFixture fixture{dir, test_context};
    fixture.start();

    Session session_1 = fixture.make_session(db_1);
    Session session_2 = fixture.make_session(db_2);
    fixture.bind_session(session_1, "/test");
    fixture.bind_session(session_2, "/test");

    Timestamp now{std::chrono::system_clock::now()};

    write_transaction_notifying_session(db_1, session_1, [&](WriteTransaction& tr) {
        auto& g = tr.get_group();
        auto foos = g.add_table_with_primary_key("class_Foo", type_Int, "id");
        auto col_dict = foos->add_column_dictionary(type_Mixed, "dict");
        auto col_dict_str = foos->add_column_dictionary(type_String, "str_dict", true);

        auto foo = foos->create_object_with_primary_key(123);

        auto dict = foo.get_dictionary(col_dict);
        dict.insert("hello", "world");
        dict.insert("cnt", 7);
        dict.insert("when", now);

        auto dict_str = foo.get_dictionary(col_dict_str);
        dict_str.insert("some", "text");
        dict_str.insert("nothing", null());
    });

    session_1.wait_for_upload_complete_or_client_stopped();
    session_2.wait_for_download_complete_or_client_stopped();

    write_transaction_notifying_session(db_2, session_2, [&](WriteTransaction& tr) {
        auto foos = tr.get_table("class_Foo");
        CHECK_EQUAL(foos->size(), 1);

        auto it = foos->begin();
        auto dict = it->get_dictionary(foos->get_column_key("dict"));
        CHECK(dict.get_value_data_type() == type_Mixed);
        CHECK_EQUAL(dict.size(), 3);

        auto col_dict_str = foos->get_column_key("str_dict");
        auto dict_str = it->get_dictionary(col_dict_str);
        CHECK(col_dict_str.is_nullable());
        CHECK(dict_str.get_value_data_type() == type_String);
        CHECK_EQUAL(dict_str.size(), 2);

        Mixed val = dict["hello"];
        CHECK_EQUAL(val.get_string(), "world");
        val = dict.get("cnt");
        CHECK_EQUAL(val.get_int(), 7);
        val = dict.get("when");
        CHECK_EQUAL(val.get<Timestamp>(), now);

        dict.erase("cnt");
        dict.insert("hello", "goodbye");
    });

    session_2.wait_for_upload_complete_or_client_stopped();
    session_1.wait_for_download_complete_or_client_stopped();

    write_transaction_notifying_session(db_1, session_1, [&](WriteTransaction& tr) {
        auto foos = tr.get_table("class_Foo");
        CHECK_EQUAL(foos->size(), 1);

        auto it = foos->begin();
        auto dict = it->get_dictionary(foos->get_column_key("dict"));
        CHECK_EQUAL(dict.size(), 2);

        Mixed val = dict["hello"];
        CHECK_EQUAL(val.get_string(), "goodbye");
        val = dict.get("when");
        CHECK_EQUAL(val.get<Timestamp>(), now);

        dict.clear();
    });

    session_1.wait_for_upload_complete_or_client_stopped();
    session_2.wait_for_download_complete_or_client_stopped();

    {
        ReadTransaction read_1{db_1};
        ReadTransaction read_2{db_2};
        // tr.get_group().to_json(std::cout);

        auto foos = read_2.get_table("class_Foo");

        CHECK_EQUAL(foos->size(), 1);

        auto it = foos->begin();
        auto dict = it->get_dictionary(foos->get_column_key("dict"));
        CHECK_EQUAL(dict.size(), 0);

        CHECK(compare_groups(read_1, read_2));
    }
}

TEST(Sync_Dictionary_Links)
{
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    TEST_DIR(dir);
    fixtures::ClientServerFixture fixture{dir, test_context};
    fixture.start();

    Session session_1 = fixture.make_session(db_1);
    Session session_2 = fixture.make_session(db_2);
    fixture.bind_session(session_1, "/test");
    fixture.bind_session(session_2, "/test");

    // Test that we can transmit links.

    ColKey col_dict;

    write_transaction_notifying_session(db_1, session_1, [&](WriteTransaction& tr) {
        auto& g = tr.get_group();
        auto foos = g.add_table_with_primary_key("class_Foo", type_Int, "id");
        auto bars = g.add_table_with_primary_key("class_Bar", type_String, "id");
        col_dict = foos->add_column_dictionary(type_Mixed, "dict");

        auto foo = foos->create_object_with_primary_key(123);
        auto a = bars->create_object_with_primary_key("a");
        auto b = bars->create_object_with_primary_key("b");

        auto dict = foo.get_dictionary(col_dict);
        dict.insert("a", a);
        dict.insert("b", b);
    });

    session_1.wait_for_upload_complete_or_client_stopped();
    session_2.wait_for_download_complete_or_client_stopped();

    {
        ReadTransaction tr{db_2};

        auto foos = tr.get_table("class_Foo");
        auto bars = tr.get_table("class_Bar");

        CHECK_EQUAL(foos->size(), 1);
        CHECK_EQUAL(bars->size(), 2);

        auto foo = foos->get_object_with_primary_key(123);
        auto a = bars->get_object_with_primary_key("a");
        auto b = bars->get_object_with_primary_key("b");

        auto dict = foo.get_dictionary(foos->get_column_key("dict"));
        CHECK_EQUAL(dict.size(), 2);

        auto dict_a = dict.get("a");
        auto dict_b = dict.get("b");
        CHECK(dict_a == Mixed{a.get_link()});
        CHECK(dict_b == Mixed{b.get_link()});
    }

    // Test that we can create tombstones for objects in dictionaries.

    write_transaction_notifying_session(db_1, session_1, [&](WriteTransaction& tr) {
        auto& g = tr.get_group();

        auto bars = g.get_table("class_Bar");
        auto a = bars->get_object_with_primary_key("a");
        a.invalidate();

        auto foos = g.get_table("class_Foo");
        auto foo = foos->get_object_with_primary_key(123);
        auto dict = foo.get_dictionary(col_dict);

        CHECK_EQUAL(dict.size(), 2);
        CHECK((*dict.find("a")).second.is_unresolved_link());

        CHECK(dict.find("b") != dict.end());
    });

    session_1.wait_for_upload_complete_or_client_stopped();
    session_2.wait_for_download_complete_or_client_stopped();

    {
        ReadTransaction tr{db_2};

        auto foos = tr.get_table("class_Foo");
        auto bars = tr.get_table("class_Bar");

        CHECK_EQUAL(foos->size(), 1);
        CHECK_EQUAL(bars->size(), 1);

        auto b = bars->get_object_with_primary_key("b");

        auto foo = foos->get_object_with_primary_key(123);
        auto dict = foo.get_dictionary(col_dict);

        CHECK_EQUAL(dict.size(), 2);
        CHECK((*dict.find("a")).second.is_unresolved_link());

        CHECK(dict.find("b") != dict.end());
        CHECK((*dict.find("b")).second == Mixed{b.get_link()});
    }
}

TEST(Sync_Set)
{
    // Test replication and synchronization of Set values.

    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);

    TEST_DIR(dir);
    fixtures::ClientServerFixture fixture{dir, test_context};
    fixture.start();

    Session session_1 = fixture.make_session(db_1);
    Session session_2 = fixture.make_session(db_2);
    fixture.bind_session(session_1, "/test");
    fixture.bind_session(session_2, "/test");

    ColKey col_ints, col_strings, col_mixeds;
    {
        WriteTransaction wt{db_1};
        auto t = wt.get_group().add_table_with_primary_key("class_Foo", type_Int, "pk");
        col_ints = t->add_column_set(type_Int, "ints");
        col_strings = t->add_column_set(type_String, "strings");
        col_mixeds = t->add_column_set(type_Mixed, "mixeds");

        auto obj = t->create_object_with_primary_key(0);

        auto ints = obj.get_set<int64_t>(col_ints);
        auto strings = obj.get_set<StringData>(col_strings);
        auto mixeds = obj.get_set<Mixed>(col_mixeds);

        ints.insert(123);
        ints.insert(456);
        ints.insert(789);
        ints.insert(123);
        ints.insert(456);
        ints.insert(789);

        CHECK_EQUAL(ints.size(), 3);
        CHECK_EQUAL(ints.find(123), 0);
        CHECK_EQUAL(ints.find(456), 1);
        CHECK_EQUAL(ints.find(789), 2);

        strings.insert("a");
        strings.insert("b");
        strings.insert("c");
        strings.insert("a");
        strings.insert("b");
        strings.insert("c");

        CHECK_EQUAL(strings.size(), 3);
        CHECK_EQUAL(strings.find("a"), 0);
        CHECK_EQUAL(strings.find("b"), 1);
        CHECK_EQUAL(strings.find("c"), 2);

        mixeds.insert(Mixed{123});
        mixeds.insert(Mixed{"a"});
        mixeds.insert(Mixed{456.0});
        mixeds.insert(Mixed{123});
        mixeds.insert(Mixed{"a"});
        mixeds.insert(Mixed{456.0});

        CHECK_EQUAL(mixeds.size(), 3);
        CHECK_EQUAL(mixeds.find(123), 0);
        CHECK_EQUAL(mixeds.find(456.0), 1);
        CHECK_EQUAL(mixeds.find("a"), 2);

        session_1.nonsync_transact_notify(wt.commit());
    }

    session_1.wait_for_upload_complete_or_client_stopped();
    session_2.wait_for_download_complete_or_client_stopped();

    // Create a conflict. Session 1 should lose, because it has a lower peer ID.
    write_transaction_notifying_session(db_1, session_1, [=](WriteTransaction& wt) {
        auto t = wt.get_table("class_Foo");
        auto obj = t->get_object_with_primary_key(0);

        auto ints = obj.get_set<int64_t>(col_ints);
        ints.insert(999);
    });

    write_transaction_notifying_session(db_2, session_2, [=](WriteTransaction& wt) {
        auto t = wt.get_table("class_Foo");
        auto obj = t->get_object_with_primary_key(0);

        auto ints = obj.get_set<int64_t>(col_ints);
        ints.insert(999);
        ints.erase(999);
    });

    session_1.wait_for_upload_complete_or_client_stopped();
    session_2.wait_for_upload_complete_or_client_stopped();
    session_1.wait_for_download_complete_or_client_stopped();
    session_2.wait_for_download_complete_or_client_stopped();

    {
        ReadTransaction read_1{db_1};
        ReadTransaction read_2{db_2};
        CHECK(compare_groups(read_1, read_2));
    }

    write_transaction_notifying_session(db_1, session_1, [=](WriteTransaction& wt) {
        auto t = wt.get_table("class_Foo");
        auto obj = t->get_object_with_primary_key(0);
        auto ints = obj.get_set<int64_t>(col_ints);
        ints.clear();
    });

    session_1.wait_for_upload_complete_or_client_stopped();
    session_2.wait_for_download_complete_or_client_stopped();

    {
        ReadTransaction read_1{db_1};
        ReadTransaction read_2{db_2};
        CHECK(compare_groups(read_1, read_2));
    }
}

TEST(Sync_DanglingLinksCountInPriorSize)
{
    SHARED_GROUP_TEST_PATH(path);
    ClientReplication repl;
    auto local_db = realm::DB::create(repl, path);
    auto& logger = test_context.logger;
    auto& history = repl.get_history();
    history.set_client_file_ident(sync::SaltedFileIdent{1, 123456}, true);

    version_type last_version, last_version_observed = 0;
    auto dump_uploadable = [&] {
        UploadCursor upload_cursor{last_version_observed, 0};
        std::vector<sync::ClientHistory::UploadChangeset> changesets_to_upload;
        version_type locked_server_version = 0;
        history.find_uploadable_changesets(upload_cursor, last_version, changesets_to_upload, locked_server_version);
        CHECK_EQUAL(changesets_to_upload.size(), static_cast<size_t>(1));
        realm::sync::Changeset parsed_changeset;
        auto unparsed_changeset = changesets_to_upload[0].changeset.get_first_chunk();
        realm::_impl::SimpleNoCopyInputStream changeset_stream(unparsed_changeset.data(), unparsed_changeset.size());
        realm::sync::parse_changeset(changeset_stream, parsed_changeset);
        logger.info("changeset at version %1: %2", last_version, parsed_changeset);
        last_version_observed = last_version;
        return parsed_changeset;
    };

    TableKey source_table_key, target_table_key;
    {
        auto wt = local_db->start_write();
        auto source_table = wt->add_table_with_primary_key("class_source", type_String, "_id");
        auto target_table = wt->add_table_with_primary_key("class_target", type_String, "_id");
        source_table->add_column_list(*target_table, "links");

        source_table_key = source_table->get_key();
        target_table_key = target_table->get_key();

        auto obj_to_keep = target_table->create_object_with_primary_key(std::string{"target1"});
        auto obj_to_delete = target_table->create_object_with_primary_key(std::string{"target2"});
        auto source_obj = source_table->create_object_with_primary_key(std::string{"source"});

        auto links_list = source_obj.get_linklist("links");
        links_list.add(obj_to_keep.get_key());
        links_list.add(obj_to_delete.get_key());
        last_version = wt->commit();
    }

    dump_uploadable();

    {
        // Simulate removing the object via the sync client so we get a dangling link
        TempShortCircuitReplication disable_repl(repl);
        auto wt = local_db->start_write();
        auto target_table = wt->get_table(target_table_key);
        auto obj = target_table->get_object_with_primary_key(std::string{"target2"});
        obj.invalidate();
        last_version = wt->commit();
    }

    {
        auto wt = local_db->start_write();
        auto source_table = wt->get_table(source_table_key);
        auto target_table = wt->get_table(target_table_key);

        auto obj_to_add = target_table->create_object_with_primary_key(std::string{"target3"});

        auto source_obj = source_table->get_object_with_primary_key(std::string{"source"});
        auto links_list = source_obj.get_linklist("links");
        links_list.add(obj_to_add.get_key());
        last_version = wt->commit();
    }

    auto changeset = dump_uploadable();
    CHECK_EQUAL(changeset.size(), static_cast<size_t>(2));
    auto changeset_it = changeset.end();
    --changeset_it;
    auto last_instr = *changeset_it;
    CHECK_EQUAL(last_instr->type(), Instruction::Type::ArrayInsert);
    auto arr_insert_instr = last_instr->get_as<Instruction::ArrayInsert>();
    CHECK_EQUAL(changeset.get_string(arr_insert_instr.table), StringData("source"));
    CHECK(arr_insert_instr.value.type == sync::instr::Payload::Type::Link);
    CHECK_EQUAL(changeset.get_string(mpark::get<InternString>(arr_insert_instr.value.data.link.target)),
                StringData("target3"));
    CHECK_EQUAL(arr_insert_instr.prior_size, 2);
}

TEST(Sync_BundledRealmFile)
{
    TEST_CLIENT_DB(db);
    SHARED_GROUP_TEST_PATH(path);

    TEST_DIR(dir);
    fixtures::ClientServerFixture fixture{dir, test_context};
    fixture.start();

    Session session = fixture.make_bound_session(db);

    write_transaction_notifying_session(db, session, [](WriteTransaction& tr) {
        auto foos = tr.get_group().add_table_with_primary_key("class_Foo", type_Int, "id");
        auto foo = foos->create_object_with_primary_key(123);
    });

    // We cannot write out file if changes are not synced to server
    CHECK_THROW_ANY(db->write_copy(path.c_str()));

    session.wait_for_upload_complete_or_client_stopped();
    session.wait_for_download_complete_or_client_stopped();

    // Now we can
    db->write_copy(path.c_str());
}

// This test is extracted from ClientReset_ThreeClients
// because it uncovers a bug in how MSVC 2019 compiles
// things in Changeset::get_key()
TEST(Sync_MergeStringPrimaryKey)
{
    TEST_DIR(dir_1); // The server.
    TEST_CLIENT_DB(db_1);
    TEST_CLIENT_DB(db_2);
    TEST_DIR(metadata_dir_1);
    TEST_DIR(metadata_dir_2);

    const std::string server_path = "/data";

    std::string real_path_1, real_path_2;

    auto create_schema = [&](Transaction& group) {
        TableRef table_0 = group.add_table("class_table_0");
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
            WriteTransaction wt{db_1};
            create_schema(wt);
            wt.commit();
        }
        {
            WriteTransaction wt{db_2};
            create_schema(wt);

            TableRef table_2 = wt.get_table("class_table_2");
            auto col = table_2->get_column_key("array_string");
            auto list_string = table_2->create_object_with_primary_key("aaa").get_list<String>(col);
            list_string.add("a");
            list_string.add("b");

            wt.commit();
        }

        Session session_1 = fixture.make_bound_session(db_1, server_path);
        Session session_2 = fixture.make_bound_session(db_2, server_path);

        session_1.wait_for_upload_complete_or_client_stopped();
        session_2.wait_for_upload_complete_or_client_stopped();
        // Download completion is not important.
    }
}

} // unnamed namespace
