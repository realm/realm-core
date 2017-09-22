////////////////////////////////////////////////////////////////////////////
//
// Copyright 2017 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include "catch.hpp"

#include "sync/session/session_util.hpp"

#include "util/event_loop.hpp"

#include <realm/util/scope_exit.hpp>

namespace {

void wait_for_session_to_activate(SyncSession& session)
{
    EventLoop::main().run_until([&] { return sessions_are_active(session); });
    // Wait for uploads and downloads
    std::atomic<bool> download_did_complete(false);
    std::atomic<bool> upload_did_complete(false);
    session.wait_for_download_completion([&](auto) { download_did_complete = true; });
    session.wait_for_upload_completion([&](auto) { upload_did_complete = true; });
    EventLoop::main().run_until([&] { return download_did_complete.load() && upload_did_complete.load(); });
}

}

// FIXME: break this up into smaller discrete test cases
TEST_CASE("progress notification", "[sync]") {
    if (!EventLoop::has_implementation())
        return;

    const std::string dummy_auth_url = "https://realm.example.org";

    SyncServer server;
    // Disable file-related functionality and metadata functionality for testing purposes.
    SyncManager::shared().configure_file_system(tmp_dir(), SyncManager::MetadataMode::NoMetadata);
    auto user = SyncManager::shared().get_user({ "user", dummy_auth_url }, "not_a_real_token");

    SECTION("runs at least once (initially when registered)") {
        auto user = SyncManager::shared().get_user({ "user-test-sync-1", dummy_auth_url }, "not_a_real_token");
        auto session = sync_session(server, user, "/test-sync-progress-1",
                                    [](const auto&, const auto&) { return s_test_token; },
                                    [](auto, auto) { },
                                    SyncSessionStopPolicy::AfterChangesUploaded);
        wait_for_session_to_activate(*session);

        REQUIRE(!session->is_in_error_state());
        std::atomic<bool> callback_was_called(false);

        SECTION("for upload notifications, with no data transfer ongoing") {
            session->register_progress_notifier([&](auto, auto) {
                callback_was_called = true;
            }, SyncSession::NotifierType::upload, false);
            EventLoop::main().run_until([&] { return callback_was_called.load(); });
        }

        SECTION("for download notifications, with no data transfer ongoing") {
            session->register_progress_notifier([&](auto, auto) {
                callback_was_called = true;
            }, SyncSession::NotifierType::download, false);
            EventLoop::main().run_until([&] { return callback_was_called.load(); });
        }

        SECTION("can register another notifier while in the initial notification without deadlock") {
            std::atomic<uint64_t> counter(0);
            session->register_progress_notifier([&](auto, auto) {
                counter++;
                session->register_progress_notifier([&](auto, auto) {
                    counter++;
                }, SyncSession::NotifierType::upload, false);
            }, SyncSession::NotifierType::download, false);
            EventLoop::main().run_until([&] { return counter.load() == 2; });
        }
    }

    SECTION("properly runs for streaming notifiers") {
        auto user = SyncManager::shared().get_user({ "user-test-sync-2", dummy_auth_url }, "not_a_real_token");
        auto session = sync_session(server, user, "/test-sync-progress-2",
                                    [](const auto&, const auto&) { return s_test_token; },
                                    [](auto, auto) { },
                                    SyncSessionStopPolicy::AfterChangesUploaded);
        wait_for_session_to_activate(*session);

        REQUIRE(!session->is_in_error_state());
        std::atomic<bool> callback_was_called(false);
        std::atomic<uint64_t> transferred(0);
        std::atomic<uint64_t> transferrable(0);
        uint64_t current_transferred = 0;
        uint64_t current_transferrable = 0;

        SECTION("for upload notifications") {
            session->register_progress_notifier([&](auto xferred, auto xferable) {
                transferred = xferred;
                transferrable = xferable;
                callback_was_called = true;
            }, SyncSession::NotifierType::upload, true);
            // Wait for the initial callback.
            EventLoop::main().run_until([&] { return callback_was_called.load(); });

            // Now manually call the notifier handler a few times.
            callback_was_called = false;
            current_transferred = 60;
            current_transferrable = 912;
            SyncSession::OnlyForTesting::handle_progress_update(*session, 25, 26, current_transferred, current_transferrable);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_transferred);
            CHECK(transferrable.load() == current_transferrable);

            // Second callback
            callback_was_called = false;
            current_transferred = 79;
            current_transferrable = 1021;
            SyncSession::OnlyForTesting::handle_progress_update(*session, 68, 191, current_transferred, current_transferrable);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_transferred);
            CHECK(transferrable.load() == current_transferrable);

            // Third callback
            callback_was_called = false;
            current_transferred = 150;
            current_transferrable = 1228;
            SyncSession::OnlyForTesting::handle_progress_update(*session, 199, 591, current_transferred, current_transferrable);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_transferred);
            CHECK(transferrable.load() == current_transferrable);
        }

        SECTION("for download notifications") {
            session->register_progress_notifier([&](auto xferred, auto xferable) {
                transferred = xferred;
                transferrable = xferable;
                callback_was_called = true;
            }, SyncSession::NotifierType::download, true);
            // Wait for the initial callback.
            EventLoop::main().run_until([&] { return callback_was_called.load(); });

            // Now manually call the notifier handler a few times.
            callback_was_called = false;
            current_transferred = 60;
            current_transferrable = 912;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_transferred, current_transferrable, 25, 26);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_transferred);
            CHECK(transferrable.load() == current_transferrable);

            // Second callback
            callback_was_called = false;
            current_transferred = 79;
            current_transferrable = 1021;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_transferred, current_transferrable, 68, 191);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_transferred);
            CHECK(transferrable.load() == current_transferrable);

            // Third callback
            callback_was_called = false;
            current_transferred = 150;
            current_transferrable = 1228;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_transferred, current_transferrable, 199, 591);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_transferred);
            CHECK(transferrable.load() == current_transferrable);
        }

        SECTION("token unregistration works") {
            uint64_t token = session->register_progress_notifier([&](auto xferred, auto xferable) {
                transferred = xferred;
                transferrable = xferable;
                callback_was_called = true;
            }, SyncSession::NotifierType::download, true);
            // Wait for the initial callback.
            EventLoop::main().run_until([&] { return callback_was_called.load(); });

            // Now manually call the notifier handler a few times.
            callback_was_called = false;
            current_transferred = 60;
            current_transferrable = 912;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_transferred, current_transferrable, 25, 26);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_transferred);
            CHECK(transferrable.load() == current_transferrable);

            // Unregister
            session->unregister_progress_notifier(token);

            // Second callback: should not actually do anything.
            callback_was_called = false;
            current_transferred = 150;
            current_transferrable = 1228;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_transferred, current_transferrable, 199, 591);
            CHECK(!callback_was_called.load());
        }

        SECTION("for multiple notifiers") {
            session->register_progress_notifier([&](auto xferred, auto xferable) {
                transferred = xferred;
                transferrable = xferable;
                callback_was_called = true;
            }, SyncSession::NotifierType::download, true);
            // Register a second notifier.
            std::atomic<bool> callback_was_called_2(false);
            std::atomic<uint64_t> transferred_2(0);
            std::atomic<uint64_t> transferrable_2(0);
            session->register_progress_notifier([&](auto xferred, auto xferable) {
                transferred_2 = xferred;
                transferrable_2 = xferable;
                callback_was_called_2 = true;
            }, SyncSession::NotifierType::upload, true);
            // Wait for the initial callback.
            EventLoop::main().run_until([&] { return callback_was_called.load() && callback_was_called_2.load(); });

            // Now manually call the notifier handler a few times.
            callback_was_called = false;
            callback_was_called_2 = false;
            uint64_t current_uploaded = 16;
            uint64_t current_uploadable = 201;
            uint64_t current_downloaded = 68;
            uint64_t current_downloadable = 182;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_downloaded, current_downloadable, current_uploaded, current_uploadable);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_downloaded);
            CHECK(transferrable.load() == current_downloadable);
            CHECK(callback_was_called_2.load());
            CHECK(transferred_2.load() == current_uploaded);
            CHECK(transferrable_2.load() == current_uploadable);

            // Second callback
            callback_was_called = false;
            callback_was_called_2 = false;
            current_uploaded = 31;
            current_uploadable = 329;
            current_downloaded = 76;
            current_downloadable = 191;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_downloaded, current_downloadable, current_uploaded, current_uploadable);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_downloaded);
            CHECK(transferrable.load() == current_downloadable);
            CHECK(callback_was_called_2.load());
            CHECK(transferred_2.load() == current_uploaded);
            CHECK(transferrable_2.load() == current_uploadable);
        }
    }

    SECTION("properly runs for non-streaming notifiers") {
        auto user = SyncManager::shared().get_user({ "user-test-sync-3", dummy_auth_url }, "not_a_real_token");
        auto session = sync_session(server, user, "/test-sync-progress-3",
                                    [](const auto&, const auto&) { return s_test_token; },
                                    [](auto, auto) { },
                                    SyncSessionStopPolicy::AfterChangesUploaded);
        wait_for_session_to_activate(*session);

        REQUIRE(!session->is_in_error_state());
        std::atomic<bool> callback_was_called(false);
        std::atomic<uint64_t> transferred(0);
        std::atomic<uint64_t> transferrable(0);
        uint64_t current_transferred = 0;
        uint64_t current_transferrable = 0;

        SECTION("for upload notifications") {
            // Prime the progress updater
            current_transferred = 60;
            current_transferrable = 501;
            const uint64_t original_transferrable = current_transferrable;
            SyncSession::OnlyForTesting::handle_progress_update(*session, 21, 26, current_transferred, current_transferrable);

            session->register_progress_notifier([&](auto xferred, auto xferable) {
                transferred = xferred;
                transferrable = xferable;
                callback_was_called = true;
            }, SyncSession::NotifierType::upload, false);
            // Wait for the initial callback.
            EventLoop::main().run_until([&] { return callback_was_called.load(); });

            // Now manually call the notifier handler a few times.
            callback_was_called = false;
            current_transferred = 66;
            current_transferrable = 582;
            SyncSession::OnlyForTesting::handle_progress_update(*session, 25, 26, current_transferred, current_transferrable);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_transferred);
            CHECK(transferrable.load() == original_transferrable);

            // Second callback
            callback_was_called = false;
            current_transferred = original_transferrable + 100;
            current_transferrable = 1021;
            SyncSession::OnlyForTesting::handle_progress_update(*session, 68, 191, current_transferred, current_transferrable);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_transferred);
            CHECK(transferrable.load() == original_transferrable);

            // The notifier should be unregistered at this point, and not fire.
            callback_was_called = false;
            current_transferred = original_transferrable + 250;
            current_transferrable = 1228;
            SyncSession::OnlyForTesting::handle_progress_update(*session, 199, 591, current_transferred, current_transferrable);
            CHECK(!callback_was_called.load());
        }

        SECTION("for download notifications") {
            // Prime the progress updater
            current_transferred = 60;
            current_transferrable = 501;
            const uint64_t original_transferrable = current_transferrable;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_transferred, current_transferrable, 21, 26);

            session->register_progress_notifier([&](auto xferred, auto xferable) {
                transferred = xferred;
                transferrable = xferable;
                callback_was_called = true;
            }, SyncSession::NotifierType::download, false);
            // Wait for the initial callback.
            EventLoop::main().run_until([&] { return callback_was_called.load(); });

            // Now manually call the notifier handler a few times.
            callback_was_called = false;
            current_transferred = 66;
            current_transferrable = 582;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_transferred, current_transferrable, 25, 26);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_transferred);
            CHECK(transferrable.load() == original_transferrable);

            // Second callback
            callback_was_called = false;
            current_transferred = original_transferrable + 100;
            current_transferrable = 1021;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_transferred, current_transferrable, 68, 191);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_transferred);
            CHECK(transferrable.load() == original_transferrable);

            // The notifier should be unregistered at this point, and not fire.
            callback_was_called = false;
            current_transferred = original_transferrable + 250;
            current_transferrable = 1228;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_transferred, current_transferrable, 199, 591);
            CHECK(!callback_was_called.load());
        }

        SECTION("token unregistration works") {
            // Prime the progress updater
            current_transferred = 60;
            current_transferrable = 501;
            const uint64_t original_transferrable = current_transferrable;
            SyncSession::OnlyForTesting::handle_progress_update(*session, 21, 26, current_transferred, current_transferrable);

            uint64_t token = session->register_progress_notifier([&](auto xferred, auto xferable) {
                transferred = xferred;
                transferrable = xferable;
                callback_was_called = true;
            }, SyncSession::NotifierType::upload, false);
            // Wait for the initial callback.
            EventLoop::main().run_until([&] { return callback_was_called.load(); });

            // Now manually call the notifier handler a few times.
            callback_was_called = false;
            current_transferred = 66;
            current_transferrable = 912;
            SyncSession::OnlyForTesting::handle_progress_update(*session, 25, 26, current_transferred, current_transferrable);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_transferred);
            CHECK(transferrable.load() == original_transferrable);

            // Unregister
            session->unregister_progress_notifier(token);

            // Second callback: should not actually do anything.
            callback_was_called = false;
            current_transferred = 67;
            current_transferrable = 1228;
            SyncSession::OnlyForTesting::handle_progress_update(*session, 199, 591, current_transferred, current_transferrable);
            CHECK(!callback_was_called.load());
        }

        SECTION("for multiple notifiers, different directions") {
            // Prime the progress updater
            uint64_t current_uploaded = 16;
            uint64_t current_uploadable = 201;
            uint64_t current_downloaded = 68;
            uint64_t current_downloadable = 182;
            const uint64_t original_uploadable = current_uploadable;
            const uint64_t original_downloadable = current_downloadable;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_downloaded, current_downloadable, current_uploaded, current_uploadable);

            session->register_progress_notifier([&](auto xferred, auto xferable) {
                transferred = xferred;
                transferrable = xferable;
                callback_was_called = true;
            }, SyncSession::NotifierType::upload, false);
            // Register a second notifier.
            std::atomic<bool> callback_was_called_2(false);
            std::atomic<uint64_t> downloaded(0);
            std::atomic<uint64_t> downloadable(0);
            session->register_progress_notifier([&](auto xferred, auto xferable) {
                downloaded = xferred;
                downloadable = xferable;
                callback_was_called_2 = true;
            }, SyncSession::NotifierType::download, false);
            // Wait for the initial callback.
            EventLoop::main().run_until([&] { return callback_was_called.load() && callback_was_called_2.load(); });

            // Now manually call the notifier handler a few times.
            callback_was_called = false;
            callback_was_called_2 = false;
            current_uploaded = 36;
            current_uploadable = 310;
            current_downloaded = 171;
            current_downloadable = 185;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_downloaded, current_downloadable, current_uploaded, current_uploadable);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_uploaded);
            CHECK(transferrable.load() == original_uploadable);
            CHECK(callback_was_called_2.load());
            CHECK(downloaded.load() == current_downloaded);
            CHECK(downloadable.load() == original_downloadable);

            // Second callback, last one for the upload notifier
            callback_was_called = false;
            callback_was_called_2 = false;
            current_uploaded = 218;
            current_uploadable = 310;
            current_downloaded = 174;
            current_downloadable = 190;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_downloaded, current_downloadable, current_uploaded, current_uploadable);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_uploaded);
            CHECK(transferrable.load() == original_uploadable);
            CHECK(callback_was_called_2.load());
            CHECK(downloaded.load() == current_downloaded);
            CHECK(downloadable.load() == original_downloadable);

            // Third callback, last one for the download notifier
            callback_was_called = false;
            callback_was_called_2 = false;
            current_uploaded = 218;
            current_uploadable = 310;
            current_downloaded = 182;
            current_downloadable = 196;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_downloaded, current_downloadable, current_uploaded, current_uploadable);
            CHECK(!callback_was_called.load());
            CHECK(callback_was_called_2.load());
            CHECK(downloaded.load() == current_downloaded);
            CHECK(downloadable.load() == original_downloadable);

            // Fourth callback, last one for the download notifier
            callback_was_called_2 = false;
            current_uploaded = 220;
            current_uploadable = 410;
            current_downloaded = 192;
            current_downloadable = 591;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_downloaded, current_downloadable, current_uploaded, current_uploadable);
            CHECK(!callback_was_called.load());
            CHECK(!callback_was_called_2.load());;
        }

        SECTION("for multiple notifiers, same direction") {
            // Prime the progress updater
            uint64_t current_uploaded = 16;
            uint64_t current_uploadable = 201;
            uint64_t current_downloaded = 68;
            uint64_t current_downloadable = 182;
            const uint64_t original_downloadable = current_downloadable;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_downloaded, current_downloadable, current_uploaded, current_uploadable);

            session->register_progress_notifier([&](auto xferred, auto xferable) {
                transferred = xferred;
                transferrable = xferable;
                callback_was_called = true;
            }, SyncSession::NotifierType::download, false);
            EventLoop::main().run_until([&] { return callback_was_called.load(); });

            // Now manually call the notifier handler a few times.
            callback_was_called = false;
            current_uploaded = 36;
            current_uploadable = 310;
            current_downloaded = 171;
            current_downloadable = 185;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_downloaded, current_downloadable, current_uploaded, current_uploadable);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_downloaded);
            CHECK(transferrable.load() == original_downloadable);

            // Register a second notifier.
            std::atomic<bool> callback_was_called_2(false);
            std::atomic<uint64_t> downloaded(0);
            std::atomic<uint64_t> downloadable(0);
            const uint64_t original_downloadable_2 = current_downloadable;
            session->register_progress_notifier([&](auto xferred, auto xferable) {
                downloaded = xferred;
                downloadable = xferable;
                callback_was_called_2 = true;
            }, SyncSession::NotifierType::download, false);
            // Wait for the initial callback.
            EventLoop::main().run_until([&] { return callback_was_called_2.load(); });

            // Second callback, last one for first notifier
            callback_was_called = false;
            callback_was_called_2 = false;
            current_uploaded = 36;
            current_uploadable = 310;
            current_downloaded = 182;
            current_downloadable = 190;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_downloaded, current_downloadable, current_uploaded, current_uploadable);
            CHECK(callback_was_called.load());
            CHECK(transferred.load() == current_downloaded);
            CHECK(transferrable.load() == original_downloadable);
            CHECK(callback_was_called_2.load());
            CHECK(downloaded.load() == current_downloaded);
            CHECK(downloadable.load() == original_downloadable_2);

            // Third callback, last one for second notifier
            callback_was_called = false;
            callback_was_called_2 = false;
            current_uploaded = 36;
            current_uploadable = 310;
            current_downloaded = 189;
            current_downloadable = 250;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_downloaded, current_downloadable, current_uploaded, current_uploadable);
            CHECK(!callback_was_called.load());
            CHECK(callback_was_called_2.load());
            CHECK(downloaded.load() == current_downloaded);
            CHECK(downloadable.load() == original_downloadable_2);

            // Fourth callback
            callback_was_called_2 = false;
            current_uploaded = 36;
            current_uploadable = 310;
            current_downloaded = 201;
            current_downloadable = 289;
            SyncSession::OnlyForTesting::handle_progress_update(*session, current_downloaded, current_downloadable, current_uploaded, current_uploadable);
            CHECK(!callback_was_called_2.load());
        }
    }
}
