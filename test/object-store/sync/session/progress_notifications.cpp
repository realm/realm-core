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

#include <realm/object-store/sync/sync_session.hpp>

#include <realm/util/scope_exit.hpp>

#ifdef REALM_ENABLE_AUTH_TESTS
#include "util/test_file.hpp"
#include "util/sync/flx_sync_harness.hpp"
#include "util/sync/sync_test_utils.hpp"

#include <realm/object-store/impl/object_accessor_impl.hpp>
#include <realm/object-store/sync/async_open_task.hpp>
#include <realm/object-store/util/scheduler.hpp>
#include <realm/sync/noinst/client_reset.hpp>

using namespace realm::app;
#endif

#include <catch2/catch_all.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>
using namespace Catch::Matchers;

#include <iomanip>

using namespace realm;
using NotifierType = SyncSession::ProgressDirection;

struct ProgressEntry {
    uint64_t transferred = 0;
    uint64_t transferrable = 0;
    double estimate = 0.0;

    inline bool operator==(const ProgressEntry& other) const noexcept
    {
        return transferred == other.transferred && transferrable == other.transferrable && estimate == other.estimate;
    }
};

static std::string estimate_to_string(double est)
{
    std::ostringstream ss;
    ss << std::setprecision(4) << est;
    return ss.str();
}

static std::ostream& operator<<(std::ostream& os, const ProgressEntry& value)
{
    return os << util::format("{ transferred: %1, transferrable: %2, estimate: %3 }", value.transferred,
                              value.transferrable, estimate_to_string(value.estimate));
}


struct WaitableProgress : public util::AtomicRefCountBase {
    WaitableProgress(const std::shared_ptr<util::Logger>& base_logger, std::string context)
        : logger(std::move(context), base_logger)
    {
    }

    std::function<SyncSession::ProgressNotifierCallback> make_cb()
    {
        auto self = util::bind_ptr(this);
        return [self](uint64_t transferred, uint64_t transferrable, double estimate) {
            self->logger.debug("Progress callback called xferred: %1, xferrable: %2, estimate: %3", transferred,
                               transferrable, estimate_to_string(estimate));
            std::lock_guard lk(self->mutex);
            self->entries.push_back(ProgressEntry{transferred, transferrable, estimate});
            self->cv.notify_one();
        };
    }

    bool empty()
    {
        std::lock_guard lk(mutex);
        return entries.empty();
    }

    std::vector<ProgressEntry> wait_for_full_sync()
    {
        std::unique_lock lk(mutex);
        if (!cv.wait_for(lk, std::chrono::seconds(30), [&] {
                return !entries.empty() && entries.back().transferred >= entries.back().transferrable &&
                       entries.back().estimate >= 1.0;
            })) {
            CAPTURE(entries);
            FAIL("Failed while waiting for progress to complete");
            return {};
        }

        std::vector<ProgressEntry> ret;
        std::swap(ret, entries);
        return ret;
    }

    util::PrefixLogger logger;
    std::mutex mutex;
    std::condition_variable cv;
    std::vector<ProgressEntry> entries;
};

struct TestInputValue {
    struct IsRegistration {};
    explicit TestInputValue(IsRegistration)
        : is_registration(true)
    {
    }

    TestInputValue(int64_t query_version, double cur_estimate, uint64_t transferred, uint64_t transferrable)
        : query_version(query_version)
        , cur_estimate(cur_estimate)
        , transferred(transferred)
        , transferrable(transferrable)
    {
    }

    int64_t query_version = 0;
    double cur_estimate = 0;
    uint64_t transferred = 0;
    uint64_t transferrable = 0;
    bool is_registration = false;
};

struct TestValues {
    std::vector<TestInputValue> input_values;
    std::vector<ProgressEntry> expected_values;
    int64_t registered_at_query_version;
};

TEST_CASE("progress notification", "[sync][session][progress]") {
    using NotifierType = SyncSession::ProgressDirection;
    _impl::SyncProgressNotifier progress;

    SECTION("callback is not called prior to first update") {
        bool callback_was_called = false;
        progress.register_callback(
            [&](auto, auto, double) {
                callback_was_called = true;
            },
            NotifierType::upload, false, 0);
        progress.register_callback(
            [&](auto, auto, double) {
                callback_was_called = true;
            },
            NotifierType::download, false, 0);
        REQUIRE_FALSE(callback_was_called);
    }

    SECTION("callback is invoked immediately when a progress update has already occurred") {
        progress.set_local_version(1);
        progress.update(0, 0, 0, 0, 1, 0.0, 0.0, 0);

        bool callback_was_called = false;
        SECTION("for upload notifications, with no data transfer ongoing") {
            double estimate = 0.0;
            progress.register_callback(
                [&](auto, auto, double ep) {
                    callback_was_called = true;
                    estimate = ep;
                },
                NotifierType::upload, false, 0);
            REQUIRE(callback_was_called);
            REQUIRE(estimate == 0.0);
        }

        SECTION("for download notifications, with no data transfer ongoing") {
            double estimate = 0.0;
            progress.register_callback(
                [&](auto, auto, double ep) {
                    callback_was_called = true;
                    estimate = ep;
                },
                NotifierType::download, false, 0);
            REQUIRE(estimate == 0.0);
            REQUIRE(callback_was_called);
        }

        SECTION("can register another notifier while in the initial notification without deadlock") {
            int counter = 0;
            progress.register_callback(
                [&](auto, auto, double) {
                    counter++;
                    progress.register_callback(
                        [&](auto, auto, double) {
                            counter++;
                        },
                        NotifierType::upload, false, 0);
                },
                NotifierType::download, false, 0);
            REQUIRE(counter == 2);
        }
    }

    SECTION("callback is invoked after each update for streaming notifiers") {
        progress.update(0, 0, 0, 0, 1, 0.0, 0.0, 0);

        bool callback_was_called = false;
        uint64_t transferred = 0;
        uint64_t transferrable = 0;
        uint64_t current_transferred = 0;
        uint64_t current_transferrable = 0;
        double estimate = 0.0;

        SECTION("for upload notifications") {
            progress.register_callback(
                [&](auto xferred, auto xferable, double ep) {
                    transferred = xferred;
                    transferrable = xferable;
                    callback_was_called = true;
                    estimate = ep;
                },
                NotifierType::upload, true, 0);
            REQUIRE(callback_was_called);

            // Now manually call the notifier handler a few times.
            callback_was_called = false;
            current_transferred = 60;
            current_transferrable = 912;
            double current_estimate = current_transferred / double(current_transferrable);
            progress.update(25, 26, current_transferred, current_transferrable, 1, 25 / double(26), current_estimate,
                            0);
            CHECK(callback_was_called);
            CHECK(transferred == current_transferred);
            CHECK(transferrable == current_transferrable);
            CHECK(estimate == current_estimate);

            // Second callback
            callback_was_called = false;
            current_transferred = 79;
            current_transferrable = 1021;
            current_estimate = current_transferred / double(current_transferrable);
            progress.update(68, 191, current_transferred, current_transferrable, 1, 68 / double(191),
                            current_estimate, 0);
            CHECK(callback_was_called);
            CHECK(transferred == current_transferred);
            CHECK(transferrable == current_transferrable);
            CHECK(estimate == current_estimate);

            // Third callback
            callback_was_called = false;
            current_transferred = 150;
            current_transferrable = 1228;
            current_estimate = current_transferred / double(current_transferrable);
            progress.update(199, 591, current_transferred, current_transferrable, 1, 199 / double(591),
                            current_estimate, 0);
            CHECK(callback_was_called);
            CHECK(transferred == current_transferred);
            CHECK(transferrable == current_transferrable);
            CHECK(estimate == current_estimate);
        }

        SECTION("for download notifications") {
            progress.register_callback(
                [&](auto xferred, auto xferable, double pe) {
                    transferred = xferred;
                    transferrable = xferable;
                    estimate = pe;
                    callback_was_called = true;
                },
                NotifierType::download, true, 0);
            REQUIRE(callback_was_called);

            // Now manually call the notifier handler a few times.
            callback_was_called = false;
            current_transferred = 60;
            current_transferrable = 912;
            progress.update(current_transferred, current_transferrable, 25, 26, 1,
                            current_transferred / double(current_transferrable), 1.0, 0);
            CHECK(callback_was_called);
            CHECK(transferred == current_transferred);
            CHECK(transferrable == current_transferrable);
            CHECK(estimate == current_transferred / double(current_transferrable));

            // Second callback
            callback_was_called = false;
            current_transferred = 79;
            current_transferrable = 1021;
            progress.update(current_transferred, current_transferrable, 68, 191, 1,
                            current_transferred / double(current_transferrable), 1.0, 0);
            CHECK(callback_was_called);
            CHECK(transferred == current_transferred);
            CHECK(transferrable == current_transferrable);
            CHECK(estimate == current_transferred / double(current_transferrable));

            // Third callback
            callback_was_called = false;
            current_transferred = 150;
            current_transferrable = 1228;
            progress.update(current_transferred, current_transferrable, 199, 591, 1,
                            current_transferred / double(current_transferrable), 1.0, 0);
            CHECK(callback_was_called);
            CHECK(transferred == current_transferred);
            CHECK(transferrable == current_transferrable);
        }

        SECTION("token unregistration works") {
            uint64_t token = progress.register_callback(
                [&](auto xferred, auto xferable, double) {
                    transferred = xferred;
                    transferrable = xferable;
                    callback_was_called = true;
                },
                NotifierType::download, true, 0);
            REQUIRE(callback_was_called);

            // Now manually call the notifier handler a few times.
            callback_was_called = false;
            current_transferred = 60;
            current_transferrable = 912;
            double current_estimate = current_transferred / double(current_transferrable);
            progress.update(current_transferred, current_transferrable, 25, 26, 1, current_estimate, 25 / double(26),
                            0);
            CHECK(callback_was_called);
            CHECK(transferred == current_transferred);
            CHECK(transferrable == current_transferrable);

            // Unregister
            progress.unregister_callback(token);

            // Second callback: should not actually do anything.
            callback_was_called = false;
            current_transferred = 150;
            current_transferrable = 1228;
            current_estimate = current_transferred / double(current_transferrable);
            progress.update(current_transferred, current_transferrable, 199, 591, 1, current_estimate,
                            199 / double(591), 0);
            CHECK(!callback_was_called);
        }

        SECTION("for multiple notifiers") {
            progress.register_callback(
                [&](auto xferred, auto xferable, double ep) {
                    transferred = xferred;
                    transferrable = xferable;
                    estimate = ep;
                    callback_was_called = true;
                },
                NotifierType::download, true, 0);
            REQUIRE(callback_was_called);

            // Register a second notifier.
            bool callback_was_called_2 = false;
            uint64_t transferred_2 = 0;
            uint64_t transferrable_2 = 0;
            double upload_estimate = 0.0;
            progress.register_callback(
                [&](auto xferred, auto xferable, double ep) {
                    transferred_2 = xferred;
                    transferrable_2 = xferable;
                    callback_was_called_2 = true;
                    upload_estimate = ep;
                },
                NotifierType::upload, true, 0);
            REQUIRE(callback_was_called_2);

            // Now manually call the notifier handler a few times.
            callback_was_called = false;
            callback_was_called_2 = false;
            uint64_t current_uploaded = 16;
            uint64_t current_uploadable = 201;
            uint64_t current_downloaded = 68;
            uint64_t current_downloadable = 182;
            auto current_down_estimate = current_downloaded / double(current_downloadable);
            auto current_up_estimate = current_uploaded / double(current_uploadable);
            progress.update(current_downloaded, current_downloadable, current_uploaded, current_uploadable, 1,
                            current_down_estimate, current_up_estimate, 0);
            CHECK(callback_was_called);
            CHECK(transferred == current_downloaded);
            CHECK(transferrable == current_downloadable);
            CHECK(estimate == current_down_estimate);
            CHECK(callback_was_called_2);
            CHECK(transferred_2 == current_uploaded);
            CHECK(transferrable_2 == current_uploadable);
            CHECK(upload_estimate == current_up_estimate);

            // Second callback
            callback_was_called = false;
            callback_was_called_2 = false;
            current_uploaded = 31;
            current_uploadable = 329;
            current_downloaded = 76;
            current_downloadable = 191;
            current_down_estimate = current_downloaded / double(current_downloadable);
            current_up_estimate = current_uploaded / double(current_uploadable);
            progress.update(current_downloaded, current_downloadable, current_uploaded, current_uploadable, 1,
                            current_down_estimate, current_up_estimate, 0);
            CHECK(callback_was_called);
            CHECK(transferred == current_downloaded);
            CHECK(transferrable == current_downloadable);
            CHECK(estimate == current_down_estimate);
            CHECK(callback_was_called_2);
            CHECK(transferred_2 == current_uploaded);
            CHECK(transferrable_2 == current_uploadable);
            CHECK(current_up_estimate == upload_estimate);
        }
    }

    SECTION("properly runs for non-streaming notifiers") {
        bool callback_was_called = false;
        uint64_t transferred = 0;
        uint64_t transferrable = 0;
        uint64_t current_transferred = 0;
        uint64_t current_transferrable = 0;
        double upload_estimate = 0;
        double download_estimate = 0;

        SECTION("for upload notifications") {
            // Prime the progress updater
            current_transferred = 60;
            current_transferrable = 501;
            const uint64_t original_transferrable = current_transferrable;
            double current_estimate = current_transferred / double(current_transferrable);
            progress.update(21, 26, current_transferred, current_transferrable, 1, 21 / double(26), current_estimate,
                            0);

            progress.register_callback(
                [&](auto xferred, auto xferable, double ep) {
                    transferred = xferred;
                    transferrable = xferable;
                    upload_estimate = ep;
                    callback_was_called = true;
                },
                NotifierType::upload, false, 0);
            REQUIRE(callback_was_called);

            // Now manually call the notifier handler a few times.
            callback_was_called = false;
            current_transferred = 66;
            current_transferrable = 582;
            current_estimate = current_transferred / double(current_transferrable);
            progress.update(21, 26, current_transferred, current_transferrable, 1, 21 / double(26), current_estimate,
                            0);
            CHECK(callback_was_called);
            CHECK(transferred == current_transferred);
            CHECK(transferrable == original_transferrable);
            CHECK(upload_estimate == current_transferred / double(original_transferrable));

            // Second callback
            callback_was_called = false;
            current_transferred = original_transferrable + 100;
            current_transferrable = 1021;
            current_estimate = current_transferred / double(current_transferrable);
            progress.update(68, 191, current_transferred, current_transferrable, 1, 68 / double(191),
                            current_estimate, 0);
            CHECK(callback_was_called);
            CHECK(transferred == current_transferred);
            CHECK(transferrable == original_transferrable);
            CHECK(upload_estimate == 1.0);

            // The notifier should be unregistered at this point, and not fire.
            callback_was_called = false;
            current_transferred = original_transferrable + 250;
            current_transferrable = 1228;
            current_estimate = current_transferred / double(current_transferrable);
            progress.update(199, 591, current_transferred, current_transferrable, 1, 199 / double(591),
                            current_estimate, 0);
            CHECK(!callback_was_called);
        }

        SECTION("upload notifications are not sent until all local changesets have been processed") {
            progress.set_local_version(4);

            progress.register_callback(
                [&](auto xferred, auto xferable, double) {
                    transferred = xferred;
                    transferrable = xferable;
                    callback_was_called = true;
                },
                NotifierType::upload, false, 0);
            REQUIRE_FALSE(callback_was_called);

            current_transferred = 66;
            current_transferrable = 582;
            double current_estimate = current_transferred / double(current_transferrable);
            progress.update(0, 0, current_transferred, current_transferrable, 3, 1.0, current_estimate, 0);
            REQUIRE_FALSE(callback_was_called);

            current_transferred = 77;
            current_transferrable = 1021;
            current_estimate = current_transferred / double(current_transferrable);
            progress.update(0, 0, current_transferred, current_transferrable, 4, 1.0, current_estimate, 0);
            REQUIRE(callback_was_called);
            CHECK(transferred == current_transferred);
            // should not have captured transferrable from the first update
            CHECK(transferrable == current_transferrable);
            CHECK(current_estimate == current_estimate);
        }

        SECTION("for download notifications") {
            // Prime the progress updater
            current_transferred = 60;
            current_transferrable = 501;
            double current_estimate = current_transferred / double(current_transferrable);
            const uint64_t original_transferrable = current_transferrable;
            progress.update(current_transferred, current_transferrable, 21, 26, 1, current_estimate, 21 / double(26),
                            0);

            progress.register_callback(
                [&](auto xferred, auto xferable, double ep) {
                    transferred = xferred;
                    transferrable = xferable;
                    download_estimate = ep;
                    callback_was_called = true;
                },
                NotifierType::download, false, 0);
            REQUIRE(callback_was_called);

            // Now manually call the notifier handler a few times.
            callback_was_called = false;
            current_transferred = 66;
            current_transferrable = 582;
            current_estimate = current_transferred / double(current_transferrable);
            progress.update(current_transferred, current_transferrable, 25, 26, 1, current_estimate, 25 / double(26),
                            0);
            CHECK(callback_was_called);
            CHECK(transferred == current_transferred);
            CHECK(transferrable == original_transferrable);
            CHECK(download_estimate == current_estimate);

            // Second callback
            callback_was_called = false;
            current_transferred = original_transferrable + 100;
            current_transferrable = 1021;
            current_estimate = current_transferred / double(current_transferrable);
            progress.update(current_transferred, current_transferrable, 68, 191, 1, current_estimate,
                            68 / double(191), 0);
            CHECK(callback_was_called);
            CHECK(transferred == current_transferred);
            CHECK(transferrable == original_transferrable);
            CHECK(download_estimate == current_estimate);

            // The notifier should be unregistered at this point, and not fire.
            callback_was_called = false;
            current_transferred = original_transferrable + 250;
            current_transferrable = 1228;
            current_estimate = current_transferred / double(current_transferrable);
            progress.update(current_transferred, current_transferrable, 199, 591, 1, current_estimate,
                            199 / double(591), 0);
            CHECK(!callback_was_called);
        }

        SECTION("token unregistration works") {
            // Prime the progress updater
            current_transferred = 60;
            current_transferrable = 501;
            double current_estimate = current_transferred / double(current_transferrable);
            const uint64_t original_transferrable = current_transferrable;
            progress.update(21, 26, current_transferred, current_transferrable, 1, 21 / double(26), current_estimate,
                            0);

            uint64_t token = progress.register_callback(
                [&](auto xferred, auto xferable, double ep) {
                    transferred = xferred;
                    transferrable = xferable;
                    upload_estimate = ep;
                    callback_was_called = true;
                },
                NotifierType::upload, false, 0);
            REQUIRE(callback_was_called);

            // Now manually call the notifier handler a few times.
            callback_was_called = false;
            current_transferred = 66;
            current_transferrable = 912;
            current_estimate = current_transferred / double(current_transferrable);
            progress.update(25, 26, current_transferred, current_transferrable, 1, 25 / double(26), current_estimate,
                            0);
            CHECK(callback_was_called);
            CHECK(transferred == current_transferred);
            CHECK(transferrable == original_transferrable);
            CHECK(upload_estimate == std::min(1.0, current_transferred / double(original_transferrable)));

            // Unregister
            progress.unregister_callback(token);

            // Second callback: should not actually do anything.
            callback_was_called = false;
            current_transferred = 67;
            current_transferrable = 1228;
            current_estimate = current_transferred / double(current_transferrable);
            progress.update(199, 591, current_transferred, current_transferrable, 1, 199 / double(591),
                            current_estimate, 0);
            CHECK(!callback_was_called);
        }

        SECTION("for multiple notifiers, different directions") {
            // Prime the progress updater
            uint64_t current_uploaded = 16;
            uint64_t current_uploadable = 201;
            uint64_t current_downloaded = 68;
            uint64_t current_downloadable = 182;
            const uint64_t original_uploadable = current_uploadable;
            const uint64_t original_downloadable = current_downloadable;
            double current_upload_estimate = current_uploaded / double(current_uploadable);
            double current_download_estimate = current_downloaded / double(current_downloadable);
            progress.update(current_downloaded, current_downloadable, current_uploaded, current_uploadable, 1,
                            current_download_estimate, current_upload_estimate, 0);

            progress.register_callback(
                [&](auto xferred, auto xferable, double ep) {
                    transferred = xferred;
                    transferrable = xferable;
                    upload_estimate = ep;
                    callback_was_called = true;
                },
                NotifierType::upload, false, 0);
            REQUIRE(callback_was_called);

            // Register a second notifier.
            bool callback_was_called_2 = false;
            uint64_t downloaded = 0;
            uint64_t downloadable = 0;
            progress.register_callback(
                [&](auto xferred, auto xferable, double ep) {
                    downloaded = xferred;
                    downloadable = xferable;
                    download_estimate = ep;
                    callback_was_called_2 = true;
                },
                NotifierType::download, false, 0);
            REQUIRE(callback_was_called_2);

            // Now manually call the notifier handler a few times.
            callback_was_called = false;
            callback_was_called_2 = false;
            current_uploaded = 36;
            current_uploadable = 310;
            current_downloaded = 171;
            current_downloadable = 185;
            current_upload_estimate = current_uploaded / double(current_uploadable);
            current_download_estimate = current_downloaded / double(current_downloadable);
            progress.update(current_downloaded, current_downloadable, current_uploaded, current_uploadable, 1,
                            current_download_estimate, current_upload_estimate, 0);
            CHECK(callback_was_called);
            CHECK(transferred == current_uploaded);
            CHECK(transferrable == original_uploadable);
            CHECK(callback_was_called_2);
            CHECK(downloaded == current_downloaded);
            CHECK(downloadable == original_downloadable);
            CHECK(upload_estimate == std::min(1.0, current_uploaded / double(original_uploadable)));
            CHECK(download_estimate == current_download_estimate);

            // Second callback, last one for the upload notifier
            callback_was_called = false;
            callback_was_called_2 = false;
            current_uploaded = 218;
            current_uploadable = 310;
            current_downloaded = 174;
            current_downloadable = 190;
            current_upload_estimate = current_uploaded / double(current_uploadable);
            current_download_estimate = current_downloaded / double(current_downloadable);
            progress.update(current_downloaded, current_downloadable, current_uploaded, current_uploadable, 1,
                            current_download_estimate, current_upload_estimate, 0);
            CHECK(callback_was_called);
            CHECK(transferred == current_uploaded);
            CHECK(transferrable == original_uploadable);
            CHECK(callback_was_called_2);
            CHECK(downloaded == current_downloaded);
            CHECK(downloadable == original_downloadable);
            CHECK(upload_estimate == std::min(1.0, current_uploaded / double(original_uploadable)));
            CHECK(download_estimate == current_download_estimate);

            // Third callback, last one for the download notifier
            callback_was_called = false;
            callback_was_called_2 = false;
            current_uploaded = 218;
            current_uploadable = 310;
            current_downloaded = 182;
            current_downloadable = 196;
            current_upload_estimate = current_uploaded / double(current_uploadable);
            current_download_estimate = current_downloaded / double(current_downloadable);
            progress.update(current_downloaded, current_downloadable, current_uploaded, current_uploadable, 1,
                            current_download_estimate, current_upload_estimate, 0);
            CHECK(!callback_was_called);
            CHECK(callback_was_called_2);
            CHECK(downloaded == current_downloaded);
            CHECK(downloadable == original_downloadable);
            CHECK(upload_estimate == std::min(1.0, current_uploaded / double(original_uploadable)));
            CHECK(download_estimate == current_download_estimate);

            // Fourth callback, last one for the download notifier
            callback_was_called_2 = false;
            current_uploaded = 220;
            current_uploadable = 410;
            current_downloaded = 192;
            current_downloadable = 591;
            current_upload_estimate = current_uploaded / double(current_uploadable);
            current_download_estimate = current_downloaded / double(current_downloadable);
            progress.update(current_downloaded, current_downloadable, current_uploaded, current_uploadable, 1,
                            current_download_estimate, current_upload_estimate, 0);
            CHECK(!callback_was_called);
            CHECK(!callback_was_called_2);
        }

        SECTION("for multiple notifiers, same direction") {
            // Prime the progress updater
            uint64_t current_uploaded = 16;
            uint64_t current_uploadable = 201;
            uint64_t current_downloaded = 68;
            uint64_t current_downloadable = 182;
            double current_upload_estimate = current_uploaded / double(current_uploadable);
            double current_download_estimate = current_downloaded / double(current_downloadable);

            const uint64_t original_downloadable = current_downloadable;
            progress.update(current_downloaded, current_downloadable, current_uploaded, current_uploadable, 1,
                            current_download_estimate, current_upload_estimate, 0);

            progress.register_callback(
                [&](auto xferred, auto xferable, double ep) {
                    transferred = xferred;
                    transferrable = xferable;
                    download_estimate = ep;
                    callback_was_called = true;
                },
                NotifierType::download, false, 0);
            REQUIRE(callback_was_called);

            // Now manually call the notifier handler a few times.
            callback_was_called = false;
            current_uploaded = 36;
            current_uploadable = 310;
            current_downloaded = 171;
            current_downloadable = 185;
            current_upload_estimate = current_uploaded / double(current_uploadable);
            current_download_estimate = current_downloaded / double(current_downloadable);

            progress.update(current_downloaded, current_downloadable, current_uploaded, current_uploadable, 1,
                            current_download_estimate, current_upload_estimate, 0);
            CHECK(callback_was_called);
            CHECK(transferred == current_downloaded);
            CHECK(transferrable == original_downloadable);

            // Register a second notifier.
            bool callback_was_called_2 = false;
            uint64_t downloaded = 0;
            uint64_t downloadable = 0;
            const uint64_t original_downloadable_2 = current_downloadable;
            progress.register_callback(
                [&](auto xferred, auto xferable, double ep) {
                    downloaded = xferred;
                    downloadable = xferable;
                    download_estimate = ep;
                    callback_was_called_2 = true;
                },
                NotifierType::download, false, 0);
            REQUIRE(callback_was_called_2);

            // Second callback, last one for first notifier
            callback_was_called = false;
            callback_was_called_2 = false;
            current_uploaded = 36;
            current_uploadable = 310;
            current_downloaded = 182;
            current_downloadable = 190;
            current_upload_estimate = current_uploaded / double(current_uploadable);
            current_download_estimate = current_downloaded / double(current_downloadable);
            progress.update(current_downloaded, current_downloadable, current_uploaded, current_uploadable, 1,
                            current_download_estimate, current_upload_estimate, 0);
            CHECK(callback_was_called);
            CHECK(transferred == current_downloaded);
            CHECK(transferrable == original_downloadable);
            CHECK(callback_was_called_2);
            CHECK(downloaded == current_downloaded);
            CHECK(downloadable == original_downloadable_2);
            CHECK(download_estimate == current_download_estimate);

            // Third callback, last one for second notifier
            callback_was_called = false;
            callback_was_called_2 = false;
            current_uploaded = 36;
            current_uploadable = 310;
            current_downloaded = 189;
            current_downloadable = 250;
            current_upload_estimate = current_uploaded / double(current_uploadable);
            current_download_estimate = current_downloaded / double(current_downloadable);
            progress.update(current_downloaded, current_downloadable, current_uploaded, current_uploadable, 1,
                            current_download_estimate, current_upload_estimate, 0);
            CHECK(!callback_was_called);
            CHECK(callback_was_called_2);
            CHECK(downloaded == current_downloaded);
            CHECK(downloadable == original_downloadable_2);
            CHECK(download_estimate == current_download_estimate);

            // Fourth callback
            callback_was_called_2 = false;
            current_uploaded = 36;
            current_uploadable = 310;
            current_downloaded = 201;
            current_downloadable = 289;
            current_upload_estimate = current_uploaded / double(current_uploadable);
            current_download_estimate = current_downloaded / double(current_downloadable);
            progress.update(current_downloaded, current_downloadable, current_uploaded, current_uploadable, 1,
                            current_download_estimate, current_upload_estimate, 0);
            CHECK(!callback_was_called_2);
        }

        SECTION("download notifiers handle transferrable decreasing") {
            // Prime the progress updater
            current_transferred = 60;
            current_transferrable = 501;
            const uint64_t original_transferrable = current_transferrable;
            double current_estimate = current_transferred / double(current_transferrable);
            progress.update(current_transferred, current_transferrable, 21, 26, 1, current_estimate, 21 / double(26),
                            0);

            progress.register_callback(
                [&](auto xferred, auto xferable, double ep) {
                    transferred = xferred;
                    transferrable = xferable;
                    callback_was_called = true;
                    download_estimate = ep;
                },
                NotifierType::download, false, 0);
            REQUIRE(callback_was_called);

            // Download some data but also drop the total. transferrable should
            // update because it decreased.
            callback_was_called = false;
            current_transferred = 160;
            current_transferrable = 451;
            current_estimate = current_transferred / double(current_transferrable);
            progress.update(current_transferred, current_transferrable, 25, 26, 1, current_estimate, 26 / double(26),
                            0);
            CHECK(callback_was_called);
            CHECK(transferred == current_transferred);
            CHECK(transferrable == current_transferrable);
            CHECK(current_estimate == download_estimate);

            // Increasing current_transferrable should not increase transferrable
            const uint64_t previous_transferrable = current_transferrable;
            callback_was_called = false;
            current_transferrable = 1000;
            current_estimate = current_transferred / double(current_transferrable);
            progress.update(current_transferred, current_transferrable, 68, 191, 1, current_estimate,
                            68 / double(191), 0);
            CHECK(callback_was_called);
            CHECK(transferred == current_transferred);
            CHECK(transferrable == previous_transferrable);
            CHECK(download_estimate == current_estimate);

            // Transferrable dropping to be equal to transferred should notify
            // and then expire the notifier
            callback_was_called = false;
            current_transferred = 200;
            current_transferrable = current_transferred;
            current_estimate = current_transferred / double(current_transferrable);
            progress.update(current_transferred, current_transferrable, 191, 192, 1, current_estimate,
                            191 / double(192), 0);
            CHECK(callback_was_called);
            CHECK(transferred == current_transferred);
            CHECK(transferrable == current_transferred);
            CHECK(current_estimate == download_estimate);

            // The notifier should be unregistered at this point, and not fire.
            callback_was_called = false;
            current_transferred = original_transferrable + 250;
            current_transferrable = 1228;
            current_estimate = current_transferred / double(current_transferrable);

            progress.update(current_transferred, current_transferrable, 199, 591, 1, current_estimate,
                            199 / double(591), 0);
            CHECK(!callback_was_called);
        }
    }

    SECTION("flx streaming notifiers") {
        // clang-format off
        TestValues test_values = GENERATE(
            // registers at the begining and should see all entries.
            TestValues{{
                TestInputValue{TestInputValue::IsRegistration{}},
                TestInputValue{0, 0, 0, 0},
                TestInputValue{0, 1, 200, 200},
                TestInputValue{1, 0.2, 300, 600},
                TestInputValue{1, 0.4, 400, 600},
                TestInputValue{1, 0.8, 600, 700},
                TestInputValue{1, 1, 700, 700},
                TestInputValue{2, 0.3, 800, 1000},
                TestInputValue{2, 0.6, 900, 1000},
                TestInputValue{2, 1, 1000, 1000},
            }, {
                ProgressEntry{0, 0, 0},
                ProgressEntry{200, 200, 1},
                ProgressEntry{300, 600, 0.2},
                ProgressEntry{400, 600, 0.4},
                ProgressEntry{600, 700, 0.8},
                ProgressEntry{700, 700, 1},
                ProgressEntry{800, 1000, 0.3},
                ProgressEntry{900, 1000, 0.6},
                ProgressEntry{1000, 1000, 1},
            }, 1},
            // registers in the middle of the initial download
            TestValues{{
                TestInputValue{1, 0.2, 300, 600},
                TestInputValue{1, 0.4, 400, 600},
                TestInputValue{TestInputValue::IsRegistration{}},
                TestInputValue{1, 0.8, 600, 700},
                TestInputValue{1, 1, 700, 700},
            }, {
                ProgressEntry{400, 600, 0.4},
                ProgressEntry{600, 700, 0.8},
                ProgressEntry{700, 700, 1.0},
            }, 1},
            // registers for a query version that's already up-to-date - should get an immediate update
            // with a progress estimate of 1 and whatever the current transferred/transferrable numbers are
            TestValues{{
                TestInputValue{2, 0.5, 800, 900},
                TestInputValue{2, 1, 900, 900},
                TestInputValue{TestInputValue::IsRegistration{}},
                TestInputValue{2, 1, 1000, 1000}
            }, {
                ProgressEntry{900, 900, 1},
                ProgressEntry{1000, 1000, 1},
            }, 1},
            // new subscription is added after registration which results in more data being downloaded
            TestValues{{
                TestInputValue{2, 1, 900, 900},
                TestInputValue{TestInputValue::IsRegistration{}},
                TestInputValue{3, 0, 900, 1000},
                TestInputValue{3, 1, 1000, 1000}
            }, {
                ProgressEntry{900, 900, 1},
                ProgressEntry{900, 1000, 0},
                ProgressEntry{1000, 1000, 1},
            }, 1},
            // new subscription is added after registration which doesn't result in more data being downloaded
            TestValues{{
                TestInputValue{2, 1, 900, 900},
                TestInputValue{TestInputValue::IsRegistration{}},
                TestInputValue{3, 0, 900, 900},
                TestInputValue{3, 1, 900, 900}
            }, {
                ProgressEntry{900, 900, 1},
                ProgressEntry{900, 900, 0},
                ProgressEntry{900, 900, 1},
            }, 1}
        );
        // clang-format on

        auto logger = util::Logger::get_default_logger();
        auto progress_output = util::make_bind<WaitableProgress>(logger, "flx non-streaming download");

        uint64_t snapshot = 1;
        for (const auto& input_val : test_values.input_values) {
            if (input_val.is_registration) {
                progress.register_callback(progress_output->make_cb(), NotifierType::download, true,
                                           test_values.registered_at_query_version);
                continue;
            }
            progress.update(input_val.transferred, input_val.transferrable, 0, 0, ++snapshot, input_val.cur_estimate,
                            0.0, input_val.query_version);
        }

        const auto output_values = progress_output->wait_for_full_sync();

        REQUIRE_THAT(output_values, Catch::Matchers::Equals(test_values.expected_values));
    }

    SECTION("flx non-streaming notifiers") {
        // clang-format off
        TestValues test_values = GENERATE(
            // registers for query version 1 on an empty realm - should see the full progression
            // of query version 1 and nothing else.
            TestValues{{
                TestInputValue{TestInputValue::IsRegistration{}},
                TestInputValue{0, 0, 0, 0},
                TestInputValue{0, 1, 200, 200},
                TestInputValue{1, 0.2, 300, 600},
                TestInputValue{1, 0.4, 400, 600},
                TestInputValue{1, 0.8, 600, 700},
                TestInputValue{1, 1, 700, 700},
                TestInputValue{2, 0.3, 800, 1000},
                TestInputValue{2, 0.6, 900, 1000},
                TestInputValue{2, 1, 1000, 1000},
            }, {
                ProgressEntry{300, 600, 0.2},
                ProgressEntry{400, 600, 0.4},
                ProgressEntry{600, 600, 0.8},
                ProgressEntry{700, 600, 1.0},
            }, 1},
            // registers a notifier in the middle of syncing the target query version
            TestValues{{
                TestInputValue{1, 0.2, 300, 600},
                TestInputValue{1, 0.4, 400, 600},
                TestInputValue{TestInputValue::IsRegistration{}},
                TestInputValue{1, 0.8, 600, 700},
                TestInputValue{1, 1, 700, 700},
                // There's also a progress notification for a regular steady state
                // download message that gets ignored because we're already up-to-date
                TestInputValue{1, 1, 800, 800},
            }, {
                ProgressEntry{400, 600, 0.4},
                ProgressEntry{600, 600, 0.8},
                ProgressEntry{700, 600, 1.0},
            }, 1},
            // registers for a notifier for a later query version - should only see notifications
            // for downloads greater than the requested query version
            TestValues{{
                TestInputValue{TestInputValue::IsRegistration{}},
                TestInputValue{1, 0.8, 700, 700},
                TestInputValue{1, 1, 700, 700},
                TestInputValue{3, 0.5, 800, 900},
                TestInputValue{3, 1, 900, 900},
            }, {
                ProgressEntry{800, 900, 0.5},
                ProgressEntry{900, 900, 1},
            }, 2},
            // registers for a query version that's already up-to-date - should get an immediate update
            // with a progress estimate of 1 and whatever the current transferred/transferrable numbers are
            TestValues{{
                TestInputValue{2, 0.5, 800, 900},
                TestInputValue{2, 1, 900, 900},
                TestInputValue{TestInputValue::IsRegistration{}},
            }, {
                ProgressEntry{900, 900, 1},
            }, 1}
        );
        // clang-format on

        auto logger = util::Logger::get_default_logger();
        auto progress_output = util::make_bind<WaitableProgress>(logger, "flx non-streaming download");

        uint64_t snapshot = 1;
        for (const auto& input_val : test_values.input_values) {
            if (input_val.is_registration) {
                progress.register_callback(progress_output->make_cb(), NotifierType::download, false,
                                           test_values.registered_at_query_version);
                continue;
            }
            progress.update(input_val.transferred, input_val.transferrable, 0, 0, ++snapshot, input_val.cur_estimate,
                            0.0, input_val.query_version);
        }

        const auto output_values = progress_output->wait_for_full_sync();

        REQUIRE_THAT(output_values, Catch::Matchers::Equals(test_values.expected_values));
    }
}

#ifdef REALM_ENABLE_AUTH_TESTS

struct TestSetup {
    TableRef get_table(const SharedRealm& r)
    {
        return r->read_group().get_table("class_" + table_name);
    }

    size_t add_objects(SharedRealm& r, int num, size_t data_size = 1024 * 1024)
    {
        CppContext ctx(r);
        for (int i = 0; i < num; ++i) {
            // use specifically separate transactions for a bit of history
            r->begin_transaction();
            Object::create(ctx, r, StringData(table_name), std::any(make_one(i, data_size)));
            r->commit_transaction();
        }
        return get_table(r)->size();
    }

    AutoVerifiedEmailCredentials create_user_and_log_in()
    {
        return ::create_user_and_log_in(app());
    }

    virtual SyncTestFile make_config() = 0;
    virtual AnyDict make_one(int64_t idx, size_t data_size) = 0;
    virtual SharedApp app() const = 0;
    virtual const AppSession& app_session() const = 0;

    std::string table_name;
};

struct PBS : TestSetup {
    PBS()
    {
        table_name = "Dog";
    }

    SyncTestFile make_config() override
    {
        return SyncTestFile(session.app()->current_user(), partition, get_default_schema());
    }

    AnyDict make_one(int64_t /* idx */, size_t data_size) override
    {
        return AnyDict{{"_id", std::any(ObjectId::gen())},
                       {"breed", std::string("bulldog")},
                       {"name", random_string(data_size)}};
    }

    SharedApp app() const override
    {
        return session.app();
    }

    const AppSession& app_session() const override
    {
        return session.app_session();
    }

    TestAppSession session;
    const std::string partition = random_string(100);
};

static std::ostream& operator<<(std::ostream& os, const PBS&)
{
    return os << "PBS";
}

struct FLX : TestSetup {
    FLX(const std::string& app_id = "flx_sync_progress")
        : harness(app_id)
    {
        table_name = harness.schema().begin()->name;
    }

    SyncTestFile make_config() override
    {
        auto config = harness.make_test_file();
        add_subscription(*config.sync_config);
        return config;
    }

    void add_subscription(SyncConfig& config)
    {
        config.rerun_init_subscription_on_open = true;
        config.subscription_initializer = [&](SharedRealm&& realm) {
            add_subscription(realm);
        };
    }

    void add_subscription(SharedRealm& realm)
    {
        auto sub = realm->get_latest_subscription_set().make_mutable_copy();
        sub.insert_or_assign(Query(get_table(realm)));
        sub.commit();
    }

    AnyDict make_one(int64_t idx, size_t data_size) override
    {
        return AnyDict{{"_id", ObjectId::gen()},
                       {"queryable_int_field", idx},
                       {"queryable_str_field", random_string(data_size)}};
    }

    SharedApp app() const override
    {
        return harness.app();
    }

    const AppSession& app_session() const override
    {
        return harness.session().app_session();
    }

    FLXSyncTestHarness harness;
};

static std::ostream& operator<<(std::ostream& os, const FLX&)
{
    return os << "FLX";
}

struct ProgressIncreasesMatcher : Catch::Matchers::MatcherGenericBase {
    enum MatchMode { ByteCountOnly, All };
    ProgressIncreasesMatcher() = default;
    explicit ProgressIncreasesMatcher(MatchMode mode)
        : m_mode(mode)
    {
    }

    bool match(std::vector<ProgressEntry> const& entries) const
    {
        if (entries.size() < 1) {
            return false;
        }

        auto last = std::ref(entries.front());
        for (size_t i = 1; i < entries.size(); ++i) {
            ProgressEntry const& cur = entries[i];
            if (cur.transferred < last.get().transferred) {
                return false;
            }
            if (m_mode == All && cur.estimate < last.get().estimate) {
                return false;
            }
            last = cur;
        }
        return true;
    }

    std::string describe() const override
    {
        return "progress notifications all increase";
    }

private:
    MatchMode m_mode = All;
};

TEMPLATE_TEST_CASE("progress notifications fire immediately when fully caught up", "[baas][progress][sync]", PBS, FLX)
{
    TestType pbs_setup;
    auto logger = util::Logger::get_default_logger();

    auto validate_noop_entry = [&](const std::vector<ProgressEntry>& entries, std::string context) {
        UNSCOPED_INFO("validating noop non-streaming entry " << context);
        REQUIRE(entries.size() == 1);
        const auto& entry = entries.front();
        REQUIRE(entry.transferred >= entry.transferrable);
        REQUIRE(entry.estimate >= 1.0);
    };

    SECTION("empty async open results in progress notification") {
        auto config = pbs_setup.make_config();
        auto async_open_task = Realm::get_synchronized_realm(config);
        auto async_open_progress = util::make_bind<WaitableProgress>(logger, "async open non-streaming progress ");
        async_open_task->register_download_progress_notifier(async_open_progress->make_cb());
        auto future = async_open_task->start();
        auto realm = Realm::get_shared_realm(std::move(future).get());
        auto noop_download_progress = util::make_bind<WaitableProgress>(logger, "non-streaming download ");
        auto noop_token = realm->sync_session()->register_progress_notifier(
            noop_download_progress->make_cb(), SyncSession::ProgressDirection::download, false);
        // The registration token for a non-streaming notifier that was expired at registration time
        // is zero because it's invoked immediately and never registered for further notifications.
        CHECK(noop_token == 0);

        auto async_open_entries = async_open_progress->wait_for_full_sync();
        REQUIRE_THAT(async_open_entries, ProgressIncreasesMatcher{});
        validate_noop_entry(noop_download_progress->wait_for_full_sync(), "noop_download_progress");
    }

    SECTION("synchronous open then waiting for download then noop notification") {
        {
            auto fill_data_config = pbs_setup.make_config();
            auto fill_data_realm = Realm::get_shared_realm(fill_data_config);
            pbs_setup.add_objects(fill_data_realm, 5);
            wait_for_upload(*fill_data_realm);
        }

        auto config = pbs_setup.make_config();
        auto realm = Realm::get_shared_realm(config);
        auto initial_progress = util::make_bind<WaitableProgress>(logger, "streaming initial progress ");
        realm->sync_session()->register_progress_notifier(initial_progress->make_cb(), NotifierType::download, true);

        auto initial_entries = initial_progress->wait_for_full_sync();
        REQUIRE(!initial_entries.empty());
        REQUIRE_THAT(initial_entries, ProgressIncreasesMatcher{});

        auto noop_download_progress = util::make_bind<WaitableProgress>(logger, "non-streaming noop download ");
        auto noop_token = realm->sync_session()->register_progress_notifier(
            noop_download_progress->make_cb(), SyncSession::ProgressDirection::download, false);
        // The registration token for a non-streaming notifier that was expired at registration time
        // is zero because it's invoked immediately and never registered for further notifications.
        CHECK(noop_token == 0);

        validate_noop_entry(noop_download_progress->wait_for_full_sync(), "noop_download_progress");
    }

    SECTION("uploads") {
        auto config = pbs_setup.make_config();
        auto realm = Realm::get_shared_realm(config);
        auto initial_progress = util::make_bind<WaitableProgress>(logger, "non-streaming initial progress ");

        pbs_setup.add_objects(realm, 5);

        auto token = realm->sync_session()->register_progress_notifier(initial_progress->make_cb(),
                                                                       NotifierType::upload, false);
        auto initial_entries = initial_progress->wait_for_full_sync();
        REQUIRE(!initial_entries.empty());
        REQUIRE_THAT(initial_entries, ProgressIncreasesMatcher{});
        realm->sync_session()->unregister_progress_notifier(token);

        // it's possible that we've reached full synchronization in the progress notifier, but because
        // of the way non-streaming notifiers work, the transferable may be higher for the next
        // non-streaming notifier than for the one that just finished. So we explicitly wait for
        // all uploads to complete to check that registering a noop notifier here is actually a noop.
        wait_for_upload(*realm);

        auto noop_upload_progress = util::make_bind<WaitableProgress>(logger, "non-streaming upload ");
        auto noop_token = realm->sync_session()->register_progress_notifier(
            noop_upload_progress->make_cb(), SyncSession::ProgressDirection::upload, false);
        // The registration token for a non-streaming notifier that was expired at registration time
        // is zero because it's invoked immediately and never registered for further notifications.
        CHECK(noop_token == 0);

        validate_noop_entry(noop_upload_progress->wait_for_full_sync(), "noop_upload_progress");
    }
}


TEMPLATE_TEST_CASE("sync progress: upload progress", "[sync][baas][progress]", PBS, FLX)
{
    TestType setup;

    auto realm = Realm::get_shared_realm(setup.make_config());
    auto sync_session = realm->sync_session();
    auto logger = util::Logger::get_default_logger();
    auto non_streaming_progress = util::make_bind<WaitableProgress>(logger, "non-streaming upload ");
    auto streaming_progress = util::make_bind<WaitableProgress>(logger, "streaming upload ");

    // There is a race between creating the objects and registering the non-streaming notifier
    // since
    sync_session->pause();

    setup.add_objects(realm, 10);
    sync_session->register_progress_notifier(non_streaming_progress->make_cb(), NotifierType::upload, false);
    sync_session->register_progress_notifier(streaming_progress->make_cb(), NotifierType::upload, true);

    sync_session->resume();
    wait_for_upload(*realm);

    auto streaming_entries = streaming_progress->wait_for_full_sync();
    auto non_streaming_entries = non_streaming_progress->wait_for_full_sync();

    REQUIRE(!streaming_entries.empty());
    REQUIRE(!non_streaming_entries.empty());
    REQUIRE_THAT(non_streaming_entries, ProgressIncreasesMatcher{});
    REQUIRE_THAT(streaming_entries, ProgressIncreasesMatcher{ProgressIncreasesMatcher::ByteCountOnly});

    setup.add_objects(realm, 5);
    wait_for_upload(*realm);

    streaming_entries = streaming_progress->wait_for_full_sync();
    REQUIRE_THAT(streaming_entries, ProgressIncreasesMatcher{ProgressIncreasesMatcher::ByteCountOnly});
    REQUIRE(non_streaming_progress->empty());
}

namespace {
struct EstimatesAreValid : Catch::Matchers::MatcherGenericBase {
    size_t initial_object_count = 0;
    EstimatesAreValid(size_t initial_count = 0)
        : initial_object_count(initial_count)
    {
    }

    bool match(std::vector<double> const& entries) const
    {
        // Download progress should always end with an estimate of 1
        if (entries.empty() || entries.back() != 1)
            return false;

        // All estimates should be between 0 and 1
        for (double estimate : entries) {
            if (estimate < 0 || estimate > 1)
                return false;
        }

        // The server will sometimes send us the final non-empty DOWNLOAD with
        // an estimate of 0.9999 and then an empty DOWNLOAD with 1. We can use
        // exact equality here because it's a specific sentinel value and not
        // the result of a computation.
        size_t size = entries.size();
        if (size >= 2 && entries[size - 2] == 0.9999)
            --size;
        if (size == 1)
            return true;

        // The actual progress for the first message should be the number of
        // objects downloaded divided by the total number of objects, but in
        // practice the server starts with a lower estimate so that's only an
        // upper bound.
        double expected_first = double(initial_object_count + 1) / (initial_object_count + size);
        if (entries.front() > expected_first + .01)
            return false;

        // As each of our DOWNLOAD messages have a fixed size, the progress
        // estimate should go up by the same amount each time.
        double expected_step = (1.0 - entries.front()) / (size - 1);
        for (size_t i = 1; i < size; ++i) {
            double expected = entries.front() + i * expected_step;
            if (!WithinRel(entries[i], 0.1).match(expected)) {
                return false;
            }
        }
        return true;
    }

    std::string describe() const override
    {
        return "estimated progress must progress from non-1 to 1 in fixed-size non-zero steps";
    }
};
} // namespace

TEST_CASE("sync progress: flx download progress", "[sync][baas][progress]") {
    static std::optional<FLXSyncTestHarness> harness;

    std::unique_ptr<char[]> buffer;
    const auto create_object = [&](const std::shared_ptr<Realm>& realm, int id) {
        const size_t padding_size = 1024 * 1024;
        if (!buffer)
            buffer = std::make_unique<char[]>(padding_size);
        auto table = realm->read_group().get_table("class_object");
        auto obj = table->create_object_with_primary_key(ObjectId::gen());
        obj.set("int", id);
        // ensure that each object is large enough that it'll be sent in
        // a separate DOWNLOAD message
        obj.set("padding", BinaryData(buffer.get(), padding_size));
    };

    if (!harness) {
        Schema schema{
            {"object",
             {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
              {"int", PropertyType::Int | PropertyType::Nullable},
              {"padding", PropertyType::Data}}},
        };
        realm::app::FLXSyncTestHarness::ServerSchema server_schema{std::move(schema), {"int"}};
        harness.emplace("flx_download_progress", std::move(server_schema));
        harness->load_initial_data([&](const std::shared_ptr<Realm>& realm) {
            for (int i = 0; i < 5; ++i)
                create_object(realm, i);
        });
    }

    SyncTestFile config = harness->make_test_file();

    SECTION("async open with no subscriptions") {
        auto task = Realm::get_synchronized_realm(config);
        std::vector<double> estimates;
        task->register_download_progress_notifier([&](uint64_t, uint64_t, double estimate) {
            // Note that no locking is needed here despite this being called on
            // a background thread as the test provides the required synchronization.
            // We register the notifier at a point where no notifications should
            // be in process, and then wait on a Future which should be fulfilled
            // after the final progress update is sent. If tsan complains about
            // this, it means that progress updates are being sent at a time
            // outside of the expected window and that's the bug to fix.
            estimates.push_back(estimate);
        });
        task->start().get();
        // A download happens for the schema, but we now don't report that
        REQUIRE(estimates.size() == 0);
    }

    SECTION("async open with initial subscriptions") {
        config.sync_config->subscription_initializer = [](const std::shared_ptr<Realm>& realm) {
            subscribe_to_all(*realm);
        };
        auto task = Realm::get_synchronized_realm(config);
        std::vector<double> estimates;
        task->register_download_progress_notifier([&](uint64_t, uint64_t, double estimate) {
            // See above about the lack of locking
            estimates.push_back(estimate);
        });
        task->start().get();

        // Since our objects are larger than the server's soft limit for batching
        // (1 MB), we expect to receive a separate DOWNLOAD message for each
        // object. We also happen to get an empty DOWNLOAD at the end, but we
        // don't want to require that.
        REQUIRE(estimates.size() >= 5);
        REQUIRE_THAT(estimates, EstimatesAreValid());
    }

    SECTION("multiple subscription updates which each trigger some downloads") {
        auto realm = successfully_async_open_realm(config);
        auto table = realm->read_group().get_table("class_object");
        auto col = table->get_column_key("int");

        std::vector<double> estimates;
        realm->sync_session()->register_progress_notifier(
            [&](uint64_t, uint64_t, double estimate) {
                // See above about the lack of locking
                estimates.push_back(estimate);
            },
            SyncSession::ProgressDirection::download, true);

        for (int i = 4; i > -2; i -= 2) {
            auto sub_set = realm->get_latest_subscription_set().make_mutable_copy();
            sub_set.insert_or_assign(table->where().greater(col, i));
            sub_set.commit().get_state_change_notification(sync::SubscriptionSet::State::Complete).get();

            // We get a variable number of DOWNLOAD messages per update but it should always be at least one
            REQUIRE(estimates.size() >= 1);
            REQUIRE_THAT(estimates, EstimatesAreValid());

            estimates.clear();
        }
    }

    SECTION("add subscription which doesn't add new objects") {
        auto realm = successfully_async_open_realm(config);
        auto table = realm->read_group().get_table("class_object");
        auto col = table->get_column_key("int");

        std::vector<double> estimates;
        realm->sync_session()->register_progress_notifier(
            [&](uint64_t, uint64_t, double estimate) {
                // See above about the lack of locking
                estimates.push_back(estimate);
            },
            SyncSession::ProgressDirection::download, true);

        {
            auto sub_set = realm->get_latest_subscription_set().make_mutable_copy();
            sub_set.insert_or_assign(table->where().less(col, 5));
            sub_set.commit().get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
        }

        estimates.clear();

        // This subscription change should not actually result in any new objects
        {
            auto sub_set = realm->get_latest_subscription_set().make_mutable_copy();
            sub_set.insert_or_assign(table->where().less(col, 10));
            sub_set.commit().get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
        }

        // We expect just a single update with progress_estimate=1, but the
        // server could legally send us multiple empty DOWNLOADs
        REQUIRE(estimates.size() >= 1);
        REQUIRE_THAT(estimates, EstimatesAreValid());
    }

    SECTION("add new objects while in the steady state") {
        config.sync_config->subscription_initializer = [](const std::shared_ptr<Realm>& realm) {
            subscribe_to_all(*realm);
        };
        auto online_realm = successfully_async_open_realm(config);

        SyncTestFile config2 = harness->make_test_file();
        config2.sync_config->subscription_initializer = config.sync_config->subscription_initializer;
        auto suspended_realm = successfully_async_open_realm(config2);

        std::vector<double> online_estimates;
        online_realm->sync_session()->register_progress_notifier(
            [&](uint64_t, uint64_t, double estimate) {
                // See above about the lack of locking
                online_estimates.push_back(estimate);
            },
            SyncSession::ProgressDirection::download, true);

        std::vector<double> suspended_estimates;
        suspended_realm->sync_session()->register_progress_notifier(
            [&](uint64_t, uint64_t, double estimate) {
                // See above about the lack of locking
                suspended_estimates.push_back(estimate);
            },
            SyncSession::ProgressDirection::download, true);

        // We should get the initial notification that downloads are already complete
        wait_for_download(*online_realm);
        wait_for_download(*suspended_realm);
        REQUIRE(online_estimates == std::vector{1.0});
        REQUIRE(suspended_estimates == std::vector{1.0});

        online_estimates.clear();
        suspended_estimates.clear();
        suspended_realm->sync_session()->pause();

        harness->do_with_new_realm([&](const std::shared_ptr<Realm>& realm) {
            subscribe_to_all(*realm);
            for (int i = 5; i < 10; ++i) {
                realm->begin_transaction();
                create_object(realm, i);
                realm->commit_transaction();
                wait_for_upload(*realm);

                // The currently connected Realm should receive exactly one
                // download message telling it that the download is complete as
                // it's always staying up to date
                wait_for_download(*online_realm);
                REQUIRE(online_estimates == std::vector{1.0});
                online_estimates.clear();
            }
        });

        // Once it reconnects, the offline Realm should receive at least five
        // separate DOWNLOAD messages, each of which should include actual
        // progress information towards completion
        suspended_realm->sync_session()->resume();
        wait_for_download(*suspended_realm);
        REQUIRE(suspended_estimates.size() >= 5);
        REQUIRE_THAT(suspended_estimates, EstimatesAreValid(5));
    }

    SECTION("cleanup") {
        harness.reset();
    }
}

TEMPLATE_TEST_CASE("sync progress: upload progress during client reset", "[sync][baas][progress][client reset]", PBS,
                   FLX)
{
    std::mutex progress_mutex;
    std::vector<ProgressEntry> streaming_progress;
    std::vector<ProgressEntry> non_streaming_progress;

    enum TestMode { NO_CHANGES, LOCAL_CHANGES, REMOTE_CHANGES, BOTH_CHANGED, BOTH_CHANGED_W_DISCARD };
    auto xlate_test_mode = [](TestMode tm) -> std::string_view {
        switch (tm) {
            case NO_CHANGES:
                return "no local or remote changes";
            case LOCAL_CHANGES:
                return "local changes only";
            case REMOTE_CHANGES:
                return "remote changes only";
            case BOTH_CHANGED:
                return "both local and remote changes";
            case BOTH_CHANGED_W_DISCARD:
                return "both local and remote changes";
        }
        FAIL(util::format("Missing case for unhandled TestMode value: ", static_cast<int>(tm)));
        REALM_UNREACHABLE();
    };

    auto logger = util::Logger::get_default_logger();
    TestType setup;
    auto test_mode = GENERATE(TestMode::NO_CHANGES, TestMode::LOCAL_CHANGES, TestMode::REMOTE_CHANGES,
                              TestMode::BOTH_CHANGED, TestMode::BOTH_CHANGED_W_DISCARD);

    // Set up the main realm for the test
    auto config = setup.make_config();
    auto&& [reset_future, reset_handler] = reset_utils::make_client_reset_handler();
    config.sync_config->notify_after_client_reset = reset_handler;
    if (test_mode == TestMode::BOTH_CHANGED_W_DISCARD) {
        config.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;
    }
    else {
        config.sync_config->client_resync_mode = ClientResyncMode::Recover;
    }

    logger->debug("PROGRESS TEST: %1 upload progress notifications after %2 client reset with %3", setup,
                  config.sync_config->client_resync_mode, xlate_test_mode(test_mode));

    // Functions to create the progress notification callbacks
    auto make_streaming_cb = [&](std::string_view desc) {
        return [&, desc](uint64_t transferred, uint64_t transferrable, double estimate) {
            logger->debug("PROGRESS TEST: %1 Progress callback called xferred: %2, xferrable: %3, estimate: %4", desc,
                          transferred, transferrable, estimate_to_string(estimate));
            std::lock_guard lk(progress_mutex);
            streaming_progress.push_back(ProgressEntry{transferred, transferrable, estimate});
        };
    };
    auto make_non_streaming_cb = [&](std::string_view desc) {
        return [&, desc](uint64_t transferred, uint64_t transferrable, double estimate) {
            logger->debug("PROGRESS TEST: %1 Progress callback called xferred: %2, xferrable: %3, estimate: %4", desc,
                          transferred, transferrable, estimate_to_string(estimate));
            std::lock_guard lk(progress_mutex);
            non_streaming_progress.push_back(ProgressEntry{transferred, transferrable, estimate});
        };
    };

    auto wait_for_sync = [](SharedRealm& r) {
        // If a FLX session, also wait for the subscription to complete
        if (r->config().sync_config->flx_sync_requested) {
            auto sub = r->get_latest_subscription_set();
            REQUIRE(sub.state() != sync::SubscriptionSet::State::Error);
            if (sub.state() != sync::SubscriptionSet::State::Complete) {
                auto result =
                    sub.get_state_change_notification(sync::SubscriptionSet::State::Complete).get_no_throw();
                REQUIRE(result.is_ok());
                REQUIRE(result.get_value() == sync::SubscriptionSet::State::Complete);
            }
        }
        wait_for_download(*r);
        wait_for_upload(*r);
    };

    {
        // Setup the realm and add some data (FLX subscription is added during initialization)
        auto realm = Realm::get_shared_realm(config);

        // For FLX sessions, don't create any more subscriptions for future realms
        config.sync_config->rerun_init_subscription_on_open = false;
        config.sync_config->subscription_initializer = nullptr;

        // Add some data and wait for upload
        setup.add_objects(realm, 10, 100);
        wait_for_sync(realm);                       // wait for sync/subs to complete
        realm->sync_session()->shutdown_and_wait(); // Close the sync session

        // Set up some local changes if the test calls for it
        if (test_mode == TestMode::LOCAL_CHANGES || test_mode == TestMode::BOTH_CHANGED ||
            test_mode == TestMode::BOTH_CHANGED_W_DISCARD) {
            logger->trace("PROGRESS TEST: adding local objects");
            setup.add_objects(realm, 5, 100); // Add some local objects while offline
        }

        // Set up some remote changes if the test calls for it
        if (test_mode == TestMode::REMOTE_CHANGES || test_mode == TestMode::BOTH_CHANGED ||
            test_mode == TestMode::BOTH_CHANGED_W_DISCARD) {
            logger->trace("PROGRESS TEST: adding remote objects");
            // Make a new config for a different user
            setup.create_user_and_log_in();
            auto remote_config = setup.make_config(); // Includes the new user just created
            auto remote_realm = Realm::get_shared_realm(remote_config);
            setup.add_objects(remote_realm, 5, 100); // Add some objects remotely
            wait_for_sync(remote_realm);             // wait for sync/subs to complete
        }
        reset_utils::trigger_client_reset(setup.app_session(), realm);
    }
    auto realm = Realm::get_shared_realm(config);
    // Register progress notifiers
    realm->sync_session()->register_progress_notifier(make_non_streaming_cb("Non-Streaming Upload"),
                                                      NotifierType::upload, false);
    realm->sync_session()->register_progress_notifier(make_streaming_cb("Streaming Upload"), NotifierType::upload,
                                                      true);

    // Wait for the client reset to complete
    auto status = wait_for_future(std::move(reset_future)).get_no_throw();
    if (!status.is_ok()) {
        FAIL(status.get_status());
    }

    // Progress notifications may not have been sent yet - wait for sync after client reset
    wait_for_download(*realm);
    wait_for_upload(*realm);

    {
        std::lock_guard<std::mutex> lk(progress_mutex);
        logger->debug("PROGRESS TEST: retrieved progress calls: streaming - %1, non-streaming - %2",
                      streaming_progress.size(), non_streaming_progress.size());

        auto print_progress = [&logger](const std::vector<ProgressEntry>& entries) {
            if (!logger->would_log(util::Logger::Level::trace))
                return; // don't print if wouldn't log
            for (size_t i = 0; i < entries.size(); ++i) {
                auto& entry = entries[i];
                logger->trace("PROGRESS TEST: entry[%1] - transferrable: %2 - transferred: %3 - estimate: %4", i,
                              entry.transferrable, entry.transferred, estimate_to_string(entry.estimate));
            }
        };
        logger->trace("PROGRESS TEST: streaming progress size: %1", streaming_progress.size());
        print_progress(streaming_progress);
        logger->trace("PROGRESS TEST: non-streaming progress size: %1", non_streaming_progress.size());
        print_progress(non_streaming_progress);

        // Validations for no changes, remote only changes, or both changes with discard local client reset
        if (test_mode == TestMode::NO_CHANGES || test_mode == TestMode::REMOTE_CHANGES ||
            test_mode == TestMode::BOTH_CHANGED_W_DISCARD) {
            // Sometimes a second upload would be sent, resulting in a size of 2
            REQUIRE(streaming_progress.size() > 0);
            REQUIRE(streaming_progress[0] == ProgressEntry{0, 0, 1.0});
            REQUIRE(non_streaming_progress.size() > 0);
            // Needs to be changed to 1.0 after PR #7957 is merged
            REQUIRE(non_streaming_progress[0] == ProgressEntry{0, 0, 0.0});
        }
        // Validations for local changes only or both local and remote changes
        else if (test_mode == TestMode::LOCAL_CHANGES || test_mode == TestMode::BOTH_CHANGED) {
            // Multiple notifications may sent for the changes to upload after client reset
            if (config.sync_config->flx_sync_requested) {
                // FLX sessions report upload progress as a single notification
                REQUIRE(streaming_progress.size() > 0);
                REQUIRE(non_streaming_progress.size() > 0);
            }
            else {
                // PBS sessions report upload progress when changes are uploaded and when upload is acked
                REQUIRE(streaming_progress.size() > 1);
                REQUIRE(non_streaming_progress.size() > 1);
            }
            REQUIRE(streaming_progress.back().estimate == 1.0);     // should end with progress of 1.0
            REQUIRE(non_streaming_progress.back().estimate == 1.0); // should end with progress of 1.0
        }
        else {
            // Unhandled TestMode case
            FAIL(util::format("Unhandled TestMode case: ", static_cast<int>(test_mode)));
        }
    }

    streaming_progress.clear();
    non_streaming_progress.clear();

    // Verify the streaming notifications are still received and non-streaming notifications have expired
    setup.add_objects(realm, 5, 100);
    wait_for_upload(*realm);

    // More streaming upload notifications were received
    REQUIRE(streaming_progress.size() > 0);
    // Non-streaming upload notification callback was expired and no more were received
    REQUIRE(non_streaming_progress.size() == 0);
}

#endif
