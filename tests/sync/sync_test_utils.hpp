////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
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

#ifndef REALM_SYNC_TEST_UTILS_HPP
#define REALM_SYNC_TEST_UTILS_HPP

#include "catch.hpp"

#include "util/test_file.hpp"

#include "sync/sync_manager.hpp"
#include "sync/sync_session.hpp"
#include "sync/impl/sync_file.hpp"
#include "sync/impl/sync_metadata.hpp"

namespace realm {

/// Open a Realm at a given path, creating its files.
bool create_dummy_realm(std::string path);
void reset_test_directory(const std::string& base_path);
bool results_contains_user(SyncUserMetadataResults& results, const std::string& identity);
std::string tmp_dir();
std::vector<char> make_test_encryption_key(const char start = 0);
bool session_is_active(const SyncSession& session);
bool session_is_inactive(const SyncSession& session);

/// Create a properly configured `SyncSession` for test purposes.
template <typename FetchAccessToken, typename ErrorHandler>
std::shared_ptr<SyncSession> sync_session(SyncServer& server, std::shared_ptr<SyncUser> user, const std::string& path,
                                          FetchAccessToken&& fetch_access_token, ErrorHandler&& error_handler,
                                          SyncSessionStopPolicy stop_policy=SyncSessionStopPolicy::AfterChangesUploaded,
                                          std::string* on_disk_path=nullptr)
{
    std::string url = server.base_url() + path;
    SyncTestFile config({user, url, std::move(stop_policy),
        [&](const std::string& path, const SyncConfig& config, std::shared_ptr<SyncSession> session) {
            auto token = fetch_access_token(path, config.realm_url);
            session->refresh_access_token(std::move(token), config.realm_url);
        }, std::forward<ErrorHandler>(error_handler)});
    if (on_disk_path) {
        *on_disk_path = config.path;
    }

    std::shared_ptr<SyncSession> session;
    {
        auto realm = Realm::get_shared_realm(config);
        session = SyncManager::shared().get_session(config.path, *config.sync_config);
    }
    return session;
}

} // namespace realm

#define REQUIRE_DIR_EXISTS(macro_path) do { \
    DIR *dir_listing = opendir((macro_path).c_str()); \
    CHECK(dir_listing); \
    if (dir_listing) closedir(dir_listing); \
} while (0)

#define REQUIRE_DIR_DOES_NOT_EXIST(macro_path) do { \
    DIR *dir_listing = opendir((macro_path).c_str()); \
    CHECK(dir_listing == NULL); \
    if (dir_listing) closedir(dir_listing); \
} while (0)

#endif // REALM_SYNC_TEST_UTILS_HPP
