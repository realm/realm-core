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

#include <realm/object-store/sync/app.hpp>
#include <realm/object-store/sync/generic_network_transport.hpp>
#include <realm/object-store/sync/impl/sync_file.hpp>
#include <realm/object-store/sync/impl/sync_metadata.hpp>
#include <realm/object-store/sync/sync_session.hpp>

#include <realm/util/functional.hpp>
#include <realm/util/function_ref.hpp>

#include "common_utils.hpp"

#include "../event_loop.hpp"
#include "../test_file.hpp"
#include "../test_utils.hpp"

#if REALM_ENABLE_SYNC

#include <realm/sync/config.hpp>
#include <realm/object-store/sync/sync_manager.hpp>

#include <realm/sync/client.hpp>
#include <realm/sync/noinst/server/server.hpp>

#endif // REALM_ENABLE_SYNC

// disable the tests that rely on having baas available on the network
// but allow opt-in by building with REALM_ENABLE_AUTH_TESTS=1
#ifndef REALM_ENABLE_AUTH_TESTS
#define REALM_ENABLE_AUTH_TESTS 0
#endif

#ifndef TEST_ENABLE_SYNC_LOGGING
#define TEST_ENABLE_SYNC_LOGGING 0 // change to 1 to enable trace-level logging
#endif

struct TestLogger : realm::util::Logger::LevelThreshold, realm::util::Logger {
    void do_log(realm::util::Logger::Level, std::string const&) override {}
    Level get() const noexcept override
    {
        return Level::off;
    }
    TestLogger()
        : Logger::LevelThreshold()
        , Logger(static_cast<Logger::LevelThreshold&>(*this))
    {
    }
};

using StartImmediately = realm::util::TaggedBool<class StartImmediatelyTag>;

class SyncServer : private realm::sync::Clock {
public:
    struct Config {
        bool start_immediately = true;
        std::string local_dir;
    };

    ~SyncServer();

    void start();
    void stop();

    std::string url_for_realm(realm::StringData realm_name) const;
    std::string base_url() const
    {
        return m_url;
    }
    std::string local_root_dir() const
    {
        return m_local_root_dir;
    }

    template <class R, class P>
    void advance_clock(std::chrono::duration<R, P> duration = std::chrono::seconds(1)) noexcept
    {
        m_now += std::chrono::duration_cast<time_point::duration>(duration).count();
    }

private:
    friend class TestSyncManager;
    SyncServer(const Config& config);
    std::string m_local_root_dir;
    std::unique_ptr<realm::util::Logger> m_logger;
    realm::sync::Server m_server;
    std::thread m_thread;
    std::string m_url;
    std::atomic<time_point::rep> m_now{0};

    time_point now() const noexcept override
    {
        return time_point{time_point::duration{m_now}};
    }
};

struct SyncTestFile : TestFile {
    template <typename ErrorHandler>
    SyncTestFile(const realm::SyncConfig& sync_config, realm::SyncSessionStopPolicy stop_policy,
                 ErrorHandler&& error_handler)
    {
        this->sync_config = std::make_shared<realm::SyncConfig>(sync_config);
        this->sync_config->stop_policy = stop_policy;
        this->sync_config->error_handler = std::forward<ErrorHandler>(error_handler);
        schema_mode = realm::SchemaMode::AdditiveExplicit;
    }

    SyncTestFile(std::shared_ptr<realm::app::App> app = nullptr, std::string name = "",
                 std::string user_name = "test");
    SyncTestFile(std::shared_ptr<realm::SyncUser> user, realm::bson::Bson partition,
                 realm::util::Optional<realm::Schema> schema = realm::util::none);
    SyncTestFile(std::shared_ptr<realm::app::App> app, realm::bson::Bson partition, realm::Schema schema);
    SyncTestFile(std::shared_ptr<realm::SyncUser> user, realm::Schema schema, realm::SyncConfig::FLXSyncEnabled);
};

class TestSyncManager {
public:
    struct Config {
        Config() {}
        realm::app::App::Config app_config;
        std::string base_path;
        realm::SyncManager::MetadataMode metadata_mode = realm::SyncManager::MetadataMode::NoEncryption;
        bool should_teardown_test_directory = true;
        realm::util::Logger::Level sync_client_log_level = realm::util::Logger::Level::TEST_ENABLE_SYNC_LOGGING_LEVEL;
        bool override_sync_route = true;
        std::shared_ptr<realm::app::GenericNetworkTransport> transport;
    };

    TestSyncManager(realm::SyncManager::MetadataMode mode);
    TestSyncManager(const Config& = Config(), const SyncServer::Config& = {});
    ~TestSyncManager();

    std::shared_ptr<realm::app::App> app() const noexcept
    {
        return m_app;
    }
    std::string base_file_path() const
    {
        return m_base_file_path;
    }
    SyncServer& sync_server()
    {
        return m_sync_server;
    }

    // Capture the token refresh callback so that we can invoke it later with
    // the desired result
    realm::util::UniqueFunction<void(const realm::app::Response&)> network_callback;
    struct Transport : realm::app::GenericNetworkTransport {
        Transport(realm::util::UniqueFunction<void(const realm::app::Response&)>* network_callback)
            : network_callback(network_callback)
        {
        }

        void send_request_to_server(
            realm::app::Request&&,
            realm::util::UniqueFunction<void(const realm::app::Response&)>&& completion_block) override
        {
            *network_callback = std::move(completion_block);
        }

        realm::util::UniqueFunction<void(const realm::app::Response&)>* network_callback;
    };
    std::shared_ptr<realm::app::GenericNetworkTransport> transport = std::make_shared<Transport>(&network_callback);

private:
    std::shared_ptr<realm::app::App> m_app;
    SyncServer m_sync_server;
    std::string m_base_file_path;
    bool m_should_teardown_test_directory = true;
};

inline TestSyncManager::TestSyncManager(realm::SyncManager::MetadataMode mode)
    : TestSyncManager([=] {
        Config config;
        config.metadata_mode = mode;
        return config;
    }())
{
}

namespace realm {

bool results_contains_user(SyncUserMetadataResults& results, const std::string& identity,
                           const std::string& auth_server);
bool results_contains_original_name(SyncFileActionMetadataResults& results, const std::string& original_name);

void timed_wait_for(util::FunctionRef<bool()> condition,
                    std::chrono::milliseconds max_ms = std::chrono::milliseconds(5000));

struct ExpectedRealmPaths {
    ExpectedRealmPaths(const std::string& base_path, const std::string& app_id, const std::string& user_identity,
                       const std::string& local_identity, const std::string& partition);
    std::string current_preferred_path;
    std::string fallback_hashed_path;
    std::string legacy_local_id_path;
    std::string legacy_sync_path;
    std::vector<std::string> legacy_sync_directories_to_make;
};

#if REALM_ENABLE_SYNC

std::ostream& operator<<(std::ostream& os, util::Optional<app::AppError> error);

template <typename Transport>
TestSyncManager::Config get_config(Transport&& transport)
{
    TestSyncManager::Config config;
    config.transport = transport;
    return config;
}

#endif // REALM_ENABLE_SYNC

namespace reset_utils {

std::unique_ptr<TestClientReset> make_fake_local_client_reset(const Realm::Config& local_config,
                                                              const Realm::Config& remote_config);

} // namespace reset_utils

} // namespace realm


#endif // REALM_SYNC_TEST_UTILS_HPP
