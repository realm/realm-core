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

#ifndef REALM_TEST_UTIL_TEST_FILE_HPP
#define REALM_TEST_UTIL_TEST_FILE_HPP

#include <realm/object-store/shared_realm.hpp>
#include <realm/util/tagged_bool.hpp>

#include <realm/util/logger.hpp>
#include <realm/util/optional.hpp>

#include <thread>

#if REALM_ENABLE_SYNC
#include <realm/sync/config.hpp>
#include <realm/object-store/sync/sync_manager.hpp>
#include <realm/object-store/sync/app.hpp>
#include "test_utils.hpp"

#include <realm/sync/client.hpp>
#include <realm/sync/noinst/server/server.hpp>

#endif // REALM_ENABLE_SYNC


namespace realm {
struct AppSession;
class Schema;
enum class SyncSessionStopPolicy;
struct DBOptions;
struct SyncConfig;
} // namespace realm

class JoiningThread {
public:
    template <typename... Args>
    JoiningThread(Args&&... args)
        : m_thread(std::forward<Args>(args)...)
    {
    }
    ~JoiningThread()
    {
        if (m_thread.joinable())
            m_thread.join();
    }
    void join()
    {
        m_thread.join();
    }

private:
    std::thread m_thread;
};


struct TestFile : realm::Realm::Config {
    TestFile();
    ~TestFile();

    // The file should outlive the object, ie. should not be deleted in destructor
    void persist()
    {
        m_persist = true;
    }

    realm::DBOptions options() const;

private:
    bool m_persist = false;
    std::string m_temp_dir;
};

struct InMemoryTestFile : TestFile {
    InMemoryTestFile();
};

void advance_and_notify(realm::Realm& realm);
void on_change_but_no_notify(realm::Realm& realm);

#if REALM_ENABLE_SYNC

#ifndef TEST_ENABLE_SYNC_LOGGING
#define TEST_ENABLE_SYNC_LOGGING 0 // change to 1 to enable trace-level logging
#endif

#ifndef TEST_ENABLE_SYNC_LOGGING_LEVEL
#if TEST_ENABLE_SYNC_LOGGING
#define TEST_ENABLE_SYNC_LOGGING_LEVEL all
#else
#define TEST_ENABLE_SYNC_LOGGING_LEVEL off
#endif // TEST_ENABLE_SYNC_LOGGING
#endif // TEST_ENABLE_SYNC_LOGGING_LEVEL


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
    std::shared_ptr<realm::util::Logger> m_logger;
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

#if REALM_ENABLE_AUTH_TESTS
using DeleteApp = realm::util::TaggedBool<struct DeleteAppTag>;
class TestAppSession {
public:
    TestAppSession();
    TestAppSession(realm::AppSession, std::shared_ptr<realm::app::GenericNetworkTransport> = nullptr,
                   DeleteApp = true);
    ~TestAppSession();

    std::shared_ptr<realm::app::App> app() const noexcept
    {
        return m_app;
    }
    const realm::AppSession& app_session() const noexcept
    {
        return *m_app_session;
    }
    realm::app::GenericNetworkTransport* transport()
    {
        return m_transport.get();
    }

private:
    std::shared_ptr<realm::app::App> m_app;
    std::unique_ptr<realm::AppSession> m_app_session;
    std::string m_base_file_path;
    bool m_delete_app = true;
    std::shared_ptr<realm::app::GenericNetworkTransport> m_transport;
};
#endif

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
        Transport(realm::util::UniqueFunction<void(const realm::app::Response&)>& network_callback)
            : network_callback(network_callback)
        {
        }

        void
        send_request_to_server(const realm::app::Request&,
                               realm::util::UniqueFunction<void(const realm::app::Response&)>&& completion) override
        {
            network_callback = std::move(completion);
        }

        realm::util::UniqueFunction<void(const realm::app::Response&)>& network_callback;
    };
    const std::shared_ptr<realm::app::GenericNetworkTransport> transport;

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

std::error_code wait_for_upload(realm::Realm& realm, std::chrono::seconds timeout = std::chrono::seconds(60));
std::error_code wait_for_download(realm::Realm& realm, std::chrono::seconds timeout = std::chrono::seconds(60));

#endif // REALM_ENABLE_SYNC

#endif
