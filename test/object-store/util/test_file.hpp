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
#include <realm/util/logger.hpp>
#include <realm/util/tagged_bool.hpp>

#if REALM_ENABLE_SYNC
#include "test_utils.hpp"
#include "unit_test_transport.hpp"

#include <realm/object-store/sync/app.hpp>
#include <realm/object-store/sync/sync_manager.hpp>
#include <realm/sync/client.hpp>
#include <realm/sync/config.hpp>
#include <realm/sync/noinst/server/server.hpp>
#endif // REALM_ENABLE_SYNC

#include <thread>

#ifndef TEST_TIMEOUT_EXTRA
#define TEST_TIMEOUT_EXTRA 0
#endif

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

    TestFile(const TestFile&) = delete;
    TestFile& operator=(const TestFile&) = delete;

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

struct InMemoryTestFile : realm::Realm::Config {
    InMemoryTestFile();
    realm::DBOptions options() const;
};

void advance_and_notify(realm::Realm& realm);
void on_change_but_no_notify(realm::Realm& realm);

#ifndef TEST_ENABLE_LOGGING
#define TEST_ENABLE_LOGGING 0 // change to 1 to enable trace-level logging
#endif

#ifndef TEST_LOGGING_LEVEL
#if TEST_ENABLE_LOGGING
#define TEST_LOGGING_LEVEL all
#else
#define TEST_LOGGING_LEVEL off
#endif // TEST_ENABLE_LOGGING
#endif // TEST_LOGGING_LEVEL

#define TEST_LOGGING_LEVEL_STORAGE off
#define TEST_LOGGING_LEVEL_SERVER off
/*
#define TEST_LOGGING_LEVEL_SYNC off
#define TEST_LOGGING_LEVEL_RESET trace
#define TEST_LOGGING_LEVEL_APP off
*/

#if REALM_ENABLE_SYNC

using StartImmediately = realm::util::TaggedBool<class StartImmediatelyTag>;
using EnableSSL = realm::util::TaggedBool<class EnableSSLTag>;

class SyncServer : private realm::sync::Clock {
public:
    struct Config {
        StartImmediately start_immediately = true;
        EnableSSL ssl = false;
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

class OfflineAppSession;
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

    SyncTestFile(TestSyncManager&, std::string name = "", std::string user_name = "test");
    SyncTestFile(OfflineAppSession&, std::string name = "");
    SyncTestFile(std::shared_ptr<realm::SyncUser> user, realm::bson::Bson partition,
                 realm::util::Optional<realm::Schema> schema = realm::util::none);
    SyncTestFile(std::shared_ptr<realm::SyncUser> user, realm::bson::Bson partition,
                 realm::util::Optional<realm::Schema> schema,
                 std::function<realm::SyncSessionErrorHandler>&& error_handler);
    SyncTestFile(std::shared_ptr<realm::app::App> app, realm::bson::Bson partition, realm::Schema schema);
    SyncTestFile(std::shared_ptr<realm::SyncUser> user, realm::Schema schema, realm::SyncConfig::FLXSyncEnabled);
};

class TestSyncManager {
public:
    struct Config {
        Config();
        std::string base_path;
        realm::SyncManager::MetadataMode metadata_mode = realm::SyncManager::MetadataMode::NoMetadata;
        bool should_teardown_test_directory = true;
        realm::util::Logger::Level log_level = realm::util::Logger::Level::TEST_LOGGING_LEVEL;
        bool start_sync_client = true;
    };

    TestSyncManager(const Config& = Config(), const SyncServer::Config& = {});
    ~TestSyncManager();

    std::string base_file_path() const
    {
        return m_base_file_path;
    }
    SyncServer& sync_server()
    {
        return m_sync_server;
    }
    const std::shared_ptr<realm::SyncManager>& sync_manager()
    {
        return m_sync_manager;
    }

    std::shared_ptr<realm::SyncUser> fake_user(const std::string& name = "test");

private:
    std::shared_ptr<realm::SyncManager> m_sync_manager;
    SyncServer m_sync_server;
    std::string m_base_file_path;
    bool m_should_teardown_test_directory = true;
};

class OfflineAppSession {
public:
    struct Config {
        Config(std::shared_ptr<realm::app::GenericNetworkTransport> = std::make_shared<UnitTestTransport>());
        std::shared_ptr<realm::app::GenericNetworkTransport> transport;
        bool delete_storage = true;
        std::optional<std::string> storage_path;
        realm::SyncManager::MetadataMode metadata_mode = realm::SyncManager::MetadataMode::NoMetadata;
        std::optional<std::string> base_url;
        std::shared_ptr<realm::sync::SyncSocketProvider> socket_provider;
        std::optional<std::string> app_id;
    };
    OfflineAppSession(Config = {});
    ~OfflineAppSession();

    std::shared_ptr<realm::app::App> app() const noexcept
    {
        return m_app;
    }
    std::shared_ptr<realm::SyncUser> make_user() const;
    realm::app::GenericNetworkTransport* transport()
    {
        return m_transport.get();
    }
    std::string base_file_path() const
    {
        return m_base_file_path;
    }
    const std::shared_ptr<realm::SyncManager>& sync_manager()
    {
        return m_app->sync_manager();
    }

private:
    std::shared_ptr<realm::app::App> m_app;
    std::string m_base_file_path;
    std::shared_ptr<realm::app::GenericNetworkTransport> m_transport;
    bool m_delete_storage = true;
};

#if REALM_ENABLE_AUTH_TESTS
using DeleteApp = realm::util::TaggedBool<struct DeleteAppTag>;
class TestAppSession {
public:
    TestAppSession();
    TestAppSession(realm::AppSession, std::shared_ptr<realm::app::GenericNetworkTransport> = nullptr,
                   DeleteApp = true, realm::ReconnectMode reconnect_mode = realm::ReconnectMode::normal,
                   std::shared_ptr<realm::sync::SyncSocketProvider> custom_socket_provider = nullptr);
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
    const std::shared_ptr<realm::SyncManager>& sync_manager() const
    {
        return m_app->sync_manager();
    }

    std::vector<realm::bson::BsonDocument> get_documents(realm::SyncUser& user, const std::string& object_type,
                                                         size_t expected_count) const;

private:
    std::shared_ptr<realm::app::App> m_app;
    std::unique_ptr<realm::AppSession> m_app_session;
    std::string m_base_file_path;
    bool m_delete_app = true;
    std::shared_ptr<realm::app::GenericNetworkTransport> m_transport;
};
#endif


bool wait_for_upload(realm::Realm& realm, std::chrono::seconds timeout = std::chrono::seconds(60));
bool wait_for_download(realm::Realm& realm, std::chrono::seconds timeout = std::chrono::seconds(60));

void set_app_config_defaults(realm::app::App::Config& app_config,
                             const std::shared_ptr<realm::app::GenericNetworkTransport>& transport);

#endif // REALM_ENABLE_SYNC

#endif
