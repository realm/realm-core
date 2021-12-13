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

#include "util/test_file.hpp"

#include "baas_admin_api.hpp"
#include "test_utils.hpp"
#include <realm/object-store/impl/realm_coordinator.hpp>

#if REALM_ENABLE_SYNC
#include <realm/object-store/sync/sync_manager.hpp>
#include <realm/object-store/sync/sync_session.hpp>
#include <realm/object-store/sync/sync_user.hpp>
#include <realm/object-store/schema.hpp>
#endif

#include <realm/db.hpp>
#include <realm/disable_sync_to_disk.hpp>
#include <realm/history.hpp>
#include <realm/string_data.hpp>
#include <realm/util/base64.hpp>
#include <realm/util/file.hpp>

#include <cstdlib>
#include <iostream>

#ifdef _WIN32
#include <io.h>
#include <fcntl.h>

inline static int mkstemp(char* _template)
{
    return _open(_mktemp(_template), _O_CREAT | _O_TEMPORARY, _S_IREAD | _S_IWRITE);
}
#else
#include <unistd.h>
#endif

#if REALM_HAVE_CLANG_FEATURE(thread_sanitizer)
#include <condition_variable>
#include <functional>
#include <thread>
#include <map>
#endif

using namespace realm;

TestFile::TestFile()
{
    disable_sync_to_disk();
    m_temp_dir = util::make_temp_dir();
    if (m_temp_dir.size() == 0 || m_temp_dir[m_temp_dir.size() - 1] != '/') {
        m_temp_dir = m_temp_dir + "/";
    }
    path = util::format("%1realm.XXXXXX", m_temp_dir);
    int fd = mkstemp(path.data());
    if (fd < 0) {
        int err = errno;
        throw std::system_error(err, std::system_category());
    }
#ifdef _WIN32
    _close(fd);
    _unlink(path.c_str());
#else // POSIX
    close(fd);
    unlink(path.c_str());
#endif

    schema_version = 0;
}

TestFile::~TestFile()
{
    if (!m_persist) {
        try {
            util::File::try_remove(path);
            util::try_remove_dir_recursive(m_temp_dir);
        }
        catch (...) {
            // clean up is best effort, ignored.
        }
    }
}

DBOptions TestFile::options() const
{
    DBOptions options;
    options.durability = in_memory ? DBOptions::Durability::MemOnly : DBOptions::Durability::Full;
    return options;
}

InMemoryTestFile::InMemoryTestFile()
{
    in_memory = true;
}

#if REALM_ENABLE_SYNC
SyncTestFile::SyncTestFile(std::shared_ptr<app::App> app, std::string name, std::string user_name)
{
    if (!app)
        throw std::runtime_error("Must provide `app` for SyncTestFile");

    if (name.empty())
        name = path.substr(path.rfind('/') + 1);

    if (name[0] != '/')
        name = "/" + name;

    std::string fake_refresh_token = ENCODE_FAKE_JWT("not_a_real_token");
    std::string fake_access_token = ENCODE_FAKE_JWT("also_not_real");
    std::string fake_device_id = "123400000000000000000000";
    sync_config =
        std::make_shared<SyncConfig>(app->sync_manager()->get_user(user_name, fake_refresh_token, fake_access_token,
                                                                   app->base_url(), fake_device_id),
                                     name);
    sync_config->stop_policy = SyncSessionStopPolicy::Immediately;
    sync_config->error_handler = [](auto, SyncError error) {
        std::cerr << util::format("An unexpected sync error was caught by the default SyncTestFile handler: '%1'",
                                  error.message)
                  << std::endl;
        abort();
    };
    schema_mode = SchemaMode::AdditiveExplicit;
}

SyncTestFile::SyncTestFile(std::shared_ptr<realm::SyncUser> user, bson::Bson partition, realm::Schema _schema)
{
    if (!user)
        throw std::runtime_error("Must provide `user` for SyncTestFile");

    sync_config = std::make_shared<realm::SyncConfig>(user, partition);
    sync_config->stop_policy = SyncSessionStopPolicy::Immediately;
    sync_config->error_handler = [](std::shared_ptr<SyncSession>, SyncError error) {
        std::cerr << util::format("An unexpected sync error was caught by the default SyncTestFile handler: '%1'",
                                  error.message)
                  << std::endl;
        abort();
    };
    schema_version = 1;
    schema = _schema;
    schema_mode = SchemaMode::AdditiveExplicit;
}

SyncTestFile::SyncTestFile(std::shared_ptr<realm::SyncUser> user, realm::Schema _schema, SyncConfig::FLXSyncEnabled)
{
    if (!user)
        throw std::runtime_error("Must provide `user` for SyncTestFile");

    sync_config = std::make_shared<realm::SyncConfig>(user, SyncConfig::FLXSyncEnabled{});
    sync_config->stop_policy = SyncSessionStopPolicy::Immediately;
    sync_config->error_handler = [](std::shared_ptr<SyncSession>, SyncError error) {
        std::cerr << util::format("An unexpected sync error was caught by the default SyncTestFile handler: '%1'",
                                  error.message)
                  << std::endl;
        abort();
    };
    schema_version = 1;
    schema = _schema;
    schema_mode = SchemaMode::AdditiveExplicit;
}

SyncTestFile::SyncTestFile(std::shared_ptr<app::App> app, bson::Bson partition, Schema schema)
{
    REALM_ASSERT(app);
    sync_config = std::make_shared<SyncConfig>(app->current_user(), partition);
    sync_config->stop_policy = SyncSessionStopPolicy::Immediately;
    sync_config->error_handler = [](auto, auto err) {
        fprintf(stderr, "Unexpected sync error: %s\n", err.message.c_str());
        abort();
    };
    schema_mode = SchemaMode::AdditiveExplicit;
    schema_version = 1;
    this->schema = std::move(schema);
}

// MARK: - SyncServer
SyncServer::SyncServer(const SyncServer::Config& config)
    : m_local_root_dir(config.local_dir.empty() ? util::make_temp_dir() : config.local_dir)
    , m_server(m_local_root_dir, util::none, ([&] {
                   using namespace std::literals::chrono_literals;

#if TEST_ENABLE_SYNC_LOGGING
                   auto logger = new util::StderrLogger();
                   logger->set_level_threshold(util::Logger::Level::all);
                   m_logger.reset(logger);
#else
                   m_logger.reset(new TestLogger());
#endif

                   sync::Server::Config config;
                   config.logger = m_logger.get();
                   config.history_compaction_clock = this;
                   config.token_expiration_clock = this;
                   config.disable_history_compaction = false;
                   config.history_ttl = 1s;
                   config.history_compaction_interval = 1s;
                   config.listen_address = "127.0.0.1";

                   return config;
               })())
{
    m_server.start();
    m_url = util::format("ws://127.0.0.1:%1", m_server.listen_endpoint().port());
    if (config.start_immediately)
        start();
}

SyncServer::~SyncServer()
{
    stop();
}

void SyncServer::start()
{
    REALM_ASSERT(!m_thread.joinable());
    m_thread = std::thread([this] {
        m_server.run();
    });
}

void SyncServer::stop()
{
    m_server.stop();
    if (m_thread.joinable())
        m_thread.join();
}

std::string SyncServer::url_for_realm(StringData realm_name) const
{
    return util::format("%1/%2", m_url, realm_name);
}

static std::error_code wait_for_session(Realm& realm, void (SyncSession::*fn)(std::function<void(std::error_code)>))
{
    std::condition_variable cv;
    std::mutex wait_mutex;
    bool wait_flag(false);
    std::error_code ec;
    auto& session = *realm.config().sync_config->user->session_for_on_disk_path(realm.config().path);
    (session.*fn)([&](std::error_code error) {
        std::unique_lock<std::mutex> lock(wait_mutex);
        wait_flag = true;
        ec = error;
        cv.notify_one();
    });
    std::unique_lock<std::mutex> lock(wait_mutex);
    cv.wait(lock, [&]() {
        return wait_flag == true;
    });
    return ec;
}

std::error_code wait_for_upload(Realm& realm)
{
    return wait_for_session(realm, &SyncSession::wait_for_upload_completion);
}

std::error_code wait_for_download(Realm& realm)
{
    return wait_for_session(realm, &SyncSession::wait_for_download_completion);
}

// MARK: - TestSyncManager

TestSyncManager::TestSyncManager(const Config& config, const SyncServer::Config& sync_server_config)
    : m_sync_server(sync_server_config)
    , m_should_teardown_test_directory(config.should_teardown_test_directory)
    , m_app_session(config.app_session)
{
    app::App::Config app_config = config.app_config;
    if (!app_config.transport) {
        app_config.transport = transport;
    }

    if (app_config.platform.empty()) {
        app_config.platform = "OS Test Platform";
    }

    if (app_config.platform_version.empty()) {
        app_config.platform_version = "OS Test Platform Version";
    }

    if (app_config.sdk_version.empty()) {
        app_config.sdk_version = "SDK Version";
    }

    if (app_config.app_id.empty()) {
        app_config.app_id = "app_id";
    }

    SyncClientConfig sc_config;
    m_base_file_path = config.base_path.empty() ? util::make_temp_dir() + random_string(10) : config.base_path;
    util::try_make_dir(m_base_file_path);
    sc_config.base_file_path = m_base_file_path;
    sc_config.metadata_mode = config.metadata_mode;
    sc_config.log_level = config.verbose_sync_client_logging ? util::Logger::Level::all : util::Logger::Level::off;

    m_app = app::App::get_shared_app(app_config, sc_config);
    if (config.override_sync_route) {
        m_app->sync_manager()->set_sync_route((config.base_url.empty() ? m_sync_server.base_url() : config.base_url) +
                                              "/realm-sync");
    }
    // initialize sync client
    m_app->sync_manager()->get_sync_client();
}

TestSyncManager::~TestSyncManager()
{
    if (m_should_teardown_test_directory) {
        if (!m_base_file_path.empty() && util::File::exists(m_base_file_path)) {
            m_app->sync_manager()->reset_for_testing();
            util::try_remove_dir_recursive(m_base_file_path);
            app::App::clear_cached_apps();
        }
#if REALM_ENABLE_AUTH_TESTS
        if (m_app_session) {
            m_app_session->admin_api.delete_app(m_app_session->server_app_id);
        }
#endif
    }
}

std::shared_ptr<app::App> TestSyncManager::app() const
{
    return m_app;
}

#endif // REALM_ENABLE_SYNC

#if REALM_HAVE_CLANG_FEATURE(thread_sanitizer)
// MARK: - TsanNotifyWorker
// A helper which synchronously runs on_change() on a fixed background thread
// so that ThreadSanitizer can potentially detect issues
// This deliberately uses an unsafe spinlock for synchronization to ensure that
// the code being tested has to supply all required safety
static class TsanNotifyWorker {
public:
    TsanNotifyWorker()
    {
        m_thread = std::thread([&] {
            work();
        });
    }

    void work()
    {
        while (true) {
            auto value = m_signal.load(std::memory_order_relaxed);
            if (value == 0 || value == 1)
                continue;
            if (value == 2)
                return;

            if (value & 1) {
                // Synchronize on the first handover of a given coordinator.
                value &= ~1;
                m_signal.load();
            }

            auto c = reinterpret_cast<_impl::RealmCoordinator*>(value);
            c->on_change();
            m_signal.store(1, std::memory_order_relaxed);
        }
    }

    ~TsanNotifyWorker()
    {
        m_signal = 2;
        m_thread.join();
    }

    void on_change(const std::shared_ptr<_impl::RealmCoordinator>& c)
    {
        auto& it = m_published_coordinators[c.get()];
        if (it.lock()) {
            m_signal.store(reinterpret_cast<uintptr_t>(c.get()), std::memory_order_relaxed);
        }
        else {
            // Synchronize on the first handover of a given coordinator.
            it = c;
            m_signal = reinterpret_cast<uintptr_t>(c.get()) | 1;
        }

        while (m_signal.load(std::memory_order_relaxed) != 1)
            ;
    }

private:
    std::atomic<uintptr_t> m_signal{0};
    std::thread m_thread;
    std::map<_impl::RealmCoordinator*, std::weak_ptr<_impl::RealmCoordinator>> m_published_coordinators;
} s_worker;

void on_change_but_no_notify(Realm& realm)
{
    s_worker.on_change(_impl::RealmCoordinator::get_existing_coordinator(realm.config().path));
}

void advance_and_notify(Realm& realm)
{
    on_change_but_no_notify(realm);
    realm.notify();
}

#else // REALM_HAVE_CLANG_FEATURE(thread_sanitizer)

void on_change_but_no_notify(Realm& realm)
{
    _impl::RealmCoordinator::get_coordinator(realm.config().path)->on_change();
}

void advance_and_notify(Realm& realm)
{
    on_change_but_no_notify(realm);
    realm.notify();
}
#endif
