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

#include "test_utils.hpp"

#include "impl/realm_coordinator.hpp"

#if REALM_ENABLE_SYNC
#include "sync/sync_config.hpp"
#include "sync/sync_manager.hpp"
#include "sync/sync_session.hpp"
#include "schema.hpp"
#endif

#include <realm/db.hpp>
#include <realm/disable_sync_to_disk.hpp>
#include <realm/history.hpp>
#include <realm/string_data.hpp>
#include <realm/util/base64.hpp>

#include <cstdlib>

#ifdef _WIN32
#include <io.h>
#include <fcntl.h>

inline static int mkstemp(char* _template) { return _open(_mktemp(_template), _O_CREAT | _O_TEMPORARY, _S_IREAD | _S_IWRITE); }
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
    static std::string tmpdir = [] {
        disable_sync_to_disk();

        const char* dir = getenv("TMPDIR");
        if (dir && *dir)
            return dir;
#if REALM_ANDROID
        return "/data/local/tmp";
#else
        return "/tmp";
#endif
    }();
    path = tmpdir + "/realm.XXXXXX";
    int fd = mkstemp(&path[0]);
    if (fd < 0) {
        int err = errno;
        throw std::system_error(err, std::system_category());
    }
    close(fd);
    unlink(path.c_str());

    schema_version = 0;
}

TestFile::~TestFile()
{
    if (!m_persist)
        unlink(path.c_str());
}

DBOptions TestFile::options() const
{
    DBOptions options;
    options.durability = in_memory
                       ? DBOptions::Durability::MemOnly
                       : DBOptions::Durability::Full;
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
        app = SyncManager::shared().app();

    if (name.empty())
        name = path.substr(path.rfind('/') + 1);

    if (name[0] != '/')
        name = "/" + name;

    std::string fake_refresh_token = ENCODE_FAKE_JWT("not_a_real_token");
    std::string fake_access_token = ENCODE_FAKE_JWT("also_not_real");
    std::string fake_device_id = "123400000000000000000000";
    sync_config = std::make_shared<SyncConfig>(SyncManager::shared().get_user(user_name, fake_refresh_token, fake_access_token, app->base_url(),
    fake_device_id), name);
    sync_config->stop_policy = SyncSessionStopPolicy::Immediately;
    sync_config->error_handler = [](auto, auto) { abort(); };
    schema_mode = SchemaMode::Additive;
}

// MARK: - SyncServer
SyncServer::SyncServer(StartImmediately start_immediately, std::string local_dir)
: m_local_root_dir(local_dir.empty() ? util::make_temp_dir() : local_dir)
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
#if TEST_ENABLE_SYNC_LOGGING
    SyncManager::shared().set_log_level(util::Logger::Level::all);
#else
    SyncManager::shared().set_log_level(util::Logger::Level::off);
#endif

    m_server.start();
    m_url = util::format("ws://127.0.0.1:%1", m_server.listen_endpoint().port());
    if (start_immediately)
        start();
}

SyncServer::~SyncServer()
{
    stop();
    SyncManager::shared().reset_for_testing();
}

void SyncServer::start()
{
    REALM_ASSERT(!m_thread.joinable());
    m_thread = std::thread([this]{ m_server.run(); });
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
    auto& session = *SyncManager::shared().get_session(realm.config().path, *realm.config().sync_config);
    (session.*fn)([&](std::error_code error) {
        std::unique_lock<std::mutex> lock(wait_mutex);
        wait_flag = true;
        ec = error;
        cv.notify_one();
    });
    std::unique_lock<std::mutex> lock(wait_mutex);
    cv.wait(lock, [&]() { return wait_flag == true; });
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

TestSyncManager::TestSyncManager(const std::string& base_url,
                                 std::string const& base_path,
                                 SyncManager::MetadataMode mode,
                                 bool should_teardown_test_directory)
: m_should_teardown_test_directory(should_teardown_test_directory)
{
    SyncClientConfig config;
    m_base_file_path = base_path.empty() ? tmp_dir() + random_string(10) : base_path;
    util::try_make_dir(m_base_file_path);
    config.base_file_path = m_base_file_path;
    config.metadata_mode = mode;
#if TEST_ENABLE_SYNC_LOGGING
    config.log_level = util::Logger::Level::all;
#else
    config.log_level = util::Logger::Level::off;
#endif
    app::App::Config app_config;
    app_config.app_id = "app_id";
    app_config.transport_generator = []() -> std::unique_ptr<app::GenericNetworkTransport> { REALM_ASSERT_RELEASE(false); };
    app_config.base_url = base_url;
    app_config.platform = "OS Test Platform";
    app_config.platform_version = "OS Test Platform Version";
    app_config.sdk_version = "SDK Version";
    SyncManager::shared().configure(config, app_config);
    app::App::OnlyForTesting::set_sync_route(*SyncManager::shared().app(), base_url + "/realm-sync");
}

TestSyncManager::~TestSyncManager()
{
    SyncManager::shared().reset_for_testing();
    if (m_should_teardown_test_directory && !m_base_file_path.empty() && util::File::exists(m_base_file_path))
        util::remove_dir_recursive(m_base_file_path);
}

TestSyncManager::TestSyncManager(const app::App::Config& config,
                                 bool should_teardown_test_dir,
                                 realm::SyncManager::MetadataMode mode)
: m_should_teardown_test_directory(should_teardown_test_dir)
{
    SyncClientConfig s_config;
    m_base_file_path = tmp_dir() + config.app_id;
    // create the directory heirarchy for easy cleanup later
    util::try_make_dir(m_base_file_path);
    s_config.base_file_path = m_base_file_path;
    s_config.metadata_mode = mode;
    #if TEST_ENABLE_SYNC_LOGGING
        s_config.log_level = util::Logger::Level::all;
    #else
        s_config.log_level = util::Logger::Level::off;
    #endif
    SyncManager::shared().configure(s_config, config);
    // initialize sync client
    SyncManager::shared().get_sync_client();
    app::App::OnlyForTesting::set_sync_route(*SyncManager::shared().app(), config.base_url.value_or("") + "/realm-sync");
}

std::shared_ptr<app::App> TestSyncManager::app() const {
    return SyncManager::shared().app();
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
        m_thread = std::thread([&] { work(); });
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

            auto c = reinterpret_cast<_impl::RealmCoordinator *>(value);
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
        } else {
            // Synchronize on the first handover of a given coordinator.
            it = c;
            m_signal = reinterpret_cast<uintptr_t>(c.get()) | 1;
        }

        while (m_signal.load(std::memory_order_relaxed) != 1) ;
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
