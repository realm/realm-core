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

#ifndef REALM_COMMON_UTILS_HPP
#define REALM_COMMON_UTILS_HPP

//#if REALM_ENABLE_SYNC
#include <realm/object-store/sync/app.hpp>
//#endif

#include <realm/object-store/object_store.hpp>
#include <realm/object-store/shared_realm.hpp>
#include <realm/object-store/sync/sync_session.hpp>
#include <realm/object-store/sync/sync_user.hpp>

#include <random>

#ifndef TEST_ENABLE_SYNC_LOGGING_LEVEL
#if TEST_ENABLE_SYNC_LOGGING
#define TEST_ENABLE_SYNC_LOGGING_LEVEL all
#else
#define TEST_ENABLE_SYNC_LOGGING_LEVEL off
#endif // TEST_ENABLE_SYNC_LOGGING
#endif // TEST_ENABLE_SYNC_LOGGING_LEVEL

//#if REALM_ENABLE_SYNC


inline std::error_code wait_for_session(realm::Realm& realm,
                                        void (realm::SyncSession::*fn)(realm::util::UniqueFunction<void(std::error_code)>&&),
                                        std::chrono::seconds timeout)
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
    bool completed = cv.wait_for(lock, timeout, [&]() {
        return wait_flag == true;
    });
    REALM_ASSERT_RELEASE(completed);
    return ec;
}

std::error_code wait_for_upload(realm::Realm& realm, std::chrono::seconds timeout = std::chrono::seconds(60));
std::error_code wait_for_download(realm::Realm& realm, std::chrono::seconds timeout = std::chrono::seconds(60));



namespace realm {

namespace reset_utils {




struct Partition {
    std::string property_name;
    std::string value;
};

Obj create_object(Realm& realm, StringData object_type, util::Optional<ObjectId> primary_key = util::none,
                  util::Optional<Partition> partition = util::none);

struct TestClientReset {
    using Callback = util::UniqueFunction<void(const SharedRealm&)>;
    TestClientReset(const Realm::Config& local_config, const Realm::Config& remote_config);
    virtual ~TestClientReset();
    TestClientReset* setup(Callback&& on_setup);
    TestClientReset* make_local_changes(Callback&& changes_local);
    TestClientReset* make_remote_changes(Callback&& changes_remote);
    TestClientReset* on_post_local_changes(Callback&& post_local);
    TestClientReset* on_post_reset(Callback&& post_reset);
    void set_pk_of_object_driving_reset(const ObjectId& pk);
    ObjectId get_pk_of_object_driving_reset() const;
    void disable_wait_for_reset_completion();

    virtual void run() = 0;

protected:
    realm::Realm::Config m_local_config;
    realm::Realm::Config m_remote_config;

    Callback m_on_setup;
    Callback m_make_local_changes;
    Callback m_make_remote_changes;
    Callback m_on_post_local;
    Callback m_on_post_reset;
    bool m_did_run = false;
    ObjectId m_pk_driving_reset = ObjectId::gen();
    bool m_wait_for_reset_completion = true;
};
} // reset_utils

void timed_sleeping_wait_for(util::FunctionRef<bool()> condition,
                             std::chrono::milliseconds max_ms = std::chrono::seconds(30));


template <typename Transport>
const std::shared_ptr<app::GenericNetworkTransport> instance_of = std::make_shared<Transport>();

}

namespace {
void inline set_app_config_defaults(realm::app::App::Config& app_config,
                                    const std::shared_ptr<realm::app::GenericNetworkTransport>& transport)
{
    if (!app_config.transport)
        app_config.transport = transport;
    if (app_config.platform.empty())
        app_config.platform = "Object Store Test Platform";
    if (app_config.platform_version.empty())
        app_config.platform_version = "Object Store Test Platform Version";
    if (app_config.sdk_version.empty())
        app_config.sdk_version = "SDK Version";
    if (app_config.app_id.empty())
        app_config.app_id = "app_id";
    if (!app_config.local_app_version)
        app_config.local_app_version.emplace("A Local App Version");
}
} // anonymous namespace

//#endif


static inline std::string random_string(std::string::size_type length)
{
    static auto& chrs = "abcdefghijklmnopqrstuvwxyz"
                        "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    thread_local static std::mt19937 rg{std::random_device{}()};
    thread_local static std::uniform_int_distribution<std::string::size_type> pick(0, sizeof(chrs) - 2);
    std::string s;
    s.reserve(length);
    while (length--)
        s += chrs[pick(rg)];
    return s;
}

#endif // REALM_COMMON_UTILS_HPP
