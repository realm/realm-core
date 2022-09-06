////////////////////////////////////////////////////////////////////////////
//
// Copyright 2021 Realm Inc.
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

#if REALM_ENABLE_SYNC
#include <catch2/catch_all.hpp>
#include <realm/object-store/impl/object_accessor_impl.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/sync/async_open_task.hpp>
#include <realm/object-store/thread_safe_reference.hpp>
#include <realm/object-store/util/event_loop_dispatcher.hpp>
#include <realm/util/default_websocket.hpp>

#include "collection_fixtures.hpp"
#include "sync_test_utils.hpp"
#include "util/baas_admin_api.hpp"
#include "util/test_utils.hpp"
#include "util/test_file.hpp"

#include <memory>


using namespace realm;
using namespace realm::sync;
using namespace realm::util::websocket;

class TestSocketFactory : public DefaultSocketFactory {
public:
    TestSocketFactory(SocketFactoryConfig config, DefaultSocketFactoryConfig legacy_config,
                      util::UniqueFunction<void()>&& factoryCallback)
        : DefaultSocketFactory(config, legacy_config)
        , didCallHandler(std::move(factoryCallback))
    {
    }

    std::unique_ptr<WebSocket> connect(SocketObserver* observer, Endpoint&& endpoint) override
    {
        if (didCallHandler) {
            didCallHandler();
        }
        return DefaultSocketFactory::connect(observer, std::move(endpoint));
    }
    util::UniqueFunction<void()> didCallHandler;
};

TEST_CASE("Can setup custom sockets factory", "[platformNetworking]") {
    bool didCallConnect = false;

    auto logger = std::make_unique<util::StderrLogger>();
    std::mt19937_64 random;
    util::network::Service service;
    logger->set_level_threshold(TEST_ENABLE_SYNC_LOGGING ? util::Logger::Level::all : util::Logger::Level::off);

    TestAppSession session(
//        get_runtime_app_session(get_base_url()), nullptr, true);
        get_runtime_app_session(get_base_url()), nullptr, true,
        std::make_shared<TestSocketFactory>(SocketFactoryConfig{"test-user-agent"},
                                            DefaultSocketFactoryConfig{
                                                *logger,
                                                random,
                                                service, },
                                            [&]() {
                                                didCallConnect = true;
                                            }));
    auto app = session.app();

    auto schema = default_app_config("").schema;
    SyncTestFile original_config(app, bson::Bson("foo"), schema);
    create_user_and_log_in(app);
    SyncTestFile target_config(app, bson::Bson("foo"), schema);

    // Create and load the realm information
    {
        auto realm = Realm::get_shared_realm(original_config);
        wait_for_download(*realm);

        // Write some data
        realm->begin_transaction();
        CppContext c;
        Object::create(c, realm, "Person",
                       std::any(realm::AnyDict{{"_id", std::any(ObjectId::gen())},
                                               {"age", INT64_C(64)},
                                               {"firstName", std::string("Paul")},
                                               {"lastName", std::string("McCartney")}}));
        realm->commit_transaction();
        wait_for_upload(*realm);
        wait_for_download(*realm);
        REQUIRE(didCallConnect);
    }
}
#endif
