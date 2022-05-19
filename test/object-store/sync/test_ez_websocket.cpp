////////////////////////////////////////////////////////////////////////////
//
// Copyright 2022 Realm Inc.
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

#include <catch2/catch.hpp>
#include "util/test_file.hpp"
#include "util/test_utils.hpp"

#include <memory>

#if REALM_ENABLE_SYNC
#include "util/event_loop.hpp"
#include <realm/util/ez_websocket.hpp>
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/thread_safe_reference.hpp>
#include <realm/object-store/util/event_loop_dispatcher.hpp>
#include <realm/object-store/sync/async_open_task.hpp>

using namespace realm;
using namespace realm::sync;
using namespace realm::util::websocket;

class TestSocketFactory: public EZSocketFactory {
public:
    TestSocketFactory(EZConfig config, std::function<void()> factoryCallback)
        : EZSocketFactory(config)
        , didCallHandler(factoryCallback)
    { }

    std::unique_ptr<EZSocket> connect(EZObserver* observer, EZEndpoint&& endpoint) override {
        didCallHandler();
        return EZSocketFactory::connect(observer, std::move(endpoint));
    }
    std::function<void()> didCallHandler;
};

TEST_CASE("Can setup custom sockets factory", "[platformNetworking]") {
    if (!util::EventLoop::has_implementation())
        return;

    bool didCallConnect = false;
    std::function<void()> factoryCallHandler = [&]() {
        didCallConnect = true;
    };

    TestSyncManager::Config testConfig = TestSyncManager::Config();
    
    // Configuring custom socket factory in SyncClientConfig
    SyncClientConfig sc_config;
    std::string m_base_file_path = testConfig.base_path.empty() ? util::make_temp_dir() + random_string(10) : testConfig.base_path;
    util::try_make_dir(m_base_file_path);
    sc_config.base_file_path = m_base_file_path;
    sc_config.metadata_mode = testConfig.metadata_mode;
    sc_config.log_level = testConfig.verbose_sync_client_logging ? util::Logger::Level::all : util::Logger::Level::off;
    sc_config.socket_factory = [&factoryCallHandler](EZConfig&& config) {
        return std::unique_ptr<EZSocketFactory>(new TestSocketFactory(std::move(config), factoryCallHandler));
    };

    TestSyncManager init_sync_manager(sc_config, testConfig);

    SyncTestFile config(init_sync_manager.app(), "default");
    config.cache = false;
    ObjectSchema object_schema = {"object",
                                  {
                                      {"_id", PropertyType::Int, Property::IsPrimary{true}},
                                      {"value", PropertyType::Int},
                                  }};
    config.schema = Schema{object_schema};

    std::mutex mutex;
    SECTION("Can setup custom sockets factory") {
        bool called = false;
        auto task = Realm::get_synchronized_realm(config);
        task->start(realm::util::EventLoopDispatcher([&](ThreadSafeReference&& ref, std::exception_ptr error) {
            REQUIRE(ref);
            REQUIRE(!error);
            called = true;
        }));
        util::EventLoop::main().run_until([&] {
            return called;
        });
        std::lock_guard<std::mutex> lock(mutex);
        REQUIRE(didCallConnect);
    }
}
#endif
