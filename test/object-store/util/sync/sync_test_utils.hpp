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

#pragma once

#include "util/event_loop.hpp"
#include "util/test_file.hpp"
#include "util/test_utils.hpp"

#include <realm/object-store/sync/app.hpp>
#include <realm/object-store/sync/generic_network_transport.hpp>
#include <realm/object-store/sync/impl/sync_file.hpp>
#include <realm/object-store/sync/impl/app_metadata.hpp>
#include <realm/object-store/sync/sync_session.hpp>
#include <realm/object-store/thread_safe_reference.hpp>
#include <realm/sync/network/http.hpp>
#include <realm/sync/socket_provider.hpp>
#include <realm/sync/network/default_socket.hpp>

#include <realm/util/functional.hpp>
#include <realm/util/function_ref.hpp>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_templated.hpp>

#include <chrono>
#include <vector>

// disable the tests that rely on having baas available on the network
// but allow opt-in by building with REALM_ENABLE_AUTH_TESTS=1
#ifndef REALM_ENABLE_AUTH_TESTS
#define REALM_ENABLE_AUTH_TESTS 0
#endif

namespace realm {

void timed_wait_for(util::FunctionRef<bool()> condition,
                    std::chrono::milliseconds max_ms = std::chrono::milliseconds(5000));

void timed_sleeping_wait_for(util::FunctionRef<bool()> condition,
                             std::chrono::milliseconds max_ms = std::chrono::seconds(30),
                             std::chrono::milliseconds sleep_ms = std::chrono::milliseconds(1));

class ReturnsTrueWithinTimeLimit : public Catch::Matchers::MatcherGenericBase {
public:
    ReturnsTrueWithinTimeLimit(std::chrono::milliseconds max_ms = std::chrono::milliseconds(5000))
        : m_max_ms(max_ms)
    {
    }

    bool match(util::FunctionRef<bool()> condition) const;

    std::string describe() const override
    {
        return util::format("PredicateReturnsTrueAfter %1ms", m_max_ms.count());
    }

private:
    std::chrono::milliseconds m_max_ms;
};

template <typename T>
struct TimedFutureState : public util::AtomicRefCountBase {
    TimedFutureState(util::Promise<T>&& promise)
        : promise(std::move(promise))
    {
    }

    util::Promise<T> promise;
    std::mutex mutex;
    std::condition_variable cv;
    bool finished = false;
};

template <typename T>
util::Future<T> wait_for_future(util::Future<T>&& input, std::chrono::milliseconds max_ms = std::chrono::seconds(60))
{
    auto pf = util::make_promise_future<T>();
    auto shared_state = util::make_bind<TimedFutureState<T>>(std::move(pf.promise));
    const auto delay = TEST_TIMEOUT_EXTRA > 0 ? max_ms + std::chrono::seconds(TEST_TIMEOUT_EXTRA) : max_ms;

    std::move(input).get_async([shared_state](StatusOrStatusWith<T> value) {
        std::unique_lock lk(shared_state->mutex);
        // If the state has already expired, then just return without doing anything.
        if (std::exchange(shared_state->finished, true)) {
            return;
        }

        shared_state->promise.set_from_status_with(std::move(value));
        shared_state->cv.notify_one();
        lk.unlock();
    });

    std::unique_lock lk(shared_state->mutex);
    if (!shared_state->cv.wait_for(lk, delay, [&] {
            return shared_state->finished;
        })) {
        shared_state->finished = true;
        shared_state->promise.set_error(
            {ErrorCodes::RuntimeError, util::format("wait_for_future exceeded %1 ms", delay.count())});
    }

    return std::move(pf.future);
}

struct ExpectedRealmPaths {
    ExpectedRealmPaths(const std::string& base_path, const std::string& app_id, const std::string& user_identity,
                       const std::vector<std::string>& legacy_identities, const std::string& partition);
    std::string current_preferred_path;
    std::string fallback_hashed_path;
    std::string legacy_local_id_path;
    std::string legacy_sync_path;
    std::vector<std::string> legacy_sync_directories_to_make;
};

// Takes a string_view of a possibly quoted string (i.e. the string begins with '"' and ends with '"')
// and returns an owned string without the quotes.
std::string unquote_string(std::string_view possibly_quoted_string);

#if REALM_ENABLE_SYNC

template <typename Transport>
const std::shared_ptr<app::GenericNetworkTransport> instance_of = std::make_shared<Transport>();

std::ostream& operator<<(std::ostream& os, util::Optional<app::AppError> error);

void subscribe_to_all_and_bootstrap(Realm& realm);

#if REALM_APP_SERVICES
struct AutoVerifiedEmailCredentials : app::AppCredentials {
    AutoVerifiedEmailCredentials();
    std::string email;
    std::string password;
};

AutoVerifiedEmailCredentials create_user_and_log_in(app::SharedApp app);
// Log in the user again using the AutoVerifiedEmailCredentials returned
// when calling create_user_and_log_in()
void log_in_user(app::SharedApp app, app::AppCredentials creds);

#endif // REALM_APP_SERVICES

#if REALM_ENABLE_AUTH_TESTS
void wait_for_sessions_to_close(const TestAppSession& test_app_session);

std::string get_compile_time_base_url();
std::string get_compile_time_admin_url();
#endif // REALM_ENABLE_AUTH_TESTS

void wait_for_advance(Realm& realm);

StatusWith<std::shared_ptr<Realm>> async_open_realm(const Realm::Config& config);
std::shared_ptr<Realm> successfully_async_open_realm(const Realm::Config& config);

app::Response do_http_request(const app::Request& request);

class SynchronousTestTransport : public app::GenericNetworkTransport {
public:
    void send_request_to_server(const app::Request& request,
                                util::UniqueFunction<void(const app::Response&)>&& completion) override
    {
        {
            std::lock_guard barrier(m_mutex);
        }
        completion(do_http_request(request));
    }

    void block()
    {
        m_mutex.lock();
    }
    void unblock()
    {
        m_mutex.unlock();
    }

private:
    std::mutex m_mutex;
};

template <typename BaseTransport = SynchronousTestTransport>
class HookedTransport : public BaseTransport {
    static_assert(std::is_base_of_v<app::GenericNetworkTransport, BaseTransport>);

public:
    void send_request_to_server(const app::Request& request,
                                util::UniqueFunction<void(const app::Response&)>&& completion) override
    {
        if (request_hook) {
            if (auto simulated_response = request_hook(request)) {
                return completion(*simulated_response);
            }
        }
        BaseTransport::send_request_to_server(request, [&](app::Response response) mutable {
            if (response_hook) {
                response_hook(request, response);
            }
            completion(response);
        });
    }

    // Optional handler for the request and response before it is returned to completion
    util::UniqueFunction<void(const app::Request&, app::Response&)> response_hook;
    // Optional handler for the request before it is sent to the server
    util::UniqueFunction<std::optional<app::Response>(const app::Request&)> request_hook;
};


struct SocketProviderError {
    SocketProviderError(sync::HTTPStatus code, std::string message = "")
        : SocketProviderError(static_cast<int>(code), message)
    {
    }

    SocketProviderError(int code, std::string message = "")
        : status_code(code)
        , was_clean(code == 101)
        , body(message)
    {
    }

    using WebSocketError = sync::websocket::WebSocketError;
    SocketProviderError(WebSocketError error, std::string message = "")
        : was_clean(false)
        , ws_error(error)
        , body(message)
    {
    }

    int status_code = 0;
    bool was_clean = true;
    WebSocketError ws_error = WebSocketError::websocket_ok;
    std::string body;
};


struct HookedSocketProvider : public sync::websocket::DefaultSocketProvider {
    HookedSocketProvider(const std::shared_ptr<util::Logger>& logger, const std::string& user_agent,
                         AutoStart auto_start = AutoStart{true})
        : DefaultSocketProvider(logger, user_agent, nullptr, auto_start)
    {
    }

    std::unique_ptr<sync::WebSocketInterface> connect(std::unique_ptr<sync::WebSocketObserver> observer,
                                                      sync::WebSocketEndpoint&& endpoint) override
    {
        std::optional<SocketProviderError> error;
        if (endpoint_verify_func) {
            endpoint_verify_func(endpoint);
        }

        if (websocket_endpoint_resolver) {
            websocket_endpoint_resolver(endpoint);
        }

        if (websocket_connect_func) {
            error = websocket_connect_func();
        }

        if (error && error->ws_error != sync::websocket::WebSocketError::websocket_ok) {
            observer->websocket_error_handler();
            observer->websocket_closed_handler(error->was_clean, error->ws_error, error->body);
            return nullptr;
        }

        std::unique_ptr<sync::WebSocketInterface> websocket =
            DefaultSocketProvider::connect(std::move(observer), std::move(endpoint));
        if (error && error->status_code > 0) {
            auto default_websocket = dynamic_cast<sync::websocket::DefaultWebSocket*>(websocket.get());
            if (default_websocket)
                default_websocket->force_handshake_response_for_testing(error->status_code, error->body);
        }
        return websocket;
    }

    util::UniqueFunction<void(sync::WebSocketEndpoint&)> websocket_endpoint_resolver;
    util::UniqueFunction<void(const sync::WebSocketEndpoint&)> endpoint_verify_func;
    util::UniqueFunction<std::optional<SocketProviderError>()> websocket_connect_func;
};

#endif // REALM_ENABLE_SYNC

namespace reset_utils {

struct Partition {
    std::string property_name;
    std::string value;
};

Obj create_object(Realm& realm, StringData object_type, util::Optional<ObjectId> primary_key = util::none,
                  util::Optional<Partition> partition = util::none);

struct TestClientReset {
    using Callback = util::UniqueFunction<void(const SharedRealm&)>;
    using InitialObjectCallback = util::UniqueFunction<ObjectId(const SharedRealm&)>;
    TestClientReset(const Realm::Config& local_config, const Realm::Config& remote_config);
    virtual ~TestClientReset();
    TestClientReset* setup(Callback&& on_setup);

    // Only used in FLX sync client reset tests.
    TestClientReset* populate_initial_object(InitialObjectCallback&& callback);
    TestClientReset* make_local_changes(Callback&& changes_local);
    TestClientReset* make_remote_changes(Callback&& changes_remote);
    TestClientReset* on_post_local_changes(Callback&& post_local);
    TestClientReset* on_post_reset(Callback&& post_reset);
    void set_pk_of_object_driving_reset(const ObjectId& pk);
    ObjectId get_pk_of_object_driving_reset() const;
    void disable_wait_for_reset_completion();

    virtual TestClientReset* set_development_mode(bool enable = true);
    virtual void run() = 0;

protected:
    realm::Realm::Config m_local_config;
    realm::Realm::Config m_remote_config;

    Callback m_on_setup;
    InitialObjectCallback m_populate_initial_object;
    Callback m_make_local_changes;
    Callback m_make_remote_changes;
    Callback m_on_post_local;
    Callback m_on_post_reset;
    bool m_did_run = false;
    ObjectId m_pk_driving_reset = ObjectId::gen();
    bool m_wait_for_reset_completion = true;
};

#if REALM_ENABLE_SYNC

#if REALM_ENABLE_AUTH_TESTS
std::unique_ptr<TestClientReset> make_baas_client_reset(const Realm::Config& local_config,
                                                        const Realm::Config& remote_config,
                                                        TestAppSession& test_app_session);

std::unique_ptr<TestClientReset> make_baas_flx_client_reset(const Realm::Config& local_config,
                                                            const Realm::Config& remote_config,
                                                            const TestAppSession& test_app_session);

void wait_for_object_to_persist_to_atlas(std::shared_ptr<app::User> user, const AppSession& app_session,
                                         const std::string& schema_name, const bson::BsonDocument& filter_bson);

void wait_for_num_objects_in_atlas(std::shared_ptr<app::User> user, const AppSession& app_session,
                                   const std::string& schema_name, size_t expected_size);

void trigger_client_reset(const AppSession& app_session, const SyncSession& sync_session);
void trigger_client_reset(const AppSession& app_session, const SharedRealm& realm);
#endif // REALM_ENABLE_AUTH_TESTS

#endif // REALM_ENABLE_SYNC

std::unique_ptr<TestClientReset> make_fake_local_client_reset(const Realm::Config& local_config,
                                                              const Realm::Config& remote_config);

} // namespace reset_utils

} // namespace realm
