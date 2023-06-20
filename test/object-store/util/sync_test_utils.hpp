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

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_templated.hpp>

#if REALM_ENABLE_SYNC
#include <realm/object-store/sync/app.hpp>
#include <realm/object-store/sync/generic_network_transport.hpp>
#include <realm/object-store/sync/impl/sync_file.hpp>
#include <realm/object-store/sync/impl/sync_metadata.hpp>
#include <realm/object-store/sync/sync_session.hpp>
#include <realm/sync/network/http.hpp>
#include <external/json/json.hpp>
#endif

#include <realm/util/functional.hpp>
#include <realm/util/function_ref.hpp>

#include <util/event_loop.hpp>
#include <util/test_file.hpp>
#include <util/test_utils.hpp>

namespace realm {

bool results_contains_user(SyncUserMetadataResults& results, const std::string& identity,
                           const std::string& auth_server);
bool results_contains_original_name(SyncFileActionMetadataResults& results, const std::string& original_name);

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
    if (!shared_state->cv.wait_for(lk, max_ms, [&] {
            return shared_state->finished;
        })) {
        shared_state->finished = true;
        shared_state->promise.set_error(
            {ErrorCodes::RuntimeError, util::format("timed_future wait exceeded %1 ms", max_ms.count())});
    }

    return std::move(pf.future);
}

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

template <typename Transport>
const std::shared_ptr<app::GenericNetworkTransport> instance_of = std::make_shared<Transport>();

std::ostream& operator<<(std::ostream& os, util::Optional<app::AppError> error);

template <typename Transport>
TestSyncManager::Config get_config(Transport&& transport)
{
    TestSyncManager::Config config;
    config.transport = transport;
    return config;
}

const std::string CONTENT_TYPE_JSON = "application/json;charset=utf-8";
const std::string CONTENT_TYPE_PLAIN = "text/plain";

inline app::Response make_ok_response(std::string&& body = {})
{
    return {static_cast<int>(sync::HTTPStatus::Ok), 0, {}, std::move(body)};
}

inline app::Response make_json_response(sync::HTTPStatus http_status, nlohmann::json&& json_body)
{
    return {static_cast<int>(http_status), 0, {{"Content-Type", CONTENT_TYPE_JSON}}, json_body.dump()};
}

inline app::Response make_test_response(sync::HTTPStatus http_status, std::string&& body)
{
    return {static_cast<int>(http_status), 0, {{"Content-Type", CONTENT_TYPE_JSON}}, std::move(body)};
}

inline app::Response make_test_response(sync::HTTPStatus http_status, app::HttpHeaders&& headers, std::string&& body)
{
    return {static_cast<int>(http_status), 0, std::move(headers), std::move(body)};
}

inline app::Response make_location_response(std::string_view http_url, std::string_view websocket_url,
                                            std::string_view model = "GLOBAL", std::string_view location = "US-VA")
{
    return make_json_response(sync::HTTPStatus::Ok, {{"deployment_model", model},
                                                     {"location", location},
                                                     {"hostname", http_url},
                                                     {"ws_hostname", websocket_url}});
}

inline app::Response make_redirect_response(sync::HTTPStatus http_status, const std::string& new_url)
{
    return {static_cast<int>(http_status),
            0,
            {{"Location", new_url}, {"Content-Type", CONTENT_TYPE_PLAIN}},
            "Some body data"};
}

// A GenericNetworkTransport that relies on the simulated_response value to be
// provided as the response to a request. A request_hook can be provided to
// optionally provide a Response based on the request received.
class LocalTransport : public app::GenericNetworkTransport {
public:
    LocalTransport(app::Response&& response)
        : simulated_response{std::move(response)}
    {
    }

    LocalTransport(const app::Response& response)
        : simulated_response{response}
    {
    }

    LocalTransport()
        : LocalTransport(make_ok_response())
    {
    }

    void send_request_to_server(const app::Request& request,
                                util::UniqueFunction<void(const app::Response&)>&& completion) override
    {
        if (send_hook) {
            return send_hook(request, std::move(completion));
        }
        if (request_hook) {
            auto response = request_hook(request);
            if (response)
                return completion(*response);
        }
        completion(simulated_response);
    }

    inline void set_http_status(sync::HTTPStatus http_status)
    {
        simulated_response.http_status_code = static_cast<int>(http_status);
    }

    inline void set_custom_error(int custom_error)
    {
        simulated_response.custom_status_code = custom_error;
    }

    inline void set_body(std::string&& body)
    {
        simulated_response.body = std::move(body);
    }

    inline void set_headers(app::HttpHeaders&& headers)
    {
        simulated_response.headers = std::move(headers);
    }

    app::Response simulated_response;
    // Provided in case the error or message needs to be adjusted based on the request
    std::function<std::optional<app::Response>(const app::Request&)> request_hook;
    // Allow a subclass provide a function that replaces send_request_to_server();
    std::function<void(const app::Request&, util::UniqueFunction<void(const app::Response&)>&&)> send_hook;
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

std::unique_ptr<TestClientReset> make_fake_local_client_reset(const Realm::Config& local_config,
                                                              const Realm::Config& remote_config);

} // namespace reset_utils

} // namespace realm
