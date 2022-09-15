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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or utilied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include <realm/sync/config.hpp>
#include <realm/sync/client.hpp>
#include <realm/sync/protocol.hpp>
#include <realm/object-store/c_api/conversion.hpp>
#include <realm/object-store/sync/sync_manager.hpp>
#include <realm/object-store/sync/sync_session.hpp>
#include <realm/object-store/sync/async_open_task.hpp>
#include <realm/util/basic_system_errors.hpp>

#include "logging.hpp"
#include "types.hpp"
#include "util.hpp"

using realm::sync::Client;
using namespace realm::sync;

namespace realm::c_api {
static_assert(realm_sync_client_metadata_mode_e(SyncClientConfig::MetadataMode::NoEncryption) ==
              RLM_SYNC_CLIENT_METADATA_MODE_PLAINTEXT);
static_assert(realm_sync_client_metadata_mode_e(SyncClientConfig::MetadataMode::Encryption) ==
              RLM_SYNC_CLIENT_METADATA_MODE_ENCRYPTED);
static_assert(realm_sync_client_metadata_mode_e(SyncClientConfig::MetadataMode::NoMetadata) ==
              RLM_SYNC_CLIENT_METADATA_MODE_DISABLED);

static_assert(realm_sync_client_reconnect_mode_e(ReconnectMode::normal) == RLM_SYNC_CLIENT_RECONNECT_MODE_NORMAL);
static_assert(realm_sync_client_reconnect_mode_e(ReconnectMode::testing) == RLM_SYNC_CLIENT_RECONNECT_MODE_TESTING);

static_assert(realm_sync_session_resync_mode_e(ClientResyncMode::Manual) == RLM_SYNC_SESSION_RESYNC_MODE_MANUAL);
static_assert(realm_sync_session_resync_mode_e(ClientResyncMode::DiscardLocal) ==
              RLM_SYNC_SESSION_RESYNC_MODE_DISCARD_LOCAL);
static_assert(realm_sync_session_resync_mode_e(ClientResyncMode::Recover) == RLM_SYNC_SESSION_RESYNC_MODE_RECOVER);
static_assert(realm_sync_session_resync_mode_e(ClientResyncMode::RecoverOrDiscard) ==
              RLM_SYNC_SESSION_RESYNC_MODE_RECOVER_OR_DISCARD);

static_assert(realm_sync_session_stop_policy_e(SyncSessionStopPolicy::Immediately) ==
              RLM_SYNC_SESSION_STOP_POLICY_IMMEDIATELY);
static_assert(realm_sync_session_stop_policy_e(SyncSessionStopPolicy::LiveIndefinitely) ==
              RLM_SYNC_SESSION_STOP_POLICY_LIVE_INDEFINITELY);
static_assert(realm_sync_session_stop_policy_e(SyncSessionStopPolicy::AfterChangesUploaded) ==
              RLM_SYNC_SESSION_STOP_POLICY_AFTER_CHANGES_UPLOADED);

static_assert(realm_sync_session_state_e(SyncSession::State::Active) == RLM_SYNC_SESSION_STATE_ACTIVE);
static_assert(realm_sync_session_state_e(SyncSession::State::Dying) == RLM_SYNC_SESSION_STATE_DYING);
static_assert(realm_sync_session_state_e(SyncSession::State::Inactive) == RLM_SYNC_SESSION_STATE_INACTIVE);
static_assert(realm_sync_session_state_e(SyncSession::State::WaitingForAccessToken) ==
              RLM_SYNC_SESSION_STATE_WAITING_FOR_ACCESS_TOKEN);

static_assert(realm_sync_connection_state_e(SyncSession::ConnectionState::Disconnected) ==
              RLM_SYNC_CONNECTION_STATE_DISCONNECTED);
static_assert(realm_sync_connection_state_e(SyncSession::ConnectionState::Connecting) ==
              RLM_SYNC_CONNECTION_STATE_CONNECTING);
static_assert(realm_sync_connection_state_e(SyncSession::ConnectionState::Connected) ==
              RLM_SYNC_CONNECTION_STATE_CONNECTED);

static_assert(realm_sync_progress_direction_e(SyncSession::ProgressDirection::upload) ==
              RLM_SYNC_PROGRESS_DIRECTION_UPLOAD);
static_assert(realm_sync_progress_direction_e(SyncSession::ProgressDirection::download) ==
              RLM_SYNC_PROGRESS_DIRECTION_DOWNLOAD);


namespace {
using namespace realm::sync;
static_assert(realm_sync_error_action_e(ProtocolErrorInfo::Action::NoAction) == RLM_SYNC_ERROR_ACTION_NO_ACTION);
static_assert(realm_sync_error_action_e(ProtocolErrorInfo::Action::ProtocolViolation) ==
              RLM_SYNC_ERROR_ACTION_PROTOCOL_VIOLATION);
static_assert(realm_sync_error_action_e(ProtocolErrorInfo::Action::ApplicationBug) ==
              RLM_SYNC_ERROR_ACTION_APPLICATION_BUG);
static_assert(realm_sync_error_action_e(ProtocolErrorInfo::Action::Warning) == RLM_SYNC_ERROR_ACTION_WARNING);
static_assert(realm_sync_error_action_e(ProtocolErrorInfo::Action::Transient) == RLM_SYNC_ERROR_ACTION_TRANSIENT);
static_assert(realm_sync_error_action_e(ProtocolErrorInfo::Action::DeleteRealm) ==
              RLM_SYNC_ERROR_ACTION_DELETE_REALM);
static_assert(realm_sync_error_action_e(ProtocolErrorInfo::Action::ClientReset) ==
              RLM_SYNC_ERROR_ACTION_CLIENT_RESET);
static_assert(realm_sync_error_action_e(ProtocolErrorInfo::Action::ClientResetNoRecovery) ==
              RLM_SYNC_ERROR_ACTION_CLIENT_RESET_NO_RECOVERY);
} // namespace

static realm_sync_error_code_t to_capi(const Status& status, std::string& message)
{
    auto ret = realm_sync_error_code_t();

    // HACK: there isn't a good way to get a hold of "our" system category
    // so we have to make one of "our" error codes to access it
    const std::error_category* realm_basic_system_category;
    {
        using namespace realm::util::error;
        std::error_code dummy = make_error_code(basic_system_errors::invalid_argument);
        realm_basic_system_category = &dummy.category();
    }

    auto error_code = status.get_std_error_code();
    const std::error_category& category = error_code.category();
    if (category == realm::sync::client_error_category()) {
        ret.category = RLM_SYNC_ERROR_CATEGORY_CLIENT;
    }
    else if (category == realm::sync::protocol_error_category()) {
        if (realm::sync::is_session_level_error(realm::sync::ProtocolError(error_code.value()))) {
            ret.category = RLM_SYNC_ERROR_CATEGORY_SESSION;
        }
        else {
            ret.category = RLM_SYNC_ERROR_CATEGORY_CONNECTION;
        }
    }
    else if (category == std::system_category() || category == *realm_basic_system_category) {
        ret.category = RLM_SYNC_ERROR_CATEGORY_SYSTEM;
    }
    else {
        ret.category = RLM_SYNC_ERROR_CATEGORY_UNKNOWN;
    }

    ret.value = error_code.value();
    message = error_code.message(); // pass the string to the caller for lifetime purposes
    ret.message = message.c_str();
    ret.category_name = category.name();


    return ret;
}

static std::error_code sync_error_to_error_code(const realm_sync_error_code_t& sync_error_code)
{
    auto error = std::error_code();
    const realm_sync_error_category_e category = sync_error_code.category;
    if (category == RLM_SYNC_ERROR_CATEGORY_CLIENT) {
        error.assign(sync_error_code.value, realm::sync::client_error_category());
    }
    else if (category == RLM_SYNC_ERROR_CATEGORY_SESSION || category == RLM_SYNC_ERROR_CATEGORY_CONNECTION) {
        error.assign(sync_error_code.value, realm::sync::protocol_error_category());
    }
    else if (category == RLM_SYNC_ERROR_CATEGORY_SYSTEM) {
        error.assign(sync_error_code.value, std::system_category());
    }
    else if (category == RLM_SYNC_ERROR_CATEGORY_UNKNOWN) {
        using namespace realm::util::error;
        std::error_code dummy = make_error_code(basic_system_errors::invalid_argument);
        error.assign(sync_error_code.value, dummy.category());
    }
    return error;
}

static Query add_ordering_to_realm_query(Query realm_query, const DescriptorOrdering& ordering)
{
    auto ordering_copy = util::make_bind<DescriptorOrdering>();
    *ordering_copy = ordering;
    realm_query.set_ordering(ordering_copy);
    return realm_query;
}

RLM_API realm_sync_client_config_t* realm_sync_client_config_new(void) noexcept
{
    return new realm_sync_client_config_t;
}

RLM_API void realm_sync_client_config_set_base_file_path(realm_sync_client_config_t* config,
                                                         const char* path) noexcept
{
    config->base_file_path = path;
}

RLM_API void realm_sync_client_config_set_metadata_mode(realm_sync_client_config_t* config,
                                                        realm_sync_client_metadata_mode_e mode) noexcept
{
    config->metadata_mode = SyncClientConfig::MetadataMode(mode);
}

RLM_API void realm_sync_client_config_set_metadata_encryption_key(realm_sync_client_config_t* config,
                                                                  const uint8_t key[64]) noexcept
{
    config->custom_encryption_key = std::vector<char>(key, key + 64);
}

RLM_API void realm_sync_client_config_set_log_callback(realm_sync_client_config_t* config, realm_log_func_t callback,
                                                       realm_userdata_t userdata,
                                                       realm_free_userdata_func_t userdata_free) noexcept
{
    config->logger_factory = make_logger_factory(callback, userdata, userdata_free);
}

RLM_API void realm_sync_client_config_set_log_level(realm_sync_client_config_t* config,
                                                    realm_log_level_e level) noexcept
{
    config->log_level = realm::util::Logger::Level(level);
}

RLM_API void realm_sync_client_config_set_reconnect_mode(realm_sync_client_config_t* config,
                                                         realm_sync_client_reconnect_mode_e mode) noexcept
{
    config->reconnect_mode = ReconnectMode(mode);
}
RLM_API void realm_sync_client_config_set_multiplex_sessions(realm_sync_client_config_t* config,
                                                             bool multiplex) noexcept
{
    config->multiplex_sessions = multiplex;
}

RLM_API void realm_sync_client_config_set_user_agent_binding_info(realm_sync_client_config_t* config,
                                                                  const char* info) noexcept
{
    config->user_agent_binding_info = info;
}

RLM_API void realm_sync_client_config_set_user_agent_application_info(realm_sync_client_config_t* config,
                                                                      const char* info) noexcept
{
    config->user_agent_application_info = info;
}

RLM_API void realm_sync_client_config_set_connect_timeout(realm_sync_client_config_t* config,
                                                          uint64_t timeout) noexcept
{
    config->timeouts.connect_timeout = timeout;
}

RLM_API void realm_sync_client_config_set_connection_linger_time(realm_sync_client_config_t* config,
                                                                 uint64_t time) noexcept
{
    config->timeouts.connection_linger_time = time;
}

RLM_API void realm_sync_client_config_set_ping_keepalive_period(realm_sync_client_config_t* config,
                                                                uint64_t period) noexcept
{
    config->timeouts.ping_keepalive_period = period;
}

RLM_API void realm_sync_client_config_set_pong_keepalive_timeout(realm_sync_client_config_t* config,
                                                                 uint64_t timeout) noexcept
{
    config->timeouts.pong_keepalive_timeout = timeout;
}

RLM_API void realm_sync_client_config_set_fast_reconnect_limit(realm_sync_client_config_t* config,
                                                               uint64_t limit) noexcept
{
    config->timeouts.fast_reconnect_limit = limit;
}

RLM_API void realm_config_set_sync_config(realm_config_t* config, realm_sync_config_t* sync_config)
{
    config->sync_config = std::make_shared<SyncConfig>(*sync_config);
}

RLM_API realm_sync_config_t* realm_sync_config_new(const realm_user_t* user, const char* partition_value) noexcept
{
    return new realm_sync_config_t(*user, partition_value);
}

RLM_API realm_sync_config_t* realm_flx_sync_config_new(const realm_user_t* user) noexcept
{
    return new realm_sync_config(*user, realm::SyncConfig::FLXSyncEnabled{});
}

RLM_API void realm_sync_config_set_session_stop_policy(realm_sync_config_t* config,
                                                       realm_sync_session_stop_policy_e policy) noexcept
{
    config->stop_policy = SyncSessionStopPolicy(policy);
}

RLM_API void realm_sync_config_set_error_handler(realm_sync_config_t* config, realm_sync_error_handler_func_t handler,
                                                 realm_userdata_t userdata,
                                                 realm_free_userdata_func_t userdata_free) noexcept
{
    auto cb = [handler, userdata = SharedUserdata(userdata, FreeUserdata(userdata_free))](
                  std::shared_ptr<SyncSession> session, SyncError error) {
        auto c_error = realm_sync_error_t();

        std::string error_code_message;
        c_error.error_code = to_capi(error.to_status(), error_code_message);
        c_error.detailed_message = error.what();
        c_error.is_fatal = error.is_fatal;
        c_error.is_unrecognized_by_client = error.is_unrecognized_by_client;
        c_error.is_client_reset_requested = error.is_client_reset_requested();
        c_error.server_requests_action = static_cast<realm_sync_error_action_e>(error.server_requests_action);
        c_error.c_original_file_path_key = error.c_original_file_path_key;
        c_error.c_recovery_file_path_key = error.c_recovery_file_path_key;

        std::vector<realm_sync_error_user_info_t> c_user_info;
        c_user_info.reserve(error.user_info.size());
        for (auto& info : error.user_info) {
            c_user_info.push_back({info.first.c_str(), info.second.c_str()});
        }

        c_error.user_info_map = c_user_info.data();
        c_error.user_info_length = c_user_info.size();

        realm_sync_session_t c_session(session);
        handler(userdata.get(), &c_session, std::move(c_error));
    };
    config->error_handler = std::move(cb);
}

RLM_API void realm_sync_config_set_client_validate_ssl(realm_sync_config_t* config, bool validate) noexcept
{
    config->client_validate_ssl = validate;
}

RLM_API void realm_sync_config_set_ssl_trust_certificate_path(realm_sync_config_t* config, const char* path) noexcept
{
    config->ssl_trust_certificate_path = std::string(path);
}

RLM_API void realm_sync_config_set_ssl_verify_callback(realm_sync_config_t* config,
                                                       realm_sync_ssl_verify_func_t callback,
                                                       realm_userdata_t userdata,
                                                       realm_free_userdata_func_t userdata_free) noexcept
{
    auto cb = [callback, userdata = SharedUserdata(userdata, FreeUserdata(userdata_free))](
                  const std::string& server_address, SyncConfig::ProxyConfig::port_type server_port,
                  const char* pem_data, size_t pem_size, int preverify_ok, int depth) {
        return callback(userdata.get(), server_address.c_str(), server_port, pem_data, pem_size, preverify_ok, depth);
    };

    config->ssl_verify_callback = std::move(cb);
}

RLM_API void realm_sync_config_set_cancel_waits_on_nonfatal_error(realm_sync_config_t* config, bool cancel) noexcept
{
    config->cancel_waits_on_nonfatal_error = cancel;
}

RLM_API void realm_sync_config_set_authorization_header_name(realm_sync_config_t* config, const char* name) noexcept
{
    config->authorization_header_name = std::string(name);
}

RLM_API void realm_sync_config_set_custom_http_header(realm_sync_config_t* config, const char* name,
                                                      const char* value) noexcept
{
    config->custom_http_headers[name] = value;
}

RLM_API void realm_sync_config_set_recovery_directory_path(realm_sync_config_t* config, const char* path) noexcept
{
    config->recovery_directory = std::string(path);
}

RLM_API void realm_sync_config_set_resync_mode(realm_sync_config_t* config,
                                               realm_sync_session_resync_mode_e mode) noexcept
{
    config->client_resync_mode = ClientResyncMode(mode);
}

RLM_API realm_object_id_t realm_sync_subscription_id(const realm_flx_sync_subscription_t* subscription) noexcept
{
    REALM_ASSERT(subscription != nullptr);
    return to_capi(subscription->id());
}

RLM_API realm_string_t realm_sync_subscription_name(const realm_flx_sync_subscription_t* subscription) noexcept
{
    REALM_ASSERT(subscription != nullptr);
    return to_capi(subscription->name());
}

RLM_API realm_string_t
realm_sync_subscription_object_class_name(const realm_flx_sync_subscription_t* subscription) noexcept
{
    REALM_ASSERT(subscription != nullptr);
    return to_capi(subscription->object_class_name());
}

RLM_API realm_string_t
realm_sync_subscription_query_string(const realm_flx_sync_subscription_t* subscription) noexcept
{
    REALM_ASSERT(subscription != nullptr);
    return to_capi(subscription->query_string());
}

RLM_API realm_timestamp_t
realm_sync_subscription_created_at(const realm_flx_sync_subscription_t* subscription) noexcept
{
    REALM_ASSERT(subscription != nullptr);
    return to_capi(subscription->created_at());
}

RLM_API realm_timestamp_t
realm_sync_subscription_updated_at(const realm_flx_sync_subscription_t* subscription) noexcept
{
    REALM_ASSERT(subscription != nullptr);
    return to_capi(subscription->updated_at());
}

RLM_API void realm_sync_config_set_before_client_reset_handler(realm_sync_config_t* config,
                                                               realm_sync_before_client_reset_func_t callback,
                                                               realm_userdata_t userdata,
                                                               realm_free_userdata_func_t userdata_free) noexcept
{
    auto cb = [callback, userdata = SharedUserdata(userdata, FreeUserdata(userdata_free))](SharedRealm before_realm) {
        realm_t r1{before_realm};
        if (!callback(userdata.get(), &r1)) {
            throw CallbackFailed();
        }
    };
    config->notify_before_client_reset = std::move(cb);
}

RLM_API void realm_sync_config_set_after_client_reset_handler(realm_sync_config_t* config,
                                                              realm_sync_after_client_reset_func_t callback,
                                                              realm_userdata_t userdata,
                                                              realm_free_userdata_func_t userdata_free) noexcept
{
    auto cb = [callback, userdata = SharedUserdata(userdata, FreeUserdata(userdata_free))](
                  SharedRealm before_realm, ThreadSafeReference after_realm, bool did_recover) {
        realm_t r1{before_realm};
        auto tsr = realm_t::thread_safe_reference(std::move(after_realm));
        if (!callback(userdata.get(), &r1, &tsr, did_recover)) {
            throw CallbackFailed();
        }
    };
    config->notify_after_client_reset = std::move(cb);
}

RLM_API realm_flx_sync_subscription_set_t* realm_sync_get_latest_subscription_set(const realm_t* realm)
{
    REALM_ASSERT(realm != nullptr);
    return wrap_err([&]() {
        return new realm_flx_sync_subscription_set_t((*realm)->get_latest_subscription_set());
    });
}

RLM_API realm_flx_sync_subscription_set_t* realm_sync_get_active_subscription_set(const realm_t* realm)
{
    REALM_ASSERT(realm != nullptr);
    return wrap_err([&]() {
        return new realm_flx_sync_subscription_set_t((*realm)->get_active_subscription_set());
    });
}

RLM_API realm_flx_sync_subscription_set_state_e
realm_sync_on_subscription_set_state_change_wait(const realm_flx_sync_subscription_set_t* subscription_set,
                                                 realm_flx_sync_subscription_set_state_e notify_when) noexcept
{
    REALM_ASSERT(subscription_set != nullptr);
    SubscriptionSet::State state =
        subscription_set->get_state_change_notification(SubscriptionSet::State{notify_when}).get();
    return realm_flx_sync_subscription_set_state_e(static_cast<int>(state));
}

RLM_API bool
realm_sync_on_subscription_set_state_change_async(const realm_flx_sync_subscription_set_t* subscription_set,
                                                  realm_flx_sync_subscription_set_state_e notify_when,
                                                  realm_sync_on_subscription_state_changed_t callback,
                                                  realm_userdata_t userdata, realm_free_userdata_func_t userdata_free)
{
    REALM_ASSERT(subscription_set != nullptr && callback != nullptr);
    return wrap_err([&]() {
        auto future_state = subscription_set->get_state_change_notification(SubscriptionSet::State{notify_when});
        std::move(future_state)
            .get_async([callback, userdata = SharedUserdata(userdata, FreeUserdata(userdata_free))](
                           const StatusWith<SubscriptionSet::State>& state) -> void {
                if (state.is_ok())
                    callback(userdata.get(),
                             realm_flx_sync_subscription_set_state_e(static_cast<int>(state.get_value())));
                else
                    callback(userdata.get(), realm_flx_sync_subscription_set_state_e::RLM_SYNC_SUBSCRIPTION_ERROR);
            });
        return true;
    });
}

RLM_API int64_t
realm_sync_subscription_set_version(const realm_flx_sync_subscription_set_t* subscription_set) noexcept
{
    REALM_ASSERT(subscription_set != nullptr);
    return subscription_set->version();
}

RLM_API realm_flx_sync_subscription_set_state_e
realm_sync_subscription_set_state(const realm_flx_sync_subscription_set_t* subscription_set) noexcept
{
    REALM_ASSERT(subscription_set != nullptr);
    return static_cast<realm_flx_sync_subscription_set_state_e>(subscription_set->state());
}

RLM_API const char*
realm_sync_subscription_set_error_str(const realm_flx_sync_subscription_set_t* subscription_set) noexcept
{
    REALM_ASSERT(subscription_set != nullptr);
    return subscription_set->error_str().data();
}

RLM_API size_t realm_sync_subscription_set_size(const realm_flx_sync_subscription_set_t* subscription_set) noexcept
{
    REALM_ASSERT(subscription_set != nullptr);
    return subscription_set->size();
}

RLM_API realm_flx_sync_subscription_t*
realm_sync_find_subscription_by_name(const realm_flx_sync_subscription_set_t* subscription_set,
                                     const char* name) noexcept
{
    REALM_ASSERT(subscription_set != nullptr);
    auto it = subscription_set->find(name);
    if (it == subscription_set->end())
        return nullptr;
    return new realm_flx_sync_subscription_t(*it);
}

RLM_API realm_flx_sync_subscription_t*
realm_sync_find_subscription_by_results(const realm_flx_sync_subscription_set_t* subscription_set,
                                        realm_results_t* results) noexcept
{
    REALM_ASSERT(subscription_set != nullptr);
    auto realm_query = add_ordering_to_realm_query(results->get_query(), results->get_ordering());
    auto it = subscription_set->find(realm_query);
    if (it == subscription_set->end())
        return nullptr;
    return new realm_flx_sync_subscription_t{*it};
}

RLM_API realm_flx_sync_subscription_t*
realm_sync_subscription_at(const realm_flx_sync_subscription_set_t* subscription_set, size_t index)
{
    REALM_ASSERT(subscription_set != nullptr && index < subscription_set->size());
    try {
        return new realm_flx_sync_subscription_t{subscription_set->at(index)};
    }
    catch (...) {
        return nullptr;
    }
}

RLM_API realm_flx_sync_subscription_t*
realm_sync_find_subscription_by_query(const realm_flx_sync_subscription_set_t* subscription_set,
                                      realm_query_t* query) noexcept
{
    REALM_ASSERT(subscription_set != nullptr);
    auto realm_query = add_ordering_to_realm_query(query->get_query(), query->get_ordering());
    auto it = subscription_set->find(realm_query);
    if (it == subscription_set->end())
        return nullptr;
    return new realm_flx_sync_subscription_t(*it);
}

RLM_API bool realm_sync_subscription_set_refresh(realm_flx_sync_subscription_set_t* subscription_set)
{
    REALM_ASSERT(subscription_set != nullptr);
    return wrap_err([&]() {
        subscription_set->refresh();
        return true;
    });
}

RLM_API realm_flx_sync_mutable_subscription_set_t*
realm_sync_make_subscription_set_mutable(realm_flx_sync_subscription_set_t* subscription_set)
{
    REALM_ASSERT(subscription_set != nullptr);
    return wrap_err([&]() {
        return new realm_flx_sync_mutable_subscription_set_t{subscription_set->make_mutable_copy()};
    });
}

RLM_API bool realm_sync_subscription_set_clear(realm_flx_sync_mutable_subscription_set_t* subscription_set)
{
    REALM_ASSERT(subscription_set != nullptr);
    return wrap_err([&]() {
        subscription_set->clear();
        return true;
    });
}

RLM_API bool
realm_sync_subscription_set_insert_or_assign_results(realm_flx_sync_mutable_subscription_set_t* subscription_set,
                                                     realm_results_t* results, const char* name, size_t* index,
                                                     bool* inserted)
{
    REALM_ASSERT(subscription_set != nullptr && results != nullptr);
    return wrap_err([&]() {
        auto realm_query = add_ordering_to_realm_query(results->get_query(), results->get_ordering());
        const auto [it, successful] = name ? subscription_set->insert_or_assign(name, realm_query)
                                           : subscription_set->insert_or_assign(realm_query);
        *index = std::distance(subscription_set->begin(), it);
        *inserted = successful;
        return true;
    });
}

RLM_API bool
realm_sync_subscription_set_insert_or_assign_query(realm_flx_sync_mutable_subscription_set_t* subscription_set,
                                                   realm_query_t* query, const char* name, size_t* index,
                                                   bool* inserted)
{
    REALM_ASSERT(subscription_set != nullptr && query != nullptr);
    return wrap_err([&]() {
        auto realm_query = add_ordering_to_realm_query(query->get_query(), query->get_ordering());
        const auto [it, successful] = name ? subscription_set->insert_or_assign(name, realm_query)
                                           : subscription_set->insert_or_assign(realm_query);
        *index = std::distance(subscription_set->begin(), it);
        *inserted = successful;
        return true;
    });
}

RLM_API bool realm_sync_subscription_set_erase_by_id(realm_flx_sync_mutable_subscription_set_t* subscription_set,
                                                     const realm_object_id_t* id, bool* erased)
{
    REALM_ASSERT(subscription_set != nullptr && id != nullptr);
    *erased = false;
    return wrap_err([&] {
        auto it = std::find_if(subscription_set->begin(), subscription_set->end(), [id](const Subscription& sub) {
            return from_capi(*id) == sub.id();
        });
        if (it != subscription_set->end()) {
            subscription_set->erase(it);
            *erased = true;
        }
        return true;
    });
}

RLM_API bool realm_sync_subscription_set_erase_by_name(realm_flx_sync_mutable_subscription_set_t* subscription_set,
                                                       const char* name, bool* erased)
{
    REALM_ASSERT(subscription_set != nullptr && name != nullptr);
    *erased = false;
    return wrap_err([&]() {
        if (auto it = subscription_set->find(name); it != subscription_set->end()) {
            subscription_set->erase(it);
            *erased = true;
        }
        return true;
    });
}

RLM_API bool realm_sync_subscription_set_erase_by_query(realm_flx_sync_mutable_subscription_set_t* subscription_set,
                                                        realm_query_t* query, bool* erased)
{
    REALM_ASSERT(subscription_set != nullptr && query != nullptr);
    *erased = false;
    return wrap_err([&]() {
        auto realm_query = add_ordering_to_realm_query(query->get_query(), query->get_ordering());
        if (auto it = subscription_set->find(realm_query); it != subscription_set->end()) {
            subscription_set->erase(it);
            *erased = true;
        }
        return true;
    });
}

RLM_API bool realm_sync_subscription_set_erase_by_results(realm_flx_sync_mutable_subscription_set_t* subscription_set,
                                                          realm_results_t* results, bool* erased)
{
    REALM_ASSERT(subscription_set != nullptr && results != nullptr);
    *erased = false;
    return wrap_err([&]() {
        auto realm_query = add_ordering_to_realm_query(results->get_query(), results->get_ordering());
        if (auto it = subscription_set->find(realm_query); it != subscription_set->end()) {
            subscription_set->erase(it);
            *erased = true;
        }
        return true;
    });
}

RLM_API realm_flx_sync_subscription_set_t*
realm_sync_subscription_set_commit(realm_flx_sync_mutable_subscription_set_t* subscription_set)
{
    REALM_ASSERT(subscription_set != nullptr);
    return wrap_err([&]() {
        return new realm_flx_sync_subscription_set_t{std::move(*subscription_set).commit()};
    });
}

RLM_API realm_async_open_task_t* realm_open_synchronized(realm_config_t* config) noexcept
{
    return wrap_err([config] {
        return new realm_async_open_task_t(Realm::get_synchronized_realm(*config));
    });
}

RLM_API void realm_async_open_task_start(realm_async_open_task_t* task, realm_async_open_task_completion_func_t done,
                                         realm_userdata_t userdata, realm_free_userdata_func_t userdata_free) noexcept
{
    auto cb = [done, userdata = SharedUserdata(userdata, FreeUserdata(userdata_free))](ThreadSafeReference realm,
                                                                                       std::exception_ptr error) {
        if (error) {
            realm_async_error_t c_error(std::move(error));
            done(userdata.get(), nullptr, &c_error);
        }
        else {
            auto tsr = new realm_t::thread_safe_reference(std::move(realm));
            done(userdata.get(), tsr, nullptr);
        }
    };
    (*task)->start(std::move(cb));
}

RLM_API void realm_async_open_task_cancel(realm_async_open_task_t* task) noexcept
{
    (*task)->cancel();
}

RLM_API uint64_t realm_async_open_task_register_download_progress_notifier(
    realm_async_open_task_t* task, realm_sync_progress_func_t notifier, realm_userdata_t userdata,
    realm_free_userdata_func_t userdata_free) noexcept
{
    auto cb = [notifier, userdata = SharedUserdata(userdata, FreeUserdata(userdata_free))](uint64_t transferred,
                                                                                           uint64_t transferrable) {
        notifier(userdata.get(), transferred, transferrable);
    };
    return (*task)->register_download_progress_notifier(std::move(cb));
}

RLM_API void realm_async_open_task_unregister_download_progress_notifier(realm_async_open_task_t* task,
                                                                         uint64_t token) noexcept
{
    (*task)->unregister_download_progress_notifier(token);
}

RLM_API realm_sync_session_t* realm_sync_session_get(const realm_t* realm) noexcept
{
    if (auto session = (*realm)->sync_session()) {
        return new realm_sync_session_t(std::move(session));
    }

    return nullptr;
}

RLM_API realm_sync_session_state_e realm_sync_session_get_state(const realm_sync_session_t* session) noexcept
{
    return realm_sync_session_state_e((*session)->state());
}

RLM_API realm_sync_connection_state_e
realm_sync_session_get_connection_state(const realm_sync_session_t* session) noexcept
{
    return realm_sync_connection_state_e((*session)->connection_state());
}

RLM_API realm_user_t* realm_sync_session_get_user(const realm_sync_session_t* session) noexcept
{
    return new realm_user_t((*session)->user());
}

RLM_API const char* realm_sync_session_get_partition_value(const realm_sync_session_t* session) noexcept
{
    return (*session)->config().partition_value.c_str();
}

RLM_API const char* realm_sync_session_get_file_path(const realm_sync_session_t* session) noexcept
{
    return (*session)->path().c_str();
}

RLM_API void realm_sync_session_pause(realm_sync_session_t* session) noexcept
{
    (*session)->log_out();
}

RLM_API void realm_sync_session_resume(realm_sync_session_t* session) noexcept
{
    (*session)->revive_if_needed();
}

RLM_API bool realm_sync_immediately_run_file_actions(realm_app* app, const char* sync_path) noexcept
{
    return wrap_err([&]() {
        return (*app)->sync_manager()->immediately_run_file_actions(sync_path);
    });
}

RLM_API uint64_t realm_sync_session_register_connection_state_change_callback(
    realm_sync_session_t* session, realm_sync_connection_state_changed_func_t callback, realm_userdata_t userdata,
    realm_free_userdata_func_t userdata_free) noexcept
{
    std::function<realm::SyncSession::ConnectionStateChangeCallback> cb =
        [callback, userdata = SharedUserdata(userdata, FreeUserdata(userdata_free))](auto old_state, auto new_state) {
            callback(userdata.get(), realm_sync_connection_state_e(old_state),
                     realm_sync_connection_state_e(new_state));
        };
    return (*session)->register_connection_change_callback(std::move(cb));
}

RLM_API void realm_sync_session_unregister_connection_state_change_callback(realm_sync_session_t* session,
                                                                            uint64_t token) noexcept
{
    (*session)->unregister_connection_change_callback(token);
}

RLM_API uint64_t realm_sync_session_register_progress_notifier(realm_sync_session_t* session,
                                                               realm_sync_progress_func_t notifier,
                                                               realm_sync_progress_direction_e direction,
                                                               bool is_streaming, realm_userdata_t userdata,
                                                               realm_free_userdata_func_t userdata_free) noexcept
{
    std::function<realm::SyncSession::ProgressNotifierCallback> cb =
        [notifier, userdata = SharedUserdata(userdata, FreeUserdata(userdata_free))](uint64_t transferred,
                                                                                     uint64_t transferrable) {
            notifier(userdata.get(), transferred, transferrable);
        };
    return (*session)->register_progress_notifier(std::move(cb), SyncSession::ProgressDirection(direction),
                                                  is_streaming);
}

RLM_API void realm_sync_session_unregister_progress_notifier(realm_sync_session_t* session, uint64_t token) noexcept
{
    (*session)->unregister_progress_notifier(token);
}

RLM_API void realm_sync_session_wait_for_download_completion(realm_sync_session_t* session,
                                                             realm_sync_wait_for_completion_func_t done,
                                                             realm_userdata_t userdata,
                                                             realm_free_userdata_func_t userdata_free) noexcept
{
    util::UniqueFunction<void(Status)> cb =
        [done, userdata = SharedUserdata(userdata, FreeUserdata(userdata_free))](Status s) {
            if (s.get_std_error_code()) {
                std::string error_code_message;
                realm_sync_error_code_t error = to_capi(s, error_code_message);
                done(userdata.get(), &error);
            }
            else {
                done(userdata.get(), nullptr);
            }
        };
    (*session)->wait_for_download_completion(std::move(cb));
}

RLM_API void realm_sync_session_wait_for_upload_completion(realm_sync_session_t* session,
                                                           realm_sync_wait_for_completion_func_t done,
                                                           realm_userdata_t userdata,
                                                           realm_free_userdata_func_t userdata_free) noexcept
{
    util::UniqueFunction<void(Status)> cb =
        [done, userdata = SharedUserdata(userdata, FreeUserdata(userdata_free))](Status s) {
            if (s.get_std_error_code()) {
                std::string error_code_message;
                realm_sync_error_code_t error = to_capi(s, error_code_message);
                done(userdata.get(), &error);
            }
            else {
                done(userdata.get(), nullptr);
            }
        };
    (*session)->wait_for_upload_completion(std::move(cb));
}

RLM_API void realm_sync_session_handle_error_for_testing(const realm_sync_session_t* session, int error_code,
                                                         int error_category, const char* error_message, bool is_fatal)
{
    REALM_ASSERT(session);
    realm_sync_error_code_t sync_error{static_cast<realm_sync_error_category_e>(error_category), error_code,
                                       error_message};
    auto err = sync_error_to_error_code(sync_error);
    SyncSession::OnlyForTesting::handle_error(*session->get(), {err, error_message, is_fatal});
}

} // namespace realm::c_api
