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

#include "logging.hpp"
#include <realm/object-store/c_api/types.hpp>
#include <realm/object-store/c_api/util.hpp>
#include <realm/object-store/sync/sync_session.hpp>

namespace realm::c_api {
namespace {
static inline realm::SyncSessionStopPolicy from_capi(realm_sync_session_stop_policy_e policy)
{
    switch (policy) {
        case RLM_SYNC_SESSION_STOP_POLICY_IMMEDIATELY:
            return realm::SyncSessionStopPolicy::Immediately;
        case RLM_SYNC_SESSION_STOP_POLICY_LIVE_INDEFINITELY:
            return realm::SyncSessionStopPolicy::LiveIndefinitely;
        case RLM_SYNC_SESSION_STOP_POLICY_AFTER_CHANGES_UPLOADED:
            return realm::SyncSessionStopPolicy::AfterChangesUploaded;
    }
    REALM_TERMINATE("Invalid session stop policy."); // LCOV_EXCL_LINE
}

static inline realm::ClientResyncMode from_capi(realm_sync_client_resync_mode_e mode)
{
    switch (mode) {
        case RLM_SYNC_CLIENT_RESYNC_MODE_DISCARD_LOCAL:
            return realm::ClientResyncMode::DiscardLocal;
        case RLM_SYNC_CLIENT_RESYNC_MODE_MANUAL:
            return realm::ClientResyncMode::Manual;
    }
    REALM_TERMINATE("Invalid client resync mode."); // LCOV_EXCL_LINE
}

static inline realm::SyncClientConfig::MetadataMode from_capi(realm_sync_client_metadata_mode mode)
{
    switch (mode) {
        case RLM_SYNC_CLIENT_METADATA_MODE_DISABLED:
            return realm::SyncClientConfig::MetadataMode::NoMetadata;
        case RLM_SYNC_CLIENT_METADATA_MODE_PLAINTEXT:
            return realm::SyncClientConfig::MetadataMode::NoEncryption;
        case RLM_SYNC_CLIENT_METADATA_MODE_ENCRYPTED:
            return realm::SyncClientConfig::MetadataMode::Encryption;
    }
    REALM_TERMINATE("Invalid metadata mode."); // LCOV_EXCL_LINE
}

static inline realm::ReconnectMode from_capi(realm_sync_client_reconnect_mode mode)
{
    switch (mode) {
        case RLM_SYNC_CLIENT_RECONNECT_MODE_NORMAL:
            return realm::ReconnectMode::normal;
        case RLM_SYNC_CLIENT_RECONNECT_MODE_TESTING:
            return realm::ReconnectMode::testing;
    }
    REALM_TERMINATE("Invalid reconnect mode."); // LCOV_EXCL_LINE
}

static inline realm_sync_session_state_e to_capi(realm::SyncSession::PublicState state)
{
    using State = realm::SyncSession::PublicState;
    switch (state) {
        case State::Active:
            return RLM_SYNC_SESSION_STATE_ACTIVE;
        case State::Dying:
            return RLM_SYNC_SESSION_STATE_DYING;
        case State::Inactive:
            return RLM_SYNC_SESSION_STATE_INACTIVE;
        case State::WaitingForAccessToken:
            return RLM_SYNC_SESSION_STATE_WAITING_FOR_ACCESS_TOKEN;
    };
    REALM_TERMINATE("Invalid session state."); // LCOV_EXCL_LINE
}

static inline realm_sync_connection_state_e to_capi(realm::SyncSession::ConnectionState state)
{
    using State = realm::SyncSession::ConnectionState;
    switch (state) {
        case State::Connected:
            return RLM_SYNC_CONNECTION_STATE_CONNECTED;
        case State::Connecting:
            return RLM_SYNC_CONNECTION_STATE_CONNECTING;
        case State::Disconnected:
            return RLM_SYNC_CONNECTION_STATE_DISCONNECTED;
    };
    REALM_TERMINATE("Invalid connection state."); // LCOV_EXCL_LINE
}

static inline realm::SyncSession::ProgressDirection from_capi(realm_sync_progress_direction_e direction)
{
    using Type = realm::SyncSession::ProgressDirection;
    switch (direction) {
        case RLM_SYNC_PROGRESS_DIRECTION_UPLOAD:
            return Type::upload;
        case RLM_SYNC_PROGRESS_DIRECTION_DOWNLOAD:
            return Type::download;
    };
    REALM_TERMINATE("Invalid progress direction."); // LCOV_EXCL_LINE
}

static inline realm_error_t to_capi(const realm::SyncError&)
{
    return {};
}

static inline realm_error_t to_capi(const std::error_code&)
{
    return {};
}

} // namespace
} // namespace realm::c_api

using namespace realm;
using namespace realm::c_api;

RLM_API realm_sync_config_t* realm_sync_config_new(const realm_user_t* user, const char* partition_value)
{
    return new realm_sync_config_t(*user, partition_value);
}

RLM_API void realm_sync_config_set_session_stop_policy(realm_sync_config_t* config,
                                                       realm_sync_session_stop_policy_e policy)
{
    config->stop_policy = from_capi(policy);
}

RLM_API void realm_sync_config_set_error_handler(realm_sync_config_t* config,
                                                 void (*callback)(void* userdata, const realm_sync_session_t*,
                                                                  realm_error_t),
                                                 void* userdata, realm_free_userdata_func_t userdata_free)
{
    auto cb = [callback, userdata = SharedUserdata(userdata, FreeUserdata(userdata_free))](
                  std::shared_ptr<SyncSession> session, SyncError error) {
        auto c_session = new realm_sync_session_t(session);
        callback(userdata.get(), c_session, to_capi(error));
        realm_release(c_session);
    };
    config->error_handler = std::move(cb);
}

RLM_API void realm_sync_config_set_client_validate_ssl(realm_sync_config_t* config, bool validate)
{
    config->client_validate_ssl = validate;
}

RLM_API void realm_sync_config_set_ssl_trust_certificate_path(realm_sync_config_t* config, const char* path)
{
    config->ssl_trust_certificate_path = std::string(path);
}

RLM_API void realm_sync_config_set_ssl_verify_callback(realm_sync_config_t* config,
                                                       realm_sync_ssl_verify_func_t callback, void* userdata,
                                                       realm_free_userdata_func_t userdata_free)
{
    auto cb = [callback, userdata = SharedUserdata(userdata, FreeUserdata(userdata_free))](
                  const std::string& server_address, SyncConfig::ProxyConfig::port_type server_port,
                  const char* pem_data, size_t pem_size, int preverify_ok, int depth) {
        return callback(userdata.get(), server_address.c_str(), server_port, pem_data, pem_size, preverify_ok, depth);
    };

    config->ssl_verify_callback = std::move(cb);
}

RLM_API void realm_sync_config_set_cancel_waits_on_nonfatal_error(realm_sync_config_t* config, bool cancel)
{
    config->cancel_waits_on_nonfatal_error = cancel;
}

RLM_API void realm_sync_config_set_authorization_header_name(realm_sync_config_t* config, const char* name)
{
    config->authorization_header_name = std::string(name);
}

RLM_API void realm_sync_config_set_custom_http_header(realm_sync_config_t* config, const char* name,
                                                      const char* value)
{
    config->custom_http_headers[name] = value;
}

RLM_API void realm_sync_config_set_recovery_directory_path(realm_sync_config_t* config, const char* path)
{
    config->recovery_directory = std::string(path);
}

RLM_API void realm_sync_config_set_resync_mode(realm_sync_config_t* config, realm_sync_client_resync_mode_e mode)
{
    config->client_resync_mode = from_capi(mode);
}

RLM_API realm_sync_client_config_t* realm_sync_client_config_new(void)
{
    return new realm_sync_client_config_t;
}

RLM_API void realm_sync_client_config_set_base_file_path(realm_sync_client_config_t* config, const char* path)
{
    config->base_file_path = path;
}

RLM_API void realm_sync_client_config_set_metadata_mode(realm_sync_client_config_t* config,
                                                        realm_sync_client_metadata_mode_e mode)
{
    config->metadata_mode = from_capi(mode);
}

RLM_API void realm_sync_client_config_set_encryption_key(realm_sync_client_config_t* config, const uint8_t key[64])
{
    config->custom_encryption_key = std::vector<char>(key, key + 64);
}

RLM_API void realm_sync_client_config_set_reset_metadata_on_error(realm_sync_client_config_t* config, bool reset)
{
    config->reset_metadata_on_error = reset;
}

RLM_API void realm_sync_client_config_set_logger_factory(realm_sync_client_config_t* config,
                                                         realm_logger_factory_func_t factory, void* userdata,
                                                         realm_free_userdata_func_t userdata_free)
{
    auto cb = [factory, userdata = SharedUserdata(userdata, FreeUserdata(userdata_free))](util::Logger::Level level) {
        realm_logger_t* logger = factory(userdata.get(), to_capi(level));
        auto scope_exit = util::make_scope_exit([logger]() noexcept {
            realm_release(logger);
        });
        return std::move(logger->logger);
    };
    config->logger_factory = std::move(cb);
}
RLM_API void realm_sync_client_config_set_log_level(realm_sync_client_config_t* config, realm_log_level_e level)
{
    config->log_level = from_capi(level);
}

RLM_API void realm_sync_client_config_set_reconnect_mode(realm_sync_client_config_t* config,
                                                         realm_sync_client_reconnect_mode_e mode)
{
    config->reconnect_mode = from_capi(mode);
}
RLM_API void realm_sync_client_config_set_multiplex_sessions(realm_sync_client_config_t* config, bool multiplex)
{
    config->multiplex_sessions = multiplex;
}

RLM_API void realm_sync_client_config_set_user_agent_binding_info(realm_sync_client_config_t* config,
                                                                  const char* info)
{
    config->user_agent_binding_info = info;
}

RLM_API void realm_sync_client_config_set_user_agent_application_info(realm_sync_client_config_t* config,
                                                                      const char* info)
{
    config->user_agent_application_info = info;
}

RLM_API void realm_sync_client_config_set_connect_timeout(realm_sync_client_config_t* config, uint64_t timeout)
{
    config->timeouts.connect_timeout = timeout;
}

RLM_API void realm_sync_client_config_set_connection_linger_time(realm_sync_client_config_t* config, uint64_t time)
{
    config->timeouts.connection_linger_time = time;
}

RLM_API void realm_sync_client_config_set_ping_keepalive_period(realm_sync_client_config_t* config, uint64_t period)
{
    config->timeouts.ping_keepalive_period = period;
}

RLM_API void realm_sync_client_config_set_pong_keepalive_timeout(realm_sync_client_config_t* config, uint64_t timeout)
{
    config->timeouts.pong_keepalive_timeout = timeout;
}

RLM_API void realm_sync_client_config_set_fast_reconnect_limit(realm_sync_client_config_t* config, uint64_t limit)
{
    config->timeouts.fast_reconnect_limit = limit;
}

RLM_API realm_sync_session_t* realm_sync_session_get(const realm_t* realm)
{
    if (auto session = (*realm)->sync_session()) {
        return new realm_sync_session_t(std::move(session));
    }

    return nullptr;
}

RLM_API realm_sync_session_state_e realm_sync_session_get_state(const realm_sync_session_t* session)
{
    return to_capi((*session)->state());
}

RLM_API realm_sync_connection_state_e realm_sync_session_get_connection_state(const realm_sync_session_t* session)
{
    return to_capi((*session)->connection_state());
}

RLM_API realm_user_t* realm_sync_session_get_user(const realm_sync_session_t* session)
{
    return new realm_user_t((*session)->user());
}

RLM_API const char* realm_sync_session_get_partition_value(const realm_sync_session_t* session)
{
    return (*session)->config().partition_value.c_str();
}

RLM_API const char* realm_sync_session_get_file_path(const realm_sync_session_t* session)
{
    return (*session)->path().c_str();
}

RLM_API bool realm_sync_session_pause(realm_sync_session_t* session)
{
    return wrap_err([&]() {
        (*session)->log_out();
        return true;
    });
}

RLM_API bool realm_sync_session_resume(realm_sync_session_t* session)
{
    return wrap_err([&]() {
        (*session)->revive_if_needed();
        return true;
    });
}

RLM_API uint64_t realm_sync_session_register_connection_state_change_callback(
    realm_sync_session_t* session,
    void (*callback)(void* userdata, realm_sync_connection_state_e old_state,
                     realm_sync_connection_state_e new_state),
    void* userdata, realm_free_userdata_func_t userdata_free)
{
    std::function<realm::SyncSession::ConnectionStateChangeCallback> cb =
        [callback, userdata = SharedUserdata{userdata, userdata_free}](auto old_state, auto new_state) {
            callback(userdata.get(), to_capi(old_state), to_capi(new_state));
        };
    return wrap_err(
        [&]() {
            return (*session)->register_connection_change_callback(std::move(cb));
        },
        -1);
}

RLM_API bool realm_sync_session_unregister_connection_state_change_callback(realm_sync_session_t* session,
                                                                            uint64_t token)
{
    return wrap_err([&]() {
        (*session)->unregister_connection_change_callback(token);
        return true;
    });
}

RLM_API uint64_t realm_sync_session_register_progress_notifier(
    realm_sync_session_t* session,
    void (*notifier)(void* userdata, uint64_t transferred_bytes, uint64_t transferrable_bytes),
    realm_sync_progress_direction_e direction, bool is_streaming, void* userdata,
    realm_free_userdata_func_t userdata_free)
{
    std::function<realm::SyncSession::ProgressNotifierCallback> cb =
        [notifier, userdata = SharedUserdata{userdata, userdata_free}](auto transferred, auto transferrable) {
            notifier(userdata.get(), transferred, transferrable);
        };
    return wrap_err(
        [&]() {
            return (*session)->register_progress_notifier(std::move(cb), from_capi(direction), is_streaming);
        },
        -1);
}

RLM_API bool realm_sync_session_unregister_progress_notifier(realm_sync_session_t* session, uint64_t token)
{
    return wrap_err([&]() {
        (*session)->unregister_progress_notifier(token);
        return true;
    });
}

RLM_API bool realm_sync_session_wait_for_download_completion(realm_sync_session_t* session,
                                                             void (*done)(void* userdata, const realm_error_t*),
                                                             void* userdata, realm_free_userdata_func_t userdata_free)
{
    std::function<void(std::error_code)> cb = [done, userdata =
                                                         SharedUserdata{userdata, userdata_free}](std::error_code e) {
        if (e) {
            realm_error_t error = to_capi(e);
            done(userdata.get(), &error);
        }
        else {
            done(userdata.get(), nullptr);
        }
    };
    return wrap_err([&]() {
        (*session)->wait_for_download_completion(std::move(cb));
        return true;
    });
}

RLM_API bool realm_sync_session_wait_for_upload_completion(realm_sync_session_t* session,
                                                           void (*done)(void* userdata, const realm_error_t*),
                                                           void* userdata, realm_free_userdata_func_t userdata_free)
{
    std::function<void(std::error_code)> cb = [done, userdata =
                                                         SharedUserdata{userdata, userdata_free}](std::error_code e) {
        if (e) {
            realm_error_t error = to_capi(e);
            done(userdata.get(), &error);
        }
        else {
            done(userdata.get(), nullptr);
        }
    };
    return wrap_err([&]() {
        (*session)->wait_for_upload_completion(std::move(cb));
        return true;
    });
}
