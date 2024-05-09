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

#include "types.hpp"
#include "util.hpp"
#include "conversion.hpp"

#include <realm/object-store/sync/sync_user.hpp>
#include <realm/object-store/sync/mongo_client.hpp>
#include <realm/object-store/sync/mongo_database.hpp>

namespace realm::c_api {
using namespace realm::app;

static_assert(realm_user_state_e(SyncUser::State::LoggedOut) == RLM_USER_STATE_LOGGED_OUT);
static_assert(realm_user_state_e(SyncUser::State::LoggedIn) == RLM_USER_STATE_LOGGED_IN);
static_assert(realm_user_state_e(SyncUser::State::Removed) == RLM_USER_STATE_REMOVED);

#if REALM_APP_SERVICES

static_assert(realm_auth_provider_e(AuthProvider::ANONYMOUS) == RLM_AUTH_PROVIDER_ANONYMOUS);
static_assert(realm_auth_provider_e(AuthProvider::ANONYMOUS_NO_REUSE) == RLM_AUTH_PROVIDER_ANONYMOUS_NO_REUSE);
static_assert(realm_auth_provider_e(AuthProvider::FACEBOOK) == RLM_AUTH_PROVIDER_FACEBOOK);
static_assert(realm_auth_provider_e(AuthProvider::GOOGLE) == RLM_AUTH_PROVIDER_GOOGLE);
static_assert(realm_auth_provider_e(AuthProvider::APPLE) == RLM_AUTH_PROVIDER_APPLE);
static_assert(realm_auth_provider_e(AuthProvider::CUSTOM) == RLM_AUTH_PROVIDER_CUSTOM);
static_assert(realm_auth_provider_e(AuthProvider::USERNAME_PASSWORD) == RLM_AUTH_PROVIDER_EMAIL_PASSWORD);
static_assert(realm_auth_provider_e(AuthProvider::FUNCTION) == RLM_AUTH_PROVIDER_FUNCTION);
static_assert(realm_auth_provider_e(AuthProvider::API_KEY) == RLM_AUTH_PROVIDER_API_KEY);

static_assert(realm_sync_client_metadata_mode_e(app::AppConfig::MetadataMode::NoEncryption) ==
              RLM_SYNC_CLIENT_METADATA_MODE_PLAINTEXT);
static_assert(realm_sync_client_metadata_mode_e(app::AppConfig::MetadataMode::Encryption) ==
              RLM_SYNC_CLIENT_METADATA_MODE_ENCRYPTED);
static_assert(realm_sync_client_metadata_mode_e(app::AppConfig::MetadataMode::InMemory) ==
              RLM_SYNC_CLIENT_METADATA_MODE_DISABLED);

static inline bson::BsonArray parse_ejson_array(const char* serialized)
{
    if (!serialized) {
        return {};
    }
    else {
        return bson::BsonArray(bson::parse({serialized, strlen(serialized)}));
    }
}

static realm_app_error_t to_capi(const AppError& error)
{
    auto ret = realm_app_error_t();

    ret.error = realm_errno_e(error.code());
    ret.categories = ErrorCodes::error_categories(error.code()).value();

    if (error.additional_status_code) {
        ret.http_status_code = *error.additional_status_code;
    }

    ret.message = error.what();

    if (error.link_to_server_logs.size() > 0) {
        ret.link_to_server_logs = error.link_to_server_logs.c_str();
    }

    return ret;
}

static inline realm_app_user_apikey_t to_capi(const App::UserAPIKey& apikey)
{
    return {to_capi(apikey.id), apikey.key ? apikey.key->c_str() : nullptr, apikey.name.c_str(), apikey.disabled};
}

static inline auto make_callback(realm_app_void_completion_func_t callback, realm_userdata_t userdata,
                                 realm_free_userdata_func_t userdata_free)
{
    return
        [callback, userdata = SharedUserdata(userdata, FreeUserdata(userdata_free))](util::Optional<AppError> error) {
            if (error) {
                realm_app_error_t c_err{to_capi(*error)};
                callback(userdata.get(), &c_err);
            }
            else {
                callback(userdata.get(), nullptr);
            }
        };
}

static inline auto make_callback(realm_app_user_completion_func_t callback, realm_userdata_t userdata,
                                 realm_free_userdata_func_t userdata_free)
{
    return [callback, userdata = SharedUserdata(userdata, FreeUserdata(userdata_free))](
               std::shared_ptr<app::User> user, util::Optional<AppError> error) {
        if (error) {
            realm_app_error_t c_err{to_capi(*error)};
            callback(userdata.get(), nullptr, &c_err);
        }
        else {
            auto c_user = realm_user_t(std::move(user));
            callback(userdata.get(), &c_user, nullptr);
        }
    };
}

static inline auto make_callback(void (*callback)(realm_userdata_t userdata, realm_app_user_apikey_t*,
                                                  const realm_app_error_t*),
                                 realm_userdata_t userdata, realm_free_userdata_func_t userdata_free)
{
    return [callback, userdata = SharedUserdata(userdata, FreeUserdata(userdata_free))](
               App::UserAPIKey apikey, util::Optional<AppError> error) {
        if (error) {
            realm_app_error_t c_error(to_capi(*error));
            callback(userdata.get(), nullptr, &c_error);
        }
        else {
            realm_app_user_apikey_t c_apikey(to_capi(apikey));
            callback(userdata.get(), &c_apikey, nullptr);
        }
    };
}

RLM_API const char* realm_app_get_default_base_url(void) noexcept
{
    return app::App::default_base_url().data();
}

RLM_API realm_app_credentials_t* realm_app_credentials_new_anonymous(bool reuse_credentials) noexcept
{
    return new realm_app_credentials_t(AppCredentials::anonymous(reuse_credentials));
}

RLM_API realm_app_credentials_t* realm_app_credentials_new_facebook(const char* access_token) noexcept
{
    return new realm_app_credentials_t(AppCredentials::facebook(access_token));
}

RLM_API realm_app_credentials_t* realm_app_credentials_new_google_id_token(const char* id_token) noexcept
{
    return new realm_app_credentials_t(AppCredentials::google(IdToken(id_token)));
}

RLM_API realm_app_credentials_t* realm_app_credentials_new_google_auth_code(const char* auth_code) noexcept
{
    return new realm_app_credentials_t(AppCredentials::google(AuthCode(auth_code)));
}

RLM_API realm_app_credentials_t* realm_app_credentials_new_apple(const char* id_token) noexcept
{
    return new realm_app_credentials_t(AppCredentials::apple(id_token));
}

RLM_API realm_app_credentials_t* realm_app_credentials_new_jwt(const char* jwt_token) noexcept
{
    return new realm_app_credentials_t(AppCredentials::custom(jwt_token));
}

RLM_API realm_app_credentials_t* realm_app_credentials_new_email_password(const char* email,
                                                                          realm_string_t password) noexcept
{
    return new realm_app_credentials_t(AppCredentials::username_password(email, from_capi(password)));
}

RLM_API realm_app_credentials_t* realm_app_credentials_new_function(const char* serialized_ejson_payload)
{
    return wrap_err([&] {
        return new realm_app_credentials_t(AppCredentials::function(serialized_ejson_payload));
    });
}

RLM_API realm_app_credentials_t* realm_app_credentials_new_api_key(const char* api_key) noexcept
{
    return new realm_app_credentials_t(AppCredentials::api_key(api_key));
}

RLM_API realm_auth_provider_e realm_auth_credentials_get_provider(realm_app_credentials_t* credentials) noexcept
{
    return realm_auth_provider_e(credentials->provider());
}

RLM_API realm_app_config_t* realm_app_config_new(const char* app_id,
                                                 const realm_http_transport_t* http_transport) noexcept
{
    auto* config = new realm_app_config_t;
    config->app_id = app_id;
    config->transport = *http_transport; // realm_http_transport_t is a shared_ptr
    return config;
}

RLM_API void realm_app_config_set_base_url(realm_app_config_t* config, const char* base_url) noexcept
{
    config->base_url = std::string(base_url);
}

RLM_API void realm_app_config_set_default_request_timeout(realm_app_config_t* config, uint64_t ms) noexcept
{
    config->default_request_timeout_ms = ms;
}

RLM_API void realm_app_config_set_platform_version(realm_app_config_t* config, const char* platform_version) noexcept
{
    config->device_info.platform_version = std::string(platform_version);
}

RLM_API void realm_app_config_set_sdk_version(realm_app_config_t* config, const char* sdk_version) noexcept
{
    config->device_info.sdk_version = std::string(sdk_version);
}

RLM_API void realm_app_config_set_sdk(realm_app_config_t* config, const char* sdk) noexcept
{
    config->device_info.sdk = std::string(sdk);
}

RLM_API void realm_app_config_set_device_name(realm_app_config_t* config, const char* device_name) noexcept
{
    config->device_info.device_name = std::string(device_name);
}

RLM_API void realm_app_config_set_device_version(realm_app_config_t* config, const char* device_version) noexcept
{
    config->device_info.device_version = std::string(device_version);
}

RLM_API void realm_app_config_set_framework_name(realm_app_config_t* config, const char* framework_name) noexcept
{
    config->device_info.framework_name = std::string(framework_name);
}

RLM_API void realm_app_config_set_framework_version(realm_app_config_t* config,
                                                    const char* framework_version) noexcept
{
    config->device_info.framework_version = std::string(framework_version);
}

RLM_API void realm_app_config_set_bundle_id(realm_app_config_t* config, const char* bundle_id) noexcept
{
    config->device_info.bundle_id = std::string(bundle_id);
}

RLM_API void realm_app_config_set_base_file_path(realm_app_config_t* config, const char* path) noexcept
{
    config->base_file_path = path;
}

RLM_API void realm_app_config_set_metadata_mode(realm_app_config_t* config,
                                                realm_sync_client_metadata_mode_e mode) noexcept
{
    config->metadata_mode = app::AppConfig::MetadataMode(mode);
}

RLM_API void realm_app_config_set_metadata_encryption_key(realm_app_config_t* config, const uint8_t key[64]) noexcept
{
    config->custom_encryption_key = std::vector<char>(key, key + 64);
}

RLM_API void realm_app_config_set_security_access_group(realm_app_config_t* config, const char* group) noexcept
{
    config->security_access_group = group;
}

RLM_API const char* realm_app_credentials_serialize_as_json(realm_app_credentials_t* app_credentials) noexcept
{
    return wrap_err([&] {
        return duplicate_string(app_credentials->serialize_as_json());
    });
}

RLM_API realm_app_t* realm_app_create(const realm_app_config_t* app_config)
{
    return wrap_err([&] {
        return new realm_app_t(App::get_app(app::App::CacheMode::Disabled, *app_config));
    });
}

RLM_API realm_app_t* realm_app_create_cached(const realm_app_config_t* app_config)
{
    return wrap_err([&] {
        return new realm_app_t(App::get_app(app::App::CacheMode::Enabled, *app_config));
    });
}

RLM_API bool realm_app_get_cached(const char* app_id, const char* base_url, realm_app_t** out_app)
{
    return wrap_err([&] {
        auto app =
            App::get_cached_app(std::string(app_id), base_url ? util::some<std::string>(base_url) : util::none);
        if (out_app) {
            *out_app = app ? new realm_app_t(app) : nullptr;
        }

        return true;
    });
}

RLM_API void realm_clear_cached_apps(void) noexcept
{
    App::clear_cached_apps();
}

RLM_API const char* realm_app_get_app_id(const realm_app_t* app) noexcept
{
    return (*app)->app_id().c_str();
}

RLM_API bool realm_app_update_base_url(realm_app_t* app, const char* base_url,
                                       realm_app_void_completion_func_t callback, realm_userdata_t userdata,
                                       realm_free_userdata_func_t userdata_free)
{
    return wrap_err([&] {
        (*app)->update_base_url(base_url ? base_url : "", make_callback(callback, userdata, userdata_free));
        return true;
    });
}

RLM_API char* realm_app_get_base_url(realm_app_t* app) noexcept
{
    auto url_stg = (*app)->get_base_url();
    return duplicate_string(url_stg);
}

RLM_API realm_user_t* realm_app_get_current_user(const realm_app_t* app) noexcept
{
    if (auto user = (*app)->current_user()) {
        return new realm_user_t(user);
    }

    return nullptr;
}

RLM_API bool realm_app_get_all_users(const realm_app_t* app, realm_user_t** out_users, size_t capacity, size_t* out_n)
{
    return wrap_err([&] {
        const auto& users = (*app)->all_users();
        set_out_param(out_n, users.size());
        if (out_users && capacity >= users.size()) {
            OutBuffer<realm_user_t> buf(out_users);
            for (const auto& user : users) {
                buf.emplace(user);
            }
            buf.release(out_n);
        }
        return true;
    });
}

RLM_API bool realm_app_log_in_with_credentials(realm_app_t* app, realm_app_credentials_t* credentials,
                                               realm_app_user_completion_func_t callback, realm_userdata_t userdata,
                                               realm_free_userdata_func_t userdata_free)
{
    return wrap_err([&] {
        (*app)->log_in_with_credentials(*credentials, make_callback(callback, userdata, userdata_free));
        return true;
    });
}

RLM_API bool realm_app_log_out_current_user(realm_app_t* app, realm_app_void_completion_func_t callback,
                                            realm_userdata_t userdata, realm_free_userdata_func_t userdata_free)
{
    return wrap_err([&] {
        (*app)->log_out(make_callback(callback, userdata, userdata_free));
        return true;
    });
}

namespace {
template <typename Fn>
auto with_app_user(const realm_user_t* user, Fn&& fn)
{
    auto app_user = std::dynamic_pointer_cast<realm::app::User>(*user);
    return wrap_err([&] {
        if (!app_user) {
            throw Exception(ErrorCodes::InvalidArgument, "App Services function require a user obtained from an App");
        }
        if constexpr (std::is_void_v<decltype(fn(app_user))>) {
            fn(app_user);
            return true;
        }
        else {
            return fn(app_user);
        }
    });
}
} // anonymous namespace

RLM_API bool realm_app_refresh_custom_data(realm_app_t* app, realm_user_t* user,
                                           realm_app_void_completion_func_t callback, realm_userdata_t userdata,
                                           realm_free_userdata_func_t userdata_free)
{
    return with_app_user(user, [&](auto& user) {
        (*app)->refresh_custom_data(user, make_callback(callback, userdata, userdata_free));
    });
}

RLM_API bool realm_app_log_out(realm_app_t* app, realm_user_t* user, realm_app_void_completion_func_t callback,
                               realm_userdata_t userdata, realm_free_userdata_func_t userdata_free)
{
    return with_app_user(user, [&](auto& user) {
        (*app)->log_out(user, make_callback(callback, userdata, userdata_free));
    });
}

RLM_API bool realm_app_link_user(realm_app_t* app, realm_user_t* user, realm_app_credentials_t* credentials,
                                 realm_app_user_completion_func_t callback, realm_userdata_t userdata,
                                 realm_free_userdata_func_t userdata_free)
{
    return with_app_user(user, [&](auto& user) {
        (*app)->link_user(user, *credentials, make_callback(callback, userdata, userdata_free));
    });
}

RLM_API bool realm_app_switch_user(realm_app_t* app, realm_user_t* user)
{
    return with_app_user(user, [&](auto& user) {
        (*app)->switch_user(user);
    });
}

RLM_API bool realm_app_remove_user(realm_app_t* app, realm_user_t* user, realm_app_void_completion_func_t callback,
                                   realm_userdata_t userdata, realm_free_userdata_func_t userdata_free)
{
    return with_app_user(user, [&](auto& user) {
        (*app)->remove_user(user, make_callback(callback, userdata, userdata_free));
    });
}

RLM_API bool realm_app_delete_user(realm_app_t* app, realm_user_t* user, realm_app_void_completion_func_t callback,
                                   realm_userdata_t userdata, realm_free_userdata_func_t userdata_free)
{
    return with_app_user(user, [&](auto& user) {
        (*app)->delete_user(user, make_callback(callback, userdata, userdata_free));
    });
}

RLM_API bool realm_app_email_password_provider_client_register_email(realm_app_t* app, const char* email,
                                                                     realm_string_t password,
                                                                     realm_app_void_completion_func_t callback,
                                                                     realm_userdata_t userdata,
                                                                     realm_free_userdata_func_t userdata_free)
{
    return wrap_err([&] {
        (*app)->provider_client<App::UsernamePasswordProviderClient>().register_email(
            email, from_capi(password), make_callback(callback, userdata, userdata_free));
        return true;
    });
}

RLM_API bool realm_app_email_password_provider_client_confirm_user(realm_app_t* app, const char* token,
                                                                   const char* token_id,
                                                                   realm_app_void_completion_func_t callback,
                                                                   realm_userdata_t userdata,
                                                                   realm_free_userdata_func_t userdata_free)
{
    return wrap_err([&] {
        (*app)->provider_client<App::UsernamePasswordProviderClient>().confirm_user(
            token, token_id, make_callback(callback, userdata, userdata_free));
        return true;
    });
}

RLM_API bool realm_app_email_password_provider_client_resend_confirmation_email(
    realm_app_t* app, const char* email, realm_app_void_completion_func_t callback, realm_userdata_t userdata,
    realm_free_userdata_func_t userdata_free)
{
    return wrap_err([&] {
        (*app)->provider_client<App::UsernamePasswordProviderClient>().resend_confirmation_email(
            email, make_callback(callback, userdata, userdata_free));
        return true;
    });
}

RLM_API bool realm_app_email_password_provider_client_send_reset_password_email(
    realm_app_t* app, const char* email, realm_app_void_completion_func_t callback, realm_userdata_t userdata,
    realm_free_userdata_func_t userdata_free)
{
    return wrap_err([&] {
        (*app)->provider_client<App::UsernamePasswordProviderClient>().send_reset_password_email(
            email, make_callback(callback, userdata, userdata_free));
        return true;
    });
}

RLM_API bool realm_app_email_password_provider_client_retry_custom_confirmation(
    realm_app_t* app, const char* email, realm_app_void_completion_func_t callback, realm_userdata_t userdata,
    realm_free_userdata_func_t userdata_free)
{
    return wrap_err([&] {
        (*app)->provider_client<App::UsernamePasswordProviderClient>().retry_custom_confirmation(
            email, make_callback(callback, userdata, userdata_free));
        return true;
    });
}

RLM_API bool realm_app_email_password_provider_client_reset_password(realm_app_t* app, realm_string_t password,
                                                                     const char* token, const char* token_id,
                                                                     realm_app_void_completion_func_t callback,
                                                                     realm_userdata_t userdata,
                                                                     realm_free_userdata_func_t userdata_free)
{
    return wrap_err([&] {
        (*app)->provider_client<App::UsernamePasswordProviderClient>().reset_password(
            from_capi(password), token, token_id, make_callback(callback, userdata, userdata_free));
        return true;
    });
}

RLM_API bool realm_app_email_password_provider_client_call_reset_password_function(
    realm_app_t* app, const char* email, realm_string_t password, const char* serialized_ejson_payload,
    realm_app_void_completion_func_t callback, realm_userdata_t userdata, realm_free_userdata_func_t userdata_free)
{
    return wrap_err([&] {
        bson::BsonArray args = parse_ejson_array(serialized_ejson_payload);
        (*app)->provider_client<App::UsernamePasswordProviderClient>().call_reset_password_function(
            email, from_capi(password), args, make_callback(callback, userdata, userdata_free));
        return true;
    });
}

RLM_API bool realm_app_user_apikey_provider_client_create_apikey(const realm_app_t* app, const realm_user_t* user,
                                                                 const char* name,
                                                                 realm_return_apikey_func_t callback,
                                                                 realm_userdata_t userdata,
                                                                 realm_free_userdata_func_t userdata_free)
{
    return with_app_user(user, [&](auto& user) {
        (*app)->provider_client<App::UserAPIKeyProviderClient>().create_api_key(
            name, user, make_callback(callback, userdata, userdata_free));
    });
}

RLM_API bool realm_app_user_apikey_provider_client_fetch_apikey(const realm_app_t* app, const realm_user_t* user,
                                                                realm_object_id_t id,
                                                                realm_return_apikey_func_t callback,
                                                                realm_userdata_t userdata,
                                                                realm_free_userdata_func_t userdata_free)
{
    return with_app_user(user, [&](auto& user) {
        (*app)->provider_client<App::UserAPIKeyProviderClient>().fetch_api_key(
            from_capi(id), user, make_callback(callback, userdata, userdata_free));
    });
}

RLM_API bool realm_app_user_apikey_provider_client_fetch_apikeys(const realm_app_t* app, const realm_user_t* user,
                                                                 realm_return_apikey_list_func_t callback,
                                                                 realm_userdata_t userdata,
                                                                 realm_free_userdata_func_t userdata_free)
{
    return with_app_user(user, [&](auto& user) {
        auto cb = [callback, userdata = SharedUserdata{userdata, FreeUserdata(userdata_free)}](
                      std::vector<App::UserAPIKey> apikeys, util::Optional<AppError> error) {
            if (error) {
                realm_app_error_t c_error(to_capi(*error));
                callback(userdata.get(), nullptr, 0, &c_error);
            }
            else {
                std::vector<realm_app_user_apikey_t> c_apikeys;
                c_apikeys.reserve(apikeys.size());
                for (const auto& apikey : apikeys) {
                    c_apikeys.push_back(to_capi(apikey));
                }
                callback(userdata.get(), c_apikeys.data(), c_apikeys.size(), nullptr);
            }
        };

        (*app)->provider_client<App::UserAPIKeyProviderClient>().fetch_api_keys(user, std::move(cb));
    });
}

RLM_API bool realm_app_user_apikey_provider_client_delete_apikey(const realm_app_t* app, const realm_user_t* user,
                                                                 realm_object_id_t id,
                                                                 realm_app_void_completion_func_t callback,
                                                                 realm_userdata_t userdata,
                                                                 realm_free_userdata_func_t userdata_free)
{
    return with_app_user(user, [&](auto& user) {
        (*app)->provider_client<App::UserAPIKeyProviderClient>().delete_api_key(
            from_capi(id), user, make_callback(callback, userdata, userdata_free));
    });
}

RLM_API bool realm_app_user_apikey_provider_client_enable_apikey(const realm_app_t* app, const realm_user_t* user,
                                                                 realm_object_id_t id,
                                                                 realm_app_void_completion_func_t callback,
                                                                 realm_userdata_t userdata,
                                                                 realm_free_userdata_func_t userdata_free)
{
    return with_app_user(user, [&](auto& user) {
        (*app)->provider_client<App::UserAPIKeyProviderClient>().enable_api_key(
            from_capi(id), user, make_callback(callback, userdata, userdata_free));
    });
}

RLM_API bool realm_app_user_apikey_provider_client_disable_apikey(const realm_app_t* app, const realm_user_t* user,
                                                                  realm_object_id_t id,
                                                                  realm_app_void_completion_func_t callback,
                                                                  realm_userdata_t userdata,
                                                                  realm_free_userdata_func_t userdata_free)
{
    return with_app_user(user, [&](auto& user) {
        (*app)->provider_client<App::UserAPIKeyProviderClient>().disable_api_key(
            from_capi(id), user, make_callback(callback, userdata, userdata_free));
    });
}

RLM_API bool realm_app_push_notification_client_register_device(
    const realm_app_t* app, const realm_user_t* user, const char* service_name, const char* registration_token,
    realm_app_void_completion_func_t callback, realm_userdata_t userdata, realm_free_userdata_func_t userdata_free)
{
    return with_app_user(user, [&](auto& user) {
        (*app)
            ->push_notification_client(service_name)
            .register_device(registration_token, user, make_callback(callback, userdata, userdata_free));
    });
}

RLM_API bool realm_app_push_notification_client_deregister_device(const realm_app_t* app, const realm_user_t* user,
                                                                  const char* service_name,
                                                                  realm_app_void_completion_func_t callback,
                                                                  realm_userdata_t userdata,
                                                                  realm_free_userdata_func_t userdata_free)
{
    return with_app_user(user, [&](auto& user) {
        (*app)
            ->push_notification_client(service_name)
            .deregister_device(user, make_callback(callback, userdata, userdata_free));
    });
}

RLM_API bool realm_app_call_function(const realm_app_t* app, const realm_user_t* user, const char* function_name,
                                     const char* serialized_ejson_payload, const char* service_name,
                                     realm_return_string_func_t callback, realm_userdata_t userdata,
                                     realm_free_userdata_func_t userdata_free)
{
    return with_app_user(user, [&](auto& user) {
        auto cb = [callback, userdata = SharedUserdata{userdata, FreeUserdata(userdata_free)}](
                      const std::string* reply, util::Optional<AppError> error) {
            if (error) {
                realm_app_error_t c_error(to_capi(*error));
                callback(userdata.get(), nullptr, &c_error);
            }
            else {
                callback(userdata.get(), reply->c_str(), nullptr);
            }
        };
        util::Optional<std::string> service_name_opt =
            service_name ? util::some<std::string>(service_name) : util::none;
        (*app)->call_function(user, function_name, serialized_ejson_payload, service_name_opt, std::move(cb));
    });
}

RLM_API void realm_app_sync_client_reconnect(realm_app_t* app) noexcept
{
    (*app)->sync_manager()->reconnect();
}

RLM_API bool realm_app_sync_client_has_sessions(const realm_app_t* app) noexcept
{
    return (*app)->sync_manager()->has_existing_sessions();
}

RLM_API void realm_app_sync_client_wait_for_sessions_to_terminate(realm_app_t* app) noexcept
{
    (*app)->sync_manager()->wait_for_sessions_to_terminate();
}

RLM_API char* realm_app_sync_client_get_default_file_path_for_realm(const realm_sync_config_t* config,
                                                                    const char* custom_filename)
{
    return wrap_err([&]() -> char* {
        auto user = std::dynamic_pointer_cast<app::User>(config->user);
        if (!user) {
            return nullptr;
        }
        util::Optional<std::string> filename =
            custom_filename ? util::some<std::string>(custom_filename) : util::none;
        std::string file_path = user->app()->path_for_realm(*config, std::move(filename));
        return duplicate_string(file_path);
    });
}

RLM_API bool realm_user_get_all_identities(const realm_user_t* user, realm_user_identity_t* out_identities,
                                           size_t max, size_t* out_n)
{
    return with_app_user(user, [&](auto& user) {
        const auto& identities = user->identities();
        set_out_param(out_n, identities.size());
        if (out_identities && max >= identities.size()) {
            for (size_t i = 0; i < identities.size(); i++) {
                out_identities[i] = {duplicate_string(identities[i].id),
                                     realm_auth_provider_e(enum_from_provider_type(identities[i].provider_type))};
            }
        }
    });
}

RLM_API char* realm_user_get_device_id(const realm_user_t* user) noexcept
{
    char* device_id = nullptr;
    with_app_user(user, [&](auto& user) {
        if (user->has_device_id()) {
            device_id = duplicate_string(user->device_id());
        }
    });
    return device_id;
}

RLM_API bool realm_user_log_out(realm_user_t* user)
{
    return with_app_user(user, [&](auto& user) {
        user->log_out();
    });
}

RLM_API char* realm_user_get_profile_data(const realm_user_t* user)
{
    return with_app_user(user, [&](auto& user) {
        std::string data = bson::Bson(user->user_profile().data()).to_string();
        return duplicate_string(data);
    });
}

RLM_API char* realm_user_get_custom_data(const realm_user_t* user) noexcept
{
    return with_app_user(user, [&](auto& user) -> char* {
        if (const auto& data = user->custom_data()) {
            std::string json = bson::Bson(*data).to_string();
            return duplicate_string(json);
        }
        return nullptr;
    });
}

RLM_API realm_app_t* realm_user_get_app(const realm_user_t* user) noexcept
{
    REALM_ASSERT(user);
    return with_app_user(user, [&](auto& user) {
        return new realm_app_t(user->app());
    });
}

RLM_API char* realm_user_get_identity(const realm_user_t* user) noexcept
{
    return duplicate_string((*user)->user_id());
}

RLM_API realm_user_state_e realm_user_get_state(const realm_user_t* user) noexcept
{
    return realm_user_state_e((*user)->state());
}

RLM_API bool realm_user_is_logged_in(const realm_user_t* user) noexcept
{
    return (*user)->is_logged_in();
}

RLM_API char* realm_user_get_access_token(const realm_user_t* user)
{
    return wrap_err([&] {
        return duplicate_string((*user)->access_token());
    });
}

RLM_API char* realm_user_get_refresh_token(const realm_user_t* user)
{
    return wrap_err([&] {
        return duplicate_string((*user)->refresh_token());
    });
}

RLM_API realm_app_user_subscription_token_t*
realm_sync_user_on_state_change_register_callback(realm_user_t* user, realm_sync_on_user_state_changed_t callback,
                                                  realm_userdata_t userdata, realm_free_userdata_func_t userdata_free)
{
    return with_app_user(user, [&](auto& user) {
        auto cb = [callback,
                   userdata = SharedUserdata{userdata, FreeUserdata(userdata_free)}](const SyncUser& sync_user) {
            callback(userdata.get(), realm_user_state_e(sync_user.state()));
        };
        auto token = user->subscribe(std::move(cb));
        return new realm_app_user_subscription_token_t{user, std::move(token)};
    });
}

RLM_API bool realm_sync_immediately_run_file_actions(realm_app_t* realm_app, const char* sync_path,
                                                     bool* did_run) noexcept
{
    return wrap_err([&]() {
        *did_run = (*realm_app)->immediately_run_file_actions(sync_path);
        return true;
    });
}

#endif // REALM_APP_SERVICES

#if !REALM_APP_SERVICES

static void cb_proxy_for_completion(realm_userdata_t userdata, const realm_app_error_t* err)
{
    SyncUser::CompletionHandler* cxx_cb = static_cast<SyncUser::CompletionHandler*>(userdata);
    REALM_ASSERT(cxx_cb);
    std::optional<AppError> cxx_err;
    if (err) {
        std::optional<int> additional_error_code;
        if (err->http_status_code) {
            additional_error_code = err->http_status_code;
        }
        cxx_err =
            AppError(ErrorCodes::Error(err->error), err->message, err->link_to_server_logs, additional_error_code);
    }
    (*cxx_cb)(cxx_err);
    delete cxx_cb;
}

struct CAPIAppUser : SyncUser {
    void* m_userdata = nullptr;
    realm_free_userdata_func_t m_free = nullptr;
    const std::string m_app_id;
    const std::string m_user_id;
    realm_user_get_access_token_cb_t m_access_token_cb = nullptr;
    realm_user_get_refresh_token_cb_t m_refresh_token_cb = nullptr;
    realm_user_state_cb_t m_state_cb = nullptr;
    realm_user_access_token_refresh_required_cb_t m_atrr_cb = nullptr;
    realm_user_get_sync_manager_cb_t m_sync_manager_cb = nullptr;
    realm_user_request_log_out_cb_t m_request_log_out_cb = nullptr;
    realm_user_request_refresh_location_cb_t m_request_refresh_location_cb = nullptr;
    realm_user_request_access_token_cb_t m_request_access_token_cb = nullptr;
    realm_user_track_realm_cb_t m_track_realm_cb = nullptr;
    realm_user_create_file_action_cb_t m_create_fa_cb = nullptr;

    CAPIAppUser(const char* app_id, const char* user_id)
        : m_app_id(app_id)
        , m_user_id(user_id)
    {
    }
    CAPIAppUser(CAPIAppUser&& other)
        : m_userdata(std::exchange(other.m_userdata, nullptr))
        , m_free(std::exchange(other.m_free, nullptr))
        , m_app_id(std::move(other.m_app_id))
        , m_user_id(std::move(other.m_user_id))
        , m_access_token_cb(std::move(other.m_access_token_cb))
        , m_refresh_token_cb(std::move(other.m_refresh_token_cb))
        , m_state_cb(std::move(other.m_state_cb))
        , m_atrr_cb(std::move(other.m_atrr_cb))
        , m_sync_manager_cb(std::move(other.m_sync_manager_cb))
        , m_request_log_out_cb(std::move(other.m_request_log_out_cb))
        , m_request_refresh_location_cb(std::move(other.m_request_refresh_location_cb))
        , m_request_access_token_cb(std::move(other.m_request_access_token_cb))
        , m_track_realm_cb(std::move(other.m_track_realm_cb))
        , m_create_fa_cb(std::move(other.m_create_fa_cb))
    {
    }

    ~CAPIAppUser()
    {
        if (m_free)
            m_free(m_userdata);
    }
    std::string user_id() const noexcept override
    {
        return m_user_id;
    }
    std::string app_id() const noexcept override
    {
        return m_app_id;
    }
    std::string access_token() const override
    {
        return m_access_token_cb(m_userdata);
    }
    std::string refresh_token() const override
    {
        return m_refresh_token_cb(m_userdata);
    }
    State state() const override
    {
        return State(m_state_cb(m_userdata));
    }
    bool access_token_refresh_required() const override
    {
        return m_atrr_cb(m_userdata);
    }
    SyncManager* sync_manager() override
    {
        auto value = m_sync_manager_cb(m_userdata);
        if (value && value->get()) {
            return (value->get());
        }
        return nullptr;
    }
    void request_log_out() override
    {
        m_request_log_out_cb(m_userdata);
    }
    void request_refresh_location(CompletionHandler&& callback) override
    {
        auto unscoped_cb = new CompletionHandler(std::move(callback));
        m_request_refresh_location_cb(m_userdata, cb_proxy_for_completion, unscoped_cb);
    }
    void request_access_token(CompletionHandler&& callback) override
    {
        auto unscoped_cb = new CompletionHandler(std::move(callback));
        m_request_access_token_cb(m_userdata, cb_proxy_for_completion, unscoped_cb);
    }
    void track_realm(std::string_view path) override
    {
        if (m_track_realm_cb) {
            m_track_realm_cb(m_userdata, path.data());
        }
    }
    std::string create_file_action(SyncFileAction a, std::string_view path,
                                   std::optional<std::string> recovery_dir) override
    {

        if (m_create_fa_cb) {
            return m_create_fa_cb(m_userdata, realm_sync_file_action_e(a), path.data(),
                                  recovery_dir ? recovery_dir->data() : nullptr);
        }
        return "";
    }
};

RLM_API realm_user_t* realm_user_new(realm_sync_user_create_config_t c) noexcept
{
    // optional to provide:
    // m_userdata
    // m_free
    // m_track_realm_cb
    // m_create_fa_cb

    REALM_ASSERT(c.app_id);
    REALM_ASSERT(c.user_id);
    REALM_ASSERT(c.access_token_cb);
    REALM_ASSERT(c.refresh_token_cb);
    REALM_ASSERT(c.state_cb);
    REALM_ASSERT(c.atrr_cb);
    REALM_ASSERT(c.sync_manager_cb);
    REALM_ASSERT(c.request_log_out_cb);
    REALM_ASSERT(c.request_refresh_location_cb);
    REALM_ASSERT(c.request_access_token_cb);

    return wrap_err([&]() {
        auto capi_user = std::make_shared<CAPIAppUser>(c.app_id, c.user_id);
        capi_user->m_userdata = c.userdata;
        capi_user->m_free = c.free_func;
        capi_user->m_access_token_cb = c.access_token_cb;
        capi_user->m_refresh_token_cb = c.refresh_token_cb;
        capi_user->m_state_cb = c.state_cb;
        capi_user->m_atrr_cb = c.atrr_cb;
        capi_user->m_sync_manager_cb = c.sync_manager_cb;
        capi_user->m_request_log_out_cb = c.request_log_out_cb;
        capi_user->m_request_refresh_location_cb = c.request_refresh_location_cb;
        capi_user->m_request_access_token_cb = c.request_access_token_cb;
        capi_user->m_track_realm_cb = c.track_realm_cb;
        capi_user->m_create_fa_cb = c.create_fa_cb;

        return new realm_user_t(std::move(capi_user));
    });
}

RLM_API realm_sync_manager_t* realm_sync_manager_create(const realm_sync_client_config_t* config)
{
    return wrap_err([&]() {
        auto manager = SyncManager::create(*config);
        return new realm_sync_manager_t(std::move(manager));
    });
}

RLM_API void realm_sync_manager_set_route(const realm_sync_manager_t* manager, const char* route, bool is_verified)
{
    REALM_ASSERT(manager);
    (*manager)->set_sync_route(route, is_verified);
}
#endif // #!REALM_APP_SERVICES

#if REALM_APP_SERVICES

namespace {
template <typename T>
util::Optional<T> convert_to_optional(T data)
{
    return data ? util::Optional<T>(data) : util::Optional<T>();
}

template <typename T>
util::Optional<T> convert_to_optional_bson(realm_string_t doc)
{
    if (doc.data == nullptr || doc.size == 0) {
        return util::Optional<T>();
    }
    return util::Optional<T>(static_cast<T>(bson::parse({doc.data, doc.size})));
}

template <typename T>
T convert_to_bson(realm_string_t doc)
{
    auto res = convert_to_optional_bson<T>(doc);
    return res ? *res : T();
}

MongoCollection::FindOptions to_mongodb_collection_find_options(const realm_mongodb_find_options_t* options)
{
    MongoCollection::FindOptions mongodb_options;
    mongodb_options.projection_bson = convert_to_optional_bson<bson::BsonDocument>(options->projection_bson);
    mongodb_options.sort_bson = convert_to_optional_bson<bson::BsonDocument>(options->sort_bson);
    mongodb_options.limit = convert_to_optional(options->limit);
    return mongodb_options;
}

MongoCollection::FindOneAndModifyOptions
to_mongodb_collection_find_one_and_modify_options(const realm_mongodb_find_one_and_modify_options_t* options)
{
    MongoCollection::FindOneAndModifyOptions mongodb_options;
    mongodb_options.projection_bson = convert_to_optional_bson<bson::BsonDocument>(options->projection_bson);
    mongodb_options.sort_bson = convert_to_optional_bson<bson::BsonDocument>(options->sort_bson);
    mongodb_options.upsert = options->upsert;
    mongodb_options.return_new_document = options->return_new_document;
    return mongodb_options;
}

void handle_mongodb_collection_result(util::Optional<bson::Bson> bson, util::Optional<AppError> app_error,
                                      UserdataPtr data, realm_mongodb_callback_t callback)
{
    if (app_error) {
        auto error = to_capi(*app_error);
        callback(data.get(), {nullptr, 0}, &error);
    }
    else if (bson) {
        const auto& bson_data = bson->to_string();
        callback(data.get(), {bson_data.c_str(), bson_data.size()}, nullptr);
    }
}
} // anonymous namespace

RLM_API realm_mongodb_collection_t* realm_mongo_collection_get(realm_user_t* user, const char* service,
                                                               const char* database, const char* collection)
{
    REALM_ASSERT(user);
    REALM_ASSERT(service);
    REALM_ASSERT(database);
    REALM_ASSERT(collection);
    return with_app_user(user, [&](auto& user) {
        auto col = user->mongo_client(service).db(database).collection(collection);
        return new realm_mongodb_collection_t(col);
    });
}

RLM_API bool realm_mongo_collection_find(realm_mongodb_collection_t* collection, realm_string_t filter_ejson,
                                         const realm_mongodb_find_options_t* options, realm_userdata_t data,
                                         realm_free_userdata_func_t delete_data, realm_mongodb_callback_t callback)
{
    REALM_ASSERT(collection);
    REALM_ASSERT(options);
    return wrap_err([&] {
        collection->find_bson(convert_to_bson<bson::BsonDocument>(filter_ejson),
                              to_mongodb_collection_find_options(options),
                              [=](util::Optional<bson::Bson> bson, util::Optional<AppError> app_error) {
                                  handle_mongodb_collection_result(bson, app_error, {data, delete_data}, callback);
                              });
        return true;
    });
}

RLM_API bool realm_mongo_collection_find_one(realm_mongodb_collection_t* collection, realm_string_t filter_ejson,
                                             const realm_mongodb_find_options_t* options, realm_userdata_t data,
                                             realm_free_userdata_func_t delete_data,
                                             realm_mongodb_callback_t callback)
{
    REALM_ASSERT(collection);
    REALM_ASSERT(options);
    return wrap_err([&] {
        collection->find_one_bson(
            convert_to_bson<bson::BsonDocument>(filter_ejson), to_mongodb_collection_find_options(options),
            [=](util::Optional<bson::Bson> bson, util::Optional<AppError> app_error) {
                handle_mongodb_collection_result(bson, app_error, {data, delete_data}, callback);
            });
        return true;
    });
}

RLM_API bool realm_mongo_collection_aggregate(realm_mongodb_collection_t* collection, realm_string_t filter_ejson,
                                              realm_userdata_t data, realm_free_userdata_func_t delete_data,
                                              realm_mongodb_callback_t callback)
{
    REALM_ASSERT(collection);
    return wrap_err([&] {
        collection->aggregate_bson(
            convert_to_bson<bson::BsonArray>(filter_ejson),
            [=](util::Optional<bson::Bson> bson, util::Optional<AppError> app_error) {
                handle_mongodb_collection_result(bson, app_error, {data, delete_data}, callback);
            });
        return true;
    });
}

RLM_API bool realm_mongo_collection_count(realm_mongodb_collection_t* collection, realm_string_t filter_ejson,
                                          int64_t limit, realm_userdata_t data,
                                          realm_free_userdata_func_t delete_data, realm_mongodb_callback_t callback)
{
    REALM_ASSERT(collection);
    return wrap_err([&] {
        collection->count_bson(convert_to_bson<bson::BsonDocument>(filter_ejson), limit,
                               [=](util::Optional<bson::Bson> bson, util::Optional<app::AppError> app_error) {
                                   handle_mongodb_collection_result(bson, app_error, {data, delete_data}, callback);
                               });
        return true;
    });
}

RLM_API bool realm_mongo_collection_insert_one(realm_mongodb_collection_t* collection, realm_string_t filter_ejson,
                                               realm_userdata_t data, realm_free_userdata_func_t delete_data,
                                               realm_mongodb_callback_t callback)
{
    REALM_ASSERT(collection);
    return wrap_err([&] {
        collection->insert_one_bson(
            convert_to_bson<bson::BsonDocument>(filter_ejson),
            [=](util::Optional<bson::Bson> bson, util::Optional<AppError> app_error) {
                handle_mongodb_collection_result(bson, app_error, {data, delete_data}, callback);
            });
        return true;
    });
}

RLM_API bool realm_mongo_collection_insert_many(realm_mongodb_collection_t* collection, realm_string_t filter_ejson,
                                                realm_userdata_t data, realm_free_userdata_func_t delete_data,
                                                realm_mongodb_callback_t callback)
{
    REALM_ASSERT(collection);
    return wrap_err([&] {
        collection->insert_many_bson(
            convert_to_bson<bson::BsonArray>(filter_ejson),
            [=](util::Optional<bson::Bson> bson, util::Optional<AppError> app_error) {
                handle_mongodb_collection_result(bson, app_error, {data, delete_data}, callback);
            });
        return true;
    });
}

RLM_API bool realm_mongo_collection_delete_one(realm_mongodb_collection_t* collection, realm_string_t filter_ejson,
                                               realm_userdata_t data, realm_free_userdata_func_t delete_data,
                                               realm_mongodb_callback_t callback)
{
    REALM_ASSERT(collection);
    return wrap_err([&] {
        collection->delete_one_bson(
            convert_to_bson<bson::BsonDocument>(filter_ejson),
            [=](util::Optional<bson::Bson> bson, util::Optional<AppError> app_error) {
                handle_mongodb_collection_result(bson, app_error, {data, delete_data}, callback);
            });
        return true;
    });
}

RLM_API bool realm_mongo_collection_delete_many(realm_mongodb_collection_t* collection, realm_string_t filter_ejson,
                                                realm_userdata_t data, realm_free_userdata_func_t delete_data,
                                                realm_mongodb_callback_t callback)
{
    REALM_ASSERT(collection);
    return wrap_err([&] {
        collection->delete_many_bson(
            convert_to_bson<bson::BsonDocument>(filter_ejson),
            [=](util::Optional<bson::Bson> bson, util::Optional<AppError> app_error) {
                handle_mongodb_collection_result(bson, app_error, {data, delete_data}, callback);
            });
        return true;
    });
}

RLM_API bool realm_mongo_collection_update_one(realm_mongodb_collection_t* collection, realm_string_t filter_ejson,
                                               realm_string_t update_ejson, bool upsert, realm_userdata_t data,
                                               realm_free_userdata_func_t delete_data,
                                               realm_mongodb_callback_t callback)
{
    REALM_ASSERT(collection);
    return wrap_err([&] {
        const auto& bson_filter = convert_to_bson<bson::BsonDocument>(filter_ejson);
        const auto& bson_update = convert_to_bson<bson::BsonDocument>(update_ejson);
        collection->update_one_bson(
            bson_filter, bson_update, upsert,
            [=](util::Optional<bson::Bson> bson, util::Optional<AppError> app_error) {
                handle_mongodb_collection_result(bson, app_error, {data, delete_data}, callback);
            });
        return true;
    });
}

RLM_API bool realm_mongo_collection_update_many(realm_mongodb_collection_t* collection, realm_string_t filter_ejson,
                                                realm_string_t update_ejson, bool upsert, realm_userdata_t data,
                                                realm_free_userdata_func_t delete_data,
                                                realm_mongodb_callback_t callback)
{
    REALM_ASSERT(collection);
    return wrap_err([&] {
        const auto& bson_filter = convert_to_bson<bson::BsonDocument>(filter_ejson);
        const auto& bson_update = convert_to_bson<bson::BsonDocument>(update_ejson);
        collection->update_many_bson(
            bson_filter, bson_update, upsert,
            [=](util::Optional<bson::Bson> bson, util::Optional<AppError> app_error) {
                handle_mongodb_collection_result(bson, app_error, {data, delete_data}, callback);
            });
        return true;
    });
}

RLM_API bool realm_mongo_collection_find_one_and_update(realm_mongodb_collection_t* collection,
                                                        realm_string_t filter_ejson, realm_string_t update_ejson,
                                                        const realm_mongodb_find_one_and_modify_options_t* options,
                                                        realm_userdata_t data, realm_free_userdata_func_t delete_data,
                                                        realm_mongodb_callback_t callback)
{
    REALM_ASSERT(collection);
    return wrap_err([&] {
        const auto& bson_filter = convert_to_bson<bson::BsonDocument>(filter_ejson);
        const auto& bson_update = convert_to_bson<bson::BsonDocument>(update_ejson);
        collection->find_one_and_update_bson(
            bson_filter, bson_update, to_mongodb_collection_find_one_and_modify_options(options),
            [=](util::Optional<bson::Bson> bson, util::Optional<AppError> app_error) {
                handle_mongodb_collection_result(bson, app_error, {data, delete_data}, callback);
            });
        return true;
    });
}

RLM_API bool realm_mongo_collection_find_one_and_replace(
    realm_mongodb_collection_t* collection, realm_string_t filter_ejson, realm_string_t replacement_ejson,
    const realm_mongodb_find_one_and_modify_options_t* options, realm_userdata_t data,
    realm_free_userdata_func_t delete_data, realm_mongodb_callback_t callback)
{
    REALM_ASSERT(collection);
    return wrap_err([&] {
        const auto& filter_bson = convert_to_bson<bson::BsonDocument>(filter_ejson);
        const auto& replacement_bson = convert_to_bson<bson::BsonDocument>(replacement_ejson);
        collection->find_one_and_replace_bson(
            filter_bson, replacement_bson, to_mongodb_collection_find_one_and_modify_options(options),
            [=](util::Optional<bson::Bson> bson, util::Optional<AppError> app_error) {
                handle_mongodb_collection_result(bson, app_error, {data, delete_data}, callback);
            });
        return true;
    });
}

RLM_API bool realm_mongo_collection_find_one_and_delete(realm_mongodb_collection_t* collection,
                                                        realm_string_t filter_ejson,
                                                        const realm_mongodb_find_one_and_modify_options_t* options,
                                                        realm_userdata_t data, realm_free_userdata_func_t delete_data,
                                                        realm_mongodb_callback_t callback)
{
    REALM_ASSERT(collection);
    return wrap_err([&] {
        const auto& bson_filter = convert_to_bson<bson::BsonDocument>(filter_ejson);
        collection->find_one_and_delete_bson(
            bson_filter, to_mongodb_collection_find_one_and_modify_options(options),
            [=](util::Optional<bson::Bson> bson, util::Optional<AppError> app_error) {
                handle_mongodb_collection_result(bson, app_error, {data, delete_data}, callback);
            });
        return true;
    });
}

#endif // REALM_APP_SERVICES

} // namespace realm::c_api

#if REALM_APP_SERVICES
// definitions outside the c_api namespace
realm_app_user_subscription_token::~realm_app_user_subscription_token()
{
    user->unsubscribe(token);
}
#endif
