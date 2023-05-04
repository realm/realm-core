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

static_assert(realm_auth_provider_e(AuthProvider::ANONYMOUS) == RLM_AUTH_PROVIDER_ANONYMOUS);
static_assert(realm_auth_provider_e(AuthProvider::ANONYMOUS_NO_REUSE) == RLM_AUTH_PROVIDER_ANONYMOUS_NO_REUSE);
static_assert(realm_auth_provider_e(AuthProvider::FACEBOOK) == RLM_AUTH_PROVIDER_FACEBOOK);
static_assert(realm_auth_provider_e(AuthProvider::GOOGLE) == RLM_AUTH_PROVIDER_GOOGLE);
static_assert(realm_auth_provider_e(AuthProvider::APPLE) == RLM_AUTH_PROVIDER_APPLE);
static_assert(realm_auth_provider_e(AuthProvider::CUSTOM) == RLM_AUTH_PROVIDER_CUSTOM);
static_assert(realm_auth_provider_e(AuthProvider::USERNAME_PASSWORD) == RLM_AUTH_PROVIDER_EMAIL_PASSWORD);
static_assert(realm_auth_provider_e(AuthProvider::FUNCTION) == RLM_AUTH_PROVIDER_FUNCTION);
static_assert(realm_auth_provider_e(AuthProvider::USER_API_KEY) == RLM_AUTH_PROVIDER_USER_API_KEY);
static_assert(realm_auth_provider_e(AuthProvider::SERVER_API_KEY) == RLM_AUTH_PROVIDER_SERVER_API_KEY);


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
               std::shared_ptr<SyncUser> user, util::Optional<AppError> error) {
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

static inline bson::BsonArray parse_ejson_array(const char* serialized)
{
    if (!serialized) {
        return {};
    }
    else {
        return bson::BsonArray(bson::parse(serialized));
    }
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

RLM_API realm_app_credentials_t* realm_app_credentials_new_user_api_key(const char* api_key) noexcept
{
    return new realm_app_credentials_t(AppCredentials::user_api_key(api_key));
}

RLM_API realm_app_credentials_t* realm_app_credentials_new_server_api_key(const char* api_key) noexcept
{
    return new realm_app_credentials_t(AppCredentials::server_api_key(api_key));
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

RLM_API void realm_app_config_set_local_app_name(realm_app_config_t* config, const char* local_app_name) noexcept
{
    config->local_app_name = std::string(local_app_name);
}

RLM_API void realm_app_config_set_local_app_version(realm_app_config_t* config,
                                                    const char* local_app_version) noexcept
{
    config->local_app_version = std::string(local_app_version);
}

RLM_API void realm_app_config_set_default_request_timeout(realm_app_config_t* config, uint64_t ms) noexcept
{
    config->default_request_timeout_ms = ms;
}

RLM_API void realm_app_config_set_platform(realm_app_config_t* config, const char* platform) noexcept
{
    config->device_info.platform = std::string(platform);
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

RLM_API void realm_app_config_set_cpu_arch(realm_app_config_t* config, const char* cpu_arch) noexcept
{
    config->device_info.cpu_arch = std::string(cpu_arch);
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

RLM_API const char* realm_app_credentials_serialize_as_json(realm_app_credentials_t* app_credentials) noexcept
{
    return wrap_err([&] {
        return duplicate_string(app_credentials->serialize_as_json());
    });
}

RLM_API realm_app_t* realm_app_create(const realm_app_config_t* app_config,
                                      const realm_sync_client_config_t* sync_client_config)
{
    return wrap_err([&] {
        return new realm_app_t(App::get_uncached_app(*app_config, *sync_client_config));
    });
}

RLM_API void realm_clear_cached_apps(void) noexcept
{
    App::clear_cached_apps();
}

RLM_API const char* realm_app_get_app_id(const realm_app_t* app) noexcept
{
    return (*app)->config().app_id.c_str();
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

RLM_API bool realm_app_refresh_custom_data(realm_app_t* app, realm_user_t* user,
                                           realm_app_void_completion_func_t callback, realm_userdata_t userdata,
                                           realm_free_userdata_func_t userdata_free)
{
    return wrap_err([&] {
        (*app)->refresh_custom_data(*user, make_callback(callback, userdata, userdata_free));
        return true;
    });
}

RLM_API bool realm_app_log_out(realm_app_t* app, realm_user_t* user, realm_app_void_completion_func_t callback,
                               realm_userdata_t userdata, realm_free_userdata_func_t userdata_free)
{
    return wrap_err([&] {
        (*app)->log_out(*user, make_callback(callback, userdata, userdata_free));
        return true;
    });
}

RLM_API bool realm_app_link_user(realm_app_t* app, realm_user_t* user, realm_app_credentials_t* credentials,
                                 realm_app_user_completion_func_t callback, realm_userdata_t userdata,
                                 realm_free_userdata_func_t userdata_free)
{
    return wrap_err([&] {
        (*app)->link_user(*user, *credentials, make_callback(callback, userdata, userdata_free));
        return true;
    });
}

RLM_API bool realm_app_switch_user(realm_app_t* app, realm_user_t* user, realm_user_t** new_user)
{
    return wrap_err([&] {
        auto new_user_local = (*app)->switch_user(*user);
        if (new_user) {
            *new_user = new realm_user_t(std::move(new_user_local));
        }
        return true;
    });
}

RLM_API bool realm_app_remove_user(realm_app_t* app, realm_user_t* user, realm_app_void_completion_func_t callback,
                                   realm_userdata_t userdata, realm_free_userdata_func_t userdata_free)
{
    return wrap_err([&] {
        (*app)->remove_user(*user, make_callback(callback, userdata, userdata_free));
        return true;
    });
}

RLM_API bool realm_app_delete_user(realm_app_t* app, realm_user_t* user, realm_app_void_completion_func_t callback,
                                   realm_userdata_t userdata, realm_free_userdata_func_t userdata_free)
{
    return wrap_err([&] {
        (*app)->delete_user(*user, make_callback(callback, userdata, userdata_free));
        return true;
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
    return wrap_err([&] {
        (*app)->provider_client<App::UserAPIKeyProviderClient>().create_api_key(
            name, *user, make_callback(callback, userdata, userdata_free));
        return true;
    });
}

RLM_API bool realm_app_user_apikey_provider_client_fetch_apikey(const realm_app_t* app, const realm_user_t* user,
                                                                realm_object_id_t id,
                                                                realm_return_apikey_func_t callback,
                                                                realm_userdata_t userdata,
                                                                realm_free_userdata_func_t userdata_free)
{
    return wrap_err([&] {
        (*app)->provider_client<App::UserAPIKeyProviderClient>().fetch_api_key(
            from_capi(id), *user, make_callback(callback, userdata, userdata_free));
        return true;
    });
}

RLM_API bool realm_app_user_apikey_provider_client_fetch_apikeys(const realm_app_t* app, const realm_user_t* user,
                                                                 realm_return_apikey_list_func_t callback,
                                                                 realm_userdata_t userdata,
                                                                 realm_free_userdata_func_t userdata_free)
{
    return wrap_err([&] {
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

        (*app)->provider_client<App::UserAPIKeyProviderClient>().fetch_api_keys(*user, std::move(cb));
        return true;
    });
}

RLM_API bool realm_app_user_apikey_provider_client_delete_apikey(const realm_app_t* app, const realm_user_t* user,
                                                                 realm_object_id_t id,
                                                                 realm_app_void_completion_func_t callback,
                                                                 realm_userdata_t userdata,
                                                                 realm_free_userdata_func_t userdata_free)
{
    return wrap_err([&] {
        (*app)->provider_client<App::UserAPIKeyProviderClient>().delete_api_key(
            from_capi(id), *user, make_callback(callback, userdata, userdata_free));
        return true;
    });
}

RLM_API bool realm_app_user_apikey_provider_client_enable_apikey(const realm_app_t* app, const realm_user_t* user,
                                                                 realm_object_id_t id,
                                                                 realm_app_void_completion_func_t callback,
                                                                 realm_userdata_t userdata,
                                                                 realm_free_userdata_func_t userdata_free)
{
    return wrap_err([&] {
        (*app)->provider_client<App::UserAPIKeyProviderClient>().enable_api_key(
            from_capi(id), *user, make_callback(callback, userdata, userdata_free));
        return true;
    });
}

RLM_API bool realm_app_user_apikey_provider_client_disable_apikey(const realm_app_t* app, const realm_user_t* user,
                                                                  realm_object_id_t id,
                                                                  realm_app_void_completion_func_t callback,
                                                                  realm_userdata_t userdata,
                                                                  realm_free_userdata_func_t userdata_free)
{
    return wrap_err([&] {
        (*app)->provider_client<App::UserAPIKeyProviderClient>().disable_api_key(
            from_capi(id), *user, make_callback(callback, userdata, userdata_free));
        return true;
    });
}

RLM_API bool realm_app_push_notification_client_register_device(
    const realm_app_t* app, const realm_user_t* user, const char* service_name, const char* registration_token,
    realm_app_void_completion_func_t callback, realm_userdata_t userdata, realm_free_userdata_func_t userdata_free)
{
    return wrap_err([&] {
        (*app)
            ->push_notification_client(service_name)
            .register_device(registration_token, *user, make_callback(callback, userdata, userdata_free));
        return true;
    });
}

RLM_API bool realm_app_push_notification_client_deregister_device(const realm_app_t* app, const realm_user_t* user,
                                                                  const char* service_name,
                                                                  realm_app_void_completion_func_t callback,
                                                                  realm_userdata_t userdata,
                                                                  realm_free_userdata_func_t userdata_free)
{
    return wrap_err([&] {
        (*app)
            ->push_notification_client(service_name)
            .deregister_device(*user, make_callback(callback, userdata, userdata_free));
        return true;
    });
}

RLM_API bool realm_app_call_function(const realm_app_t* app, const realm_user_t* user, const char* function_name,
                                     const char* serialized_ejson_payload, const char* service_name,
                                     realm_return_string_func_t callback, realm_userdata_t userdata,
                                     realm_free_userdata_func_t userdata_free)
{
    return wrap_err([&] {
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
        (*app)->call_function(*user, function_name, serialized_ejson_payload, service_name_opt, std::move(cb));
        return true;
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
    return wrap_err([&]() {
        util::Optional<std::string> filename =
            custom_filename ? util::some<std::string>(custom_filename) : util::none;
        std::string file_path = config->user->sync_manager()->path_for_realm(*config, std::move(filename));
        return duplicate_string(file_path);
    });
}

RLM_API const char* realm_user_get_identity(const realm_user_t* user) noexcept
{
    return (*user)->identity().c_str();
}

RLM_API realm_user_state_e realm_user_get_state(const realm_user_t* user) noexcept
{
    return realm_user_state_e((*user)->state());
}

RLM_API bool realm_user_get_all_identities(const realm_user_t* user, realm_user_identity_t* out_identities,
                                           size_t max, size_t* out_n)
{
    return wrap_err([&] {
        const auto& identities = (*user)->identities();
        set_out_param(out_n, identities.size());
        if (out_identities && max >= identities.size()) {
            for (size_t i = 0; i < identities.size(); i++) {
                out_identities[i] = {duplicate_string(identities[i].id),
                                     realm_auth_provider_e(enum_from_provider_type(identities[i].provider_type))};
            }
        }
        return true;
    });
}

RLM_API const char* realm_user_get_local_identity(const realm_user_t* user) noexcept
{
    return (*user)->local_identity().c_str();
}

RLM_API char* realm_user_get_device_id(const realm_user_t* user) noexcept
{
    if ((*user)->has_device_id()) {
        return duplicate_string((*user)->device_id());
    }

    return nullptr;
}

RLM_API realm_auth_provider_e realm_user_get_auth_provider(const realm_user_t* user) noexcept
{
    return realm_auth_provider_e(enum_from_provider_type((*user)->provider_type()));
}

RLM_API bool realm_user_log_out(realm_user_t* user)
{
    return wrap_err([&] {
        (*user)->log_out();
        return true;
    });
}

RLM_API bool realm_user_is_logged_in(const realm_user_t* user) noexcept
{
    return (*user)->is_logged_in();
}

RLM_API char* realm_user_get_profile_data(const realm_user_t* user)
{
    return wrap_err([&] {
        std::string data = bson::Bson((*user)->user_profile().data()).to_string();
        return duplicate_string(data);
    });
}

RLM_API char* realm_user_get_custom_data(const realm_user_t* user) noexcept
{
    if (const auto& data = (*user)->custom_data()) {
        std::string json = bson::Bson(*data).to_string();
        return duplicate_string(json);
    }

    return nullptr;
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

RLM_API realm_app_t* realm_user_get_app(const realm_user_t* user) noexcept
{
    REALM_ASSERT(user);
    try {
        if (auto shared_app = (*user)->sync_manager()->app().lock()) {
            return new realm_app_t(shared_app);
        }
    }
    catch (const std::exception&) {
    }
    return nullptr;
}

template <typename T>
inline util::Optional<T> convert_to_optional(T data)
{
    return data ? util::Optional<T>(data) : util::Optional<T>();
}

template <typename T>
inline util::Optional<T> convert_to_optional_bson(realm_string_t doc)
{
    if (doc.data == nullptr || doc.size == 0) {
        return util::Optional<T>();
    }
    return util::Optional<T>(static_cast<T>(bson::parse({doc.data, doc.size})));
}

template <typename T>
inline T convert_to_bson(realm_string_t doc)
{
    auto res = convert_to_optional_bson<T>(doc);
    return res ? *res : T();
}

static MongoCollection::FindOptions to_mongodb_collection_find_options(const realm_mongodb_find_options_t* options)
{
    MongoCollection::FindOptions mongodb_options;
    mongodb_options.projection_bson = convert_to_optional_bson<bson::BsonDocument>(options->projection_bson);
    mongodb_options.sort_bson = convert_to_optional_bson<bson::BsonDocument>(options->sort_bson);
    mongodb_options.limit = convert_to_optional(options->limit);
    return mongodb_options;
}

static MongoCollection::FindOneAndModifyOptions
to_mongodb_collection_find_one_and_modify_options(const realm_mongodb_find_one_and_modify_options_t* options)
{
    MongoCollection::FindOneAndModifyOptions mongodb_options;
    mongodb_options.projection_bson = convert_to_optional_bson<bson::BsonDocument>(options->projection_bson);
    mongodb_options.sort_bson = convert_to_optional_bson<bson::BsonDocument>(options->sort_bson);
    mongodb_options.upsert = options->upsert;
    mongodb_options.return_new_document = options->return_new_document;
    return mongodb_options;
}

static void handle_mongodb_collection_result(util::Optional<bson::Bson> bson, util::Optional<AppError> app_error,
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

RLM_API realm_mongodb_collection_t* realm_mongo_collection_get(realm_user_t* user, const char* service,
                                                               const char* database, const char* collection)
{
    REALM_ASSERT(user);
    REALM_ASSERT(service);
    REALM_ASSERT(database);
    REALM_ASSERT(collection);
    return wrap_err([&]() {
        auto col = (*user)->mongo_client(service).db(database).collection(collection);
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

} // namespace realm::c_api
