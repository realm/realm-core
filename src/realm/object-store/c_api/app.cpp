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

#include <realm/object-store/c_api/types.hpp>
#include <realm/object-store/c_api/util.hpp>

namespace realm::c_api {
namespace {
using namespace realm::app;

static inline realm_auth_provider_e to_capi(AuthProvider provider)
{
    switch (provider) {
        case AuthProvider::ANONYMOUS:
            return RLM_AUTH_PROVIDER_ANONYMOUS;
        case AuthProvider::FACEBOOK:
            return RLM_AUTH_PROVIDER_FACEBOOK;
        case AuthProvider::GOOGLE:
            return RLM_AUTH_PROVIDER_GOOGLE;
        case AuthProvider::APPLE:
            return RLM_AUTH_PROVIDER_APPLE;
        case AuthProvider::CUSTOM:
            return RLM_AUTH_PROVIDER_CUSTOM;
        case AuthProvider::USERNAME_PASSWORD:
            return RLM_AUTH_PROVIDER_USERNAME_PASSWORD;
        case AuthProvider::FUNCTION:
            return RLM_AUTH_PROVIDER_FUNCTION;
        case AuthProvider::USER_API_KEY:
            return RLM_AUTH_PROVIDER_USER_API_KEY;
        case AuthProvider::SERVER_API_KEY:
            return RLM_AUTH_PROVIDER_SERVER_API_KEY;
    };
    REALM_TERMINATE("Invalid auth provider method."); // LCOV_EXCL_LINE
}

static inline AuthProvider from_capi(realm_auth_provider_e provider)
{
    switch (provider) {
        case RLM_AUTH_PROVIDER_ANONYMOUS:
            return AuthProvider::ANONYMOUS;
        case RLM_AUTH_PROVIDER_FACEBOOK:
            return AuthProvider::FACEBOOK;
        case RLM_AUTH_PROVIDER_GOOGLE:
            return AuthProvider::GOOGLE;
        case RLM_AUTH_PROVIDER_APPLE:
            return AuthProvider::APPLE;
        case RLM_AUTH_PROVIDER_CUSTOM:
            return AuthProvider::CUSTOM;
        case RLM_AUTH_PROVIDER_USERNAME_PASSWORD:
            return AuthProvider::USERNAME_PASSWORD;
        case RLM_AUTH_PROVIDER_FUNCTION:
            return AuthProvider::FUNCTION;
        case RLM_AUTH_PROVIDER_USER_API_KEY:
            return AuthProvider::USER_API_KEY;
        case RLM_AUTH_PROVIDER_SERVER_API_KEY:
            return AuthProvider::SERVER_API_KEY;
    };
    REALM_TERMINATE("Invalid auth provider method."); // LCOV_EXCL_LINE
}

static inline realm_error_t to_capi(const AppError& error)
{
    return {};
}
} // namespace
} // namespace realm::c_api

using namespace realm;
using namespace realm::app;
using namespace realm::c_api;

RLM_API realm_app_credentials_t* realm_app_credentials_new_anonymous(void)
{
    return new realm_app_credentials_t(AppCredentials::anonymous());
}

RLM_API realm_app_credentials_t* realm_app_credentials_new_facebook(const char* access_token)
{
    return new realm_app_credentials_t(AppCredentials::facebook(access_token));
}

RLM_API realm_app_credentials_t* realm_app_credentials_new_google(const char* id_token)
{
    return new realm_app_credentials_t(AppCredentials::google(AuthCode(id_token)));
}

RLM_API realm_app_credentials_t* realm_app_credentials_new_apple(const char* id_token)
{
    return new realm_app_credentials_t(AppCredentials::apple(id_token));
}

RLM_API realm_app_credentials_t* realm_app_credentials_new_custom(const char* token)
{
    return new realm_app_credentials_t(AppCredentials::custom(token));
}

RLM_API realm_app_credentials_t* realm_app_credentials_new_username_password(const char* username,
                                                                             const char* password)
{
    return new realm_app_credentials_t(AppCredentials::username_password(username, password));
}

RLM_API realm_app_credentials_t* realm_app_credentials_new_function(const char* serialized_bson_payload)
{
    return new realm_app_credentials_t(AppCredentials::function(std::string(serialized_bson_payload)));
}

RLM_API realm_app_credentials_t* realm_app_credentials_new_user_api_key(const char* api_key)
{
    return new realm_app_credentials_t(AppCredentials::user_api_key(api_key));
}

RLM_API realm_app_credentials_t* realm_app_credentials_new_server_api_key(const char* api_key)
{
    return new realm_app_credentials_t(AppCredentials::server_api_key(api_key));
}

RLM_API realm_auth_provider_e realm_auth_credentials_get_provider(realm_app_credentials_t* credentials)
{
    return to_capi(credentials->provider());
}

RLM_API realm_app_config_t* realm_app_config_new(const char* app_id,
                                                 realm_http_transport_factory_func_t http_transport_factory,
                                                 void* transport_factory_userdata,
                                                 realm_free_userdata_func_t transport_factory_userdata_free)
{
    auto* config = new realm_app_config_t;
    config->app_id = app_id;
    config->transport_generator =
        [http_transport_factory,
         userdata = SharedUserdata{transport_factory_userdata, FreeUserdata(transport_factory_userdata_free)}]() {
            realm_http_transport_t* transport = http_transport_factory(userdata.get());
            auto scope_exit = util::make_scope_exit([transport]() noexcept {
                realm_release(transport);
            });
            return std::move(transport->transport);
        };
    return config;
}

RLM_API void realm_app_config_set_base_url(realm_app_config_t* config, const char* base_url)
{
    config->base_url = std::string(base_url);
}

RLM_API void realm_app_config_set_local_app_name(realm_app_config_t* config, const char* local_app_name)
{
    config->local_app_name = std::string(local_app_name);
}

RLM_API void realm_app_config_set_local_app_version(realm_app_config_t* config, const char* local_app_version)
{
    config->local_app_version = std::string(local_app_version);
}

RLM_API void realm_app_config_set_default_request_timeout(realm_app_config_t* config, uint64_t ms)
{
    config->default_request_timeout_ms = ms;
}

RLM_API void realm_app_config_set_platform(realm_app_config_t* config, const char* platform)
{
    config->platform = std::string(platform);
}

RLM_API void realm_app_config_set_platform_version(realm_app_config_t* config, const char* platform_version)
{
    config->platform_version = std::string(platform_version);
}

RLM_API void realm_app_config_set_sdk_version(realm_app_config_t* config, const char* sdk_version)
{
    config->sdk_version = std::string(sdk_version);
}

RLM_API realm_app_t* realm_app_new(const realm_app_config_t* app_config,
                                   const realm_sync_client_config_t* sync_client_config)
{
    return new realm_app_t(App::get_shared_app(*app_config, *sync_client_config));
}

RLM_API realm_app_t* realm_app_get_cached(const char* app_id)
{
    return new realm_app_t(App::get_cached_app(app_id));
}

RLM_API const char* realm_app_get_app_id(realm_app_t* app)
{
    return (*app)->config().app_id.c_str();
}

RLM_API realm_user_t* realm_app_get_current_user(realm_app_t* app)
{
    return new realm_user_t((*app)->current_user());
}

RLM_API bool realm_app_get_all_users(realm_app_t* app, realm_user_t** out_users, size_t max, size_t* out_n)
{
    if (out_users) {
        std::vector<std::shared_ptr<SyncUser>> users = (*app)->all_users();
        size_t i = 0;
        for (; i < max; i++) {
            out_users[i] = new realm_user_t(users[i]);
        }
        if (out_n) {
            *out_n = i;
        }
    }
    else {
        if (out_n) {
            *out_n = (*app)->all_users().size();
        }
    }
    return true;
}

RLM_API bool realm_app_log_in_with_credentials(realm_app_t* app, realm_app_credentials_t* credentials,
                                               void (*callback)(void* userdata, realm_user_t*, realm_error_t*),
                                               void* userdata, realm_free_userdata_func_t userdata_free)
{
    return wrap_err([&]() {
        auto cb = [callback, userdata = SharedUserdata(userdata, FreeUserdata(userdata_free))](
                      std::shared_ptr<SyncUser> user, util::Optional<AppError> error) {
            if (error) {
                realm_error_t c_err{to_capi(*error)};
                callback(userdata.get(), nullptr, &c_err);
            }
            else {
                callback(userdata.get(), new realm_user_t(std::move(user)), nullptr);
            }
        };
        (*app)->log_in_with_credentials(*credentials, std::move(cb));
        return true;
    });
}

RLM_API bool realm_app_log_out_current_user(realm_app_t* app, void (*callback)(void* userdata, realm_error_t*),
                                            void* userdata, realm_free_userdata_func_t userdata_free)
{
    return wrap_err([&]() {
        auto cb = [callback,
                   userdata = SharedUserdata(userdata, FreeUserdata(userdata_free))](util::Optional<AppError> error) {
            if (error) {
                realm_error_t c_err{to_capi(*error)};
                callback(userdata.get(), &c_err);
            }
            else {
                callback(userdata.get(), nullptr);
            }
        };
        (*app)->log_out(std::move(cb));
        return true;
    });
}

RLM_API bool realm_app_refresh_custom_data(realm_app_t* app, realm_user_t* user,
                                           void (*callback)(void* userdata, realm_error_t*), void* userdata,
                                           realm_free_userdata_func_t userdata_free)
{
    return wrap_err([&]() {
        auto cb = [callback,
                   userdata = SharedUserdata(userdata, FreeUserdata(userdata_free))](util::Optional<AppError> error) {
            if (error) {
                realm_error_t c_err{to_capi(*error)};
                callback(userdata.get(), &c_err);
            }
            else {
                callback(userdata.get(), nullptr);
            }
        };
        (*app)->refresh_custom_data(*user, std::move(cb));
        return true;
    });
}

RLM_API bool realm_app_log_out(realm_app_t* app, realm_user_t* user, void (*callback)(void* userdata, realm_error_t*),
                               void* userdata, realm_free_userdata_func_t userdata_free)
{
    return wrap_err([&]() {
        auto cb = [callback,
                   userdata = SharedUserdata(userdata, FreeUserdata(userdata_free))](util::Optional<AppError> error) {
            if (error) {
                realm_error_t c_err{to_capi(*error)};
                callback(userdata.get(), &c_err);
            }
            else {
                callback(userdata.get(), nullptr);
            }
        };
        (*app)->log_out(*user, std::move(cb));
        return true;
    });
}

RLM_API bool realm_app_link_user(realm_app_t* app, realm_user_t* user, realm_app_credentials_t* credentials,
                                 void (*callback)(void* userdata, realm_user_t*, realm_error_t*), void* userdata,
                                 realm_free_userdata_func_t userdata_free)
{
    return wrap_err([&]() {
        auto cb = [callback, userdata = SharedUserdata(userdata, FreeUserdata(userdata_free))](
                      std::shared_ptr<SyncUser> user, util::Optional<AppError> error) {
            if (error) {
                realm_error_t c_err{to_capi(*error)};
                callback(userdata.get(), nullptr, &c_err);
            }
            else {
                callback(userdata.get(), new realm_user_t(std::move(user)), nullptr);
            }
        };
        (*app)->link_user(*user, *credentials, std::move(cb));
        return true;
    });
}

RLM_API bool realm_app_switch_user(realm_app_t* app, realm_user_t* user, realm_user_t** new_user)
{
    return wrap_err([&]() {
        auto new_user_local = (*app)->switch_user(*user);
        if (new_user) {
            *new_user = new realm_user_t(std::move(new_user_local));
        }
        return true;
    });
}

RLM_API bool realm_app_remove_user(realm_app_t* app, realm_user_t* user,
                                   void (*callback)(void* userdata, realm_error_t*), void* userdata,
                                   realm_free_userdata_func_t userdata_free)
{
    return wrap_err([&]() {
        auto cb = [callback,
                   userdata = SharedUserdata(userdata, FreeUserdata(userdata_free))](util::Optional<AppError> error) {
            if (error) {
                realm_error_t c_err{to_capi(*error)};
                callback(userdata.get(), &c_err);
            }
            else {
                callback(userdata.get(), nullptr);
            }
        };
        (*app)->remove_user(*user, std::move(cb));
        return true;
    });
}

RLM_API const char* realm_user_get_identity(const realm_user_t* user)
{
    return strdup((*user)->identity().c_str());
}

RLM_API const char* realm_user_get_local_identity(const realm_user_t* user)
{
    return strdup((*user)->local_identity().c_str());
}

RLM_API const char* realm_user_get_device_id(const realm_user_t* user)
{
    if ((*user)->has_device_id()) {
        return strdup((*user)->device_id().c_str());
    }

    return nullptr;
}

RLM_API const char* realm_user_get_refresh_token(const realm_user_t* user)
{
    return strdup((*user)->refresh_token().c_str());
}

RLM_API const char* realm_user_get_access_token(const realm_user_t* user)
{
    return strdup((*user)->access_token().c_str());
}

RLM_API realm_auth_provider_e realm_user_get_auth_provider(const realm_user_t* user)
{
    return to_capi(enum_from_provider_type((*user)->provider_type()));
}