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

#pragma once

#include "realm/object-store/property.hpp"
#include "realm/object-store/object_schema.hpp"
#include "realm/object-store/schema.hpp"
#include "realm/object-store/sync/generic_network_transport.hpp"

#include "external/json/json.hpp"

#ifdef REALM_ENABLE_AUTH_TESTS
namespace realm {
app::Response do_http_request(const app::Request& request);

class AdminAPIEndpoint {
public:
    app::Response get() const;
    app::Response patch(std::string body) const;
    app::Response post(std::string body) const;
    app::Response put(std::string body) const;
    nlohmann::json get_json() const;
    nlohmann::json patch_json(nlohmann::json body) const;
    nlohmann::json post_json(nlohmann::json body) const;
    nlohmann::json put_json(nlohmann::json body) const;

    AdminAPIEndpoint operator[](StringData name) const;

protected:
    friend class AdminAPISession;
    AdminAPIEndpoint(std::string url, std::string access_token)
        : m_url(std::move(url))
        , m_access_token(std::move(access_token))
    {
    }

    app::Response do_request(app::Request request) const;

private:
    std::string m_url;
    std::string m_access_token;
};

class AdminAPISession {
public:
    static AdminAPISession login(const std::string& base_url, const std::string& username,
                                 const std::string& password);

    AdminAPIEndpoint apps() const;
    void revoke_user_sessions(const std::string& user_id, const std::string app_id);
    void disable_user_sessions(const std::string& user_id, const std::string app_id);
    void enable_user_sessions(const std::string& user_id, const std::string app_id);
    bool verify_access_token(const std::string& access_token, const std::string app_id);

    const std::string& base_url() const noexcept
    {
        return m_base_url;
    }

private:
    AdminAPISession(std::string base_url, std::string access_token, std::string group_id)
        : m_base_url(std::move(base_url))
        , m_access_token(std::move(access_token))
        , m_group_id(std::move(group_id))
    {
    }

    std::string m_base_url;
    std::string m_access_token;
    std::string m_group_id;
};

struct AppCreateConfig {
    struct FunctionDef {
        std::string name;
        std::string source;
        bool is_private;
    };

    struct UserPassAuthConfig {
        bool auto_confirm;
        std::string confirm_email_subject;
        std::string confirmation_function_name;
        std::string email_confirmation_url;
        std::string reset_function_name;
        std::string reset_password_subject;
        std::string reset_password_url;
        bool run_confirmation_function;
        bool run_reset_function;
    };

    std::string app_name;
    std::string base_url;
    std::string admin_username;
    std::string admin_password;

    std::string mongo_uri;
    std::string mongo_dbname;

    Schema schema;
    Property partition_key;
    bool dev_mode_enabled;

    std::vector<FunctionDef> functions;

    util::Optional<UserPassAuthConfig> user_pass_auth;
    util::Optional<std::string> custom_function_auth;
    bool enable_api_key_auth = false;
    bool enable_anonymous_auth = false;
};

AppCreateConfig default_app_config(const std::string& base_url);
AppCreateConfig minimal_app_config(const std::string& base_url, const std::string& name, const Schema& schema);

struct AppSession {
    std::string client_app_id;
    std::string server_app_id;
    AdminAPISession admin_api;
    AppCreateConfig config;
};
AppSession create_app(const AppCreateConfig& config);

} // namespace realm

#endif // REALM_ENABLE_AUTH_TESTS
