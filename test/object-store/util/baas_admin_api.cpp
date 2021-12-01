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

#include "baas_admin_api.hpp"

#if REALM_ENABLE_AUTH_TESTS

#include <iostream>
#include <mutex>

#include <catch2/catch.hpp>
#include <curl/curl.h>

#include "realm/object_id.hpp"
#include "realm/util/scope_exit.hpp"
#include "realm/util/string_buffer.hpp"

namespace realm {
namespace {
std::string property_type_to_bson_type_str(PropertyType type)
{
    switch (type & ~PropertyType::Flags) {
        case PropertyType::UUID:
            return "uuid";
        case PropertyType::Mixed:
            return "mixed";
        case PropertyType::Bool:
            return "bool";
        case PropertyType::Data:
            return "binData";
        case PropertyType::Date:
            return "date";
        case PropertyType::Decimal:
            return "decimal";
        case PropertyType::Double:
            return "double";
        case PropertyType::Float:
            return "float";
        case PropertyType::Int:
            return "long";
        case PropertyType::Object:
            return "object";
        case PropertyType::ObjectId:
            return "objectId";
        case PropertyType::String:
            return "string";
        case PropertyType::LinkingObjects:
            return "linkingObjects";
        default:
            REALM_COMPILER_HINT_UNREACHABLE();
    }
}

class BaasRuleBuilder {
public:
    using IncludePropCond = std::function<bool(const Property&)>;
    BaasRuleBuilder(const Schema& schema, const Property& partition_key, const std::string& service_name,
                    const std::string& db_name, IncludePropCond include_prop)
        : m_schema(schema)
        , m_partition_key(partition_key)
        , m_mongo_service_name(service_name)
        , m_mongo_db_name(db_name)
        , m_include_prop(std::move(include_prop))
    {
    }

    nlohmann::json property_to_jsonschema(const Property& prop);
    nlohmann::json object_schema_to_jsonschema(const ObjectSchema& obj_schema, bool clear_path = false);
    nlohmann::json object_schema_to_baas_rule(const ObjectSchema& obj_schema);

private:
    const Schema& m_schema;
    const Property& m_partition_key;
    const std::string& m_mongo_service_name;
    const std::string& m_mongo_db_name;
    std::function<bool(const Property&)> m_include_prop;
    nlohmann::json m_relationships;
    std::vector<std::string> m_current_path;
};


static const auto include_all_props = [](const Property&) -> bool {
    return true;
};

nlohmann::json BaasRuleBuilder::object_schema_to_jsonschema(const ObjectSchema& obj_schema, bool clear_path)
{
    nlohmann::json required = nlohmann::json::array();
    nlohmann::json properties;
    for (const auto& prop : obj_schema.persisted_properties) {
        if (!m_include_prop(prop)) {
            continue;
        }
        if (clear_path) {
            m_current_path.clear();
        }
        properties.emplace(prop.name, property_to_jsonschema(prop));
        if (!is_nullable(prop.type) && !is_collection(prop.type)) {
            required.push_back(prop.name);
        }
    }

    return {
        {"properties", properties},
        {"required", required},
    };
}

nlohmann::json BaasRuleBuilder::property_to_jsonschema(const Property& prop)
{
    nlohmann::json type_output;

    if ((prop.type & ~PropertyType::Flags) == PropertyType::Object) {
        auto target_obj = m_schema.find(prop.object_type);
        REALM_ASSERT(target_obj != m_schema.end());

        if (target_obj->is_embedded) {
            m_current_path.push_back(prop.name);
            if (is_collection(prop.type)) {
                m_current_path.push_back("[]");
            }

            type_output = object_schema_to_jsonschema(*target_obj);
            type_output.emplace("bsonType", "object");
        }
        else {
            REALM_ASSERT(target_obj->primary_key_property());
            util::StringBuffer rel_name_buf;
            for (const auto& path_elem : m_current_path) {
                rel_name_buf.append(path_elem);
                rel_name_buf.append(".", 1);
            }
            rel_name_buf.append(prop.name);
            m_relationships[rel_name_buf.c_str()] = {
                {"ref",
                 util::format("#/relationship/%1/%2/%3", m_mongo_service_name, m_mongo_db_name, target_obj->name)},
                {"foreign_key", target_obj->primary_key_property()->name},
                {"is_list", is_collection(prop.type)},
            };
            type_output.emplace("bsonType", property_type_to_bson_type_str(target_obj->primary_key_property()->type));
        }
    }
    else {
        type_output = {{"bsonType", property_type_to_bson_type_str(prop.type)}};
    }

    if (is_array(prop.type)) {
        return nlohmann::json{{"bsonType", "array"}, {"items", type_output}};
    }
    if (is_set(prop.type)) {
        return nlohmann::json{{"bsonType", "array"}, {"uniqueItems", true}, {"items", type_output}};
    }
    if (is_dictionary(prop.type)) {
        return nlohmann::json{
            {"bsonType", "object"}, {"properties", nlohmann::json::object()}, {"additionalProperties", type_output}};
    }

    // At this point we should have handled all the collection types and it's safe to return the prop_obj,
    REALM_ASSERT(!is_collection(prop.type));
    return type_output;
}

nlohmann::json BaasRuleBuilder::object_schema_to_baas_rule(const ObjectSchema& obj_schema)
{
    auto schema_json = object_schema_to_jsonschema(obj_schema, true);
    schema_json.emplace("title", obj_schema.name);
    auto& prop_sub_obj = schema_json["properties"];
    if (m_include_prop(m_partition_key) && prop_sub_obj.find(m_partition_key.name) == prop_sub_obj.end()) {
        prop_sub_obj.emplace(m_partition_key.name, property_to_jsonschema(m_partition_key));
        if (!is_nullable(m_partition_key.type)) {
            schema_json["required"].push_back(m_partition_key.name);
        }
    }
    std::string test = schema_json.dump();
    return {
        {"database", m_mongo_db_name},
        {"collection", obj_schema.name},
        {"relationships", m_relationships},
        {"schema", schema_json},
        {"roles", nlohmann::json::array({{{"name", "default"},
                                          {"apply_when", nlohmann::json::object()},
                                          {"insert", true},
                                          {"delete", true},
                                          {"additional_fields", nlohmann::json::object()}}})},
    };
}

class CurlGlobalGuard {
public:
    CurlGlobalGuard()
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        if (++m_users == 1) {
            curl_global_init(CURL_GLOBAL_ALL);
        }
    }

    ~CurlGlobalGuard()
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        if (--m_users == 0) {
            curl_global_cleanup();
        }
    }

    CurlGlobalGuard(const CurlGlobalGuard&) = delete;
    CurlGlobalGuard(CurlGlobalGuard&&) = delete;
    CurlGlobalGuard& operator=(const CurlGlobalGuard&) = delete;
    CurlGlobalGuard& operator=(CurlGlobalGuard&&) = delete;

private:
    static std::mutex m_mutex;
    static int m_users;
};

std::mutex CurlGlobalGuard::m_mutex = {};
int CurlGlobalGuard::m_users = 0;

size_t curl_write_cb(char* ptr, size_t size, size_t nmemb, std::string* response)
{
    REALM_ASSERT(response);
    size_t realsize = size * nmemb;
    response->append(ptr, realsize);
    return realsize;
}

size_t curl_header_cb(char* buffer, size_t size, size_t nitems, std::map<std::string, std::string>* response_headers)
{
    REALM_ASSERT(response_headers);
    std::string combined(buffer, size * nitems);
    if (auto pos = combined.find(':'); pos != std::string::npos) {
        std::string key = combined.substr(0, pos);
        std::string value = combined.substr(pos + 1);
        while (value.size() > 0 && value[0] == ' ') {
            value = value.substr(1);
        }
        while (value.size() > 0 && (value[value.size() - 1] == '\r' || value[value.size() - 1] == '\n')) {
            value = value.substr(0, value.size() - 1);
        }
        response_headers->insert({key, value});
    }
    else {
        if (combined.size() > 5 && combined.substr(0, 5) != "HTTP/") { // ignore for now HTTP/1.1 ...
            std::cerr << "test transport skipping header: " << combined << std::endl;
        }
    }
    return nitems * size;
}

} // namespace

app::Response do_http_request(const app::Request& request)
{
    CurlGlobalGuard curl_global_guard;
    auto curl = curl_easy_init();
    if (!curl) {
        return app::Response{500, -1};
    }

    struct curl_slist* list = nullptr;
    auto curl_cleanup = util::ScopeExit([&]() noexcept {
        curl_easy_cleanup(curl);
        curl_slist_free_all(list);
    });

    std::string response;
    std::map<std::string, std::string> response_headers;

    /* First set the URL that is about to receive our POST. This URL can
     just as well be a https:// URL if that is what should receive the
     data. */
    curl_easy_setopt(curl, CURLOPT_URL, request.url.c_str());

    /* Now specify the POST data */
    if (request.method == app::HttpMethod::post) {
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request.body.c_str());
    }
    else if (request.method == app::HttpMethod::put) {
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request.body.c_str());
    }
    else if (request.method == app::HttpMethod::patch) {
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PATCH");
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request.body.c_str());
    }
    else if (request.method == app::HttpMethod::del) {
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request.body.c_str());
    }
    else if (request.method == app::HttpMethod::patch) {
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PATCH");
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request.body.c_str());
    }

    curl_easy_setopt(curl, CURLOPT_TIMEOUT, request.timeout_ms);

    for (auto header : request.headers) {
        auto header_str = util::format("%1: %2", header.first, header.second);
        list = curl_slist_append(list, header_str.c_str());
    }

    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, list);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, curl_header_cb);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &response_headers);

    auto response_code = curl_easy_perform(curl);
    if (response_code != CURLE_OK) {
        fprintf(stderr, "curl_easy_perform() failed when sending request to '%s' with body '%s': %s\n",
                request.url.c_str(), request.body.c_str(), curl_easy_strerror(response_code));
    }
    int http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    return {
        http_code,
        0, // binding_response_code
        std::move(response_headers),
        std::move(response),
    };
}

AdminAPIEndpoint AdminAPIEndpoint::operator[](StringData name) const
{
    return AdminAPIEndpoint(util::format("%1/%2", m_url, name), m_access_token);
}

app::Response AdminAPIEndpoint::do_request(app::Request request) const
{
    request.url = util::format("%1?bypass_service_change=SyncProtocolVersionIncrease", request.url);
    request.headers["Content-Type"] = "application/json;charset=utf-8";
    request.headers["Accept"] = "application/json";
    request.headers["Authorization"] = util::format("Bearer %1", m_access_token);
    auto resp = do_http_request(request);
    return resp;
}

app::Response AdminAPIEndpoint::get() const
{
    app::Request req;
    req.method = app::HttpMethod::get;
    req.url = m_url;
    return do_request(std::move(req));
}

app::Response AdminAPIEndpoint::del() const
{
    app::Request req;
    req.method = app::HttpMethod::del;
    req.url = m_url;
    return do_request(std::move(req));
}

nlohmann::json AdminAPIEndpoint::get_json() const
{
    auto resp = get();
    REALM_ASSERT(resp.http_status_code >= 200 && resp.http_status_code < 300);
    return nlohmann::json::parse(resp.body.empty() ? "{}" : resp.body);
}

app::Response AdminAPIEndpoint::post(std::string body) const
{
    app::Request req;
    req.method = app::HttpMethod::post;
    req.url = m_url;
    req.body = std::move(body);
    return do_request(std::move(req));
}

nlohmann::json AdminAPIEndpoint::post_json(nlohmann::json body) const
{
    auto resp = post(body.dump());
    REALM_ASSERT(resp.http_status_code >= 200 && resp.http_status_code < 300);
    return nlohmann::json::parse(resp.body.empty() ? "{}" : resp.body);
}

app::Response AdminAPIEndpoint::put(std::string body) const
{
    app::Request req;
    req.method = app::HttpMethod::put;
    req.url = m_url;
    req.body = std::move(body);
    return do_request(std::move(req));
}

nlohmann::json AdminAPIEndpoint::put_json(nlohmann::json body) const
{
    auto resp = put(body.dump());
    REALM_ASSERT(resp.http_status_code >= 200 && resp.http_status_code < 300);
    return nlohmann::json::parse(resp.body.empty() ? "{}" : resp.body);
}

app::Response AdminAPIEndpoint::patch(std::string body) const
{
    app::Request req;
    req.method = app::HttpMethod::patch;
    req.url = m_url;
    req.body = std::move(body);
    return do_request(std::move(req));
}

nlohmann::json AdminAPIEndpoint::patch_json(nlohmann::json body) const
{
    auto resp = patch(body.dump());
    REALM_ASSERT(resp.http_status_code >= 200 && resp.http_status_code < 300);
    return nlohmann::json::parse(resp.body.empty() ? "{}" : resp.body);
}

AdminAPISession AdminAPISession::login(const std::string& base_url, const std::string& username,
                                       const std::string& password)
{
    nlohmann::json login_req_body{
        {"provider", "userpass"},
        {"username", username},
        {"password", password},
    };
    app::Request auth_req{
        app::HttpMethod::post,
        util::format("%1/api/admin/v3.0/auth/providers/local-userpass/login", base_url),
        60000, // 1 minute timeout
        {
            {"Content-Type", "application/json;charset=utf-8"},
            {"Accept", "application/json"},
        },
        login_req_body.dump(),
    };
    auto login_resp = do_http_request(auth_req);
    REALM_ASSERT(login_resp.http_status_code == 200);
    auto login_resp_body = nlohmann::json::parse(login_resp.body);

    std::string access_token = login_resp_body["access_token"];

    AdminAPIEndpoint user_profile(util::format("%1/api/admin/v3.0/auth/profile", base_url), access_token);
    auto profile_resp = user_profile.get_json();

    std::string group_id = profile_resp["roles"][0]["group_id"];

    return AdminAPISession(std::move(base_url), std::move(access_token), std::move(group_id));
}

void AdminAPISession::revoke_user_sessions(const std::string& user_id, const std::string& app_id)
{
    auto endpoint = apps()[app_id]["users"][user_id]["logout"];
    auto response = endpoint.put("");
    REALM_ASSERT(response.http_status_code == 204);
}

void AdminAPISession::disable_user_sessions(const std::string& user_id, const std::string& app_id)
{
    auto endpoint = apps()[app_id]["users"][user_id]["disable"];
    auto response = endpoint.put("");
    REALM_ASSERT(response.http_status_code == 204);
}

void AdminAPISession::enable_user_sessions(const std::string& user_id, const std::string& app_id)
{
    auto endpoint = apps()[app_id]["users"][user_id]["enable"];
    auto response = endpoint.put("");
    REALM_ASSERT(response.http_status_code == 204);
}

// returns false for an invalid/expired access token
bool AdminAPISession::verify_access_token(const std::string& access_token, const std::string& app_id)
{
    auto endpoint = apps()[app_id]["users"]["verify_token"];
    nlohmann::json request_body{
        {"token", access_token},
    };
    auto response = endpoint.post(request_body.dump());
    if (response.http_status_code == 200) {
        auto resp_json = nlohmann::json::parse(response.body.empty() ? "{}" : response.body);
        try {
            // if these fields are found, then the token is valid according to the server.
            // if it is invalid or expired then an error response is sent.
            int64_t issued_at = resp_json["iat"];
            int64_t expires_at = resp_json["exp"];
            return issued_at != 0 && expires_at != 0;
        }
        catch (...) {
            return false;
        }
    }
    return false;
}

void AdminAPISession::set_development_mode_to(const std::string& app_id, bool enable)
{
    auto endpoint = apps()[app_id]["sync"]["config"];
    endpoint.put_json({{"development_mode_enabled", enable}});
}

void AdminAPISession::delete_app(const std::string& app_id)
{
    auto app_endpoint = apps()[app_id];
    auto resp = app_endpoint.del();
    REALM_ASSERT(resp.http_status_code == 204);
}

std::vector<AdminAPISession::Service> AdminAPISession::get_services(const std::string& app_id)
{
    auto endpoint = apps()[app_id]["services"];
    auto response = endpoint.get_json();
    std::vector<AdminAPISession::Service> services;
    for (auto service : response) {
        services.push_back(
            {service["_id"], service["name"], service["type"], service["version"], service["last_modified"]});
    }
    return services;
}


AdminAPISession::Service AdminAPISession::get_sync_service(const std::string& app_id)
{
    auto services = get_services(app_id);
    auto sync_service = std::find_if(services.begin(), services.end(), [&](auto s) {
        return s.type == "mongodb";
    });
    REALM_ASSERT(sync_service != services.end());
    return *sync_service;
}

nlohmann::json convert_config(AdminAPISession::ServiceConfig config)
{
    return nlohmann::json{
        {"database_name", config.database_name}, {"partition", config.partition}, {"state", config.state}};
}

AdminAPIEndpoint AdminAPISession::service_config_endpoint(const std::string& app_id, const std::string& service_id)
{
    return apps()[app_id]["services"][service_id]["config"];
}

AdminAPISession::ServiceConfig AdminAPISession::disable_sync(const std::string& app_id, const std::string& service_id,
                                                             AdminAPISession::ServiceConfig sync_config)
{
    auto endpoint = service_config_endpoint(app_id, service_id);
    if (sync_config.state != "") {
        sync_config.state = "";
        endpoint.patch_json({{"sync", convert_config(sync_config)}});
    }
    return sync_config;
}

AdminAPISession::ServiceConfig AdminAPISession::pause_sync(const std::string& app_id, const std::string& service_id,
                                                           AdminAPISession::ServiceConfig sync_config)
{
    auto endpoint = service_config_endpoint(app_id, service_id);
    if (sync_config.state != "disabled") {
        sync_config.state = "disabled";
        endpoint.patch_json({{"sync", convert_config(sync_config)}});
    }
    return sync_config;
}

AdminAPISession::ServiceConfig AdminAPISession::enable_sync(const std::string& app_id, const std::string& service_id,
                                                            AdminAPISession::ServiceConfig sync_config)
{
    auto endpoint = service_config_endpoint(app_id, service_id);
    sync_config.state = "enabled";
    endpoint.patch_json({{"sync", convert_config(sync_config)}});
    return sync_config;
}

AdminAPISession::ServiceConfig AdminAPISession::get_config(const std::string& app_id,
                                                           const AdminAPISession::Service& service)
{
    auto endpoint = service_config_endpoint(app_id, service.id);
    auto response = endpoint.get_json();
    AdminAPISession::ServiceConfig config;
    try {
        auto sync = response["sync"];
        config.state = sync["state"];
        config.database_name = sync["database_name"];
        config.partition = sync["partition"];
    }
    catch (const std::exception&) {
        // ignored - the config for a disabled sync service will be empty
    }
    return config;
}

bool AdminAPISession::is_sync_enabled(const std::string& app_id)
{
    auto sync_service = get_sync_service(app_id);
    auto config = get_config(app_id, sync_service);
    return config.state == "enabled";
}

AdminAPIEndpoint AdminAPISession::apps() const
{
    return AdminAPIEndpoint(util::format("%1/api/admin/v3.0/groups/%2/apps", m_base_url, m_group_id), m_access_token);
}

AppCreateConfig default_app_config(const std::string& base_url)
{
    ObjectId id = ObjectId::gen();
    std::string db_name = util::format("test_data_%1", id.to_string());

    std::string update_user_data_func = util::format(R"(
        exports = async function(data) {
            const user = context.user;
            const mongodb = context.services.get("BackingDB");
            const userDataCollection = mongodb.db("%1").collection("UserData");
            await userDataCollection.updateOne(
                                               { "user_id": user.id },
                                               { "$set": data },
                                               { "upsert": true }
                                               );
            return true;
        };
    )",
                                                     db_name);

    constexpr const char* sum_func = R"(
        exports = function(...args) {
            return args.reduce((a,b) => a + b, 0);
        };
    )";

    constexpr const char* confirm_func = R"(
        exports = ({ token, tokenId, username }) => {
            // process the confirm token, tokenId and username
            if (username.includes("realm_tests_do_autoverify")) {
              return { status: 'success' }
            }
            // do not confirm the user
            return { status: 'fail' };
        };
    )";

    constexpr const char* auth_func = R"(
        exports = (loginPayload) => {
            return loginPayload["realmCustomAuthFuncUserId"];
        };
    )";

    constexpr const char* reset_func = R"(
        exports = ({ token, tokenId, username, password }) => {
            // process the reset token, tokenId, username and password
            if (password.includes("realm_tests_do_reset")) {
              return { status: 'success' };
            }
            // will not reset the password
            return { status: 'fail' };
        };
    )";

    std::vector<AppCreateConfig::FunctionDef> funcs = {
        {"updateUserData", update_user_data_func, false},
        {"sumFunc", sum_func, false},
        {"confirmFunc", confirm_func, false},
        {"authFunc", auth_func, false},
        {"resetFunc", reset_func, false},
    };

    const auto dog_schema =
        ObjectSchema("Dog", {realm::Property("_id", PropertyType::ObjectId | PropertyType::Nullable, true),
                             realm::Property("breed", PropertyType::String | PropertyType::Nullable),
                             realm::Property("name", PropertyType::String),
                             realm::Property("realm_id", PropertyType::String | PropertyType::Nullable)});
    const auto person_schema =
        ObjectSchema("Person", {realm::Property("_id", PropertyType::ObjectId | PropertyType::Nullable, true),
                                realm::Property("age", PropertyType::Int),
                                realm::Property("dogs", PropertyType::Object | PropertyType::Array, "Dog"),
                                realm::Property("firstName", PropertyType::String),
                                realm::Property("lastName", PropertyType::String),
                                realm::Property("realm_id", PropertyType::String | PropertyType::Nullable)});
    realm::Schema default_schema({dog_schema, person_schema});

    Property partition_key("realm_id", PropertyType::String | PropertyType::Nullable);

    AppCreateConfig::UserPassAuthConfig user_pass_config{
        false,
        "",
        "confirmFunc",
        "http://localhost/confirmEmail",
        "resetFunc",
        "",
        "http://localhost/resetPassword",
        true,
        true,
    };

    return AppCreateConfig{"test",
                           base_url,
                           "unique_user@domain.com",
                           "password",
                           "mongodb://localhost:26000",
                           db_name,
                           std::move(default_schema),
                           std::move(partition_key),
                           true,
                           util::none, // Default to no FLX sync config
                           std::move(funcs),
                           std::move(user_pass_config),
                           std::string{"authFunc"},
                           true,
                           true,
                           true};
}

AppCreateConfig minimal_app_config(const std::string& base_url, const std::string& name, const Schema& schema)
{
    Property partition_key("partition", PropertyType::String | PropertyType::Nullable);

    AppCreateConfig::UserPassAuthConfig user_pass_config{
        true,  "Confirm", "", "http://example.com/confirmEmail", "", "Reset", "http://exmaple.com/resetPassword",
        false, false,
    };

    ObjectId id = ObjectId::gen();
    return AppCreateConfig{
        name,
        base_url,
        "unique_user@domain.com",
        "password",
        "mongodb://localhost:26000",
        util::format("test_data_%1_%2", name, id.to_string()),
        schema,
        std::move(partition_key),
        true,                        // dev_mode_enabled
        util::none,                  // no FLX sync config
        {},                          // no functions
        std::move(user_pass_config), // enable basic user/pass auth
        util::none,                  // disable custom auth
        true,                        // enable api key auth
        true,                        // enable anonymous auth
    };
}

AppSession create_app(const AppCreateConfig& config)
{
    auto session = AdminAPISession::login(config.base_url, config.admin_username, config.admin_password);
    auto create_app_resp = session.apps().post_json(nlohmann::json{{"name", config.app_name}});
    std::string app_id = create_app_resp["_id"];
    std::string client_app_id = create_app_resp["client_app_id"];

    auto app = session.apps()[app_id];

    auto functions = app["functions"];
    std::unordered_map<std::string, std::string> function_name_to_id;
    for (const auto& func : config.functions) {
        auto create_func_resp = functions.post_json({
            {"name", func.name},
            {"private", func.is_private},
            {"can_evaluate", nlohmann::json::object()},
            {"source", func.source},
        });
        function_name_to_id.insert({func.name, create_func_resp["_id"]});
    }

    auto auth_providers = app["auth_providers"];
    if (config.enable_anonymous_auth) {
        auth_providers.post_json({{"type", "anon-user"}});
    }
    if (config.user_pass_auth) {
        auto user_pass_config_obj = nlohmann::json{
            {"autoConfirm", config.user_pass_auth->auto_confirm},
            {"confirmEmailSubject", config.user_pass_auth->confirm_email_subject},
            {"emailConfirmationUrl", config.user_pass_auth->email_confirmation_url},
            {"resetPasswordSubject", config.user_pass_auth->reset_password_subject},
            {"resetPasswordUrl", config.user_pass_auth->reset_password_url},
        };
        if (!config.user_pass_auth->confirmation_function_name.empty()) {
            const auto& confirm_func_name = config.user_pass_auth->confirmation_function_name;
            user_pass_config_obj.emplace("confirmationFunctionName", confirm_func_name);
            user_pass_config_obj.emplace("confirmationFunctionId", function_name_to_id[confirm_func_name]);
            user_pass_config_obj.emplace("runConfirmationFunction", config.user_pass_auth->run_confirmation_function);
        }
        if (!config.user_pass_auth->reset_function_name.empty()) {
            const auto& reset_func_name = config.user_pass_auth->reset_function_name;
            user_pass_config_obj.emplace("resetFunctionName", reset_func_name);
            user_pass_config_obj.emplace("resetFunctionId", function_name_to_id[reset_func_name]);
            user_pass_config_obj.emplace("runResetFunction", config.user_pass_auth->run_reset_function);
        }
        auth_providers.post_json({{"type", "local-userpass"}, {"config", std::move(user_pass_config_obj)}});
    }
    if (config.custom_function_auth) {
        auth_providers.post_json({{"type", "custom-function"},
                                  {"config",
                                   {
                                       {"authFunctionName", *config.custom_function_auth},
                                       {"authFunctionId", function_name_to_id[*config.custom_function_auth]},
                                   }}});
    }

    if (config.enable_api_key_auth) {
        auto all_auth_providers = auth_providers.get_json();
        auto api_key_provider =
            std::find_if(all_auth_providers.begin(), all_auth_providers.end(), [](const nlohmann::json& provider) {
                return provider["type"] == "api-key";
            });
        REALM_ASSERT(api_key_provider != all_auth_providers.end());
        std::string api_key_provider_id = (*api_key_provider)["_id"];
        auto api_key_enable_resp = auth_providers[api_key_provider_id]["enable"].put("");
        REALM_ASSERT(api_key_enable_resp.http_status_code >= 200 && api_key_enable_resp.http_status_code < 300);
    }

    auto secrets = app["secrets"];
    secrets.post_json({{"name", "BackingDB_uri"}, {"value", config.mongo_uri}});
    secrets.post_json({{"name", "gcm"}, {"value", "gcm"}});
    secrets.post_json({{"name", "customTokenKey"}, {"value", "My_very_confidential_secretttttt"}});

    if (config.enable_custom_token_auth) {
        auth_providers.post_json(
            {{"type", "custom-token"},
             {"config",
              {
                  {"audience", nlohmann::json::array()},
                  {"signingAlgorithm", "HS256"},
                  {"useJWKURI", false},
              }},
             {"secret_config", {{"signingKeys", nlohmann::json::array({"customTokenKey"})}}},
             {"disabled", false},
             {"metadata_fields",
              {{{"required", false}, {"name", "user_data.name"}, {"field_name", "name"}},
               {{"required", true}, {"name", "user_data.occupation"}, {"field_name", "occupation"}},
               {{"required", true}, {"name", "my_metadata.name"}, {"field_name", "anotherName"}}}}});
    }

    auto services = app["services"];
    static const std::string mongo_service_name = "BackingDB";

    nlohmann::json mongo_service_def = {
        {"name", mongo_service_name},
        {"type", "mongodb"},
        {"config", {{"uri", config.mongo_uri}}},
    };
    if (config.flx_sync_config) {
        auto queryable_fields = nlohmann::json::object();
        for (const auto& kv : config.flx_sync_config->queryable_fields) {
            auto table = nlohmann::json::object();
            for (const auto& field : kv.second) {
                table[field] = {{"name", field}};
            }
            queryable_fields[kv.first] = table;
        }
        mongo_service_def["config"]["sync_query"] = nlohmann::json{
            {"state", "enabled"},
            {"database_name", config.mongo_dbname},
            {"queryable_fields", std::move(queryable_fields)},
            {"permissions",
             {{"rules", nlohmann::json::object()},
              {"defaultRoles",
               nlohmann::json::array(
                   {{{"name", "all"}, {"applyWhen", nlohmann::json::object()}, {"read", true}, {"write", true}}})}}}};
    }
    else {
        mongo_service_def["config"]["sync"] = nlohmann::json{
            {"state", "enabled"},
            {"database_name", config.mongo_dbname},
            {"partition",
             {
                 {"key", config.partition_key.name},
                 {"type", property_type_to_bson_type_str(config.partition_key.type)},
                 {"required", !is_nullable(config.partition_key.type)},
                 {"permissions",
                  {
                      {"read", true},
                      {"write", true},
                  }},
             }},
        };
    }

    auto create_mongo_service_resp = services.post_json(std::move(mongo_service_def));
    std::string mongo_service_id = create_mongo_service_resp["_id"];
    auto rules = services[mongo_service_id]["rules"];

    std::unordered_map<std::string, std::string> obj_schema_name_to_id;
    for (const auto& obj_schema : config.schema) {
        if (auto pk_prop = obj_schema.primary_key_property(); pk_prop == nullptr || pk_prop->name != "_id") {
            continue;
        }

        BaasRuleBuilder rule_builder(config.schema, config.partition_key, mongo_service_name, config.mongo_dbname,
                                     [&](const Property& prop) {
                                         return prop.name == "_id" || prop.name == config.partition_key.name;
                                     });
        auto schema_to_create = rule_builder.object_schema_to_baas_rule(obj_schema);

        auto rule_create_resp = rules.post_json(schema_to_create);
        obj_schema_name_to_id.insert({obj_schema.name, rule_create_resp["_id"]});
    }

    for (const auto& obj_schema : config.schema) {
        if (auto pk_prop = obj_schema.primary_key_property(); pk_prop == nullptr || pk_prop->name != "_id") {
            continue;
        }

        auto rule_id = obj_schema_name_to_id.find(obj_schema.name);
        REALM_ASSERT(rule_id != obj_schema_name_to_id.end());
        BaasRuleBuilder rule_builder(config.schema, config.partition_key, mongo_service_name, config.mongo_dbname,
                                     include_all_props);
        auto schema_to_create = rule_builder.object_schema_to_baas_rule(obj_schema);
        schema_to_create["_id"] = rule_id->second;

        rules[rule_id->second].put_json(schema_to_create);
    }

    app["sync"]["config"].put_json({{"development_mode_enabled", config.dev_mode_enabled}});

    rules.post_json({
        {"database", config.mongo_dbname},
        {"collection", "UserData"},
        {"roles",
         {{
             {"name", "default"},
             {"apply_when", nlohmann::json::object()},
             {"insert", true},
             {"delete", true},
             {"additional_fields", nlohmann::json::object()},
         }}},
        {"schema", nlohmann::json::object()},
        {"relationships", nlohmann::json::object()},
    });

    app["custom_user_data"].patch_json({
        {"mongo_service_id", mongo_service_id},
        {"enabled", true},
        {"database_name", config.mongo_dbname},
        {"collection_name", "UserData"},
        {"user_id_field", "user_id"},
    });

    services.post_json({
        {"name", "gcm"},
        {"type", "gcm"},
        {"config",
         {
             {"senderId", "gcm"},
         }},
        {"secret_config",
         {
             {"apiKey", "gcm"},
         }},
        {"version", 1},
    });

    return {client_app_id, app_id, session, config};
}

app::App::Config get_integration_config()
{
    std::string base_url = get_base_url();
    REQUIRE(!base_url.empty());
    auto app_session = get_runtime_app_session(base_url);
    return get_config(instance_of<SynchronousTestTransport>, app_session);
}

AppSession get_runtime_app_session(std::string base_url)
{
    static const AppSession cached_app_session = [&] {
        auto cached_app_session = create_app(default_app_config(base_url));
        return cached_app_session;
    }();
    return cached_app_session;
}


#ifdef REALM_MONGODB_ENDPOINT
TEST_CASE("app: baas admin api", "[sync][app]") {
    std::string base_url = REALM_QUOTE(REALM_MONGODB_ENDPOINT);
    base_url.erase(std::remove(base_url.begin(), base_url.end(), '"'), base_url.end());
    SECTION("embedded objects") {
        Schema schema{{"top",
                       {{"_id", PropertyType::String, true},
                        {"location", PropertyType::Object | PropertyType::Nullable, "location"}}},
                      {"location",
                       ObjectSchema::IsEmbedded{true},
                       {{"coordinates", PropertyType::Double | PropertyType::Array}}}};

        auto test_app_config = minimal_app_config(base_url, "test", schema);
        auto app_id = create_app(test_app_config);
    }

    SECTION("embedded object with array") {
        Schema schema{
            {"a",
             {{"_id", PropertyType::String, true},
              {"b_link", PropertyType::Object | PropertyType::Array | PropertyType::Nullable, "b"}}},
            {"b", ObjectSchema::IsEmbedded{true}, {{"c_link", PropertyType::Object | PropertyType::Nullable, "c"}}},
            {"c", {{"_id", PropertyType::String, true}, {"d_str", PropertyType::String}}},
        };
        auto test_app_config = minimal_app_config(base_url, "test", schema);
        auto app_id = create_app(test_app_config);
    }

    SECTION("dictionaries") {
        Schema schema{
            {"a", {{"_id", PropertyType::String, true}, {"b_dict", PropertyType::Dictionary | PropertyType::String}}},
        };

        auto test_app_config = minimal_app_config(base_url, "test", schema);
        auto app_id = create_app(test_app_config);
    }

    SECTION("set") {
        Schema schema{
            {"a", {{"_id", PropertyType::String, true}, {"b_dict", PropertyType::Set | PropertyType::String}}},
        };

        auto test_app_config = minimal_app_config(base_url, "test", schema);
        auto app_id = create_app(test_app_config);
    }
}
#endif

} // namespace realm

#endif
