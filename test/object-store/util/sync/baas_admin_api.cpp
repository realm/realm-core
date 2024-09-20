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

#include <util/sync/baas_admin_api.hpp>
#include <util/sync/redirect_server.hpp>

#include <realm/object-store/sync/app_credentials.hpp>

#include <external/mpark/variant.hpp>

#if REALM_ENABLE_AUTH_TESTS

#include <realm/exceptions.hpp>
#include <realm/object_id.hpp>

#include <realm/util/scope_exit.hpp>

#include <catch2/catch_all.hpp>
#include <curl/curl.h>

#include <iostream>
#include <mutex>

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
    using IncludePropCond = util::UniqueFunction<bool(const Property&)>;
    BaasRuleBuilder(const Schema& schema, const Property& partition_key, const std::string& service_name,
                    const std::string& db_name, bool is_flx_sync)
        : m_schema(schema)
        , m_partition_key(partition_key)
        , m_mongo_service_name(service_name)
        , m_mongo_db_name(db_name)
        , m_is_flx_sync(is_flx_sync)
    {
    }

    nlohmann::json property_to_jsonschema(const Property& prop);
    nlohmann::json object_schema_to_jsonschema(const ObjectSchema& obj_schema, const IncludePropCond& include_prop,
                                               bool clear_path = false);
    nlohmann::json object_schema_to_baas_schema(const ObjectSchema& obj_schema, IncludePropCond include_prop);

private:
    const Schema& m_schema;
    const Property& m_partition_key;
    const std::string& m_mongo_service_name;
    const std::string& m_mongo_db_name;
    const bool m_is_flx_sync;
    nlohmann::json m_relationships;
    std::vector<std::string> m_current_path;
};

nlohmann::json BaasRuleBuilder::object_schema_to_jsonschema(const ObjectSchema& obj_schema,
                                                            const IncludePropCond& include_prop, bool clear_path)
{
    nlohmann::json required = nlohmann::json::array();
    nlohmann::json properties = nlohmann::json::object();
    for (const auto& prop : obj_schema.persisted_properties) {
        if (include_prop && !include_prop(prop)) {
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
        {"title", obj_schema.name},
    };
}

nlohmann::json BaasRuleBuilder::property_to_jsonschema(const Property& prop)
{
    nlohmann::json type_output;

    if ((prop.type & ~PropertyType::Flags) == PropertyType::Object) {
        auto target_obj = m_schema.find(prop.object_type);
        REALM_ASSERT(target_obj != m_schema.end());

        if (target_obj->table_type == ObjectSchema::ObjectType::Embedded) {
            m_current_path.push_back(prop.name);
            if (is_collection(prop.type)) {
                m_current_path.push_back("[]");
            }

            // embedded objects are normally not allowed to be queryable,
            // except if it is a GeoJSON type, and in that case the server
            // needs to know if it conforms to the expected schema shape.
            IncludePropCond always = [](const Property&) -> bool {
                return true;
            };
            type_output = object_schema_to_jsonschema(*target_obj, always);
            type_output.emplace("bsonType", "object");
        }
        else {
            REALM_ASSERT(target_obj->primary_key_property());
            std::string rel_name;
            for (const auto& path_elem : m_current_path) {
                rel_name.append(path_elem);
                rel_name.append(".");
            }
            rel_name.append(prop.name);
            m_relationships[rel_name] = {
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

nlohmann::json BaasRuleBuilder::object_schema_to_baas_schema(const ObjectSchema& obj_schema,
                                                             IncludePropCond include_prop)
{
    m_relationships.clear();

    auto schema_json = object_schema_to_jsonschema(obj_schema, include_prop, true);
    auto& prop_sub_obj = schema_json["properties"];
    if (!prop_sub_obj.contains(m_partition_key.name) && !m_is_flx_sync) {
        prop_sub_obj.emplace(m_partition_key.name, property_to_jsonschema(m_partition_key));
        if (!is_nullable(m_partition_key.type)) {
            schema_json["required"].push_back(m_partition_key.name);
        }
    }
    return {
        {"schema", schema_json},
        {"metadata", nlohmann::json::object({{"database", m_mongo_db_name},
                                             {"collection", obj_schema.name},
                                             {"data_source", m_mongo_service_name}})},
        {"relationships", m_relationships},
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
    std::string_view combined(buffer, size * nitems);
    if (auto pos = combined.find(':'); pos != std::string::npos) {
        std::string_view key = combined.substr(0, pos);
        std::string_view value = combined.substr(pos + 1);
        if (auto first_not_space = value.find_first_not_of(' '); first_not_space != std::string::npos) {
            value = value.substr(first_not_space);
        }
        if (auto last_not_nl = value.find_last_not_of("\r\n"); last_not_nl != std::string::npos) {
            value = value.substr(0, last_not_nl + 1);
        }
        response_headers->insert({std::string{key}, std::string{value}});
    }
    else {
        if (combined.size() > 5 && combined.substr(0, 5) != "HTTP/") { // ignore for now HTTP/1.1 ...
            std::cerr << "test transport skipping header: " << combined << std::endl;
        }
    }
    return nitems * size;
}

std::string_view getenv_sv(const char* name) noexcept
{
    if (auto ptr = ::getenv(name); ptr != nullptr) {
        return std::string_view(ptr);
    }

    return {};
}

const static std::string g_baas_coid_header_name("x-appservices-request-id");

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
    app::HttpHeaders response_headers;

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

    // Enable redirection, and don't revert POST to GET for 301/302/303 redirects
    // Max redirects is 30 by default
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);
    curl_easy_setopt(curl, CURLOPT_POSTREDIR, CURL_REDIR_POST_ALL);

    // Set callbacks to write the response headers and data
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, list);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, curl_header_cb);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &response_headers);

#ifdef REALM_CURL_CACERTS
    auto ca_info = unquote_string(REALM_QUOTE(REALM_CURL_CACERTS));
    curl_easy_setopt(curl, CURLOPT_CAINFO, ca_info.c_str());
#endif

    auto start_time = std::chrono::steady_clock::now();
    auto response_code = curl_easy_perform(curl);
    auto total_time = std::chrono::steady_clock::now() - start_time;

    auto logger = util::Logger::get_default_logger();
    if (response_code != CURLE_OK) {
        std::string message = curl_easy_strerror(response_code);
        logger->error("curl_easy_perform() failed when sending request to '%1' with body '%2': %3", request.url,
                      request.body, message);
        // Return a failing response with the CURL error as the custom code
        return {0, response_code, {}, message};
    }
    if (logger->would_log(util::Logger::Level::trace)) {
        std::string coid = [&] {
            auto coid_header = response_headers.find(g_baas_coid_header_name);
            if (coid_header == response_headers.end()) {
                return std::string{};
            }
            return util::format("BaaS Coid: \"%1\"", coid_header->second);
        }();

        logger->trace("Baas API %1 request to %2 took %3 %4\n", request.method, request.url,
                      std::chrono::duration_cast<std::chrono::milliseconds>(total_time), coid);
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

class Baasaas {
public:
    enum class StartMode { Default, GitHash, Branch, PatchId };
    explicit Baasaas(std::string api_key, StartMode mode, std::string ref_spec)
        : m_api_key(std::move(api_key))
        , m_base_url(get_baasaas_base_url())
        , m_externally_managed_instance(false)
    {
        auto logger = util::Logger::get_default_logger();
        std::string url_path = "startContainer";
        if (mode == StartMode::GitHash) {
            url_path = util::format("startContainer?githash=%1", ref_spec);
            logger->info("Starting baasaas container with githash of %1", ref_spec);
        }
        else if (mode == StartMode::Branch) {
            url_path = util::format("startContainer?branch=%1", ref_spec);
            logger->info("Starting baasaas container on branch %1", ref_spec);
        }
        else if (mode == StartMode::PatchId) {
            url_path = util::format("startContainer?patchId=%1", ref_spec);
            logger->info("Starting baasaas container for patch id %1", ref_spec);
        }
        else {
            logger->info("Starting baasaas container");
        }

        auto [resp, baas_coid] = do_request(std::move(url_path), app::HttpMethod::post);
        if (!resp["id"].is_string()) {
            throw RuntimeError(
                ErrorCodes::RuntimeError,
                util::format(
                    "Failed to start baas container, got response without container ID: \"%1\" (baas coid: %2)",
                    resp.dump(), baas_coid));
        }
        m_container_id = resp["id"].get<std::string>();
        if (m_container_id.empty()) {
            throw RuntimeError(
                ErrorCodes::InvalidArgument,
                util::format("Failed to start baas container, got response with empty container ID (baas coid: %1)",
                             baas_coid));
        }
        logger->info("Baasaas container started with id \"%1\"", m_container_id);
        util::File lock_file(s_baasaas_lock_file_name, util::File::mode_Write);
        lock_file.write(0, m_container_id);
    }

    explicit Baasaas(std::string api_key, std::string baasaas_instance_id)
        : m_api_key(std::move(api_key))
        , m_base_url(get_baasaas_base_url())
        , m_container_id(std::move(baasaas_instance_id))
        , m_externally_managed_instance(true)
    {
        auto logger = util::Logger::get_default_logger();
        logger->info("Using externally managed baasaas instance \"%1\"", m_container_id);
    }

    Baasaas(const Baasaas&) = delete;
    Baasaas(Baasaas&&) = delete;
    Baasaas& operator=(const Baasaas&) = delete;
    Baasaas& operator=(Baasaas&&) = delete;

    ~Baasaas()
    {
        stop();
    }

    void poll()
    {
        if (!m_http_endpoint.empty() || m_container_id.empty()) {
            return;
        }

        auto logger = util::Logger::get_default_logger();
        auto poll_start_at = std::chrono::system_clock::now();
        std::string http_endpoint;
        std::string mongo_endpoint;
        bool logged = false;
        while (std::chrono::system_clock::now() - poll_start_at < std::chrono::minutes(2) &&
               m_http_endpoint.empty()) {
            if (http_endpoint.empty()) {
                auto [status_obj, baas_coid] =
                    do_request(util::format("containerStatus?id=%1", m_container_id), app::HttpMethod::get);
                if (!status_obj["httpUrl"].is_null()) {
                    if (!status_obj["httpUrl"].is_string() || !status_obj["mongoUrl"].is_string()) {
                        throw RuntimeError(ErrorCodes::RuntimeError,
                                           util::format("Error polling for baasaas instance. httpUrl or mongoUrl is "
                                                        "the wrong format: \"%1\" (baas coid: %2)",
                                                        status_obj.dump(), baas_coid));
                    }
                    http_endpoint = status_obj["httpUrl"].get<std::string>();
                    mongo_endpoint = status_obj["mongoUrl"].get<std::string>();
                }
            }
            else {
                app::Request baas_req;
                baas_req.url = util::format("%1/api/private/v1.0/version", http_endpoint);
                baas_req.method = app::HttpMethod::get;
                baas_req.headers.insert_or_assign("Content-Type", "application/json");
                auto baas_resp = do_http_request(baas_req);
                if (baas_resp.http_status_code >= 200 && baas_resp.http_status_code < 300) {
                    m_http_endpoint = http_endpoint;
                    m_mongo_endpoint = mongo_endpoint;
                    break;
                }
            }

            if (!logged) {
                logger->info("Waiting for baasaas container \"%1\" to be ready", m_container_id);
                logged = true;
            }
            std::this_thread::sleep_for(std::chrono::seconds(3));
        }

        if (m_http_endpoint.empty()) {
            throw std::runtime_error(
                util::format("Failed to launch baasaas container %1 within 2 minutes", m_container_id));
        }
    }

    void stop()
    {
        if (m_externally_managed_instance) {
            return;
        }
        auto container_id = std::move(m_container_id);
        if (container_id.empty()) {
            return;
        }

        auto logger = util::Logger::get_default_logger();
        logger->info("Stopping baasaas container with id \"%1\"", container_id);
        do_request(util::format("stopContainer?id=%1", container_id), app::HttpMethod::post);
        auto lock_file = util::File(std::string{s_baasaas_lock_file_name}, util::File::mode_Write);
        lock_file.resize(0);
        lock_file.close();
        util::File::remove(lock_file.get_path());
    }

    const std::string admin_endpoint()
    {
        poll();
        return m_http_endpoint;
    }

    std::string http_endpoint()
    {
        poll();
        return m_http_endpoint;
    }

    const std::string& mongo_endpoint()
    {
        poll();
        return m_mongo_endpoint;
    }

private:
    std::pair<nlohmann::json, std::string> do_request(std::string api_path, app::HttpMethod method)
    {
        app::Request request;

        request.url = util::format("%1/%2", m_base_url, api_path);
        request.method = method;
        request.headers.insert_or_assign("apiKey", m_api_key);
        request.headers.insert_or_assign("Content-Type", "application/json");
        auto response = do_http_request(request);
        if (response.http_status_code < 200 || response.http_status_code >= 300) {
            throw RuntimeError(ErrorCodes::HTTPError,
                               util::format("Baasaas api response code: %1 Response body: %2, Baas coid: %3",
                                            response.http_status_code, response.body,
                                            baas_coid_from_response(response)));
        }
        try {
            return {nlohmann::json::parse(response.body), baas_coid_from_response(response)};
        }
        catch (const nlohmann::json::exception& e) {
            throw RuntimeError(
                ErrorCodes::MalformedJson,
                util::format("Error making baasaas request to %1 (baas coid %2): Invalid json returned \"%3\" (%4)",
                             request.url, baas_coid_from_response(response), response.body, e.what()));
        }
    }

    std::string baas_coid_from_response(const app::Response& resp)
    {
        if (auto it = resp.headers.find(g_baas_coid_header_name); it != resp.headers.end()) {
            return it->second;
        }
        return "<not found>";
    }

    static std::string get_baasaas_base_url()
    {
        auto env_value = getenv_sv("BAASAAS_BASE_URL");
        if (env_value.empty()) {
            // This is the current default endpoint for baasaas maintained by the sync team.
            // You can reach out for help in #appx-device-sync-internal if there are problems.
            return "https://us-east-1.aws.data.mongodb-api.com/app/baas-container-service-autzb/endpoint";
        }

        return unquote_string(env_value);
    }

    constexpr static std::string_view s_baasaas_lock_file_name = "baasaas_instance.lock";

    std::string m_api_key;
    std::string m_base_url;
    std::string m_container_id;
    bool m_externally_managed_instance;
    std::string m_http_endpoint;
    std::string m_mongo_endpoint;
};

static std::optional<sync::RedirectingHttpServer>& get_redirector(const std::string& base_url)
{
    static std::optional<sync::RedirectingHttpServer> redirector;
    auto redirector_enabled = [&] {
        const static auto enabled_values = {"On", "on", "1"};
        auto enable_redirector = getenv_sv("ENABLE_BAAS_REDIRECTOR");
        return std::any_of(enabled_values.begin(), enabled_values.end(), [&](const auto val) {
            return val == enable_redirector;
        });
    };

    if (redirector_enabled() && !redirector && !base_url.empty()) {
        redirector.emplace(base_url, util::Logger::get_default_logger());
    }

    return redirector;
}

class BaasaasLauncher : public Catch::EventListenerBase {
public:
    static std::optional<Baasaas>& get_baasaas_holder()
    {
        static std::optional<Baasaas> global_baasaas = std::nullopt;
        return global_baasaas;
    }

    using Catch::EventListenerBase::EventListenerBase;

    void testRunStarting(Catch::TestRunInfo const&) override
    {
        std::string_view api_key(getenv_sv("BAASAAS_API_KEY"));
        if (api_key.empty()) {
            return;
        }

        // Allow overriding the baas base url at runtime via an environment variable, even if BAASAAS_API_KEY
        // is also specified.
        if (!getenv_sv("BAAS_BASE_URL").empty()) {
            return;
        }

        // If we've started a baasaas container outside of running the tests, then use that instead of
        // figuring out how to start our own.
        if (auto baasaas_instance = getenv_sv("BAASAAS_INSTANCE_ID"); !baasaas_instance.empty()) {
            auto& baasaas_holder = get_baasaas_holder();
            REALM_ASSERT(!baasaas_holder);
            baasaas_holder.emplace(std::string{api_key}, std::string{baasaas_instance});
            return;
        }

        std::string_view ref_spec(getenv_sv("BAASAAS_REF_SPEC"));
        std::string_view mode_spec(getenv_sv("BAASAAS_START_MODE"));
        Baasaas::StartMode mode = Baasaas::StartMode::Default;
        if (mode_spec == "branch") {
            if (ref_spec.empty()) {
                throw std::runtime_error("Expected branch name in BAASAAS_REF_SPEC env variable, but it was empty");
            }
            mode = Baasaas::StartMode::Branch;
        }
        else if (mode_spec == "githash") {
            if (ref_spec.empty()) {
                throw std::runtime_error("Expected git hash in BAASAAS_REF_SPEC env variable, but it was empty");
            }
            mode = Baasaas::StartMode::GitHash;
        }
        else if (mode_spec == "patchid") {
            if (ref_spec.empty()) {
                throw std::runtime_error("Expected patch id in BAASAAS_REF_SPEC env variable, but it was empty");
            }
            mode = Baasaas::StartMode::PatchId;
        }
        else {
            if (!mode_spec.empty()) {
                throw std::runtime_error("Expected BAASAAS_START_MODE to be \"githash\", \"patchid\", or \"branch\"");
            }
            ref_spec = {};
        }

        auto& baasaas_holder = get_baasaas_holder();
        REALM_ASSERT(!baasaas_holder);
        baasaas_holder.emplace(std::string{api_key}, mode, std::string{ref_spec});

        get_runtime_app_session();
    }

    void testRunEnded(Catch::TestRunStats const&) override
    {
        if (auto& redirector = get_redirector({})) {
            redirector = std::nullopt;
        }

        if (auto& baasaas_holder = get_baasaas_holder()) {
            baasaas_holder->stop();
        }
    }
};

CATCH_REGISTER_LISTENER(BaasaasLauncher)

AdminAPIEndpoint AdminAPIEndpoint::operator[](StringData name) const
{
    return AdminAPIEndpoint(util::format("%1/%2", m_url, name), m_access_token);
}

app::Response AdminAPIEndpoint::do_request(app::Request request) const
{
    if (request.url.find('?') == std::string::npos) {
        request.url = util::format("%1?bypass_service_change=SyncSchemaVersionIncrease", request.url);
    }
    else {
        request.url = util::format("%1&bypass_service_change=SyncSchemaVersionIncrease", request.url);
    }
    request.headers["Content-Type"] = "application/json;charset=utf-8";
    request.headers["Accept"] = "application/json";
    request.headers["Authorization"] = util::format("Bearer %1", m_access_token);
    return do_http_request(std::move(request));
}

app::Response AdminAPIEndpoint::get(const std::vector<std::pair<std::string, std::string>>& params) const
{
    app::Request req;
    req.method = app::HttpMethod::get;
    std::stringstream ss;
    bool needs_and = false;
    ss << m_url;
    if (!params.empty() && m_url.find('?') != std::string::npos) {
        needs_and = true;
    }
    for (const auto& param : params) {
        if (needs_and) {
            ss << "&";
        }
        else {
            ss << "?";
        }
        needs_and = true;
        ss << param.first << "=" << param.second;
    }
    req.url = ss.str();
    return do_request(std::move(req));
}

app::Response AdminAPIEndpoint::del() const
{
    app::Request req;
    req.method = app::HttpMethod::del;
    req.url = m_url;
    return do_request(std::move(req));
}

nlohmann::json AdminAPIEndpoint::get_json(const std::vector<std::pair<std::string, std::string>>& params) const
{
    auto resp = get(params);
    REALM_ASSERT_EX(resp.http_status_code >= 200 && resp.http_status_code < 300, m_url, resp.http_status_code,
                    resp.body);
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
    REALM_ASSERT_EX(resp.http_status_code >= 200 && resp.http_status_code < 300, m_url, body.dump(),
                    resp.http_status_code, resp.body);
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
    REALM_ASSERT_EX(resp.http_status_code >= 200 && resp.http_status_code < 300, m_url, body.dump(),
                    resp.http_status_code, resp.body);
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
    REALM_ASSERT_EX(resp.http_status_code >= 200 && resp.http_status_code < 300, m_url, body.dump(),
                    resp.http_status_code, resp.body);
    return nlohmann::json::parse(resp.body.empty() ? "{}" : resp.body);
}

AdminAPISession AdminAPISession::login(const AppCreateConfig& config)
{
    std::string admin_url = config.admin_url;
    nlohmann::json login_req_body{
        {"provider", "userpass"},
        {"username", config.admin_username},
        {"password", config.admin_password},
    };
    if (config.logger) {
        config.logger->trace("Logging into baas admin api: %1", admin_url);
    }
    app::Request auth_req{
        app::HttpMethod::post,
        util::format("%1/api/admin/v3.0/auth/providers/local-userpass/login", admin_url),
        60000, // 1 minute timeout
        {
            {"Content-Type", "application/json;charset=utf-8"},
            {"Accept", "application/json"},
        },
        login_req_body.dump(),
    };
    auto login_resp = do_http_request(std::move(auth_req));
    REALM_ASSERT_EX(login_resp.http_status_code == 200, login_resp.http_status_code, login_resp.body);
    auto login_resp_body = nlohmann::json::parse(login_resp.body);

    std::string access_token = login_resp_body["access_token"];

    AdminAPIEndpoint user_profile(util::format("%1/api/admin/v3.0/auth/profile", admin_url), access_token);
    auto profile_resp = user_profile.get_json();

    std::string group_id = profile_resp["roles"][0]["group_id"];

    return AdminAPISession(std::move(admin_url), std::move(access_token), std::move(group_id));
}

void AdminAPISession::revoke_user_sessions(const std::string& user_id, const std::string& app_id) const
{
    auto endpoint = apps()[app_id]["users"][user_id]["logout"];
    auto response = endpoint.put("");
    REALM_ASSERT_EX(response.http_status_code == 204, response.http_status_code, response.body);
}

void AdminAPISession::disable_user_sessions(const std::string& user_id, const std::string& app_id) const
{
    auto endpoint = apps()[app_id]["users"][user_id]["disable"];
    auto response = endpoint.put("");
    REALM_ASSERT_EX(response.http_status_code == 204, response.http_status_code, response.body);
}

void AdminAPISession::enable_user_sessions(const std::string& user_id, const std::string& app_id) const
{
    auto endpoint = apps()[app_id]["users"][user_id]["enable"];
    auto response = endpoint.put("");
    REALM_ASSERT_EX(response.http_status_code == 204, response.http_status_code, response.body);
}

// returns false for an invalid/expired access token
bool AdminAPISession::verify_access_token(const std::string& access_token, const std::string& app_id) const
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

void AdminAPISession::set_development_mode_to(const std::string& app_id, bool enable) const
{
    auto endpoint = apps()[app_id]["sync"]["config"];
    endpoint.put_json({{"development_mode_enabled", enable}});
}

void AdminAPISession::delete_app(const std::string& app_id) const
{
    auto app_endpoint = apps()[app_id];
    auto resp = app_endpoint.del();
    REALM_ASSERT_EX(resp.http_status_code == 204, resp.http_status_code, resp.body);
}

std::vector<AdminAPISession::Service> AdminAPISession::get_services(const std::string& app_id) const
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


std::vector<std::string> AdminAPISession::get_errors(const std::string& app_id) const
{
    auto endpoint = apps()[app_id]["logs"];
    auto response = endpoint.get_json({{"errors_only", "true"}});
    std::vector<std::string> errors;
    const auto& logs = response["logs"];
    std::transform(logs.begin(), logs.end(), std::back_inserter(errors), [](const auto& err) {
        return err["error"];
    });
    return errors;
}


AdminAPISession::Service AdminAPISession::get_sync_service(const std::string& app_id) const
{
    auto services = get_services(app_id);
    auto sync_service = std::find_if(services.begin(), services.end(), [&](auto s) {
        return s.type == "mongodb";
    });
    REALM_ASSERT(sync_service != services.end());
    return *sync_service;
}

void AdminAPISession::trigger_client_reset(const std::string& app_id, int64_t file_ident) const
{
    auto endpoint = apps(APIFamily::Admin)[app_id]["sync"]["force_reset"];
    endpoint.put_json(nlohmann::json{{"file_ident", file_ident}});
}

void AdminAPISession::migrate_to_flx(const std::string& app_id, const std::string& service_id,
                                     bool migrate_to_flx) const
{
    auto endpoint = apps()[app_id]["sync"]["migration"];
    endpoint.put_json(nlohmann::json{{"serviceId", service_id}, {"action", migrate_to_flx ? "start" : "rollback"}});
}

// Each breaking change bumps the schema version, so you can create a new version for each breaking change if
// 'use_draft' is false. Set 'use_draft' to true if you want all changes to the schema to be deployed at once
// resulting in only one schema version.
void AdminAPISession::create_schema(const std::string& app_id, const AppCreateConfig& config, bool use_draft) const
{
    static const std::string mongo_service_name = "BackingDB";

    auto drafts = apps()[app_id]["drafts"];
    std::string draft_id;
    if (use_draft) {
        auto draft_create_resp = drafts.post_json({});
        draft_id = draft_create_resp["_id"];
    }

    auto schemas = apps()[app_id]["schemas"];
    auto current_schema = schemas.get_json();
    auto target_schema = config.schema;

    std::unordered_map<std::string, std::string> current_schema_tables;
    for (const auto& schema : current_schema) {
        current_schema_tables[schema["metadata"]["collection"]] = schema["_id"];
    }

    // Add new tables

    auto pk_and_queryable_only = [&](const Property& prop) {
        if (config.flx_sync_config) {
            const auto& queryable_fields = config.flx_sync_config->queryable_fields;

            if (std::find(queryable_fields.begin(), queryable_fields.end(), prop.name) != queryable_fields.end()) {
                return true;
            }
        }
        return prop.name == "_id" || prop.name == config.partition_key.name;
    };

    // Create the schemas in two passes: first populate just the primary key and
    // partition key, then add the rest of the properties. This ensures that the
    // targets of links exist before adding the links.
    std::vector<std::pair<std::string, const ObjectSchema*>> object_schema_to_create;
    BaasRuleBuilder rule_builder(target_schema, config.partition_key, mongo_service_name, config.mongo_dbname,
                                 static_cast<bool>(config.flx_sync_config));
    for (const auto& obj_schema : target_schema) {
        auto it = current_schema_tables.find(obj_schema.name);
        if (it != current_schema_tables.end()) {
            object_schema_to_create.push_back({it->second, &obj_schema});
            continue;
        }

        auto schema_to_create = rule_builder.object_schema_to_baas_schema(obj_schema, pk_and_queryable_only);
        auto schema_create_resp = schemas.post_json(schema_to_create);
        object_schema_to_create.push_back({schema_create_resp["_id"], &obj_schema});
    }

    // Update existing tables (including the ones just created)
    for (const auto& [id, obj_schema] : object_schema_to_create) {
        auto schema_to_create = rule_builder.object_schema_to_baas_schema(*obj_schema, nullptr);
        schema_to_create["_id"] = id;
        schemas[id].put_json(schema_to_create);
    }

    // Delete removed tables
    for (const auto& table : current_schema_tables) {
        if (target_schema.find(table.first) == target_schema.end()) {
            schemas[table.second].del();
        }
    }

    if (use_draft) {
        drafts[draft_id]["deployment"].post_json({});
    }
}

bool AdminAPISession::set_feature_flag(const std::string& app_id, const std::string& flag_name, bool enable) const
{
    auto features = apps(APIFamily::Private)[app_id]["features"];
    auto flag_response =
        features.post_json(nlohmann::json{{"action", enable ? "enable" : "disable"}, {"feature_flags", {flag_name}}});
    return flag_response.empty();
}

bool AdminAPISession::get_feature_flag(const std::string& app_id, const std::string& flag_name) const
{
    auto features = apps(APIFamily::Private)[app_id]["features"];
    auto response = features.get_json();
    if (auto feature_list = response["enabled"]; !feature_list.empty()) {
        return std::find_if(feature_list.begin(), feature_list.end(), [&flag_name](const auto& feature) {
                   return feature == flag_name;
               }) != feature_list.end();
    }
    return false;
}

nlohmann::json AdminAPISession::get_default_rule(const std::string& app_id) const
{
    auto baas_sync_service = get_sync_service(app_id);
    auto rule_endpoint = apps()[app_id]["services"][baas_sync_service.id]["default_rule"];
    auto rule = rule_endpoint.get_json();
    return rule;
}

bool AdminAPISession::update_default_rule(const std::string& app_id, nlohmann::json rule_json) const
{
    if (auto id = rule_json.find("_id");
        id == rule_json.end() || !id->is_string() || id->get<std::string>().empty()) {
        return false;
    }

    auto baas_sync_service = get_sync_service(app_id);
    auto rule_endpoint = apps()[app_id]["services"][baas_sync_service.id]["default_rule"];
    auto response = rule_endpoint.put_json(rule_json);
    return response.empty();
}

nlohmann::json AdminAPISession::get_app_settings(const std::string& app_id) const
{
    auto settings_endpoint = apps(APIFamily::Private)[app_id]["settings"];
    return settings_endpoint.get_json();
}

bool AdminAPISession::patch_app_settings(const std::string& app_id, nlohmann::json&& json) const
{
    auto settings_endpoint = apps(APIFamily::Private)[app_id]["settings"];
    auto response = settings_endpoint.patch_json(std::move(json));
    return response.empty();
}

static nlohmann::json convert_config(AdminAPISession::ServiceConfig config)
{
    if (config.mode == AdminAPISession::ServiceConfig::SyncMode::Flexible) {
        auto payload = nlohmann::json{{"database_name", config.database_name},
                                      {"state", config.state},
                                      {"is_recovery_mode_disabled", config.recovery_is_disabled}};
        if (config.queryable_field_names) {
            payload["queryable_fields_names"] = *config.queryable_field_names;
        }
        if (config.permissions) {
            payload["permissions"] = *config.permissions;
        }
        if (config.asymmetric_tables) {
            payload["asymmetric_tables"] = *config.asymmetric_tables;
        }
        return payload;
    }
    return nlohmann::json{{"database_name", config.database_name},
                          {"partition", *config.partition},
                          {"state", config.state},
                          {"is_recovery_mode_disabled", config.recovery_is_disabled}};
}

AdminAPIEndpoint AdminAPISession::service_config_endpoint(const std::string& app_id,
                                                          const std::string& service_id) const
{
    return apps()[app_id]["services"][service_id]["config"];
}

AdminAPISession::ServiceConfig AdminAPISession::disable_sync(const std::string& app_id, const std::string& service_id,
                                                             AdminAPISession::ServiceConfig sync_config) const
{
    auto endpoint = service_config_endpoint(app_id, service_id);
    if (sync_config.state != "") {
        sync_config.state = "";
        endpoint.patch_json({{sync_config.sync_service_name(), convert_config(sync_config)}});
    }
    return sync_config;
}

AdminAPISession::ServiceConfig AdminAPISession::pause_sync(const std::string& app_id, const std::string& service_id,
                                                           AdminAPISession::ServiceConfig sync_config) const
{
    auto endpoint = service_config_endpoint(app_id, service_id);
    if (sync_config.state != "disabled") {
        sync_config.state = "disabled";
        endpoint.patch_json({{sync_config.sync_service_name(), convert_config(sync_config)}});
    }
    return sync_config;
}

AdminAPISession::ServiceConfig AdminAPISession::enable_sync(const std::string& app_id, const std::string& service_id,
                                                            AdminAPISession::ServiceConfig sync_config) const
{
    auto endpoint = service_config_endpoint(app_id, service_id);
    sync_config.state = "enabled";
    endpoint.patch_json({{sync_config.sync_service_name(), convert_config(sync_config)}});
    return sync_config;
}

AdminAPISession::ServiceConfig AdminAPISession::set_disable_recovery_to(const std::string& app_id,
                                                                        const std::string& service_id,
                                                                        ServiceConfig sync_config, bool disable) const
{
    auto endpoint = service_config_endpoint(app_id, service_id);
    sync_config.recovery_is_disabled = disable;
    endpoint.patch_json({{sync_config.sync_service_name(), convert_config(sync_config)}});
    return sync_config;
}

std::vector<AdminAPISession::SchemaVersionInfo> AdminAPISession::get_schema_versions(const std::string& app_id) const
{
    std::vector<AdminAPISession::SchemaVersionInfo> ret;
    auto endpoint = apps()[app_id]["sync"]["schemas"]["versions"];
    auto res = endpoint.get_json();
    for (auto&& version : res["versions"].get<std::vector<nlohmann::json>>()) {
        SchemaVersionInfo info;
        info.version_major = version["version_major"];
        ret.push_back(std::move(info));
    }

    return ret;
}

AdminAPISession::ServiceConfig AdminAPISession::get_config(const std::string& app_id,
                                                           const AdminAPISession::Service& service) const
{
    auto endpoint = service_config_endpoint(app_id, service.id);
    auto response = endpoint.get_json();
    AdminAPISession::ServiceConfig config;
    if (response.contains("flexible_sync")) {
        auto sync = response["flexible_sync"];
        config.mode = AdminAPISession::ServiceConfig::SyncMode::Flexible;
        config.state = sync["state"];
        config.database_name = sync["database_name"];
        config.permissions = sync["permissions"];
        config.queryable_field_names = sync["queryable_fields_names"];
        auto recovery_disabled = sync["is_recovery_mode_disabled"];
        config.recovery_is_disabled = recovery_disabled.is_boolean() ? recovery_disabled.get<bool>() : false;
    }
    else if (response.contains("sync")) {
        auto sync = response["sync"];
        config.mode = AdminAPISession::ServiceConfig::SyncMode::Partitioned;
        config.state = sync["state"];
        config.database_name = sync["database_name"];
        config.partition = sync["partition"];
        auto recovery_disabled = sync["is_recovery_mode_disabled"];
        config.recovery_is_disabled = recovery_disabled.is_boolean() ? recovery_disabled.get<bool>() : false;
    }
    else {
        throw std::runtime_error(util::format("Unsupported config format from server: %1", response));
    }
    return config;
}

bool AdminAPISession::is_sync_enabled(const std::string& app_id) const
{
    auto sync_service = get_sync_service(app_id);
    auto config = get_config(app_id, sync_service);
    return config.state == "enabled";
}

bool AdminAPISession::is_sync_terminated(const std::string& app_id) const
{
    auto sync_service = get_sync_service(app_id);
    auto config = get_config(app_id, sync_service);
    if (config.state == "enabled") {
        return false;
    }
    auto state_endpoint = apps()[app_id]["sync"]["state"];
    auto state_result = state_endpoint.get_json(
        {{"sync_type", config.mode == ServiceConfig::SyncMode::Flexible ? "flexible" : "partition"}});
    return state_result["state"].get<std::string>().empty();
}

bool AdminAPISession::is_initial_sync_complete(const std::string& app_id, bool is_flx_sync) const
{
    auto progress_endpoint = apps()[app_id]["sync"]["progress"];
    auto progress_result = progress_endpoint.get_json();
    if (is_flx_sync) {
        // accepting_clients key is only true in FLX after the first initial sync has completed
        auto it = progress_result.find("accepting_clients");
        return it != progress_result.end() && it->is_boolean() && it->get<bool>();
    }

    if (auto it = progress_result.find("progress"); it != progress_result.end() && it->is_object() && !it->empty()) {
        for (auto& elem : *it) {
            auto is_complete = elem["complete"];
            if (!is_complete.is_boolean() || !is_complete.get<bool>()) {
                return false;
            }
        }
        return true;
    }
    return false;
}

AdminAPISession::MigrationStatus AdminAPISession::get_migration_status(const std::string& app_id) const
{
    MigrationStatus status;
    auto progress_endpoint = apps()[app_id]["sync"]["migration"];
    auto progress_result = progress_endpoint.get_json();
    auto errorMessage = progress_result["errorMessage"];
    if (errorMessage.is_string() && !errorMessage.get<std::string>().empty()) {
        throw Exception(Status{ErrorCodes::RuntimeError, errorMessage.get<std::string>()});
    }
    if (!progress_result["statusMessage"].is_string() || !progress_result["isMigrated"].is_boolean()) {
        throw Exception(
            Status{ErrorCodes::RuntimeError, util::format("Invalid result returned from migration status request: %1",
                                                          progress_result.dump(4, 32, true))});
    }

    status.statusMessage = progress_result["statusMessage"].get<std::string>();
    status.isMigrated = progress_result["isMigrated"].get<bool>();
    status.isCancelable = progress_result["isCancelable"].get<bool>();
    status.isRevertible = progress_result["isRevertible"].get<bool>();
    status.complete = status.statusMessage.empty();
    return status;
}

AdminAPIEndpoint AdminAPISession::apps(APIFamily family) const
{
    switch (family) {
        case APIFamily::Admin:
            return AdminAPIEndpoint(util::format("%1/api/admin/v3.0/groups/%2/apps", m_base_url, m_group_id),
                                    m_access_token);
        case APIFamily::Private:
            return AdminAPIEndpoint(util::format("%1/api/private/v1.0/groups/%2/apps", m_base_url, m_group_id),
                                    m_access_token);
    }
    REALM_UNREACHABLE();
}

realm::Schema get_default_schema()
{
    const auto dog_schema =
        ObjectSchema("Dog", {realm::Property("_id", PropertyType::ObjectId | PropertyType::Nullable, true),
                             realm::Property("breed", PropertyType::String | PropertyType::Nullable),
                             realm::Property("name", PropertyType::String),
                             realm::Property("realm_id", PropertyType::String | PropertyType::Nullable)});
    const auto cat_schema =
        ObjectSchema("Cat", {realm::Property("_id", PropertyType::String | PropertyType::Nullable, true),
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
    return realm::Schema({dog_schema, cat_schema, person_schema});
}

std::string get_base_url()
{
    auto base_url = get_real_base_url();

    auto& redirector = get_redirector(base_url);
    if (redirector) {
        return redirector->base_url();
    }
    return base_url;
}

std::string get_real_base_url()
{
    if (auto baas_url = getenv_sv("BAAS_BASE_URL"); !baas_url.empty()) {
        return std::string{baas_url};
    }
    if (auto& baasaas_holder = BaasaasLauncher::get_baasaas_holder(); baasaas_holder.has_value()) {
        return baasaas_holder->http_endpoint();
    }

    return get_compile_time_base_url();
}


std::string get_admin_url()
{
    if (auto baas_admin_url = getenv_sv("BAAS_ADMIN_URL"); !baas_admin_url.empty()) {
        return std::string{baas_admin_url};
    }
    if (auto compile_url = get_compile_time_admin_url(); !compile_url.empty()) {
        return compile_url;
    }
    if (auto& baasaas_holder = BaasaasLauncher::get_baasaas_holder(); baasaas_holder.has_value()) {
        return baasaas_holder->admin_endpoint();
    }

    return get_real_base_url();
}

std::string get_mongodb_server()
{
    if (auto baas_url = getenv_sv("BAAS_MONGO_URL"); !baas_url.empty()) {
        return std::string{baas_url};
    }

    if (auto& baasaas_holder = BaasaasLauncher::get_baasaas_holder(); baasaas_holder.has_value()) {
        return baasaas_holder->mongo_endpoint();
    }
    return "mongodb://localhost:26000";
}


AppCreateConfig default_app_config()
{
    ObjectId id = ObjectId::gen();
    std::string db_name = util::format("test_data_%1", id.to_string());
    std::string app_url = get_base_url();
    std::string admin_url = get_admin_url();
    REALM_ASSERT(!app_url.empty());
    REALM_ASSERT(!admin_url.empty());

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

    return AppCreateConfig{
        "test",
        std::move(app_url),
        std::move(admin_url), // BAAS Admin API URL may be different
        "unique_user@domain.com",
        "password",
        get_mongodb_server(),
        db_name,
        get_default_schema(),
        std::move(partition_key),
        false,                              // Dev mode disabled
        util::none,                         // Default to no FLX sync config
        std::move(funcs),                   // Add default functions
        std::move(user_pass_config),        // enable basic user/pass auth
        std::string{"authFunc"},            // custom auth function
        true,                               // enable_api_key_auth
        true,                               // enable_anonymous_auth
        true,                               // enable_custom_token_auth
        {},                                 // no service roles on the default rule
        util::Logger::get_default_logger(), // provide the logger to the admin api
    };
}

AppCreateConfig minimal_app_config(const std::string& name, const Schema& schema)
{
    Property partition_key("realm_id", PropertyType::String | PropertyType::Nullable);
    std::string app_url = get_base_url();
    std::string admin_url = get_admin_url();
    REALM_ASSERT(!app_url.empty());
    REALM_ASSERT(!admin_url.empty());

    AppCreateConfig::UserPassAuthConfig user_pass_config{
        true,  "Confirm", "", "http://example.com/confirmEmail", "", "Reset", "http://exmaple.com/resetPassword",
        false, false,
    };

    ObjectId id = ObjectId::gen();
    return AppCreateConfig{
        name,
        std::move(app_url),
        std::move(admin_url), // BAAS Admin API URL may be different
        "unique_user@domain.com",
        "password",
        get_mongodb_server(),
        util::format("test_data_%1_%2", name, id.to_string()),
        schema,
        std::move(partition_key),
        false,                              // Dev mode disabled
        util::none,                         // no FLX sync config
        {},                                 // no functions
        std::move(user_pass_config),        // enable basic user/pass auth
        util::none,                         // disable custom auth
        true,                               // enable api key auth
        true,                               // enable anonymous auth
        false,                              // enable_custom_token_auth
        {},                                 // no service roles on the default rule
        util::Logger::get_default_logger(), // provide the logger to the admin api
    };
}

nlohmann::json transform_service_role(const AppCreateConfig::ServiceRole& role_def)
{
    return {
        {"name", role_def.name},
        {"apply_when", role_def.apply_when},
        {"document_filters",
         {
             {"read", role_def.document_filters.read},
             {"write", role_def.document_filters.write},
         }},
        {"insert", role_def.insert_filter},
        {"delete", role_def.delete_filter},
        {"read", role_def.read},
        {"write", role_def.write},
    };
}

AppSession create_app(const AppCreateConfig& config)
{
    auto session = AdminAPISession::login(config);
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
    nlohmann::json sync_config;
    if (config.flx_sync_config) {
        auto queryable_fields = nlohmann::json::array();
        const auto& queryable_fields_src = config.flx_sync_config->queryable_fields;
        std::copy(queryable_fields_src.begin(), queryable_fields_src.end(), std::back_inserter(queryable_fields));
        auto asymmetric_tables = nlohmann::json::array();
        for (const auto& obj_schema : config.schema) {
            if (obj_schema.table_type == ObjectSchema::ObjectType::TopLevelAsymmetric) {
                asymmetric_tables.emplace_back(obj_schema.name);
            }
        }
        sync_config = nlohmann::json{{"database_name", config.mongo_dbname},
                                     {"queryable_fields_names", queryable_fields},
                                     {"asymmetric_tables", asymmetric_tables}};
        mongo_service_def["config"]["flexible_sync"] = sync_config;
    }
    else {
        sync_config = nlohmann::json{
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
        mongo_service_def["config"]["sync"] = sync_config;
    }

    auto create_mongo_service_resp = services.post_json(std::move(mongo_service_def));
    std::string mongo_service_id = create_mongo_service_resp["_id"];

    auto default_rule = services[mongo_service_id]["default_rule"];
    auto service_roles = nlohmann::json::array();
    if (config.service_roles.empty()) {
        service_roles.push_back(transform_service_role({"default"}));
    }
    else {
        std::transform(config.service_roles.begin(), config.service_roles.end(), std::back_inserter(service_roles),
                       transform_service_role);
    }

    default_rule.post_json({{"roles", service_roles}});

    // No need for a draft because there are no breaking changes in the initial schema when the app is created.
    bool use_draft = false;
    session.create_schema(app_id, config, use_draft);

    // Enable sync after schema is created.
    AdminAPISession::ServiceConfig service_config;
    service_config.database_name = sync_config["database_name"];
    if (config.flx_sync_config) {
        service_config.mode = AdminAPISession::ServiceConfig::SyncMode::Flexible;
        service_config.queryable_field_names = sync_config["queryable_fields_names"];
        service_config.asymmetric_tables = sync_config["asymmetric_tables"];
    }
    else {
        service_config.mode = AdminAPISession::ServiceConfig::SyncMode::Partitioned;
        service_config.partition = sync_config["partition"];
    }
    session.enable_sync(app_id, mongo_service_id, service_config);

    app["sync"]["config"].put_json({{"development_mode_enabled", config.dev_mode_enabled}});

    auto rules = services[mongo_service_id]["rules"];
    rules.post_json({
        {"database", config.mongo_dbname},
        {"collection", "UserData"},
        {"roles",
         {{{"name", "default"},
           {"apply_when", nlohmann::json::object()},
           {"document_filters",
            {
                {"read", true},
                {"write", true},
            }},
           {"read", true},
           {"write", true},
           {"insert", true},
           {"delete", true}}}},
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

    // Wait for initial sync to complete, as connecting while this is happening
    // causes various problems
    bool any_sync_types = std::any_of(config.schema.begin(), config.schema.end(), [](auto& object_schema) {
        return object_schema.table_type == ObjectSchema::ObjectType::TopLevel;
    });
    if (any_sync_types) {
        // Increasing timeout due to occasional slow startup of the translator on baasaas
        timed_sleeping_wait_for(
            [&] {
                return session.is_initial_sync_complete(app_id, config.flx_sync_config.has_value());
            },
            std::chrono::seconds(60), std::chrono::seconds(1));
    }

    return {client_app_id, app_id, session, config};
}

AppSession get_runtime_app_session()
{
    static const AppSession cached_app_session = [&] {
        auto cached_app_session = create_app(default_app_config());
        return cached_app_session;
    }();
    return cached_app_session;
}


#ifdef REALM_MONGODB_ENDPOINT
TEST_CASE("app: baas admin api", "[sync][app][admin api][baas]") {
    SECTION("embedded objects") {
        Schema schema{{"top",
                       {{"_id", PropertyType::String, true},
                        {"location", PropertyType::Object | PropertyType::Nullable, "location"}}},
                      {"location",
                       ObjectSchema::ObjectType::Embedded,
                       {{"coordinates", PropertyType::Double | PropertyType::Array}}}};

        auto test_app_config = minimal_app_config("test", schema);
        create_app(test_app_config);
    }

    SECTION("embedded object with array") {
        Schema schema{
            {"a",
             {{"_id", PropertyType::String, true},
              {"b_link", PropertyType::Object | PropertyType::Array | PropertyType::Nullable, "b"}}},
            {"b",
             ObjectSchema::ObjectType::Embedded,
             {{"c_link", PropertyType::Object | PropertyType::Nullable, "c"}}},
            {"c", {{"_id", PropertyType::String, true}, {"d_str", PropertyType::String}}},
        };
        auto test_app_config = minimal_app_config("test", schema);
        create_app(test_app_config);
    }

    SECTION("dictionaries") {
        Schema schema{
            {"a", {{"_id", PropertyType::String, true}, {"b_dict", PropertyType::Dictionary | PropertyType::String}}},
        };

        auto test_app_config = minimal_app_config("test", schema);
        create_app(test_app_config);
    }

    SECTION("set") {
        Schema schema{
            {"a", {{"_id", PropertyType::String, true}, {"b_dict", PropertyType::Set | PropertyType::String}}},
        };

        auto test_app_config = minimal_app_config("test", schema);
        create_app(test_app_config);
    }
}
#endif

} // namespace realm

#endif
