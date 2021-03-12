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

#ifndef REALM_TEST_UTILS_HPP
#define REALM_TEST_UTILS_HPP

#include <catch2/catch.hpp>
#include <realm/util/file.hpp>

#include <functional>
#if REALM_ENABLE_AUTH_TESTS
#include <curl/curl.h>
#include <realm/object-store/sync/app.hpp>
#endif

namespace realm {

/// Open a Realm at a given path, creating its files.
bool create_dummy_realm(std::string path);
void reset_test_directory(const std::string& base_path);
std::vector<char> make_test_encryption_key(const char start = 0);
void catch2_ensure_section_run_workaround(bool did_run_a_section, std::string section_name,
                                          std::function<void()> func);

std::string encode_fake_jwt(const std::string& in);
static inline std::string random_string(std::string::size_type length)
{
    static auto& chrs = "abcdefghijklmnopqrstuvwxyz"
                        "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    thread_local static std::mt19937 rg{std::random_device{}()};
    thread_local static std::uniform_int_distribution<std::string::size_type> pick(0, sizeof(chrs) - 2);
    std::string s;
    s.reserve(length);
    while (length--)
        s += chrs[pick(rg)];
    return s;
}

#if REALM_ENABLE_AUTH_TESTS

using namespace realm::app;

class IntTestTransport : public GenericNetworkTransport {
public:
    IntTestTransport()
    {
        curl_global_init(CURL_GLOBAL_ALL);
    }

    ~IntTestTransport()
    {
        curl_global_cleanup();
    }

    static size_t write(char* ptr, size_t size, size_t nmemb, std::string* data)
    {
        REALM_ASSERT(data);
        size_t realsize = size * nmemb;
        data->append(ptr, realsize);
        return realsize;
    }
    static size_t header_callback(char* buffer, size_t size, size_t nitems,
                                  std::map<std::string, std::string>* headers_storage)
    {
        REALM_ASSERT(headers_storage);
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
            headers_storage->insert({key, value});
        }
        else {
            if (combined.size() > 5 && combined.substr(0, 5) != "HTTP/") { // ignore for now HTTP/1.1 ...
                std::cerr << "test transport skipping header: " << combined << std::endl;
            }
        }
        return nitems * size;
    }

    void send_request_to_server(const Request request, std::function<void(const Response)> completion_block) override
    {
        CURL* curl;
        CURLcode response_code;
        std::string response;
        std::map<std::string, std::string> response_headers;

        /* get a curl handle */
        curl = curl_easy_init();

        struct curl_slist* list = NULL;

        if (curl) {
            /* First set the URL that is about to receive our POST. This URL can
             just as well be a https:// URL if that is what should receive the
             data. */
            curl_easy_setopt(curl, CURLOPT_URL, request.url.c_str());

            /* Now specify the POST data */
            if (request.method == HttpMethod::post) {
                curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request.body.c_str());
            }
            else if (request.method == HttpMethod::put) {
                curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
                curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request.body.c_str());
            }
            else if (request.method == HttpMethod::del) {
                curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
                curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request.body.c_str());
            }

            curl_easy_setopt(curl, CURLOPT_TIMEOUT, request.timeout_ms);

            for (auto header : request.headers) {
                std::stringstream h;
                h << header.first << ": " << header.second;
                list = curl_slist_append(list, h.str().data());
            }

            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, list);
            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write);
            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
            curl_easy_setopt(curl, CURLOPT_HEADERDATA, &response_headers);
            curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, header_callback);

            /* Perform the request, res will get the return code */
            response_code = curl_easy_perform(curl);
            int http_code = 0;
            curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

            /* Check for errors */
            if (response_code != CURLE_OK)
                fprintf(stderr, "curl_easy_perform() failed when sending request to '%s' with body '%s': %s\n",
                        request.url.c_str(), request.body.c_str(), curl_easy_strerror(response_code));

            /* always cleanup */
            curl_easy_cleanup(curl);
            curl_slist_free_all(list); /* free the list again */
            int binding_response_code = 0;
            completion_block(Response{http_code, binding_response_code, response_headers, response});
        }

        curl_global_cleanup();
    }
};

// When a stitch instance starts up and imports the app at this config location,
// it will generate a new app_id and write it back to the config. This is why we
// need to parse it at runtime after spinning up the instance.
static inline std::string get_runtime_app_id(std::string config_path)
{
    static std::string cached_app_id;
    if (cached_app_id.empty()) {
        util::File config(config_path);
        std::string contents;
        contents.resize(config.get_size());
        config.read(contents.data(), config.get_size());
        nlohmann::json json;
        json = nlohmann::json::parse(contents);
        cached_app_id = json["app_id"].get<std::string>();
        std::cout << "found app_id: " << cached_app_id << " in stitch config" << std::endl;
    }
    return cached_app_id;
}

#ifdef REALM_MONGODB_ENDPOINT
static inline std::string get_base_url()
{
    // allows configuration with or without quotes
    std::string base_url = REALM_QUOTE(REALM_MONGODB_ENDPOINT);
    if (base_url.size() > 0 && base_url[0] == '"') {
        base_url.erase(0, 1);
    }
    if (base_url.size() > 0 && base_url[base_url.size() - 1] == '"') {
        base_url.erase(base_url.size() - 1);
    }
    return base_url;
}
#endif
#ifdef REALM_STITCH_CONFIG
static inline std::string get_config_path()
{
    std::string config_path = REALM_QUOTE(REALM_STITCH_CONFIG);
    if (config_path.size() > 0 && config_path[0] == '"') {
        config_path.erase(0, 1);
    }
    if (config_path.size() > 0 && config_path[config_path.size() - 1] == '"') {
        config_path.erase(config_path.size() - 1);
    }
    return config_path;
}
#endif
#endif // REALM_ENABLE_AUTH_TESTS
} // namespace realm

#define REQUIRE_DIR_EXISTS(macro_path)                                                                               \
    do {                                                                                                             \
        CHECK(util::File::is_dir(macro_path) == true);                                                               \
    } while (0)

#define REQUIRE_DIR_DOES_NOT_EXIST(macro_path)                                                                       \
    do {                                                                                                             \
        CHECK(util::File::exists(macro_path) == false);                                                              \
    } while (0)

#define REQUIRE_REALM_EXISTS(macro_path)                                                                             \
    do {                                                                                                             \
        REQUIRE(util::File::exists(macro_path));                                                                     \
        REQUIRE(util::File::exists((macro_path) + ".lock"));                                                         \
        REQUIRE_DIR_EXISTS((macro_path) + ".management");                                                            \
    } while (0)

#define REQUIRE_REALM_DOES_NOT_EXIST(macro_path)                                                                     \
    do {                                                                                                             \
        REQUIRE(!util::File::exists(macro_path));                                                                    \
        REQUIRE(!util::File::exists((macro_path) + ".lock"));                                                        \
        REQUIRE_DIR_DOES_NOT_EXIST((macro_path) + ".management");                                                    \
    } while (0)

#define REQUIRE_THROWS_CONTAINING(expr, msg) REQUIRE_THROWS_WITH(expr, Catch::Matchers::Contains(msg))

#define ENCODE_FAKE_JWT(in) realm::encode_fake_jwt(in)

#endif // REALM_TEST_UTILS_HPP
