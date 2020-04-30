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

#include "catch2/catch.hpp"
#include "sync/app.hpp"
#include "sync/app_credentials.hpp"
#include "sync/remote_mongo_client.hpp"
#include "sync/remote_mongo_database.hpp"

#include "util/test_utils.hpp"
#include "util/test_file.hpp"

#include <curl/curl.h>
#include <json.hpp>
#include <thread>

using namespace realm;
using namespace realm::app;

// temporarily disable these tests for now,
// but allow opt-in by building with REALM_ENABLE_AUTH_TESTS=1
#ifndef REALM_ENABLE_AUTH_TESTS
#define REALM_ENABLE_AUTH_TESTS 0
#endif

static std::string random_string(std::string::size_type length)
{
    static auto& chrs = 
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    thread_local static std::mt19937 rg{std::random_device{}()};
    thread_local static std::uniform_int_distribution<std::string::size_type> pick(0, sizeof(chrs) - 2);
    std::string s;
    s.reserve(length);
    while(length--)
        s += chrs[pick(rg)];
    return s;
}

#if REALM_ENABLE_AUTH_TESTS

// When a stitch instance starts up and imports the app at this config location,
// it will generate a new app_id and write it back to the config. This is why we
// need to parse it at runtime after spinning up the instance.
static std::string get_runtime_app_id(std::string config_path)
{
    static std::string cached_app_id;
    if (cached_app_id.empty()) {
        File config(config_path);
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

class IntTestTransport : public GenericNetworkTransport {
public:
    
    IntTestTransport() {
        curl_global_init(CURL_GLOBAL_ALL);
    }
    
    ~IntTestTransport() {
        curl_global_cleanup();
    }
    
    static size_t write(char *ptr, size_t size, size_t nmemb, std::string* data) {
        REALM_ASSERT(data);
        size_t realsize = size * nmemb;
        data->append(ptr, realsize);
        return realsize;
    }
    static size_t header_callback(char *buffer, size_t size, size_t nitems, std::map<std::string, std::string> *headers_storage)
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
        } else {
            if (combined.size() > 5 && combined.substr(0, 5) != "HTTP/") { // ignore for now HTTP/1.1 ...
                std::cerr << "test transport skipping header: " << combined << std::endl;
            }
        }
        return nitems * size;
    }

    void send_request_to_server(const Request request, std::function<void (const Response)> completion_block) override
    {
        CURL *curl;
        CURLcode response_code;
        std::string response;
        std::map<std::string, std::string> response_headers;
        
        /* get a curl handle */
        curl = curl_easy_init();

        struct curl_slist *list = NULL;

        if (curl) {
            /* First set the URL that is about to receive our POST. This URL can
             just as well be a https:// URL if that is what should receive the
             data. */
            curl_easy_setopt(curl, CURLOPT_URL, request.url.c_str());

            /* Now specify the POST data */
            if (request.method == HttpMethod::post) {
                curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request.body.c_str());
            } else if (request.method == HttpMethod::put) {
                curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
            } else if (request.method == HttpMethod::del) {
                curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
            }

            curl_easy_setopt(curl, CURLOPT_TIMEOUT, request.timeout_ms);

            for (auto header : request.headers)
            {
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
            curl_easy_getinfo (curl, CURLINFO_RESPONSE_CODE, &http_code);

            /* Check for errors */
            if(response_code != CURLE_OK)
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

#ifdef REALM_MONGODB_ENDPOINT
static std::string get_base_url() {
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
static std::string get_config_path() {
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

TEST_CASE("app: login_with_credentials integration", "[sync][app]") {

    SECTION("login") {
        std::unique_ptr<GenericNetworkTransport> (*factory)() = []{
            return std::unique_ptr<GenericNetworkTransport>(new IntTestTransport);
        };

        std::string base_url = get_base_url();
        std::string config_path = get_config_path();
        std::cout << "base_url for [app] integration tests is set to: " << base_url << std::endl;
        std::cout << "config_path for [app] integration tests is set to: " << config_path << std::endl;
        REQUIRE(!base_url.empty());
        REQUIRE(!config_path.empty());

        // this app id is configured in tests/mongodb/stitch.json
        auto app = App(App::Config{get_runtime_app_id(config_path), factory, base_url});

        bool processed = false;

        static const std::string base_path = realm::tmp_dir();
        auto tsm = TestSyncManager(base_path);

        app.log_in_with_credentials(AppCredentials::anonymous(),
                                   [&](std::shared_ptr<SyncUser> user, Optional<app::AppError> error) {
            if (error) {
                std::cerr << "login_with_credentials failed: "
                    << error->message << " error_code: "
                    << error->error_code.message() << " (value: "
                    << error->error_code.value() << ")" <<std::endl;
            }
            CHECK(user);
            CHECK(!error);
        });

        app.log_out([&](auto error) {
            CHECK(!error);
            processed = true;
        });
        CHECK(processed);
    }
}

// MARK: - UsernamePasswordProviderClient Tests

TEST_CASE("app: UsernamePasswordProviderClient integration", "[sync][app]") {
    auto email = util::format("realm_tests_do_autoverify%1@%2.com", random_string(10), random_string(10));

    auto password = random_string(10);

    std::unique_ptr<GenericNetworkTransport> (*factory)() = []{
        return std::unique_ptr<GenericNetworkTransport>(new IntTestTransport);
    };
    std::string base_url = get_base_url();
    std::string config_path = get_config_path();
    REQUIRE(!base_url.empty());
    REQUIRE(!config_path.empty());
    auto config = App::Config{get_runtime_app_id(config_path), factory, base_url};
    auto app = App(config);
    std::string base_path = tmp_dir() + "/" + config.app_id;
    reset_test_directory(base_path);
    TestSyncManager init_sync_manager(base_path);

    bool processed = false;

    app.provider_client<App::UsernamePasswordProviderClient>()
    .register_email(email,
                    password,
                    [&](Optional<app::AppError> error) {
                        CHECK(!error); // first registration success
                        if (error) {
                            std::cout << "register failed for email: " << email << " pw: " << password << " message: " << error->error_code.message() << "+" << error->message << std::endl;
                        }
                    });

    SECTION("double registration should fail") {
        app.provider_client<App::UsernamePasswordProviderClient>()
            .register_email(email,
                            password,
                            [&](Optional<app::AppError> error) {
                // Error returned states the account has already been created
                REQUIRE(error);
                CHECK(error->message == "name already in use");
                CHECK(app::ServiceErrorCode(error->error_code.value()) == app::ServiceErrorCode::account_name_in_use);
                processed = true;
        });
        CHECK(processed);
    }

    SECTION("double registration should fail") {
        // the server registration function will reject emails that do not contain "realm_tests_do_autoverify"
        std::string email_to_reject = util::format("%1@%2.com", random_string(10), random_string(10));
        app.provider_client<App::UsernamePasswordProviderClient>()
            .register_email(email_to_reject,
                password,
                [&](Optional<app::AppError> error) {
                    REQUIRE(error);
                    CHECK(error->message == util::format("failed to confirm user %1", email_to_reject));
                    CHECK(app::ServiceErrorCode(error->error_code.value()) == app::ServiceErrorCode::bad_request);
                    processed = true;
                });
        CHECK(processed);
    }

    SECTION("can login with registered account") {
        app.log_in_with_credentials(realm::app::AppCredentials::username_password(email, password),
            [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
                REQUIRE(user);
                CHECK(!error);
                processed = true;
            });
        CHECK(processed);
        processed = false;
        auto user = app.current_user();
        REQUIRE(user);
        CHECK(user->user_profile().email == email);
    }

    SECTION("confirm user") {
        app.provider_client<App::UsernamePasswordProviderClient>()
            .confirm_user("a_token",
                          "a_token_id",
                          [&](Optional<app::AppError> error) {
                REQUIRE(error);
                CHECK(error->message == "invalid token data");
                processed = true;
        });
        CHECK(processed);
    }

    SECTION("resend confirmation email") {
        app.provider_client<App::UsernamePasswordProviderClient>()
            .resend_confirmation_email(email,
                                       [&](Optional<app::AppError> error) {
                REQUIRE(error);
                CHECK(error->message == "already confirmed");
                processed = true;
        });
        CHECK(processed);
    }

    SECTION("reset password invalid tokens") {
        app.provider_client<App::UsernamePasswordProviderClient>()
            .reset_password(password,
                            "token_sample",
                            "token_id_sample",
                            [&](Optional<app::AppError> error) {
                REQUIRE(error);
                CHECK(error->message == "invalid token data");
                processed = true;
        });
        CHECK(processed);
    }

    SECTION("reset password function success") {
        // the imported test app will accept password reset if the password contains "realm_tests_do_reset" via a function
        std::string accepted_new_password = util::format("realm_tests_do_reset%1", random_string(10));
        app.provider_client<App::UsernamePasswordProviderClient>()
            .call_reset_password_function(email,
                accepted_new_password,
                "[]",
                [&](Optional<app::AppError> error) {
                    REQUIRE(!error);
                    processed = true;
            });
        CHECK(processed);
    }

    SECTION("reset password function failure") {
        std::string rejected_password = util::format("%1", random_string(10));
        app.provider_client<App::UsernamePasswordProviderClient>()
            .call_reset_password_function(email,
                rejected_password,
                "[\"foo\", \"bar\"]",
                [&](Optional<app::AppError> error) {
                    REQUIRE(error);
                    CHECK(error->message == util::format("failed to reset password for user %1", email));
                    CHECK(error->is_service_error());
                    processed = true;
                });
        CHECK(processed);
    }

    SECTION("reset password function for invalid user fails") {
        app.provider_client<App::UsernamePasswordProviderClient>()
            .call_reset_password_function(util::format("%1@%2.com", random_string(5), random_string(5)),
                password,
                "[\"foo\", \"bar\"]",
                [&](Optional<app::AppError> error) {
                    REQUIRE(error);
                    CHECK(error->message == "user not found");
                    CHECK(error->is_service_error());
                    CHECK(app::ServiceErrorCode(error->error_code.value()) == app::ServiceErrorCode::user_not_found);
                    processed = true;
                });
        CHECK(processed);
    }
}

// MARK: - UserAPIKeyProviderClient Tests

TEST_CASE("app: UserAPIKeyProviderClient integration", "[sync][app]") {

    std::unique_ptr<GenericNetworkTransport> (*factory)() = []{
        return std::unique_ptr<GenericNetworkTransport>(new IntTestTransport);
    };
    std::string base_url = get_base_url();
    std::string config_path = get_config_path();
    REQUIRE(!base_url.empty());
    REQUIRE(!config_path.empty());
    auto config = App::Config{get_runtime_app_id(config_path), factory, base_url};
    auto app = App(config);
    std::string base_path = tmp_dir() + "/" + config.app_id;
    reset_test_directory(base_path);
    TestSyncManager init_sync_manager(base_path);

    bool processed = false;

    auto register_and_log_in_user = [&]() -> std::shared_ptr<SyncUser> {
        auto email = util::format("realm_tests_do_autoverify%1@%2.com", random_string(10), random_string(10));
        auto password = util::format("%1", random_string(15));
        app.provider_client<App::UsernamePasswordProviderClient>()
            .register_email(email,
                password,
                [&](Optional<app::AppError> error) {
                    CHECK(!error); // first registration should succeed
                    if (error) {
                        std::cout << "register failed for email: " << email << " pw: " << password << " message: " << error->error_code.message() << "+" << error->message << std::endl;
                    }
                });
        std::shared_ptr<SyncUser> logged_in_user;
        app.log_in_with_credentials(realm::app::AppCredentials::username_password(email, password),
            [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
                REQUIRE(user);
                CHECK(!error);
                logged_in_user = user;
                processed = true;
            });
        CHECK(processed);
        processed = false;
        return logged_in_user;
    };

    App::UserAPIKey api_key;

    SECTION("api-key") {
        std::shared_ptr<SyncUser> logged_in_user = register_and_log_in_user();
        auto api_key_name = util::format("%1", random_string(15));
        app.provider_client<App::UserAPIKeyProviderClient>()
            .create_api_key(api_key_name, logged_in_user,
                            [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                CHECK(!error);
                CHECK(user_api_key.name == api_key_name);
                api_key = user_api_key;
        });

        app.provider_client<App::UserAPIKeyProviderClient>()
            .fetch_api_key(api_key.id, logged_in_user,
                           [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                CHECK(!error);
                CHECK(user_api_key.name == api_key_name);
                CHECK(user_api_key.id == api_key.id);
        });

        app.provider_client<App::UserAPIKeyProviderClient>()
            .fetch_api_keys(logged_in_user,
                            [&](std::vector<App::UserAPIKey> api_keys, Optional<AppError> error) {
                CHECK(api_keys.size() == 1);
                for(auto key : api_keys) {
                    CHECK(key.id.to_string() == api_key.id.to_string());
                    CHECK(api_key.name == api_key_name);
                    CHECK(key.id == api_key.id);
                }
                CHECK(!error);
        });

        app.provider_client<App::UserAPIKeyProviderClient>()
            .enable_api_key(api_key.id, logged_in_user, [&](Optional<AppError> error) {
                CHECK(!error);
        });

        app.provider_client<App::UserAPIKeyProviderClient>()
            .fetch_api_key(api_key.id, logged_in_user,
                           [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                CHECK(!error);
                CHECK(user_api_key.disabled == false);
                CHECK(user_api_key.name == api_key_name);
                CHECK(user_api_key.id == api_key.id);
        });

        app.provider_client<App::UserAPIKeyProviderClient>()
            .disable_api_key(api_key.id, logged_in_user, [&](Optional<AppError> error) {
                CHECK(!error);
        });

        app.provider_client<App::UserAPIKeyProviderClient>()
            .fetch_api_key(api_key.id, logged_in_user, [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                CHECK(!error);
                CHECK(user_api_key.disabled == true);
                CHECK(user_api_key.name == api_key_name);
        });

        app.provider_client<App::UserAPIKeyProviderClient>()
            .delete_api_key(api_key.id, logged_in_user, [&](Optional<AppError> error) {
                CHECK(!error);
        });

        app.provider_client<App::UserAPIKeyProviderClient>()
            .fetch_api_key(api_key.id, logged_in_user, [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                CHECK(user_api_key.name == "");
                CHECK(error);
                processed = true;
        });

        CHECK(processed);
    }

    SECTION("api-key without a user") {
        std::shared_ptr<SyncUser> no_user = nullptr;
        auto api_key_name = util::format("%1", random_string(15));
        app.provider_client<App::UserAPIKeyProviderClient>()
            .create_api_key(api_key_name, no_user,
                            [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                                REQUIRE(error);
                                CHECK(error->is_service_error());
                                CHECK(error->message == "must authenticate first");
                                CHECK(user_api_key.name == "");
                            });

        app.provider_client<App::UserAPIKeyProviderClient>()
            .fetch_api_key(api_key.id, no_user,
                           [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                               REQUIRE(error);
                               CHECK(error->is_service_error());
                               CHECK(error->message == "must authenticate first");
                               CHECK(user_api_key.name == "");
                           });

        app.provider_client<App::UserAPIKeyProviderClient>()
            .fetch_api_keys(no_user,
                            [&](std::vector<App::UserAPIKey> api_keys, Optional<AppError> error) {
                                REQUIRE(error);
                                CHECK(error->is_service_error());
                                CHECK(error->message == "must authenticate first");
                                CHECK(api_keys.size() == 0);
                            });

        app.provider_client<App::UserAPIKeyProviderClient>()
            .enable_api_key(api_key.id, no_user,
                            [&](Optional<AppError> error) {
                                REQUIRE(error);
                                CHECK(error->is_service_error());
                                CHECK(error->message == "must authenticate first");
                            });

        app.provider_client<App::UserAPIKeyProviderClient>()
            .fetch_api_key(api_key.id, no_user,
                           [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                               REQUIRE(error);
                               CHECK(error->is_service_error());
                               CHECK(error->message == "must authenticate first");
                               CHECK(user_api_key.name == "");
                           });

        app.provider_client<App::UserAPIKeyProviderClient>()
            .disable_api_key(api_key.id, no_user,
                             [&](Optional<AppError> error) {
                                 REQUIRE(error);
                                 CHECK(error->is_service_error());
                                 CHECK(error->message == "must authenticate first");
                             });

        app.provider_client<App::UserAPIKeyProviderClient>()
            .fetch_api_key(api_key.id, no_user,
                           [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                               REQUIRE(error);
                               CHECK(error->is_service_error());
                               CHECK(error->message == "must authenticate first");
                               CHECK(user_api_key.name == "");
                           });

        app.provider_client<App::UserAPIKeyProviderClient>()
            .delete_api_key(api_key.id, no_user,
                            [&](Optional<AppError> error) {
                                REQUIRE(error);
                                CHECK(error->is_service_error());
                                CHECK(error->message == "must authenticate first");
                            });

        app.provider_client<App::UserAPIKeyProviderClient>()
            .fetch_api_key(api_key.id, no_user,
                           [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                               CHECK(user_api_key.name == "");
                               REQUIRE(error);
                               CHECK(error->is_service_error());
                               CHECK(error->message == "must authenticate first");
                               processed = true;
                           });
        CHECK(processed);
    }

    SECTION("api-key against the wrong user") {
        std::shared_ptr<SyncUser> first_user = register_and_log_in_user();
        std::shared_ptr<SyncUser> second_user = register_and_log_in_user();
        auto api_key_name = util::format("%1", random_string(15));
        App::UserAPIKey api_key;
        App::UserAPIKeyProviderClient provider = app.provider_client<App::UserAPIKeyProviderClient>();

        provider.create_api_key(api_key_name, first_user,
                        [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                            CHECK(!error);
                            CHECK(user_api_key.name == api_key_name);
                            api_key = user_api_key;
                        });

        provider.fetch_api_key(api_key.id, first_user,
                       [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                           CHECK(!error);
                           CHECK(user_api_key.name == api_key_name);
                           CHECK(user_api_key.id.to_string() == user_api_key.id.to_string());
                       });

        provider.fetch_api_key(api_key.id, second_user,
                       [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                           REQUIRE(error);
                           CHECK(error->message == "API key not found");
                           CHECK(error->is_service_error());
                           CHECK(app::ServiceErrorCode(error->error_code.value()) == app::ServiceErrorCode::api_key_not_found);
                           CHECK(user_api_key.name == "");
                       });

        provider.fetch_api_keys(first_user,
                        [&](std::vector<App::UserAPIKey> api_keys, Optional<AppError> error) {
                            CHECK(api_keys.size() == 1);
                            for(auto api_key : api_keys) {
                                CHECK(api_key.name == api_key_name);
                            }
                            CHECK(!error);
                        });

        provider.fetch_api_keys(second_user,
                                [&](std::vector<App::UserAPIKey> api_keys, Optional<AppError> error) {
                                    CHECK(api_keys.size() == 0);
                                    CHECK(!error);
                                });

        provider.enable_api_key(api_key.id, first_user, [&](Optional<AppError> error) {
            CHECK(!error);
        });

        provider.enable_api_key(api_key.id, second_user, [&](Optional<AppError> error) {
            REQUIRE(error);
            CHECK(error->message == "API key not found");
            CHECK(error->is_service_error());
            CHECK(app::ServiceErrorCode(error->error_code.value()) == app::ServiceErrorCode::api_key_not_found);
        });

        provider.fetch_api_key(api_key.id, first_user,
                       [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                           CHECK(!error);
                           CHECK(user_api_key.disabled == false);
                           CHECK(user_api_key.name == api_key_name);
                       });

        provider.fetch_api_key(api_key.id, second_user,
                               [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
                                   REQUIRE(error);
                                   CHECK(user_api_key.name == "");
                                   CHECK(error->message == "API key not found");
                                   CHECK(error->is_service_error());
                                   CHECK(app::ServiceErrorCode(error->error_code.value()) == app::ServiceErrorCode::api_key_not_found);
                               });

        provider.disable_api_key(api_key.id, first_user, [&](Optional<AppError> error) {
            CHECK(!error);
        });

        provider.disable_api_key(api_key.id, second_user, [&](Optional<AppError> error) {
            REQUIRE(error);
            CHECK(error->message == "API key not found");
            CHECK(error->is_service_error());
            CHECK(app::ServiceErrorCode(error->error_code.value()) == app::ServiceErrorCode::api_key_not_found);
        });

        provider.fetch_api_key(api_key.id, first_user, [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
            CHECK(!error);
            CHECK(user_api_key.disabled == true);
            CHECK(user_api_key.name == api_key_name);
        });

        provider.fetch_api_key(api_key.id, second_user, [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
            REQUIRE(error);
            CHECK(user_api_key.name == "");
            CHECK(error->message == "API key not found");
            CHECK(error->is_service_error());
            CHECK(app::ServiceErrorCode(error->error_code.value()) == app::ServiceErrorCode::api_key_not_found);
        });

        provider.delete_api_key(api_key.id, second_user, [&](Optional<AppError> error) {
            REQUIRE(error);
            CHECK(error->message == "API key not found");
            CHECK(error->is_service_error());
            CHECK(app::ServiceErrorCode(error->error_code.value()) == app::ServiceErrorCode::api_key_not_found);
        });

        provider.delete_api_key(api_key.id, first_user, [&](Optional<AppError> error) {
            CHECK(!error);
        });

        provider.fetch_api_key(api_key.id, first_user, [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
            CHECK(user_api_key.name == "");
            REQUIRE(error);
            CHECK(error->message == "API key not found");
            CHECK(error->is_service_error());
            CHECK(app::ServiceErrorCode(error->error_code.value()) == app::ServiceErrorCode::api_key_not_found);
            processed = true;
        });

        provider.fetch_api_key(api_key.id, second_user, [&](App::UserAPIKey user_api_key, Optional<app::AppError> error) {
            CHECK(user_api_key.name == "");
            REQUIRE(error);
            CHECK(error->message == "API key not found");
            CHECK(error->is_service_error());
            CHECK(app::ServiceErrorCode(error->error_code.value()) == app::ServiceErrorCode::api_key_not_found);
            processed = true;
        });

        CHECK(processed);
    }

}

TEST_CASE("app: auth providers function integration", "[sync][app]") {
    
    std::unique_ptr<GenericNetworkTransport> (*factory)() = []{
        return std::unique_ptr<GenericNetworkTransport>(new IntTestTransport);
    };
    std::string base_url = get_base_url();
    std::string config_path = get_config_path();
    REQUIRE(!base_url.empty());
    REQUIRE(!config_path.empty());
    auto config = App::Config{get_runtime_app_id(config_path), factory, base_url};
    auto app = App(config);
    std::string base_path = tmp_dir() + "/" + config.app_id;
    reset_test_directory(base_path);
    TestSyncManager init_sync_manager(base_path);
    
    bool processed = false;
    
    SECTION("auth providers function integration") {
        
        auto credentials = realm::app::AppCredentials::function(nlohmann::json({{"realmCustomAuthFuncUserId", "123456"}}).dump());
        
        app.log_in_with_credentials(credentials,
                                    [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
                                        REQUIRE(user);
                                        CHECK(user->provider_type() == IdentityProviderFunction);
                                        CHECK(!error);
                                        processed = true;
                                    });
        
        CHECK(processed);
        
    }
    
}

TEST_CASE("app: link_user integration", "[sync][app]") {
    SECTION("link_user intergration") {
        
        auto email = util::format("realm_tests_do_autoverify%1@%2.com", random_string(10), random_string(10));

        auto password = random_string(10);

        std::unique_ptr<GenericNetworkTransport> (*factory)() = []{
            return std::unique_ptr<GenericNetworkTransport>(new IntTestTransport);
        };
        std::string base_url = get_base_url();
        std::string config_path = get_config_path();
        REQUIRE(!base_url.empty());
        REQUIRE(!config_path.empty());
        auto config = App::Config{get_runtime_app_id(config_path), factory, base_url};
        auto app = App(config);
        std::string base_path = tmp_dir() + "/" + config.app_id;
        reset_test_directory(base_path);
        TestSyncManager init_sync_manager(base_path);

        bool processed = false;

        std::shared_ptr<SyncUser> sync_user;
        
        auto email_pass_credentials = realm::app::AppCredentials::username_password(email, password);
        
        app.provider_client<App::UsernamePasswordProviderClient>()
        .register_email(email,
                        password,
                        [&](Optional<app::AppError> error) {
                            CHECK(!error); // first registration success
                            if (error) {
                                std::cout << "register failed for email: " << email << " pw: " << password << " message: " << error->error_code.message() << "+" << error->message << std::endl;
                            }
                        });

        app.log_in_with_credentials(realm::app::AppCredentials::anonymous(),
                                    [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
            REQUIRE(user);
            CHECK(!error);
            sync_user = user;
        });
        
        CHECK(sync_user->provider_type() == IdentityProviderAnonymous);

        app.link_user(sync_user,
                      email_pass_credentials,
                      [&](std::shared_ptr<SyncUser> user, Optional<app::AppError> error) {
            CHECK(!error);
            REQUIRE(user);
            CHECK(user->identity() == sync_user->identity());
            CHECK(user->identities().size() == 2);
            processed = true;
        });

        CHECK(processed);
    }
}

TEST_CASE("app: remote mongo client", "[sync][app]") {
    
    std::unique_ptr<GenericNetworkTransport> (*factory)() = []{
        return std::unique_ptr<GenericNetworkTransport>(new IntTestTransport);
    };
    std::string base_url = get_base_url();
    std::string config_path = get_config_path();
    REQUIRE(!base_url.empty());
    REQUIRE(!config_path.empty());
    auto config = App::Config{get_runtime_app_id(config_path), factory, base_url};
    auto app = App(config);
    std::string base_path = tmp_dir() + "/" + config.app_id;
    reset_test_directory(base_path);
    TestSyncManager init_sync_manager(base_path);
    
    auto remote_client = app.remote_mongo_client("BackingDB");
    auto db = remote_client.db("test_data");
    auto collection = db["Dog"];
    
    auto dog_document = "{\"name\":\"fido\", \"breed\":\"king charles\"}";
    auto dog_document2 = "{\"name\":\"fido\", \"breed\":\"french bulldog\"}";

    auto email = util::format("realm_tests_do_autoverify%1@%2.com", random_string(10), random_string(10));
    auto password = random_string(10);
    
    app.provider_client<App::UsernamePasswordProviderClient>()
    .register_email(email,
                    password,
                    [&](Optional<app::AppError> error) {
                        CHECK(!error);
                    });
    
    app.log_in_with_credentials(realm::app::AppCredentials::username_password(email, password),
                                [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
                                    REQUIRE(user);
                                    CHECK(!error);
                                });
    
    collection.delete_many(dog_document, [&](uint64_t, Optional<app::AppError> error) {
        CHECK(!error);
    });
    
    
    collection.delete_many(dog_document2, [&](uint64_t, Optional<app::AppError> error) {
        CHECK(!error);
    });
    
    SECTION("insert") {
        
        bool processed = false;
        std::string dog_object_id;
        
        collection.insert_one("{\"bad\" : \"value\"}",
                              [&](Optional<std::string> document, Optional<app::AppError> error) {
            CHECK(error);
            CHECK(!document);
        });
        
        collection.insert_one(dog_document,
                              [&](Optional<std::string> document_json, Optional<app::AppError> error) {
            CHECK(!error);
            auto json = nlohmann::json::parse(*document_json);
            auto object_id = json.at("insertedId").at("$oid").get<std::string>();
            dog_object_id = object_id;
        });
        
        auto documents = std::vector<std::string>();
        documents.push_back(dog_document2);
        
        collection.insert_many(documents,
                               [&](std::vector<std::string> inserted_docs,
                                   Optional<app::AppError> error) {
            CHECK(!error);
            CHECK(inserted_docs.size() == 1);
            processed = true;
        });
        
        CHECK(processed);
    }
    
    SECTION("find") {
        bool processed = false;
        
        collection.find(dog_document,
                        [&](Optional<std::string> document_json, Optional<app::AppError> error) {
            CHECK(!error);
            auto json = nlohmann::json::parse(*document_json);
            CHECK(json.is_array());
        });
        
        collection.find_one(dog_document,
                            [&](Optional<std::string> document_json, Optional<app::AppError> error) {
            CHECK(!error);
            CHECK(!document_json);
        });
        
        collection.insert_one(dog_document,
                              [&](Optional<std::string> document_json, Optional<app::AppError> error) {
            CHECK(!error);
            auto json = nlohmann::json::parse(*document_json);
            auto object_id = json.at("insertedId").at("$oid").get<std::string>();
            CHECK(object_id != "");
        });
        
        collection.find(dog_document,
                        [&](Optional<std::string> document_json, Optional<app::AppError> error) {
            CHECK(!error);
            auto json = nlohmann::json::parse(*document_json);
            CHECK(json.is_array());
            CHECK(json.size() == 1);
        });
        
        realm::app::RemoteMongoCollection::RemoteFindOptions options {
            1, //document limit
            util::Optional<std::string>(nlohmann::json({{"name", "fido"}}).dump()), //project
            util::Optional<std::string>(nlohmann::json({{"name", -1}}).dump()) //sort
        };
        
        collection.find(dog_document, options, [&](Optional<std::string> document_json, Optional<app::AppError> error) {
            CHECK(!error);
            auto json = nlohmann::json::parse(*document_json);
            CHECK(json.is_array());
            auto json_array = json.get<std::vector<nlohmann::json>>();
            CHECK(json_array.size() == 1);
        });
        
        collection.find_one(dog_document,
                            [&](Optional<std::string> document_json, Optional<app::AppError> error) {
            CHECK(!error);
            auto json = nlohmann::json::parse(*document_json);
            auto name = json.at("name").get<std::string>();
            CHECK(name == "fido");
        });
        
        collection.find_one(dog_document, options, [&](Optional<std::string> document_json, Optional<app::AppError> error) {
            CHECK(!error);
            auto json = nlohmann::json::parse(*document_json);
            auto name = json.at("name").get<std::string>();
            CHECK(name == "fido");
        });
        
        realm::app::RemoteMongoCollection::RemoteFindOneAndModifyOptions find_and_modify_options {
            util::Optional<std::string>(nlohmann::json({{"name", "fido"}}).dump()), //project
            util::Optional<std::string>(nlohmann::json({{"name", 1}}).dump()), //sort,
            true, //upsert
            true // return new doc
        };
        
        collection.find(dog_document, options, [&](Optional<std::string> document_json, Optional<app::AppError> error) {
            CHECK(!error);
            auto json = nlohmann::json::parse(*document_json);
            CHECK(json.is_array());
            auto json_array = json.get<std::vector<nlohmann::json>>();
            CHECK(json_array.size() == 1);
        });
        
        collection.find_one_and_delete(dog_document, find_and_modify_options, [&](Optional<app::AppError> error) {
            CHECK(!error);
        });
        
        collection.find(dog_document, options, [&](Optional<std::string> document_json, Optional<app::AppError> error) {
            CHECK(!error);
            auto json = nlohmann::json::parse(*document_json);
            CHECK(json.is_array());
            auto json_array = json.get<std::vector<nlohmann::json>>();
            CHECK(json_array.size() == 0);
            processed = true;
        });
        
        CHECK(processed);
    }
    
    SECTION("count and aggregate") {
        bool processed = false;
        
        collection.insert_one(dog_document,
                              [&](Optional<std::string> document_json, Optional<app::AppError> error) {
            CHECK(!error);
            auto json = nlohmann::json::parse(*document_json);
            auto object_id = json.at("insertedId").at("$oid").get<std::string>();
            CHECK(object_id != "");
        });
        
        collection.insert_one(dog_document,
                              [&](Optional<std::string> document_json, Optional<app::AppError> error) {
            CHECK(!error);
            auto json = nlohmann::json::parse(*document_json);
            auto object_id = json.at("insertedId").at("$oid").get<std::string>();
            CHECK(object_id != "");
        });
        
        std::vector<std::string> pipeline = {
            "{\"$match\": { \"name\": \"fido\"}}",
            "{\"$group\": { \"_id\": \"$name\"}}"
        };
        
        collection.aggregate(pipeline, [&](Optional<std::string> document_json, Optional<app::AppError> error) {
            CHECK(!error);
            auto json = nlohmann::json::parse(*document_json);
            CHECK(json.is_array());
        });
        
        collection.count("{\"breed\":\"king charles\"}", [&](uint64_t count, Optional<app::AppError> error) {
            CHECK(!error);
            CHECK(count >= 1);
        });
        
        collection.count("{\"breed\":\"king charles\"}", 1, [&](uint64_t count, Optional<app::AppError> error) {
            CHECK(!error);
            CHECK(count == 1);
            processed = true;
        });
        
        CHECK(processed);
    }
    
    SECTION("find and update") {
        
        bool processed = false;
        
        realm::app::RemoteMongoCollection::RemoteFindOneAndModifyOptions find_and_modify_options {
            util::Optional<std::string>(nlohmann::json({{"name", "fido"}}).dump()), //project
            util::Optional<std::string>(nlohmann::json({{"name", 1}}).dump()), //sort,
            true, //upsert
            true // return new doc
        };
        
        collection.find_one_and_update(dog_document, dog_document2, [&](Optional<std::string> document, Optional<app::AppError> error) {
            CHECK(!error);
            CHECK(!document);
        });
        
        collection.insert_one(dog_document,
                              [&](Optional<std::string> document_json, Optional<app::AppError> error) {
            CHECK(!error);
            auto json = nlohmann::json::parse(*document_json);
            auto object_id = json.at("insertedId").at("$oid").get<std::string>();
            CHECK(object_id != "");
        });
        
        collection.find_one_and_update(dog_document, dog_document2, [&](Optional<std::string> document, Optional<app::AppError> error) {
            CHECK(!error);
            auto json = nlohmann::json::parse(*document);
            auto breed = json.at("breed").get<std::string>();
            CHECK(breed == "king charles");
        });
        
        collection.find_one_and_update(dog_document, dog_document2, find_and_modify_options, [&](Optional<std::string> document, Optional<app::AppError> error) {
            CHECK(!error);
            auto json = nlohmann::json::parse(*document);
            auto breed = json.at("breed").get<std::string>();
            CHECK(breed == "french bulldog");
            processed = true;
        });
        
        CHECK(processed);
    }
    
    SECTION("update") {
        bool processed = false;
        
        collection.update_one(dog_document,
                              dog_document2,
                              true,
                              [&](realm::app::RemoteMongoCollection::RemoteUpdateResult result, Optional<app::AppError> error) {
            CHECK(!error);
            CHECK(result.upserted_id != "");
        });
        
        collection.update_one(dog_document2,
                              dog_document,
                              [&](realm::app::RemoteMongoCollection::RemoteUpdateResult result, Optional<app::AppError> error) {
            CHECK(!error);
            CHECK(result.upserted_id == "");
            processed = true;
        });
        
        CHECK(processed);
    }
    
    SECTION("update many") {
        bool processed = false;
        
        collection.insert_one(dog_document,
                              [&](Optional<std::string> document_json, Optional<app::AppError> error) {
            CHECK(!error);
            auto json = nlohmann::json::parse(*document_json);
            auto object_id = json.at("insertedId").at("$oid").get<std::string>();
            CHECK(object_id != "");
        });

        collection.update_many(dog_document, dog_document2, true, [&](realm::app::RemoteMongoCollection::RemoteUpdateResult result, Optional<app::AppError> error) {
            CHECK(!error);
            CHECK(result.upserted_id == "");
        });
        
        collection.update_many(dog_document2, dog_document, [&](realm::app::RemoteMongoCollection::RemoteUpdateResult result, Optional<app::AppError> error) {
            CHECK(!error);
            CHECK(result.upserted_id == "");
            processed = true;
        });
        
        CHECK(processed);
    }
    
    SECTION("find and replace") {
        bool processed = false;

        realm::app::RemoteMongoCollection::RemoteFindOneAndModifyOptions find_and_modify_options {
            util::Optional<std::string>(nlohmann::json({{ "name", "fido" }}).dump()), //project
            util::Optional<std::string>(nlohmann::json({{ "name", 1 }}).dump()), //sort,
            true, //upsert
            true // return new doc
        };
        
        collection.find_one_and_replace(dog_document,
                                        dog_document2,
                                        [&](Optional<std::string> document, Optional<app::AppError> error) {
            CHECK(!error);
            CHECK(!document);
        });
        
        collection.insert_one(dog_document,
                              [&](Optional<std::string> document_json, Optional<app::AppError> error) {
            CHECK(!error);
            auto json = nlohmann::json::parse(*document_json);
            auto object_id = json.at("insertedId").at("$oid").get<std::string>();
            CHECK(object_id != "");
        });
        
        collection.find_one_and_replace(dog_document,
                                        dog_document2,
                                        [&](Optional<std::string> document, Optional<app::AppError> error) {
            CHECK(!error);
            auto json = nlohmann::json::parse(*document);
            auto name = json.at("name").get<std::string>();
            CHECK(name == "fido");
        });
        
        collection.find_one_and_replace(dog_document,
                                        dog_document2,
                                        find_and_modify_options,
                                        [&](Optional<std::string> document, Optional<app::AppError> error) {
            CHECK(!error);
            auto json = nlohmann::json::parse(*document);
            auto name = json.at("name").get<std::string>();
            CHECK(name == "fido");
            processed = true;
        });
                
        CHECK(processed);
    }
    
    SECTION("delete") {
        
        bool processed = false;
        
        auto documents = std::vector<std::string>();
        documents.assign(3, dog_document);
        
        collection.insert_many(documents,
                               [&](std::vector<std::string> inserted_docs,
                                   Optional<app::AppError> error) {
                                   CHECK(!error);
                                   CHECK(inserted_docs.size() == 3);
                               });
        
        realm::app::RemoteMongoCollection::RemoteFindOneAndModifyOptions find_and_modify_options {
            util::Optional<std::string>(nlohmann::json({{ "name", "fido" }}).dump()), //project
            util::Optional<std::string>(nlohmann::json({{ "name", 1 }}).dump()), //sort,
            true, //upsert
            true // return new doc
        };
        
        collection.delete_one(dog_document, [&](uint64_t deleted_count,
                                                Optional<app::AppError> error) {
            CHECK(!error);
            CHECK(deleted_count >= 1);
        });
        
        collection.delete_many(dog_document, [&](uint64_t deleted_count,
                                                 Optional<app::AppError> error) {
            CHECK(!error);
            CHECK(deleted_count >= 1);
            processed = true;
        });
        
        CHECK(processed);
    }
}


#endif // REALM_ENABLE_AUTH_TESTS

class CustomErrorTransport : public GenericNetworkTransport {
public:
    CustomErrorTransport(int code, const std::string& message) : m_code(code), m_message(message) { }

    void send_request_to_server(const Request, std::function<void (const Response)> completion_block) override
    {
        completion_block(Response{0, m_code, std::map<std::string, std::string>(), m_message});
    }

private:
    int m_code;
    std::string m_message;
};

TEST_CASE("app: custom error handling", "[sync][app][custom_errors]") {
    SECTION("custom code and message is sent back") {
        std::unique_ptr<GenericNetworkTransport> (*factory)() = []{
            return std::unique_ptr<GenericNetworkTransport>(new CustomErrorTransport(1001, "Boom!"));
        };

        auto app = App(App::Config{"anything", factory});
        bool processed = false;
        app.log_in_with_credentials(AppCredentials::anonymous(),
            [&](std::shared_ptr<SyncUser> user, Optional<app::AppError> error) {
                CHECK(!user);
                CHECK(error);
                CHECK(error->is_custom_error());
                CHECK(error->error_code.value() == 1001);
                CHECK(error->message == "Boom!");
                processed = true;
            });
        CHECK(processed);
    }
}


static const std::string profile_0_name = "Ursus americanus Ursus boeckhi";
static const std::string profile_0_first_name = "Ursus americanus";
static const std::string profile_0_last_name = "Ursus boeckhi";
static const std::string profile_0_email = "Ursus ursinus";
static const std::string profile_0_picture_url = "Ursus malayanus";
static const std::string profile_0_gender = "Ursus thibetanus";
static const std::string profile_0_birthday = "Ursus americanus";
static const std::string profile_0_min_age = "Ursus maritimus";
static const std::string profile_0_max_age = "Ursus arctos";

static const nlohmann::json profile_0 = {
    {"name", profile_0_name},
    {"first_name", profile_0_first_name},
    {"last_name", profile_0_last_name},
    {"email", profile_0_email},
    {"picture_url", profile_0_picture_url},
    {"gender", profile_0_gender},
    {"birthday", profile_0_birthday},
    {"min_age", profile_0_min_age},
    {"max_age", profile_0_max_age}
};

static nlohmann::json user_json(std::string access_token, std::string user_id = random_string(15)) {
    return {
        {"access_token", access_token},
        {"refresh_token", access_token},
        {"user_id", user_id},
        {"device_id", "Panda Bear"}
    };
}

static nlohmann::json user_profile_json(std::string user_id = random_string(15),
                                        std::string identity_0_id = "Ursus arctos isabellinus",
                                        std::string identity_1_id = "Ursus arctos horribilis",
                                        std::string provider_type = "anon-user") {
    return {
        {"user_id", user_id},
        {"identities", {
            {
                {"id", identity_0_id},
                {"provider_type", provider_type},
                {"provider_id", "lol"}
            },
            {
                {"id", identity_1_id},
                {"provider_type", "lol_wut"},
                {"provider_id", "nah_dawg"}
            }
        }},
        {"data", profile_0}
    };
}

// MARK: - Unit Tests

class UnitTestTransport : public GenericNetworkTransport {

public:
    static std::string access_token;
    static std::string provider_type;
    static const std::string api_key;
    static const std::string api_key_id;
    static const std::string api_key_name;
    static const std::string auth_route;
    static const std::string user_id;
    static const std::string identity_0_id;
    static const std::string identity_1_id;

private:
    void handle_profile(const Request request,
                        std::function<void (Response)> completion_block)
    {
        CHECK(request.method == HttpMethod::get);
        CHECK(request.headers.at("Content-Type") == "application/json;charset=utf-8");
        CHECK(request.headers.at("Authorization") == "Bearer " + access_token);
        CHECK(request.body.empty());
        CHECK(request.timeout_ms == 60000);

        std::string response = nlohmann::json({
            {"user_id", user_id},
            {"identities", {
                {
                    {"id", identity_0_id},
                    {"provider_type", provider_type},
                    {"provider_id", "lol"}
                },
                {
                    {"id", identity_1_id},
                    {"provider_type", "lol_wut"},
                    {"provider_id", "nah_dawg"}
                }
            }},
            {"data", profile_0}
        }).dump();

        completion_block(Response{.http_status_code = 200, .custom_status_code = 0, .headers = {}, .body = response});
    }

    void handle_login(const Request request,
                      std::function<void (Response)> completion_block)
    {
        CHECK(request.method == HttpMethod::post);
        CHECK(request.headers.at("Content-Type") == "application/json;charset=utf-8");

        CHECK(nlohmann::json::parse(request.body) == nlohmann::json({{"provider", provider_type}}));
        CHECK(request.timeout_ms == 60000);

        std::string response = nlohmann::json({
            {"access_token", access_token},
            {"refresh_token", access_token},
            {"user_id", random_string(15)},
            {"device_id", "Panda Bear"}}).dump();
        
        completion_block(Response { .http_status_code = 200,
                                    .custom_status_code = 0,
                                    .headers = {},
                                    .body = response });
    }

    void handle_location(const Request request,
                         std::function<void (Response)> completion_block)
    {
        CHECK(request.method == HttpMethod::get);
        CHECK(request.timeout_ms == 60000);

        std::string response = nlohmann::json({
            {"deployment_model", "this"},
            {"hostname", "field"},
            {"ws_hostname", "shouldn't"},
            {"location", "matter"}}).dump();

        completion_block(Response { .http_status_code = 200,
                                    .custom_status_code = 0,
                                    .headers = {},
                                    .body = response });
    }

    void handle_create_api_key(const Request request,
                      std::function<void (Response)> completion_block)
    {
        CHECK(request.method == HttpMethod::post);
        CHECK(request.headers.at("Content-Type") == "application/json;charset=utf-8");
        CHECK(nlohmann::json::parse(request.body) == nlohmann::json({{"name", api_key_name}}));
        CHECK(request.timeout_ms == 60000);

        std::string response = nlohmann::json({
            {"_id", api_key_id},
            {"key", api_key},
            {"name", api_key_name},
            {"disabled", false}}).dump();

        completion_block(Response { .http_status_code = 200,
                                    .custom_status_code = 0,
                                    .headers = {},
                                    .body = response });
    }
    
    void handle_fetch_api_key(const Request request,
                      std::function<void (Response)> completion_block)
    {
        CHECK(request.method == HttpMethod::get);
        CHECK(request.headers.at("Content-Type") == "application/json;charset=utf-8");

        CHECK(request.body == "");
        CHECK(request.timeout_ms == 60000);
        
        std::string response = nlohmann::json({
            {"_id", api_key_id},
            {"name", api_key_name},
            {"disabled", false}}).dump();
        
        completion_block(Response { .http_status_code = 200,
                                    .custom_status_code = 0,
                                    .headers = {},
                                    .body = response });
    }
    
    void handle_fetch_api_keys(const Request request,
                      std::function<void (Response)> completion_block)
    {
        CHECK(request.method == HttpMethod::get);
        CHECK(request.headers.at("Content-Type") == "application/json;charset=utf-8");

        CHECK(request.body == "");
        CHECK(request.timeout_ms == 60000);
        
        auto elements = std::vector<nlohmann::json>();
        for (int i = 0; i < 2; i++) {
            elements.push_back({
                {"_id", api_key_id},
                {"name", api_key_name},
                {"disabled", false}});
        }
        
        completion_block(Response { .http_status_code = 200,
                                    .custom_status_code = 0,
                                    .headers = {},
                                    .body = nlohmann::json(elements).dump() });
    }
    
    void handle_token_refresh(const Request request,
                              std::function<void (Response)> completion_block) {
        CHECK(request.method == HttpMethod::post);
        CHECK(request.headers.at("Content-Type") == "application/json;charset=utf-8");

        CHECK(request.body == "");
        CHECK(request.timeout_ms == 60000);
        
        auto elements = std::vector<nlohmann::json>();
        nlohmann::json json {
            {"access_token", access_token }
        };
        
        completion_block(Response { .http_status_code = 200,
                                    .custom_status_code = 0,
                                    .headers = {},
                                    .body = json.dump() });
        
    }

public:
    void send_request_to_server(const Request request, std::function<void (const Response)> completion_block) override
    {
        if (request.url.find("/login") != std::string::npos) {
            handle_login(request, completion_block);
        } else if (request.url.find("/profile") != std::string::npos) {
            handle_profile(request, completion_block);
        } else if (request.url.find("/session") != std::string::npos && request.method != HttpMethod::post) {
            completion_block(Response { .http_status_code = 200, .custom_status_code = 0, .headers = {}, .body = "" });
        } else if (request.url.find("/api_keys") != std::string::npos && request.method == HttpMethod::post) {
            handle_create_api_key(request, completion_block);
        } else if (request.url.find(util::format("/api_keys/%1", api_key_id)) != std::string::npos && request.method == HttpMethod::get) {
            handle_fetch_api_key(request, completion_block);
        } else if (request.url.find("/api_keys") != std::string::npos && request.method == HttpMethod::get) {
            handle_fetch_api_keys(request, completion_block);
        } else if (request.url.find("/session") != std::string::npos && request.method == HttpMethod::post) {
            handle_token_refresh(request, completion_block);
        } else if (request.url.find("/location") != std::string::npos && request.method == HttpMethod::get) {
            handle_location(request, completion_block);
        } else {
            completion_block(Response { .http_status_code = 200, .custom_status_code = 0, .headers = {}, .body = "something arbitrary" });
        }
    }
};

static const std::string good_access_token =  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE1ODE1MDc3OTYsImlhdCI6MTU4MTUwNTk5NiwiaXNzIjoiNWU0M2RkY2M2MzZlZTEwNmVhYTEyYmRjIiwic3RpdGNoX2RldklkIjoiMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwIiwic3RpdGNoX2RvbWFpbklkIjoiNWUxNDk5MTNjOTBiNGFmMGViZTkzNTI3Iiwic3ViIjoiNWU0M2RkY2M2MzZlZTEwNmVhYTEyYmRhIiwidHlwIjoiYWNjZXNzIn0.0q3y9KpFxEnbmRwahvjWU1v9y1T1s3r2eozu93vMc3s";

static const std::string good_access_token2 =  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE1ODkzMDE3MjAsImlhdCI6MTU4NDExODcyMCwiaXNzIjoiNWU2YmJiYzBhNmI3ZGZkM2UyNTA0OGI3Iiwic3RpdGNoX2RldklkIjoiMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwIiwic3RpdGNoX2RvbWFpbklkIjoiNWUxNDk5MTNjOTBiNGFmMGViZTkzNTI3Iiwic3ViIjoiNWU2YmJiYzBhNmI3ZGZkM2UyNTA0OGIzIiwidHlwIjoiYWNjZXNzIn0.eSX4QMjIOLbdOYOPzQrD_racwLUk1HGFgxtx2a34k80";

std::string UnitTestTransport::access_token = good_access_token;

static const std::string bad_access_token = "lolwut";

const std::string UnitTestTransport::api_key = "lVRPQVYBJSIbGos2ZZn0mGaIq1SIOsGaZ5lrcp8bxlR5jg4OGuGwQq1GkektNQ3i";
const std::string UnitTestTransport::api_key_id = "5e5e6f0abe4ae2a2c2c2d329";
const std::string UnitTestTransport::api_key_name = "some_api_key_name";
const std::string UnitTestTransport::auth_route = "https://mongodb.com/unittests";
const std::string UnitTestTransport::user_id = "Ailuropoda melanoleuca";
std::string UnitTestTransport::provider_type = "anon-user";
const std::string UnitTestTransport::identity_0_id = "Ursus arctos isabellinus";
const std::string UnitTestTransport::identity_1_id = "Ursus arctos horribilis";


TEST_CASE("app: login_with_credentials unit_tests", "[sync][app]") {
    static const std::string base_path = realm::tmp_dir();
    auto tsm = TestSyncManager(base_path);
    
    std::unique_ptr<GenericNetworkTransport> (*factory)() = []{
        return std::unique_ptr<GenericNetworkTransport>(new UnitTestTransport);
    };

    App app(App::Config{"django", factory});

    SECTION("login_anonymous good") {
        UnitTestTransport::access_token = good_access_token;

        bool processed = false;

        app.log_in_with_credentials(realm::app::AppCredentials::anonymous(),
                                    [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
            CHECK(user);
            CHECK(!error);

            CHECK(user->identities().size() == 2);
            CHECK(user->identities()[0].id == UnitTestTransport::identity_0_id);
            CHECK(user->identities()[1].id == UnitTestTransport::identity_1_id);
            SyncUserProfile user_profile = user->user_profile();

            CHECK(user_profile.name == profile_0_name);
            CHECK(user_profile.first_name == profile_0_first_name);
            CHECK(user_profile.last_name == profile_0_last_name);
            CHECK(user_profile.email == profile_0_email);
            CHECK(user_profile.picture_url == profile_0_picture_url);
            CHECK(user_profile.gender == profile_0_gender);
            CHECK(user_profile.birthday == profile_0_birthday);
            CHECK(user_profile.min_age == profile_0_min_age);
            CHECK(user_profile.max_age == profile_0_max_age);

            processed = true;
        });

        CHECK(processed);
    }

    SECTION("login_anonymous bad") {
        std::unique_ptr<GenericNetworkTransport> (*factory)() = []{
            struct transport : GenericNetworkTransport {
                void send_request_to_server(const Request request,
                                            std::function<void (const Response)> completion_block)
                {
                    if (request.url.find("/login") != std::string::npos) {
                        completion_block({
                            200, 0, {}, user_json(bad_access_token).dump()
                        });
                    } else if (request.url.find("/profile") != std::string::npos) {
                        completion_block({
                            200, 0, {}, user_profile_json().dump()
                        });
                    }
                }
            };
            return std::unique_ptr<GenericNetworkTransport>(new transport);
        };
        app = App(App::Config{"django", factory});

        bool processed = false;

        app.log_in_with_credentials(AppCredentials::anonymous(),
                                    [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
            CHECK(!user);
            CHECK(error);
            CHECK(error->message == std::string("jwt missing parts"));
            CHECK(error->error_code.message() == "bad token");
            CHECK(error->error_code.category() == app::json_error_category());
            CHECK(error->is_json_error());
            CHECK(app::JSONErrorCode(error->error_code.value()) == app::JSONErrorCode::bad_token);
            processed = true;
        });

        CHECK(processed);
    }
}

TEST_CASE("app: UserAPIKeyProviderClient unit_tests", "[sync][app]") {
    std::unique_ptr<GenericNetworkTransport> (*factory)() = []{
        return std::unique_ptr<GenericNetworkTransport>(new UnitTestTransport);
    };
    
    auto config = App::Config{"translate-utwuv", factory};
    auto app = App(config);
    std::string base_path = tmp_dir() + "/" + config.app_id;
    reset_test_directory(base_path);
    TestSyncManager init_sync_manager(base_path);
    std::shared_ptr<SyncUser> logged_in_user = realm::SyncManager::shared().get_user(UnitTestTransport::user_id,
                                                                                     good_access_token,
                                                                                     good_access_token,
                                                                                     "anon-user");
    bool processed = false;
    ObjectId obj_id(UnitTestTransport::api_key_id.c_str());

    SECTION("create api key") {
        app.provider_client<App::UserAPIKeyProviderClient>().create_api_key(UnitTestTransport::api_key_name, logged_in_user,
                                                                            [&](App::UserAPIKey user_api_key, Optional<AppError> error) {
            CHECK(!error);
            CHECK(user_api_key.disabled == false);
            CHECK(user_api_key.id.to_string() == UnitTestTransport::api_key_id);
            CHECK(user_api_key.key == UnitTestTransport::api_key);
            CHECK(user_api_key.name == UnitTestTransport::api_key_name);
        });        
    }
    
    SECTION("fetch api key") {
        app.provider_client<App::UserAPIKeyProviderClient>().fetch_api_key(obj_id, logged_in_user,
                                                                           [&](App::UserAPIKey user_api_key, Optional<AppError> error) {
            CHECK(!error);
            CHECK(user_api_key.disabled == false);
            CHECK(user_api_key.id.to_string() == UnitTestTransport::api_key_id);
            CHECK(user_api_key.name == UnitTestTransport::api_key_name);
        });
    }
    
    SECTION("fetch api keys") {
        app.provider_client<App::UserAPIKeyProviderClient>().fetch_api_keys(logged_in_user,
                                                                            [&](std::vector<App::UserAPIKey> user_api_keys, Optional<AppError> error) {
            CHECK(!error);
            CHECK(user_api_keys.size() == 2);
            for(auto user_api_key : user_api_keys) {
                CHECK(user_api_key.disabled == false);
                CHECK(user_api_key.id.to_string() == UnitTestTransport::api_key_id);
                CHECK(user_api_key.name == UnitTestTransport::api_key_name);
            }
            processed = true;
        });
        CHECK(processed);
    }
}


TEST_CASE("app: user_semantics", "[app]") {
    static const std::string base_path = realm::tmp_dir();
    auto tsm = TestSyncManager(base_path);

    std::unique_ptr<GenericNetworkTransport> (*factory)() = []{
        struct transport : GenericNetworkTransport {
            void send_request_to_server(const Request request,
                                        std::function<void (const Response)> completion_block)
            {
                if (request.url.find("/login") != std::string::npos) {
                    completion_block({
                        200, 0, {}, user_json(good_access_token).dump()
                    });
                } else if (request.url.find("/profile") != std::string::npos) {
                    completion_block({
                        200, 0, {}, user_profile_json().dump()
                    });
                } else if (request.url.find("/session") != std::string::npos) {
                    CHECK(request.method == HttpMethod::del);
                    completion_block({ 200, 0, {}, "" });
                } else if (request.url.find("/location") != std::string::npos) {
                    CHECK(request.method == HttpMethod::get);
                    completion_block({ 200, 0, {},  "{\"deployment_model\":\"GLOBAL\",\"location\":\"US-VA\",\"hostname\":\"http://localhost:9090\",\"ws_hostname\":\"ws://localhost:9090\"}"});
                }
            }
        };
        return std::unique_ptr<GenericNetworkTransport>(new transport);
    };

    const auto app_id = random_string(36);
    App app(App::Config{app_id, factory});

    const std::function<std::shared_ptr<SyncUser>(app::AppCredentials)> login_user = [&app](app::AppCredentials creds) {
        std::shared_ptr<SyncUser> test_user;
        app.log_in_with_credentials(creds,
                                    [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
            CHECK(!error);
            test_user = user;
        });
        return test_user;
    };

    const std::function<std::shared_ptr<SyncUser>(void)> login_user_email_pass = [login_user] {
        return login_user(realm::app::AppCredentials::username_password("bob", "thompson"));
    };

    const std::function<std::shared_ptr<SyncUser>(void)> login_user_anonymous = [login_user] {
        return login_user(realm::app::AppCredentials::anonymous());
    };

    CHECK(!app.current_user());

    SECTION("current user is populated") {
        const auto user1 = login_user_anonymous();
        CHECK(app.current_user()->identity() == user1->identity());
    }

    SECTION("current user is updated on login") {
        const auto user1 = login_user_anonymous();
        CHECK(app.current_user()->identity() == user1->identity());
        const auto user2 = login_user_anonymous();
        CHECK(app.current_user()->identity() == user2->identity());
        CHECK(user1->identity() != user2->identity());
    }

    SECTION("current user is updated on login") {
        const auto user1 = login_user_anonymous();
        CHECK(app.current_user()->identity() == user1->identity());
        const auto user2 = login_user_anonymous();
        CHECK(app.current_user()->identity() == user2->identity());
        CHECK(user1->identity() != user2->identity());
    }

    SECTION("current user is updated to last used user on logout") {
        const auto user1 = login_user_anonymous();
        CHECK(app.current_user()->identity() == user1->identity());
        CHECK(app.all_users()[0]->state() == SyncUser::State::LoggedIn);

        const auto user2 = login_user_email_pass();
        CHECK(app.all_users()[0]->state() == SyncUser::State::LoggedIn);
        CHECK(app.all_users()[1]->state() == SyncUser::State::LoggedIn);
        CHECK(app.current_user()->identity() == user2->identity());
        CHECK(user1->identity() != user2->identity());

        app.log_out([&](auto){});
        CHECK(app.current_user()->identity() == user1->identity());

        CHECK(app.all_users().size() == 2);
        CHECK(app.all_users()[0]->state() == SyncUser::State::LoggedIn);
        CHECK(app.all_users()[1]->state() == SyncUser::State::LoggedOut);
    }

    SECTION("anon users are removed on logout") {
        const auto user1 = login_user_anonymous();
        CHECK(app.current_user()->identity() == user1->identity());
        CHECK(app.all_users()[0]->state() == SyncUser::State::LoggedIn);

        const auto user2 = login_user_anonymous();
        CHECK(app.all_users()[0]->state() == SyncUser::State::LoggedIn);
        CHECK(app.all_users()[1]->state() == SyncUser::State::LoggedIn);
        CHECK(app.current_user()->identity() == user2->identity());
        CHECK(user1->identity() != user2->identity());

        app.log_out([&](auto){});
        CHECK(app.current_user()->identity() == user1->identity());

        CHECK(app.all_users().size() == 1);
        CHECK(app.all_users()[0]->state() == SyncUser::State::LoggedIn);
    }

    SECTION("logout user") {
        auto user1 = login_user_email_pass();
        auto user2 = login_user_anonymous();

        // Anonymous users are special
        app.log_out(user2, [](Optional<AppError> error) {
            CHECK(!error);
        });
        CHECK(user2->state() == SyncUser::State::Removed);

        // Other users can be LoggedOut
        app.log_out(user1, [](Optional<AppError> error) {
            CHECK(!error);
        });
        CHECK(user1->state() == SyncUser::State::LoggedOut);

        // Logging out already logged out users, does nothing
        app.log_out(user1, [](Optional<AppError> error) {
            CHECK(!error);
        });
        CHECK(user1->state() == SyncUser::State::LoggedOut);

        app.log_out(user2, [](Optional<AppError> error) {
            CHECK(!error);
        });
        CHECK(user2->state() == SyncUser::State::Removed);
    }
}

struct ErrorCheckingTransport : public GenericNetworkTransport {
    ErrorCheckingTransport(Response r)
    : m_response(r)
    {
    }
    void send_request_to_server(const Request, std::function<void (const Response)> completion_block) override
    {
        completion_block(m_response);
    }
private:
    Response m_response;
};

TEST_CASE("app: response error handling", "[sync][app]") {
    static const std::string base_path = realm::tmp_dir();
    auto tsm = TestSyncManager(base_path);

    std::string response_body = nlohmann::json({
        {"access_token", good_access_token},
        {"refresh_token", good_access_token},
        {"user_id", "Brown Bear"},
        {"device_id", "Panda Bear"}}).dump();

    Response response{.http_status_code = 200, .headers = {{"Content-Type", "application/json"}}, .body = response_body};

    std::function<std::unique_ptr<GenericNetworkTransport>()> transport_generator = [&response] {
        return std::unique_ptr<GenericNetworkTransport>(new ErrorCheckingTransport(response));
    };
    App app(App::Config{"my-app-id", transport_generator});
    bool processed = false;

    SECTION("http 404") {
        response.http_status_code = 404;
        app.log_in_with_credentials(realm::app::AppCredentials::anonymous(),
                                    [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
            CHECK(!user);
            CHECK(error);
            CHECK(!error->is_json_error());
            CHECK(!error->is_custom_error());
            CHECK(!error->is_service_error());
            CHECK(error->is_http_error());
            CHECK(error->error_code.value() == 404);
            CHECK(error->message == std::string("http error code considered fatal"));
            CHECK(error->error_code.message() == "Client Error: 404");
            processed = true;
        });
        CHECK(processed);
    }
    SECTION("http 500") {
        response.http_status_code = 500;
        app.log_in_with_credentials(realm::app::AppCredentials::anonymous(),
                                    [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
            CHECK(!user);
            CHECK(error);
            CHECK(!error->is_json_error());
            CHECK(!error->is_custom_error());
            CHECK(!error->is_service_error());
            CHECK(error->is_http_error());
            CHECK(error->error_code.value() == 500);
            CHECK(error->message == std::string("http error code considered fatal"));
            CHECK(error->error_code.message() == "Server Error: 500");
            processed = true;
        });
        CHECK(processed);
    }
    SECTION("custom error code") {
        response.custom_status_code = 42;
        response.body = "Custom error message";
        app.log_in_with_credentials(realm::app::AppCredentials::anonymous(),
                                    [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
            CHECK(!user);
            CHECK(error);
            CHECK(!error->is_http_error());
            CHECK(!error->is_json_error());
            CHECK(!error->is_service_error());
            CHECK(error->is_custom_error());
            CHECK(error->error_code.value() == 42);
            CHECK(error->message == std::string("Custom error message"));
            CHECK(error->error_code.message() == "code 42");
            processed = true;
        });
        CHECK(processed);
    }

    SECTION("session error code") {
        response.http_status_code = 400;
        response.body = nlohmann::json({
            {"error_code", "MongoDBError"},
            {"error", "a fake MongoDB error message!"},
            {"access_token", good_access_token},
            {"refresh_token", good_access_token},
            {"user_id", "Brown Bear"},
            {"device_id", "Panda Bear"}}).dump();
        app.log_in_with_credentials(realm::app::AppCredentials::anonymous(),
                                    [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
            CHECK(!user);
            CHECK(error);
            CHECK(!error->is_http_error());
            CHECK(!error->is_json_error());
            CHECK(!error->is_custom_error());
            CHECK(error->is_service_error());
            CHECK(app::ServiceErrorCode(error->error_code.value()) == app::ServiceErrorCode::mongodb_error);
            CHECK(error->message == std::string("a fake MongoDB error message!"));
            CHECK(error->error_code.message() == "MongoDBError");
            processed = true;
        });
        CHECK(processed);
    }

    SECTION("json error code") {
        response.body = "this: is not{} a valid json body!";
        app.log_in_with_credentials(realm::app::AppCredentials::anonymous(),
                                    [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
            CHECK(!user);
            CHECK(error);
            CHECK(!error->is_http_error());
            CHECK(error->is_json_error());
            CHECK(!error->is_custom_error());
            CHECK(!error->is_service_error());
            CHECK(app::JSONErrorCode(error->error_code.value()) == app::JSONErrorCode::malformed_json);
            CHECK(error->message == std::string("[json.exception.parse_error.101] parse error at line 1, column 2: syntax error while parsing value - invalid literal; last read: 'th'"));
            CHECK(error->error_code.message() == "malformed json");
            processed = true;
        });
        CHECK(processed);
    }
}

TEST_CASE("app: switch user", "[sync][app]") {
        
    std::function<std::unique_ptr<GenericNetworkTransport>()> transport_generator = [&] {
        return std::unique_ptr<GenericNetworkTransport>(new UnitTestTransport());
    };

    auto config = App::Config{"translate-utwuv", transport_generator};
    auto app = App(config);
    
    std::string base_path = tmp_dir() + "/" + config.app_id;
    reset_test_directory(base_path);

    auto tsm = TestSyncManager(base_path);
    
    bool processed = false;
    
    std::shared_ptr<SyncUser> user_a;
    std::shared_ptr<SyncUser> user_b;

    SECTION("switch user expect success") {
        
        CHECK(SyncManager::shared().all_users().size() == 0);

        // Log in user 1
        app.log_in_with_credentials(realm::app::AppCredentials::anonymous(),
                                    [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
            CHECK(!error);
            CHECK(SyncManager::shared().get_current_user() == user);
            user_a = user;
        });
        
        // Log in user 2
        app.log_in_with_credentials(realm::app::AppCredentials::anonymous(),
                                    [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
            CHECK(!error);
            CHECK(SyncManager::shared().get_current_user() == user);
            user_b = user;
        });
        
        CHECK(SyncManager::shared().all_users().size() == 2);

        auto user1 = app.switch_user(user_a);
        CHECK(user1 == user_a);
        
        CHECK(SyncManager::shared().get_current_user() == user_a);

        auto user2 = app.switch_user(user_b);
        CHECK(user2 == user_b);

        CHECK(SyncManager::shared().get_current_user() == user_b);
        processed = true;
        CHECK(processed);
    }
        
    SECTION("switch user expect fail") {
        CHECK(SyncManager::shared().all_users().size() == 0);

        // Log in user 1
        app.log_in_with_credentials(realm::app::AppCredentials::anonymous(),
                                    [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
            user_a = user;
            CHECK(!error);
        });
        
        CHECK(SyncManager::shared().get_current_user() == user_a);
        
        app.log_out([&](Optional<app::AppError> error) {
            CHECK(!error);
        });

        CHECK(SyncManager::shared().get_current_user() == nullptr);
        CHECK(user_a->state() == SyncUser::State::Removed);

        // Log in user 2
        app.log_in_with_credentials(realm::app::AppCredentials::anonymous(),
                                    [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
            user_b = user;
            CHECK(!error);
        });
        
        CHECK(SyncManager::shared().get_current_user() == user_b);
        CHECK(SyncManager::shared().all_users().size() == 1);

        try {
            auto user = app.switch_user(user_a);
            CHECK(!user);
        } catch (AppError error) {
            CHECK(error.error_code.value() > 0);
        }
        
        CHECK(SyncManager::shared().get_current_user() == user_b);
        
        processed = true;
        CHECK(processed);
    }
    
}

TEST_CASE("app: remove anonymous user", "[sync][app]") {
    
    std::function<std::unique_ptr<GenericNetworkTransport>()> transport_generator = [&] {
        return std::unique_ptr<GenericNetworkTransport>(new UnitTestTransport());
    };

    auto config = App::Config{"translate-utwuv", transport_generator};
    auto app = App(config);
    
    std::string base_path = tmp_dir() + "/" + config.app_id;
    reset_test_directory(base_path);

    auto tsm = TestSyncManager(base_path);
    
    bool processed = false;
    std::shared_ptr<SyncUser> user_a;
    std::shared_ptr<SyncUser> user_b;

    SECTION("remove user expect success") {
        CHECK(SyncManager::shared().all_users().size() == 0);

        // Log in user 1
        app.log_in_with_credentials(realm::app::AppCredentials::anonymous(),
                                    [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
            CHECK(!error);
            CHECK(SyncManager::shared().get_current_user() == user);
            user_a = user;
        });
        
        CHECK(user_a->state() == SyncUser::State::LoggedIn);
        
        app.log_out(user_a, [&](Optional<app::AppError> error) {
            CHECK(!error);
            // a logged out anon user will be marked as Removed, not LoggedOut
            CHECK(user_a->state() == SyncUser::State::Removed);
        });
        
        app.remove_user(user_a, [&](Optional<app::AppError> error) {
            CHECK(error->message == "User has already been removed");
            CHECK(SyncManager::shared().all_users().size() == 0);
        });
                
        // Log in user 2
        app.log_in_with_credentials(realm::app::AppCredentials::anonymous(),
                                    [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
            CHECK(!error);
            CHECK(SyncManager::shared().get_current_user() == user);
            user_b = user;
        });
        
        CHECK(user_b->state() == SyncUser::State::LoggedIn);
        CHECK(SyncManager::shared().all_users().size() == 1);

        app.remove_user(user_b, [&](Optional<app::AppError> error) {
            CHECK(!error);
            CHECK(SyncManager::shared().all_users().size() == 0);
        });
        
        CHECK(SyncManager::shared().get_current_user() == nullptr);
        
        //check both handles are no longer valid
        CHECK(user_a->state() == SyncUser::State::Removed);
        CHECK(user_b->state() == SyncUser::State::Removed);

        processed = true;
        CHECK(processed);
    }
    
}

TEST_CASE("app: remove user with credentials", "[sync][app]") {
        
    std::unique_ptr<GenericNetworkTransport> (*transport_generator)() = []{
        struct transport : GenericNetworkTransport {
            void send_request_to_server(const Request request,
                                        std::function<void (const Response)> completion_block)
            {
                if (request.url.find("/login") != std::string::npos) {
                    completion_block({
                        200, 0, {}, user_json(good_access_token).dump()
                    });
                } else if (request.url.find("/profile") != std::string::npos) {
                    completion_block({
                        200, 0, {}, user_profile_json().dump()
                    });
                } else if (request.url.find("/session") != std::string::npos) {
                    CHECK(request.method == HttpMethod::del);
                    completion_block({ 200, 0, {}, "" });
                } else if (request.url.find("/location") != std::string::npos) {
                    CHECK(request.method == HttpMethod::get);
                    completion_block({ 200, 0, {},  "{\"deployment_model\":\"GLOBAL\",\"location\":\"US-VA\",\"hostname\":\"http://localhost:9090\",\"ws_hostname\":\"ws://localhost:9090\"}"});
                }
            }
        };
        return std::unique_ptr<GenericNetworkTransport>(new transport);
    };
    
    auto config = App::Config{"translate-utwuv", transport_generator};
    std::string base_path = tmp_dir() + "/" + config.app_id;
    reset_test_directory(base_path);
    auto tsm = TestSyncManager(base_path);
    
    auto app = App(config);

    CHECK(!app.current_user());
    bool processed = false;
    std::shared_ptr<SyncUser> test_user;

    SECTION("log in, log out and remove") {
        
        CHECK(SyncManager::shared().all_users().size() == 0);
        CHECK(SyncManager::shared().get_current_user() == nullptr);
        
        app.log_in_with_credentials(realm::app::AppCredentials::username_password("email", "pass"),
                                    [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
            CHECK(!error);
            test_user = user;
        });
        
        CHECK(test_user->state() == SyncUser::State::LoggedIn);
        
        app.log_out(test_user, [&](Optional<app::AppError> error) {
            CHECK(!error);
        });
        
        CHECK(test_user->state() == SyncUser::State::LoggedOut);
        
        app.remove_user(test_user, [&](Optional<app::AppError> error) {
            CHECK(!error);
            CHECK(SyncManager::shared().all_users().size() == 0);
        });
        
        app.remove_user(test_user, [&](Optional<app::AppError> error) {
            CHECK(error->error_code.value() > 0);
            CHECK(SyncManager::shared().all_users().size() == 0);
            processed = true;
        });
        
        CHECK(test_user->state() == SyncUser::State::Removed);
        CHECK(processed);
    }
    
}

TEST_CASE("app: link_user", "[sync][app]") {

    SECTION("link_user") {
        std::unique_ptr<GenericNetworkTransport> (*transport_generator)() = []{
            struct transport : GenericNetworkTransport {
                void send_request_to_server(const Request request,
                                            std::function<void (const Response)> completion_block)
                {
                    if (request.url.find("/login?link=true") != std::string::npos) {
                        completion_block({
                            200, 0, {}, user_json(good_access_token).dump()
                        });
                    }else if (request.url.find("/login") != std::string::npos) {
                        completion_block({
                            200, 0, {}, user_json(good_access_token).dump()
                        });
                    } else if (request.url.find("/profile") != std::string::npos) {
                        completion_block({
                            200, 0, {}, user_profile_json().dump()
                        });
                    } else if (request.url.find("/session") != std::string::npos) {
                        CHECK(request.method == HttpMethod::del);
                        completion_block({ 200, 0, {}, "" });
                    } else if (request.url.find("/location") != std::string::npos) {
                        CHECK(request.method == HttpMethod::get);
                        completion_block({ 200, 0, {},  "{\"deployment_model\":\"GLOBAL\",\"location\":\"US-VA\",\"hostname\":\"http://localhost:9090\",\"ws_hostname\":\"ws://localhost:9090\"}"});
                    }
                }
            };
            return std::unique_ptr<GenericNetworkTransport>(new transport);
        };
        
        auto config = App::Config{"translate-utwuv", transport_generator};
        std::string base_path = tmp_dir() + "/" + config.app_id;
        reset_test_directory(base_path);
        auto tsm = TestSyncManager(base_path);
        
        auto app = App(config);

        bool processed = false;

        std::shared_ptr<SyncUser> sync_user;
        
        auto email = util::format("realm_tests_do_autoverify%1@%2.com", random_string(10), random_string(10));
        auto password = random_string(10);
        
        auto custom_credentials = realm::app::AppCredentials::facebook("a_token");
        auto email_pass_credentials = realm::app::AppCredentials::username_password(email, password);

        app.log_in_with_credentials(email_pass_credentials,
                                    [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
            REQUIRE(user);
            CHECK(!error);
            sync_user = user;
        });
        
        CHECK(sync_user->provider_type() == IdentityProviderUsernamePassword);

        app.link_user(sync_user,
                      custom_credentials,
                      [&](std::shared_ptr<SyncUser> user, Optional<app::AppError> error) {
            CHECK(!error);
            REQUIRE(user);
            CHECK(user->identity() == sync_user->identity());
            processed = true;
        });

        CHECK(processed);
    }
    
    SECTION("link_user should fail") {
        std::unique_ptr<GenericNetworkTransport> (*transport_generator)() = []{
            struct transport : GenericNetworkTransport {
                void send_request_to_server(const Request request,
                                            std::function<void (const Response)> completion_block)
                {
                    if (request.url.find("/login") != std::string::npos) {
                        completion_block({
                            200, 0, {}, user_json(good_access_token).dump()
                        });
                    } else if (request.url.find("/profile") != std::string::npos) {
                        completion_block({
                            200, 0, {}, user_profile_json().dump()
                        });
                    } else if (request.url.find("/session") != std::string::npos) {
                        CHECK(request.method == HttpMethod::del);
                        completion_block({ 200, 0, {}, "" });
                    } else if (request.url.find("/location") != std::string::npos) {
                        CHECK(request.method == HttpMethod::get);
                        completion_block({ 200, 0, {},  "{\"deployment_model\":\"GLOBAL\",\"location\":\"US-VA\",\"hostname\":\"http://localhost:9090\",\"ws_hostname\":\"ws://localhost:9090\"}"});
                    }
                }
            };
            return std::unique_ptr<GenericNetworkTransport>(new transport);
        };
        
        auto config = App::Config{"translate-utwuv", transport_generator};
        std::string base_path = tmp_dir() + "/" + config.app_id;
        reset_test_directory(base_path);
        auto tsm = TestSyncManager(base_path);
        
        auto app = App(config);

        bool processed = false;

        std::shared_ptr<SyncUser> sync_user;
        
        auto email = util::format("realm_tests_do_autoverify%1@%2.com", random_string(10), random_string(10));
        auto password = random_string(10);
        
        auto custom_credentials = realm::app::AppCredentials::facebook("a_token");
        auto email_pass_credentials = realm::app::AppCredentials::username_password(email, password);

        app.log_in_with_credentials(email_pass_credentials,
                                    [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
            REQUIRE(user);
            CHECK(!error);
            sync_user = user;
        });
        
        app.log_out([&](Optional<app::AppError> error) {
            CHECK(!error);
        });
        
        CHECK(sync_user->provider_type() == IdentityProviderUsernamePassword);

        app.link_user(sync_user,
                      custom_credentials,
                      [&](std::shared_ptr<SyncUser> user, Optional<app::AppError> error) {
            CHECK(error->message == "The specified user is not logged in");
            CHECK(!user);
            processed = true;
        });

        CHECK(processed);
    }
}

TEST_CASE("app: auth providers", "[sync][app]") {
    
    SECTION("auth providers facebook") {
        auto credentials = realm::app::AppCredentials::facebook("a_token");
        CHECK(credentials.provider() == AuthProvider::FACEBOOK);
        CHECK(credentials.provider_as_string() == IdentityProviderFacebook);
        CHECK(credentials.serialize_as_json() == "{\"access_token\":\"a_token\",\"provider\":\"oauth2-facebook\"}");
    }
    
    SECTION("auth providers anonymous") {
        auto credentials = realm::app::AppCredentials::anonymous();
        CHECK(credentials.provider() == AuthProvider::ANONYMOUS);
        CHECK(credentials.provider_as_string() == IdentityProviderAnonymous);
        CHECK(credentials.serialize_as_json() == "{\"provider\":\"anon-user\"}");
    }
    
    SECTION("auth providers google") {
        auto credentials = realm::app::AppCredentials::google("a_token");
        CHECK(credentials.provider() == AuthProvider::GOOGLE);
        CHECK(credentials.provider_as_string() == IdentityProviderGoogle);
        CHECK(credentials.serialize_as_json() == "{\"authCode\":\"a_token\",\"provider\":\"oauth2-google\"}");
    }
    
    SECTION("auth providers apple") {
        auto credentials = realm::app::AppCredentials::apple("a_token");
        CHECK(credentials.provider() == AuthProvider::APPLE);
        CHECK(credentials.provider_as_string() == IdentityProviderApple);
        CHECK(credentials.serialize_as_json() == "{\"id_token\":\"a_token\",\"provider\":\"oauth2-apple\"}");
    }
    
    SECTION("auth providers custom") {
        auto credentials = realm::app::AppCredentials::custom("a_token");
        CHECK(credentials.provider() == AuthProvider::CUSTOM);
        CHECK(credentials.provider_as_string() == IdentityProviderCustom);
        CHECK(credentials.serialize_as_json() == "{\"provider\":\"custom-token\",\"token\":\"a_token\"}");
    }
    
    SECTION("auth providers username password") {
        auto credentials = realm::app::AppCredentials::username_password("user", "pass");
        CHECK(credentials.provider() == AuthProvider::USERNAME_PASSWORD);
        CHECK(credentials.provider_as_string() == IdentityProviderUsernamePassword);
        CHECK(credentials.serialize_as_json() == "{\"password\":\"pass\",\"provider\":\"local-userpass\",\"username\":\"user\"}");
    }
    
    SECTION("auth providers function") {
        auto credentials = realm::app::AppCredentials::function(nlohmann::json({{"name", "mongo"}}).dump());
        CHECK(credentials.provider() == AuthProvider::FUNCTION);
        CHECK(credentials.provider_as_string() == IdentityProviderFunction);
        CHECK(credentials.serialize_as_json() == "{\"name\":\"mongo\"}");
    }
    
    SECTION("auth providers user api key") {
        auto credentials = realm::app::AppCredentials::user_api_key("a key");
        CHECK(credentials.provider() == AuthProvider::USER_API_KEY);
        CHECK(credentials.provider_as_string() == IdentityProviderUserAPIKey);
        CHECK(credentials.serialize_as_json() == "{\"key\":\"a key\",\"provider\":\"api-key\"}");
    }
    
    SECTION("auth providers server api key") {
        auto credentials = realm::app::AppCredentials::server_api_key("a key");
        CHECK(credentials.provider() == AuthProvider::SERVER_API_KEY);
        CHECK(credentials.provider_as_string() == IdentityProviderServerAPIKey);
        CHECK(credentials.serialize_as_json() == "{\"key\":\"a key\",\"provider\":\"api-key\"}");
    }
    
}

TEST_CASE("app: refresh access token unit tests", "[sync][app]") {
    
    SECTION("refresh custom data happy path") {
        
        auto setup_user = []() {
            if (realm::SyncManager::shared().get_current_user()) {
                return;
            }
            
            realm::SyncManager::shared().get_user("a_user_id",
                                                  good_access_token,
                                                  good_access_token,
                                                  "anon-user");
        };
        
        static bool session_route_hit = false;
        
        std::unique_ptr<GenericNetworkTransport> (*generic_factory)() = [] {
            struct transport : GenericNetworkTransport {
                void send_request_to_server(const Request request,
                                            std::function<void (const Response)> completion_block)
                {
                    if (request.url.find("/session") != std::string::npos) {
                        session_route_hit = true;
                        nlohmann::json json {
                            { "access_token", good_access_token }
                        };
                        completion_block({ 200, 0, {}, json.dump() });
                    } else if (request.url.find("/location") != std::string::npos) {
                        CHECK(request.method == HttpMethod::get);
                        completion_block({ 200, 0, {},  "{\"deployment_model\":\"GLOBAL\",\"location\":\"US-VA\",\"hostname\":\"http://localhost:9090\",\"ws_hostname\":\"ws://localhost:9090\"}"});
                    }
                }
            };
            return std::unique_ptr<GenericNetworkTransport>(new transport);
        };
        
        auto config = App::Config{"translate-utwuv", generic_factory};
        auto app = App(config);
        std::string base_path = tmp_dir() + "/" + config.app_id;
        reset_test_directory(base_path);
        TestSyncManager init_sync_manager(base_path);
        
        setup_user();
        
        bool processed = false;
        
        app.refresh_custom_data(SyncManager::shared().get_current_user(), [&](const Optional<AppError>& error) {
            CHECK(!error);
            CHECK(session_route_hit);
            processed = true;
        });
        
        CHECK(processed);
    }
    
    SECTION("refresh custom data sad path") {
        
        auto setup_user = []() {
            if (realm::SyncManager::shared().get_current_user()) {
                return;
            }
            
            realm::SyncManager::shared().get_user("a_user_id",
                                                  good_access_token,
                                                  good_access_token,
                                                  "anon-user");
        };
        
        static bool session_route_hit = false;
        
        std::unique_ptr<GenericNetworkTransport> (*generic_factory)() = [] {
            struct transport : GenericNetworkTransport {
                void send_request_to_server(const Request request,
                                            std::function<void (const Response)> completion_block)
                {
                    if (request.url.find("/session") != std::string::npos) {
                        session_route_hit = true;
                        nlohmann::json json {
                            { "access_token", bad_access_token }
                        };
                        completion_block({ 200, 0, {}, json.dump() });
                    }  else if (request.url.find("/location") != std::string::npos) {
                        CHECK(request.method == HttpMethod::get);
                        completion_block({ 200, 0, {},  "{\"deployment_model\":\"GLOBAL\",\"location\":\"US-VA\",\"hostname\":\"http://localhost:9090\",\"ws_hostname\":\"ws://localhost:9090\"}"});
                    }
                }
            };
            return std::unique_ptr<GenericNetworkTransport>(new transport);
        };
        
        auto config = App::Config{"translate-utwuv", generic_factory};
        auto app = App(config);
        std::string base_path = tmp_dir() + "/" + config.app_id;
        reset_test_directory(base_path);
        TestSyncManager init_sync_manager(base_path);
        
        setup_user();
        
        bool processed = false;
        
        app.refresh_custom_data(SyncManager::shared().get_current_user(), [&](const Optional<AppError>& error) {
            CHECK(error->message == "jwt missing parts");
            CHECK(error->error_code.value() == 1);
            CHECK(session_route_hit);
            processed = true;
        });
        
        CHECK(processed);
    }
    
    SECTION("refresh token ensure flow is correct") {
        
        auto setup_user = []() {
            if (realm::SyncManager::shared().get_current_user()) {
                return;
            }
            
            realm::SyncManager::shared().get_user("a_user_id",
                                                  good_access_token,
                                                  good_access_token,
                                                  "anon-user");
        };
        
        /*
         Expected flow:
         Login - this gets access and refresh tokens
         Get profile - throw back a 401 error
         Refresh token - get a new token for the user
         Get profile - get the profile with the new token
         */
        
        static bool login_hit = false;
        static bool get_profile_1_hit = false;
        static bool get_profile_2_hit = false;
        static bool refresh_hit = false;
        
        std::unique_ptr<GenericNetworkTransport> (*factory)() = [](){
            struct transport : GenericNetworkTransport {
                
                void send_request_to_server(const Request request,
                                            std::function<void (const Response)> completion_block)
                {
                    if (request.url.find("/login") != std::string::npos) {
                        login_hit = true;
                        completion_block({
                            200, 0, {}, user_json(good_access_token).dump()
                        });
                    } else if (request.url.find("/profile") != std::string::npos) {
                        
                        CHECK(login_hit);
                        
                        auto access_token = request.headers.at("Authorization");
                        // simulated bad token request
                        if (access_token.find(good_access_token2) != std::string::npos) {
                            CHECK(login_hit);
                            CHECK(get_profile_1_hit);
                            CHECK(refresh_hit);
                            
                            get_profile_2_hit = true;
                            
                            completion_block({
                                200, 0, {}, user_profile_json().dump()
                            });
                        } else if (access_token.find(good_access_token) != std::string::npos) {
                            CHECK(!get_profile_2_hit);
                            get_profile_1_hit = true;
                            
                            completion_block({
                                401, 0, {}
                            });
                        }
                        
                    } else if (request.url.find("/session") != std::string::npos &&
                               request.method == HttpMethod::post) {
                        
                        CHECK(login_hit);
                        CHECK(get_profile_1_hit);
                        CHECK(!get_profile_2_hit);
                        refresh_hit = true;
                        
                        nlohmann::json json {
                            { "access_token", good_access_token2 }
                        };
                        completion_block({ 200, 0, {}, json.dump() });
                    }  else if (request.url.find("/location") != std::string::npos) {
                        CHECK(request.method == HttpMethod::get);
                        completion_block({ 200, 0, {},  "{\"deployment_model\":\"GLOBAL\",\"location\":\"US-VA\",\"hostname\":\"http://localhost:9090\",\"ws_hostname\":\"ws://localhost:9090\"}"});
                    }
                }
            };
            return std::unique_ptr<GenericNetworkTransport>(new transport);
        };
        
        auto config = App::Config{"translate-utwuv", factory};
        auto app = App(config);
        std::string base_path = tmp_dir() + "/" + config.app_id;
        reset_test_directory(base_path);
        TestSyncManager init_sync_manager(base_path);
        
        setup_user();
        
        bool processed = false;
        
        app.log_in_with_credentials(AppCredentials::anonymous(),
                                    [&](std::shared_ptr<SyncUser> user, Optional<app::AppError> error) {
                                        CHECK(user);
                                        CHECK(!error);
                                        processed = true;
                                    });
        
        CHECK(processed);
    }
}
