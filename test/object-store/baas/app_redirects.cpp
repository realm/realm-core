////////////////////////////////////////////////////////////////////////////
//
// Copyright 2023 Realm Inc.
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

#if REALM_ENABLE_SYNC
#if REALM_ENABLE_AUTH_TESTS

#include <util/baas_admin_api.hpp>
#include <util/baas_test_utils.hpp>

#include <realm/object-store/sync/app.hpp>
#include <realm/object-store/sync/app_credentials.hpp>
#include <realm/object-store/sync/app_utils.hpp>
#include <realm/object-store/sync/generic_network_transport.hpp>
#include <realm/object-store/sync/sync_session.hpp>
#include <realm/object-store/sync/sync_user.hpp>
#include <realm/sync/network/default_socket.hpp>
#include <realm/sync/network/websocket.hpp>
#include <realm/util/future.hpp>
#include <realm/util/logger.hpp>
#include <realm/util/uri.hpp>

#include <chrono>
#include <iostream>

// include file for sleep()
#ifdef _WIN32
#include <windows.h> // DEBUGGING
#else
#include <unistd.h> // DEBUGGING
#endif

using namespace realm;
using namespace realm::app;

struct HookedSocketProvider : public sync::websocket::DefaultSocketProvider {
    HookedSocketProvider(const std::shared_ptr<util::Logger>& logger, const std::string user_agent,
                         AutoStart auto_start = AutoStart{true})
        : DefaultSocketProvider(logger, user_agent, nullptr, auto_start)
    {
    }

    std::unique_ptr<sync::WebSocketInterface> connect(std::unique_ptr<sync::WebSocketObserver> observer,
                                                      sync::WebSocketEndpoint&& endpoint) override
    {
        std::optional<std::pair<int, std::string>> simulated_response;
        if (handshake_response_func) {
            simulated_response = handshake_response_func();
        }

        auto websocket = DefaultSocketProvider::connect(std::move(observer), std::move(endpoint));
        if (simulated_response) {
            auto default_websocket = static_cast<sync::websocket::DefaultWebSocket*>(websocket.get());
            if (default_websocket) {
                default_websocket->force_handshake_response_for_testing(simulated_response->first,
                                                                        simulated_response->second);
            }
        }
        return websocket;
    }

    std::function<std::optional<std::pair<int, std::string>>()> handshake_response_func;
};


TEST_CASE("app: redirects", "[sync][pbs][app][baas][redirects][new]") {
    auto logger = util::Logger::get_default_logger();

    // redirect URL is localhost or 127.0.0.1 depending on what the initial value is
    std::string original_host = "localhost";
    std::string port = "9090";
    std::string userinfo = "";
    std::string app_scheme = "http:";
    std::string ws_scheme = "ws:";
    std::string redirect_host = "127.0.0.1";
    std::string app_url = "http://localhost:9090";
    std::string ws_url = "ws://localhost:9090";
    std::string redir_app_url = "http://127.0.0.1:9090";
    std::string redir_ws_url = "ws://127.0.0.1:9090";
    std::string mongodb_realm_host = "mongodb-realm";

    // Parse the first request to determine the current and redirect URL values
    auto parse_url = [&](std::string request_url) {
        auto host_url = util::Uri(request_url);
        app_scheme = host_url.get_scheme();
        host_url.get_auth(userinfo, original_host, port);

        logger->debug("Parse url: [%1]//[%2]:[%3]", app_scheme, original_host, port);

        // using https?
        if (app_scheme == "https:") {
            ws_scheme = "wss:";
        }

        // using local baas @ 127.0.0.1 - use 'localhost' as redirect
        if (original_host == "127.0.0.1") {
            redirect_host = "localhost";
        }
        // using local baas @ ::1 - use 'localhost' as redirect
        else if (original_host == "::1") {
            redirect_host = "localhost";
        }
        // using baas docker - can't test redirect due to custom hostname
        else if (original_host == mongodb_realm_host) {
            redirect_host = mongodb_realm_host;
        }
        app_url = util::format("%1//%2:%3", app_scheme, original_host, port);
        ws_url = util::format("%1//%2:%3", ws_scheme, original_host, port);
        redir_app_url = util::format("%1//%2:%3", app_scheme, redirect_host, port);
        redir_ws_url = util::format("%1//%2:%3", ws_scheme, redirect_host, port);
        logger->trace("- App URL:             %1", app_url);
        logger->trace("- Websocket URL:       %1", ws_url);
        logger->trace("- Redir App URL:       %1", redir_app_url);
        logger->trace("- Redir Websocket URL: %1", redir_ws_url);
    };

    const auto base_url = get_base_url();
    const auto partition = random_string(100);
    const auto schema = default_app_config("").schema;
    auto redir_transport = std::make_shared<HookedTransport>();
    auto redir_provider = std::make_shared<HookedSocketProvider>(logger, "");
    AutoVerifiedEmailCredentials creds, creds2;

    auto server_app_config = minimal_app_config(base_url, "redirect_tests", schema);
    TestAppSession session(create_app(server_app_config), redir_transport, DeleteApp{true},
                           realm::ReconnectMode::normal, redir_provider);
    auto app = session.app();

    auto app_config = get_config(redir_transport, session.app_session());
    set_app_config_defaults(app_config, redir_transport);

    std::string base_file_path = util::make_temp_dir() + random_string(10);
    util::try_make_dir(base_file_path);
    SyncClientConfig sc_config;
    sc_config.base_file_path = base_file_path;
    sc_config.metadata_mode = realm::SyncManager::MetadataMode::NoEncryption;


    SECTION("Test invalid redirect response") {
        int request_count = 0;
        int redirect_count = 0;
        const int max_request_count = 3;

        // initialize app and sync client
        auto redir_app = app::App::get_uncached_app(app_config, sc_config);

        redir_transport->request_hook = [&](const Request& request) {
            REALM_ASSERT(request_count < max_request_count);
            logger->trace("Received request[%1]: %2", request_count, request.url);
            if (request_count == 0) {
                // This will fail due to no headers
                redir_transport->simulated_response = {
                    static_cast<int>(sync::HTTPStatus::MovedPermanently), 0, {}, "Some body data"};
            }
            else if (request_count == 1) {
                // This will fail due to no Location header
                redir_transport->simulated_response = {static_cast<int>(sync::HTTPStatus::PermanentRedirect),
                                                       0,
                                                       {{"Content-Type", "application/json"}},
                                                       "Some body data"};
            }
            else if (request_count == 2) {
                // This will fail due to empty Location header
                redir_transport->simulated_response = make_redirect_response(sync::HTTPStatus::MovedPermanently, "");
            }
            request_count++;
        };

        auto check_redirect_error = [&] {
            std::unique_ptr<StatusWith<AppError>> app_error;
            redir_app->provider_client<app::App::UsernamePasswordProviderClient>().register_email(
                creds.email, creds.password, [&app_error](util::Optional<app::AppError> error) {
                    if (!error) {
                        app_error = std::make_unique<StatusWith<AppError>>(
                            Status{ErrorCodes::RuntimeError, "App error not received for invalid redirect response"});
                        return;
                    }
                    app_error = std::make_unique<StatusWith<AppError>>(std::move(*error));
                });

            REQUIRE(app_error);
            if (!app_error->is_ok()) {
                logger->error("Invalid redirect response test failed: %1", app_error->get_status().reason());
            }
            REQUIRE(app_error->is_ok());

            REQUIRE(app_error->get_value().is_client_error());
            REQUIRE(app_error->get_value().code() == ErrorCodes::ClientRedirectError);
            REQUIRE(app_error->get_value().reason() == "Redirect response missing location header");
        };

        while (redirect_count < max_request_count) {
            check_redirect_error();
            redirect_count++;
        }
    }

    SECTION("Test redirect response") {
        int request_count = 0;
        // initialize app and sync client
        auto redir_app = app::App::get_uncached_app(app_config, sc_config);

        redir_transport->request_hook = [&](const Request& request) {
            logger->trace("Received request[%1]: %2", request_count, request.url);
            if (request_count == 0) {
                // HTTP request #1 should be to location - use this request to determine
                // which original host is being used (localhost or 127.0.0.1)
                REQUIRE(request.url.find("/location") != std::string::npos);
                parse_url(request.url);
            }
            else if (request_count == 1) {
                // HTTP request #2 will respond with a redirect to an invalid URL ("somehost:9090")
                REQUIRE(!request.redirect_count);
                redir_transport->simulated_response =
                    make_redirect_response(sync::HTTPStatus::MovedPermanently, "http://somehost:9090");
            }
            else if (request_count == 2) {
                // HTTP request #3 should be a location request to "somehost:9090"
                // A redirect response to the redirect URL will be sent
                REQUIRE(request.url.find("/location") != std::string::npos);
                REQUIRE(request.url.find("somehost:9090") != std::string::npos);
                redir_transport->simulated_response =
                    make_redirect_response(sync::HTTPStatus::PermanentRedirect, redir_app_url);
            }
            else if (request_count == 3) {
                // HTTP request #4 should be a location request to the redirect url
                // A redirect response to the original URL will be sent
                REQUIRE(request.url.find("/location") != std::string::npos);
                REQUIRE(request.url.find(redir_app_url) != std::string::npos);
                redir_transport->simulated_response =
                    make_redirect_response(sync::HTTPStatus::MovedPermanently, app_url);
            }
            else if (request_count == 4) {
                // HTTP request #5 will be a location request to the original URL
                REQUIRE(request.url.find("/location") != std::string::npos);
                REQUIRE(request.url.find(app_url) != std::string::npos);
                // Let the location request go through
                redir_transport->simulated_response.reset();
            }
            else if (request_count == 5) {
                // This should be a login request to the original URL
                REQUIRE(request.url.find(app_url) != std::string::npos);
                // Validate the retry count tracked in the original message
                // The location requests do not have a redirect count
                logger->trace("Request redirect_count: %1", request.redirect_count);
                REQUIRE(request.redirect_count == 3);

                auto sync_manager = redir_app->sync_manager();
                REQUIRE(sync_manager);
                auto app_metadata = sync_manager->app_metadata();
                REQUIRE(app_metadata);
                // Print and verify the location information received from the server
                logger->trace("Deployment model: %1", app_metadata->deployment_model);
                logger->trace("Location: %1", app_metadata->location);
                logger->trace("Hostname: %1", app_metadata->hostname);
                logger->trace("WS Hostname: %1", app_metadata->ws_hostname);
                REQUIRE(app_metadata->hostname.find(app_url) != std::string::npos);
                REQUIRE(app_metadata->ws_hostname.find(ws_url) != std::string::npos);
                redir_transport->simulated_response.reset();
            }
            request_count++;
        };

        // This will be successful after a couple of retries due to the redirect response
        redir_app->provider_client<app::App::UsernamePasswordProviderClient>().register_email(
            creds.email, creds.password, [&](util::Optional<app::AppError> error) {
                REQUIRE(!error);
            });
    }
    SECTION("Test too many redirects") {
        int request_count = 0;
        // initialize app and sync client
        auto redir_app = app::App::get_uncached_app(app_config, sc_config);

        redir_transport->request_hook = [&](const Request& request) {
            logger->trace("Received request[%1]: %2", request_count, request.url);
            REQUIRE(request_count <= 21);
            redir_transport->simulated_response = make_redirect_response(
                request_count % 2 == 1 ? sync::HTTPStatus::PermanentRedirect : sync::HTTPStatus::MovedPermanently,
                "http://somehost:9090");
            request_count++;
        };

        redir_app->log_in_with_credentials(
            realm::app::AppCredentials::username_password(creds.email, creds.password),
            [&](std::shared_ptr<realm::SyncUser> user, util::Optional<app::AppError> error) {
                REQUIRE(!user);
                REQUIRE(error);
                REQUIRE(error->is_client_error());
                REQUIRE(error->code() == ErrorCodes::ClientTooManyRedirects);
                REQUIRE(error->reason() == "number of redirections exceeded 20");
            });
    }
    SECTION("Test app redirect with no metadata") {
        int request_count = 0;
        // initialize app and sync client without persistent metadata
        sc_config.metadata_mode = realm::SyncManager::MetadataMode::NoMetadata;
        auto redir_app = app::App::get_uncached_app(app_config, sc_config);

        redir_transport->request_hook = [&](const Request& request) {
            logger->trace("Received request[%1]: %2", request_count, request.url);
            if (request_count == 0) {
                // HTTP request #1 should be to location - use this request to determine
                // which original host is being used (localhost or 127.0.0.1)
                REQUIRE(request.url.find("/location") != std::string::npos);
                parse_url(request.url);
            }
            else if (request_count == 1) {
                REQUIRE(!request.redirect_count);
                redir_transport->simulated_response = make_redirect_response(
                sync::HTTPStatus::PermanentRedirect, "http://somehost:9090");
            }
            else if (request_count == 2) {
                REQUIRE(request.url.find("http://somehost:9090") != std::string::npos);
                REQUIRE(request.url.find("/location") != std::string::npos);
                // app hostname will be updated via the metadata info
                redir_transport->simulated_response = make_location_response(app_url, ws_url);
            }
            else {
                REQUIRE(request.url.find(app_url) != std::string::npos);
                redir_transport->simulated_response.reset();
            }
            request_count++;
        };

        // This will be successful after a couple of retries due to the redirect response
        redir_app->provider_client<app::App::UsernamePasswordProviderClient>().register_email(
            creds.email, creds.password, [&](util::Optional<app::AppError> error) {
                REQUIRE(!error);
            });
        REQUIRE(!redir_app->sync_manager()->app_metadata()); // no stored app metadata
        REQUIRE(redir_app->sync_manager()->sync_route().find(ws_url) != std::string::npos);

        // Register another email address and verify location data isn't requested again
        request_count = 0;
        redir_transport->request_hook = [&](const Request& request) {
            logger->trace("Received request[%1]: %2", request_count, request.url);
            redir_transport->simulated_response.reset();
            REQUIRE(request.url.find("/location") == std::string::npos);
            request_count++;
        };

        redir_app->provider_client<app::App::UsernamePasswordProviderClient>().register_email(
            creds2.email, creds2.password, [&](util::Optional<app::AppError> error) {
                REQUIRE(!error);
            });
    }

    SECTION("Websocket redirects") {
        auto [promise, logout_future] = util::make_promise_future<bool>();
        util::CopyablePromiseHolder<bool> logout_promise(std::move(promise));

        // Use the transport to grab the current url so it can be converted
        redir_transport->request_hook = [&](const Request& request) {
            logger->trace("Received request: %1", request.url);
            // Parse the URL to determine the scheme, host and port
            parse_url(request.url);
        };

        auto user1 = session.app()->current_user();
        SyncTestFile r_config(user1, partition, schema);
        // Overrride the default
        r_config.sync_config->error_handler = [&logger, promise = std::move(logout_promise)](
                                                  std::shared_ptr<SyncSession>, SyncError error) mutable {
            if (error.get_system_error() == sync::make_error_code(realm::sync::ProtocolError::bad_authentication)) {
                logger->error("Websocket redirect test: User logged out\n");
                promise.get_promise().emplace_value(true);
                return;
            }
            promise.get_promise().set_error(error.to_status());
        };

        auto r = Realm::get_shared_realm(r_config);
        REQUIRE(!wait_for_download(*r));

        SECTION("Valid websocket redirect") {
            auto sync_manager = session.app()->sync_manager();
            auto sync_session = sync_manager->get_existing_session(r->config().path);
            sync_session->pause();

            int connect_count = 0;
            logger->debug(">>> Session paused - Setting up for 'Valid websocket redirect' test"); // DEBUGGING
            redir_provider->handshake_response_func =
                [&logger, &connect_count]() -> std::optional<std::pair<int, std::string>> {
                // Only return the simulated response on the first connection attempt
                if (connect_count++ > 0)
                    return {};

                logger->debug("Received websocket request; returning PermanentRedirect");
                return std::make_pair(static_cast<int>(sync::HTTPStatus::PermanentRedirect), "");
            };
            int request_count = 0;
            redir_transport->request_hook = [&](const Request& request) {
                logger->debug("Received request[%1]: '%2'", request_count, request.url);
                logger->debug("app_url: '%1'", app_url); // DEBUGGING
                std::cerr << std::flush;                 // DEBUGGING
                sleep(2);                                // DEBUGGING
                if (request_count++ == 0) {
                    // First request should be a location request against the original URL
                    REQUIRE(request.url.find(app_url) != std::string::npos);
                    REQUIRE(request.url.find("/location") != std::string::npos);
                    REQUIRE(request.redirect_count == 0);
                    redir_transport->simulated_response =
                        make_redirect_response(sync::HTTPStatus::PermanentRedirect, redir_app_url);
                }
                // Otherwise, if there are any location requests, respond with the redirect URLs
                else if (request.url.find("/location") != std::string::npos) {
                    redir_transport->simulated_response = make_location_response(redir_app_url, redir_ws_url);
                }
                else {
                    redir_transport->simulated_response.reset();
                }
            };

            SyncManager::OnlyForTesting::voluntary_disconnect_all_connections(*sync_manager);
            sync_session->resume();
            REQUIRE(!wait_for_download(*r));
            REQUIRE(user1->is_logged_in());

            // Verify session is using the updated server url from the redirect
            auto server_url = sync_session->full_realm_url();
            logger->trace("FULL_REALM_URL: %1", server_url);
            REQUIRE((server_url && server_url->find(redir_ws_url) != std::string::npos));
        }
        SECTION("Websocket redirect logs out user") {
            auto sync_manager = session.app()->sync_manager();
            auto sync_session = sync_manager->get_existing_session(r->config().path);
            sync_session->pause();

            int connect_count = 0;
            redir_provider->handshake_response_func =
                [&connect_count]() -> std::optional<std::pair<int, std::string>> {
                // Only return the simulated response on the first connection attempt
                if (connect_count++ > 0)
                    return {};

                return std::make_pair(static_cast<int>(sync::HTTPStatus::MovedPermanently), "");
            };
            int request_count = 0;
            redir_transport->request_hook = [&](const Request& request) {
                logger->trace("Received request[%1]: %2", request_count, request.url);
                if (request_count++ == 0) {
                    // First request should be a location request against the original URL
                    REQUIRE(request.url.find(original_host) != std::string::npos);
                    REQUIRE(request.url.find("/location") != std::string::npos);
                    REQUIRE(request.redirect_count == 0);
                    redir_transport->simulated_response =
                        make_redirect_response(sync::HTTPStatus::MovedPermanently, redir_app_url);
                }
                else if (request.url.find("/location") != std::string::npos) {
                    redir_transport->simulated_response = make_location_response(redir_app_url, redir_ws_url);
                }
                else if (request.url.find("auth/session") != std::string::npos) {
                    redir_transport->simulated_response = {static_cast<int>(sync::HTTPStatus::Unauthorized),
                                                           0,
                                                           {{"Content-Type", "application/json"}},
                                                           ""};
                }
                else {
                    redir_transport->simulated_response.reset();
                }
            };

            SyncManager::OnlyForTesting::voluntary_disconnect_all_connections(*sync_manager);
            sync_session->resume();
            REQUIRE(wait_for_download(*r));
            auto result = wait_for_future(std::move(logout_future), std::chrono::seconds(15)).get_no_throw();
            if (!result.is_ok()) {
                logger->error("Redirect logout error: %1", result.get_status().reason());
            }
            REQUIRE(result.is_ok());
            REQUIRE(!user1->is_logged_in());
        }
        SECTION("Too many websocket redirects logs out user") {
            auto sync_manager = session.app()->sync_manager();
            auto sync_session = sync_manager->get_existing_session(r->config().path);
            sync_session->pause();

            int connect_count = 0;
            redir_provider->handshake_response_func =
                [&connect_count]() -> std::optional<std::pair<int, std::string>> {
                // Only return the simulated response on the first connection attempt
                if (connect_count++ > 0)
                    return {};

                return std::make_pair(static_cast<int>(sync::HTTPStatus::MovedPermanently), "");
            };
            int request_count = 0;
            const int max_http_redirects = 20; // from app.cpp in object-store
            redir_transport->request_hook = [&](const Request& request) {
                logger->trace("Received request[%1]: %2", request_count, request.url);
                if (request_count++ == 0) {
                    // First request should be a location request against the original URL
                    REQUIRE(request.url.find(app_url) != std::string::npos);
                    REQUIRE(request.url.find("/location") != std::string::npos);
                    REQUIRE(request.redirect_count == 0);
                }
                if (request.url.find("/location") != std::string::npos) {
                    // Keep returning the redirected response
                    REQUIRE(request.redirect_count < max_http_redirects);
                    redir_transport->simulated_response =
                        make_redirect_response(sync::HTTPStatus::MovedPermanently, redir_app_url);
                }
                else {
                    // should not get any other types of requests during the test - the log out is local
                    REQUIRE(false);
                }
            };

            SyncManager::OnlyForTesting::voluntary_disconnect_all_connections(*sync_manager);
            sync_session->resume();
            REQUIRE(wait_for_download(*r));
            auto result = wait_for_future(std::move(logout_future), std::chrono::seconds(15)).get_no_throw();
            if (!result.is_ok()) {
                logger->error("Redirect logout error: %1", result.get_status().reason());
            }
            REQUIRE(result.is_ok());
            REQUIRE(!user1->is_logged_in());
        }
    }
}

#endif // REALM_ENABLE_AUTH_TESTS
#endif // REALM_ENABLE_SYNC
