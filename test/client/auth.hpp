/*************************************************************************
 *
 * Copyright 2018 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#ifndef REALM_SYNC_AUTH_HPP
#define REALM_SYNC_AUTH_HPP

#include <cstdint>
#include <limits>
#include <functional>
#include <set>
#include <deque>
#include <random>

#include <realm/util/logger.hpp>
#include <realm/util/http.hpp>
#include <realm/util/optional.hpp>
#include <realm/util/network_ssl.hpp>


namespace realm {
namespace sync {
namespace auth {

enum class Error {
    unexpected_response_status = 1,
    unauthorized,
    bad_syntax,
};

const std::error_category& auth_error_category() noexcept;
std::error_code make_error_code(Error) noexcept;


class Client {
public:
    using port_type = util::network::Endpoint::port_type;
    using LoginHandler = void(std::error_code, std::string access_token, std::string refresh_token);
    using RefreshHandler = void(std::error_code, std::string access_token);
    using SSLVerifyCallback = bool(const std::string& server_address, port_type server_port, const char* pem_data,
                                   std::size_t pem_size, int preverify_ok, int depth);

    class Config {
    public:
        Config() {}

        /// The logger to be used by the auth client. If no logger is specified,
        /// the client will use an instance of util::StderrLogger with the log
        /// level threshold set to util::Logger::Level::info. The client does
        /// not require a thread-safe logger, and it guarantees that all logging
        /// happens either on behalf of the constructor or on behalf of the
        /// invocation of run().
        util::Logger* logger = nullptr;

        /// Specifies the maximum number of TCP connections the client can have
        /// to the auth server at any point in time.
        std::int_fast64_t max_number_of_connections = std::numeric_limits<std::int_fast64_t>::max();

        /// See Client::Config::verify_servers_ssl_certificate.
        bool verify_servers_ssl_certificate = true;

        /// See Client::Config::ssl_trust_certificate_path.
        util::Optional<std::string> ssl_trust_certificate_path;

        /// See Client::Config::ssl_verify_callback.
        std::function<SSLVerifyCallback> ssl_verify_callback;

        std::string request_base_path = "/api/client/v2.0";
    };

    util::Logger& logger;

    /// The client runs in its own thread with an event loop. auth_address and
    /// auth_port specifies the address and port of an username/password
    /// authentication server. Typically, an instance of Realm Object Server
    /// would be running at the address.
    Client(bool auth_ssl, std::string auth_address, port_type auth_port, std::string app_id, Config = {});

    /// Starts the event loop. This function is thread safe.
    void run();

    /// Stops the event loop. This function is thread safe.
    void stop() noexcept;

    //@{
    /// The specified handler will be called exactly once, as long as the event
    /// loop keeps running. The handler will be called by the client's event
    /// loop thread.
    ///
    /// If the operation is successful, the handler will be called with the
    /// access token and the refresh token, and no error. Otherwise, it will be
    /// called with an appropriate error code.
    ///
    /// These functions are thread-safe.
    void login_anon(std::function<LoginHandler>);
    void login_user(std::string username, std::string password, std::function<LoginHandler>);
    //@}

    /// The specified handler will be called exactly once, as long as the event
    /// loop keeps running. The handler will be called by the client's event
    /// loop thread.
    ///
    /// If the operation is successful, the handler will be called with the new
    /// access token, and no error. Otherwise, it will be called with an
    /// appropriate error code.
    ///
    /// This functions is thread-safe.
    void refresh(std::string refresh_token, std::function<RefreshHandler>);

    util::network::Service& get_service();
    const std::string& get_auth_address();
    port_type get_auth_port();
    void request_is_done(std::int_fast64_t request_counter);

    std::mt19937_64& get_random();

    ~Client() noexcept;

private:
    class Request;
    class LoginRequest;
    class RefreshRequest;

    util::network::Service m_service;
    const bool m_auth_ssl;
    const std::string m_auth_address;
    const port_type m_auth_port;
    const std::string m_http_host; // Contents of `Host:` request header
    const std::int_fast64_t m_max_number_of_connections;
    const bool m_verify_servers_ssl_certificate;
    const util::Optional<std::string> m_ssl_trust_certificate_path;
    const std::function<SSLVerifyCallback> m_ssl_verify_callback;
    const std::string m_request_base_path;
    const std::string m_app_request_path;

    std::mt19937_64 m_random;

    util::network::DeadlineTimer m_keep_running_timer;

    std::int_fast64_t m_request_counter = 0;

    // Map of all requests indexed by their counter.
    std::map<std::int_fast64_t, std::unique_ptr<Request>> m_requests;

    // Smallest request_counter where the request has not been initiated.
    std::int_fast64_t m_pending_request_counter = 0;

    // The number requests in progress. This number is limited by
    // max_number_of_connections.
    std::int_fast64_t m_active_requests = 0;

    void start_keep_running_timer();

    // Will be called on the event loop thread by a post handler.
    void do_login_anon(std::function<LoginHandler>);
    void do_login_user(std::string username, std::string password, std::function<LoginHandler>);
    void do_refresh(std::string access_token, std::function<RefreshHandler>);

    void schedule_requests();
};

} // namespace auth
} // namespace sync
} // namespace realm

namespace std {

template <>
struct is_error_code_enum<realm::sync::auth::Error> {
    static const bool value = true;
};

} // namespace std

#endif // REALM_SYNC_AUTH_HPP
