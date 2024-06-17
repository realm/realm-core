////////////////////////////////////////////////////////////////////////////
//
// Copyright 2024 Realm Inc.
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

#if defined(REALM_ENABLE_SYNC) && defined(REALM_ENABLE_AUTH_TESTS)

#include <realm/sync/network/http.hpp>
#include <realm/sync/network/network.hpp>
#include <realm/sync/network/websocket.hpp>
#include <realm/util/future.hpp>
#include <realm/util/uri.hpp>
#include <external/json/json.hpp>

#include <catch2/catch_all.hpp>

#include <thread>

namespace realm::sync {

class RedirectingHttpServer {
public:
    RedirectingHttpServer(std::string redirect_to_base_url, std::shared_ptr<util::Logger> logger)
        : m_redirect_to_base_url(std::move(redirect_to_base_url))
        , m_logger(std::make_shared<util::PrefixLogger>("HTTP Redirector ", std::move(logger)))
        , m_acceptor(m_service)
        , m_server_thread([this] {
            m_service.run_until_stopped();
        })
    {
        m_acceptor.open(m_endpoint.protocol());
        m_acceptor.bind(m_endpoint);
        m_endpoint = m_acceptor.local_endpoint();
        m_acceptor.listen();
        m_service.post([this](Status status) {
            REALM_ASSERT(status.is_ok());
            do_accept();
        });
    }

    ~RedirectingHttpServer()
    {
        m_acceptor.cancel();
        m_service.stop();
        m_server_thread.join();
    }

    std::string base_url() const
    {
        return util::format("http://localhost:%1", m_endpoint.port());
    }

private:
    struct BufferedSocket : network::Socket {
        BufferedSocket(network::Service& service)
            : network::Socket(service)
        {
        }

        BufferedSocket(network::Service& service, const network::StreamProtocol& protocol,
                       native_handle_type native_handle)
            : network::Socket(service, protocol, native_handle)
        {
        }


        template <class H>
        void async_read_until(char* buffer, std::size_t size, char delim, H handler)
        {
            network::Socket::async_read_until(buffer, size, delim, m_read_buffer, std::move(handler));
        }

        template <class H>
        void async_read(char* buffer, std::size_t size, H handler)
        {
            network::Socket::async_read(buffer, size, m_read_buffer, std::move(handler));
        }

    private:
        network::ReadAheadBuffer m_read_buffer;
    };

    struct Conn : public util::RefCountBase, websocket::Config {
        Conn(network::Service& service, const std::shared_ptr<util::Logger>& logger)
            : random(Catch::getSeed())
            , logger(logger)
            , socket(service)
            , http_server(socket, logger)
        {
        }

        // Implement the websocket::Config interface
        const std::shared_ptr<util::Logger>& websocket_get_logger() noexcept override
        {
            return logger;
        }

        std::mt19937_64& websocket_get_random() noexcept override
        {
            return random;
        }

        void async_write(const char* data, size_t size, websocket::WriteCompletionHandler handler) override
        {
            socket.async_write(data, size, std::move(handler));
        }

        void async_read(char* buffer, size_t size, websocket::ReadCompletionHandler handler) override
        {
            socket.async_read(buffer, size, std::move(handler));
        }

        void async_read_until(char* buffer, size_t size, char delim,
                              websocket::ReadCompletionHandler handler) override
        {
            socket.async_read_until(buffer, size, delim, std::move(handler));
        }

        void websocket_handshake_completion_handler(const HTTPHeaders&) override {}

        void websocket_read_error_handler(std::error_code) override {}

        void websocket_write_error_handler(std::error_code) override {}

        void websocket_handshake_error_handler(std::error_code, const HTTPHeaders*, std::string_view) override {}

        void websocket_protocol_error_handler(std::error_code) override {}

        bool websocket_text_message_received(const char*, size_t) override
        {
            return false;
        }

        bool websocket_binary_message_received(const char*, size_t) override
        {
            return false;
        }

        bool websocket_close_message_received(websocket::WebSocketError, std::string_view) override
        {
            return false;
        }

        bool websocket_ping_message_received(const char*, size_t) override
        {
            return false;
        }

        bool websocket_pong_message_received(const char*, size_t) override
        {
            return false;
        }

        std::mt19937_64 random;
        const std::shared_ptr<util::Logger> logger;
        BufferedSocket socket;
        HTTPServer<BufferedSocket> http_server;
        std::optional<websocket::Socket> websocket;
    };

    void send_simple_response(util::bind_ptr<Conn> conn, HTTPStatus status, std::string reason, std::string body)
    {
        HTTPResponse resp;
        resp.status = status;
        resp.reason = std::move(reason);
        resp.body = std::move(body);
        m_logger->debug("Sending http response %1: %2", status, reason);
        conn->http_server.async_send_response(resp, [this, conn](std::error_code ec) {
            if (ec && ec != util::error::operation_aborted) {
                m_logger->warn("Error sending response: %1", ec);
            }
        });
    }

    void do_websocket_redirect(util::bind_ptr<Conn> conn, const HTTPRequest& req)
    {
        auto protocol_it = req.headers.find("Sec-WebSocket-Protocol");
        REALM_ASSERT(protocol_it != req.headers.end());
        auto protocols = protocol_it->second;

        auto first_comma = protocols.find(',');
        std::string protocol;
        if (first_comma == std::string::npos) {
            protocol = protocols;
        }
        else {
            protocol = protocols.substr(0, first_comma);
        }
        std::error_code ec;
        auto maybe_resp = websocket::make_http_response(req, protocol, ec);
        REALM_ASSERT(maybe_resp);
        REALM_ASSERT(!ec);
        conn->http_server.async_send_response(*maybe_resp, [this, conn](std::error_code ec) {
            if (ec) {
                if (ec != util::error::operation_aborted) {
                    m_logger->warn("Error sending websocket HTTP upgrade response: %1", ec);
                }
            }

            conn->websocket.emplace(*conn);
            conn->websocket->initiate_server_websocket_after_handshake();

            static const std::string_view msg("\x0f\xa3Permanently moved");
            conn->websocket->async_write_close(msg.data(), msg.size(), [conn](std::error_code, size_t) {
                conn->logger->debug("Sent close frame with move code");
                conn->websocket.reset();
            });
        });
    }

    void do_accept()
    {
        auto conn = util::make_bind<Conn>(m_service, m_logger);
        m_acceptor.async_accept(conn->socket, [this, conn](std::error_code ec) {
            if (ec == util::error::operation_aborted) {
                return;
            }
            do_accept();
            if (ec) {
                m_logger->error("Error accepting new connection: %1", ec);
                return;
            }

            conn->http_server.async_receive_request([this, conn](HTTPRequest req, std::error_code ec) {
                if (ec) {
                    if (ec != util::error::operation_aborted) {
                        m_logger->error("Error receiving HTTP request to redirect: %1", ec);
                    }
                    return;
                }

                if (req.path.find("/location") != std::string::npos) {
                    std::string_view base_url(m_redirect_to_base_url);
                    auto scheme = base_url.find("://");
                    auto ws_url = util::format("ws%1", base_url.substr(scheme));
                    nlohmann::json body{{"deployment_model", "GLOBAL"},
                                        {"location", "US-VA"},
                                        {"hostname", m_redirect_to_base_url},
                                        {"ws_hostname", ws_url}};
                    auto body_str = body.dump();

                    m_logger->debug("sending fake location update: %1", body_str);
                    send_simple_response(conn, HTTPStatus::Ok, "Okay", std::move(body_str));
                    return;
                }

                if (req.path.find("/realm-sync") != std::string::npos) {
                    do_websocket_redirect(conn, req);
                    return;
                }

                send_simple_response(conn, HTTPStatus::NotFound, "Not found", {});
            });
        });
    }

    const std::string m_redirect_to_base_url;
    const std::shared_ptr<util::Logger> m_logger;
    network::Service m_service;
    network::Acceptor m_acceptor;
    network::Endpoint m_endpoint;
    std::thread m_server_thread;
};

} // namespace realm::sync

#endif
