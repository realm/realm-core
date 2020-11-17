/*
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "dogless/utils/sockets.hpp"

namespace {

std::vector<std::string> split(std::string const& input, char delim)
{
    std::vector<std::string> matches;
    std::stringstream ss;
    std::string token;

    ss << input;

    while (std::getline(ss, token, delim)) {
        if (!token.empty())
            matches.push_back(token);
    }

    return matches;
}

} // anonymous namespace

namespace dogless {
namespace utils {

// ctors/dtors

UDPSocket::UDPSocket(asio::io_service& io_service, std::string const& hostname, int port)
    : UDPSocket(io_service)
{
    add_endpoint(hostname, port);
}

UDPSocket::UDPSocket(asio::io_service& io_service, std::vector<std::string> const& endpoints)
    : UDPSocket(io_service)
{
    add_endpoints(endpoints);
}

UDPSocket::UDPSocket(asio::io_service& io_service)
    : m_io_service(io_service)
    , m_socket(m_io_service, asio::ip::udp::endpoint(asio::ip::udp::v4(), 0))
    , m_backoff_timer(m_io_service)
{
    m_backoff_timer.expires_at(boost::posix_time::pos_infin);
}

// main API

void UDPSocket::add_endpoint(std::string const& endpoint)
{
    auto endpoint_parts = split(endpoint, ':');

    if (endpoint_parts.size() != 2) {
        // throw
    }

    std::string hostname = endpoint_parts[0];
    std::string port = endpoint_parts[1];

    add_endpoint(hostname, port);
}

void UDPSocket::add_endpoint(std::string const& hostname, int port)
{
    add_endpoint(hostname, std::to_string(port));
}

void UDPSocket::add_endpoints(std::vector<std::string> const& endpoints)
{
    for (auto const& endpoint : endpoints) {
        add_endpoint(endpoint);
    }
}

// internal

void UDPSocket::add_endpoint(std::string const& hostname, std::string const& port)
{
    asio::ip::udp::resolver resolver(m_io_service);
    asio::ip::udp::resolver::query query(asio::ip::udp::v4(), hostname, port);

    m_endpoints.push_back(*resolver.resolve(query));
}

void UDPSocket::send(std::string const& line)
{
    if (!m_backing_off) {
        std::lock_guard<std::mutex> lock(m_socket_mutex);
        size_t errors = 0;

        auto buffer = asio::buffer(line.data(), line.size());
        for (auto& endpoint : m_endpoints) {
            try {
                m_socket.send_to(buffer, endpoint);
            }
            catch (asio::system_error const& e) {
                // We count the number of times that we've had an error while
                // sending. This typically means that there's either a problem
                // in the configuration, or that the port is actively blocked.
                ++errors;
            }
        }

        // If every single destination errors out, we can start backing off. If
        // one of the destinations is able to receive to our packets without a
        // problem (or at least, appears to be, from our perspective), then we
        // shouldn't back off, and keep sending.
        if (errors == m_endpoints.size()) {
            back_off();
            return;
        }

        if (m_reconnect_attempts != 0) {
            m_reconnect_attempts = 0;
        }
    }
}

void UDPSocket::back_off()
{
    m_backing_off = true;
    ++m_reconnect_attempts;

    m_backoff_timer.expires_from_now(boost::posix_time::seconds(2 * m_reconnect_attempts));
    m_backoff_timer.async_wait([&](const asio::error_code) {
        m_backing_off = false;
    });
}

// ctors & dtors

BufferedUDPSocket::BufferedUDPSocket(asio::io_service& io_service, std::string const& hostname, int port,
                                     std::size_t mtu)
    : m_socket(io_service, hostname, port)
    , m_mtu(mtu)
    , m_send_timer(io_service)
{
    m_send_timer.expires_at(boost::posix_time::pos_infin);
}

BufferedUDPSocket::BufferedUDPSocket(asio::io_service& io_service, std::vector<std::string> const& endpoints,
                                     std::size_t mtu)
    : m_socket(io_service, endpoints)
    , m_mtu(mtu)
    , m_send_timer(io_service)
{
    m_send_timer.expires_at(boost::posix_time::pos_infin);
}

BufferedUDPSocket::BufferedUDPSocket(asio::io_service& io_service, std::size_t mtu)
    : m_socket(io_service)
    , m_mtu(mtu)
    , m_send_timer(io_service)
{
    m_send_timer.expires_at(boost::posix_time::pos_infin);
}

// accessors

int BufferedUDPSocket::loop_interval() const noexcept
{
    return m_interval;
}

std::size_t BufferedUDPSocket::mtu() const noexcept
{
    return m_mtu;
}

// modifiers

void BufferedUDPSocket::add_endpoint(std::string const& endpoint)
{
    m_socket.add_endpoint(endpoint);
}

void BufferedUDPSocket::add_endpoint(std::string const& hostname, int port)
{
    m_socket.add_endpoint(hostname, port);
}

void BufferedUDPSocket::add_endpoints(std::vector<std::string> const& endpoints)
{
    m_socket.add_endpoints(endpoints);
}

void BufferedUDPSocket::loop_interval(int interval) noexcept
{
    m_interval = interval;

    if (!m_loop_running && m_interval > 0) {
        m_loop_running = true;
        send_loop(asio::error_code());
    }
}

void BufferedUDPSocket::mtu(std::size_t mtu) noexcept
{
    m_mtu = mtu;
}

// main API

void BufferedUDPSocket::send(std::string const& line)
{
    std::lock_guard<std::mutex> lock(m_buffer_mutex);

    if ((line.size() + m_buffer.size()) < m_mtu)
        m_buffer += line;

    else {
        m_socket.send(m_buffer);

        if (line.size() >= m_mtu) {
            m_socket.send(line);
            m_buffer = "";
        }

        else
            m_buffer = line;
    }
}

void BufferedUDPSocket::flush()
{
    std::lock_guard<std::mutex> lock(m_buffer_mutex);

    if (!m_buffer.empty()) {
        m_socket.send(m_buffer);
        m_buffer = "";
    }
}

void BufferedUDPSocket::send_loop(asio::error_code ec)
{
    if (m_loop_running) {
        using namespace std::placeholders;

        m_send_timer.expires_from_now(boost::posix_time::seconds(m_interval));
        m_send_timer.async_wait(std::bind(&BufferedUDPSocket::send_loop, this, _1));

        if (!ec) {
            flush();
        }
    }
}

} // namespace utils
} // namespace dogless
