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

#ifndef DATADOG_CXX_UTILS_SOCKETS_HPP_20160112
#define DATADOG_CXX_UTILS_SOCKETS_HPP_20160112

#include <mutex>
#include <vector>

#include <asio/deadline_timer.hpp>
#include <asio/io_service.hpp>
#include <asio/ip/udp.hpp>

namespace dogless {

/* The minimum MTU a host can set is 576. If we account for the longest IP
 * headers (60 bytes), and UDP headers (8 bytes), this brings the safest UDP
 * payload down to 508 bytes.
 * Alternatively, if you know that your MTU is 1500 (fairly typical on a good
 * LAN), then you could potentially get away with a `1500 - 68 = 1432 bytes`
 * MTU.
 * On Linux, if your network stack supports jumbo packets, it might even be
 * possible for you to fly with a 64 KiB MTU. Hence your maximum payload would
 * be: `65536 - 68 = 65468`. On OSX, jumbo packets are limited to 9216 bytes.
 * Accounting for headers, this gives an effective payload size of 9148 bytes.
 */
enum MTU {
    MTU_InternetSafe = 508,
    MTU_LAN = 1432,
#if defined(__APPLE__) // Apple platforms
    MTU_Jumbo = 9148,
#else  // Linux
    MTU_Jumbo = 65468
#endif // Windows not accounted for
};

namespace utils {

class UDPSocket {
public:
    // ctors/dtors
    UDPSocket(asio::io_service& io_service, std::string const& hostname, int port);
    UDPSocket(asio::io_service& io_service, std::vector<std::string> const& endpoints);
    UDPSocket(asio::io_service& io_service);
    UDPSocket(UDPSocket const&) = delete;

    // main API
    void add_endpoint(std::string const& endpoint);
    void add_endpoint(std::string const& hostname, int port);
    void add_endpoints(std::vector<std::string> const& endpoints);
    void send(std::string const& line);

private:
    // internal
    void add_endpoint(std::string const& hostname, std::string const& port);
    void back_off();

private:
    // properties
    bool m_backing_off = false;
    std::uint32_t m_reconnect_attempts = 0;
    asio::io_service& m_io_service;
    asio::ip::udp::socket m_socket;
    asio::deadline_timer m_backoff_timer;
    std::mutex m_socket_mutex;
    std::vector<asio::ip::udp::endpoint> m_endpoints;
};

class BufferedUDPSocket {
public:
    // ctors/detors
    BufferedUDPSocket(asio::io_service& io_service, std::string const& hostname, int port,
                      std::size_t mtu = MTU_InternetSafe);
    BufferedUDPSocket(asio::io_service& io_service, std::vector<std::string> const& endpoints,
                      std::size_t mtu = MTU_InternetSafe);
    BufferedUDPSocket(asio::io_service& io_service, std::size_t mtu = MTU_InternetSafe);
    BufferedUDPSocket(BufferedUDPSocket const&) = delete;

    // accessors
    int loop_interval() const noexcept;
    std::size_t mtu() const noexcept;

    // modifiers
    void add_endpoint(std::string const& endpoint);
    void add_endpoint(std::string const& hostname, int port);
    void add_endpoints(std::vector<std::string> const& endpoints);
    void loop_interval(int interval) noexcept;
    void mtu(std::size_t mtu) noexcept;

    // main API
    void send(std::string const& line);
    void flush();

private:
    // internal methods
    void send_loop(asio::error_code ec);

private:
    // properties
    UDPSocket m_socket;
    std::size_t m_mtu;
    std::mutex m_buffer_mutex;
    std::string m_buffer;
    asio::deadline_timer m_send_timer;
    int m_interval = 1;
    bool m_loop_running = false;
};

} // namespace utils
} // namespace dogless

#endif // DATADOG_CXX_UTILS_SOCKETS_HPP_20160112
