/*
 * Copyright 2015 Realm Inc.
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

#ifndef DATADOG_CXX_STATSD_HPP_20160106
#define DATADOG_CXX_STATSD_HPP_20160106

#include <string>

#include "dogless/stats_collector.hpp"

#include "dogless/utils/misc.hpp"
#include "dogless/utils/random.hpp"
#include "dogless/utils/sockets.hpp"
#include "dogless/utils/io.hpp"

namespace dogless {

using std::string;

namespace _impl {

template <typename SocketType>
class Statsd : public StatsCollectorBase {
protected:
    inline Statsd(string const& prefix, string const& hostname, int port);

    inline Statsd(std::vector<string> const& endpoints, string const& prefix);

    // modifiers
public:
    inline void add_endpoint(string const& endpoint);

    inline void add_endpoint(string const& hostname, int port);

    inline void add_endpoints(std::vector<string> const& endpoints);

    inline void prefix(string const& prefix);

    inline void prefix(string&& prefix);

    // main API
    void decrement(const char* metric, int value = 1, float sample_rate = 1.0, const char* eol = "\n") final;

    void gauge(const char* metric, double value, float sample_rate = 1.0, const char* eol = "\n") final;

    void gauge_relative(const char* metric, double amount, float sample_rate = 1.0, const char* eol = "\n") final;

    void histogram(const char* metric, double value, float sample_rate = 1.0, const char* eol = "\n") final;

    void increment(const char* metric, int value = 1, float sample_rate = 1.0, const char* eol = "\n") final;

    void timing(const char* metric, double value, float sample_rate = 1.0, const char* eol = "\n") final;

private:
    // internal methods
    inline void report(const char* metric, const char* metric_type, string const& value, float sample_rate,
                       string const& eol = "\n");

private:
    // properties
    utils::IOServiceRunner m_io_service;
    utils::Random<0, 1> m_random;
    string m_prefix;

protected:
    SocketType m_socket;
};

// ctors

template <typename SocketType>
inline Statsd<SocketType>::Statsd(string const& prefix, string const& hostname, int port)
    : m_io_service()
    , m_socket(m_io_service(), hostname, port)
{
    this->prefix(prefix);
}

template <typename SocketType>
inline Statsd<SocketType>::Statsd(std::vector<string> const& endpoints, string const& prefix)
    : m_io_service()
    , m_socket(m_io_service(), endpoints)
{
    this->prefix(prefix);
}


// modifiers

template <typename SocketType>
inline void Statsd<SocketType>::add_endpoint(string const& endpoint)
{
    m_socket.add_endpoint(endpoint);
}

template <typename SocketType>
inline void Statsd<SocketType>::add_endpoint(string const& hostname, int port)
{
    m_socket.add_endpoint(hostname, port);
}

template <typename SocketType>
inline void Statsd<SocketType>::add_endpoints(std::vector<string> const& endpoints)
{
    m_socket.add_endpoints(endpoints);
}
template <typename SocketType>
inline void Statsd<SocketType>::decrement(const char* metric, int value, float sample_rate, const char* eol)
{
    increment(metric, value * -1, sample_rate, eol);
}

template <typename SocketType>
inline void Statsd<SocketType>::prefix(string const& prefix)
{
    if (!prefix.empty()) {
        m_prefix = prefix;
        m_prefix += '.';
    }
}

template <typename SocketType>
inline void Statsd<SocketType>::prefix(string&& prefix)
{
    m_prefix = std::move(prefix);
    if (!m_prefix.empty()) {
        m_prefix += '.';
    }
}

// main API

template <typename SocketType>
void Statsd<SocketType>::gauge(const char* metric, double value, float sample_rate, const char* eol)
{
    report(metric, "g", utils::to_string(value), sample_rate, eol);
}

template <typename SocketType>
void Statsd<SocketType>::gauge_relative(const char* metric, double amount, float sample_rate, const char* eol)
{
    std::string str;

    if (amount >= 0.) {
        str = '+' + utils::to_string(amount);
    }
    else {
        str = utils::to_string(amount);
    }

    report(metric, "g", str, sample_rate, eol);
}

template <typename SocketType>
void Statsd<SocketType>::histogram(const char* metric, double value, float sample_rate, const char* eol)
{
    report(metric, "h", utils::to_string(value), sample_rate, eol);
}

template <typename SocketType>
void Statsd<SocketType>::increment(const char* metric, int value, float sample_rate, const char* eol)
{
    report(metric, "c", utils::to_string(value), sample_rate, eol);
}

template <typename SocketType>
void Statsd<SocketType>::timing(const char* metric, double value, float sample_rate, const char* eol)
{
    report(metric, "ms", utils::to_string(value), sample_rate, eol);
}

// internal methods

template <typename SocketType>
void Statsd<SocketType>::report(const char* metric, const char* metric_type, string const& value, float sample_rate,
                                string const& eol)
{
    if (sample_rate == 0.0f || (sample_rate != 1.0f && m_random() > sample_rate))
        return;

    std::stringstream ss;

    if (!m_prefix.empty()) {
        ss << m_prefix;
    }

    ss << metric << ':' << value << '|' << metric_type;

    if (sample_rate != 1.0f)
        ss << "|@" << utils::to_string(sample_rate);

    ss << eol;

    m_socket.send(ss.str());
}

} // namespace _impl

/* Raw metrics sender. Everything is sent as soon as it is reported by the
 * code.
 *
 * Features:
 * - Supports sending to multiple endpoints.
 * - Supports delivery failure detection, with automatic back-off.
 */
class UnbufferedStatsd : public _impl::Statsd<utils::UDPSocket> {
public:
    // ctors & dtors
    UnbufferedStatsd(string const& prefix = {}, string const& hostname = "localhost", int port = 8125);
    UnbufferedStatsd(std::vector<string> const& endpoints, string const& prefix = {});
    UnbufferedStatsd(UnbufferedStatsd const&) = delete;
};

/* Buffered metrics sender. All metrics are buffered until one of two things
 * happens:
 * - the buffer is about to exceed the configured MTU size,
 * - the maximum send delay has been exceeded.
 *
 * By default, the MTU size is 508 bytes, and the initial loop time is 1
 * second.
 *
 * Features:
 * - Sending to multiple endpoints.
 * - Delivery failure detection, with automatic back-off.
 * - Tries to fit as many metrics in a single UDP packet as possible.
 * - Supports jumbo frames.
 * - Relatively thread-safe.
 *
 * Recommendations:
 * - If you are sending your metrics to localhost, on Linux, you should
 *   typically be able to use Jumbo frames (65kB), which allows for more
 *   efficient sending/receiving.
 */
class BufferedStatsd : public _impl::Statsd<utils::BufferedUDPSocket> {
public:
    // ctors & dtors
    BufferedStatsd(string const& prefix = {}, string const& hostname = "localhost", int port = 8125,
                   std::size_t mtu = MTU_InternetSafe);
    BufferedStatsd(std::vector<string> const& endpoints, string const& prefix = {},
                   std::size_t mtu = MTU_InternetSafe);
    BufferedStatsd(BufferedStatsd const&) = delete;

    // accessors
    int loop_interval() const noexcept;
    std::size_t mtu() const noexcept;

    // modifiers
    void loop_interval(int interval) noexcept;
    void mtu(std::size_t mtu) noexcept;

    // main API
    void flush();
};

} // namespace dogless

#endif // DATADOG_CXX_STATSD_HPP_20160106
