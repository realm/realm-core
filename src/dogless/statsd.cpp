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

#include "dogless/statsd.hpp"

#include <functional>
#include <sstream>

namespace dogless {

// ctors and dtors

UnbufferedStatsd::UnbufferedStatsd(string const& prefix, string const& hostname, int port)
    : Statsd(prefix, hostname, port)
{
}

UnbufferedStatsd::UnbufferedStatsd(std::vector<string> const& endpoints, string const& prefix)
    : Statsd(endpoints, prefix)
{
}

// ctors and dtors

BufferedStatsd::BufferedStatsd(string const& prefix, string const& hostname, int port, std::size_t mtu)
    : Statsd(prefix, hostname, port)
{
    m_socket.mtu(mtu);
}

BufferedStatsd::BufferedStatsd(std::vector<string> const& endpoints, string const& prefix, std::size_t mtu)
    : Statsd(endpoints, prefix)
{
    m_socket.mtu(mtu);
}

// accessors

int BufferedStatsd::loop_interval() const noexcept
{
    return m_socket.loop_interval();
}

std::size_t BufferedStatsd::mtu() const noexcept
{
    return m_socket.mtu();
}

// modifiers

void BufferedStatsd::loop_interval(int interval) noexcept
{
    m_socket.loop_interval(interval);
}

void BufferedStatsd::mtu(std::size_t mtu) noexcept
{
    m_socket.mtu(mtu);
}

// main API

void BufferedStatsd::flush()
{
    m_socket.flush();
}

} // namespace dogless
