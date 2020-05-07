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

#include "dogless/dogstatsd.hpp"

#include <sstream>

namespace dogless {

// ctors and dtors

DogStatsd::DogStatsd(Tags const& default_tags)
    : m_default_tags(default_tags)
{
}

// accessors

DogStatsd::Tags const& DogStatsd::default_tags() const noexcept
{
    return m_default_tags;
}

// modifiers

void DogStatsd::default_tags(Tags const& default_tags)
{
    m_default_tags = default_tags;
}

// internal methods

string DogStatsd::build_eol(const char** tags, size_t num_tags) const
{
    if (!m_default_tags.empty() || num_tags) {
        std::stringstream ss;

        Tags all_tags = m_default_tags;
        all_tags.insert(all_tags.end(), tags, tags + num_tags);
        ss << "|#";

        bool first = true;
        for (auto const& tag : all_tags) {
            if (!first)
                ss << ',';
            else
                first = false;

            ss << tag;
        }

        ss << '\n';

        return ss.str();
    }

    else
        return "\n";
}

// ctors & dtors

UnbufferedDogStatsd::UnbufferedDogStatsd(string const& hostname, int port, Tags const& default_tags)
    : DogStatsd(default_tags)
    , m_statsd({}, hostname, port)
{
}

UnbufferedDogStatsd::UnbufferedDogStatsd(std::vector<string> const& endpoints, Tags const& default_tags)
    : DogStatsd(default_tags)
    , m_statsd(endpoints)
{
}

UnbufferedDogStatsd::UnbufferedDogStatsd(Tags const& default_tags)
    : DogStatsd(default_tags)
{
}

// modifiers

void UnbufferedDogStatsd::add_endpoint(string const& endpoint)
{
    m_statsd.add_endpoint(endpoint);
}

void UnbufferedDogStatsd::add_endpoint(string const& hostname, int port)
{
    m_statsd.add_endpoint(hostname, port);
}

void UnbufferedDogStatsd::add_endpoints(std::vector<string> const& endpoints)
{
    m_statsd.add_endpoints(endpoints);
}

// main API

void UnbufferedDogStatsd::decrement(const char* metric, int value, const char** tags, size_t num_tags,
                                    float sample_rate)
{
    auto eol = build_eol(tags, num_tags);
    m_statsd.decrement(metric, value, sample_rate, eol.c_str());
}

void UnbufferedDogStatsd::gauge(const char* metric, double value, const char** tags, size_t num_tags,
                                float sample_rate)
{
    auto eol = build_eol(tags, num_tags);
    m_statsd.gauge(metric, value, sample_rate, eol.c_str());
}

void UnbufferedDogStatsd::histogram(const char* metric, double value, const char** tags, size_t num_tags,
                                    float sample_rate)
{
    auto eol = build_eol(tags, num_tags);
    m_statsd.histogram(metric, value, sample_rate, eol.c_str());
}

void UnbufferedDogStatsd::increment(const char* metric, int value, const char** tags, size_t num_tags,
                                    float sample_rate)
{
    auto eol = build_eol(tags, num_tags);
    m_statsd.increment(metric, value, sample_rate, eol.c_str());
}

void UnbufferedDogStatsd::timing(const char* metric, double value, const char** tags, size_t num_tags,
                                 float sample_rate)
{
    auto eol = build_eol(tags, num_tags);
    m_statsd.timing(metric, value, sample_rate, eol.c_str());
}

// ctors & dtors

BufferedDogStatsd::BufferedDogStatsd(string const& hostname, int port, Tags const& default_tags, std::size_t mtu)
    : DogStatsd(default_tags)
    , m_statsd({}, hostname, port, mtu)
{
}

BufferedDogStatsd::BufferedDogStatsd(std::vector<string> const& endpoints, Tags const& default_tags, std::size_t mtu)
    : DogStatsd(default_tags)
    , m_statsd(endpoints, {}, mtu)
{
}

BufferedDogStatsd::BufferedDogStatsd(Tags const& default_tags)
    : DogStatsd(default_tags)
{
}

// accessors

int BufferedDogStatsd::loop_interval() const noexcept
{
    return m_statsd.loop_interval();
}

std::size_t BufferedDogStatsd::mtu() const noexcept
{
    return m_statsd.mtu();
}

// modifiers

void BufferedDogStatsd::add_endpoint(string const& endpoint)
{
    m_statsd.add_endpoint(endpoint);
}

void BufferedDogStatsd::add_endpoint(string const& hostname, int port)
{
    m_statsd.add_endpoint(hostname, port);
}

void BufferedDogStatsd::add_endpoints(std::vector<string> const& endpoints)
{
    m_statsd.add_endpoints(endpoints);
}

void BufferedDogStatsd::loop_interval(int interval) noexcept
{
    m_statsd.loop_interval(interval);
}

void BufferedDogStatsd::mtu(std::size_t mtu) noexcept
{
    m_statsd.mtu(mtu);
}

// main API

void BufferedDogStatsd::decrement(const char* metric, int value, const char** tags, size_t num_tags,
                                  float sample_rate)
{
    auto eol = build_eol(tags, num_tags);
    m_statsd.decrement(metric, value, sample_rate, eol.c_str());
}

void BufferedDogStatsd::gauge(const char* metric, double value, const char** tags, size_t num_tags, float sample_rate)
{
    auto eol = build_eol(tags, num_tags);
    m_statsd.gauge(metric, value, sample_rate, eol.c_str());
}

void BufferedDogStatsd::histogram(const char* metric, double value, const char** tags, size_t num_tags,
                                  float sample_rate)
{
    auto eol = build_eol(tags, num_tags);
    m_statsd.histogram(metric, value, sample_rate, eol.c_str());
}

void BufferedDogStatsd::increment(const char* metric, int value, const char** tags, size_t num_tags,
                                  float sample_rate)
{
    auto eol = build_eol(tags, num_tags);
    m_statsd.increment(metric, value, sample_rate, eol.c_str());
}

void BufferedDogStatsd::timing(const char* metric, double value, const char** tags, size_t num_tags,
                               float sample_rate)
{
    auto eol = build_eol(tags, num_tags);
    m_statsd.timing(metric, value, sample_rate, eol.c_str());
}

void BufferedDogStatsd::flush()
{
    m_statsd.flush();
}

} // namespace dogless
