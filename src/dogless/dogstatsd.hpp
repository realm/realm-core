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

#ifndef DATADOG_CXX_DOGSTATSD_HPP_20151216
#define DATADOG_CXX_DOGSTATSD_HPP_20151216

#include "dogless/statsd.hpp"
#include "dogless/stats_collector.hpp"

namespace dogless {

using std::string;

class DogStatsd : public TaggedStatsCollectorBase {
public:
    using Tags = std::vector<string>;

    // ctors & dtors
    DogStatsd(Tags const& default_tags);

    // accessors
    Tags const& default_tags() const noexcept;

    // modifiers
    void default_tags(Tags const& default_tags);

protected:
    // internal methods
    string build_eol(const char** tags, size_t num_tags) const;

private:
    // options
    Tags m_default_tags;
};

class UnbufferedDogStatsd : public DogStatsd {
public:
    // ctors & dtors
    UnbufferedDogStatsd(string const& hostname = "localhost", int port = 8125, Tags const& default_tags = Tags());
    UnbufferedDogStatsd(std::vector<string> const& endpoints, Tags const& default_tags);
    UnbufferedDogStatsd(Tags const& default_tags);
    UnbufferedDogStatsd(UnbufferedDogStatsd const&) = delete;

    // modifiers
    void add_endpoint(string const& endpoint);
    void add_endpoint(string const& hostname, int port);
    void add_endpoints(std::vector<string> const& endpoints);

    // main API
    void decrement(const char* metric, int value = 1, const char** tags = nullptr, size_t num_tags = 0,
                   float sample_rate = 1.0) final;
    void gauge(const char* metric, double value, const char** tags = nullptr, size_t num_tags = 0,
               float sample_rate = 1.0) final;
    void histogram(const char* metric, double value, const char** tags = nullptr, size_t num_tags = 0,
                   float sample_rate = 1.0) final;
    void increment(const char* metric, int value = 1, const char** tags = nullptr, size_t num_tags = 0,
                   float sample_rate = 1.0) final;
    void timing(const char* metric, double value, const char** tags = nullptr, size_t num_tags = 0,
                float sample_rate = 1.0) final;

private:
    // properties
    UnbufferedStatsd m_statsd;
};

class BufferedDogStatsd : public DogStatsd {
public:
    // ctors & dtors
    BufferedDogStatsd(string const& hostname = "localhost", int port = 8125, Tags const& default_tags = Tags(),
                      std::size_t mtu = MTU_InternetSafe);
    BufferedDogStatsd(std::vector<string> const& endpoints, Tags const& default_tags,
                      std::size_t mtu = MTU_InternetSafe);
    BufferedDogStatsd(Tags const& default_tags);
    BufferedDogStatsd(BufferedDogStatsd const&) = delete;

    // accessors
    int loop_interval() const noexcept;
    std::size_t mtu() const noexcept;

    // modifiers
    void add_endpoint(string const& endpoint);
    void add_endpoint(string const& hostname, int port);
    void add_endpoints(std::vector<string> const& endpoints);
    void loop_interval(int interval) noexcept;
    void mtu(std::size_t mtu) noexcept;

    // main API
    void decrement(const char* metric, int value = 1, const char** tags = nullptr, size_t num_tags = 0,
                   float sample_rate = 1.0) final;
    void gauge(const char* metric, double value, const char** tags = nullptr, size_t num_tags = 0,
               float sample_rate = 1.0) final;
    void histogram(const char* metric, double value, const char** tags = nullptr, size_t num_tags = 0,
                   float sample_rate = 1.0) final;
    void increment(const char* metric, int value = 1, const char** tags = nullptr, size_t num_tags = 0,
                   float sample_rate = 1.0) final;
    void timing(const char* metric, double value, const char** tags = nullptr, size_t num_tags = 0,
                float sample_rate = 1.0) final;
    void flush();

private:
    // options
    BufferedStatsd m_statsd;
};

} // namespace dogless

#endif // DATADOG_CXX_DOGSTATSD_HPP_20151216
