/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
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

#ifndef REALM_TEST_CLIENT_METRICS_HPP
#define REALM_TEST_CLIENT_METRICS_HPP

#if REALM_HAVE_DOGLESS
#include <dogless.hpp>
#endif


namespace realm {
namespace test_client {

class Metrics {
public:
    /// \param prefix Metric label prefix. The effective prefix is the passed
    /// string plus a dot (`.`).
    Metrics(const std::string& prefix, const std::string statsd_address, int statsd_port);

    /// Increment the counter identified by the specified label.
    void increment(const char* metric, int value = 1);

    /// Send the timing identified by the specified label.
    void timing(const char* metric, double value);

    /// Set value of the guage identified by the specified label.
    void gauge(const char* metric, double value);

private:
#if REALM_HAVE_DOGLESS
    dogless::BufferedStatsd m_dogless;
#endif
};


// Implementation

#if REALM_HAVE_DOGLESS

inline Metrics::Metrics(const std::string& prefix, const std::string statsd_address, int statsd_port)
    : m_dogless(prefix, statsd_address, statsd_port) // Throws
{
    m_dogless.loop_interval(1);
}

inline void Metrics::increment(const char* metric, int value)
{
    m_dogless.increment(metric, value); // Throws
}

inline void Metrics::timing(const char* metric, double value)
{
    m_dogless.timing(metric, value); // Throws
}

inline void Metrics::gauge(const char* metric, double value)
{
    m_dogless.gauge(metric, value); // Throws
}

#else // !REALM_HAVE_DOGLESS

inline Metrics::Metrics(const std::string&, const std::string, int) {}

inline void Metrics::increment(const char*, int) {}

inline void Metrics::timing(const char*, double) {}

inline void Metrics::gauge(const char*, double) {}

#endif // !REALM_HAVE_DOGLESS

} // namespace test_client
} // namespace realm

#endif // REALM_TEST_CLIENT_METRICS_HPP
