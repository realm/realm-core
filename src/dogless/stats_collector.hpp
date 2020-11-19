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

#ifndef DATADOG_CXX_STATS_COLLECTOR_HPP
#define DATADOG_CXX_STATS_COLLECTOR_HPP

#include <stddef.h> // size_t

namespace dogless {

class StatsCollectorBase {
public:
    virtual ~StatsCollectorBase() {}

    // FIXME: Use std::string_view instead of const char*.

    virtual void decrement(const char* metric, int value = 1, float sample_rate = 1.0f, const char* eol = "\n") = 0;
    virtual void increment(const char* metric, int value = 1, float sample_rate = 1.0f, const char* eol = "\n") = 0;
    virtual void gauge(const char* metric, double amount, float sample_rate = 1.0f, const char* eol = "\n") = 0;
    virtual void gauge_relative(const char* metric, double amount, float sample_rate = 1.0f,
                                const char* eol = "\n") = 0;
    virtual void histogram(const char* metric, double value, float sample_rate = 1.0f, const char* eol = "\n") = 0;
    virtual void timing(const char* metric, double value, float sample_rate = 1.0f, const char* eol = "\n") = 0;
};

class TaggedStatsCollectorBase {
public:
    virtual ~TaggedStatsCollectorBase() {}

    // FIXME: Use std::string_view instead of const char*.
    // FIXME: Use std::array_view instead of const char**.

    virtual void decrement(const char* metric, int value = 1, const char** tags = nullptr, size_t num_tags = 0,
                           float sample_rate = 1.0) = 0;
    virtual void gauge(const char* metric, double value, const char** tags = nullptr, size_t num_tags = 0,
                       float sample_rate = 1.0) = 0;
    virtual void histogram(const char* metric, double value, const char** tags = nullptr, size_t num_tags = 0,
                           float sample_rate = 1.0) = 0;
    virtual void increment(const char* metric, int value = 1, const char** tags = nullptr, size_t num_tags = 0,
                           float sample_rate = 1.0) = 0;
    virtual void timing(const char* metric, double value, const char** tags = nullptr, size_t num_tags = 0,
                        float sample_rate = 1.0) = 0;
};

} // namespace dogless

#endif
