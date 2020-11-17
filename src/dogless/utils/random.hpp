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

#ifndef DATADOG_CXX_UTILS_HPP_20151216
#define DATADOG_CXX_UTILS_HPP_20151216

#include <memory>
#include <random>

namespace dogless {
namespace utils {

template <int start, int end>
class Random {
public:
    // ctors and dtors
    Random();

    float operator()();

private:
    std::random_device m_random_device;
    std::unique_ptr<std::mt19937> m_random_generator;
    std::uniform_real_distribution<> m_random_distribution =
        std::uniform_real_distribution<>(static_cast<float>(start), static_cast<float>(end));
};

template <int start, int end>
inline Random<start, end>::Random()
{
    m_random_generator = std::unique_ptr<std::mt19937>(new std::mt19937(m_random_device()));
}

template <int start, int end>
inline float Random<start, end>::operator()()
{
    return m_random_distribution(*m_random_generator);
}

} // namespace utils
} // namespace dogless

#endif // DATADOG_CXX_UTILS_HPP_20151216
