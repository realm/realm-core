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

#ifndef DATADOG_CXX_UTILS_IO_HPP_20151217
#define DATADOG_CXX_UTILS_IO_HPP_20151217

#include <thread>
#include <vector>

#include <asio/io_service.hpp>

namespace dogless {
namespace utils {

class IOServiceRunner {
public:
    // ctor/dtor
    IOServiceRunner(int thread_count = 1);
    IOServiceRunner(IOServiceRunner const&) = delete;
    ~IOServiceRunner();

    // accessor
    asio::io_service& operator()();

private:
    void run(int thread_count);

private:
    asio::io_service m_io_service;
    asio::io_service::work m_work;
    std::vector<std::thread> m_threads;
};

} // namespace utils
} // namespace dogless

#endif // DATADOG_CXX_UTILS_IO_HPP_20151217
