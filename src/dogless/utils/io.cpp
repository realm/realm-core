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

#include "dogless/utils/io.hpp"

namespace dogless {
namespace utils {

IOServiceRunner::IOServiceRunner(int thread_count)
    : m_work(m_io_service)
{
    run(thread_count);
}

IOServiceRunner::~IOServiceRunner()
{
    // release work from threads
    m_io_service.stop();

    // wait for all threads to have shut down
    for (auto& thread : m_threads)
        thread.join();
}

asio::io_service& IOServiceRunner::operator()()
{
    return m_io_service;
}

void IOServiceRunner::run(int thread_count)
{
    for (int i = 0; i < thread_count; ++i)
        m_threads.emplace_back([&]() {
            m_io_service.run();
        });
}

} // namespace utils
} // namespace dogless
