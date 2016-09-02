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

#ifndef REALM_TEST_UTIL_SEMAPHORE_HPP
#define REALM_TEST_UTIL_SEMAPHORE_HPP

#include <realm/util/thread.hpp>

namespace realm {
namespace test_util {

class BowlOfStonesSemaphore {
public:
    BowlOfStonesSemaphore(int initial_number_of_stones = 0)
        : m_num_stones(initial_number_of_stones)
    {
    }
    void get_stone()
    {
        util::LockGuard lock(m_mutex);
        while (m_num_stones == 0)
            m_cond_var.wait(lock);
        --m_num_stones;
    }
    void add_stone()
    {
        util::LockGuard lock(m_mutex);
        ++m_num_stones;
        m_cond_var.notify();
    }

private:
    util::Mutex m_mutex;
    int m_num_stones;
    util::CondVar m_cond_var;
};

} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_SEMAPHORE_HPP
