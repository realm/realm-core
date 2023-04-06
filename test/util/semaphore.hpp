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

#include <realm/util/semaphore.hpp>

namespace realm::test_util {

class BowlOfStonesSemaphore {
public:
    BowlOfStonesSemaphore(int initial_number_of_stones = 0)
        : m_semaphore(initial_number_of_stones)
    {
    }
    void get_stone()
    {
        m_semaphore.acquire();
    }
    void add_stone()
    {
        m_semaphore.release();
    }

private:
    util::CountingSemaphore<> m_semaphore;
};

} // namespace realm::test_util

#endif // REALM_TEST_UTIL_SEMAPHORE_HPP
