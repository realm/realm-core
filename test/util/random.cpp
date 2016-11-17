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

#include "random.hpp"


namespace realm {
namespace test_util {


unsigned int produce_nondeterministic_random_seed()
{
    std::random_device rd;
    return rd();
}


} // namespace test_util

namespace _impl {


GlobalRandom& GlobalRandom::get() noexcept
{
    static GlobalRandom r;
    return r;
}


} // namespace _impl
} // namespace realm
