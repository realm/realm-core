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

#ifndef REALM_TEST_UTIL_MEM_HPP
#define REALM_TEST_UTIL_MEM_HPP

#include <cstddef>

namespace realm {
namespace test_util {


/// Returns the amount (in number of bytes) of virtaul memory
/// allocated to the calling process.
///
/// FIXME: 'size_t' is inappropriate for holding the total memory
/// usage. C++11 guarantees only that it can hold the size of a single
/// object or array. 'uintptr_t' would have been the ideal type
/// to use here, but C++11 does not required it to be available (see
/// 18.4.1 "Header <cstdint> synopsis".)
size_t get_mem_usage();


} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_MEM_HPP
