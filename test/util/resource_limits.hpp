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

#ifndef REALM_TEST_UTIL_RESOURCE_LIMITS_HPP
#define REALM_TEST_UTIL_RESOURCE_LIMITS_HPP

namespace realm {
namespace test_util {


enum Resource {
    /// One plus the maximum file descriptor number that can be opened by this
    /// process. Same as RLIMIT_NOFILE in POSIX.
    resource_NumOpenFiles
};

bool system_has_rlimit(Resource) noexcept;

//@{

/// Get or set resouce limits. A negative value means 'unlimited' both when
/// getting and when setting.
long get_hard_rlimit(Resource);
long get_soft_rlimit(Resource);
void set_soft_rlimit(Resource, long value);

//@}


} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_RESOURCE_LIMITS_HPP
