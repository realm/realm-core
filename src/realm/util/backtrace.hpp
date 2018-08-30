/*************************************************************************
 *
 * Copyright 2018 Realm Inc.
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

#ifndef REALM_UTIL_BACKTRACE_HPP
#define REALM_UTIL_BACKTRACE_HPP

#include <vector>
#include <string>
#include <iosfwd>

namespace realm {
namespace util {

struct Backtrace {
    static Backtrace capture() noexcept;
    void print(std::ostream&) const;

    Backtrace() {}
    ~Backtrace();
private:
    Backtrace(const char** strs, size_t len) : m_strs(strs), m_len(len) {}

    const char* const* m_strs = nullptr;
    size_t m_len = 0;
};

} // namespace util
} // namespace realm

inline std::ostream& operator<<(std::ostream& os, const realm::util::Backtrace& bt)
{
    bt.print(os);
    return os;
}

#endif // REALM_UTIL_BACKTRACE_HPP
