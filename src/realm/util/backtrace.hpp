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

    Backtrace() noexcept {}
    Backtrace(Backtrace&&) noexcept;
    Backtrace(const Backtrace&) noexcept;
    ~Backtrace();
    Backtrace& operator=(Backtrace&&) noexcept;
    Backtrace& operator=(const Backtrace&) noexcept;
private:
    Backtrace(void* memory, const char* const* strs, size_t len)
        : m_memory(memory)
        , m_strs(strs)
        , m_len(len)
    {}
    Backtrace(void* memory, size_t len)
        : m_memory(memory)
        , m_strs(static_cast<char* const*>(memory))
        , m_len(len)
    {}

    void* m_memory = nullptr;
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
