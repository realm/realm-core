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

/// Backtrace encapsulates a stack trace, usually as captured by `backtrace()`
/// and `backtrace_symbols()` (or platform-specific equivalents).
struct Backtrace {
    /// Capture a symbolicated stack trace, excluding the call to `capture()`
    /// itself. If any error occurs while capturing the stack trace or
    /// translating symbol names, a `Backtrace` object is returned containing a
    /// single line describing the error.
    ///
    /// This function only allocates memory as part of calling
    /// `backtrace_symbols()` (or the current platform's equivalent).
    static Backtrace capture() noexcept;

    /// Print the backtrace to the stream. Each line is separated by a newline.
    /// The format of the output is unspecified.
    void print(std::ostream&) const;

    /// Construct an empty stack trace.
    Backtrace() noexcept
    {
    }

    /// Move constructor. This operation cannot fail.
    Backtrace(Backtrace&&) noexcept;

    /// Copy constructor. See the copy assignment operator.
    Backtrace(const Backtrace&) noexcept;

    ~Backtrace();

    /// Move assignment operator. This operation cannot fail.
    Backtrace& operator=(Backtrace&&) noexcept;

    /// Copy assignment operator. Copying a `Backtrace` object may result in a
    /// memory allocation. If such an allocation fails, the backtrace is
    /// replaced with a single line describing the error.
    Backtrace& operator=(const Backtrace&) noexcept;

private:
    Backtrace(void* memory, const char* const* strs, size_t len)
        : m_memory(memory)
        , m_strs(strs)
        , m_len(len)
    {
    }
    Backtrace(void* memory, size_t len)
        : m_memory(memory)
        , m_strs(static_cast<char* const*>(memory))
        , m_len(len)
    {
    }

    // m_memory is a pointer to the memory block returned by
    // `backtrace_symbols()`. It is usually equal to `m_strs`, except in the
    // case where an error has occurred and `m_strs` points to statically
    // allocated memory describing the error.
    //
    // When `m_memory` is non-null, the memory is owned by this object.
    void* m_memory = nullptr;

    // A pointer to a list of string pointers describing the stack trace (same
    // format as returned by `backtrace_symbols()`).
    const char* const* m_strs = nullptr;

    // Number of entries in this stack trace.
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
