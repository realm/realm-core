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

#include <realm/util/backtrace.hpp>
#include <realm/util/features.h>

#include <sstream>
#include <cstdlib>
#include <cstring>

#if REALM_PLATFORM_APPLE || (defined(__linux__) && !REALM_ANDROID)
#include <execinfo.h>
#endif

using namespace realm::util;

static const size_t g_backtrace_depth = 128;
static const char* g_backtrace_error = "<error calculating backtrace>";
static const char* g_backtrace_alloc_error = "<error allocating backtrace>";
static const char* g_backtrace_symbolicate_error = "<error symbolicating backtrace>";
static const char* g_backtrace_unsupported_error = "<backtrace not supported on this platform>";

Backtrace::~Backtrace()
{
    // std::free(nullptr) is guaranteed to silently do nothing.
    std::free(m_memory);
}

Backtrace::Backtrace(Backtrace&& other) noexcept
    : Backtrace()
{
    *this = std::move(other);
}

Backtrace::Backtrace(const Backtrace& other) noexcept
    : Backtrace()
{
    *this = other;
}

Backtrace& Backtrace::operator=(Backtrace&& other) noexcept
{
    std::swap(m_memory, other.m_memory);
    std::swap(m_strs, other.m_strs);
    std::swap(m_len, other.m_len);
    return *this;
}

Backtrace& Backtrace::operator=(const Backtrace& other) noexcept
{
    // For this class to work as a member of an exception, it has to define a
    // copy-constructor, because std::current_exception() may copy the exception
    // object.

    m_len = other.m_len;
    size_t required_memory = sizeof(char*) * m_len;
    for (size_t i = 0; i < m_len; ++i) {
        required_memory += std::strlen(other.m_strs[i]) + 1;
    }

    void* new_memory = std::malloc(required_memory);
    if (new_memory == nullptr) {
        std::free(m_memory);
        m_memory = nullptr;
        m_strs = &g_backtrace_alloc_error;
        m_len = 1;
        return *this;
    }

    char** new_strs = static_cast<char**>(new_memory);
    char* p = static_cast<char*>(new_memory) + sizeof(char*) * m_len;
    for (size_t i = 0; i < m_len; ++i) {
        *(new_strs++) = p;
        // FIXME: stpcpy() is not supported on Android, so we gotta manually
        // calculate the end of the destination here.
        size_t len = std::strlen(other.m_strs[i]);
        std::memcpy(p, other.m_strs[i], len);
        p[len] = '\0';
        p += len + 1;
    }
    std::free(m_memory);
    m_memory = new_memory;
    m_strs = static_cast<char* const*>(m_memory);
    m_len = other.m_len;
    return *this;
}


Backtrace Backtrace::capture() noexcept
{
#if REALM_PLATFORM_APPLE || (defined(__linux__) && !REALM_ANDROID)
    static_cast<void>(g_backtrace_unsupported_error);
    void* callstack[g_backtrace_depth];
    int frames = ::backtrace(callstack, g_backtrace_depth);
    if (REALM_UNLIKELY(frames <= 1)) {
        return Backtrace(nullptr, &g_backtrace_error, 1);
    }
    else {
        // Translate the backtrace to symbols (and exclude the call to the
        // capture() function from the trace).
        --frames;
        void* memory = ::backtrace_symbols(callstack + 1, frames);
        if (REALM_UNLIKELY(memory == nullptr)) {
            return Backtrace(nullptr, &g_backtrace_symbolicate_error, 1);
        }
        else {
            return Backtrace{memory, size_t(frames)};
        }
    }
#else
    static_cast<void>(g_backtrace_error);
    static_cast<void>(g_backtrace_symbolicate_error);
    return Backtrace(nullptr, &g_backtrace_unsupported_error, 1);
#endif
}


void Backtrace::print(std::ostream& os) const
{
    for (size_t i = 0; i < m_len; ++i) {
        os << m_strs[i];
        if (i + 1 != m_len) {
            os << "\n";
        }
    }
}

const char* detail::ExceptionWithBacktraceBase::materialize_message() const noexcept
{
    if (m_has_materialized_message) {
        return m_materialized_message.c_str();
    }

    const char* msg = message();

    try {
        std::stringstream ss;
        ss << msg << "\n";
        ss << "Exception backtrace:\n";
        m_backtrace.print(ss);
        m_materialized_message = ss.str();
        m_has_materialized_message = true;
        return m_materialized_message.c_str();
    }
    catch (...) {
        return msg;
    }
}
