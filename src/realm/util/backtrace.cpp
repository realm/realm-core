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
#include <type_traits>

#if REALM_PLATFORM_APPLE || (defined(__linux__) && !REALM_ANDROID)
#include <execinfo.h>
#elif __QNX__
#include <backtrace.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#endif

using namespace realm::util;

#if REALM_PLATFORM_APPLE || (defined(__linux__) && !REALM_ANDROID || defined(__QNX__))
static const size_t g_backtrace_depth = 128;
#endif
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
#elif __QNX__
    static_cast<void>(g_backtrace_symbolicate_error);
    static_cast<void>(g_backtrace_unsupported_error);
    static char out[1024];
    char out_format[] = "%a (%f + %o)";
    bt_addr_t *pc = nullptr;
    bt_accessor_t acc;
    bt_memmap_t memmap;
    int frames = 0;

    if (bt_init_accessor(&acc, BT_SELF) == -1) {
        snprintf(out, sizeof(out), "%s:%i %s (%i)%s", __FUNCTION__, __LINE__,
                "bt_init_accessor", errno, strerror(errno));
        return Backtrace(nullptr, reinterpret_cast<const char* const*>(&out), 1);
    }
    printf("bt_init_accessor success\n");

    if (bt_load_memmap(&acc, &memmap) == -1) {
        bt_release_accessor(&acc);
        snprintf(out, sizeof(out), "%s:%i %s (%i)%s", __FUNCTION__, __LINE__,
                "bt_load_memmap", errno, strerror(errno));
        return Backtrace(nullptr, reinterpret_cast<const char* const*>(&out), 1);
    }
    printf("bt_load_memmap success\n");
    // Malloc storage 
    pc = reinterpret_cast<bt_addr_t*>(std::malloc(sizeof(bt_addr_t) * g_backtrace_depth));
    if (pc == nullptr) {
        bt_unload_memmap(&memmap);
        bt_release_accessor(&acc);
        snprintf(out, sizeof(out), "%s:%i %s", __FUNCTION__, __LINE__,
                "not enough memory for backtrace");
        return Backtrace(nullptr, reinterpret_cast<const char* const*>(&out), 1);
    }
    printf("malloc pc success - %lu bytes\n", sizeof(bt_addr_t) * g_backtrace_depth);
    // Populate the backtrace addresses and save how many were extracted
    frames = bt_get_backtrace(&acc, pc, g_backtrace_depth);
    if (frames <= 0) {
        std::free(pc);
        bt_unload_memmap(&memmap);
        bt_release_accessor(&acc);
        return Backtrace(nullptr, reinterpret_cast<const char* const*>(&g_backtrace_error), 1);
    }
    printf("bt_get_backtrace success - frames: %d\n", frames);

    enum {Count = 0, Print = 1, Done = 2};
    int state, ret, num_strings;
    char * out_str = out;
    size_t curr_len;
    int max_str = sizeof(out);
    size_t tot_buf = 0;
    // outputs 
    char** out_strings = nullptr;

    // Iterate over the strings first to get a malloc size and save the values
    // on the second run.
    for (state = Count; state < Done; state++) {
        // Print out the memory map
        printf("build backtrace loop: %s\n", state == Count ? "Count" : "Print");
        num_strings = 0;
        //ret = bt_sprn_memmap(&memmap, out_str, max_str);
        //if (ret < 0) {
        //    std::free(pc);
        //    std::free(out_strings);
        //    bt_unload_memmap(&memmap);
        //    bt_release_accessor(&acc);
        //    snprintf(out, sizeof(out), "%s:%i %s (%i)%s", __FUNCTION__, __LINE__,
        //            "bt_sprn_memmap", errno, strerror(errno));
        //    return Backtrace(nullptr, reinterpret_cast<const char* const*>(&out), 1);
        //}
        //out_str[max_str - 1] = '\0'; // ensure null termination
        //curr_len = strnlen(out_str, max_str) + 1;
        //printf("bt_get_backtrace success - size: %lu\n", curr_len);
        //if (state == Count) {
        //    tot_buf += curr_len;
        //}
        //else {
        //    // save the string and move the indexes
        //    out_strings[num_strings] = out_str;
        //    out_str += curr_len;
        //    max_str -= curr_len;
        //    if (max_str < 0) {
        //        std::free(pc);
        //        std::free(out_strings);
        //        bt_unload_memmap(&memmap);
        //        bt_release_accessor(&acc);
        //        snprintf(out, sizeof(out), "%s:%i %s(tot: %lu, current: %lu)", __FUNCTION__, __LINE__,
        //                "string buffer overflow", tot_buf, curr_len);
        //        return Backtrace(nullptr, reinterpret_cast<const char* const*>(&out), 1);
        //    }
        //}
        //num_strings++;
        // Iterate over the backtrace addresses
        for (int i = 0; i < frames; i++) {
            // The remove_const is to remove the non-const warning
            ret = bt_sprnf_addrs(&memmap, pc + i, 1, out_format, out_str, max_str, 0);
            if (ret < 0) {
                std::free(pc);
                std::free(out_strings);
                bt_unload_memmap(&memmap);
                bt_release_accessor(&acc);
                snprintf(out, sizeof(out), "%s:%i %s[%d] (%i)%s", __FUNCTION__, __LINE__,
                        "bt_sprnf_addrs", i, errno, strerror(errno));
                return Backtrace(nullptr, reinterpret_cast<const char* const*>(&out), 1);
            }
            // Did the backtrace string fit?
            else if (ret == 0) {
                out_str[0] = '\0'; // Empty string
                curr_len = 1;
            }
            else {
                out_str[max_str - 1] = '\0'; // ensure null termination
                curr_len = strnlen(out_str, max_str) + 1;
            }
            printf("bt_sprnf_addrs[%d] success - size: %lu\n", i, curr_len);
            if (state == Count) {
                tot_buf += curr_len;
            }
            else {
                // save the string and move the indexes
                out_strings[num_strings] = out_str;
                out_str += curr_len;
                max_str -= curr_len;
                if (max_str < 0) {
                    std::free(pc);
                    std::free(out_strings);
                    bt_unload_memmap(&memmap);
                    bt_release_accessor(&acc);
                    snprintf(out, sizeof(out), "%s:%i %s[%d](tot: %lu, current: %lu)", __FUNCTION__, __LINE__,
                            "string buffer overflow", i, tot_buf, curr_len);
                    return Backtrace(nullptr, reinterpret_cast<const char* const*>(&out), 1);
                }
            }
            num_strings++;
        }
        if (state == Count) {
            printf("Count complete - %lu total bytes\n", tot_buf);
            // Storage for the char* array plus the string data bytes
            out_strings = reinterpret_cast<char**>(std::malloc(num_strings * sizeof(char*) + tot_buf));
            // Set up the start of the string data to after the char** array
            if (out_strings == nullptr) {
                std::free(pc);
                std::free(out_strings);
                bt_unload_memmap(&memmap);
                bt_release_accessor(&acc);
                snprintf(out, sizeof(out), "%s:%i %s(tot: %lu, current: %lu)", __FUNCTION__, __LINE__,
                        "alloc string memory failed", tot_buf, curr_len);
                return Backtrace(nullptr, reinterpret_cast<const char* const*>(&out), 1);
            }
            out_str = out_strings[num_strings];
            max_str = tot_buf;
        }
        // For printing, the Backtrace with the data will be returned later
        // out_strings will be deleted by Backtrace when it is destroyed
    }
    std::free(pc);
    bt_unload_memmap(&memmap);
    if (bt_release_accessor(&acc) == -1) {
        snprintf(out, sizeof(out), "%s:%i %s (%i)%s\n", __FUNCTION__, __LINE__,
                 "bt_release_accessor", errno, strerror(errno));
        return Backtrace(nullptr, reinterpret_cast<const char* const*>(&out), 1);
    }
    return Backtrace(out_strings, size_t(num_strings));
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
