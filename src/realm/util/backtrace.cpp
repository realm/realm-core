#include <realm/util/backtrace.hpp>
#include <realm/util/features.h>

#include <ostream>

#if REALM_PLATFORM_APPLE || (defined(__linux__) && !REALM_ANDROID)
#include <execinfo.h>
#include <cstdlib>
#include <cstring>
#endif

using namespace realm::util;

static const size_t g_backtrace_depth = 128;
static const char* g_backtrace_error = "<error getting backtrace>";
static const char* g_backtrace_symbolicate_error = "<error symbolicating backtrace>";
static const char* g_backtrace_unsupported_error = "<backtrace unsupported on this platform>";

Backtrace::~Backtrace()
{
    // std::free(nullptr) is guaranteed to silently do nothing.
    std::free(const_cast<char**>(m_strs));
}


Backtrace Backtrace::capture() noexcept
{
#if REALM_PLATFORM_APPLE || (defined(__linux__) && !REALM_ANDROID)
    static_cast<void>(g_backtrace_unsupported_error);
    void* callstack[g_backtrace_depth];
    int frames = ::backtrace(callstack, g_backtrace_depth);
    const char** strs;
    if (REALM_UNLIKELY(frames <= 0)) {
        strs = static_cast<const char**>(std::malloc(sizeof(char*)));
        strs[0] = g_backtrace_error;
        frames = 1;
    }
    else {
        strs = const_cast<const char**>(::backtrace_symbols(callstack, frames));
        if (REALM_UNLIKELY(strs == nullptr)) {
            strs = static_cast<const char**>(std::malloc(sizeof(char*)));
            strs[0] = g_backtrace_symbolicate_error;
            frames = 1;
        }
    }
    return Backtrace{strs, size_t(frames)};
#else
    static_cast<void>(g_backtrace_error);
    static_cast<void>(g_backtrace_symbolicate_error);
    const char** strs = static_cast<const char**>(std::malloc(sizeof(char*)));
    strs[0] = g_backtrace_unsupported_error;
    return Backtrace{strs, 1};
#endif
}


void Backtrace::print(std::ostream& os) const
{
    for (size_t i = 0; i < m_len; ++i) {
        os << "[#" << i << "]: " << m_strs[i];
        if (i + 1 != m_len) {
            os << "\n";
        }
    }
}
