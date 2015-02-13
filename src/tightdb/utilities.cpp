#include <cstdlib> // size_t
#include <string>
#include <iostream>
#include <stdint.h>

#include <tightdb/utilities.hpp>
#include <tightdb/unicode.hpp>
#include <tightdb/util/thread.hpp>

#ifdef TIGHTDB_COMPILER_SSE
#  ifdef _MSC_VER
#    include <intrin.h>
#  endif
#endif


namespace {

#ifdef TIGHTDB_COMPILER_SSE
#  if !defined __clang__ && ((_MSC_FULL_VER >= 160040219) || defined __GNUC__)
#    if defined TIGHTDB_COMPILER_AVX && defined __GNUC__
#      define _XCR_XFEATURE_ENABLED_MASK 0

inline unsigned long long _xgetbv(unsigned index)
{
#if TIGHTDB_HAVE_AT_LEAST_GCC(4, 4)
    unsigned int eax, edx;
    __asm__ __volatile__("xgetbv" : "=a"(eax), "=d"(edx) : "c"(index));
    return (static_cast<unsigned long long>(edx) << 32) | eax;
#else
    static_cast<void>(index);
    return 0;
#endif
}

#    endif
#  endif
#endif

} // anonymous namespace


namespace tightdb {

signed char sse_support = -1;
signed char avx_support = -1;

StringCompareCallback string_compare_callback = null_ptr;
string_compare_method_t string_compare_method = STRING_COMPARE_CORE;

void cpuid_init()
{
#ifdef TIGHTDB_COMPILER_SSE
    int cret;
#  ifdef _MSC_VER
    int CPUInfo[4];
    __cpuid(CPUInfo, 1);
    cret = CPUInfo[2];
#  else
    int a = 1;
    __asm ( "mov %1, %%eax; "            // a into eax
          "cpuid;"
          "mov %%ecx, %0;"             // ecx into b
          :"=r"(cret)                     // output
          :"r"(a)                      // input
          :"%eax","%ebx","%ecx","%edx" // clobbered register
         );
#  endif

// Byte is atomic. Race can/will occur but that's fine
    if (cret & 0x100000) { // test for 4.2
        sse_support = 1;
    }
    else if (cret & 0x1) { // Test for 3
        sse_support = 0;
    }
    else {
        sse_support = -2;
    }

    bool avxSupported = false;

// seems like in jenkins builds, __GNUC__ is defined for clang?! todo fixme
#  if !defined __clang__ && ((_MSC_FULL_VER >= 160040219) || defined __GNUC__)
    bool osUsesXSAVE_XRSTORE = cret & (1 << 27) || false;
    bool cpuAVXSuport = cret & (1 << 28) || false;

    if (osUsesXSAVE_XRSTORE && cpuAVXSuport) {
        // Check if the OS will save the YMM registers
        unsigned long long xcrFeatureMask = _xgetbv(_XCR_XFEATURE_ENABLED_MASK);
        avxSupported = (xcrFeatureMask & 0x6) || false;
    }
#  endif

    if (avxSupported) {
        avx_support = 0; // AVX1 supported
    }
    else {
        avx_support = -1; // No AVX supported
    }

    // 1 is reserved for AVX2

#endif
}


// FIXME: Move all these rounding functions to the header file to
// allow inlining.
void* round_up(void* p, size_t align)
{
    // FIXME: The C++ standard does not guarantee that a pointer can
    // be stored in size_t. Use uintptr_t instead. The problem with
    // uintptr_t, is that is is not part of C++03.
    size_t r = size_t(p) % align == 0 ? 0 : align - size_t(p) % align;
    return static_cast<char *>(p) + r;
}

void* round_down(void* p, size_t align)
{
    // FIXME: The C++ standard does not guarantee that a pointer can
    // be stored in size_t. Use uintptr_t instead. The problem with
    // uintptr_t, is that is is not part of C++03.
    size_t r = size_t(p);
    return reinterpret_cast<void *>(r & ~(align - 1));
}

size_t round_up(size_t p, size_t align)
{
    size_t r = p % align == 0 ? 0 : align - p % align;
    return p + r;
}

size_t round_down(size_t p, size_t align)
{
    size_t r = p;
    return r & (~(align - 1));
}


void checksum_init(checksum_t* t)
{
    t->remainder = 0;
    t->remainder_len = 0;
    t->b_val = 0x794e80091e8f2bc7ULL;
    t->a_val = 0xc20f9a8b761b7e4cULL;
    t->result = 0;
}

unsigned long long checksum(unsigned char* data, size_t len)
{
    checksum_t t;
    checksum_init(&t);
    checksum_rolling(data, len, &t);
    return t.result;
}

void checksum_rolling(unsigned char* data, size_t len, checksum_t* t)
{
    while (t->remainder_len < 8 && len > 0) {
        t->remainder = t->remainder >> 8;
        t->remainder = t->remainder | static_cast<unsigned long long>(*data) << (7*8);
        t->remainder_len++;
        data++;
        len--;
    }

    if (t->remainder_len < 8) {
        t->result = t->a_val + t->b_val;
        return;
    }

    t->a_val += t->remainder * t->b_val;
    t->b_val++;
    t->remainder_len = 0;
    t->remainder = 0;

    while (len >= 8) {
#ifdef TIGHTDB_X86_OR_X64
        t->a_val += (*reinterpret_cast<unsigned long long*>(data)) * t->b_val;
#else
        unsigned long long l = 0;
        for (unsigned int i = 0; i < 8; i++) {
            l = l >> 8;
            l = l | static_cast<unsigned long long>(*(data + i)) << (7*8);
        }
        t->a_val += l * t->b_val;
#endif
        t->b_val++;
        len -= 8;
        data += 8;
    }

    while (len > 0) {
        t->remainder = t->remainder >> 8;
        t->remainder = t->remainder | static_cast<unsigned long long>(*data) << (7*8);
        t->remainder_len++;
        data++;
        len--;
    }

    t->result = t->a_val + t->b_val;
}

} // namespace tightdb


// popcount, counts number of set (1) bits in argument. Intrinsics has been disabled because it's just 10-20% faster
// than fallback method, so a runtime-detection of support would be more expensive in total. Popcount is supported
// with SSE42 but not with SSE3, and we don't want separate builds for each architecture - hence a runtime check would
// be required.
#if 0 // defined(_MSC_VER) && _MSC_VER >= 1500
#  include <intrin.h>

namespace tightdb {

int fast_popcount32(int32_t x)
{
    return __popcnt(x);
}
#  if defined(_M_X64)
int fast_popcount64(int64_t x)
{
    return int(__popcnt64(x));
}
#  else
int fast_popcount64(int64_t x)
{
    return __popcnt(unsigned(x)) + __popcnt(unsigned(x >> 32));
}
#  endif

} // namespace tightdb

#elif 0 // defined(__GNUC__) && __GNUC__ >= 4 || defined(__INTEL_COMPILER) && __INTEL_COMPILER >= 900
#  define fast_popcount32 __builtin_popcount

namespace tightdb {

#  if ULONG_MAX == 0xffffffff
int fast_popcount64(int64_t x)
{
    return __builtin_popcount(unsigned(x)) + __builtin_popcount(unsigned(x >> 32));
}
#  else
int fast_popcount64(int64_t x)
{
    return __builtin_popcountll(x);
}
#  endif

} // namespace tightdb

#else

namespace {

const char a_popcount_bits[256] = {
    0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,        1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,        1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,        2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,        1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,        2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,        2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,        3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,        1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,        2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,        2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,        3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,        2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,        3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,        3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,        4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
};

} // anonymous namespace

namespace tightdb {

// Masking away bits might be faster than bit shifting (which can be slow). Note that the compiler may optimize this automatically. Todo, investigate.
int fast_popcount32(int32_t x)
{
    return a_popcount_bits[255 & x] + a_popcount_bits[255 & x>> 8] + a_popcount_bits[255 & x>>16] + a_popcount_bits[255 & x>>24];
}
int fast_popcount64(int64_t x)
{
    return fast_popcount32(static_cast<int32_t>(x)) + fast_popcount32(static_cast<int32_t>(x >> 32));
}

// A fast, mediocre-quality random number generator named Xorshift. Thread safe.
uint64_t fastrand(uint64_t max) {
    static util::Atomic<uint64_t> state = 1;
    state.fetch_add_release(1); // Prevent two threads from producing the same value if called at the exact same time
    uint64_t x = state.load_acquire();
    x ^= x >> 12; // a
    x ^= x << 25; // b
    x ^= x >> 27; // c
    state.store_release(x);
    return ((x * 2685821657736338717ULL) % max) + 1;
}

} // namespace tightdb

#endif // select best popcount implementations
