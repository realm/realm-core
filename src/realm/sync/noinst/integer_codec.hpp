#ifndef REALM_NOINST_INTEGER_CODEC_HPP
#define REALM_NOINST_INTEGER_CODEC_HPP

#include <cstdint>
#include <limits>
#include <utility>

#include <realm/util/features.h>
#include <realm/util/safe_int_ops.hpp>
#include <realm/util/assert.hpp>


namespace realm {
namespace _impl {

struct Bid128 {
    uint64_t w[2];
};

/// The maximum number of bytes that can be consumed by encode_int() for an
/// integer of the same type, \a T.
template <class T>
constexpr std::size_t encode_int_max_bytes();

/// The size of the specified buffer must be at least what is returned by
/// encode_int_max_bytes().
///
/// Returns the number of bytes used to hold the encoded value.
template <class T>
std::size_t encode_int(T value, char* buffer) noexcept;

/// If decoding succeeds, the decoded value is assigned to \a value and the
/// number of consumed bytes is returned (which will always be at least
/// one). Otherwise \a value is left unmodified, and zero is returned.
template <class T>
std::size_t decode_int(const char* buffer, std::size_t size, T& value) noexcept;

/// \tparam I Must have member function `bool read_char(char&)`.
///
/// If decoding succeeds, the decoded value is assigned to \a value and `true`
/// is returned. Otherwise \a value is left unmodified, and `false` is returned.
template <class I, class T>
bool decode_int(I& input, T& value) noexcept(noexcept(std::declval<I>().read_char(std::declval<char&>())));


// Implementation

// Work-around for insufficient constexpr support from GCC 4.9.
template <class T>
struct EncodeIntMaxBytesHelper {
    // One sign bit plus number of value bits
    static const int num_bits = 1 + std::numeric_limits<T>::digits;
    // Only the first 7 bits are available per byte. Had it not been
    // for the fact that maximum guaranteed bit width of a char is 8,
    // this value could have been increased to 15 (one less than the
    // number of value bits in 'unsigned').
    static const int bits_per_byte = 7;
    static const int max_bytes = (num_bits + (bits_per_byte - 1)) / bits_per_byte;
};

template <class T>
constexpr std::size_t encode_int_max_bytes()
{
    return std::size_t(EncodeIntMaxBytesHelper<T>::max_bytes);
}

template <class T>
std::size_t encode_int(char* buffer, T value)
{
    REALM_DIAG_PUSH();
    REALM_DIAG_IGNORE_UNSIGNED_MINUS();
    static_assert(std::numeric_limits<T>::is_integer, "Integer required");
    T value_2 = value;
    bool negative = false;
    if constexpr (std::is_signed_v<T>) {
        if (value < 0) {
            // The following conversion is guaranteed by C++11 to never overflow
            // (contrast this with "-value_2" which indeed could overflow). See
            // C99+TC3 section 6.2.6.2 paragraph 2.
            value_2 = -(value_2 + 1);
            negative = true;
        }
    }
    // At this point 'value_2' is always a positive number or 0. Also, small negative
    // numbers have been converted to small positive numbers.
    REALM_ASSERT(value_2 >= 0);
    int bits_per_byte = 7; // See encode_int_max_bytes()
    constexpr int max_bytes = int(encode_int_max_bytes<T>());
    using uchar = unsigned char;
    char* ptr = buffer;
    // An explicit constant maximum number of iterations is specified
    // in the hope that it will help the optimizer (to do loop
    // unrolling, for example).
    for (int i = 0; i < max_bytes; ++i) {
        if (value_2 >> (bits_per_byte - 1) == 0)
            break;
        *reinterpret_cast<uchar*>(ptr) =
            uchar((1U << bits_per_byte) | unsigned(value_2 & ((1U << bits_per_byte) - 1)));
        ++ptr;
        value_2 >>= bits_per_byte;
    }
    *reinterpret_cast<uchar*>(ptr) = uchar(negative ? (1U << (bits_per_byte - 1)) | unsigned(value_2) : value_2);
    ++ptr;
    return std::size_t(ptr - buffer);
    REALM_DIAG_POP();
}

template <>
inline std::size_t encode_int(char* buffer, Bid128 value)
{
    auto value_0 = value.w[0];
    auto value_1 = value.w[1];
    constexpr int bits_per_byte = 7;
    using uchar = unsigned char;
    char* ptr = buffer;
    while (value_0 >> (bits_per_byte - 1) || value_1 != 0) {
        unsigned c = unsigned(value_0 & ((1U << bits_per_byte) - 1));
        *reinterpret_cast<uchar*>(ptr) = uchar((1U << bits_per_byte) | c);
        ++ptr;

        value_0 >>= bits_per_byte;
        if (value_1) {
            uint64_t tmp = value_1 & ((1U << bits_per_byte) - 1);
            value_1 >>= bits_per_byte;
            value_0 |= (tmp << (64 - bits_per_byte));
        }
    }
    *reinterpret_cast<uchar*>(ptr) = uchar(value_0);
    ++ptr;
    return std::size_t(ptr - buffer);
}


template <class I, class T>
bool decode_int(I& input, T& value) noexcept(noexcept(std::declval<I>().read_char(std::declval<char&>())))
{
    REALM_DIAG_PUSH();
    REALM_DIAG_IGNORE_UNSIGNED_MINUS();
    T value_2 = 0;
    int part = 0;
    constexpr int max_bytes = int(encode_int_max_bytes<T>());
    for (int i = 0; i != max_bytes; ++i) {
        char c;
        if (!input.read_char(c))
            return false; // Failure: Premature end of input
        using uchar = unsigned char;
        part = uchar(c);
        if (0xFF < part)
            return false; // Failure: Only the first 8 bits may be used in each byte
        if ((part & 0x80) == 0) {
            T p = part & 0x3F;
            if (util::int_shift_left_with_overflow_detect(p, i * 7))
                return false; // Failure: Encoded value too large for `T`
            value_2 |= p;
            break;
        }
        if (i == max_bytes - 1)
            return false; // Failure: Too many bytes
        value_2 |= T(part & 0x7F) << (i * 7);
    }
    if (part & 0x40) {
        // The real value is negative. Because 'value_2' is positive at this
        // point, the following negation is guaranteed by C++11 to never
        // overflow. See C99+TC3 section 6.2.6.2 paragraph 2.
        value_2 = -value_2;
        if (util::int_subtract_with_overflow_detect(value_2, 1))
            return false; // Failure: Encoded value too large for `T`
    }
    value = value_2;
    return true; // Success
    REALM_DIAG_POP();
}

template <class I>
bool decode_int(I& input, _impl::Bid128& value) noexcept
{
    uint64_t value_0 = 0;
    uint64_t value_1 = 0;
    constexpr int max_bytes = 17; // 113 bits / 7

    int i = 0;
    for (;;) {
        char c;
        if (!input.read_char(c))
            return false; // Failure: Premature end of input
        uint64_t part = c & 0x7F;
        if (i < 9) {
            value_0 |= part << (7 * i);
        }
        else if (i == 9) {
            value_0 |= part << 63;
            value_1 |= part >> 1;
        }
        else if (i < max_bytes) {
            value_1 |= part << ((7 * i) - 64);
        }
        else {
            return false; // Failure: Too many bytes
        }
        if ((c & 0x80) == 0) {
            break;
        }
        i++;
    }

    value.w[0] = value_0;
    value.w[1] = value_1;

    return true; // Success
}

template <class T>
std::size_t decode_int(const char* buffer, std::size_t size, T& value) noexcept
{
    struct Input {
        const char* ptr;
        const char* end;
        bool read_char(char& c) noexcept
        {
            if (REALM_LIKELY(ptr != end)) {
                c = *ptr++;
                return true;
            }
            return false;
        }
    };
    Input input{buffer, buffer + size};
    if (REALM_LIKELY(decode_int(input, value))) {
        REALM_ASSERT(input.ptr > buffer);
        return std::size_t(input.ptr - buffer); // Success
    }
    return 0; // Failure
}

} // namespace _impl
} // namespace realm

#endif // REALM_NOINST_INTEGER_CODEC_HPP
