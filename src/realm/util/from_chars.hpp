#ifndef FROM_CHARS_HPP
#define FROM_CHARS_HPP
#include <algorithm>
#include <system_error>
#include <type_traits>

#include <realm/util/safe_int_ops.hpp>

namespace realm::util {
struct FromCharsResult {
    const char* ptr;
    std::errc ec;
};

/*
 * from_chars implements the interface and behavior of https://en.cppreference.com/w/cpp/utility/from_chars
 * for integer values. When from_chars is implemented on all supported platforms/compilers, we can remove
 * this implementation.
 *
 * Use this if you need to parse a string into an integer value without any allocations or having to worry
 * about any user-set locale.
 */
template <typename T>
FromCharsResult from_chars(const char* const first, const char* const last, T& value, int base = 10) noexcept
{
    const auto digit_value = [](char c) -> uint8_t {
        if (c >= '0' && c <= '9')
            return uint8_t(c - '0');
        if (c >= 'a' && c <= 'z')
            return uint8_t(c - 'a' + 10);
        if (c >= 'A' && c <= 'Z')
            return uint8_t(c - 'A' + 10);
        return 0xff; // Illegal digit value for all supported bases.
    };

    if (first == last) {
        return {first, std::errc::invalid_argument};
    }

    auto ptr = first;
    if constexpr (std::is_signed_v<T>) {
        if (*ptr == '-') {
            ++ptr;
        }
    }

    T res_value = 0;
    for (; ptr != last; ++ptr) {
        auto digit = digit_value(*ptr);
        if (digit >= base) {
            break;
        }

        bool overflow = int_multiply_with_overflow_detect(res_value, base);
        if (!overflow) {
            overflow = int_add_with_overflow_detect(res_value, digit);
        }
        if (overflow) {
            ptr = std::find_if_not(ptr, last, [&](char ch) {
                return digit_value(ch) <= base - 1;
            });
            return {ptr, std::errc::result_out_of_range};
        }
    }

    if (first == ptr) {
        return {first, std::errc::invalid_argument};
    }

    if constexpr (std::is_signed_v<T>) {
        if (*first == '-' && int_multiply_with_overflow_detect(res_value, -1)) {
            return {ptr, std::errc::result_out_of_range};
        }
    }

    value = res_value;
    return {ptr, {}};
}

} // namespace realm::util

#endif // FROM_CHARS_HPP
