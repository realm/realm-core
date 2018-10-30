/*************************************************************************
 *
 * Copyright 2017 Realm Inc.
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

#include "string_data.hpp"

#include <vector>

using namespace realm;

namespace {

template <bool has_alternate_pattern>
REALM_FORCEINLINE
bool matchlike(const StringData& text, const StringData& pattern, const StringData* alternate_pattern=nullptr) noexcept
{
    // If alternate_pattern is provided, it is assumed to differ from `pattern` only in case.
    REALM_ASSERT_DEBUG(has_alternate_pattern == bool(alternate_pattern));
    REALM_ASSERT_DEBUG(!alternate_pattern || pattern.size() == alternate_pattern->size());

    std::vector<size_t> textpos;
    std::vector<size_t> patternpos;
    size_t p1 = 0; // position in text (haystack)
    size_t p2 = 0; // position in pattern (needle)

    while (true) {
        if (p1 == text.size()) {
            // We're at the end of the text. This is a match if:
            // - we're also at the end of the pattern; or
            if (p2 == pattern.size())
                return true;

            // - we're at the last character of the pattern, and it's a multi-character wildcard.
            if (p2 == pattern.size() - 1 && pattern[p2] == '*')
                return true;

            goto no_match;
        }

        if (p2 == pattern.size()) {
            // We've hit the end of the pattern without matching the entirety of the text.
            goto no_match;
        }

        if (pattern[p2] == '*') {
            // Multi-character wildcard. Remember our position in case we need to backtrack.
            textpos.push_back(p1);
            patternpos.push_back(++p2);
            continue;
        }

        if (pattern[p2] == '?') {
            // utf-8 encoded characters may take up multiple bytes
            if ((text[p1] & 0x80) == 0) {
                ++p1;
                ++p2;
                continue;
            }
            else {
                size_t p = 1;
                while (p1 + p != text.size() && (text[p1 + p] & 0xc0) == 0x80)
                    ++p;
                p1 += p;
                ++p2;
                continue;
            }
        }

        if (pattern[p2] == text[p1]) {
            ++p1;
            ++p2;
            continue;
        }

        if (has_alternate_pattern && (*alternate_pattern)[p2] == text[p1]) {
            ++p1;
            ++p2;
            continue;
        }

    no_match:
        if (textpos.empty()) {
            // We were performing the outermost level of matching, so if we made it here the text did not match.
            return false;
        }

        if (p1 == text.size()) {
            // We've hit the end of the text without a match, so backtrack.
            textpos.pop_back();
            patternpos.pop_back();
        }

        if (textpos.empty()) {
            // We finished our last backtrack attempt without finding a match, so the text did not match.
            return false;
        }

        // Reattempt the match from the next character.
        p1 = ++textpos.back();
        p2 = patternpos.back();
    }
}

} // unnamed namespace

bool StringData::matchlike(const realm::StringData& text, const realm::StringData& pattern) noexcept
{
    return ::matchlike<false>(text, pattern);
}

bool StringData::matchlike_ins(const StringData& text, const StringData& pattern_upper,
                               const StringData& pattern_lower) noexcept
{
    return ::matchlike<true>(text, pattern_upper, &pattern_lower);
}


namespace {
template <size_t = sizeof(void*)>
struct Murmur2OrCityHash;

template <>
struct Murmur2OrCityHash<8> {
    uint_least64_t operator()(const unsigned char* data, size_t len) const noexcept
    {
        return cityhash_64(data, len);
    }
};

template <>
struct Murmur2OrCityHash<4> {
    uint_least32_t operator()(const unsigned char* data, size_t len) const noexcept
    {
        return murmur2_32(data, len);
    }
};

uint_least32_t load4(const unsigned char* data)
{
    uint_least32_t word = 0;
    std::memcpy(&word, data, 4);
    return word;
}

uint_least64_t load8(const unsigned char* data)
{
    uint_least64_t word = 0;
    std::memcpy(&word, data, 8);
    return word;
}

} // unnamed namespace


size_t realm::murmur2_or_cityhash(const unsigned char* data, size_t len) noexcept
{
    return size_t(Murmur2OrCityHash<>{}(data, len));
}

uint_least32_t realm::murmur2_32(const unsigned char* data, size_t len) noexcept
{
    // This implementation is copied from libc++.
    // See: https://github.com/llvm-mirror/libcxx/blob/master/include/utility

    REALM_ASSERT_DEBUG(len <= std::numeric_limits<uint_least32_t>::max());

    const uint_least32_t m = 0x5bd1e995UL;
    const uint_least32_t r = 24;
    uint_least32_t h = uint_least32_t(len);

    for (; len >= 4; data += 4, len -= 4) {
        uint_least32_t k = load4(data);
        k *= m;
        k ^= k >> r;
        k *= m;
        h *= m;
        h ^= k;
    }

    switch (len) {
        case 3:
            h ^= data[2] << 16;
        case 2:
            h ^= data[1] << 8;
        case 1:
            h ^= data[0];
            h *= m;
    }
    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;
    return h;
}

namespace {
struct CityHash64 {
    // This implementation is copied from libc++.
    // See: https://github.com/llvm-mirror/libcxx/blob/master/include/utility

    static const uint_least64_t k0 = 0xc3a5c85c97cb3127ULL;
    static const uint_least64_t k1 = 0xb492b66fbe98f273ULL;
    static const uint_least64_t k2 = 0x9ae16a3b2f90404fULL;
    static const uint_least64_t k3 = 0xc949d7c7509e6557ULL;
    using pair = std::pair<uint_least64_t, uint_least64_t>;

    uint_least64_t operator()(const unsigned char* data, size_t len) const noexcept
    {
        if (len <= 32) {
            if (len <= 16) {
                return hash_len_0_to_16(data, len);
            } else {
                return hash_len_17_to_32(data, len);
            }
        } else if (len <= 64) {
            return hash_len_33_to_64(data, len);
        }
        uint_least64_t x = load8(data + len - 40);
        uint_least64_t y = load8(data + len - 16) + load8(data + len - 56);
        uint_least64_t z = z = hash_len_16(load8(data + len - 48) + len,
                                           load8(data + len - 24));
        pair v = weak_hash_len_32_with_seeds(data + len - 64, len, z);
        pair w = weak_hash_len_32_with_seeds(data + len - 32, y + k1, x);
        x = x * k1 + load8(data);

        // Decrease len to the nearest multiple of 64, and operate on 64-byte
        // chunks.
        len = (len - 1) & ~static_cast<uint_least64_t>(63);
        do {
            x = rotate(x + y + v.first + load8(data + 8), 37) * k1;
            y = rotate(y + v.second + load8(data + 48), 42) * k1;
            x ^= w.second;
            y += v.first + load8(data + 40);
            z = rotate(z + w.first, 33) * k1;
            v = weak_hash_len_32_with_seeds(data, v.second * k1, x + w.first);
            w = weak_hash_len_32_with_seeds(data + 32, z + w.second,
                                            y + load8(data + 16));
            std::swap(z, x);
            data += 64;
            len -= 64;
        } while (len != 0);
        return hash_len_16(
            hash_len_16(v.first, w.first) + shift_mix(y) * k1 + z,
            hash_len_16(v.second, w.second) + x);
    }

    static uint_least64_t hash_len_0_to_16(const unsigned char* data, size_t len) noexcept
    {
        if (len > 8) {
            const auto a = load8(data);
            const auto b = load8(data + len - 8);
            return hash_len_16(a, rotate_by_at_least_1(b + len, int(len))) ^ b;
        }
        if (len >= 4) {
            const auto a = load4(data);
            const auto b = load4(data + len - 4);
            return hash_len_16(len + (a << 3), b);
        }
        if (len > 0) {
            const auto a = data[0];
            const auto b = data[len >> 1];
            const auto c = data[len - 1];
            const auto y = static_cast<uint_least32_t>(a) + 
                (static_cast<uint_least32_t>(b) << 8);
            const auto z = static_cast<uint_least32_t>(len) +
                (static_cast<uint_least32_t>(c) << 2);
            return shift_mix(y * k2 ^z * k3) * k2;
        }
        return k2;
    }

    static uint_least64_t hash_len_17_to_32(const unsigned char* data, size_t len) noexcept
    {
        const auto a = load8(data) * k1;
        const auto b = load8(data + 8);
        const auto c = load8(data + len - 8) * k2;
        const auto d = load8(data + len - 16) * k0;
        return hash_len_16(rotate(a - b, 43) + rotate(c, 30) + d,
                           a + rotate(b ^ k3, 20) - c + len);
    }

    static uint_least64_t hash_len_33_to_64(const unsigned char* data, size_t len) noexcept
    {
        uint_least64_t z = load8(data + 24);
        uint_least64_t a = load8(data) + (len + load8(data + len - 16)) * k0;
        uint_least64_t b = rotate(a + z, 52);
        uint_least64_t c = rotate(a, 37);
        a += load8(data + 8);
        c += rotate(a, 7);
        a += load8(data + 16);
        uint_least64_t vf = a + z;
        uint_least64_t vs = b + rotate(a, 31) + c;
        a = load8(data + 16) + load8(data + len - 32);
        z += load8(data + len - 8);
        b = rotate(a + z, 52);
        c = rotate(a, 37);
        a += load8(data + len - 24);
        c += rotate(a, 7);
        a += load8(data + len - 16);
        uint_least64_t wf = a + z;
        uint_least64_t ws = b + rotate(a, 31) + c;
        uint_least64_t r = shift_mix((vf + ws) * k2 + (wf + vs) * k0);
        return shift_mix(r * k0 + vs) * k2;
    }

    static uint_least64_t hash_len_16(uint_least64_t u, uint_least64_t v) noexcept
    {
        const uint_least64_t mul = 0x9ddfea08eb382d69ULL;
        uint_least64_t a = (u ^ v) * mul;
        a ^= (a >> 47);
        uint_least64_t b = (v ^ a) * mul;
        b ^= (b >> 47);
        b *= mul;
        return b;
    }

    static pair weak_hash_len_32_with_seeds(uint_least64_t w, uint_least64_t x,
                                            uint_least64_t y, uint_least64_t z,
                                            uint_least64_t a, uint_least64_t b) noexcept
    {
        a += w;
        b = rotate(b + a + z, 21);
        const uint_least64_t c = a;
        a += x;
        a += y;
        b += rotate(a, 44);
        return pair{a + z, b + c};
    }

    static pair weak_hash_len_32_with_seeds(const unsigned char* data,
                                            uint_least64_t a, uint_least64_t b) noexcept
    {
        return weak_hash_len_32_with_seeds(load8(data), load8(data + 8),
                                           load8(data + 16), load8(data + 24),
                                           a, b);
    }

    static inline uint_least64_t rotate(uint_least64_t val, int shift) noexcept
    {
        return shift == 0 ? val : rotate_by_at_least_1(val, shift);
    }

    static inline uint_least64_t rotate_by_at_least_1(uint_least64_t val, int shift) noexcept
    {
        return (val >> shift) | (val << (64 - shift));
    }

    static inline uint_least64_t shift_mix(uint_least64_t val) noexcept
    {
        return val ^ (val >> 47);
    }
};
} // unnamed namespace

uint_least64_t realm::cityhash_64(const unsigned char* data, size_t len) noexcept
{
    return CityHash64{}(data, len);
}

