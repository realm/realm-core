/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
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

#ifndef REALM_TEST_UTIL_RANDOM_HPP
#define REALM_TEST_UTIL_RANDOM_HPP

#include <limits>
#include <iterator>
#include <utility>
#include <algorithm>
#include <random>

#include <realm/util/features.h>
#include <realm/util/assert.hpp>
#include <realm/util/type_traits.hpp>
#include <realm/util/safe_int_ops.hpp>
#include <realm/util/thread.hpp>

namespace realm {
namespace test_util {


/// Draw a uniformly distributed integer from the specified range using the
/// global pseudorandom number generator. This global generator is based on an
/// instance of `Random` and is therefore independent of other generators such
/// as the one availble via std::rand(). This function is thread safe.
///
/// The thread-safety of this function means that it is relatively slow, so if
/// you need to draw many random numbers efficiently, consider creating your own
/// instance of `Random`.
template <class T>
T random_int(T min, T max) noexcept;

/// Same as `random_int(lim::min(), lim::max())` where `lim` is
/// `std::numeric_limits<T>`.
template <class T>
T random_int() noexcept;

/// Reseed the global pseudorandom number generator that is used by
/// random_int().
///
/// This function is thread safe.
void random_seed(unsigned long) noexcept;

/// To the extent possible, produce a nondeterministic value for seeding a
/// pseudorandom number genrator.
///
/// This function is thread safe.
unsigned int produce_nondeterministic_random_seed();


/// Simple pseudorandom number generator.
class Random {
public:
    Random() noexcept;

    Random(unsigned long) noexcept;

    /// Reseed this pseudorandom number generator.
    void seed(unsigned long) noexcept;

    /// Draw a uniformly distributed floating point value from the half-open
    /// interval [`a`, `b`). It is an error if `b` is less than, or equal to
    /// `a`.
    ///
    /// \tparam T One of the fundamental floating point types.
    template <class T>
    T draw_float(T a, T b) noexcept;

    /// Same as draw_float(T(0), T(1)).
    template <class T>
    T draw_float() noexcept;

    /// Draw a uniformly distributed integer from the specified range. It is an
    /// error if `min` is greater than `max`.
    ///
    /// \tparam T One of the fundamental integer types. All character types are
    /// supported.
    template <class T>
    T draw_int(T min, T max) noexcept;

    /// Same as `draw_int(lim::min(), lim::max())` where `lim` is
    /// `std::numeric_limits<T>`.
    template <class T>
    T draw_int() noexcept;

    /// Same as `draw_int<T>(0, max)`. It is an error to specify a `max` less
    /// than 0.
    template <class T>
    T draw_int_max(T max) noexcept;

    /// Same as `draw_int_max(module_size-1)`. It is an error to specify a
    /// module size less than 1.
    template <class T>
    T draw_int_mod(T module_size) noexcept;

    /// Same as `draw_int<T>(max)` where `max` is one less than 2 to the power
    /// of `bits`. It is an error to specify a number of bits less than zero, or
    /// greater than `lim::digits` where `lim` is `std::numeric_limits<T>`.
    template <class T>
    T draw_int_bits(int bits) noexcept;

    /// Draw true `n` out of `m` times. It is an error if `n` is less than 1, or
    /// if `m` is less than `n`.
    bool chance(int n, int m) noexcept;

    /// Same as `chance(1,2)`.
    bool draw_bool() noexcept;

    /// Reorder the specified elements such that each possible permutation has
    /// an equal probability of appearing.
    template <class RandomIt>
    void shuffle(RandomIt begin, RandomIt end);

private:
    std::mt19937 m_engine;
};

// Implementation

inline Random::Random() noexcept
    : m_engine(std::mt19937::default_seed)
{
}

inline Random::Random(unsigned long initial_seed) noexcept
    : m_engine(std::mt19937::result_type(initial_seed))
{
}

inline void Random::seed(unsigned long new_seed) noexcept
{
    m_engine.seed(std::mt19937::result_type(new_seed));
}

template <class T>
inline T Random::draw_float(T a, T b) noexcept
{
    return std::uniform_real_distribution<T>(a, b)(m_engine);
}

template <class T>
inline T Random::draw_float() noexcept
{
    return draw_float(T(0), T(1));
}

template <class T>
inline T Random::draw_int(T min, T max) noexcept
{
    // Since the standard does not require that
    // `std::uniform_int_distribution<>` can handle character types, we need
    // special treatment of those.
    using U = decltype(T() + 0); // Convert character to interger type
    U value = std::uniform_int_distribution<U>(min, max)(m_engine);
    return T(value);
}

template <class T>
inline T Random::draw_int() noexcept
{
    typedef std::numeric_limits<T> lim;
    return draw_int(lim::min(), lim::max());
}

template <class T>
inline T Random::draw_int_max(T max) noexcept
{
    return draw_int(T(), max);
}

template <class T>
inline T Random::draw_int_mod(T module_size) noexcept
{
    return draw_int_max(T(module_size - 1));
}

template <class T>
inline T Random::draw_int_bits(int bits) noexcept
{
    if (bits <= 0)
        return T();
    T bit = T(1) << (bits - 1);
    T max = bit + (bit - 1);
    return draw_int_max(max);
}

inline bool Random::chance(int n, int m) noexcept
{
    return draw_int_mod(m) < n;
}

inline bool Random::draw_bool() noexcept
{
    return draw_int(0, 1) == 1;
}

template <class RandomIt>
inline void Random::shuffle(RandomIt begin, RandomIt end)
{
    typedef typename std::iterator_traits<RandomIt>::difference_type diff_type;
    diff_type n = end - begin;
    for (diff_type i = n - 1; i > 0; --i) {
        using std::swap;
        swap(begin[i], begin[draw_int_max(i)]);
    }
}


} // namespace test_util

namespace _impl {

struct GlobalRandom {
    util::Mutex m_mutex;
    test_util::Random m_random;
    static GlobalRandom& get() noexcept;
};

} // namespace _impl

namespace test_util {


template <class T>
inline T random_int(T min, T max) noexcept
{
    _impl::GlobalRandom& r = _impl::GlobalRandom::get();
    util::LockGuard lock(r.m_mutex);
    return r.m_random.draw_int(min, max);
}

template <class T>
inline T random_int() noexcept
{
    typedef std::numeric_limits<T> lim;
    return random_int(lim::min(), lim::max());
}

inline void random_seed(unsigned long initial_seed) noexcept
{
    _impl::GlobalRandom& r = _impl::GlobalRandom::get();
    util::LockGuard lock(r.m_mutex);
    return r.m_random.seed(initial_seed);
}


} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_RANDOM_HPP
