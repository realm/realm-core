/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2015] Realm Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Realm Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Realm Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Realm Incorporated.
 *
 **************************************************************************/

#ifndef REALM_UTIL_RANDOM_HPP
#define REALM_UTIL_RANDOM_HPP

#include <limits>
#include <random>

#include <realm/util/thread.hpp>

namespace realm {
namespace util {

/// Draw a uniformly distributed integer from the specified range using the
/// global pseudorandom number generator. This global generator is based on an
/// instance of `Random` and is therefore independent of other generators such
/// as the one availble via std::rand(). This function is thread safe.
///
/// The thread-safety of this function means that it is relatively slow, so if
/// you need to draw many random numbers efficiently, consider creating your own
/// instance of `Random`.
template <class T> T random_int(T min, T max) REALM_NOEXCEPT;

/// Same as `random_int(lim::min(), lim::max())` where `lim` is
/// `std::numeric_limits<T>`.
template <class T> T random_int() REALM_NOEXCEPT;

/// Reseed the global pseudorandom number generator that is used by
/// random_int().
///
/// This function is thread safe.
void random_seed(unsigned long) REALM_NOEXCEPT;

/// To the extent possible, produce a nondeterministic value for seeding a
/// pseudorandom number genrator.
///
/// This function is thread safe.
unsigned long produce_nondeterministic_random_seed();


/// Simple pseudorandom number generator.
class Random {
public:
    Random() REALM_NOEXCEPT;

    Random(unsigned long seed) REALM_NOEXCEPT;

    /// Reseed this pseudorandom number generator.
    void seed(unsigned long) REALM_NOEXCEPT;

    /// Draw a uniformly distributed integer from the specified range. It is an
    /// error if `min` is greater than `max`.
    template<class T> T draw_int(T min, T max) REALM_NOEXCEPT;

    /// Same as `draw_int(lim::min(), lim::max())` where `lim` is
    /// `std::numeric_limits<T>`.
    template<class T> T draw_int() REALM_NOEXCEPT;

    /// Same as `draw_int<T>(0, max)`. It is an error to specify a `max` less
    /// than 0.
    template<class T> T draw_int_max(T max) REALM_NOEXCEPT;

    /// Same as `draw_int_max(module_size-1)`. It is an error to specify a
    /// module size less than 1.
    template<class T> T draw_int_mod(T module_size) REALM_NOEXCEPT;

    /// Same as `draw_int<T>(max)` where `max` is one less than 2 to the power
    /// of `bits`. It is an error to specify a number of bits less than zero, or
    /// greater than `lim::digits` where `lim` is `std::numeric_limits<T>`.
    template<class T> T draw_int_bits(int bits) REALM_NOEXCEPT;

    /// Draw true `n` out of `m` times. It is an error if `n` is less than 1, or
    /// if `m` is less than `n`.
    bool chance(int n, int m) REALM_NOEXCEPT;

    /// Same as `chance(1,2)`.
    bool draw_bool() REALM_NOEXCEPT;

    /// Reorder the specified elements such that each possible permutation has
    /// an equal probability of appearing.
    template<class RandomIt> void shuffle(RandomIt begin, RandomIt end);

private:
    std::mt19937 m_engine;
};


/// Implementation:

inline Random::Random() REALM_NOEXCEPT
{
}

inline Random::Random(unsigned long seed) REALM_NOEXCEPT:
    m_engine(std::mt19937::result_type(seed))
{
}

inline void Random::seed(unsigned long seed) REALM_NOEXCEPT
{
    m_engine.seed(std::mt19937::result_type(seed));
}

template<class T> inline T Random::draw_int(T min, T max) REALM_NOEXCEPT
{
    return std::uniform_int_distribution<T>(min, max)(m_engine);
}

template<class T> inline T Random::draw_int() REALM_NOEXCEPT
{
    typedef std::numeric_limits<T> lim;
    return draw_int(lim::min(), lim::max());
}

template<class T> inline T Random::draw_int_max(T max) REALM_NOEXCEPT
{
    return draw_int(T(), max);
}

template<class T> inline T Random::draw_int_mod(T module_size) REALM_NOEXCEPT
{
    return draw_int_max(T(module_size-1));
}

template<class T> inline T Random::draw_int_bits(int bits) REALM_NOEXCEPT
{
    if (bits <= 0)
        return T();
    T bit = T(1) << (bits-1);
    T max = bit + (bit-1);
    return draw_int_max(max);
}

inline bool Random::chance(int n, int m) REALM_NOEXCEPT
{
    return draw_int_mod(m) < n;
}

inline bool Random::draw_bool() REALM_NOEXCEPT
{
    return draw_int(0,1) == 1;
}

template<class RandomIt> inline void Random::shuffle(RandomIt begin, RandomIt end)
{
    typedef typename std::iterator_traits<RandomIt>::difference_type diff_type;
    diff_type n = end - begin;
    for (diff_type i = n-1; i > 0; --i) {
        using std::swap;
        swap(begin[i], begin[draw_int_max(i)]);
    }
}

} // namespace util

namespace _impl {

struct GlobalRandom {
    util::Mutex m_mutex;
    util::Random m_random;
    static GlobalRandom& get() REALM_NOEXCEPT;
};

} // namespace _impl

namespace util {

template<class T> inline T random_int(T min, T max) REALM_NOEXCEPT
{
    _impl::GlobalRandom& r = _impl::GlobalRandom::get();
    util::LockGuard lock(r.m_mutex);
    return r.m_random.draw_int(min, max);
}

template<class T> inline T random_int() REALM_NOEXCEPT
{
    typedef std::numeric_limits<T> lim;
    return random_int(lim::min(), lim::max());
}

inline void random_seed(unsigned long seed) REALM_NOEXCEPT
{
    _impl::GlobalRandom& r = _impl::GlobalRandom::get();
    util::LockGuard lock(r.m_mutex);
    return r.m_random.seed(seed);
}

} // namespace util
} // namespace realm

#endif // REALM_UTIL_RANDOM_HPP
