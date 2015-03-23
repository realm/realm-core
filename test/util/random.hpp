/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/
#ifndef REALM_TEST_UTIL_RANDOM_HPP
#define REALM_TEST_UTIL_RANDOM_HPP

#include <limits>
#include <iterator>
#include <utility>
#include <algorithm>

#include <tightdb/util/features.h>
#include <tightdb/util/assert.hpp>
#include <tightdb/util/type_traits.hpp>
#include <tightdb/util/safe_int_ops.hpp>
#include <tightdb/util/thread.hpp>

namespace tightdb {
namespace test_util {


/// Draw a uniformly distributed integer from the specified range using the
/// global pseudorandom number generator. This global generator is based on an
/// instance of `Random` and is therefore independent of other generators such
/// as the one availble via std::rand(). This function is thread safe.
///
/// The thread-safety of this function means that it is relatively slow, so if
/// you need to draw many random numbers efficiently, consider creating your own
/// instance of `Random`.
template<class T> T random_int(T min, T max) REALM_NOEXCEPT;

/// Same as `random_int(lim::min(), lim::max())` where `lim` is
/// `std::numeric_limits<T>`.
template<class T> T random_int() REALM_NOEXCEPT;

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



/// Mersenne Twister by Matsumoto and Nishimura, 1998 (MT19937).
///
/// http://www.math.sci.hiroshima-u.ac.jp/~m-mat/MT/MT2002/emt19937ar.html
///
/// \tparam w Word size, the number of bits in each element of the state vector.
///
/// \tparam n, m Shift values.
///
/// \tparam r, a Twist value and conditional xor-mask.
///
/// \tparam u, d, s, b, t, c, l Bit-scrambling matrix.
///
/// \tparam f Initialization multiplier.
template<class UIntType, int w, int n, int m, int r, UIntType a,
         int u, UIntType d, int s, UIntType b, int t, UIntType c, int l,
         UIntType f>
class MersenneTwisterEngine {
public:
    typedef UIntType result_type;

    static const int word_size = w;
    static const int state_size = n;
    static const int shift_size = m;
    static const int mask_bits = r;
    static const result_type xor_mask = a;
    static const int tempering_u = u;
    static const result_type tempering_d = d;
    static const int tempering_s = s;
    static const result_type tempering_b = b;
    static const int tempering_t = t;
    static const result_type tempering_c = c;
    static const int tempering_l = l;
    static const result_type initialization_multiplier = f;
    static const result_type default_seed = 5489U;

    MersenneTwisterEngine(result_type value = default_seed) REALM_NOEXCEPT;

    void seed(result_type) REALM_NOEXCEPT;

    result_type operator()() REALM_NOEXCEPT;

    static result_type min() REALM_NOEXCEPT;
    static result_type max() REALM_NOEXCEPT;

private:
    UIntType m_x[state_size];
    int m_p;

    void gen_rand() REALM_NOEXCEPT;
};


/// 32-bit Mersenne Twister.
typedef MersenneTwisterEngine<util::FastestUnsigned<32>::type,
                              32, 624, 397, 31, 0x9908B0DFUL,
                              11, 0xFFFFFFFFUL,
                              7,  0x9D2C5680UL,
                              15, 0xEFC60000UL,
                              18, 1812433253UL> MT19937;


template<class T> class UniformIntDistribution {
public:
    UniformIntDistribution(T min = 0, T max = std::numeric_limits<T>::max()) REALM_NOEXCEPT;

    template<class G> T operator()(G& generator) const REALM_NOEXCEPT;

private:
    T m_min, m_max;

    // Requires that `min` < `max`
    template<class G> static T draw(G& generator, T min, T max) REALM_NOEXCEPT;
};



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
    MT19937 m_engine;
};





// Implementation

template<class UIntType, int w, int n, int m, int r, UIntType a,
         int u, UIntType d, int s, UIntType b, int t, UIntType c, int l,
         UIntType f>
inline MersenneTwisterEngine<UIntType, w, n, m, r, a, u, d, s, b, t, c, l, f>::
MersenneTwisterEngine(result_type value) REALM_NOEXCEPT
{
    seed(value);
}

template<class UIntType, int w, int n, int m, int r, UIntType a,
         int u, UIntType d, int s, UIntType b, int t, UIntType c, int l,
         UIntType f>
inline void MersenneTwisterEngine<UIntType, w, n, m, r, a, u, d, s, b, t, c, l, f>::
seed(result_type sd) REALM_NOEXCEPT
{
    int excess_bits = std::numeric_limits<UIntType>::digits - word_size;
    UIntType mask = ~UIntType() >> excess_bits;
    m_x[0] = sd & mask;

    for (int i = 1; i != state_size; ++i) {
        UIntType x = m_x[i - 1];
        x ^= x >> (word_size - 2);
        x *= initialization_multiplier;
        x += i;
        m_x[i] = x & mask;
    }
    m_p = state_size;
}

template<class UIntType, int w, int n, int m, int r, UIntType a,
         int u, UIntType d, int s, UIntType b, int t, UIntType c, int l,
         UIntType f>
inline UIntType MersenneTwisterEngine<UIntType, w, n, m, r, a, u, d, s, b, t, c, l, f>::
operator()() REALM_NOEXCEPT
{
    if (m_p >= state_size)
        gen_rand();

    UIntType z = m_x[m_p++];
    z ^= (z >> tempering_u) & tempering_d;
    z ^= (z << tempering_s) & tempering_b;
    z ^= (z << tempering_t) & tempering_c;
    z ^= (z >> tempering_l);

    return z;
}

template<class UIntType, int w, int n, int m, int r, UIntType a,
         int u, UIntType d, int s, UIntType b, int t, UIntType c, int l,
         UIntType f>
inline UIntType MersenneTwisterEngine<UIntType, w, n, m, r, a, u, d, s, b, t, c, l, f>::
min() REALM_NOEXCEPT
{
    return 0;
}

template<class UIntType, int w, int n, int m, int r, UIntType a,
         int u, UIntType d, int s, UIntType b, int t, UIntType c, int l,
         UIntType f>
inline UIntType MersenneTwisterEngine<UIntType, w, n, m, r, a, u, d, s, b, t, c, l, f>::
max() REALM_NOEXCEPT
{
    int excess_bits = std::numeric_limits<UIntType>::digits - word_size;
    return ~result_type() >> excess_bits;
}

template<class UIntType, int w, int n, int m, int r, UIntType a,
         int u, UIntType d, int s, UIntType b, int t, UIntType c, int l,
         UIntType f>
inline void MersenneTwisterEngine<UIntType, w, n, m, r, a, u, d, s, b, t, c, l, f>::
gen_rand() REALM_NOEXCEPT
{
    UIntType upper_mask = (~UIntType()) << mask_bits;
    UIntType lower_mask = ~upper_mask;

    for (int i = 0; i != (state_size - shift_size); ++i) {
        UIntType x = ((m_x[i] & upper_mask) | (m_x[i + 1] & lower_mask));
        m_x[i] = (m_x[i + shift_size] ^ (x >> 1) ^ ((x & 0x01) ? xor_mask : 0));
    }

    for (int i = (state_size - shift_size); i != (state_size - 1); ++i) {
        UIntType x = ((m_x[i] & upper_mask) | (m_x[i + 1] & lower_mask));
        m_x[i] = (m_x[i + (shift_size - state_size)] ^ (x >> 1) ^ ((x & 0x01) ? xor_mask : 0));
    }

    UIntType x = ((m_x[state_size - 1] & upper_mask) | (m_x[0] & lower_mask));
    m_x[state_size - 1] = (m_x[shift_size - 1] ^ (x >> 1) ^ ((x & 0x01) ? xor_mask : 0));

    m_p = 0;
}


template<class T>
inline UniformIntDistribution<T>::UniformIntDistribution(T min, T max) REALM_NOEXCEPT:
    m_min(min),
    m_max(max)
{
}

template<class T> template<class G>
inline T UniformIntDistribution<T>::operator()(G& generator) const REALM_NOEXCEPT
{
    if (m_min >= m_max)
        return m_min;
    return draw(generator, m_min, m_max);
}

template<class T> template<class G>
inline T UniformIntDistribution<T>::draw(G& generator, T min, T max) REALM_NOEXCEPT
{
    // FIXME: This implementation assumes that if `T` is signed then there
    // exists an unsigned type with at least one more value bit than `T`
    // has. While this is typically the case, it is not guaranteed by the
    // standard, not even when `T` is a standard integer type.
    typedef std::numeric_limits<typename G::result_type> lim_g;
    typedef std::numeric_limits<T> lim_t;
    const int uint_bits_g = lim_g::is_signed ? lim_g::digits + 1 : lim_g::digits;
    const int uint_bits_t = lim_t::is_signed ? lim_t::digits + 1 : lim_t::digits;
    const int uint_bits = uint_bits_g >= uint_bits_t ? uint_bits_g : uint_bits_t;
    typedef typename util::FastestUnsigned<uint_bits>::type uint_type;

    uint_type gen_max = uint_type(G::max()) - uint_type(G::min());
    uint_type val_max = uint_type(max) - uint_type(min);

    uint_type value = uint_type(generator()) - uint_type(G::min());
    if (val_max < gen_max) {
        // Reduction
        uint_type num_values = val_max + 1;
        uint_type num_modules = 1 + (gen_max - val_max) / num_values;
        uint_type compound_size = num_modules * num_values;
        if (compound_size > 0) {
            // `(gen_max+1) / num_values` has remainder
            while (REALM_UNLIKELY(value >= compound_size))
                value = uint_type(generator()) - uint_type(G::min());
        }
        value /= num_modules;
    }
    else if (val_max > gen_max) {
        // Expansion
        uint_type num_gen_values = gen_max + 1;
        uint_type val_max_2 = val_max / (num_gen_values ? num_gen_values : 1); // removed div by 0 warning by vs2013 (todo, investigate)
        for (;;) {
            uint_type v = num_gen_values * draw(generator, 0, T(val_max_2));
            value += v;
            // Previous addition may have overflowed, so we have to test for
            // that too
            if (value <= val_max && value >= v)
                break;
            value = uint_type(generator()) - uint_type(G::min());
        }
    }
    return util::from_twos_compl<T>(uint_type(min) + value);
}


inline Random::Random() REALM_NOEXCEPT:
    m_engine(MT19937::default_seed)
{
}

inline Random::Random(unsigned long seed) REALM_NOEXCEPT:
    m_engine(MT19937::result_type(seed))
{
}

inline void Random::seed(unsigned long seed) REALM_NOEXCEPT
{
    m_engine.seed(MT19937::result_type(seed));
}

template<class T> inline T Random::draw_int(T min, T max) REALM_NOEXCEPT
{
    return UniformIntDistribution<T>(min, max)(m_engine);
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


} // namespace test_util

namespace _impl {

struct GlobalRandom {
    util::Mutex m_mutex;
    test_util::Random m_random;
    static GlobalRandom& get() REALM_NOEXCEPT;
};

} // namespace _impl

namespace test_util {


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


} // namespace test_util
} // namespace tightdb

#endif // REALM_TEST_UTIL_RANDOM_HPP
