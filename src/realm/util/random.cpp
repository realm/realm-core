#include "realm/util/random.hpp"

namespace realm {
namespace util {

unsigned long produce_nondeterministic_random_seed()
{
    return std::random_device{}();
}

} // namespace util

namespace _impl {

GlobalRandom& GlobalRandom::get() REALM_NOEXCEPT
{
    // NOTE: MSVC13 does not implement thread-safe local statics, as otherwise guaranteed by C++11.
    static GlobalRandom r { std::mt19937::result_type(util::produce_nondeterministic_random_seed()) };
    return r;
}

} // namespace _impl

} // namespace realm
