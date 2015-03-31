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
    // FIXME: Initialization of local statics are not guaranteed to be
    // thread safe.
    static GlobalRandom r;
    return r;
}

} // namespace _impl

} // namespace realm
