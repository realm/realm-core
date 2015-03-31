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
    static GlobalRandom r { std::mt19937::result_type(util::produce_nondeterministic_random_seed()) };
    return r;
}

} // namespace _impl

} // namespace realm
