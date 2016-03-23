#include <realm/impl/simulated_failure.hpp>

using namespace realm::_impl;

#if REALM_DEBUG

void SimulatedFailure::do_fail(type event, void* userdata)
{
    static_cast<void>(userdata);
    unprime(event);
    throw SimulatedFailure();
}

#endif // REALM_DEBUG

