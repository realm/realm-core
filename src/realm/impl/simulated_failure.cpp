#include <realm/impl/simulated_failure.hpp>

using namespace realm::_impl;

void SimulatedFailure::do_fail(type event, void* userdata)
{
    static_cast<void>(userdata);
    unprime(event);
    throw SimulatedFailure();
}

