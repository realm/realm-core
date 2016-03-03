#include <realm/impl/simulated_failure.hpp>

using namespace realm::_impl;

void SimulatedFailure::do_fail(void* userdata)
{
    auto event = static_cast<DebugTrace::Event>(reinterpret_cast<size_t>(userdata));
    unprime(event);
    throw SimulatedFailure();
}

