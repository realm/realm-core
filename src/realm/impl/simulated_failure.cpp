#include <stdexcept>

#include <realm/impl/simulated_failure.hpp>

using namespace realm::_impl;

#ifdef REALM_DEBUG
#  if !REALM_IOS

namespace {

const int num_failure_types = SimulatedFailure::_num_failure_types;
REALM_THREAD_LOCAL bool primed_failure_types[num_failure_types];

} // anonymous namespace


void SimulatedFailure::do_prime(type failure_type)
{
    primed_failure_types[failure_type] = true;
}

void SimulatedFailure::do_unprime(type failure_type) REALM_NOEXCEPT
{
    primed_failure_types[failure_type] = false;
}

void SimulatedFailure::do_check(type failure_type)
{
    if (primed_failure_types[failure_type]) {
        primed_failure_types[failure_type] = false;
        throw SimulatedFailure();
    }
}

#  else // !REALM_IOS

void SimulatedFailure::do_prime(type)
{
    throw std::runtime_error("Simulated failure is not supported on iOS");
}

void SimulatedFailure::do_unprime(type) REALM_NOEXCEPT
{
    throw std::runtime_error("Simulated failure is not supported on iOS");
}

void SimulatedFailure::do_check(type)
{
    throw std::runtime_error("Simulated failure is not supported on iOS");
}

#  endif // !REALM_IOS
#endif // REALM_DEBUG
