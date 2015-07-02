#include <realm/impl/simulated_failure.hpp>

using namespace realm::_impl;


#ifdef REALM_DEBUG

thread_local bool SimulatedFailure::primed_failure_types[SimulatedFailure::num_failure_types];

#endif
