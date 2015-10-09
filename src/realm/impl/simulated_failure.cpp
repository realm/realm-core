#include <stdexcept>

#include <realm/impl/simulated_failure.hpp>

using namespace realm::_impl;

#ifdef REALM_DEBUG
#  if !REALM_PLATFORM_APPLE_IOS

namespace {

const int num_failure_types = SimulatedFailure::_num_failure_types;
REALM_THREAD_LOCAL bool primed_failure_types[num_failure_types];

} // anonymous namespace


void SimulatedFailure::do_prime(type failure_type)
{
    primed_failure_types[failure_type] = true;
}

void SimulatedFailure::do_unprime(type failure_type) noexcept
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

#  else // !REALM_PLATFORM_APPLE_IOS

#include <pthread.h>
#include <stdlib.h>

namespace {

    const int num_failure_types = SimulatedFailure::_num_failure_types;
    pthread_key_t key;

    bool * get_threadlocal_primed_failure_types() {
        pthread_key_create(&key, NULL);
        if (bool *primed_failure_types = static_cast<bool *>(pthread_getspecific(key))) {
            return primed_failure_types;
        }
        auto primed_failure_types = calloc(num_failure_types, sizeof(bool));
        pthread_setspecific(key, primed_failure_types);
        return static_cast<bool *>(primed_failure_types);
    }

} // anonymous namespace

void SimulatedFailure::do_prime(type failure_type)
{
    get_threadlocal_primed_failure_types()[failure_type] = true;
}

void SimulatedFailure::do_unprime(type failure_type) noexcept
{
    get_threadlocal_primed_failure_types()[failure_type] = false;
}

void SimulatedFailure::do_check(type failure_type)
{
    auto primed_failure_types = get_threadlocal_primed_failure_types();
    if (primed_failure_types[failure_type]) {
        primed_failure_types[failure_type] = false;
        throw SimulatedFailure();
    }
}

#  endif // !REALM_IOS
#endif // REALM_DEBUG
