#include <algorithm>
#include <stdexcept>
#include <system_error>

#if REALM_IOS
#  define USE_PTHREADS_IMPL 1
#else
#  define USE_PTHREADS_IMPL 0
#endif

#if USE_PTHREADS_IMPL
#  include <pthread.h>
#endif

#include <realm/util/basic_system_errors.hpp>
#include <realm/impl/simulated_failure.hpp>

using namespace realm;
using namespace realm::_impl;

#ifdef REALM_DEBUG

namespace {

const int num_failure_types = SimulatedFailure::_num_failure_types;

#  if !USE_PTHREADS_IMPL


REALM_THREAD_LOCAL bool primed_failure_types[num_failure_types];

bool* get() noexcept
{
    return primed_failure_types;
}


#  else // USE_PTHREADS_IMPL


pthread_key_t key;
pthread_once_t key_once = PTHREAD_ONCE_INIT;

void destroy(void* ptr) noexcept
{
    bool* primed_failure_types = static_cast<bool*>(ptr);
    delete[] primed_failure_types;
}

void create() noexcept
{
    int ret = pthread_key_create(&key, &destroy);
    if (REALM_UNLIKELY(ret != 0)) {
        std::error_code ec = util::make_basic_system_error_code(errno);
        throw std::system_error(ec); // Termination intended
    }
}

bool* get() noexcept
{
    pthread_once(&key_once, &create);
    void* ptr = pthread_getspecific(key);
    bool* primed_failure_types = static_cast<bool*>(ptr);
    if (!primed_failure_types) {
        primed_failure_types = new bool[num_failure_types]; // Throws with intended termination
        std::fill(primed_failure_types, primed_failure_types+num_failure_types, false);
        int ret = pthread_setspecific(key, primed_failure_types);
        if (REALM_UNLIKELY(ret != 0)) {
            std::error_code ec = util::make_basic_system_error_code(errno);
            throw std::system_error(ec); // Termination intended
        }
    }
    return primed_failure_types;
}


#  endif // USE_PTHREADS_IMPL

} // unnamed namespace


void SimulatedFailure::do_prime(type failure_type)
{
    get()[failure_type] = true;
}

void SimulatedFailure::do_unprime(type failure_type) noexcept
{
    get()[failure_type] = false;
}

void SimulatedFailure::do_check(type failure_type)
{
    bool* primed_failure_types = get();
    if (primed_failure_types[failure_type]) {
        primed_failure_types[failure_type] = false;
        throw SimulatedFailure();
    }
}

#endif // REALM_DEBUG
