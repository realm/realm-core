#include <algorithm>
#include <stdexcept>
#include <system_error>
#include <random>

#include <realm/util/features.h>
#include <realm/util/assert.hpp>
#include <realm/util/basic_system_errors.hpp>
#include <realm/impl/simulated_failure.hpp>

#if REALM_IOS
#  define USE_PTHREADS_IMPL 1
#else
#  define USE_PTHREADS_IMPL 0
#endif

#if USE_PTHREADS_IMPL
#  include <pthread.h>
#endif

using namespace realm;
using namespace realm::_impl;

#ifdef REALM_DEBUG

namespace {

const int num_failure_types = SimulatedFailure::_num_failure_types;

enum class PrimeMode { none, one_shot, random };

struct PrimeSlot {
    PrimeMode mode = PrimeMode::none;
    std::uniform_int_distribution<int> dist;
    int n;
};

struct PrimeState {
    PrimeSlot slots[num_failure_types];
    std::mt19937_64 random;
    PrimeState()
    {
        random.seed(std::random_device()());
    }
};

#  if !USE_PTHREADS_IMPL


REALM_THREAD_LOCAL PrimeState t_prime_state;

PrimeState& get() noexcept
{
    return t_prime_state;
}


#  else // USE_PTHREADS_IMPL


pthread_key_t key;
pthread_once_t key_once = PTHREAD_ONCE_INIT;

void destroy(void* ptr) noexcept
{
    PrimeState* prime_state = static_cast<PrimeState*>(ptr);
    delete prime_state;
}

void create() noexcept
{
    int ret = pthread_key_create(&key, &destroy);
    if (REALM_UNLIKELY(ret != 0)) {
        std::error_code ec = util::make_basic_system_error_code(errno);
        throw std::system_error(ec); // Termination intended
    }
}

PrimeState& get() noexcept
{
    pthread_once(&key_once, &create);
    void* ptr = pthread_getspecific(key);
    PrimeState* prime_state = static_cast<PrimeState*>(ptr);
    if (!prime_state) {
        prime_state = new PrimeState; // Throws with intended termination
        int ret = pthread_setspecific(key, prime_state);
        if (REALM_UNLIKELY(ret != 0)) {
            std::error_code ec = util::make_basic_system_error_code(errno);
            throw std::system_error(ec); // Termination intended
        }
    }
    return *prime_state;
}


#  endif // USE_PTHREADS_IMPL

} // unnamed namespace


void SimulatedFailure::do_prime_one_shot(FailureType failure_type)
{
    PrimeState& state = get();
    if (state.slots[failure_type].mode != PrimeMode::none)
        throw std::runtime_error("Overlapping priming");
    state.slots[failure_type].mode = PrimeMode::one_shot;
}

void SimulatedFailure::do_prime_random(FailureType failure_type, int n, int m)
{
    REALM_ASSERT(n >= 0 && m > 0);
    PrimeState& state = get();
    if (state.slots[failure_type].mode != PrimeMode::none)
        throw std::runtime_error("Overlapping priming");
    state.slots[failure_type].mode = PrimeMode::random;
    using param_type = std::uniform_int_distribution<int>::param_type;
    state.slots[failure_type].dist.param(param_type(0,m-1));
    state.slots[failure_type].dist.reset();
    state.slots[failure_type].n = n;
}

void SimulatedFailure::do_unprime(FailureType failure_type) noexcept
{
    PrimeState& state = get();
    state.slots[failure_type].mode = PrimeMode::none;
}

bool SimulatedFailure::do_check_trigger(FailureType failure_type)
{
    PrimeState& state = get();
    switch (state.slots[failure_type].mode) {
        case PrimeMode::none:
            return false;
        case PrimeMode::one_shot:
            state.slots[failure_type].mode = PrimeMode::none;
            return true;
        case PrimeMode::random: {
            int i = state.slots[failure_type].dist(state.random);
            return i < state.slots[failure_type].n;
        }
    }
    REALM_ASSERT(false);
}

#endif // REALM_DEBUG
