/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <memory>
#include <algorithm>
#include <stdexcept>
#include <system_error>
#include <random>

#include <realm/util/features.h>
#include <realm/util/assert.hpp>
#include <realm/util/basic_system_errors.hpp>
#include <realm/impl/simulated_failure.hpp>

#if REALM_PLATFORM_APPLE || REALM_ANDROID
#define USE_PTHREADS_IMPL 1
#else
#define USE_PTHREADS_IMPL 0
#endif

#if USE_PTHREADS_IMPL
#include <pthread.h>
#endif

using namespace realm;
using namespace realm::_impl;

#ifdef REALM_ENABLE_SIMULATED_FAILURE

namespace {

const int num_failure_types = SimulatedFailure::_num_failure_types;

struct PrimeMode {
    virtual bool check_trigger() noexcept = 0;
    virtual ~PrimeMode() noexcept {}
};

struct PrimeState {
    std::unique_ptr<PrimeMode> slots[num_failure_types];
};

struct OneShotPrimeMode : PrimeMode {
    bool triggered = false;
    bool check_trigger() noexcept override
    {
        if (triggered)
            return false;
        triggered = true;
        return true;
    }
};

struct RandomPrimeMode : PrimeMode {
    std::mt19937_64 random;
    std::uniform_int_distribution<int> dist;
    int n;
    RandomPrimeMode(int trigger_threshold, int m, uint_fast64_t seed)
        : random(seed)
        , dist(0, m - 1)
        , n(trigger_threshold)
    {
        REALM_ASSERT(n >= 0 && m > 0);
    }
    bool check_trigger() noexcept override
    {
        int i = dist(random);
        return i < n;
    }
};

#if !USE_PTHREADS_IMPL


thread_local PrimeState t_prime_state;

PrimeState& get() noexcept
{
    return t_prime_state;
}


#else // USE_PTHREADS_IMPL


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
        REALM_TERMINATE(ec.message().c_str());
    }
}

PrimeState& get() noexcept
{
    pthread_once(&key_once, &create);
    void* ptr = pthread_getspecific(key);
    PrimeState* prime_state = static_cast<PrimeState*>(ptr);
    if (!prime_state) {
        prime_state = new (std::nothrow) PrimeState; // Throws with intended termination
        int ret = prime_state ? pthread_setspecific(key, prime_state) : ENOMEM;
        if (REALM_UNLIKELY(ret != 0)) {
            std::error_code ec = util::make_basic_system_error_code(errno);
            REALM_TERMINATE(ec.message().c_str());
        }
    }
    return *prime_state;
}


#endif // USE_PTHREADS_IMPL

} // unnamed namespace


void SimulatedFailure::do_prime_one_shot(FailureType failure_type)
{
    PrimeState& state = get();
    if (state.slots[failure_type])
        throw std::runtime_error("Already primed");
    state.slots[failure_type].reset(new OneShotPrimeMode); // Throws
}

void SimulatedFailure::do_prime_random(FailureType failure_type, int n, int m, uint_fast64_t seed)
{
    PrimeState& state = get();
    if (state.slots[failure_type])
        throw std::runtime_error("Already primed");
    state.slots[failure_type].reset(new RandomPrimeMode(n, m, seed)); // Throws
}

void SimulatedFailure::do_unprime(FailureType failure_type) noexcept
{
    PrimeState& state = get();
    state.slots[failure_type].reset();
}

bool SimulatedFailure::do_check_trigger(FailureType failure_type) noexcept
{
    PrimeState& state = get();
    if (PrimeMode* p = state.slots[failure_type].get())
        return p->check_trigger();
    return false;
}

#if REALM_ARCHITECTURE_X86_32 && REALM_IOS
bool (*s_mmap_predicate)(size_t);
#else
thread_local bool (*s_mmap_predicate)(size_t);
#endif // REALM_ARCHITECTURE_X86_32 && REALM_IOS

void SimulatedFailure::do_prime_mmap(bool (*predicate)(size_t))
{
    s_mmap_predicate = predicate;
}

void SimulatedFailure::do_trigger_mmap(size_t size)
{
    if (s_mmap_predicate && s_mmap_predicate(size))
        throw std::bad_alloc();
}

#endif // REALM_ENABLE_SIMULATED_FAILURE


namespace {

class ErrorCategory : public std::error_category {
public:
    const char* name() const noexcept override;
    std::string message(int) const override;
};

const ErrorCategory g_error_category{};

const char* ErrorCategory::name() const noexcept
{
    return "realm.simulated_failure";
}

std::string ErrorCategory::message(int value) const
{
    switch (SimulatedFailure::FailureType(value)) {
        case SimulatedFailure::generic:
            return "Simulated failure (generic)";
        case SimulatedFailure::slab_alloc__reset_free_space_tracking:
            return "Simulated failure (slab_alloc__reset_free_space_tracking)";
        case SimulatedFailure::slab_alloc__remap:
            return "Simulated failure (slab_alloc__remap)";
        case SimulatedFailure::shared_group__grow_reader_mapping:
            return "Simulated failure (shared_group__grow_reader_mapping)";
        case SimulatedFailure::sync_client__read_head:
            return "Simulated failure (sync_client__read_head)";
        case SimulatedFailure::sync_server__read_head:
            return "Simulated failure (sync_server__read_head)";
        case SimulatedFailure::_num_failure_types:
            break;
    }
    REALM_ASSERT(false);
    return std::string();
}

} // unnamed namespace


namespace realm {
namespace _impl {

std::error_code make_error_code(SimulatedFailure::FailureType failure_type) noexcept
{
    return std::error_code(failure_type, g_error_category);
}

} // namespace _impl
} // namespace realm
