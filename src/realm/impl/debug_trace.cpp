#include <algorithm>
#include <system_error>

#include <realm/impl/debug_trace.hpp>

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

namespace {

const int num_events = static_cast<int>(DebugTrace::Event::_num_events);

#if !USE_PTHREADS_IMPL

REALM_THREAD_LOCAL DebugTrace::Callback installed_event_callbacks[num_events];

DebugTrace::Callback* get_callback_vector() noexcept
{
    return installed_event_callbacks;
}

#else // USE_PTHREADS_IMPL

pthread_key_t key;
pthread_once_t key_once = PTHREAD_ONCE_INIT;

void destroy(void* ptr) noexcept
{
    auto installed_event_callbacks = static_cast<DebugTrace::Callback*>(ptr);
    delete[] installed_event_callbacks;
}

void create() noexcept
{
    int ret = pthread_key_create(&key, &destroy);
    if (REALM_UNLIKELY(ret != 0)) {
        std::error_code ec = util::make_basic_system_error_code(errno);
        throw std::system_error(ec); // Termination intended
    }
}

DebugTrace::Callback* get_callback_vector() noexcept
{
    pthread_once(&key_once, &create);
    void* ptr = pthread_getspecific(key);
    auto callbacks = static_cast<DebugTrace::Callback*>(ptr);
    if (callbacks == nullptr) {
        callbacks = new DebugTrace::Callback[num_events]; // Throws with intended termination
        std::fill(callbacks, callbacks + num_events, Callback{nullptr, nullptr});
        int ret = pthread_setspecific(key, callbacks);
        if (REALM_UNLIKELY(ret != 0)) {
            std::error_code ec = util::make_basic_system_error_code(errno);
            throw std::system_error(ec); // Termination intended
        }
    }
    return callbacks;
}

#endif // USE_PTHREADS_IMPL

} // unnamed namespace


DebugTrace::Callback DebugTrace::install(Event event, Callback callback) noexcept
{
    auto callbacks = get_callback_vector();
    size_t event_ndx = static_cast<size_t>(event);
    Callback original = callbacks[event_ndx];
    callbacks[event_ndx] = callback;
    return original;
}

void DebugTrace::do_trace(Event event)
{
    auto callbacks = get_callback_vector();
    size_t event_ndx = static_cast<size_t>(event);
    Callback& callback = callbacks[event_ndx];
    if (callback) {
        callback(event);
    }
}


