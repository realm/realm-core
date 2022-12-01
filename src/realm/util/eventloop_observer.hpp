#ifndef REALM_UTIL_EVENTLOOP_OBSERVER
#define REALM_UTIL_EVENTLOOP_OBSERVER

#include <exception>

namespace realm::util {

struct EventLoopObserver {
    // This method is called immediately after the event loop (thread) has been started.
    virtual void did_create_thread() = 0;

    // This method is called after stop() has been called, all events have been processed,
    // and the event loop is being stopped (or the event loop thread is exiting).
    virtual void will_destroy_thread() = 0;

    // This method is called with any exception thrown during processing of the event
    // loop. This will occur before will_destroy_thread() is called.
    virtual void handle_error(std::exception const& e) = 0;

    virtual ~EventLoopObserver() = default;
};

} // namespace realm::util

#endif // REALM_UTIL_EVENTLOOP_OBSERVER