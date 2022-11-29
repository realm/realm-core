#ifndef REALM_UTIL_EVENTLOOP_OBSERVER
#define REALM_UTIL_EVENTLOOP_OBSERVER

#include <exception>

namespace realm::util {

struct EventLoopObserver {
    // This method is called just before the thread is started
    virtual void did_create_thread() = 0;

    // This method is called just before the thread is being destroyed
    virtual void will_destroy_thread() = 0;

    // This method is called with any exception thrown by client.run().
    virtual void handle_error(std::exception const& e) = 0;

    virtual ~EventLoopObserver() = default;
};

} // namespace realm::util

#endif // REALM_UTIL_EVENTLOOP_OBSERVER