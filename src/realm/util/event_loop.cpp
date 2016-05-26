#include <realm/util/features.h>
#include <realm/util/event_loop.hpp>

using namespace realm;
using namespace realm::util;

namespace realm {
namespace _impl {

EventLoop::Implementation* get_posix_event_loop_impl();
EventLoop::Implementation* get_apple_cf_event_loop_impl();

} // namespace_impl


namespace util {

EventLoop::Implementation& EventLoop::Implementation::get_default()
{
    // On iOS, prefer Apple Core Foundation
#if REALM_IOS
    if (auto impl = _impl::get_apple_cf_event_loop_impl()) // Throws
        return *impl;
#endif

    // Everywhere else, prefer POSIX
    if (auto impl = _impl::get_posix_event_loop_impl()) // Throws
        return *impl;

    auto all = get_all(); // Throws
    if (!all.empty())
        return *all[0];

    throw NotAvailable();
}


EventLoop::Implementation& EventLoop::Implementation::get(const std::string& name)
{
    for (Implementation* impl: get_all()) { // Throws
        if (impl->name() == name) // Throws
            return *impl;
    }
    throw NotAvailable();
}


std::vector<EventLoop::Implementation*> EventLoop::Implementation::get_all()
{
    std::vector<EventLoop::Implementation*> list;
    if (auto impl = _impl::get_posix_event_loop_impl()) // Throws
        list.push_back(impl); // Throws
    if (auto impl = _impl::get_apple_cf_event_loop_impl()) // Throws
        list.push_back(impl); // Throws
    return list;
}


EventLoop::Implementation& EventLoop::Implementation::get_posix()
{
    if (auto impl = _impl::get_posix_event_loop_impl()) // Throws
        return *impl;
    throw NotAvailable();
}


EventLoop::Implementation& EventLoop::Implementation::get_apple_cf()
{
    if (auto impl = _impl::get_apple_cf_event_loop_impl()) // Throws
        return *impl;
    throw NotAvailable();
}

} // namespace util
} // namespace realm

