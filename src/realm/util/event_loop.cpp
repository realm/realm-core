#include <realm/util/features.h>
#include <realm/util/event_loop.hpp>


#ifndef _WIN32
#  define HAVE_POSIX_IMPLEMENTATION 1
#endif

#if REALM_PLATFORM_APPLE
#  define HAVE_APPLE_CF_IMPLEMENTATION 1
#endif


using namespace realm;
using namespace realm::util;

namespace realm {
namespace _impl {

#if HAVE_POSIX_IMPLEMENTATION
EventLoop::Implementation& get_posix_event_loop_impl();
#endif

#if HAVE_APPLE_CF_IMPLEMENTATION
EventLoop::Implementation& get_apple_cf_event_loop_impl();
#endif

} // namespace_impl

namespace util {


EventLoop::Implementation* EventLoop::Implementation::get_default()
{
    // On iOS, prefer Apple Core Foundation
#if REALM_IOS
    if (auto impl = get_apple_cf()) // Throws
        return impl; // Throws
#endif

    // Everywhere else, prefer POSIX
    if (auto impl = get_posix()) // Throws
        return impl; // Throws

    auto all = get_all(); // Throws
    if (!all.empty())
        return all[0];

    return nullptr;
}


EventLoop::Implementation* EventLoop::Implementation::get(std::string name)
{
    for (Implementation* impl: get_all()) { // Throws
        if (impl->name() == name) // Throws
            return impl;
    }
    return nullptr;
}


std::vector<EventLoop::Implementation*> EventLoop::Implementation::get_all()
{
    std::vector<EventLoop::Implementation*> list;
    if (auto impl = get_posix()) // Throws
        list.push_back(impl); // Throws
    if (auto impl = get_apple_cf()) // Throws
        list.push_back(impl); // Throws
    return list;
}


EventLoop::Implementation* EventLoop::Implementation::get_posix()
{
#if HAVE_POSIX_IMPLEMENTATION
    return &_impl::get_posix_event_loop_impl(); // Throws
#else
    return nullptr;
#endif
}


EventLoop::Implementation* EventLoop::Implementation::get_apple_cf()
{
#if HAVE_APPLE_CF_IMPLEMENTATION
    return &_impl::get_apple_cf_event_loop_impl(); // Throws
#else
    return nullptr;
#endif
}


} // namespace util
} // namespace realm

