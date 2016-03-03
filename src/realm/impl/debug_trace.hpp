/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2015] Realm Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Realm Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Realm Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Realm Incorporated.
 *
 **************************************************************************/

#ifndef REALM_IMPL_DEBUG_TRACE_HPP
#define REALM_IMPL_DEBUG_TRACE_HPP

#include <realm/util/features.h>
#include <realm/util/assert.hpp>

#include <functional>

namespace realm {
namespace _impl {

class DebugTrace {
public:
    enum class Event {
        slab_alloc__reset_free_space_tracking,
        slab_alloc__remap,
        shared_group__grow_reader_mapping,
        _num_events
    };

    struct Callback;
    template <class>
    class InstallGuard;

    // Install a callback to be triggered when the gived event is occuring.
    //
    // If a callback was previous installed for the given event, that callback
    // is replaced by the incoming callback, and the old callback is returned. The
    // new callback is allowed to invoke the old callback as part of its own invocation.
    //
    // If a callback wishes to unregister itself, it is allowed to reinstall the old
    // callback in its place. Care must be taken to ensure that a callback only
    // uninstalls itself as part of this process.
    //
    // \sa install_guard(), InstallGuard
    static Callback install(Event, Callback) noexcept;

    // Indicate that the given event is occuring.
    //
    // This has no effect unless compiled with REALM_DEBUG enabled.
    static void trace(Event);

    // Install a callback and return an instance of InstallGuard, which automatically
    // unregisters the given callback when it goes out of scope. The previous callback
    // is stored inside of InstallGuard, and will be reinstalled when the returned
    // instance goes out of scope.
    //
    // F is expected to be a callable with the signature `void(const Callback& previous)`,
    // where `previous` is the callback that was previously installed for this event.
    template <class F>
    static InstallGuard<F> install_guard(Event, F&&);
private:
    static Callback do_install(Event, Callback) noexcept;
    static void do_trace(Event);
};

struct DebugTrace::Callback {
    void(*m_function)(void*);
    void* m_userdata;
    
    explicit operator bool() const
    {
        return m_userdata != nullptr;
    }

    void operator()() const
    {
        m_function(m_userdata);
    }
};

template <class F>
class DebugTrace::InstallGuard {
public:
    InstallGuard(Event event, F f):
        m_callback(std::move(f)),
        m_event(event)
    {
        m_next = install(m_event, Callback{trigger, this});
    }
    ~InstallGuard() {
        auto tmp = install(m_event, std::move(m_next));
        static_cast<void>(tmp);
        REALM_ASSERT(tmp.m_userdata == this); // Inconsistency detected
    }
private:
    F m_callback;
    const Callback m_next;
    const Event m_event;

    static void trigger(void* userdata)
    {
        auto self = static_cast<InstallGuard<F>*>(userdata);
        self->m_callback(self->m_next);
    }
};

template <class F>
DebugTrace::InstallGuard<F> DebugTrace::install_guard(Event event, F&& f)
{
    return InstallGuard<F>{event, std::forward<F>(f)};
}

/// Implementation:

inline
void DebugTrace::trace(DebugTrace::Event event)
{
#if REALM_DEBUG
    do_trace(event);
#else
    static_cast<void>(event);
#endif
}

} // namespace _impl
} // namespace realm

#endif // REALM_IMPL_DEBUG_TRACE_HPP

