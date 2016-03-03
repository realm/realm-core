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

#ifndef REALM_IMPL_SIMULATED_FAILURE_HPP
#define REALM_IMPL_SIMULATED_FAILURE_HPP

#include <exception>

#include <realm/impl/debug_trace.hpp>

namespace realm {
namespace _impl {

class SimulatedFailure: public std::exception {
public:
    using type = DebugTrace::Event;

    class PrimeGuard;

    // Prime the specified failure type on the calling thread.
    static void prime(type);

    // Unprime the specified failure type on the calling thread.
    static void unprime(type) noexcept;

private:
#ifdef REALM_DEBUG
    static void do_fail(void*);
#endif
};


class SimulatedFailure::PrimeGuard {
public:
    PrimeGuard(type failure_type):
        m_type(failure_type)
    {
        prime(m_type);
    }

    ~PrimeGuard() noexcept
    {
        unprime(m_type);
    }

private:
    const type m_type;
};





// Implementation

inline void SimulatedFailure::prime(type failure_type)
{
#ifdef REALM_DEBUG
    DebugTrace::install(failure_type, DebugTrace::Callback{&SimulatedFailure::do_fail, reinterpret_cast<void*>(failure_type)});
#else
    static_cast<void>(failure_type);
#endif
}

inline void SimulatedFailure::unprime(type failure_type) noexcept
{
#ifdef REALM_DEBUG
    DebugTrace::install(failure_type, DebugTrace::Callback{nullptr, nullptr});
#else
    static_cast<void>(failure_type);
#endif
}

} // namespace _impl
} // namespace realm

#endif // REALM_IMPL_SIMULATED_FAILURE_HPP
