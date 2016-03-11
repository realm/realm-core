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
#ifndef REALM_UTIL_SCOPE_EXIT_HPP
#define REALM_UTIL_SCOPE_EXIT_HPP

namespace realm {
namespace util {

template<class H>
class ScopeExit {
public:
    explicit ScopeExit(const H& handler) noexcept(noexcept(H(handler))):
        m_handler(handler)
    {
    }

    ScopeExit(ScopeExit&& se) noexcept:
        m_handler(std::move(se.m_handler))
    {
        se.m_expired = true;
    }

    ~ScopeExit()
    {
        if (!m_expired)
            m_handler();
    }
private:
    H m_handler;
    bool m_expired = false;
    static_assert(noexcept(m_handler()), "Handler must not throw");
};

template<class H>
ScopeExit<H> make_scope_exit(const H& handler) noexcept(noexcept(ScopeExit<H>(handler)))
{
    return ScopeExit<H>(handler);
}

} // namespace util
} // namespace realm

#endif // REALM_UTIL_SCOPE_EXIT_HPP
