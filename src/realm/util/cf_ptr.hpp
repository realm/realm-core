/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2016] Realm Inc
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

#ifndef REALM_UTIL_CF_PTR_HPP
#define REALM_UTIL_CF_PTR_HPP

#include <realm/util/assert.hpp>

#if REALM_PLATFORM_APPLE

#include <CoreFoundation/CoreFoundation.h>

namespace realm {
namespace util {

template<class Ref> class ReleaseGuard {
public:
    explicit ReleaseGuard(Ref ref = nullptr) noexcept:
        m_ref(ref)
    {
    }

    ReleaseGuard(ReleaseGuard&& rg) noexcept:
        m_ref(rg.m_ref)
    {
        rg.m_ref = nullptr;
    }

    ~ReleaseGuard() noexcept
    {
        if (m_ref)
            CFRelease(m_ref);
    }

    ReleaseGuard &operator=(ReleaseGuard&& rg) noexcept
    {
        REALM_ASSERT(!m_ref || m_ref != rg.m_ref);
        if (m_ref)
            CFRelease(m_ref);
        m_ref = rg.m_ref;
        rg.m_ref = nullptr;
        return *this;
    }

    explicit operator bool() const noexcept
    {
        return bool(m_ref);
    }

    Ref get() const noexcept
    {
        return m_ref;
    }

    Ref release() noexcept
    {
        Ref ref = m_ref;
        m_ref = nullptr;
        return ref;
    }

    void reset(Ref ref = nullptr) noexcept
    {
        REALM_ASSERT(!m_ref || m_ref != ref);
        if (m_ref)
            CFRelease(m_ref);
        m_ref = ref;
    }

private:
    Ref m_ref;
};


}
}


#endif // REALM_PLATFORM_APPLE

#endif // REALM_UTIL_CF_PTR_HPP
