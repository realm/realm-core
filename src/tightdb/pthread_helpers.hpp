/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/
#ifndef TIGHTDB_PTHREAD_HELPERS_HPP
#define TIGHTDB_PTHREAD_HELPERS_HPP

#include <stdexcept>

#include <pthread.h>

#include <tightdb/assert.hpp>
#include <tightdb/terminate.hpp>

namespace tightdb {


class Mutex {
public:
    void init()
    {
        const int r = pthread_mutex_init(&m_impl, 0);
        if (TIGHTDB_UNLIKELY(r != 0)) init_failed(r);
    }

    /// Initialize mutex for use across multiple processes.
    void init_shared()
    {
#ifdef _POSIX_THREAD_PROCESS_SHARED
        pthread_mutexattr_t attr;
        int r = pthread_mutexattr_init(&attr);
        if (TIGHTDB_UNLIKELY(r != 0)) attr_init_failed(r);
        r = pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
        TIGHTDB_ASSERT(r == 0);
        // FIXME: Should also do pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST) when available. Check for availability with: #if _POSIX_THREADS >= 200809L
        r = pthread_mutex_init(&m_impl, &attr);
        const int r2 = pthread_mutexattr_destroy(&attr);
        TIGHTDB_ASSERT(r2 == 0);
        static_cast<void>(r2);
        if (TIGHTDB_UNLIKELY(r != 0)) init_failed(r);
#else // !_POSIX_THREAD_PROCESS_SHARED
        throw std::runtime_error("No support for shared mutexes");
#endif
    }

    void destroy() TIGHTDB_NOEXCEPT
    {
        int r = pthread_mutex_destroy(&m_impl);
        if (TIGHTDB_UNLIKELY(r != 0)) destroy_failed(r);
    }

    class Lock;
    class DestroyGuard;

private:
    pthread_mutex_t m_impl;

    static TIGHTDB_NORETURN void init_failed(int);
    static TIGHTDB_NORETURN void attr_init_failed(int);
    static TIGHTDB_NORETURN void destroy_failed(int) TIGHTDB_NOEXCEPT;
    static TIGHTDB_NORETURN void lock_failed(int) TIGHTDB_NOEXCEPT;

    friend class Condition;
};


class Mutex::Lock {
public:
    Lock(Mutex& m) TIGHTDB_NOEXCEPT: m_mutex(m)
    {
        int r = pthread_mutex_lock(&m_mutex.m_impl);
        if (TIGHTDB_UNLIKELY(r != 0)) Mutex::lock_failed(r);
    }

    ~Lock() TIGHTDB_NOEXCEPT
    {
        int r = pthread_mutex_unlock(&m_mutex.m_impl);
        TIGHTDB_ASSERT(r == 0);
        static_cast<void>(r);
    }

private:
    Mutex& m_mutex;

    friend class Condition;
};


class Condition {
public:
    void wait(Mutex::Lock& l) TIGHTDB_NOEXCEPT
    {
        int r = pthread_cond_wait(&m_impl, &l.m_mutex.m_impl);
        if (TIGHTDB_UNLIKELY(r != 0)) TIGHTDB_TERMINATE("pthread_cond_wait() failed");
    }

    void notify_all() TIGHTDB_NOEXCEPT
    {
        int r = pthread_cond_broadcast(&m_impl);
        TIGHTDB_ASSERT(r == 0);
        static_cast<void>(r);
    }

    void init()
    {
        int r = pthread_cond_init(&m_impl, 0);
        if (TIGHTDB_UNLIKELY(r != 0)) init_failed(r);
    }

    /// Initialize condition for use across multiple processes.
    void init_shared()
    {
#ifdef _POSIX_THREAD_PROCESS_SHARED
        pthread_condattr_t attr;
        int r = pthread_condattr_init(&attr);
        if (TIGHTDB_UNLIKELY(r != 0)) attr_init_failed(r);
        r = pthread_condattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
        TIGHTDB_ASSERT(r == 0);
        r = pthread_cond_init(&m_impl, &attr);
        int r2 = pthread_condattr_destroy(&attr);
        TIGHTDB_ASSERT(r2 == 0);
        static_cast<void>(r2);
        if (TIGHTDB_UNLIKELY(r != 0)) init_failed(r);
#else // !_POSIX_THREAD_PROCESS_SHARED
        throw std::runtime_error("No support for shared conditions");
#endif
    }

    void destroy() TIGHTDB_NOEXCEPT
    {
        int r = pthread_cond_destroy(&m_impl);
        if (TIGHTDB_UNLIKELY(r != 0)) destroy_failed(r);
    }

    class DestroyGuard;

private:
    pthread_cond_t m_impl;

    static TIGHTDB_NORETURN void init_failed(int);
    static TIGHTDB_NORETURN void attr_init_failed(int);
    static TIGHTDB_NORETURN void destroy_failed(int) TIGHTDB_NOEXCEPT;
};


class Mutex::DestroyGuard {
public:
    DestroyGuard(Mutex& m) TIGHTDB_NOEXCEPT: m_mutex(&m) {}
    ~DestroyGuard() TIGHTDB_NOEXCEPT { if (m_mutex) m_mutex->destroy(); }

    void release() TIGHTDB_NOEXCEPT { m_mutex = 0; }

private:
    Mutex* m_mutex;
};


class Condition::DestroyGuard {
public:
    DestroyGuard(Condition& c) TIGHTDB_NOEXCEPT: m_cond(&c) {}
    ~DestroyGuard() TIGHTDB_NOEXCEPT { if (m_cond) m_cond->destroy(); }

    void release() TIGHTDB_NOEXCEPT { m_cond = 0; }

private:
    Condition* m_cond;
};


} // namespace tightdb

#endif // TIGHTDB_PTHREAD_HELPERS_HPP
