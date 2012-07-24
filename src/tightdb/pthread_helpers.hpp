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

#include <cerrno>
#include <pthread.h>

#include <tightdb/error.hpp>
#include <tightdb/terminate.hpp>

namespace tightdb {


struct Mutex {
    error_code init()
    {
        int r = pthread_mutex_init(&m_impl, 0);
        if (r != 0) {
            switch (r) {
            case EAGAIN: return ERROR_NO_RESOURCE;
            case ENOMEM: return ERROR_OUT_OF_MEMORY;
            case EPERM:  return ERROR_PERMISSION;
            default:     return ERROR_OTHER;
            }
        }
        return ERROR_NONE;
    }

    /// Initialize mutex for use across multiple processes.
    error_code init_shared()
    {
        pthread_mutexattr_t attr;
        int r = pthread_mutexattr_init(&attr);
        if (r != 0) {
            if (r == ENOMEM) return ERROR_OUT_OF_MEMORY;
            else return ERROR_OTHER;
        }
        r = pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
        TIGHTDB_ASSERT(r == 0);
        r = pthread_mutex_init(&m_impl, &attr);
        int r2 = pthread_mutexattr_destroy(&attr);
        TIGHTDB_ASSERT(r2 == 0);
        static_cast<void>(r2);
        if (r != 0) {
            switch (r) {
            case EAGAIN: return ERROR_NO_RESOURCE;
            case ENOMEM: return ERROR_OUT_OF_MEMORY;
            case EPERM:  return ERROR_PERMISSION;
            default:     return ERROR_OTHER;
            }
        }
        return ERROR_NONE;
    }

    void destroy()
    {
        int r = pthread_mutex_destroy(&m_impl);
        if (r != 0) {
            if (r == EBUSY) TIGHTDB_TERMINATE("Destruction of mutex in use");
            else TIGHTDB_TERMINATE("pthread_mutex_destroy() failed");
        }
    }

private:
    pthread_mutex_t m_impl;

    friend struct LockGuard;
    friend struct Condition;
};


struct LockGuard {
    LockGuard(Mutex& m): m_mutex(m)
    {
        int r = pthread_mutex_lock(&m_mutex.m_impl);
        if (r != 0) {
            if (r == EDEADLK) TIGHTDB_TERMINATE("Recursive locking of mutex");
            else TIGHTDB_TERMINATE("pthread_mutex_lock() failed");
        }
    }

    ~LockGuard()
    {
        int r = pthread_mutex_unlock(&m_mutex.m_impl);
        TIGHTDB_ASSERT(r == 0);
        static_cast<void>(r);
    }

private:
    Mutex& m_mutex;

    friend struct Condition;
};


struct Condition {
    void wait(LockGuard& g)
    {
        int r = pthread_cond_wait(&m_impl, &g.m_mutex.m_impl);
        if (r != 0) TIGHTDB_TERMINATE("pthread_cond_wait() failed");
    }

    void notify_all()
    {
        int r = pthread_cond_broadcast(&m_impl);
        TIGHTDB_ASSERT(r == 0);
        static_cast<void>(r);
    }

    /// \return Zero on success, otherwise an error as defined in
    /// <error.hpp>.
    error_code init()
    {
        int r = pthread_cond_init(&m_impl, 0);
        if (r != 0) {
            switch (r) {
            case EAGAIN: return ERROR_NO_RESOURCE;
            case ENOMEM: return ERROR_OUT_OF_MEMORY;
            default:     return ERROR_OTHER;
            }
        }
        return ERROR_NONE;
    }

    /// Initialize condition for use across multiple processes.
    ///
    /// \return Zero on success, otherwise an error as defined in
    /// <error.hpp>.
    error_code init_shared()
    {
        pthread_condattr_t attr;
        int r = pthread_condattr_init(&attr);
        if (r != 0) {
            if (r == ENOMEM) return ERROR_OUT_OF_MEMORY;
            else return ERROR_OTHER;
        }
        r = pthread_condattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
        TIGHTDB_ASSERT(r == 0);
        r = pthread_cond_init(&m_impl, &attr);
        int r2 = pthread_condattr_destroy(&attr);
        TIGHTDB_ASSERT(r2 == 0);
        static_cast<void>(r2);
        if (r != 0) {
            switch (r) {
            case EAGAIN: return ERROR_NO_RESOURCE;
            case ENOMEM: return ERROR_OUT_OF_MEMORY;
            default:     return ERROR_OTHER;
            }
        }
        return ERROR_NONE;
    }

    void destroy()
    {
        int r = pthread_cond_destroy(&m_impl);
        if (r != 0) {
            if (r == EBUSY) TIGHTDB_TERMINATE("Destruction of condition in use");
            else TIGHTDB_TERMINATE("pthread_cond_destroy() failed");
        }
    }

private:
    pthread_cond_t m_impl;
};


struct MutexDestroyGuard {
    MutexDestroyGuard(Mutex& m): m_mutex(&m) {}
    ~MutexDestroyGuard() { if (m_mutex) m_mutex->destroy(); }

    void release() { m_mutex = 0; }

private:
    Mutex* m_mutex;
};


struct ConditionDestroyGuard {
    ConditionDestroyGuard(Condition& c): m_cond(&c) {}
    ~ConditionDestroyGuard() { if (m_cond) m_cond->destroy(); }

    void release() { m_cond = 0; }

private:
    Condition* m_cond;
};


} // namespace tightdb

#endif // TIGHTDB_PTHREAD_HELPERS_HPP
