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
#ifndef TIGHTDB_THREAD_HPP
#define TIGHTDB_THREAD_HPP

#include <stdexcept>

#include <pthread.h>

#include <tightdb/config.h>
#include <tightdb/assert.hpp>
#include <tightdb/terminate.hpp>
#include <tightdb/unique_ptr.hpp>

namespace tightdb {


/// A separate thread of execution.
///
/// This class is a C++03 compatible reproduction of a subset of
/// std::thread from C++11 (when discounting Thread::start()).
class Thread {
public:
    Thread();
    ~Thread();

    template<class F> explicit Thread(F func);

    /// This method is an extension to the API provided by
    /// std::thread. This method exists because proper move semantics
    /// is unavailable in C++03. If move semantics had been available,
    /// calling <tt>start(func)</tt> would have been equivalent to
    /// <tt>*this = Thread(func)</tt>. Please see
    /// std::thread::operator=() for details.
    template<class F> void start(F func);

    bool joinable() TIGHTDB_NOEXCEPT;

    void join();

private:
    pthread_t m_id;
    bool m_joinable;

    typedef void* (*entry_func_type)(void*);

    void start(entry_func_type, void* arg);

    template<class> static void* entry_point(void*) TIGHTDB_NOEXCEPT;

    TIGHTDB_NORETURN static void create_failed(int);
    TIGHTDB_NORETURN static void join_failed(int);
};


/// Low-level mutual exclusion device.
class Mutex {
public:
    Mutex();
    ~Mutex() TIGHTDB_NOEXCEPT;

    struct shared_tag {};

    /// Initialize this mutex for use across multiple processes. When
    /// constructed this way, the instance may be placed in memory
    /// shared by multimple processes, as well as in a memory mapped
    /// file. Such a mutex remains valid even after the constructing
    /// process terminates. Deleting the instance (freeing the memory
    /// or deleting the file) without first calling the destructor is
    /// legal and will not cause any system resources to be leaked.
    Mutex(shared_tag);

    class Lock;

private:
    pthread_mutex_t m_impl;

    TIGHTDB_NORETURN static void init_failed(int);
    TIGHTDB_NORETURN static void attr_init_failed(int);
    TIGHTDB_NORETURN static void destroy_failed(int) TIGHTDB_NOEXCEPT;
    TIGHTDB_NORETURN static void lock_failed(int) TIGHTDB_NOEXCEPT;

    friend class CondVar;
};


/// Scoped lock on a mutex.
class Mutex::Lock {
public:
    Lock(Mutex& m) TIGHTDB_NOEXCEPT;
    ~Lock() TIGHTDB_NOEXCEPT;

private:
    Mutex& m_mutex;

    friend class CondVar;
};


/// Condition variable for use in synchronization monitors.
class CondVar {
public:
    CondVar();
    ~CondVar() TIGHTDB_NOEXCEPT;

    struct shared_tag {};

    /// Initialize this condition variable for use across multiple
    /// processes. When constructed this way, the instance may be
    /// placed in memory shared by multimple processes, as well as in
    /// a memory mapped file. Such a condition variable remains valid
    /// even after the constructing process terminates. Deleting the
    /// instance (freeing the memory or deleting the file) without
    /// first calling the destructor is legal and will not cause any
    /// system resources to be leaked.
    CondVar(shared_tag);

    void wait(Mutex::Lock& l) TIGHTDB_NOEXCEPT;

    void notify_all() TIGHTDB_NOEXCEPT;

private:
    pthread_cond_t m_impl;

    TIGHTDB_NORETURN static void init_failed(int);
    TIGHTDB_NORETURN static void attr_init_failed(int);
    TIGHTDB_NORETURN static void destroy_failed(int) TIGHTDB_NOEXCEPT;
};





// Implementation:

inline Thread::Thread(): m_joinable(false) {}

template<class F> inline Thread::Thread(F func): m_joinable(true)
{
    UniquePtr<F> func2(new F(func)); // Throws
    start(&Thread::entry_point<F>, func2.get()); // Throws
    func2.release();
}

template<class F> inline void Thread::start(F func)
{
    if (m_joinable)
        std::terminate();
    UniquePtr<F> func2(new F(func)); // Throws
    start(&Thread::entry_point<F>, func2.get()); // Throws
    func2.release();
    m_joinable = true;
}

inline Thread::~Thread()
{
    if (m_joinable) {
        std::terminate();
    }
}

inline bool Thread::joinable() TIGHTDB_NOEXCEPT
{
    return m_joinable;
}

inline void Thread::join()
{
    if (!m_joinable) {
        throw std::runtime_error("Thread is not joinable");
    }
    void** value_ptr = 0; // Ignore return value
    int r = pthread_join(m_id, value_ptr);
    if (TIGHTDB_UNLIKELY(r != 0)) {
        join_failed(r); // Throws
    }
    m_joinable = false;
}

inline void Thread::start(entry_func_type entry_func, void* arg)
{
    const pthread_attr_t* attr = 0; // Use default thread attributes
    int r = pthread_create(&m_id, attr, entry_func, arg);
    if (TIGHTDB_UNLIKELY(r != 0)) {
        create_failed(r); // Throws
    }
}

template<class F> inline void* Thread::entry_point(void* cookie) TIGHTDB_NOEXCEPT
{
    UniquePtr<F> func(static_cast<F*>(cookie));
    try {
        (*func)();
    }
    catch (...) {
        std::terminate();
    }
    return 0;
}


inline Mutex::Mutex()
{
    int r = pthread_mutex_init(&m_impl, 0);
    if (TIGHTDB_UNLIKELY(r != 0)) init_failed(r);
}

inline Mutex::Mutex(shared_tag)
{
#ifdef _POSIX_THREAD_PROCESS_SHARED
    pthread_mutexattr_t attr;
    int r = pthread_mutexattr_init(&attr);
    if (TIGHTDB_UNLIKELY(r != 0)) attr_init_failed(r);
    r = pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
    TIGHTDB_ASSERT(r == 0);
    // FIXME: Should also do pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST) when available. Check for availability with: #if _POSIX_THREADS >= 200809L
    r = pthread_mutex_init(&m_impl, &attr);
    int r2 = pthread_mutexattr_destroy(&attr);
    TIGHTDB_ASSERT(r2 == 0);
    static_cast<void>(r2);
    if (TIGHTDB_UNLIKELY(r != 0)) init_failed(r);
#else // !_POSIX_THREAD_PROCESS_SHARED
    throw std::runtime_error("No support for shared mutexes");
#endif
}

inline Mutex::~Mutex() TIGHTDB_NOEXCEPT
{
    int r = pthread_mutex_destroy(&m_impl);
    if (TIGHTDB_UNLIKELY(r != 0)) destroy_failed(r);
}


inline Mutex::Lock::Lock(Mutex& m) TIGHTDB_NOEXCEPT: m_mutex(m)
{
    int r = pthread_mutex_lock(&m_mutex.m_impl);
    if (TIGHTDB_UNLIKELY(r != 0)) Mutex::lock_failed(r);
}

inline Mutex::Lock::~Lock() TIGHTDB_NOEXCEPT
{
    int r = pthread_mutex_unlock(&m_mutex.m_impl);
    TIGHTDB_ASSERT(r == 0);
    static_cast<void>(r);
}


inline CondVar::CondVar()
{
    int r = pthread_cond_init(&m_impl, 0);
    if (TIGHTDB_UNLIKELY(r != 0)) init_failed(r);
}

inline CondVar::CondVar(shared_tag)
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
    throw std::runtime_error("No support for shared condition variables");
#endif
}

inline CondVar::~CondVar() TIGHTDB_NOEXCEPT
{
    int r = pthread_cond_destroy(&m_impl);
    if (TIGHTDB_UNLIKELY(r != 0)) destroy_failed(r);
}

inline void CondVar::wait(Mutex::Lock& l) TIGHTDB_NOEXCEPT
{
    int r = pthread_cond_wait(&m_impl, &l.m_mutex.m_impl);
    if (TIGHTDB_UNLIKELY(r != 0)) TIGHTDB_TERMINATE("pthread_cond_wait() failed");
}

inline void CondVar::notify_all() TIGHTDB_NOEXCEPT
{
    int r = pthread_cond_broadcast(&m_impl);
    TIGHTDB_ASSERT(r == 0);
    static_cast<void>(r);
}


} // namespace tightdb

#endif // TIGHTDB_THREAD_HPP
