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
    ~Thread() TIGHTDB_NOEXCEPT;

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

    struct process_shared_tag {};

    /// Initialize this mutex for use across multiple processes. When
    /// constructed this way, the instance may be placed in memory
    /// shared by multiple processes, as well as in a memory mapped
    /// file. Such a mutex remains valid even after the constructing
    /// process terminates. Deleting the instance (freeing the memory
    /// or deleting the file) without first calling the destructor is
    /// legal and will not cause any system resources to be leaked.
    Mutex(process_shared_tag);

    class Lock;

protected:
    pthread_mutex_t m_impl;

    void init_as_regular();
    void init_as_process_shared(bool robust_if_available);

    void lock() TIGHTDB_NOEXCEPT;
    void unlock() TIGHTDB_NOEXCEPT;

    TIGHTDB_NORETURN static void init_failed(int);
    TIGHTDB_NORETURN static void attr_init_failed(int);
    TIGHTDB_NORETURN static void destroy_failed(int) TIGHTDB_NOEXCEPT;
    TIGHTDB_NORETURN static void lock_failed(int) TIGHTDB_NOEXCEPT;

    friend class CondVar;
};


/// A simple scoped lock on a mutex.
class Mutex::Lock {
public:
    Lock(Mutex& m) TIGHTDB_NOEXCEPT;
    ~Lock() TIGHTDB_NOEXCEPT;

private:
    Mutex& m_mutex;
    friend class CondVar;
};


/// A robust version of a process-shared mutex.
///
/// A robust mutex is one that detects whether a thread (or process)
/// has died while holding a lock on the mutex.
///
/// When the present platform does not offer support for robust
/// mutexes, this mutex class behaves as a regular process-shared
/// mutex, which means that if a thread dies while holding a lock, any
/// future attempt at locking will block indefinitely.
class RobustMutex: private Mutex {
public:
    RobustMutex();
    ~RobustMutex() TIGHTDB_NOEXCEPT;

    static bool is_robust_on_this_platform() TIGHTDB_NOEXCEPT;

    class NotRecoverable;

    /// \param recover_func If the present platform does not support
    /// robust mutexes, this function is never called. Otherwise it is
    /// called if, and only if a thread has died while holding a
    /// lock. The purpose of the function is to reestablish a
    /// consistent shared state. If it fails to do this by throwing an
    /// exception, the mutex enters the 'unrecoverable' state where
    /// any future attempt at locking it will fail and cause
    /// NotRecoverable to be thrown. This function is advised to throw
    /// NotRecoverable when it fails, but it may throw any exception.
    ///
    /// \throw NotRecoverable If thrown by the specified recover
    /// function, or if the mutex has entered the 'unrecoverable'
    /// state due to a different thread throwing from its recover
    /// function.
    template<class Func> void lock(Func recover_func);

    void unlock() TIGHTDB_NOEXCEPT;

    /// Low-level locking of robust mutex.
    ///
    /// If the present platform does not support robust mutexes, this
    /// function always returns true. Otherwise it returns true if,
    /// and only if a thread has died while holding a lock.
    ///
    /// \note Most application should never call this function
    /// directly. It is called automatically when using the ordinary
    /// lock() function.
    ///
    /// \throw NotRecoverable If this mutex has entered the "not
    /// recoverable" state. It enters this state if
    /// mark_as_consistent() is not called between a call to
    /// robust_lock() that returns false and the corresponding call to
    /// unlock().
    bool low_level_lock();

    /// Pull this mutex out of the 'inconsistent' state.
    ///
    /// Must be called only after robust_lock() has returned false.
    ///
    /// \note Most application should never call this function
    /// directly. It is called automatically when using the ordinary
    /// lock() function.
    void mark_as_consistent() TIGHTDB_NOEXCEPT;
};

class RobustMutex::NotRecoverable: std::exception {
public:
    const char* what() const TIGHTDB_NOEXCEPT_OR_NOTHROW TIGHTDB_OVERRIDE
    {
        return "Failed to recover consistent state of shared memory";
    }
};


/// Condition variable for use in synchronization monitors.
class CondVar {
public:
    CondVar();
    ~CondVar() TIGHTDB_NOEXCEPT;

    struct process_shared_tag {};

    /// Initialize this condition variable for use across multiple
    /// processes. When constructed this way, the instance may be
    /// placed in memory shared by multimple processes, as well as in
    /// a memory mapped file. Such a condition variable remains valid
    /// even after the constructing process terminates. Deleting the
    /// instance (freeing the memory or deleting the file) without
    /// first calling the destructor is legal and will not cause any
    /// system resources to be leaked.
    CondVar(process_shared_tag);

    void wait(Mutex::Lock& l) TIGHTDB_NOEXCEPT;

    void notify_all() TIGHTDB_NOEXCEPT;

private:
    pthread_cond_t m_impl;

    TIGHTDB_NORETURN static void init_failed(int);
    TIGHTDB_NORETURN static void attr_init_failed(int);
    TIGHTDB_NORETURN static void destroy_failed(int) TIGHTDB_NOEXCEPT;
};





// Implementation:

inline Thread::Thread(): m_joinable(false)
{
}

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

inline Thread::~Thread() TIGHTDB_NOEXCEPT
{
    if (m_joinable)
        std::terminate();
}

inline bool Thread::joinable() TIGHTDB_NOEXCEPT
{
    return m_joinable;
}

inline void Thread::start(entry_func_type entry_func, void* arg)
{
    const pthread_attr_t* attr = 0; // Use default thread attributes
    int r = pthread_create(&m_id, attr, entry_func, arg);
    if (TIGHTDB_UNLIKELY(r != 0))
        create_failed(r); // Throws
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
    init_as_regular();
}

inline Mutex::Mutex(process_shared_tag)
{
    bool robust_if_available = false;
    init_as_process_shared(robust_if_available);
}

inline Mutex::~Mutex() TIGHTDB_NOEXCEPT
{
    int r = pthread_mutex_destroy(&m_impl);
    if (TIGHTDB_UNLIKELY(r != 0))
        destroy_failed(r);
}

inline void Mutex::init_as_regular()
{
    int r = pthread_mutex_init(&m_impl, 0);
    if (TIGHTDB_UNLIKELY(r != 0))
        init_failed(r);
}

inline void Mutex::lock() TIGHTDB_NOEXCEPT
{
    int r = pthread_mutex_lock(&m_impl);
    if (TIGHTDB_LIKELY(r == 0))
        return;
    lock_failed(r);
}

inline void Mutex::unlock() TIGHTDB_NOEXCEPT
{
    int r = pthread_mutex_unlock(&m_impl);
    TIGHTDB_ASSERT(r == 0);
    static_cast<void>(r);
}


inline Mutex::Lock::Lock(Mutex& m) TIGHTDB_NOEXCEPT: m_mutex(m)
{
    m_mutex.lock();
}

inline Mutex::Lock::~Lock() TIGHTDB_NOEXCEPT
{
    m_mutex.unlock();
}


inline RobustMutex::RobustMutex()
{
    bool robust_if_available = true;
    init_as_process_shared(robust_if_available);
}

inline RobustMutex::~RobustMutex() TIGHTDB_NOEXCEPT
{
}

template<class Func> inline void RobustMutex::lock(Func recover_func)
{
    bool no_thread_has_died = low_level_lock(); // Throws
    if (TIGHTDB_LIKELY(no_thread_has_died))
        return;
    try {
        recover_func(); // Throws
        mark_as_consistent();
        // If we get this far, the protected memory has been
        // brought back into a consistent state, and the mutex has
        // been notified aboit this. This means that we can safely
        // enter the applications critical section.
    }
    catch (...) {
        // Unlocking without first calling mark_as_consistent()
        // means that the mutex enters the "not recoverable"
        // state, which will cause all future attempts at locking
        // to fail.
        unlock();
        throw;
    }
}

inline void RobustMutex::unlock() TIGHTDB_NOEXCEPT
{
    Mutex::unlock();
}


inline CondVar::CondVar()
{
    int r = pthread_cond_init(&m_impl, 0);
    if (TIGHTDB_UNLIKELY(r != 0))
        init_failed(r);
}

inline CondVar::~CondVar() TIGHTDB_NOEXCEPT
{
    int r = pthread_cond_destroy(&m_impl);
    if (TIGHTDB_UNLIKELY(r != 0))
        destroy_failed(r);
}

inline void CondVar::wait(Mutex::Lock& l) TIGHTDB_NOEXCEPT
{
    int r = pthread_cond_wait(&m_impl, &l.m_mutex.m_impl);
    if (TIGHTDB_UNLIKELY(r != 0))
        TIGHTDB_TERMINATE("pthread_cond_wait() failed");
}

inline void CondVar::notify_all() TIGHTDB_NOEXCEPT
{
    int r = pthread_cond_broadcast(&m_impl);
    TIGHTDB_ASSERT(r == 0);
    static_cast<void>(r);
}


} // namespace tightdb

#endif // TIGHTDB_THREAD_HPP
