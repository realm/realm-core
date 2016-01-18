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

#ifndef REALM_UTIL_EMULATED_ROBUST_MUTEX
#define REALM_UTIL_EMULATED_ROBUST_MUTEX

// Enable this only on platforms where it might be needed
// currently APPLE.
#if REALM_PLATFORM_APPLE
#define REALM_ROBUST_MUTEX_EMULATION
#endif

#include <realm/util/features.h>
#include <realm/util/thread.hpp>
#include <realm/util/file.hpp>

namespace realm {
namespace util {

// fwd decl to support friend decl below
class PlatformSpecificCondvar;


/// Emulation of a Robust Mutex.
/// A Robust Mutex is an interprocess mutex which will automatically
/// release any locks held by a process when it crashes. Contrary to
/// Posix robust mutexes, this robust mutex is not capable of informing
/// participants that they have been granted a lock after a crash of
/// the process holding it.

class EmulatedRobustMutex {
public:
    EmulatedRobustMutex();
    ~EmulatedRobustMutex() noexcept;

#ifdef REALM_ROBUST_MUTEX_EMULATION
    struct SharedPart { };
#else
    using SharedPart = RobustMutex;
#endif

    class LockGuard {
    public:
        LockGuard(EmulatedRobustMutex& mutex) : m_mutex(mutex)
        {
            m_mutex.lock();
        }
        ~LockGuard()
        {
            m_mutex.unlock();
        }
    private:
        EmulatedRobustMutex& m_mutex;
    };


    /// You need to bind the emulation to a SharedPart in shared/mmapped memory.
    /// The SharedPart is assumed to have been initialized (possibly by another process)
    /// elsewhere.
    void set_shared_part(SharedPart& shared_part, std::string path, std::string mutex_name);

    /// Lock the mutex. If the mutex is already locked, wait for it to be unlocked.
    void lock();

    /// Unlock the mutex
    void unlock();

    /// Attempt to check if the mutex is valid (only relevant if not emulating)
    bool is_valid() noexcept;

//private:
#ifndef REALM_ROBUST_MUTEX_EMULATION
    SharedPart* m_shared_part = 0;
#else
    std::string m_filename;
    File m_file;
    Mutex m_local_mutex;
#endif
    friend class PlatformSpecificCondvar;
};


inline EmulatedRobustMutex::EmulatedRobustMutex()
{
}

inline EmulatedRobustMutex::~EmulatedRobustMutex() noexcept
{
#ifdef REALM_ROBUST_MUTEX_EMULATION
    m_local_mutex.lock();
    m_file.close();
    m_local_mutex.unlock();
#endif
}

inline void EmulatedRobustMutex::set_shared_part(SharedPart& shared_part,
                                                 std::string path,
                                                 std::string mutex_name)
{
#ifndef REALM_ROBUST_MUTEX_EMULATION
    m_shared_part = &shared_part;
    static_cast<void>(path);
    static_cast<void>(mutex_name);
#else
    static_cast<void>(shared_part);
    if (m_file.is_attached()) {
        m_file.close();
    }
    m_filename = path + "_" + mutex_name + ".lck";
    m_local_mutex.lock();
    m_file.open(m_filename, File::mode_Write);
    m_local_mutex.unlock();
#endif
}

inline void EmulatedRobustMutex::lock()
{
#ifndef REALM_ROBUST_MUTEX_EMULATION
    REALM_ASSERT(m_shared_part);
    m_shared_part->lock([](){});
#else
    m_local_mutex.lock();
    m_file.lock_exclusive();
#endif
}


inline void EmulatedRobustMutex::unlock()
{
#ifndef REALM_ROBUST_MUTEX_EMULATION
    REALM_ASSERT(m_shared_part);
    m_shared_part->unlock();
#else
    m_file.unlock();
    m_local_mutex.unlock();
#endif
}


inline bool EmulatedRobustMutex::is_valid() noexcept
{
#ifndef REALM_ROBUST_MUTEX_EMULATION
    REALM_ASSERT(m_shared_part);
    return m_shared_part->is_valid();
#else
    return true;
#endif
}


} // namespace util
} // namespace realm

#endif // #ifndef REALM_UTIL_EMULATED_ROBUST_MUTEX
