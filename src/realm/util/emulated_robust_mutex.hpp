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
class PlatformSpecificCondVar;


/// Emulation of a Robust Mutex.
/// A Robust Mutex is an interprocess mutex which will automatically
/// release any locks held by a process when it crashes. Contrary to
/// Posix robust mutexes, this robust mutex is not capable of informing
/// participants that they have been granted a lock after a crash of
/// the process holding it (though it could be added if needed).

class EmulatedRobustMutex {
public:
    EmulatedRobustMutex();
    ~EmulatedRobustMutex() noexcept;

#ifdef REALM_ROBUST_MUTEX_EMULATION
    struct SharedPart { };
#else
    using SharedPart = RobustMutex;
#endif

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

    static bool is_robust_on_this_platform() 
    { 
#ifdef REALM_ROBUST_MUTEX_EMULATION
        return true;  // we're faking it!
#else
        return RobustMutex::is_robust_on_this_platform();
#endif
    }
private:
#ifdef REALM_ROBUST_MUTEX_EMULATION
    std::string m_filename;
    File m_file;
    Mutex m_local_mutex;
#else
    SharedPart* m_shared_part = 0;
#endif
    friend class PlatformSpecificCondVar;
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
#ifdef REALM_ROBUST_MUTEX_EMULATION
    static_cast<void>(shared_part);
    if (m_file.is_attached()) {
        m_file.close();
    }
    m_filename = path + "." + mutex_name + ".mx";
    m_local_mutex.lock();
    m_file.open(m_filename, File::mode_Write);
    m_local_mutex.unlock();
#else
    m_shared_part = &shared_part;
    static_cast<void>(path);
    static_cast<void>(mutex_name);

#endif
}

inline void EmulatedRobustMutex::lock()
{
#ifdef REALM_ROBUST_MUTEX_EMULATION
    m_local_mutex.lock();
    m_file.lock_exclusive();
#else
    REALM_ASSERT(m_shared_part);
    m_shared_part->lock([](){});
#endif
}


inline void EmulatedRobustMutex::unlock()
{
#ifdef REALM_ROBUST_MUTEX_EMULATION
    m_file.unlock();
    m_local_mutex.unlock();
#else
    REALM_ASSERT(m_shared_part);
    m_shared_part->unlock();
#endif
}


inline bool EmulatedRobustMutex::is_valid() noexcept
{
#ifdef REALM_ROBUST_MUTEX_EMULATION
    return true;
#else
    REALM_ASSERT(m_shared_part);
    return m_shared_part->is_valid();
#endif
}


} // namespace util
} // namespace realm

#endif // #ifndef REALM_UTIL_EMULATED_ROBUST_MUTEX
