/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#ifndef REALM_UTIL_INTERPROCESS_MUTEX
#define REALM_UTIL_INTERPROCESS_MUTEX

#include <realm/util/features.h>
#include <realm/util/thread.hpp>
#include <realm/util/file.hpp>
#include <realm/utilities.hpp>
#include <mutex>
#include <map>
#include <thread>

#if REALM_PLATFORM_APPLE
#include <dispatch/dispatch.h>
#endif

// Enable this only on platforms where it might be needed
#if REALM_PLATFORM_APPLE || REALM_ANDROID
#define REALM_ROBUST_MUTEX_EMULATION 1
#else
#define REALM_ROBUST_MUTEX_EMULATION 0
#endif

namespace realm::util {

// fwd decl to support friend decl below
class InterprocessCondVar;

// A wrapper for a semaphore to expose a mutex interface. Unlike a real mutex,
// this can be locked and unlocked from different threads. Currently only
// implemented on Apple platforms
class SemaphoreMutex {
public:
    SemaphoreMutex() noexcept;
    ~SemaphoreMutex() noexcept;

    SemaphoreMutex(const SemaphoreMutex&) = delete;
    SemaphoreMutex& operator=(const SemaphoreMutex&) = delete;

    void lock() noexcept;
    void unlock() noexcept;
    bool try_lock() noexcept;

private:
#if REALM_PLATFORM_APPLE
    dispatch_semaphore_t m_semaphore;
#endif
};


/// Emulation of a Robust Mutex.
/// A Robust Mutex is an interprocess mutex which will automatically
/// release any locks held by a process when it crashes. Contrary to
/// Posix robust mutexes, this robust mutex is not capable of informing
/// participants that they have been granted a lock after a crash of
/// the process holding it (though it could be added if needed).

class InterprocessMutex {
public:
    InterprocessMutex() = default;
    ~InterprocessMutex() noexcept;

    // Disable copying. Copying a locked Mutex will create a scenario
    // where the same file descriptor will be locked once but unlocked twice.
    InterprocessMutex(const InterprocessMutex&) = delete;
    InterprocessMutex& operator=(const InterprocessMutex&) = delete;

#if REALM_ROBUST_MUTEX_EMULATION || defined(_WIN32)
    struct SharedPart {};
#else
    using SharedPart = RobustMutex;
#endif

    /// You need to bind the emulation to a SharedPart in shared/mmapped memory.
    /// The SharedPart is assumed to have been initialized (possibly by another process)
    /// elsewhere.
    void set_shared_part(SharedPart& shared_part, const std::string& path, const std::string& mutex_name);

    /// Destroy shared object. Potentially release system resources. Caller must
    /// ensure that the shared_part is not in use at the point of call.
    void release_shared_part();

    /// Lock the mutex. If the mutex is already locked, wait for it to be unlocked.
    void lock();

    /// Non-blocking attempt to lock the mutex. Returns true if the lock is obtained.
    /// If the lock can not be obtained return false immediately.
    bool try_lock();

    /// Unlock the mutex
    void unlock();

    /// Attempt to check if the mutex is valid (only relevant if not emulating)
    bool is_valid() noexcept;

#if REALM_ROBUST_MUTEX_EMULATION
    constexpr static bool is_robust_on_this_platform = true; // we're faking it!
#else
    constexpr static bool is_robust_on_this_platform = RobustMutex::is_robust_on_this_platform;
#endif

#if REALM_PLATFORM_APPLE
    // On Apple platforms we support locking and unlocking InterprocessMutex on
    // different threads, while on other platforms the locking thread owns the
    // mutex. The non-thread-confined version should be implementable on more
    // platforms if desired.
    constexpr static bool is_thread_confined = false;
#else
    constexpr static bool is_thread_confined = true;
#endif

private:
#if REALM_ROBUST_MUTEX_EMULATION
    File m_file;
#if REALM_PLATFORM_APPLE
    SemaphoreMutex m_local_mutex;
#else
    Mutex m_local_mutex;
#endif

#else
    SharedPart* m_shared_part = nullptr;

#ifdef _WIN32
    HANDLE m_handle = 0;
#endif

#endif
    friend class InterprocessCondVar;
};

inline InterprocessMutex::~InterprocessMutex() noexcept
{
#ifdef _WIN32
    if (m_handle) {
        bool b = CloseHandle(m_handle);
        REALM_ASSERT_RELEASE(b);
    }
#endif
}

inline void InterprocessMutex::set_shared_part(SharedPart& shared_part, const std::string& path,
                                               const std::string& mutex_name)
{
#if REALM_ROBUST_MUTEX_EMULATION
    static_cast<void>(shared_part);

    std::string filename;
    if (path.size() == 0) {
        filename = make_temp_file(mutex_name.c_str());
    }
    else {
        filename = util::format("%1.%2.mx", path, mutex_name);
    }

    // Always open file for write and retreive the uid in case other process
    // deletes the file. Avoid using just mode_Write (which implies truncate).
    // On fat32/exfat uid could be reused by OS in a situation when
    // multiple processes open and truncate the same lock file concurrently.
    m_file.close();
    m_file.open(filename, File::mode_Append);
    // exFAT does not allocate a unique id for the file until it's non-empty
    m_file.resize(1);

#elif defined(_WIN32)
    if (m_handle) {
        bool b = CloseHandle(m_handle);
        REALM_ASSERT_RELEASE(b);
    }
    // replace backslashes because they're significant in object namespace names
    std::string path_escaped = path;
    std::replace(path_escaped.begin(), path_escaped.end(), '\\', '/');
    std::string name = "Local\\realm_named_intermutex_" + path_escaped + mutex_name;

    std::wstring wname(name.begin(), name.end());
    m_handle = CreateMutexW(0, false, wname.c_str());
    if (!m_handle) {
        throw std::system_error(GetLastError(), std::system_category(), "Error opening mutex");
    }
#else
    m_shared_part = &shared_part;
    static_cast<void>(path);
    static_cast<void>(mutex_name);
#endif
}

inline void InterprocessMutex::release_shared_part()
{
#if REALM_ROBUST_MUTEX_EMULATION
    if (m_file.is_attached()) {
        m_file.close();
        File::try_remove(m_file.get_path());
    }
#else
    m_shared_part = nullptr;
#endif
}

inline void InterprocessMutex::lock()
{
#if REALM_ROBUST_MUTEX_EMULATION
    std::unique_lock mutex_lock(m_local_mutex);
    m_file.lock();
    mutex_lock.release();
#else

#ifdef _WIN32
    DWORD d = WaitForSingleObject(m_handle, INFINITE);
    REALM_ASSERT_RELEASE(d != WAIT_FAILED);
#else
    REALM_ASSERT(m_shared_part);
    m_shared_part->lock([]() {});
#endif
#endif
}

inline bool InterprocessMutex::try_lock()
{
#if REALM_ROBUST_MUTEX_EMULATION
    std::unique_lock mutex_lock(m_local_mutex, std::try_to_lock_t());
    if (!mutex_lock.owns_lock()) {
        return false;
    }
    if (!m_file.try_lock()) {
        return false;
    }
    mutex_lock.release();
    return true;
#elif defined(_WIN32)
    DWORD ret = WaitForSingleObject(m_handle, 0);
    REALM_ASSERT_RELEASE(ret != WAIT_FAILED);

    if (ret == WAIT_OBJECT_0) {
        return true;
    }
    else {
        return false;
    }
#else
    REALM_ASSERT(m_shared_part);
    return m_shared_part->try_lock([]() {});
#endif
}


inline void InterprocessMutex::unlock()
{
#if REALM_ROBUST_MUTEX_EMULATION
    m_file.unlock();
    m_local_mutex.unlock();
#else
#ifdef _WIN32
    bool b = ReleaseMutex(m_handle);
    REALM_ASSERT_RELEASE(b);
#else
    REALM_ASSERT(m_shared_part);
    m_shared_part->unlock();
#endif
#endif
}


inline bool InterprocessMutex::is_valid() noexcept
{
#if REALM_ROBUST_MUTEX_EMULATION
    return true;
#elif defined(_WIN32)
    // There is no safe way of testing if the m_handle mutex handle is valid on Windows, without having bad side
    // effects for the cases where it is indeed invalid. If m_handle contains an arbitrary value, it might by
    // coincidence be equal to a real live handle of another kind. This excludes a try_lock implementation and many
    // other ideas.
    return true;
#else
    REALM_ASSERT(m_shared_part);
    return m_shared_part->is_valid();
#endif
}


} // namespace realm::util

#endif // #ifndef REALM_UTIL_INTERPROCESS_MUTEX
