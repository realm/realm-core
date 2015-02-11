#include <stdexcept>

#include <tightdb/util/thread.hpp>

#if !defined _WIN32
#  include <unistd.h>
#endif

// "Process shared mutexes" are not officially supported on Android,
// but they appear to work anyway.
#if _POSIX_THREAD_PROCESS_SHARED > 0 || TIGHTDB_ANDROID
#  define TIGHTDB_HAVE_PTHREAD_PROCESS_SHARED
#endif

// Unfortunately Older Ubuntu releases such as 10.04 reports support
// for robust mutexes by setting _POSIX_THREADS = 200809L and
// _POSIX_THREAD_PROCESS_SHARED = 200809L even though they do not
// provide pthread_mutex_consistent(). See also
// http://www.gnu.org/software/gnulib/manual/gnulib.html#pthread_005fmutex_005fconsistent.
// Support was added to glibc 2.12, so we disable for earlier versions
// of glibs
#ifdef TIGHTDB_HAVE_PTHREAD_PROCESS_SHARED
#  if !defined _WIN32 // 'robust' not supported by our windows pthreads port
#    if _POSIX_THREADS >= 200809L
#      ifdef __GNU_LIBRARY__
#        if __GLIBC__ >= 2  && __GLIBC_MINOR__ >= 12
#          define TIGHTDB_HAVE_ROBUST_PTHREAD_MUTEX
#        endif
#      else
#        define TIGHTDB_HAVE_ROBUST_PTHREAD_MUTEX
#      endif
#    endif
#  endif
#endif


using namespace std;
using namespace tightdb;
using namespace tightdb::util;


namespace {

// Valgrind can show still-reachable leaks for pthread_create() on many systems (AIX, Debian, etc) because
// glibc declares a static memory pool for threads which are free'd by the OS on process termination. See
// http://www.network-theory.co.uk/docs/valgrind/valgrind_20.html under --run-libc-freeres=<yes|no>.
// This can give false positives because of missing suppression, etc (not real leaks!). It's also a problem
// on Windows, so we have written our own clean-up method for the Windows port.
#if defined _WIN32 && defined TIGHTDB_DEBUG
void free_threadpool();

class Initialization
{
public:
    ~Initialization()
    {
        free_threadpool();
    }
};

Initialization initialization;

void free_threadpool()
{
    pthread_cleanup();
}
#endif

} // anonymous namespace


void Thread::join()
{
    if (!m_joinable)
        throw runtime_error("Thread is not joinable");
    void** value_ptr = 0; // Ignore return value
    int r = pthread_join(m_id, value_ptr);
    if (TIGHTDB_UNLIKELY(r != 0))
        join_failed(r); // Throws
    m_joinable = false;
}

TIGHTDB_NORETURN void Thread::create_failed(int)
{
    throw runtime_error("pthread_create() failed");
}

TIGHTDB_NORETURN void Thread::join_failed(int)
{
    // It is intentional that the argument is ignored here.
    throw runtime_error("pthread_join() failed.");
}

void Mutex::init_as_process_shared(bool robust_if_available)
{
#ifdef TIGHTDB_HAVE_PTHREAD_PROCESS_SHARED
    pthread_mutexattr_t attr;
    int r = pthread_mutexattr_init(&attr);
    if (TIGHTDB_UNLIKELY(r != 0))
        attr_init_failed(r);
    r = pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
    TIGHTDB_ASSERT(r == 0);
#  ifdef TIGHTDB_HAVE_ROBUST_PTHREAD_MUTEX
    if (robust_if_available) {
        r = pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
        TIGHTDB_ASSERT(r == 0);
    }
#  else // !TIGHTDB_HAVE_ROBUST_PTHREAD_MUTEX
    static_cast<void>(robust_if_available);
#  endif
    r = pthread_mutex_init(&m_impl, &attr);
    int r2 = pthread_mutexattr_destroy(&attr);
    TIGHTDB_ASSERT(r2 == 0);
    static_cast<void>(r2);
    if (TIGHTDB_UNLIKELY(r != 0))
        init_failed(r);
#else // !TIGHTDB_HAVE_PTHREAD_PROCESS_SHARED
    static_cast<void>(robust_if_available);
    throw runtime_error("No support for process-shared mutexes");
#endif
}

TIGHTDB_NORETURN void Mutex::init_failed(int err)
{
    switch (err) {
        case ENOMEM:
            throw bad_alloc();
        default:
            throw runtime_error("pthread_mutex_init() failed");
    }
}

TIGHTDB_NORETURN void Mutex::attr_init_failed(int err)
{
    switch (err) {
        case ENOMEM:
            throw bad_alloc();
        default:
            throw runtime_error("pthread_mutexattr_init() failed");
    }
}

TIGHTDB_NORETURN void Mutex::destroy_failed(int err) TIGHTDB_NOEXCEPT
{
    if (err == EBUSY)
        TIGHTDB_TERMINATE("Destruction of mutex in use");
    TIGHTDB_TERMINATE("pthread_mutex_destroy() failed");
}


TIGHTDB_NORETURN void Mutex::lock_failed(int err) TIGHTDB_NOEXCEPT
{
    if (err == EDEADLK)
        TIGHTDB_TERMINATE("Recursive locking of mutex");
    TIGHTDB_TERMINATE("pthread_mutex_lock() failed");
}


bool RobustMutex::is_robust_on_this_platform() TIGHTDB_NOEXCEPT
{
#ifdef TIGHTDB_HAVE_ROBUST_PTHREAD_MUTEX
    return true;
#else
    return false;
#endif
}

bool RobustMutex::low_level_lock()
{
    int r = pthread_mutex_lock(&m_impl);
    if (TIGHTDB_LIKELY(r == 0))
        return true;
#ifdef TIGHTDB_HAVE_ROBUST_PTHREAD_MUTEX
    if (r == EOWNERDEAD)
        return false;
    if (r == ENOTRECOVERABLE)
        throw NotRecoverable();
#endif
    lock_failed(r);
}

bool RobustMutex::is_valid() TIGHTDB_NOEXCEPT
{
    int r = pthread_mutex_trylock(&m_impl);
    if (r == 0) {
        r = pthread_mutex_unlock(&m_impl);
        TIGHTDB_ASSERT(r == 0);
        static_cast<void>(r);
        return true;
    }
    return r != EINVAL;
}


void RobustMutex::mark_as_consistent() TIGHTDB_NOEXCEPT
{
#ifdef TIGHTDB_HAVE_ROBUST_PTHREAD_MUTEX
    int r = pthread_mutex_consistent(&m_impl);
    TIGHTDB_ASSERT(r == 0);
    static_cast<void>(r);
#endif
}

string PlatformSpecificCondVar::internal_naming_prefix = "/RealmsBigFriendlySemaphore";

void PlatformSpecificCondVar::set_resource_naming_prefix(std::string prefix)
{
    internal_naming_prefix = prefix + "RLM";
}

PlatformSpecificCondVar::PlatformSpecificCondVar()
{
#ifdef TIGHTDB_HAVE_PTHREAD_PROCESS_SHARED
    m_shared_part = 0;
    m_sem = 0;
#else // !TIGHTDB_HAVE_PTHREAD_PROCESS_SHARED
    throw runtime_error("No support for process-shared condition variables");
#endif
}






void PlatformSpecificCondVar::close() TIGHTDB_NOEXCEPT
{
    if (m_sem) { // true if emulating a process shared condvar
        sem_close(m_sem);
        m_sem = 0;
        return; // we don't need to clean up the SharedPart
    }
    // we don't do anything to the shared part, other CondVars may shared it
    m_shared_part = 0;
}


PlatformSpecificCondVar::~PlatformSpecificCondVar() TIGHTDB_NOEXCEPT
{
    close();
}



void PlatformSpecificCondVar::set_shared_part(SharedPart& shared_part, dev_t device, ino_t inode, std::size_t offset_of_condvar)
{
    TIGHTDB_ASSERT(m_shared_part == 0);
    close();
    m_shared_part = &shared_part;
    static_cast<void>(device);
    static_cast<void>(inode);
    static_cast<void>(offset_of_condvar);
#ifdef TIGHTDB_CONDVAR_EMULATION
    m_sem = get_semaphore(device,inode,offset_of_condvar);
#endif
}

sem_t* PlatformSpecificCondVar::get_semaphore(dev_t device, ino_t inode, std::size_t offset)
{
    std::string name = internal_naming_prefix;
    uint64_t magic = device;
    magic += inode;
    magic += offset;
    name += 'A'+(magic % 23);
    magic /= 23;
    name += 'A'+(magic % 23);
    magic /= 23;
    name += 'A'+(magic % 23);
    magic /= 23;
    TIGHTDB_ASSERT(m_shared_part);
    if (m_sem == 0) {
        m_sem = sem_open(name.c_str(), O_CREAT, S_IRWXG | S_IRWXU, 0);
        // FIXME: error checking
    }
    return m_sem;
}


void PlatformSpecificCondVar::init_shared_part(SharedPart& shared_part) {
#ifdef TIGHTDB_CONDVAR_EMULATION
    shared_part.waiters = 0;
    shared_part.signal_counter = 0;
#else
    pthread_condattr_t attr;
    int r = pthread_condattr_init(&attr);
    if (TIGHTDB_UNLIKELY(r != 0))
        attr_init_failed(r);
    r = pthread_condattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
    TIGHTDB_ASSERT(r == 0);
    r = pthread_cond_init(&shared_part.m_impl, &attr);
    int r2 = pthread_condattr_destroy(&attr);
    TIGHTDB_ASSERT(r2 == 0);
    static_cast<void>(r2);
    if (TIGHTDB_UNLIKELY(r != 0))
        init_failed(r);
#endif // TIGHTDB_CONDVAR_EMULATION
}

TIGHTDB_NORETURN void PlatformSpecificCondVar::init_failed(int err)
{
    switch (err) {
        case ENOMEM:
            throw bad_alloc();
        default:
            throw runtime_error("pthread_cond_init() failed");
    }
}

void PlatformSpecificCondVar::handle_wait_error(int err)
{
#ifdef TIGHTDB_HAVE_ROBUST_PTHREAD_MUTEX
    if (err == ENOTRECOVERABLE)
        throw RobustMutex::NotRecoverable();
    if (err == EOWNERDEAD)
        return;
#else
    static_cast<void>(err);
#endif
    TIGHTDB_TERMINATE("pthread_mutex_lock() failed");
}

TIGHTDB_NORETURN void PlatformSpecificCondVar::attr_init_failed(int err)
{
    switch (err) {
        case ENOMEM:
            throw bad_alloc();
        default:
            throw runtime_error("pthread_condattr_init() failed");
    }
}

TIGHTDB_NORETURN void PlatformSpecificCondVar::destroy_failed(int err) TIGHTDB_NOEXCEPT
{
    if (err == EBUSY)
        TIGHTDB_TERMINATE("Destruction of condition variable in use");
    TIGHTDB_TERMINATE("pthread_cond_destroy() failed");
}


CondVar::CondVar(process_shared_tag)
{
#ifdef TIGHTDB_HAVE_PTHREAD_PROCESS_SHARED
    pthread_condattr_t attr;
    int r = pthread_condattr_init(&attr);
    if (TIGHTDB_UNLIKELY(r != 0))
        attr_init_failed(r);
    r = pthread_condattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
    TIGHTDB_ASSERT(r == 0);
    r = pthread_cond_init(&m_impl, &attr);
    int r2 = pthread_condattr_destroy(&attr);
    TIGHTDB_ASSERT(r2 == 0);
    static_cast<void>(r2);
    if (TIGHTDB_UNLIKELY(r != 0))
        init_failed(r);
#else // !TIGHTDB_HAVE_PTHREAD_PROCESS_SHARED
    throw runtime_error("No support for process-shared condition variables");
#endif
}

TIGHTDB_NORETURN void CondVar::init_failed(int err)
{
    switch (err) {
        case ENOMEM:
            throw bad_alloc();
        default:
            throw runtime_error("pthread_cond_init() failed");
    }
}

void CondVar::handle_wait_error(int err)
{
#ifdef TIGHTDB_HAVE_ROBUST_PTHREAD_MUTEX
    if (err == ENOTRECOVERABLE)
        throw RobustMutex::NotRecoverable();
    if (err == EOWNERDEAD)
        return;
#else
    static_cast<void>(err);
#endif
    TIGHTDB_TERMINATE("pthread_mutex_lock() failed");
}

TIGHTDB_NORETURN void CondVar::attr_init_failed(int err)
{
    switch (err) {
        case ENOMEM:
            throw bad_alloc();
        default:
            throw runtime_error("pthread_condattr_init() failed");
    }
}

TIGHTDB_NORETURN void CondVar::destroy_failed(int err) TIGHTDB_NOEXCEPT
{
    if (err == EBUSY)
        TIGHTDB_TERMINATE("Destruction of condition variable in use");
    TIGHTDB_TERMINATE("pthread_cond_destroy() failed");
}
