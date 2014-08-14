#include <cerrno>
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

void RobustMutex::mark_as_consistent() TIGHTDB_NOEXCEPT
{
#ifdef TIGHTDB_HAVE_ROBUST_PTHREAD_MUTEX
    int r = pthread_mutex_consistent(&m_impl);
    TIGHTDB_ASSERT(r == 0);
    static_cast<void>(r);
#endif
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
