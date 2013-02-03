#include <cerrno>

#include <tightdb/exceptions.hpp>
#include <tightdb/pthread_helpers.hpp>

using namespace std;

namespace tightdb {

TIGHTDB_NORETURN void Mutex::init_failed(int r)
{
    switch (r) {
        case ENOMEM: throw bad_alloc();
        case EAGAIN: throw ResourceAllocError("pthread_mutex_init() failed");
        default:     throw runtime_error("pthread_mutex_init() failed");
    }
}

TIGHTDB_NORETURN void Mutex::attr_init_failed(int r)
{
    switch (r) {
        case ENOMEM: throw bad_alloc();
        default:     throw runtime_error("pthread_mutexattr_init() failed");
    }
}

TIGHTDB_NORETURN void Mutex::destroy_failed(int r) TIGHTDB_NOEXCEPT
{
    if (r == EBUSY) TIGHTDB_TERMINATE("Destruction of mutex in use");
    else TIGHTDB_TERMINATE("pthread_mutex_destroy() failed");
}


TIGHTDB_NORETURN void Mutex::lock_failed(int r) TIGHTDB_NOEXCEPT
{
    if (r == EDEADLK) TIGHTDB_TERMINATE("Recursive locking of mutex");
    else TIGHTDB_TERMINATE("pthread_mutex_lock() failed");
}

TIGHTDB_NORETURN void Condition::init_failed(int r)
{
    switch (r) {
        case ENOMEM: throw bad_alloc();
        case EAGAIN: throw ResourceAllocError("pthread_cond_init() failed");
        default:     throw runtime_error("pthread_cond_init() failed");
    }
}

TIGHTDB_NORETURN void Condition::attr_init_failed(int r)
{
    switch (r) {
        case ENOMEM: throw bad_alloc();
        default:     throw runtime_error("pthread_condattr_init() failed");
    }
}

TIGHTDB_NORETURN void Condition::destroy_failed(int r) TIGHTDB_NOEXCEPT
{
    if (r == EBUSY) TIGHTDB_TERMINATE("Destruction of condition in use");
    else TIGHTDB_TERMINATE("pthread_cond_destroy() failed");
}

} // namespace tightdb
