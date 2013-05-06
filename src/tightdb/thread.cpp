#include <cerrno>

#include <tightdb/exceptions.hpp>
#include <tightdb/thread.hpp>

using namespace std;
using namespace tightdb;


TIGHTDB_NORETURN void Thread::create_failed(int err)
{
    switch (err) {
        case EAGAIN: throw ResourceAllocError("pthread_create() failed");
        default:     throw runtime_error("pthread_create() failed");
    }
}

TIGHTDB_NORETURN void Thread::join_failed(int err)
{
    switch (err) {
        default: throw runtime_error("pthread_join() failed");
    }
}

TIGHTDB_NORETURN void Mutex::init_failed(int err)
{
    switch (err) {
        case ENOMEM: throw bad_alloc();
        case EAGAIN: throw ResourceAllocError("pthread_mutex_init() failed");
        default:     throw runtime_error("pthread_mutex_init() failed");
    }
}

TIGHTDB_NORETURN void Mutex::attr_init_failed(int err)
{
    switch (err) {
        case ENOMEM: throw bad_alloc();
        default:     throw runtime_error("pthread_mutexattr_init() failed");
    }
}

TIGHTDB_NORETURN void Mutex::destroy_failed(int err) TIGHTDB_NOEXCEPT
{
    if (err == EBUSY) TIGHTDB_TERMINATE("Destruction of mutex in use");
    else TIGHTDB_TERMINATE("pthread_mutex_destroy() failed");
}


TIGHTDB_NORETURN void Mutex::lock_failed(int err) TIGHTDB_NOEXCEPT
{
    if (err == EDEADLK) TIGHTDB_TERMINATE("Recursive locking of mutex");
    else TIGHTDB_TERMINATE("pthread_mutex_lock() failed");
}

TIGHTDB_NORETURN void CondVar::init_failed(int err)
{
    switch (err) {
        case ENOMEM: throw bad_alloc();
        case EAGAIN: throw ResourceAllocError("pthread_cond_init() failed");
        default:     throw runtime_error("pthread_cond_init() failed");
    }
}

TIGHTDB_NORETURN void CondVar::attr_init_failed(int err)
{
    switch (err) {
        case ENOMEM: throw bad_alloc();
        default:     throw runtime_error("pthread_condattr_init() failed");
    }
}

TIGHTDB_NORETURN void CondVar::destroy_failed(int err) TIGHTDB_NOEXCEPT
{
    if (err == EBUSY) TIGHTDB_TERMINATE("Destruction of condition variable in use");
    else TIGHTDB_TERMINATE("pthread_cond_destroy() failed");
}
