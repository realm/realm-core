#include <utility>
#include <ostream>
#include <iomanip>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/mman.h>

#include <tightdb/terminate.hpp>
#include <tightdb/safe_int_ops.hpp>
#include <tightdb/string_buffer.hpp>
#include <tightdb/pthread_helpers.hpp>
#include <tightdb/table.hpp>
#include <tightdb/group.hpp>
#include <tightdb/replication.hpp>

using namespace std;
using namespace tightdb;

namespace {


// Note: The sum of this value and sizeof(SharedState) must not
// exceed the maximum values of any of the types 'size_t',
// 'ptrdiff_t', or 'off_t'.
#ifdef TIGHTDB_DEBUG
const size_t initial_transact_log_buffer_size = 128;
#else
const size_t initial_transact_log_buffer_size = 16*1024;
#endif

const size_t init_subtab_path_buf_size = 2*8-1; // 8 table levels (soft limit)


struct CloseGuard {
    CloseGuard(int fd): m_fd(fd) {}
    ~CloseGuard()
    {
        if (0 <= m_fd) {
            int r = close(m_fd);
            TIGHTDB_ASSERT(r == 0);
            static_cast<void>(r);
        }
    }

    void release() { m_fd = -1; }

private:
    int m_fd;
};


struct FileLockGuard {
    FileLockGuard(): m_fd(-1) {}

    error_code init(int fd)
    {
    again:
        int r = flock(fd, LOCK_EX);
        if (r<0) {
            if (errno == EINTR) goto again;
            if (errno == ENOLCK) return ERROR_NO_RESOURCE;
            return ERROR_OTHER;
        }
        m_fd = fd;
        return ERROR_NONE;
    }

    ~FileLockGuard()
    {
        if (0 <= m_fd) {
            int r = flock(m_fd, LOCK_UN);
            TIGHTDB_ASSERT(r == 0);
            static_cast<void>(r);
        }
    }

private:
    int m_fd;
};


struct UnmapGuard {
    UnmapGuard(void* a, size_t s): m_addr(a), m_size(s) {}
    ~UnmapGuard()
    {
        if (m_addr) {
            int r = munmap(m_addr, m_size);
            TIGHTDB_ASSERT(r == 0);
            static_cast<void>(r);
        }
    }

    void release() { m_addr = 0; }

private:
    void* m_addr;
    size_t m_size;
};


error_code expand_file(int fd, off_t size)
{
    int res = ftruncate(fd, size);
    if (res<0) {
        switch (errno) {
            case EFBIG:
            case EINVAL: return ERROR_NO_RESOURCE;
            case EIO:    return ERROR_IO;
            case EROFS:  return ERROR_PERMISSION;
            default:     return ERROR_OTHER;
        }
    }
    return ERROR_NONE;
}


error_code map_file(int fd, off_t size, void** addr)
{
    void* const a = mmap(0, size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    if (a == MAP_FAILED) {
        switch (errno) {
            case EAGAIN:
            case EMFILE: return ERROR_NO_RESOURCE;
            case ENOMEM: return ERROR_OUT_OF_MEMORY;
            case ENODEV:
            case ENXIO:  return ERROR_BAD_FILESYS_PATH;
            default:     return ERROR_OTHER;
        }
    }
    *addr = a;
    return ERROR_NONE;
}


} // anonymous namespace


namespace tightdb {


struct Replication::SharedState {
    int m_use_count;
    Mutex m_mutex;
    int m_want_write_transact;
    bool m_write_transact_available, m_write_transact_finished;
    Condition m_cond_want_write_transact, m_cond_write_transact_available,
        m_cond_write_transact_finished, m_cond_transact_log_free, m_cond_persisted_db_version;

    /// Valid when m_write_transact_available is true, and is the
    /// version that the database will have after completion of the
    /// 'write' transaction that is made available.
    db_version_type m_write_transact_db_version;

    /// The version of the database that has been made
    /// persistent. After a commit, a client must wait for this
    /// version to reach the value of m_write_transact_db_version as
    /// it was at the time the transaction was initiated.
    db_version_type m_persisted_db_version;

    /// Size of the file. Invariant: 'm_size <= s' where 's' is the
    /// actual size of the file. This obviously assumes that the file
    /// is modified only through TightDB.
    size_t m_size;

    /// Index within file of the first byte of the first completed
    /// transaction log.
    size_t m_transact_log_used_begin;

    /// Index within file of the byte that follows the last byte of
    /// the last completed transaction log. m_transact_log_used_begin
    /// == m_transact_log_used_end if there are no completed
    /// transaction logs in the buffer.
    size_t m_transact_log_used_end;

    /// If m_transact_log_used_end < m_transact_log_used_begin, then
    /// the used area in the transaction log buffer is wrapped. In
    /// this case, the first section of the used area runs from
    /// m_transact_log_used_begin and has size
    /// (m_transact_log_used_wrap-m_transact_log_used_begin), and the
    /// second section runs from sizeof(SharedState) and has size
    /// (m_transact_log_used_end-sizeof(SharedState)).
    size_t m_transact_log_used_wrap;

    /// Index within file of the first byte of the last recently
    /// completed transaction log. This value need only be valid while
    /// m_write_transact_finished is true.
    size_t m_transact_log_new_begin;

    error_code init(size_t file_size)
    {
        m_want_write_transact = 0;
        m_write_transact_available = false;
        m_write_transact_finished  = false;
        m_persisted_db_version = 0;
        m_size = file_size;
        m_transact_log_used_begin = sizeof(SharedState);
        m_transact_log_used_end = m_transact_log_used_begin;
        error_code err = m_mutex.init_shared();
        if (err) return err;
        MutexDestroyGuard mdg(m_mutex);
        err = m_cond_want_write_transact.init_shared();
        if (err) return err;
        ConditionDestroyGuard cdg1(m_cond_want_write_transact);
        err = m_cond_write_transact_available.init_shared();
        if (err) return err;
        ConditionDestroyGuard cdg2(m_cond_write_transact_available);
        err = m_cond_write_transact_finished.init_shared();
        if (err) return err;
        ConditionDestroyGuard cdg3(m_cond_write_transact_finished);
        err = m_cond_transact_log_free.init_shared();
        if (err) return err;
        ConditionDestroyGuard cdg4(m_cond_transact_log_free);
        err = m_cond_persisted_db_version.init_shared();
        if (err) return err;
        cdg4.release();
        cdg3.release();
        cdg2.release();
        cdg1.release();
        mdg.release();
        return ERROR_NONE;
    }

    void destroy()
    {
        m_cond_want_write_transact.destroy();
        m_cond_write_transact_available.destroy();
        m_cond_write_transact_finished.destroy();
        m_cond_transact_log_free.destroy();
        m_cond_persisted_db_version.destroy();
        m_mutex.destroy();
    }
};


error_code Replication::init(string path_to_database_file, bool map_transact_log_buf)
{
    if (path_to_database_file.empty()) path_to_database_file = get_path_to_database_file();
    if (!m_subtab_path_buf.set_size(init_subtab_path_buf_size)) return ERROR_OUT_OF_MEMORY;
    const string repl_path = path_to_database_file + ".repl";
again:
    m_fd = open(repl_path.c_str(), O_RDWR|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    if (m_fd<0) {
        switch (errno) {
            case EACCES:
            case EROFS:        return ERROR_PERMISSION;
            case EIO:
            case EISDIR:
            case ELOOP:
            case ENAMETOOLONG:
            case ENOSR:
            case ENOTDIR:
            case ENXIO:
            case EOVERFLOW:
            case EAGAIN:
            case ENOMEM:
            case ETXTBSY:      return ERROR_BAD_FILESYS_PATH;
            case ENOENT:       return ERROR_NO_SUCH_FILE;
            case EMFILE:
            case ENFILE:
            case ENOSPC:       return ERROR_NO_RESOURCE;
            case EINTR:        goto again;
            default:           return ERROR_OTHER;
        }
    }
    CloseGuard cg(m_fd);
    {
        // Acquire a lock on the file
        FileLockGuard flg;
        error_code err = flg.init(m_fd);
        if (err) return err;
        // If empty, expand its size
        struct stat statbuf;
        int res = fstat(m_fd, &statbuf);
        if (res<0) {
            if (errno == ENOMEM) return ERROR_OUT_OF_MEMORY;
            return ERROR_OTHER;
        }
        size_t file_size = statbuf.st_size;
        if (file_size == 0) {
            file_size = sizeof(SharedState) + initial_transact_log_buffer_size;
            err = expand_file(m_fd, file_size);
            if (err) return err;
        }
        size_t mapped_size = map_transact_log_buf ? file_size : sizeof(SharedState);
        void* addr = 0;
        err = map_file(m_fd, mapped_size, &addr);
        if (err) return err;
        SharedState* const shared_state = static_cast<SharedState*>(addr);
        if (shared_state->m_use_count == 0) {
            UnmapGuard ug(addr, mapped_size);
            err = shared_state->init(file_size);
            if (err) return err;
            ug.release();  // success, so do not unmap
        }
        ++shared_state->m_use_count;
        m_shared_state = shared_state;
        m_shared_state_mapped_size = mapped_size;
    }
    cg.release(); // success, so do not close the file descriptor
    return ERROR_NONE;
}


Replication::~Replication()
{
    if (m_shared_state) {
        {
            FileLockGuard flg;
            error_code err = flg.init(m_fd);
            if (err) TIGHTDB_TERMINATE("Failed to obtain file lock");
            if (--m_shared_state->m_use_count == 0) {
                m_shared_state->destroy();
                int r = ftruncate(m_fd, 0);
                TIGHTDB_ASSERT(r == 0);
                static_cast<void>(r); // Deliberately ignoring errors here
            }
        }
        int r = munmap(m_shared_state, m_shared_state_mapped_size);
        TIGHTDB_ASSERT(r == 0);
        r = close(m_fd);
        TIGHTDB_ASSERT(r == 0);
        static_cast<void>(r);
    }
}


void Replication::interrupt()
{
    LockGuard lg(m_shared_state->m_mutex);
    m_interrupt = true;
    m_shared_state->m_cond_want_write_transact.notify_all();
    m_shared_state->m_cond_write_transact_available.notify_all();
    m_shared_state->m_cond_write_transact_finished.notify_all();
    m_shared_state->m_cond_transact_log_free.notify_all();
    m_shared_state->m_cond_persisted_db_version.notify_all();
}


error_code Replication::begin_write_transact()
{
    size_t file_size, transact_log_used_begin, transact_log_used_end;
    {
        LockGuard lg(m_shared_state->m_mutex);
        ++m_shared_state->m_want_write_transact;
        m_shared_state->m_cond_want_write_transact.notify_all();
        while (!m_shared_state->m_write_transact_available) {
            if (m_interrupt) {
                // FIXME: Retracting the request for a write transaction may create problems fo the local coordinator
                --m_shared_state->m_want_write_transact;
                return ERROR_INTERRUPTED;
            }
            m_shared_state->m_cond_write_transact_available.wait(lg);
        }
        m_shared_state->m_write_transact_available = false;
        --m_shared_state->m_want_write_transact;
        file_size = m_shared_state->m_size;
        transact_log_used_begin = m_shared_state->m_transact_log_used_begin;
        transact_log_used_end   = m_shared_state->m_transact_log_used_end;
        m_write_transact_db_version = m_shared_state->m_write_transact_db_version;
    }
    // At this point we know that the file size cannot change because
    // this cleint is the only one who may change it.
    TIGHTDB_ASSERT(m_shared_state_mapped_size <= file_size);
    if (m_shared_state_mapped_size < file_size) {
        error_code err = remap_file(file_size);
        if (err) {
            rollback_write_transact();
            return err;
        }
    }
    char* const base = static_cast<char*>(static_cast<void*>(m_shared_state));
    m_transact_log_free_begin = base + transact_log_used_end;
    if (transact_log_used_end < transact_log_used_begin) {
        // Used area is wrapped. We subtract one from
        // transact_log_used_begin to avoid using the last free byte
        // so we can distinguish between full and empty buffer.
        m_transact_log_free_end = base + transact_log_used_begin - 1;
    }
    else {
        m_transact_log_free_end = base + m_shared_state_mapped_size;
    }
    m_selected_table = 0;
    m_selected_spec  = 0;
    return ERROR_NONE;
}


bool Replication::commit_write_transact()
{
    LockGuard lg(m_shared_state->m_mutex);
    m_shared_state->m_transact_log_new_begin = m_shared_state->m_transact_log_used_end;
    m_shared_state->m_transact_log_used_end =
        m_transact_log_free_begin - static_cast<char*>(static_cast<void*>(m_shared_state));
    m_shared_state->m_write_transact_finished = true;
    m_shared_state->m_cond_write_transact_finished.notify_all();

    // Wait for the transaction log to be made persistent
    while (m_shared_state->m_persisted_db_version < m_write_transact_db_version) {
        if (m_interrupt) return false;
        m_shared_state->m_cond_persisted_db_version.wait(lg);
    }

    return true;
}


void Replication::rollback_write_transact()
{
    LockGuard lg(m_shared_state->m_mutex);
    m_shared_state->m_transact_log_new_begin = m_shared_state->m_transact_log_used_end;
    m_shared_state->m_write_transact_finished = true;
    m_shared_state->m_cond_write_transact_finished.notify_all();
}


void Replication::clear_interrupt()
{
    LockGuard lg(m_shared_state->m_mutex);
    m_interrupt = false;
}


bool Replication::wait_for_write_request()
{
    LockGuard lg(m_shared_state->m_mutex);
    while (m_shared_state->m_want_write_transact == 0) {
        if (m_interrupt) return false;
        m_shared_state->m_cond_want_write_transact.wait(lg);
    }
    return true;
}


// FIXME: Consider what should happen if nobody remains interested in this write transaction
bool Replication::grant_write_access_and_wait_for_completion(TransactLog& transact_log)
{
    LockGuard lg(m_shared_state->m_mutex);
    m_shared_state->m_write_transact_db_version = transact_log.m_db_version;
    m_shared_state->m_write_transact_available = true;
    m_shared_state->m_cond_write_transact_available.notify_all();
    while (!m_shared_state->m_write_transact_finished) {
        if (m_interrupt) return false;
        m_shared_state->m_cond_write_transact_finished.wait(lg);
    }
    m_shared_state->m_write_transact_finished = false;
    transact_log.m_offset1 = m_shared_state->m_transact_log_new_begin;
    if (m_shared_state->m_transact_log_used_end < m_shared_state->m_transact_log_new_begin) {
        transact_log.m_size1   = m_shared_state->m_transact_log_used_wrap - transact_log.m_offset1;
        transact_log.m_offset2 = sizeof(SharedState);
        transact_log.m_size2   = m_shared_state->m_transact_log_used_end - sizeof(SharedState);
    }
    else {
        transact_log.m_size1   = m_shared_state->m_transact_log_used_end - transact_log.m_offset1;
        transact_log.m_offset2 = transact_log.m_size2 = 0;
    }
    return true;
}


error_code Replication::map_transact_log(const TransactLog& transact_log,
                                         const char** addr1, const char** addr2)
{
    if (m_shared_state_mapped_size < transact_log.m_offset1+transact_log.m_size1 ||
        m_shared_state_mapped_size < transact_log.m_offset2+transact_log.m_size2) {
        size_t file_size;
        {
            LockGuard lg(m_shared_state->m_mutex);
            file_size = m_shared_state->m_size;
        }
        error_code err = remap_file(file_size);
        if (err) return err;
    }
    *addr1 = static_cast<char*>(static_cast<void*>(m_shared_state)) + transact_log.m_offset1;
    *addr2 = static_cast<char*>(static_cast<void*>(m_shared_state)) + transact_log.m_offset2;
    return ERROR_NONE;
}


void Replication::update_persisted_db_version(db_version_type version)
{
    LockGuard lg(m_shared_state->m_mutex);
    m_shared_state->m_persisted_db_version = version;
    m_shared_state->m_cond_persisted_db_version.notify_all();
}


void Replication::transact_log_consumed(size_t size)
{
    LockGuard lg(m_shared_state->m_mutex);
    if (m_shared_state->m_transact_log_used_end < m_shared_state->m_transact_log_used_begin) {
        // Used area is wrapped
        size_t contig = m_shared_state->m_transact_log_used_wrap -
            m_shared_state->m_transact_log_used_begin;
        if (contig < size) {
            m_shared_state->m_transact_log_used_begin = sizeof(SharedState);
            size -= contig;
        }
    }
    m_shared_state->m_transact_log_used_begin += size;
    m_shared_state->m_cond_transact_log_free.notify_all();
}


error_code Replication::transact_log_reserve_contig(size_t n)
{
    const size_t used_end = m_transact_log_free_begin -
        static_cast<char*>(static_cast<void*>(m_shared_state));
    {
        LockGuard lg(m_shared_state->m_mutex);
        for (;;) {
            const size_t used_begin = m_shared_state->m_transact_log_used_begin;
            if (used_begin <= used_end) {
                // In this case the used area is not wrapped across
                // the end of the buffer. This means that the free
                // area extends all the way to the end of the buffer.
                const size_t avail = m_shared_state->m_size - used_end;
                if (n <= avail) {
                    m_transact_log_free_end = m_transact_log_free_begin + avail;
                    return ERROR_NONE;
                }
                // Check if there is there enough space if we wrap the
                // used area at this point and continue at the
                // beginning of the buffer. Note that we again require
                // one unused byte.
                const size_t avail2 = used_begin - sizeof(SharedState);
                if (n < avail2) {
                    m_shared_state->m_transact_log_used_wrap = used_end;
                    m_transact_log_free_begin =
                        static_cast<char*>(static_cast<void*>(m_shared_state)) +
                        sizeof(SharedState);
                    m_transact_log_free_end = m_transact_log_free_begin + avail2;
                    return ERROR_NONE;
                }
            }
            else {
                // Note: We subtract 1 from the actual amount of free
                // space. This means that whenver the used area is
                // wrapped across the end of the buffer, then the last
                // byte of free space is never used. This, in turn,
                // ensures that whenever used_begin is equal to
                // used_end, it means that the buffer is empty, not
                // full.
                const size_t avail = used_begin - used_end - 1;
                if (n <= avail) {
                    m_transact_log_free_end = m_transact_log_free_begin + avail;
                    return ERROR_NONE;
                }
            }
            // At this point we know that the transaction log buffer
            // does not contain a contiguous unused regioun of size
            // 'n' or more. If the buffer contains other transaction
            // logs than the one we are currently creating, more space
            // will eventually become available as those transaction
            // logs gets transmitted to other clients. So in that case
            // we will simply wait.
            if (m_shared_state->m_transact_log_used_begin ==
                m_shared_state->m_transact_log_used_end) break;
            if (m_interrupt) return ERROR_INTERRUPTED;
            m_shared_state->m_cond_transact_log_free.wait(lg);
        }
    }
    // At this point we know that we have to expand the file. We also
    // know that thare are no readers of transaction logs, so we can
    // safly rearrange the buffer and its contents.

    // FIXME: In some cases it might be preferable to expand the
    // buffer even when we could simply wait for transmission
    // completion of complete logs. In that case we would have to wait
    // until all logs had disappeared from the buffer, and then
    // proceed to expand. But not if we are already at the maximum
    // size. Ideally we would base this decision on runtime buffer
    // utilization meassurments averaged over periods of time.

    return transact_log_expand(n, true);
}


error_code Replication::transact_log_append_overflow(const char* data, std::size_t size)
{
    // FIXME: During write access, it should be possible to use m_mapped_size instead of m_shared_state->m_size.
    bool need_expand = false;
    {
        char* const base = static_cast<char*>(static_cast<void*>(m_shared_state));
        const size_t used_end = m_transact_log_free_begin - base;
        LockGuard lg(m_shared_state->m_mutex);
        for (;;) {
            const size_t used_begin = m_shared_state->m_transact_log_used_begin;
            if (used_begin <= used_end) {
                // In this case the used area is not wrapped across
                // the end of the buffer.
                size_t avail = m_shared_state->m_size - used_end;
                // Require one unused byte.
                if (sizeof(SharedState) < used_begin) avail += used_begin - sizeof(SharedState) - 1; // FIXME: Use static const memeber
                if (size <= avail) {
                    m_transact_log_free_end = base + m_shared_state->m_size;
                    break;
                }
            }
            else {
                // In this case the used area is wrapped. Note: We
                // subtract 1 from the actual amount of free space to
                // avoid using the last byte when the used area is
                // wrapped.
                const size_t avail = used_begin - used_end - 1;
                if (size <= avail) {
                    m_transact_log_free_end = base + (used_begin - 1);
                    break;
                }
            }

            if     (m_shared_state->m_transact_log_used_begin ==
                    m_shared_state->m_transact_log_used_end) {
                need_expand = true;
                break;
            }

            if (m_interrupt) return ERROR_INTERRUPTED;
            m_shared_state->m_cond_transact_log_free.wait(lg);
        }
    }
    if (need_expand) {
        // We know at this point that no one else is trying to access
        // the transaction log buffer.
        error_code err = transact_log_expand(size, false);
        if (err) return err;
    }
    const size_t contig = m_transact_log_free_end - m_transact_log_free_begin;
    if (contig < size) {
        copy(data, data+contig, m_transact_log_free_begin);
        data += contig;
        size -= contig;
        char* const base = static_cast<char*>(static_cast<void*>(m_shared_state));
        m_transact_log_free_begin = base + sizeof(SharedState);
        {
            LockGuard lg(m_shared_state->m_mutex);
            m_shared_state->m_transact_log_used_wrap = m_shared_state->m_size;
            m_transact_log_free_end = base + (m_shared_state->m_transact_log_used_begin - 1);
        }
    }
    m_transact_log_free_begin = copy(data, data + size, m_transact_log_free_begin);
    return ERROR_NONE;
}


error_code Replication::transact_log_expand(size_t free, bool contig)
{
    // This function proceeds in the following steps:
    // 1) Determine the new larger buffer size
    // 2) Expand the file
    // 3) Remap the file into memory
    // 4) Rearrange the buffer contents

    // Since there are no transaction logs in the buffer except the
    // one being created, nobody else is accessing the transaction log
    // buffer information in SharedInfo, so we can access it without
    // locking. We can also freely rearrange the contents of the
    // buffer without locking.
    const size_t buffer_begin = sizeof(SharedState);
    const size_t used_begin = m_shared_state->m_transact_log_used_begin;
    const size_t used_end = m_transact_log_free_begin -
        static_cast<char*>(static_cast<void*>(m_shared_state));
    const size_t used_wrap = m_shared_state->m_transact_log_used_wrap;
    size_t min_size;
    if (used_end < used_begin) {
        // Used area is wrapped
        const size_t used_upper = used_wrap - used_begin;
        const size_t used_lower = used_end - buffer_begin;
        if (used_lower < used_upper) {
            // Move lower section
            min_size = used_wrap;
            if (int_add_with_overflow_detect(min_size, used_lower)) return ERROR_NO_RESOURCE;
            const size_t avail_lower = used_begin - buffer_begin;
            if (avail_lower <= free) { // Require one unused byte
                if (int_add_with_overflow_detect(min_size, free)) return ERROR_NO_RESOURCE;
            }
        }
        else {
            // Move upper section
            min_size = used_end + 1 + used_upper; // Require one unused byte
            if (int_add_with_overflow_detect(min_size, free)) return ERROR_NO_RESOURCE;
        }
    }
    else {
        // Used area is not wrapped
        if (contig || used_begin == buffer_begin) {
            min_size = used_end;
        }
        else {
            // Require one unused byte
            min_size = buffer_begin + (used_end-used_begin) + 1;
        }
        if (int_add_with_overflow_detect(min_size, free)) return ERROR_NO_RESOURCE;
    }

    size_t new_size = m_shared_state->m_size;
    if (int_multiply_with_overflow_detect(new_size, 2)) {
        new_size = numeric_limits<size_t>::max();
    }
    if (new_size < min_size) new_size = min_size;

    // Check that the new size fits in both ptrdiff_t and off_t (file size)
    typedef ArithBinOpType<ptrdiff_t, off_t>::type ptrdiff_off_type;
    typedef ArithBinOpType<ptrdiff_off_type, size_t>::type max_type;
    const max_type max = min<max_type>(numeric_limits<ptrdiff_t>::max(),
                                       numeric_limits<off_t>::max());
    if (max < new_size) {
        if (max < min_size) return ERROR_NO_RESOURCE;
        new_size = max;
    }

    error_code err = expand_file(m_fd, new_size);
    if (err) return err;
    m_shared_state->m_size = new_size;

    err = remap_file(new_size);
    if (err) return err;

    // Rearrange the buffer contents
    char* base = static_cast<char*>(static_cast<void*>(m_shared_state));
    if (used_end < used_begin) {
        // Used area is wrapped
        const size_t used_upper = used_wrap - used_begin;
        const size_t used_lower = used_end - buffer_begin;
        if (used_lower < used_upper) {
            // Move lower section
            copy(base+buffer_begin, base+used_end, base+used_wrap);
            if (m_shared_state->m_transact_log_used_end < used_begin)
                m_shared_state->m_transact_log_used_end += used_wrap - buffer_begin;
            if (contig && new_size - (used_wrap + used_lower) < free) {
                m_shared_state->m_transact_log_used_wrap = used_wrap + used_lower;
                m_transact_log_free_begin = base + buffer_begin;
                m_transact_log_free_end   = base + used_begin - 1; // Require one unused byte
            }
            else {
                m_transact_log_free_begin = base + (used_wrap + used_lower);
                m_transact_log_free_end   = base + new_size;
            }
        }
        else {
            // Move upper section
            copy_backward(base+used_begin, base+used_wrap, base+new_size);
            m_shared_state->m_transact_log_used_begin = new_size - used_upper;
            if (used_begin <= m_shared_state->m_transact_log_used_end)
                m_shared_state->m_transact_log_used_end +=
                    m_shared_state->m_transact_log_used_begin - used_begin;
            m_shared_state->m_transact_log_used_wrap = new_size;
            m_transact_log_free_begin = base + used_end;
            // Require one unused byte
            m_transact_log_free_end = base + (m_shared_state->m_transact_log_used_begin - 1);
        }
    }
    else {
        // Used area is not wrapped
        m_transact_log_free_begin = base + used_end;
        m_transact_log_free_end   = base + new_size;
    }

    return ERROR_NONE;
}


error_code Replication::remap_file(size_t size)
{
    // Take care to leave Replication instance in valid state on error
    // by mapping a new bigger region before unmapping the old one.
    void* addr = 0;
    error_code err = map_file(m_fd, size, &addr);
    if (err) return err;

    int r = munmap(m_shared_state, m_shared_state_mapped_size);
    TIGHTDB_ASSERT(r == 0);
    static_cast<void>(r);

    m_shared_state = static_cast<SharedState*>(addr);
    m_shared_state_mapped_size = size;
    return ERROR_NONE;
}


error_code Replication::select_table(const Table* table)
{
    size_t* begin;
    size_t* end;
    for (;;) {
        begin = m_subtab_path_buf.m_data;
        end = table->record_subtable_path(begin, begin+m_subtab_path_buf.m_size);
        if (end) break;
        size_t new_size = m_subtab_path_buf.m_size;
        if (int_multiply_with_overflow_detect(new_size, 2)) return ERROR_NO_RESOURCE;
        if (!m_subtab_path_buf.set_size(new_size)) return ERROR_OUT_OF_MEMORY;
    }
    char* buf;
    const int max_elems_per_chunk = 8; // FIXME: Use smaller number when compiling in debug mode
    error_code err = transact_log_reserve(&buf, 1 + (1+max_elems_per_chunk)*max_enc_bytes_per_int);
    if (err) return err;
    *buf++ = 'T';
    TIGHTDB_ASSERT(1 <= end - begin);
    const ptrdiff_t level = (end - begin) / 2;
    buf = encode_int(buf, level);
    for (;;) {
        for (int i=0; i<max_elems_per_chunk; ++i) {
            buf = encode_int(buf, *--end);
            if (begin == end) goto good;
        }
        transact_log_advance(buf);
        err = transact_log_reserve(&buf, max_elems_per_chunk*max_enc_bytes_per_int);
        if (err) return err;
    }

good:
    transact_log_advance(buf);
    m_selected_spec  = 0;
    m_selected_table = table;
    return ERROR_NONE;
}


error_code Replication::select_spec(const Table* table, const Spec* spec)
{
    error_code err = check_table(table);
    if (err) return err;
    size_t* begin;
    size_t* end;
    for (;;) {
        begin = m_subtab_path_buf.m_data;
        end = table->record_subspec_path(spec, begin, begin+m_subtab_path_buf.m_size);
        if (end) break;
        size_t new_size = m_subtab_path_buf.m_size;
        if (int_multiply_with_overflow_detect(new_size, 2)) return ERROR_NO_RESOURCE;
        if (!m_subtab_path_buf.set_size(new_size)) return ERROR_OUT_OF_MEMORY;
    }
    char* buf;
    const int max_elems_per_chunk = 8; // FIXME: Use smaller number when compiling in debug mode
    err = transact_log_reserve(&buf, 1 + (1+max_elems_per_chunk)*max_enc_bytes_per_int);
    if (err) return err;
    *buf++ = 'S';
    const ptrdiff_t level = end - begin;
    buf = encode_int(buf, level);
    if (begin == end) goto good;
    for (;;) {
        for (int i=0; i<max_elems_per_chunk; ++i) {
            buf = encode_int(buf, *--end);
            if (begin == end) goto good;
        }
        transact_log_advance(buf);
        err = transact_log_reserve(&buf, max_elems_per_chunk*max_enc_bytes_per_int);
        if (err) return err;
    }

good:
    transact_log_advance(buf);
    m_selected_spec = spec;
    return ERROR_NONE;
}


struct Replication::TransactLogApplier {
    TransactLogApplier(InputStream& transact_log, Group& group):
        m_input(transact_log), m_group(group), m_input_buffer(0),
        m_num_subspecs(0), m_dirty_spec(false) {}

    ~TransactLogApplier()
    {
        delete[] m_input_buffer;
        delete_subspecs();
    }

#ifdef TIGHTDB_DEBUG
    void set_apply_log(ostream* log) { m_log = log; if (m_log) *m_log << boolalpha; }
#endif

    error_code apply();

private:
    InputStream& m_input;
    Group& m_group;
    static const size_t m_input_buffer_size = 4096; // FIXME: Use smaller number when compiling in debug mode
    char* m_input_buffer;
    const char* m_input_begin;
    const char* m_input_end;
    TableRef m_table;
    Buffer<Spec*> m_subspecs;
    size_t m_num_subspecs;
    bool m_dirty_spec;
    StringBuffer m_string_buffer;
#ifdef TIGHTDB_DEBUG
    ostream* m_log;
#endif

    // Returns false on end-of-input
    bool fill_input_buffer()
    {
        const size_t n = m_input.read(m_input_buffer, m_input_buffer_size);
        if (n == 0) return false;
        m_input_begin = m_input_buffer;
        m_input_end   = m_input_buffer + n;
        return true;
    }

    // Returns false on premature end-of-input
    bool read_char(char& c)
    {
        if (m_input_begin == m_input_end && !fill_input_buffer()) return false;
        c = *m_input_begin++;
        return true;
    }

    // Returns false on overflow or premature end-of-input
    template<class T> bool read_int(T&);

    error_code read_string(StringBuffer&);

    error_code add_subspec(Spec*);

    template<bool insert> error_code set_or_insert(int column_ndx, size_t ndx);

    void delete_subspecs()
    {
        const size_t n = m_num_subspecs;
        for (size_t i=0; i<n; ++i) delete m_subspecs[i];
        m_num_subspecs = 0;
    }

    void finalize_spec() // FIXME: May fail
    {
        TIGHTDB_ASSERT(m_table);
        m_table->update_from_spec();
#ifdef TIGHTDB_DEBUG
        if (m_log) *m_log << "table->update_from_spec()\n";
#endif
        m_dirty_spec = false;
    }

    bool is_valid_column_type(int type)
    {
        switch (type) {
        case COLUMN_TYPE_INT:
        case COLUMN_TYPE_BOOL:
        case COLUMN_TYPE_DATE:
        case COLUMN_TYPE_STRING:
        case COLUMN_TYPE_BINARY:
        case COLUMN_TYPE_TABLE:
        case COLUMN_TYPE_MIXED: return true;
        default: break;
        }
        return false;
    }
};


template<class T> bool Replication::TransactLogApplier::read_int(T& v)
{
    T value = 0;
    int part;
    const int max_bytes = (std::numeric_limits<T>::digits+1+6)/7;
    for (int i=0; i<max_bytes; ++i) {
        char c;
        if (!read_char(c)) return false;
        part = static_cast<unsigned char>(c);
        if (0xFF < part) return false; // Only the first 8 bits may be used in each byte
        if ((part & 0x80) == 0) {
            T p = part & 0x3F;
            if (int_shift_left_with_overflow_detect(p, i*7)) return false;
            value |= p;
            break;
        }
        if (i == max_bytes-1) return false; // Two many bytes
        value |= T(part & 0x7F) << (i*7);
    }
    if (part & 0x40) {
        // The real value is negative. Because 'value' is positive at
        // this point, the following negation is guaranteed by the
        // standard to never overflow.
        value = -value;
        if (int_subtract_with_overflow_detect(value, 1)) return false;
    }
    v = value;
    return true;
}


error_code Replication::TransactLogApplier::read_string(StringBuffer& buf)
{
    buf.clear();
    size_t size;
    if (!read_int(size)) return ERROR_IO;
    buf.resize(size); // FIXME: Now throws
    char* str_end = buf.data();
    for (;;) {
        const size_t avail = m_input_end - m_input_begin;
        if (size <= avail) break;
        const char* to = m_input_begin + avail;
        copy(m_input_begin, to, str_end);
        if (!fill_input_buffer()) return ERROR_IO;
        str_end += avail;
        size -= avail;
    }
    const char* to = m_input_begin + size;
    copy(m_input_begin, to, str_end);
    m_input_begin = to;
    return ERROR_NONE;
}


error_code Replication::TransactLogApplier::add_subspec(Spec* spec)
{
    if (m_num_subspecs == m_subspecs.m_size) {
        Buffer<Spec*> new_subspecs;
        size_t new_size = m_subspecs.m_size;
        if (new_size == 0) {
            new_size = 16; // FIXME: Use a small value (1) when compiling in debug mode
        }
        else {
            if (int_multiply_with_overflow_detect(new_size, 2)) return ERROR_NO_RESOURCE;
        }
        if (!new_subspecs.set_size(new_size)) return ERROR_OUT_OF_MEMORY;
        copy(m_subspecs.m_data, m_subspecs.m_data+m_num_subspecs, new_subspecs.m_data);
        using std::swap;
        swap(m_subspecs, new_subspecs);
    }
    m_subspecs[m_num_subspecs++] = spec;
    return ERROR_NONE;
}


template<bool insert>
error_code Replication::TransactLogApplier::set_or_insert(int column_ndx, size_t ndx)
{
    switch (m_table->get_column_type(column_ndx)) {
    case COLUMN_TYPE_INT:
        {
            int64_t value;
            if (!read_int(value)) return ERROR_IO;
            if (insert) m_table->insert_int(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
            else m_table->set_int(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
            if (m_log) {
                if (insert) *m_log << "table->insert_int("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
                else *m_log << "table->set_int("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
            }
#endif
        }
        break;
    case COLUMN_TYPE_BOOL:
        {
            bool value;
            if (!read_int(value)) return ERROR_IO;
            if (insert) m_table->insert_bool(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
            else m_table->set_bool(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
            if (m_log) {
                if (insert) *m_log << "table->insert_bool("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
                else *m_log << "table->set_bool("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
            }
#endif
        }
        break;
    case COLUMN_TYPE_DATE:
        {
            time_t value;
            if (!read_int(value)) return ERROR_IO;
            if (insert) m_table->insert_date(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
            else m_table->set_date(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
            if (m_log) {
                if (insert) *m_log << "table->insert_date("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
                else *m_log << "table->set_date("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
            }
#endif
        }
        break;
    case COLUMN_TYPE_STRING:
        {
            const error_code err = read_string(m_string_buffer);
            if (err) return err;
            const char* const value = m_string_buffer.c_str();
            if (insert) m_table->insert_string(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
            else m_table->set_string(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
            if (m_log) {
                if (insert) *m_log << "table->insert_string("<<column_ndx<<", "<<ndx<<", \""<<value<<"\")\n";
                else *m_log << "table->set_string("<<column_ndx<<", "<<ndx<<", \""<<value<<"\")\n";
            }
#endif
        }
        break;
    case COLUMN_TYPE_BINARY:
        {
            const error_code err = read_string(m_string_buffer);
            if (err) return err;
            if (insert) m_table->insert_binary(column_ndx, ndx, m_string_buffer.data(),
                                               m_string_buffer.size()); // FIXME: Memory allocation failure!!!
            else m_table->set_binary(column_ndx, ndx, m_string_buffer.data(),
                                     m_string_buffer.size()); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
            if (m_log) {
                if (insert) *m_log << "table->insert_binary("<<column_ndx<<", "<<ndx<<", ...)\n";
                else *m_log << "table->set_binary("<<column_ndx<<", "<<ndx<<", ...)\n";
            }
#endif
        }
        break;
    case COLUMN_TYPE_TABLE:
        if (insert) m_table->insert_subtable(column_ndx, ndx); // FIXME: Memory allocation failure!!!
        else m_table->clear_subtable(column_ndx, ndx); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
        if (m_log) {
            if (insert) *m_log << "table->insert_subtable("<<column_ndx<<", "<<ndx<<")\n";
            else *m_log << "table->clear_subtable("<<column_ndx<<", "<<ndx<<")\n";
        }
#endif
        break;
    case COLUMN_TYPE_MIXED:
        {
            int type;
            if (!read_int(type)) return ERROR_IO;
            switch (type) {
            case COLUMN_TYPE_INT:
                {
                    int64_t value;
                    if (!read_int(value)) return ERROR_IO;
                    if (insert) m_table->insert_mixed(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
                    else m_table->set_mixed(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
                    if (m_log) {
                        if (insert) *m_log << "table->insert_mixed("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
                        else *m_log << "table->set_mixed("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
                    }
#endif
                }
                break;
            case COLUMN_TYPE_BOOL:
                {
                    bool value;
                    if (!read_int(value)) return ERROR_IO;
                    if (insert) m_table->insert_mixed(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
                    else m_table->set_mixed(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
                    if (m_log) {
                        if (insert) *m_log << "table->insert_mixed("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
                        else *m_log << "table->set_mixed("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
                    }
#endif
                }
                break;
            case COLUMN_TYPE_DATE:
                {
                    time_t value;
                    if (!read_int(value)) return ERROR_IO;
                    if (insert) m_table->insert_mixed(column_ndx, ndx, Date(value)); // FIXME: Memory allocation failure!!!
                    else m_table->set_mixed(column_ndx, ndx, Date(value)); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
                    if (m_log) {
                        if (insert) *m_log << "table->insert_mixed("<<column_ndx<<", "<<ndx<<", Date("<<value<<"))\n";
                        else *m_log << "table->set_mixed("<<column_ndx<<", "<<ndx<<", Date("<<value<<"))\n";
                    }
#endif
                }
                break;
            case COLUMN_TYPE_STRING:
                {
                    const error_code err = read_string(m_string_buffer);
                    if (err) return err;
                    const char* const value = m_string_buffer.c_str();
                    if (insert) m_table->insert_mixed(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
                    else m_table->set_mixed(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
                    if (m_log) {
                        if (insert) *m_log << "table->insert_mixed("<<column_ndx<<", "<<ndx<<", \""<<value<<"\")\n";
                        else *m_log << "table->set_mixed("<<column_ndx<<", "<<ndx<<", \""<<value<<"\")\n";
                    }
#endif
                }
                break;
            case COLUMN_TYPE_BINARY:
                {
                    const error_code err = read_string(m_string_buffer);
                    if (err) return err;
                    const BinaryData value(m_string_buffer.data(), m_string_buffer.size());
                    if (insert) m_table->insert_mixed(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
                    else m_table->set_mixed(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
                    if (m_log) {
                        if (insert) *m_log << "table->insert_mixed("<<column_ndx<<", "<<ndx<<", BinaryData(...))\n";
                        else *m_log << "table->set_mixed("<<column_ndx<<", "<<ndx<<", BinaryData(...))\n";
                    }
#endif
                }
                break;
            case COLUMN_TYPE_TABLE:
                if (insert) m_table->insert_mixed(column_ndx, ndx, Mixed::subtable_tag()); // FIXME: Memory allocation failure!!!
                else m_table->set_mixed(column_ndx, ndx, Mixed::subtable_tag()); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
                if (m_log) {
                    if (insert) *m_log << "table->insert_mixed("<<column_ndx<<", "<<ndx<<", Mixed::subtable_tag())\n";
                    else *m_log << "table->set_mixed("<<column_ndx<<", "<<ndx<<", Mixed::subtable_tag())\n";
                }
#endif
                break;
            default:
                return ERROR_IO;
            }
        }
        break;
    default:
        return ERROR_IO;
    }
    return ERROR_NONE;
}


error_code Replication::TransactLogApplier::apply()
{
    if (!m_input_buffer) {
        m_input_buffer = new (nothrow) char[m_input_buffer_size];
        if (!m_input_buffer) return ERROR_OUT_OF_MEMORY;
    }
    m_input_begin = m_input_end = m_input_buffer;

    // FIXME: Problem: The modifying methods of group, table, and spec generally throw.
    Spec* spec = 0;
    for (;;) {
        char instr;
        if (!read_char(instr)) {
            break;
        }
// cerr << "["<<instr<<"]";
        switch (instr) {
        case 's':  // Set value
            {
                if (m_dirty_spec) finalize_spec();
                int column_ndx;
                if (!read_int(column_ndx)) return ERROR_IO;
                size_t ndx;
                if (!read_int(ndx)) return ERROR_IO;
                if (!m_table) return ERROR_IO;
                if (column_ndx < 0 || int(m_table->get_column_count()) <= column_ndx)
                    return ERROR_IO;
                if (m_table->size() <= ndx) return ERROR_IO;
                const bool insert = false;
                const error_code err = set_or_insert<insert>(column_ndx, ndx);
                if (err) return err;
            }
            break;

        case 'i':  // Insert value
            {
                if (m_dirty_spec) finalize_spec();
                int column_ndx;
                if (!read_int(column_ndx)) return ERROR_IO;
                size_t ndx;
                if (!read_int(ndx)) return ERROR_IO;
                if (!m_table) return ERROR_IO;
                if (column_ndx < 0 || int(m_table->get_column_count()) <= column_ndx)
                    return ERROR_IO;
                if (m_table->size() < ndx) return ERROR_IO;
                const bool insert = true;
                const error_code err = set_or_insert<insert>(column_ndx, ndx);
                if (err) return err;
            }
            break;

        case 'c':  // Row insert complete
            if (m_dirty_spec) finalize_spec();
            if (!m_table) return ERROR_IO;
            m_table->insert_done(); // FIXME: May fail
#ifdef TIGHTDB_DEBUG
            if (m_log) *m_log << "table->insert_done()\n";
#endif
            break;

        case 'I':  // Insert empty rows
            {
                if (m_dirty_spec) finalize_spec();
                size_t ndx, num_rows;
                if (!read_int(ndx) || !read_int(num_rows)) return ERROR_IO;
                if (!m_table || m_table->size() < ndx) return ERROR_IO;
                m_table->insert_empty_row(ndx, num_rows); // FIXME: May fail
#ifdef TIGHTDB_DEBUG
                if (m_log) *m_log << "table->insert_empty_row("<<ndx<<", "<<num_rows<<")\n";
#endif
            }
            break;

        case 'R':  // Remove row
            {
                if (m_dirty_spec) finalize_spec();
                size_t ndx;
                if (!read_int(ndx)) return ERROR_IO;
                if (!m_table || m_table->size() < ndx) return ERROR_IO;
                m_table->remove(ndx); // FIXME: May fail
#ifdef TIGHTDB_DEBUG
                if (m_log) *m_log << "table->remove("<<ndx<<")\n";
#endif
            }
            break;

        case 'a':  // Add int to column
            {
                if (m_dirty_spec) finalize_spec();
                int column_ndx;
                if (!read_int(column_ndx)) return ERROR_IO;
                if (!m_table) return ERROR_IO;
                if (column_ndx < 0 || int(m_table->get_column_count()) <= column_ndx)
                    return ERROR_IO;
                int64_t value;
                if (!read_int(value)) return ERROR_IO;
                m_table->add_int(column_ndx, value); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
                if (m_log) *m_log << "table->add_int("<<column_ndx<<", "<<value<<")\n";
#endif
            }
            break;

        case 'T':  // Select table
            {
                if (m_dirty_spec) finalize_spec();
                int levels;
                if (!read_int(levels)) return ERROR_IO;
                size_t ndx;
                if (!read_int(ndx)) return ERROR_IO;
                if (m_group.get_table_count() <= ndx) return ERROR_IO;
                m_table = m_group.get_table_ptr(ndx)->get_table_ref();
#ifdef TIGHTDB_DEBUG
                if (m_log) *m_log << "table = group->get_table_by_ndx("<<ndx<<")\n";
#endif
                spec = 0;
                for (int i=0; i<levels; ++i) {
                    int column_ndx;
                    if (!read_int(column_ndx)) return ERROR_IO;
                    if (!read_int(ndx)) return ERROR_IO;
                    if (column_ndx < 0 || int(m_table->get_column_count()) <= column_ndx)
                        return ERROR_IO;
                    if (m_table->size() <= ndx) return ERROR_IO;
                    switch (m_table->get_column_type(column_ndx)) {
                    case COLUMN_TYPE_TABLE:
                        m_table = m_table->get_subtable(column_ndx, ndx);
                        break;
                    case COLUMN_TYPE_MIXED:
                        m_table = m_table->get_subtable(column_ndx, ndx);
                        if (!m_table) return ERROR_IO;
                        break;
                    default:
                        return ERROR_IO;
                    }
#ifdef TIGHTDB_DEBUG
                    if (m_log) *m_log << "table = table->get_subtable("<<column_ndx<<", "<<ndx<<")\n";
#endif
                }
            }
            break;

        case 'C':  // Clear table
            {
                if (m_dirty_spec) finalize_spec();
                if (!m_table) return ERROR_IO;
                m_table->clear(); // FIXME: Can probably fail!
#ifdef TIGHTDB_DEBUG
                if (m_log) *m_log << "table->clear()\n";
#endif
            }
            break;

        case 'x':  // Add index to column
            {
                if (m_dirty_spec) finalize_spec();
                int column_ndx;
                if (!read_int(column_ndx)) return ERROR_IO;
                if (!m_table) return ERROR_IO;
                if (column_ndx < 0 || int(m_table->get_column_count()) <= column_ndx)
                    return ERROR_IO;
                m_table->set_index(column_ndx); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
                if (m_log) *m_log << "table->set_index("<<column_ndx<<")\n";
#endif
            }
            break;

        case 'A':  // Add column to selected spec
            {
                int type;
                if (!read_int(type)) return ERROR_IO;
                if (!is_valid_column_type(type)) return ERROR_IO;
                const error_code err = read_string(m_string_buffer);
                if (err) return err;
                const char* const name = m_string_buffer.c_str();
                if (!spec) return ERROR_IO;
                // FIXME: Is it legal to have multiple columns with the same name?
                if (spec->get_column_index(name) != size_t(-1)) return ERROR_IO;
                spec->add_column(ColumnType(type), name);
#ifdef TIGHTDB_DEBUG
                if (m_log) *m_log << "spec->add_column("<<type<<", \""<<name<<"\")\n";
#endif
                m_dirty_spec = true;
            }
            break;

        case 'S':  // Select spec for currently selected table
            {
                delete_subspecs();
                if (!m_table) return ERROR_IO;
                spec = &m_table->get_spec();
#ifdef TIGHTDB_DEBUG
                if (m_log) *m_log << "spec = table->get_spec()\n";
#endif
                int levels;
                if (!read_int(levels)) return ERROR_IO;
                for (int i=0; i<levels; ++i) {
                    int subspec_ndx;
                    if (!read_int(subspec_ndx)) return ERROR_IO;
                    if (subspec_ndx < 0 || int(spec->get_num_subspecs()) <= subspec_ndx)
                        return ERROR_IO;
                    spec = new (nothrow) Spec(spec->get_subspec_by_ndx(subspec_ndx));
                    if (!spec) return ERROR_OUT_OF_MEMORY;
                    const error_code err = add_subspec(spec);
                    if (err) {
                        delete spec;
                        return err;
                    }
#ifdef TIGHTDB_DEBUG
                    if (m_log) *m_log << "spec = spec->get_subspec_by_ndx("<<subspec_ndx<<")\n";
#endif
                }
            }
            break;

        case 'N':  // New top level table
            {
                const error_code err = read_string(m_string_buffer);
                if (err) return err;
                const char* const name = m_string_buffer.c_str();
                if (m_group.has_table(name)) return ERROR_IO;
                if (!m_group.create_new_table(name)) return ERROR_OUT_OF_MEMORY;
#ifdef TIGHTDB_DEBUG
                if (m_log) *m_log << "group->create_new_table(\""<<name<<"\")\n";
#endif
            }
            break;

        case 'Z':  // Optimize table
            {
                if (m_dirty_spec) finalize_spec();
                if (!m_table) return ERROR_IO;
                m_table->optimize(); // FIXME: May fail
#ifdef TIGHTDB_DEBUG
                if (m_log) *m_log << "table->optimize()\n";
#endif
            }
            break;

        default:
            return ERROR_IO;
        }
    }

    if (m_dirty_spec) finalize_spec(); // FIXME: Why is this necessary?
    return ERROR_NONE;
}


#ifdef TIGHTDB_DEBUG
error_code Replication::apply_transact_log(InputStream& transact_log, Group& group, ostream* log)
{
    TransactLogApplier applier(transact_log, group);
    applier.set_apply_log(log);
    return applier.apply();
}
#else
error_code Replication::apply_transact_log(InputStream& transact_log, Group& group)
{
    TransactLogApplier applier(transact_log, group);
    return applier.apply();
}
#endif


} // namespace tightdb
