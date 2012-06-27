#ifdef TIGHTDB_ENABLE_REPLICATION

#include <new>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/mman.h>

#include <tightdb/static_assert.hpp>
#include <tightdb/pthread_helpers.hpp>
#include <tightdb/table.hpp>
#include <tightdb/replication.hpp>

using namespace std;
using namespace tightdb;

namespace {

    // Note: The sum of this value and sizeof(SharedState) may not
    // exceed the maximum values of any of the types 'size_t',
    // 'ptrdiff_t', or 'off_t'.
#ifdef _DEBUG
    const size_t initial_transact_log_buffer_size = 128;
#else
    const size_t initial_transact_log_buffer_size = 16*1024;
#endif

    const size_t init_subtab_path_buf_size = 2*8-1; // 8 table levels (soft limit)


    // Works for integers only. 'rval' must not be negative.
    template<class L, class R> inline bool add_with_overflow_detect(L& lval, R rval)
    {
        TIGHTDB_STATIC_ASSERT((SameType<L,R>::value), "Same type required");
        if (numeric_limits<R>::max() - rval < lval) return true;
        lval += rval;
        return false;
    }

    // Works for integers only. 'lval' must not be negative. 'rval'
    // must be stricly greater than zero.
    template<class L, class R> inline bool mul_with_overflow_detect(L& lval, R rval)
    {
        TIGHTDB_STATIC_ASSERT((SameType<L,R>::value), "Same type required");
        if (numeric_limits<R>::max() / rval < lval) return true;
        lval *= rval;
        return false;
    }


    struct StringBuffer {
        StringBuffer(): m_data(0), m_size(0), m_capacity(0) {}
        ~StringBuffer() { delete[] m_data; }

        error_code init(size_t capacity = 0)
        {
            if (capacity < numeric_limits<size_t>::max()) ++capacity;
            m_data = new (nothrow) char[capacity];
            if (!m_data) return ERROR_OUT_OF_MEMORY;
            m_capacity = capacity;
            m_data[0] = 0;
            return ERROR_NONE;
        }

        const char* data() const { return m_data; }
        size_t size() const { return m_size; }
        const char* c_str() const { return m_data; }

        error_code append(const char* data, size_t size)
        {
            size_t new_size = m_size;
            if (add_with_overflow_detect(new_size, size)) return ERROR_NO_RESOURCE;
            {
                size_t new_min_capacity = new_size;
                if (add_with_overflow_detect(new_min_capacity, size_t(1))) return ERROR_NO_RESOURCE;
                if (m_capacity < new_min_capacity) {
                    size_t new_capacity = m_capacity;
                    if (mul_with_overflow_detect(new_capacity, size_t(2)))
                        new_capacity = numeric_limits<size_t>::max();
                    if (new_capacity < new_min_capacity) new_capacity = new_min_capacity;
                    char* new_data = new (nothrow) char[new_capacity];
                    if (!new_data) return ERROR_OUT_OF_MEMORY;
                    copy(m_data, m_data + m_size+1, new_data);
                    m_data     = new_data;
                    m_capacity = new_capacity;
                }
            }
            copy(data, data+size, m_data+m_size);
            m_size += size;
            m_data[m_size] = 0;
            return ERROR_NONE;
        }

        error_code append_c_str(const char* str)
        {
            const size_t length = strlen(str);
            return append(str, length);
        }

    private:
        char *m_data;
        size_t m_size;
        size_t m_capacity;
    };


    struct CloseGuard {
        CloseGuard(int fd): m_fd(fd) {}
        ~CloseGuard()
        {
            if (0 <= m_fd) {
                int r = close(m_fd);
                assert(r == 0);
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
            int r = flock(fd, LOCK_EX);
            if (r<0) {
                if (errno == EINTR) return ERROR_INTERRUPTED;
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
                assert(r == 0);
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
                assert(r == 0);
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
        m_cond_write_transact_finished, m_cond_transact_log_free;

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

    error_code init(size_t file_size);
    void destroy();
};


error_code Replication::init()
{
    if (!m_subtab_path_buf.set_size(init_subtab_path_buf_size)) return ERROR_OUT_OF_MEMORY;
    StringBuffer str_buf;
    error_code err = str_buf.init();
    if (err) return err;
    err = str_buf.append_c_str(get_path_to_database_file());
    if (err) return err;
    err = str_buf.append_c_str(".repl");
    if (err) return err;
    m_fd = open(str_buf.c_str(), O_RDWR|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
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
        default:           return ERROR_OTHER;
            // FIXME: What about EINTR? Can it even happen for a regular file?
        }
    }
    CloseGuard cg(m_fd);
    {
        // Acquire a lock on the file
        FileLockGuard flg;
        err = flg.init(m_fd);
        if (err) return err;
        // If empty, expand its size
        struct stat statbuf;
        int res = fstat(m_fd, &statbuf);
        if (res<0) {
            if (errno == ENOMEM) return ERROR_OUT_OF_MEMORY;
            return ERROR_OTHER;
        }
        size_t size = statbuf.st_size;
        if (size == 0) {
            size = sizeof(SharedState) + initial_transact_log_buffer_size;
            err = expand_file(m_fd, size);
            if (err) return err;
        }
        void* addr = 0;
        err = map_file(m_fd, size, &addr);
        if (err) return err;
        SharedState* const shared_state = static_cast<SharedState*>(addr);
        if (shared_state->m_use_count == 0) {
            UnmapGuard ug(addr, size);
            err = shared_state->init(size);
            if (err) return err;
            ug.release();  // success, so do not unmap
        }
        ++shared_state->m_use_count;
        m_shared_state = shared_state;
        m_shared_state_mapped_size = size;
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
                assert(r == 0);
                static_cast<void>(r); // Deliberately ignoring errors here
            }
        }
        int r = munmap(m_shared_state, m_shared_state_mapped_size);
        assert(r == 0);
        r = close(m_fd);
        assert(r == 0);
        static_cast<void>(r);
    }
}


void Replication::shutdown()
{
    LockGuard lg(m_shared_state->m_mutex);
    m_shared_state_shutdown = true;
    m_shared_state->m_cond_want_write_transact.notify_all();
    m_shared_state->m_cond_write_transact_available.notify_all();
    m_shared_state->m_cond_write_transact_finished.notify_all();
    m_shared_state->m_cond_transact_log_free.notify_all();
}


error_code Replication::acquire_write_access()
{
    size_t file_size, transact_log_used_begin, transact_log_used_end;
    {
        LockGuard lg(m_shared_state->m_mutex);
        ++m_shared_state->m_want_write_transact;
        m_shared_state->m_cond_want_write_transact.notify_all();
        while (!m_shared_state->m_write_transact_available) {
            m_shared_state->m_cond_write_transact_available.wait(lg);
            if (m_shared_state_shutdown) return ERROR_INTERRUPTED;
        }
        m_shared_state->m_write_transact_available = false;
        --m_shared_state->m_want_write_transact;
        file_size = m_shared_state->m_size;
        transact_log_used_begin = m_shared_state->m_transact_log_used_begin;
        transact_log_used_end   = m_shared_state->m_transact_log_used_end;
    }
    // At this point we know that the file size cannot change because
    // this cleint is the only one who may change it.
    assert(m_shared_state_mapped_size <= file_size);
    if (m_shared_state_mapped_size < file_size) {
        error_code err = remap_file(file_size);
        if (err) {
            release_write_access(true); // Rollback
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


void Replication::release_write_access(bool rollback)
{
    LockGuard lg(m_shared_state->m_mutex);
    m_shared_state->m_transact_log_new_begin = m_shared_state->m_transact_log_used_end;
    if (!rollback) {
        m_shared_state->m_transact_log_used_end =
            m_transact_log_free_begin - static_cast<char*>(static_cast<void*>(m_shared_state));
    }
    m_shared_state->m_write_transact_finished = true;
    m_shared_state->m_cond_write_transact_finished.notify_all();
}


bool Replication::wait_for_write_request()
{
    LockGuard lg(m_shared_state->m_mutex);
    while (m_shared_state->m_want_write_transact == 0) {
        m_shared_state->m_cond_want_write_transact.wait(lg);
        if (m_shared_state_shutdown) return false;
    }
    return true;
}


bool Replication::grant_write_access_and_wait_for_completion(TransactLog& l)
{
    LockGuard lg(m_shared_state->m_mutex);
    m_shared_state->m_write_transact_available = true;
    m_shared_state->m_cond_write_transact_available.notify_all();
    while (!m_shared_state->m_write_transact_finished) {
        m_shared_state->m_cond_write_transact_finished.wait(lg);
        if (m_shared_state_shutdown) return false;
    }
    m_shared_state->m_write_transact_finished = false;
    l.offset1 = m_shared_state->m_transact_log_new_begin;
    if (m_shared_state->m_transact_log_used_end < m_shared_state->m_transact_log_new_begin) {
        l.size1   = m_shared_state->m_transact_log_used_wrap - l.offset1;
        l.offset2 = sizeof(SharedState);
        l.size2   = m_shared_state->m_transact_log_used_end - sizeof(SharedState);
    }
    else {
        l.size1   = m_shared_state->m_transact_log_used_end - l.offset1;
        l.offset2 = l.size2 = 0;
    }
    return true;
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
            m_shared_state->m_cond_transact_log_free.wait(lg);
            if (m_shared_state_shutdown) return ERROR_INTERRUPTED;
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
    // FIXME: During write access, it should be possible to used m_mapped_size instead of m_shared_state->m_size.
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

            m_shared_state->m_cond_transact_log_free.wait(lg);
            if (m_shared_state_shutdown) return ERROR_INTERRUPTED;
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
            if (add_with_overflow_detect(min_size, used_lower)) return ERROR_NO_RESOURCE;
            const size_t avail_lower = used_begin - buffer_begin;
            if (avail_lower <= free) { // Require one unused byte
                if (add_with_overflow_detect(min_size, free)) return ERROR_NO_RESOURCE;
            }
        }
        else {
            // Move upper section
            min_size = used_end + 1 + used_upper; // Require one unused byte
            if (add_with_overflow_detect(min_size, free)) return ERROR_NO_RESOURCE;
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
        if (add_with_overflow_detect(min_size, free)) return ERROR_NO_RESOURCE;
    }

    size_t new_size = m_shared_state->m_size;
    if (mul_with_overflow_detect(new_size, size_t(2))) {
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
    assert(r == 0);
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
        if (mul_with_overflow_detect(new_size, size_t(2))) return ERROR_NO_RESOURCE;
        if (!m_subtab_path_buf.set_size(new_size)) return ERROR_OUT_OF_MEMORY;
    }
    char* buf;
    const int max_elems_per_chunk = 8;
    error_code err = transact_log_reserve(&buf, 1 + (1+max_elems_per_chunk)*max_enc_bytes_per_int);
    if (err) return err;
    *buf++ = 'T';
    assert(1 <= end - begin);
    const ptrdiff_t level = (end - begin)/2;
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
    check_table(table);
    size_t* begin;
    size_t* end;
    for (;;) {
        begin = m_subtab_path_buf.m_data;
        end = table->record_subspec_path(spec, begin, begin+m_subtab_path_buf.m_size);
        if (end) break;
        size_t new_size = m_subtab_path_buf.m_size;
        if (mul_with_overflow_detect(new_size, size_t(2))) return ERROR_NO_RESOURCE;
        if (!m_subtab_path_buf.set_size(new_size)) return ERROR_OUT_OF_MEMORY;
    }
    char* buf;
    const int max_elems_per_chunk = 8;
    error_code err = transact_log_reserve(&buf, 1 + (1+max_elems_per_chunk)*max_enc_bytes_per_int);
    if (err) return err;
    *buf++ = 'S';
    assert(1 <= end - begin);
    const ptrdiff_t level = end - begin - 1;
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
    m_selected_spec = spec;
    return ERROR_NONE;
}


bool Replication::Buffer::set_size(std::size_t new_size)
{
    size_t* new_data = new (nothrow) size_t[new_size];
    if (!new_data) return false;
    delete[] m_data;
    m_data = new_data;
    m_size = new_size;
    return true;
}


// FIXME: This one can be moved into the class
error_code Replication::SharedState::init(size_t file_size)
{
    m_want_write_transact = 0;
    m_write_transact_available = false;
    m_write_transact_finished  = false;
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
    cdg3.release();
    cdg2.release();
    cdg1.release();
    mdg.release();
    return ERROR_NONE;
}


void Replication::SharedState::destroy()
{
    m_cond_want_write_transact.destroy();
    m_cond_write_transact_available.destroy();
    m_cond_write_transact_finished.destroy();
    m_cond_transact_log_free.destroy();
    m_mutex.destroy();
}


} // namespace tightdb

#endif // TIGHTDB_ENABLE_REPLICATION
