#include <utility>
#include <ostream>
#include <iomanip>

#include <tightdb/terminate.hpp>
#include <tightdb/safe_int_ops.hpp>
#include <tightdb/string_buffer.hpp>
#include <tightdb/pthread_helpers.hpp>
#include <tightdb/unique_ptr.hpp>
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

// SharedState size must fit in size_t, ptrdiff_t, and off_t
typedef ArithBinOpType<ptrdiff_t, off_t>::type ptrdiff_off_type;
typedef ArithBinOpType<ptrdiff_off_type, size_t>::type unsigned_max_type;
const unsigned_max_type max_file_size = min<unsigned_max_type>(numeric_limits<ptrdiff_t>::max(),
                                                               numeric_limits<off_t>::max());

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

    void init(size_t file_size)
    {
        m_want_write_transact = 0;
        m_write_transact_available = false;
        m_write_transact_finished  = false;
        m_persisted_db_version = 0;
        m_size = file_size;
        m_transact_log_used_begin = sizeof(SharedState);
        m_transact_log_used_end = m_transact_log_used_begin;

        m_mutex.init_shared();
        Mutex::DestroyGuard mdg(m_mutex);

        m_cond_want_write_transact.init_shared();
        Condition::DestroyGuard cdg1(m_cond_want_write_transact);

        m_cond_write_transact_available.init_shared();
        Condition::DestroyGuard cdg2(m_cond_write_transact_available);

        m_cond_write_transact_finished.init_shared();
        Condition::DestroyGuard cdg3(m_cond_write_transact_finished);

        m_cond_transact_log_free.init_shared();
        Condition::DestroyGuard cdg4(m_cond_transact_log_free);

        m_cond_persisted_db_version.init_shared();

        cdg4.release();
        cdg3.release();
        cdg2.release();
        cdg1.release();
        mdg.release();
    }

    void destroy() TIGHTDB_NOEXCEPT
    {
        m_cond_want_write_transact.destroy();
        m_cond_write_transact_available.destroy();
        m_cond_write_transact_finished.destroy();
        m_cond_transact_log_free.destroy();
        m_cond_persisted_db_version.destroy();
        m_mutex.destroy();
    }
};


void Replication::open(const string& file, bool map_transact_log_buf)
{
    string repl_file = file.empty() ? get_path_to_database_file() : file;
    repl_file += ".repl"; // Throws
    m_subtab_path_buf.set_size(init_subtab_path_buf_size); // Throws
    m_file.open(repl_file, File::access_ReadWrite, File::create_Auto, 0); // Throws
    File::CloseGuard fcg(m_file);
    {
        File::ExclusiveLock efl(m_file); // Throws
        // If empty, expand its size
        size_t file_size;
        if (int_cast_with_overflow_detect(m_file.get_size(), file_size))
            throw runtime_error("File too large");
        if (file_size == 0) {
            file_size = sizeof(SharedState) + initial_transact_log_buffer_size;
            m_file.resize(file_size); // Throws
        }
        const size_t map_size = map_transact_log_buf ? file_size : sizeof(SharedState);
        m_file_map.map(m_file, File::access_ReadWrite, map_size); // Throws
        SharedState* const shared_state = m_file_map.get_addr();
        if (shared_state->m_use_count == 0) {
            File::UnmapGuard fug(m_file_map);
            shared_state->init(file_size); // Throws
            fug.release();  // Do not unmap
        }
        ++shared_state->m_use_count;
    }
    fcg.release(); // Do not close
}


void Replication::remap_file(size_t size)
{
    m_file_map.remap(m_file, File::access_ReadWrite, size); // Throws
}


Replication::~Replication() TIGHTDB_NOEXCEPT
{
    if (!is_attached()) return;

    try {
        File::ExclusiveLock efl(m_file);
        SharedState* const shared_state = m_file_map.get_addr();
        if (--shared_state->m_use_count == 0) {
            shared_state->destroy();
            m_file.resize(0);
        }
    }
    catch (...) {} // Deliberately ignoring errors here
}


void Replication::interrupt() TIGHTDB_NOEXCEPT
{
    SharedState* const shared_state = m_file_map.get_addr();
    Mutex::Lock ml(shared_state->m_mutex);
    m_interrupt = true;
    shared_state->m_cond_want_write_transact.notify_all();
    shared_state->m_cond_write_transact_available.notify_all();
    shared_state->m_cond_write_transact_finished.notify_all();
    shared_state->m_cond_transact_log_free.notify_all();
    shared_state->m_cond_persisted_db_version.notify_all();
}


void Replication::begin_write_transact()
{
    size_t file_size, transact_log_used_begin, transact_log_used_end;
    {
        SharedState* const shared_state = m_file_map.get_addr();
        Mutex::Lock ml(shared_state->m_mutex);
        ++shared_state->m_want_write_transact;
        shared_state->m_cond_want_write_transact.notify_all();
        while (!shared_state->m_write_transact_available) {
            if (m_interrupt) {
                // FIXME: Retracting the request for a write transaction may create problems fo the local coordinator
                --shared_state->m_want_write_transact;
                throw Interrupted();
            }
            shared_state->m_cond_write_transact_available.wait(ml);
        }
        shared_state->m_write_transact_available = false;
        --shared_state->m_want_write_transact;
        file_size = shared_state->m_size;
        transact_log_used_begin = shared_state->m_transact_log_used_begin;
        transact_log_used_end   = shared_state->m_transact_log_used_end;
        m_write_transact_db_version = shared_state->m_write_transact_db_version;
    }
    // At this point we know that the file size cannot change because
    // this cleint is the only one who may change it.
    TIGHTDB_ASSERT(m_file_map.get_size() <= file_size);
    if (m_file_map.get_size() < file_size) {
        try {
            remap_file(file_size);
        }
        catch (...) {
            rollback_write_transact();
            throw;
        }
    }
    char* const base = static_cast<char*>(static_cast<void*>(m_file_map.get_addr()));
    m_transact_log_free_begin = base + transact_log_used_end;
    if (transact_log_used_end < transact_log_used_begin) {
        // Used area is wrapped. We subtract one from
        // transact_log_used_begin to avoid using the last free byte
        // so we can distinguish between full and empty buffer.
        m_transact_log_free_end = base + transact_log_used_begin - 1;
    }
    else {
        m_transact_log_free_end = base + m_file_map.get_size();
    }
    m_selected_table = 0;
    m_selected_spec  = 0;
}


void Replication::commit_write_transact()
{
    SharedState* const shared_state = m_file_map.get_addr();
    Mutex::Lock ml(shared_state->m_mutex);
    shared_state->m_transact_log_new_begin = shared_state->m_transact_log_used_end;
    shared_state->m_transact_log_used_end =
        m_transact_log_free_begin - static_cast<char*>(static_cast<void*>(shared_state));
    shared_state->m_write_transact_finished = true;
    shared_state->m_cond_write_transact_finished.notify_all();

    // Wait for the transaction log to be made persistent
    while (shared_state->m_persisted_db_version < m_write_transact_db_version) {
        if (m_interrupt) throw Interrupted();
        shared_state->m_cond_persisted_db_version.wait(ml);
    }
}


void Replication::rollback_write_transact() TIGHTDB_NOEXCEPT
{
    SharedState* const shared_state = m_file_map.get_addr();
    Mutex::Lock ml(shared_state->m_mutex);
    shared_state->m_transact_log_new_begin = shared_state->m_transact_log_used_end;
    shared_state->m_write_transact_finished = true;
    shared_state->m_cond_write_transact_finished.notify_all();
}


void Replication::clear_interrupt() TIGHTDB_NOEXCEPT
{
    SharedState* const shared_state = m_file_map.get_addr();
    Mutex::Lock ml(shared_state->m_mutex);
    m_interrupt = false;
}


bool Replication::wait_for_write_request() TIGHTDB_NOEXCEPT
{
    SharedState* const shared_state = m_file_map.get_addr();
    Mutex::Lock ml(shared_state->m_mutex);
    while (shared_state->m_want_write_transact == 0) {
        if (m_interrupt) return false;
        shared_state->m_cond_want_write_transact.wait(ml);
    }
    return true;
}


// FIXME: Consider what should happen if nobody remains interested in this write transaction
bool Replication::grant_write_access_and_wait_for_completion(TransactLog& transact_log) TIGHTDB_NOEXCEPT
{
    SharedState* const shared_state = m_file_map.get_addr();
    Mutex::Lock ml(shared_state->m_mutex);
    shared_state->m_write_transact_db_version = transact_log.m_db_version;
    shared_state->m_write_transact_available = true;
    shared_state->m_cond_write_transact_available.notify_all();
    while (!shared_state->m_write_transact_finished) {
        if (m_interrupt) return false;
        shared_state->m_cond_write_transact_finished.wait(ml);
    }
    shared_state->m_write_transact_finished = false;
    transact_log.m_offset1 = shared_state->m_transact_log_new_begin;
    if (shared_state->m_transact_log_used_end < shared_state->m_transact_log_new_begin) {
        transact_log.m_size1   = shared_state->m_transact_log_used_wrap - transact_log.m_offset1;
        transact_log.m_offset2 = sizeof(SharedState);
        transact_log.m_size2   = shared_state->m_transact_log_used_end - sizeof(SharedState);
    }
    else {
        transact_log.m_size1   = shared_state->m_transact_log_used_end - transact_log.m_offset1;
        transact_log.m_offset2 = transact_log.m_size2 = 0;
    }
    return true;
}


void Replication::map_transact_log(const TransactLog& transact_log,
                                   const char** addr1, const char** addr2)
{
    const size_t mapped_size = m_file_map.get_size();
    if (mapped_size < transact_log.m_offset1+transact_log.m_size1 ||
        mapped_size < transact_log.m_offset2+transact_log.m_size2) {
        size_t file_size;
        {
            SharedState* const shared_state = m_file_map.get_addr();
            Mutex::Lock ml(shared_state->m_mutex);
            file_size = shared_state->m_size;
        }
        remap_file(file_size); // Throws
    }
    SharedState* const shared_state = m_file_map.get_addr();
    *addr1 = static_cast<char*>(static_cast<void*>(shared_state)) + transact_log.m_offset1;
    *addr2 = static_cast<char*>(static_cast<void*>(shared_state)) + transact_log.m_offset2;
}


void Replication::update_persisted_db_version(db_version_type version) TIGHTDB_NOEXCEPT
{
    SharedState* const shared_state = m_file_map.get_addr();
    Mutex::Lock ml(shared_state->m_mutex);
    shared_state->m_persisted_db_version = version;
    shared_state->m_cond_persisted_db_version.notify_all();
}


void Replication::transact_log_consumed(size_t size) TIGHTDB_NOEXCEPT
{
    SharedState* const shared_state = m_file_map.get_addr();
    Mutex::Lock ml(shared_state->m_mutex);
    if (shared_state->m_transact_log_used_end < shared_state->m_transact_log_used_begin) {
        // Used area is wrapped
        size_t contig = shared_state->m_transact_log_used_wrap -
            shared_state->m_transact_log_used_begin;
        if (contig < size) {
            shared_state->m_transact_log_used_begin = sizeof(SharedState);
            size -= contig;
        }
    }
    shared_state->m_transact_log_used_begin += size;
    shared_state->m_cond_transact_log_free.notify_all();
}


void Replication::transact_log_reserve_contig(size_t n)
{
    SharedState* const shared_state = m_file_map.get_addr();
    const size_t used_end = m_transact_log_free_begin -
        static_cast<char*>(static_cast<void*>(shared_state));
    {
        Mutex::Lock ml(shared_state->m_mutex);
        for (;;) {
            const size_t used_begin = shared_state->m_transact_log_used_begin;
            if (used_begin <= used_end) {
                // In this case the used area is not wrapped across
                // the end of the buffer. This means that the free
                // area extends all the way to the end of the buffer.
                const size_t avail = shared_state->m_size - used_end;
                if (n <= avail) {
                    m_transact_log_free_end = m_transact_log_free_begin + avail;
                    return;
                }
                // Check if there is there enough space if we wrap the
                // used area at this point and continue at the
                // beginning of the buffer. Note that we again require
                // one unused byte.
                const size_t avail2 = used_begin - sizeof(SharedState);
                if (n < avail2) {
                    shared_state->m_transact_log_used_wrap = used_end;
                    m_transact_log_free_begin =
                        static_cast<char*>(static_cast<void*>(shared_state)) +
                        sizeof(SharedState);
                    m_transact_log_free_end = m_transact_log_free_begin + avail2;
                    return;
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
                    return;
                }
            }
            // At this point we know that the transaction log buffer
            // does not contain a contiguous unused regioun of size
            // 'n' or more. If the buffer contains other transaction
            // logs than the one we are currently creating, more space
            // will eventually become available as those transaction
            // logs gets transmitted to other clients. So in that case
            // we will simply wait.
            if (shared_state->m_transact_log_used_begin ==
                shared_state->m_transact_log_used_end) break;
            if (m_interrupt) throw Interrupted();
            shared_state->m_cond_transact_log_free.wait(ml);
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

    transact_log_expand(n, true); // Throws
}


void Replication::transact_log_append_overflow(const char* data, std::size_t size)
{
    // FIXME: During write access, it should be possible to use m_file_map.get_size() instead of SharedState::m_size.
    bool need_expand = false;
    {
        SharedState* const shared_state = m_file_map.get_addr();
        char* const base = static_cast<char*>(static_cast<void*>(shared_state));
        const size_t used_end = m_transact_log_free_begin - base;
        Mutex::Lock ml(shared_state->m_mutex);
        for (;;) {
            const size_t used_begin = shared_state->m_transact_log_used_begin;
            if (used_begin <= used_end) {
                // In this case the used area is not wrapped across
                // the end of the buffer.
                size_t avail = shared_state->m_size - used_end;
                // Require one unused byte.
                if (sizeof(SharedState) < used_begin) avail += used_begin - sizeof(SharedState) - 1; // FIXME: Use static const memeber
                if (size <= avail) {
                    m_transact_log_free_end = base + shared_state->m_size;
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

            if (shared_state->m_transact_log_used_begin == shared_state->m_transact_log_used_end) {
                need_expand = true;
                break;
            }

            if (m_interrupt) throw Interrupted();
            shared_state->m_cond_transact_log_free.wait(ml);
        }
    }
    if (need_expand) {
        // We know at this point that no one else is trying to access
        // the transaction log buffer.
        transact_log_expand(size, false); // Throws
    }
    const size_t contig = m_transact_log_free_end - m_transact_log_free_begin;
    if (contig < size) {
        copy(data, data+contig, m_transact_log_free_begin);
        data += contig;
        size -= contig;
        SharedState* const shared_state = m_file_map.get_addr();
        char* const base = static_cast<char*>(static_cast<void*>(shared_state));
        m_transact_log_free_begin = base + sizeof(SharedState);
        {
            Mutex::Lock ml(shared_state->m_mutex);
            shared_state->m_transact_log_used_wrap = shared_state->m_size;
            m_transact_log_free_end = base + (shared_state->m_transact_log_used_begin - 1);
        }
    }
    m_transact_log_free_begin = copy(data, data + size, m_transact_log_free_begin);
}


void Replication::transact_log_expand(size_t free, bool contig)
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
    SharedState* shared_state = m_file_map.get_addr();
    const size_t buffer_begin = sizeof(SharedState);
    const size_t used_begin = shared_state->m_transact_log_used_begin;
    const size_t used_end = m_transact_log_free_begin -
        static_cast<char*>(static_cast<void*>(shared_state));
    const size_t used_wrap = shared_state->m_transact_log_used_wrap;
    size_t min_size, new_size;
    if (used_end < used_begin) {
        // Used area is wrapped
        const size_t used_upper = used_wrap - used_begin;
        const size_t used_lower = used_end - buffer_begin;
        if (used_lower < used_upper) {
            // Move lower section
            min_size = used_wrap;
            if (int_add_with_overflow_detect(min_size, used_lower)) goto transact_log_too_big;
            const size_t avail_lower = used_begin - buffer_begin;
            if (avail_lower <= free) { // Require one unused byte
                if (int_add_with_overflow_detect(min_size, free)) goto transact_log_too_big;
            }
        }
        else {
            // Move upper section
            min_size = used_end + 1 + used_upper; // Require one unused byte
            if (int_add_with_overflow_detect(min_size, free)) goto transact_log_too_big;
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
        if (int_add_with_overflow_detect(min_size, free)) goto transact_log_too_big;
    }

    new_size = shared_state->m_size;
    if (int_multiply_with_overflow_detect(new_size, 2)) {
        new_size = numeric_limits<size_t>::max();
    }
    if (new_size < min_size) new_size = min_size;

    // Check that the new size fits in both ptrdiff_t and off_t (file size)
    if (max_file_size < new_size) {
        if (max_file_size < min_size) {
          transact_log_too_big:
            throw runtime_error("Transaction log too big");
        }
        new_size = max_file_size;
    }

    m_file.resize(new_size); // Throws
    shared_state->m_size = new_size;

    remap_file(new_size); // Throws
    shared_state = m_file_map.get_addr();

    // Rearrange the buffer contents
    char* base = static_cast<char*>(static_cast<void*>(shared_state));
    if (used_end < used_begin) {
        // Used area is wrapped
        const size_t used_upper = used_wrap - used_begin;
        const size_t used_lower = used_end - buffer_begin;
        if (used_lower < used_upper) {
            // Move lower section
            copy(base+buffer_begin, base+used_end, base+used_wrap);
            if (shared_state->m_transact_log_used_end < used_begin)
                shared_state->m_transact_log_used_end += used_wrap - buffer_begin;
            if (contig && new_size - (used_wrap + used_lower) < free) {
                shared_state->m_transact_log_used_wrap = used_wrap + used_lower;
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
            shared_state->m_transact_log_used_begin = new_size - used_upper;
            if (used_begin <= shared_state->m_transact_log_used_end)
                shared_state->m_transact_log_used_end +=
                    shared_state->m_transact_log_used_begin - used_begin;
            shared_state->m_transact_log_used_wrap = new_size;
            m_transact_log_free_begin = base + used_end;
            // Require one unused byte
            m_transact_log_free_end = base + (shared_state->m_transact_log_used_begin - 1);
        }
    }
    else {
        // Used area is not wrapped
        m_transact_log_free_begin = base + used_end;
        m_transact_log_free_end   = base + new_size;
    }
}


void Replication::select_table(const Table* table)
{
    size_t* begin;
    size_t* end;
    for (;;) {
        begin = m_subtab_path_buf.m_data;
        end = table->record_subtable_path(begin, begin+m_subtab_path_buf.m_size);
        if (end) break;
        size_t new_size = m_subtab_path_buf.m_size;
        if (int_multiply_with_overflow_detect(new_size, 2))
            throw runtime_error("To many subtable nesting levels");
        m_subtab_path_buf.set_size(new_size); // Throws
    }
    char* buf;
    const int max_elems_per_chunk = 8; // FIXME: Use smaller number when compiling in debug mode
    transact_log_reserve(&buf, 1 + (1+max_elems_per_chunk)*max_enc_bytes_per_int); // Throws
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
        transact_log_reserve(&buf, max_elems_per_chunk*max_enc_bytes_per_int); // Throws
    }

good:
    transact_log_advance(buf);
    m_selected_spec  = 0;
    m_selected_table = table;
}


void Replication::select_spec(const Table* table, const Spec* spec)
{
    check_table(table);
    size_t* begin;
    size_t* end;
    for (;;) {
        begin = m_subtab_path_buf.m_data;
        end = table->record_subspec_path(spec, begin, begin+m_subtab_path_buf.m_size);
        if (end) break;
        size_t new_size = m_subtab_path_buf.m_size;
        if (int_multiply_with_overflow_detect(new_size, 2))
            throw runtime_error("To many subspec nesting levels");
        m_subtab_path_buf.set_size(new_size); // Throws
    }
    char* buf;
    const int max_elems_per_chunk = 8; // FIXME: Use smaller number when compiling in debug mode
    transact_log_reserve(&buf, 1 + (1+max_elems_per_chunk)*max_enc_bytes_per_int); // Throws
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
        transact_log_reserve(&buf, max_elems_per_chunk*max_enc_bytes_per_int); // Throws
    }

good:
    transact_log_advance(buf);
    m_selected_spec = spec;
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

    void apply();

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

    // Returns false if no more input was available
    bool fill_input_buffer()
    {
        const size_t n = m_input.read(m_input_buffer, m_input_buffer_size);
        if (n == 0) return false;
        m_input_begin = m_input_buffer;
        m_input_end   = m_input_buffer + n;
        return true;
    }

    // Returns false if no input was available
    bool read_char(char& c)
    {
        if (m_input_begin == m_input_end && !fill_input_buffer()) return false;
        c = *m_input_begin++;
        return true;
    }

    template<class T> T read_int();

    void read_string(StringBuffer&);

    void add_subspec(Spec*);

    template<bool insert> void set_or_insert(int column_ndx, size_t ndx);

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
        case type_Int:
        case type_Bool:
        case type_Date:
        case type_String:
        case type_Binary:
        case type_Table:
        case type_Mixed: return true;
        default: break;
        }
        return false;
    }
};


template<class T> T Replication::TransactLogApplier::read_int()
{
    T value = 0;
    int part;
    const int max_bytes = (std::numeric_limits<T>::digits+1+6)/7;
    for (int i=0; i<max_bytes; ++i) {
        char c;
        if (!read_char(c)) goto bad_transact_log;
        part = static_cast<unsigned char>(c);
        if (0xFF < part) goto bad_transact_log; // Only the first 8 bits may be used in each byte
        if ((part & 0x80) == 0) {
            T p = part & 0x3F;
            if (int_shift_left_with_overflow_detect(p, i*7)) goto bad_transact_log;
            value |= p;
            break;
        }
        if (i == max_bytes-1) goto bad_transact_log; // Two many bytes
        value |= T(part & 0x7F) << (i*7);
    }
    if (part & 0x40) {
        // The real value is negative. Because 'value' is positive at
        // this point, the following negation is guaranteed by the
        // standard to never overflow.
        value = -value;
        if (int_subtract_with_overflow_detect(value, 1)) goto bad_transact_log;
    }
    return value;

  bad_transact_log:
    throw BadTransactLog();
}


void Replication::TransactLogApplier::read_string(StringBuffer& buf)
{
    buf.clear();
    size_t size = read_int<size_t>(); // Throws
    buf.resize(size); // Throws
    char* str_end = buf.data();
    for (;;) {
        const size_t avail = m_input_end - m_input_begin;
        if (size <= avail) break;
        const char* to = m_input_begin + avail;
        copy(m_input_begin, to, str_end);
        if (!fill_input_buffer()) throw BadTransactLog();
        str_end += avail;
        size -= avail;
    }
    const char* to = m_input_begin + size;
    copy(m_input_begin, to, str_end);
    m_input_begin = to;
}


void Replication::TransactLogApplier::add_subspec(Spec* spec)
{
    if (m_num_subspecs == m_subspecs.m_size) {
        Buffer<Spec*> new_subspecs;
        size_t new_size = m_subspecs.m_size;
        if (new_size == 0) {
            new_size = 16; // FIXME: Use a small value (1) when compiling in debug mode
        }
        else {
            if (int_multiply_with_overflow_detect(new_size, 2))
                throw runtime_error("To many subspec nesting levels");
        }
        new_subspecs.set_size(new_size); // Throws
        copy(m_subspecs.m_data, m_subspecs.m_data+m_num_subspecs, new_subspecs.m_data);
        using std::swap;
        swap(m_subspecs, new_subspecs);
    }
    m_subspecs[m_num_subspecs++] = spec;
}


template<bool insert>
void Replication::TransactLogApplier::set_or_insert(int column_ndx, size_t ndx)
{
    switch (m_table->get_column_type(column_ndx)) {
    case type_Int:
        {
            int64_t value = read_int<int64_t>(); // Throws
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
    case type_Bool:
        {
            bool value = read_int<bool>(); // Throws
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
    case type_Date:
        {
            time_t value = read_int<time_t>(); // Throws
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
    case type_String:
        {
            read_string(m_string_buffer); // Throws
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
    case type_Binary:
        {
            read_string(m_string_buffer); // Throws
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
    case type_Table:
        if (insert) m_table->insert_subtable(column_ndx, ndx); // FIXME: Memory allocation failure!!!
        else m_table->clear_subtable(column_ndx, ndx); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
        if (m_log) {
            if (insert) *m_log << "table->insert_subtable("<<column_ndx<<", "<<ndx<<")\n";
            else *m_log << "table->clear_subtable("<<column_ndx<<", "<<ndx<<")\n";
        }
#endif
        break;
    case type_Mixed:
        {
            int type = read_int<int>(); // Throws
            switch (type) {
            case type_Int:
                {
                    int64_t value = read_int<int64_t>(); // Throws
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
            case type_Bool:
                {
                    bool value = read_int<bool>(); // Throws
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
            case type_Date:
                {
                    time_t value = read_int<time_t>(); // Throws
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
            case type_String:
                {
                    read_string(m_string_buffer); // Throws
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
            case type_Binary:
                {
                    read_string(m_string_buffer); // Throws
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
            case type_Table:
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
                throw BadTransactLog();
            }
        }
        break;
    default:
        throw BadTransactLog();
    }
}


void Replication::TransactLogApplier::apply()
{
    if (!m_input_buffer) m_input_buffer = new char[m_input_buffer_size]; // Throws
    m_input_begin = m_input_end = m_input_buffer;

    // FIXME: Problem: The modifying methods of group, table, and spec generally throw.
    Spec* spec = 0;
    for (;;) {
        char instr;
        if (!read_char(instr)) break;
// cerr << "["<<instr<<"]";
        switch (instr) {
            case 's': { // Set value
                if (m_dirty_spec) finalize_spec();
                int column_ndx = read_int<int>(); // Throws
                size_t ndx = read_int<size_t>(); // Throws
                if (!m_table) goto bad_transact_log;
                if (column_ndx < 0 || int(m_table->get_column_count()) <= column_ndx)
                    goto bad_transact_log;
                if (m_table->size() <= ndx) goto bad_transact_log;
                const bool insert = false;
                set_or_insert<insert>(column_ndx, ndx); // Throws
                break;
            }

            case 'i': { // Insert value
                if (m_dirty_spec) finalize_spec();
                int column_ndx = read_int<int>(); // Throws
                size_t ndx = read_int<size_t>(); // Throws
                if (!m_table) goto bad_transact_log;
                if (column_ndx < 0 || int(m_table->get_column_count()) <= column_ndx)
                    goto bad_transact_log;
                if (m_table->size() < ndx) goto bad_transact_log;
                const bool insert = true;
                set_or_insert<insert>(column_ndx, ndx); // Throws
                break;
            }

            case 'c': { // Row insert complete
                if (m_dirty_spec) finalize_spec();
                if (!m_table) goto bad_transact_log;
                m_table->insert_done(); // FIXME: May fail
#ifdef TIGHTDB_DEBUG
                if (m_log) *m_log << "table->insert_done()\n";
#endif
                break;
            }

            case 'I': { // Insert empty rows
                if (m_dirty_spec) finalize_spec();
                size_t ndx = read_int<size_t>(); // Throws
                size_t num_rows = read_int<size_t>(); // Throws
                if (!m_table || m_table->size() < ndx) goto bad_transact_log;
                m_table->insert_empty_row(ndx, num_rows); // FIXME: May fail
#ifdef TIGHTDB_DEBUG
                if (m_log) *m_log << "table->insert_empty_row("<<ndx<<", "<<num_rows<<")\n";
#endif
                break;
            }

            case 'R': { // Remove row
                if (m_dirty_spec) finalize_spec();
                size_t ndx = read_int<size_t>(); // Throws
                if (!m_table || m_table->size() < ndx) goto bad_transact_log;
                m_table->remove(ndx); // FIXME: May fail
#ifdef TIGHTDB_DEBUG
                if (m_log) *m_log << "table->remove("<<ndx<<")\n";
#endif
                break;
            }

            case 'a': { // Add int to column
                if (m_dirty_spec) finalize_spec();
                int column_ndx = read_int<int>(); // Throws
                if (!m_table) goto bad_transact_log;
                if (column_ndx < 0 || int(m_table->get_column_count()) <= column_ndx)
                    goto bad_transact_log;
                int64_t value = read_int<int64_t>(); // Throws
                m_table->add_int(column_ndx, value); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
                if (m_log) *m_log << "table->add_int("<<column_ndx<<", "<<value<<")\n";
#endif
                break;
            }

            case 'T': { // Select table
                if (m_dirty_spec) finalize_spec();
                int levels = read_int<int>(); // Throws
                size_t ndx = read_int<size_t>(); // Throws
                if (m_group.size() <= ndx) goto bad_transact_log;
                m_table = m_group.get_table_ptr(ndx)->get_table_ref();
#ifdef TIGHTDB_DEBUG
                if (m_log) *m_log << "table = group->get_table_by_ndx("<<ndx<<")\n";
#endif
                spec = 0;
                for (int i=0; i<levels; ++i) {
                    int column_ndx = read_int<int>(); // Throws
                    ndx = read_int<size_t>(); // Throws
                    if (column_ndx < 0 || int(m_table->get_column_count()) <= column_ndx)
                        goto bad_transact_log;
                    if (m_table->size() <= ndx) goto bad_transact_log;
                    switch (m_table->get_column_type(column_ndx)) {
                    case type_Table:
                        m_table = m_table->get_subtable(column_ndx, ndx);
                        break;
                    case type_Mixed:
                        m_table = m_table->get_subtable(column_ndx, ndx);
                        if (!m_table) goto bad_transact_log;
                        break;
                    default:
                        goto bad_transact_log;
                    }
#ifdef TIGHTDB_DEBUG
                    if (m_log) *m_log << "table = table->get_subtable("<<column_ndx<<", "<<ndx<<")\n";
#endif
                }
                break;
            }

            case 'C': { // Clear table
                if (m_dirty_spec) finalize_spec();
                if (!m_table) goto bad_transact_log;
                m_table->clear(); // FIXME: Can probably fail!
#ifdef TIGHTDB_DEBUG
                if (m_log) *m_log << "table->clear()\n";
#endif
                break;
            }

            case 'x': { // Add index to column
                if (m_dirty_spec) finalize_spec();
                int column_ndx = read_int<int>(); // Throws
                if (!m_table) goto bad_transact_log;
                if (column_ndx < 0 || int(m_table->get_column_count()) <= column_ndx)
                    goto bad_transact_log;
                m_table->set_index(column_ndx); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
                if (m_log) *m_log << "table->set_index("<<column_ndx<<")\n";
#endif
                break;
            }

            case 'A': { // Add column to selected spec
                int type = read_int<int>(); // Throws
                if (!is_valid_column_type(type)) goto bad_transact_log;
                read_string(m_string_buffer); // Throws
                StringData name(m_string_buffer.data(), m_string_buffer.size());
                if (!spec) goto bad_transact_log;
                // FIXME: Is it legal to have multiple columns with the same name?
                if (spec->get_column_index(name) != size_t(-1)) goto bad_transact_log;
                spec->add_column(DataType(type), name);
#ifdef TIGHTDB_DEBUG
                if (m_log) *m_log << "spec->add_column("<<type<<", \""<<name<<"\")\n";
#endif
                m_dirty_spec = true;
                break;
            }

            case 'S': { // Select spec for currently selected table
                delete_subspecs();
                if (!m_table) goto bad_transact_log;
                spec = &m_table->get_spec();
#ifdef TIGHTDB_DEBUG
                if (m_log) *m_log << "spec = table->get_spec()\n";
#endif
                int levels = read_int<int>(); // Throws
                for (int i=0; i<levels; ++i) {
                    int subspec_ndx = read_int<int>(); // Throws
                    if (subspec_ndx < 0 || int(spec->get_num_subspecs()) <= subspec_ndx)
                        goto bad_transact_log;
                    UniquePtr<Spec> spec2(new Spec(spec->get_subspec_by_ndx(subspec_ndx)));
                    add_subspec(spec2.get());
                    spec = spec2.release();
#ifdef TIGHTDB_DEBUG
                    if (m_log) *m_log << "spec = spec->get_subspec_by_ndx("<<subspec_ndx<<")\n";
#endif
                }
                break;
            }

            case 'N': { // New top level table
                read_string(m_string_buffer); // Throws
                StringData name(m_string_buffer.data(), m_string_buffer.size());
                if (m_group.has_table(name)) goto bad_transact_log;
                m_group.create_new_table(name); // Throws
#ifdef TIGHTDB_DEBUG
                if (m_log) *m_log << "group->create_new_table(\""<<name<<"\")\n";
#endif
                break;
            }

            case 'Z': { // Optimize table
                if (m_dirty_spec) finalize_spec();
                if (!m_table) goto bad_transact_log;
                m_table->optimize(); // FIXME: May fail
#ifdef TIGHTDB_DEBUG
                if (m_log) *m_log << "table->optimize()\n";
#endif
                break;
            }

        default:
            goto bad_transact_log;
        }
    }

    if (m_dirty_spec) finalize_spec(); // FIXME: Why is this necessary?
    return;

  bad_transact_log:
    throw BadTransactLog();
}


#ifdef TIGHTDB_DEBUG
void Replication::apply_transact_log(InputStream& transact_log, Group& group, ostream* log)
{
    TransactLogApplier applier(transact_log, group);
    applier.set_apply_log(log);
    applier.apply(); // Throws
}
#else
void Replication::apply_transact_log(InputStream& transact_log, Group& group)
{
    TransactLogApplier applier(transact_log, group);
    applier.apply(); // Throws
}
#endif


} // namespace tightdb
