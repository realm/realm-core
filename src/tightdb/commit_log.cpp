/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2014] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/

#include <exception>

#include <tightdb/util/unique_ptr.hpp>
#include <tightdb/util/thread.hpp>
#include <tightdb/util/file.hpp>
#include <tightdb/group_shared.hpp>
#include <map>
#include <tightdb/commit_log.hpp>

#include <tightdb/replication.hpp>
#ifdef TIGHTDB_ENABLE_REPLICATION

typedef uint_fast64_t version_type;

namespace tightdb {


namespace _impl {
using namespace util;
using namespace std;

// Design of the commit logs:
//
// We use two files to hold the commit logs. Using two files (instead of one) allows us to append data to the
// end of one of the files, instead of doing complex memory management. Initially, both files
// hold only a header, and one of them is designated 'active'. New commit logs are appended
// to the active file. Each file holds a consecutive range of commits, the active file holding
// the latest commits. A commit log entry is never split between the files.
//
// Calls to set_oldest_version_needed() checks if the non-active file holds stale commit logs only.
// If so, the non-active file is reset and becomes active instead.
//
// Filesizes are determined by heuristics. When a file runs out of space, its size is doubled.
// When changing the active file, the total amount memory that can be reached is computed,
// and if it is below 1/8 of the current filesize, the file is truncated to half its old size.
// the intention is to strike a balance between shrinking the files, when they are much bigger
// than needed, while at the same time avoiding many repeated shrinks and expansions.
//
// Calls to get_commit_entries determines which file(s) needs to be accessed, maps them to
// memory and builds a vector of BinaryData with pointers to the buffers. The pointers may
// end up going to both mappings/files.
//
// Access to the commit-logs metadata is protected by an inter-process mutex.
//
// FIXME: we should not use size_t for memory mapped members, but one where the size is
// guaranteed

class WriteLogCollector : public Replication
{
public:
    WriteLogCollector(std::string database_name);
    ~WriteLogCollector() TIGHTDB_NOEXCEPT;
    std::string do_get_database_path() TIGHTDB_OVERRIDE { return m_database_name; }
    void do_begin_write_transact(SharedGroup& sg) TIGHTDB_OVERRIDE;
    version_type do_commit_write_transact(SharedGroup& sg, version_type orig_version) TIGHTDB_OVERRIDE;
    void do_rollback_write_transact(SharedGroup& sg) TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    void do_interrupt() TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE {};
    void do_clear_interrupt() TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE {};
    void do_transact_log_reserve(std::size_t sz) TIGHTDB_OVERRIDE;
    void do_transact_log_append(const char* data, std::size_t size) TIGHTDB_OVERRIDE;
    void transact_log_reserve(std::size_t n);
    version_type internal_submit_log(const char*, uint64_t);
    virtual void submit_transact_log(BinaryData);
    virtual void stop_logging() TIGHTDB_OVERRIDE;
    virtual void reset_log_management(version_type last_version) TIGHTDB_OVERRIDE;
    void cleanup_stale_versions();
    virtual void set_last_version_seen_locally(uint_fast64_t last_seen_version_number) TIGHTDB_NOEXCEPT;
    virtual void set_last_version_synced(uint_fast64_t last_seen_version_number) TIGHTDB_NOEXCEPT;
    virtual uint_fast64_t get_last_version_synced(uint_fast64_t* newest_version_number) TIGHTDB_NOEXCEPT;
    virtual void get_commit_entries(uint_fast64_t from_version, uint_fast64_t to_version,
                                    BinaryData* logs_buffer) TIGHTDB_NOEXCEPT;
protected:
    static const size_t page_size = 4096; // file and memory mappings are always multipla of this size
    static const size_t minimal_pages = 1; 
    // Layout of the commit logs preamble. The preamble is placed at the start of the first commitlog file.
    // (space is reserved at the start of both files, but only the preamble in m_log_a is used).
    struct CommitLogPreamble {
        // lock:
        RobustMutex lock;
        // indicates which file is active/being written.
        bool active_file_is_log_a;

        // The following are monotonically increasing:
        uint64_t begin_oldest_commit_range; // for commits residing in in-active file
        uint64_t begin_newest_commit_range; // for commits residing in the active file
        uint64_t end_commit_range;
        // The log bringing us from state A to state A+1 is given the number A.
        // The end_commit_range is a traditional C++ limit, it points one past the last number
        uint64_t write_offset; // within active file, value always kept aligned to uint64_t

        // Last seen versions by Sync and local sharing, respectively
        uint64_t last_version_seen_locally;
        // A value of zero for last_version_synced indicates that Sync is not used, so this
        // member can be disregarded when determining which logs are stale.
        uint64_t last_version_synced;

        // proper intialization:
        CommitLogPreamble() : lock() 
        { 
            active_file_is_log_a = true;
            // The first commit will be from state 1 -> state 2, so we must set 1 initially
            begin_oldest_commit_range = begin_newest_commit_range = end_commit_range = 1;
            last_version_seen_locally = last_version_synced = 1;
            write_offset = sizeof(*this);
        }
    };
    // Each of the actual logs are preceded by their size (in uint64_t format), and each log start
    // aligned to uint64_t (required on some architectures). The size does not count any padding
    // needed at the end of each log.

    // Metadata for a file:
    struct CommitLogMetadata {
        util::File file;
        std::string name;
        util::File::Map<CommitLogPreamble> map;
        util::File::SizeType last_seen_size;
        CommitLogMetadata(std::string name) : name(name) {}
    };

    std::string m_database_name;
    CommitLogMetadata m_log_a;
    CommitLogMetadata m_log_b;
    util::Buffer<char> m_transact_log_buffer;
    util::File::Map<CommitLogPreamble> m_preamble;
    // last seen version and associated offset - 0 for invalid
    uint64_t m_read_version;
    uint64_t m_read_offset;

    // make sure the preamble (in log file A) is available and mapped. This is required for
    // mutex access.
    void map_preamble_if_needed();

    // Ensure the file is open so that it can be resized or mapped
    void open_if_needed(CommitLogMetadata& log);

    // Ensure the log files memory mapping is up to date (the mapping needs to be changed 
    // if the size of the file has changed since the previous mapping).
    void remap_if_needed(CommitLogMetadata& log);

    // Reset mapping and file
    void reset_file(CommitLogMetadata& log);

    // Get the buffers pointing into the two files in order of their commits.
    void get_buffers_in_order(char*& first, char*& second);
};

// little helper:
uint64_t aligned_to(uint64_t alignment, uint64_t value)
{
    return (value + alignment - 1) & ~(alignment - 1);
}

// Files must be mapped before calling this method
void WriteLogCollector::get_buffers_in_order(char*& first, char*& second)
{
    CommitLogPreamble* preamble = m_log_a.map.get_addr();
    if (preamble->active_file_is_log_a) {
        first  = reinterpret_cast<char*>(m_log_b.map.get_addr());
        second = reinterpret_cast<char*>(m_log_a.map.get_addr());
    } 
    else {
        first  = reinterpret_cast<char*>(m_log_a.map.get_addr());
        second = reinterpret_cast<char*>(m_log_b.map.get_addr());
    }
}


void WriteLogCollector::open_if_needed(CommitLogMetadata& log)
{
    if (log.file.is_attached() == false) {
        log.file.open(log.name, File::mode_Update);
    }
}

void WriteLogCollector::remap_if_needed(CommitLogMetadata& log)
{
    if (log.map.is_attached() == false) {
        open_if_needed(log);
        log.last_seen_size = log.file.get_size();
        log.map.map(log.file, File::access_ReadWrite, log.last_seen_size);
        return;
    }
    if (log.last_seen_size != log.file.get_size()) {
        log.map.remap(log.file, File::access_ReadWrite, log.file.get_size());
        log.last_seen_size = log.file.get_size();
    }
}

void WriteLogCollector::map_preamble_if_needed()
{
    if (m_preamble.is_attached() == false) {
        open_if_needed(m_log_a);
        m_preamble.map(m_log_a.file, File::access_ReadWrite, sizeof(CommitLogPreamble));
    }
}

void WriteLogCollector::reset_file(CommitLogMetadata& log)
{
    log.map.unmap();
    log.file.close();
    File::try_remove(log.name);
    log.file.open(log.name, File::mode_Write);
    log.file.resize(minimal_pages * page_size);
    log.map.map(log.file, File::access_ReadWrite, minimal_pages * page_size);
    log.last_seen_size = minimal_pages * page_size;
}

WriteLogCollector::~WriteLogCollector() TIGHTDB_NOEXCEPT 
{
    m_log_a.map.unmap();
    m_log_a.file.close();
    m_log_b.map.unmap();
    m_log_b.file.close();
    m_preamble.unmap();
}

void WriteLogCollector::stop_logging()
{
    File::try_remove(m_log_a.name);
    File::try_remove(m_log_b.name);
}

void WriteLogCollector::reset_log_management(version_type last_version)
{
    // TODO:
    // regardless of version, it should be ok to initialize the mutex. This protects
    // us against deadlock when we restart after crash on a platform without support
    // for robust mutexes.
    //
    // for version number 1 the log files will be completely (re)initialized.
    // for all other versions, the log files must be there and pass an integrity check.
    if (last_version == 1) {
        m_preamble.unmap();
        reset_file(m_log_a);
        reset_file(m_log_b);
        map_preamble_if_needed();
        new(m_preamble.get_addr()) CommitLogPreamble();
    }
}



void recover_from_dead_owner()
{
    // nothing!
}


void WriteLogCollector::set_last_version_synced(uint_fast64_t last_seen_version_number) TIGHTDB_NOEXCEPT
{
    map_preamble_if_needed();
    CommitLogPreamble* preamble = m_preamble.get_addr();
    RobustLockGuard rlg(preamble->lock, &recover_from_dead_owner);
    preamble->last_version_synced = last_seen_version_number;
    cleanup_stale_versions();
}

uint_fast64_t WriteLogCollector::get_last_version_synced(uint_fast64_t* end_version_number) TIGHTDB_NOEXCEPT
{
    map_preamble_if_needed();
    CommitLogPreamble* preamble = m_preamble.get_addr();
    RobustLockGuard rlg(preamble->lock, &recover_from_dead_owner);
    if (end_version_number) {
        *end_version_number = preamble->end_commit_range;
    }
    if (preamble->last_version_synced)
        return preamble->last_version_synced;
    else
        return preamble->last_version_seen_locally;
}

void WriteLogCollector::set_last_version_seen_locally(version_type last_seen_version_number) TIGHTDB_NOEXCEPT
{
    map_preamble_if_needed();
    CommitLogPreamble* preamble = m_preamble.get_addr();
    RobustLockGuard rlg(preamble->lock, &recover_from_dead_owner);
    preamble->last_version_seen_locally = last_seen_version_number;
    cleanup_stale_versions();
}

void WriteLogCollector::cleanup_stale_versions()
{
    // if a file holds only versions before last_seen_version_number, it can be recycled.
    // recycling is done by updating the preamble of log file A, which must be mapped by
    // the caller.
    CommitLogPreamble* preamble = m_preamble.get_addr();
    version_type last_seen_version_number;
    last_seen_version_number = preamble->last_version_seen_locally;
    if (preamble->last_version_synced 
        && preamble->last_version_synced < preamble->last_version_seen_locally)
        last_seen_version_number = preamble->last_version_synced;

    // cerr << "oldest_version(" << last_seen_version_number << ")" << endl; 
    if (last_seen_version_number >= preamble->begin_newest_commit_range) {
        // oldest file holds only stale commitlogs, so let's swap files and update the range
        preamble->active_file_is_log_a = !preamble->active_file_is_log_a;
        preamble->begin_oldest_commit_range = preamble->begin_newest_commit_range;
        preamble->begin_newest_commit_range = preamble->end_commit_range;
        preamble->write_offset = sizeof(CommitLogPreamble);
    }
}


void WriteLogCollector::get_commit_entries(version_type from_version, version_type to_version,
                                           BinaryData* logs_buffer) TIGHTDB_NOEXCEPT
{
    map_preamble_if_needed();
    CommitLogPreamble* preamble = m_preamble.get_addr();
    {
        preamble->lock.lock(&recover_from_dead_owner);
        TIGHTDB_ASSERT(from_version >= preamble->begin_oldest_commit_range);
        TIGHTDB_ASSERT(to_version <= preamble->end_commit_range);

        // - make sure the files are open and mapped, possibly update stale mappings
        remap_if_needed(m_log_a);
        remap_if_needed(m_log_b);
        // cerr << "get_commit_entries(" << from_version << ", " << to_version <<")" << endl;
        char* buffer;
        char* second_buffer;
        get_buffers_in_order(buffer, second_buffer);

        // setup local offset and version tracking variables if needed
        if (m_read_version < preamble->begin_oldest_commit_range) {
            m_read_version = preamble->begin_oldest_commit_range;
            m_read_offset = sizeof(CommitLogPreamble);
            // cerr << "  -- reset tracking" << endl;
        }

        // switch buffer if we are starting scanning in the second file:
        if (m_read_version >= preamble->begin_newest_commit_range) {
            buffer = second_buffer;
            second_buffer = 0;
            // cerr << "  -- resuming directly in second file" << endl;
            // The saved offset (m_read_offset) should still be valid
        }

        // traverse commits:
        // FIXME: The layout of this loop is very carefully crafted to ensure proper
        // updates of read tracking (m_read_version and m_read_offset), and most notably
        // to PREVENT update of read tracking if it is unsafe, i.e. could lead to problems
        // when reading is resumed during a later call.
        while (1) {

            // switch from first to second file if needed (at most once)
            if (second_buffer && m_read_version >= preamble->begin_newest_commit_range) {
                buffer = second_buffer;
                second_buffer = 0;
                m_read_offset = sizeof(CommitLogPreamble);
                // cerr << "  -- switching from first to second file" << endl;
            }

            // this condition cannot be moved to be a condition for the entire while loop,
            // because we need to do the above updates to read tracking
            if (m_read_version >= to_version)
                break;

            // follow buffer layout
            uint64_t size = *reinterpret_cast<uint64_t*>(buffer + m_read_offset);
            uint64_t tmp_offset = m_read_offset + sizeof(uint64_t);
            if (m_read_version >= from_version) {
                // cerr << "  --at: " << m_read_offset << ", " << size << endl;
                *logs_buffer = BinaryData(buffer + tmp_offset, size);
                ++logs_buffer;
            }
            // break early to avoid updating tracking information, if we've reached past
            // the final entry.. We CAN resume from the final entry, but we cannot safely
            // resume once we've read past the final entry. The reason is that an intervening
            // call to set_oldest_version could shift the write point to the beginning of the
            // other file.
            if (m_read_version+1 >= preamble->end_commit_range) break;
            size = aligned_to(sizeof(uint64_t), size);
            m_read_offset = tmp_offset + size;
            m_read_version++;
        }
        preamble->lock.unlock();
    }
}

// returns the current "from" version
version_type WriteLogCollector::internal_submit_log(const char* data, uint64_t sz)
{
    version_type orig_version;
    map_preamble_if_needed();
    CommitLogPreamble* preamble = m_preamble.get_addr();
    {
        preamble->lock.lock(&recover_from_dead_owner);
        // cerr << "commit_write_transaction(" << orig_version << ")" << endl;
        CommitLogMetadata* active_log;
        if (preamble->active_file_is_log_a)
            active_log = &m_log_a;
        else
            active_log = &m_log_b;

        // make sure the file is available for potential resizing
        open_if_needed(*active_log);

        // make sure we have space (allocate if not)
        File::SizeType size_needed = aligned_to(sizeof(uint64_t), preamble->write_offset + sizeof(uint64_t) + sz);
        size_needed = aligned_to(page_size, size_needed);
        if (size_needed > active_log->file.get_size()) {
            active_log->file.resize(size_needed);
        }

        // create/update mapping so that we are sure it covers the file we are about write:
        remap_if_needed(*active_log);

        // append data from write pointer and onwards:
        char* write_ptr = reinterpret_cast<char*>(active_log->map.get_addr()) + preamble->write_offset;
        *reinterpret_cast<uint64_t*>(write_ptr) = sz;
        write_ptr += sizeof(uint64_t);
        std::copy(data, data+sz, write_ptr);
        // cerr << "    -- at: " << preamble->write_offset << ", " << sz << endl;

        // update metadata to reflect the added commit log
        preamble->write_offset += aligned_to(sizeof(uint64_t), sz + sizeof(uint64_t));
        orig_version = preamble->end_commit_range;
        preamble->end_commit_range = orig_version+1;
        preamble->lock.unlock();
    }
    return orig_version;
}

void WriteLogCollector::submit_transact_log(BinaryData bd)
{
    internal_submit_log(bd.data(), bd.size());
}

WriteLogCollector::version_type 
WriteLogCollector::do_commit_write_transact(SharedGroup& sg, 
	WriteLogCollector::version_type orig_version)
{
    static_cast<void>(sg);
    char* data = m_transact_log_buffer.data();
    uint64_t sz = m_transact_log_free_begin - data;
    version_type from_version = internal_submit_log(data,sz);
    TIGHTDB_ASSERT(from_version == orig_version);
    static_cast<void>(from_version);
    version_type new_version = orig_version + 1;
    return new_version;
}


void WriteLogCollector::do_begin_write_transact(SharedGroup& sg)
{
    static_cast<void>(sg);
    m_transact_log_free_begin = m_transact_log_buffer.data();
    m_transact_log_free_end   = m_transact_log_free_begin + m_transact_log_buffer.size();
}


void WriteLogCollector::do_rollback_write_transact(SharedGroup& sg) TIGHTDB_NOEXCEPT
{
    // forward transaction log buffer
    sg.do_rollback_and_continue_as_read(m_transact_log_buffer.data(), m_transact_log_free_begin);
}


void WriteLogCollector::do_transact_log_reserve(std::size_t sz)
{
    transact_log_reserve(sz);
}


void WriteLogCollector::do_transact_log_append(const char* data, std::size_t size)
{
    transact_log_reserve(size);
    m_transact_log_free_begin = std::copy(data, data+size, m_transact_log_free_begin);
}


void WriteLogCollector::transact_log_reserve(std::size_t n)
{
    char* data = m_transact_log_buffer.data();
    std::size_t size = m_transact_log_free_begin - data;
    m_transact_log_buffer.reserve_extra(size, n);
    data = m_transact_log_buffer.data();
    m_transact_log_free_begin = data + size;
    m_transact_log_free_end = data + m_transact_log_buffer.size();
}


WriteLogCollector::WriteLogCollector(std::string database_name)
    : m_log_a(database_name + ".log_a"), m_log_b(database_name + ".log_b")
{
    m_database_name = database_name;
    m_read_version = 0;
    m_read_offset = 0;
}

} // namespace _impl




Replication* makeWriteLogCollector(std::string database_name)
{
    return  new _impl::WriteLogCollector(database_name);
}



} // namespace tightdb
#endif // TIGHTDB_ENABLE_REPLICATION
