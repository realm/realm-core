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

using namespace util;
using namespace std;

namespace tightdb {

namespace _impl {


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
    WriteLogCollector(std::string database_name, bool server_synchronization_mode, const char *encryption_key);
    ~WriteLogCollector() TIGHTDB_NOEXCEPT;
    std::string do_get_database_path() TIGHTDB_OVERRIDE { return m_database_name; }
    void do_begin_write_transact(SharedGroup& sg) TIGHTDB_OVERRIDE;
    version_type do_commit_write_transact(SharedGroup& sg, version_type orig_version) TIGHTDB_OVERRIDE;
    void do_rollback_write_transact(SharedGroup& sg) TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    void do_interrupt() TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE {};
    void do_clear_interrupt() TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE {};
    void do_transact_log_reserve(std::size_t size) TIGHTDB_OVERRIDE;
    void do_transact_log_append(const char* data, std::size_t size) TIGHTDB_OVERRIDE;
    void transact_log_reserve(std::size_t size);
    virtual bool is_in_server_synchronization_mode() { return m_is_persisting; }
    virtual void submit_transact_log(BinaryData);
    virtual void stop_logging() TIGHTDB_OVERRIDE;
    virtual void reset_log_management(version_type last_version) TIGHTDB_OVERRIDE;
    virtual void set_last_version_seen_locally(version_type last_seen_version_number) TIGHTDB_NOEXCEPT;
    virtual void set_last_version_synced(version_type last_seen_version_number) TIGHTDB_NOEXCEPT;
    virtual version_type get_last_version_synced(version_type* newest_version_number) TIGHTDB_NOEXCEPT;
    virtual void get_commit_entries(version_type from_version, version_type to_version,
                                    BinaryData* logs_buffer) TIGHTDB_NOEXCEPT;
protected:
    static const size_t page_size = 4096; // file and memory mappings are always multiples of this size
    static const size_t minimal_pages = 1; 

    // Layout of the commit logs preamble and header. The header contains a mutex, two
    // preambles and a flag indicating which preamble is in use. Changes to the commitlogs
    // are crash safe because of the order of updates to the file. When commit logs are 
    // added, they are appended to the active file, the preamble is copied, the copy
    // is updated and sync'ed to disk. Then the flag selecting which preamble to use is
    // updated and sync'ed. This way, should we crash during updates, the old preamble
    // will be in effect once we restart, and the more-or-less written changes are just
    // ignored.
    struct CommitLogPreamble {

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
        CommitLogPreamble(uint64_t version)
        { 
            active_file_is_log_a = true;
            // The first commit will be from state 1 -> state 2, so we must set 1 initially
            begin_oldest_commit_range = begin_newest_commit_range = end_commit_range = version;
            last_version_seen_locally = last_version_synced = version;
            write_offset = 0;
        }
    };

    // The header:
    struct CommitLogHeader {
        // lock:
        RobustMutex lock;

        // selector:
        bool use_preamble_a;

        // preambles:
        CommitLogPreamble preamble_a;
        CommitLogPreamble preamble_b;

        CommitLogHeader(uint64_t version) : preamble_a(version), preamble_b(version) { 
            use_preamble_a = true; 
        }
    };

    // Each of the actual logs are preceded by their size (in uint64_t format), and each log start
    // aligned to uint64_t (required on some architectures). The size does not count any padding
    // needed at the end of each log.

    // Metadata for a file (in memory):
    struct CommitLogMetadata {
        util::File file;
        std::string name;
        util::File::Map<CommitLogHeader> map;
        util::File::SizeType last_seen_size;
        CommitLogMetadata(std::string name) : name(name) {}
    };

    std::string m_database_name;
    std::string m_header_name;
    CommitLogMetadata m_log_a;
    CommitLogMetadata m_log_b;
    util::Buffer<char> m_transact_log_buffer;
    util::File::Map<CommitLogHeader> m_header;
    bool m_is_persisting;

    // last seen version and associated offset - 0 for invalid
    uint64_t m_read_version;
    uint64_t m_read_offset;


    // make sure the header is available and mapped. This is required for any access to metadata.
    // Calling the method while the mutex is locked will result in undefined behavior, so DON'T.
    void map_header_if_needed();

    // Get the current preamble for reading only - use get_preamble_for_write() if you are
    // going to change stuff in the preamble, and remember to call sync_header() to commit those
    // changes.
    const CommitLogPreamble* get_preamble();

    // Creates in-mapped-memory copy of the active preamble and returns a pointer to it.
    // Allows you to do in-place updates of the preamble, then commit those changes
    // by calling sync_header().
    CommitLogPreamble* get_preamble_for_write();

    // commit any changes to the preamble obtained by get_preamble_for_writing.
    void sync_header();

    // Get the active log file. The active log file is the file to which
    // log entries are currently appended.
    CommitLogMetadata* get_active_log(CommitLogPreamble*);

    // Get the buffers pointing into the two files in order of their commits.
    // The first buffer maps the file containing log entries:
    //    [ preamble->begin_oldest_commit_range .. preamble->begin_newest_commit_range [
    // The second buffer maps the file containing log entries:
    //    [ preamble->begin_newest_commit_range .. preamble->end_commit_range [
    void get_buffers_in_order(const CommitLogPreamble* preamble, const char*& first, const char*& second);

    // Ensure the file is open so that it can be resized or mapped
    void open_if_needed(CommitLogMetadata& log);

    // Ensure the log files memory mapping is up to date (the mapping needs to be changed 
    // if the size of the file has changed since the previous mapping).
    void remap_if_needed(CommitLogMetadata& log);

    // Reset mapping and file
    void reset_file(CommitLogMetadata& log);

    // Reset mapping and file for the header
    void reset_header();

    // Add a single log entry to the logs. The log data is copied.
    version_type internal_submit_log(const char*, uint64_t);

    // Determine if one of the log files hold only stale log entries.
    // If so, recycle said log file.
    void cleanup_stale_versions(CommitLogPreamble*);
};



// little helpers:
inline uint64_t aligned_to(uint64_t alignment, uint64_t value)
{
    return (value + alignment - 1) & ~(alignment - 1);
}


void recover_from_dead_owner()
{
    // nothing!
}



// Header access and manipulation methods:

inline const WriteLogCollector::CommitLogPreamble* WriteLogCollector::get_preamble()
{
    CommitLogHeader* header = m_header.get_addr();
    if (header->use_preamble_a)
        return & header->preamble_a;
    else
        return & header->preamble_b;
}


inline WriteLogCollector::CommitLogPreamble* WriteLogCollector::get_preamble_for_write()
{
    CommitLogHeader* header = m_header.get_addr();
    CommitLogPreamble* from;
    CommitLogPreamble* to;
    if (header->use_preamble_a) {
        from = &(header->preamble_a);
        to = &(header->preamble_b);
    }
    else {
        from = &(header->preamble_b);
        to = &(header->preamble_a);
    }
    *to = *from;
    return to;
}


inline void WriteLogCollector::sync_header()
{
    CommitLogHeader* header = m_header.get_addr();
    if (m_is_persisting)
        m_header.sync();
    header->use_preamble_a = ! header->use_preamble_a;
    if (m_is_persisting)
        m_header.sync();
}


inline void WriteLogCollector::map_header_if_needed()
{
    if (m_header.is_attached() == false) {
        File header_file(m_header_name, File::mode_Update);
        m_header.map(header_file, File::access_ReadWrite, sizeof(CommitLogHeader));
    }
}



// convenience methods for getting to buffers and logs.

void WriteLogCollector::get_buffers_in_order(const CommitLogPreamble* preamble, 
                                             const char*& first, const char*& second)
{
    if (preamble->active_file_is_log_a) {
        first  = reinterpret_cast<char*>(m_log_b.map.get_addr());
        second = reinterpret_cast<char*>(m_log_a.map.get_addr());
    } 
    else {
        first  = reinterpret_cast<char*>(m_log_a.map.get_addr());
        second = reinterpret_cast<char*>(m_log_b.map.get_addr());
    }
}

WriteLogCollector::CommitLogMetadata* WriteLogCollector::get_active_log(CommitLogPreamble* preamble)
{
    if (preamble->active_file_is_log_a)
        return &m_log_a;
    else
        return &m_log_b;
}



// File and memory mapping functions:

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

void WriteLogCollector::reset_header()
{
    m_header.unmap();
    File::try_remove(m_header_name);

    File header_file(m_header_name, File::mode_Write);
    header_file.resize(sizeof(CommitLogHeader));
    m_header.map(header_file, File::access_ReadWrite, sizeof(CommitLogHeader));
}



// Helper methods for adding and cleaning up commit log entries:

void WriteLogCollector::cleanup_stale_versions(CommitLogPreamble* preamble)
{
    // if a file holds only versions before last_seen_version_number, it can be recycled.
    // recycling is done by updating the preamble of log file A, which must be mapped by
    // the caller.
    version_type last_seen_version_number;
    last_seen_version_number = preamble->last_version_seen_locally;
    if (m_is_persisting
        && preamble->last_version_synced < preamble->last_version_seen_locally)
        last_seen_version_number = preamble->last_version_synced;

    // cerr << "oldest_version(" << last_seen_version_number << ")" << endl; 
    if (last_seen_version_number >= preamble->begin_newest_commit_range) {
        // oldest file holds only stale commitlogs, so let's swap files and update the range
        preamble->active_file_is_log_a = !preamble->active_file_is_log_a;
        preamble->begin_oldest_commit_range = preamble->begin_newest_commit_range;
        preamble->begin_newest_commit_range = preamble->end_commit_range;
        preamble->write_offset = sizeof(CommitLogHeader);

        // shrink the recycled file by 1/4
        CommitLogMetadata* active_log = get_active_log(preamble);
        open_if_needed(*active_log);
        File::SizeType size = active_log->file.get_size();
        size /= page_size * minimal_pages;
        if (size > 4) {
            size -= size/4;
            size *= page_size * minimal_pages;
            active_log->file.resize(size);
        }
    }
}


// returns the current "from" version
version_type WriteLogCollector::internal_submit_log(const char* data, uint64_t size)
{
    version_type orig_version;
    map_header_if_needed();
    RobustLockGuard rlg(m_header.get_addr()->lock, &recover_from_dead_owner);
    CommitLogPreamble* preamble = get_preamble_for_write();
    // cerr << "commit_write_transaction(" << orig_version << ")" << endl;
    CommitLogMetadata* active_log = get_active_log(preamble);

    // make sure the file is available for potential resizing
    open_if_needed(*active_log);

    // make sure we have space (allocate if not)
    File::SizeType size_needed = aligned_to(sizeof(uint64_t), preamble->write_offset + sizeof(uint64_t) + size);
    size_needed = aligned_to(page_size, size_needed);
    if (size_needed > active_log->file.get_size()) {
        active_log->file.resize(size_needed);
    }

    // create/update mapping so that we are sure it covers the file we are about write:
    remap_if_needed(*active_log);

    // append data from write pointer and onwards:
    char* write_ptr = reinterpret_cast<char*>(active_log->map.get_addr()) + preamble->write_offset;
    *reinterpret_cast<uint64_t*>(write_ptr) = size;
    write_ptr += sizeof(uint64_t);
    std::copy(data, data+size, write_ptr);
    active_log->map.sync();
    // cerr << "    -- at: " << preamble->write_offset << ", " << size << endl;

    // update metadata to reflect the added commit log
    preamble->write_offset += aligned_to(sizeof(uint64_t), size + sizeof(uint64_t));
    orig_version = preamble->end_commit_range;
    preamble->end_commit_range = orig_version+1;
    sync_header();
    return orig_version;
}




// Public methods:

WriteLogCollector::~WriteLogCollector() TIGHTDB_NOEXCEPT 
{
}

void WriteLogCollector::stop_logging()
{
    if (m_is_persisting)
        return;

    File::try_remove(m_log_a.name);
    File::try_remove(m_log_b.name);
    File::try_remove(m_header_name);
}

void WriteLogCollector::reset_log_management(version_type last_version)
{
    if (last_version == 1 || m_is_persisting == false) {
        // for version number 1 the log files will be completely (re)initialized.
        reset_header();
        reset_file(m_log_a);
        reset_file(m_log_b);
        new(m_header.get_addr()) CommitLogHeader(last_version);
    }
    else {
        // for all other versions, the log files must be there:
        try {
            open_if_needed(m_log_a);
            open_if_needed(m_log_b);
            map_header_if_needed();
        } catch (util::File::AccessError& e) {
            throw LogFileError(m_database_name);
        }
        CommitLogPreamble* preamble = const_cast<CommitLogPreamble*>(get_preamble());

        // Verify that end of the commit range is equal to or after 'last_version'
        // TODO: This most likely should throw an exception ?
        TIGHTDB_ASSERT(last_version <= preamble->end_commit_range);

        if (last_version <= preamble->end_commit_range) {
            if (last_version < preamble->begin_newest_commit_range) {
                // writepoint is somewhere in the in-active (oldest) file, so
                // discard data in the active file, and make the in-active file active
                preamble->end_commit_range = preamble->begin_newest_commit_range;
                preamble->begin_newest_commit_range = preamble->begin_oldest_commit_range;
                preamble->active_file_is_log_a = ! preamble->active_file_is_log_a;
            }
            // writepoint is somewhere in the active file. 
            // We scan from the start to find it.
            TIGHTDB_ASSERT(last_version >= preamble->begin_newest_commit_range);
            CommitLogMetadata* active_log = get_active_log(preamble);
            remap_if_needed(*active_log);
            version_type current_version;
            const char* old_buffer; 
            const char* buffer;
            get_buffers_in_order(preamble, old_buffer, buffer);
            preamble->write_offset = sizeof(CommitLogHeader);
            for (current_version = preamble->begin_newest_commit_range; 
                 current_version < last_version;
                 current_version++) {
                // advance write ptr to next buffer start:
                uint64_t size = *reinterpret_cast<const uint64_t*>(buffer + preamble->write_offset);
                uint64_t tmp_offset = preamble->write_offset + sizeof(uint64_t);
                size = aligned_to(sizeof(uint64_t), size);
                preamble->write_offset = tmp_offset + size;
            }
            preamble->end_commit_range = current_version;
        }
    }
    // regardless of version, it should be ok to initialize the mutex. This protects
    // us against deadlock when we restart after crash on a platform without support
    // for robust mutexes.
    new (& m_header.get_addr()->lock) RobustMutex;
    m_header.sync();
}


void WriteLogCollector::set_last_version_synced(version_type last_seen_version_number) TIGHTDB_NOEXCEPT
{
    map_header_if_needed();
    RobustLockGuard rlg(m_header.get_addr()->lock, &recover_from_dead_owner);
    CommitLogPreamble* preamble = get_preamble_for_write();
    preamble->last_version_synced = last_seen_version_number;
    cleanup_stale_versions(preamble);
    sync_header();
}


version_type WriteLogCollector::get_last_version_synced(version_type* end_version_number) TIGHTDB_NOEXCEPT
{
    map_header_if_needed();
    RobustLockGuard rlg(m_header.get_addr()->lock, &recover_from_dead_owner);
    const CommitLogPreamble* preamble = get_preamble();
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
    map_header_if_needed();
    RobustLockGuard rlg(m_header.get_addr()->lock, &recover_from_dead_owner);
    CommitLogPreamble* preamble = get_preamble_for_write();
    preamble->last_version_seen_locally = last_seen_version_number;
    cleanup_stale_versions(preamble);
    sync_header();
}


void WriteLogCollector::get_commit_entries(version_type from_version, version_type to_version,
                                           BinaryData* logs_buffer) TIGHTDB_NOEXCEPT
{
    map_header_if_needed();
    RobustLockGuard rlg(m_header.get_addr()->lock, &recover_from_dead_owner);
    const CommitLogPreamble* preamble = get_preamble();
    TIGHTDB_ASSERT(from_version >= preamble->begin_oldest_commit_range);
    TIGHTDB_ASSERT(to_version <= preamble->end_commit_range);

    // - make sure the files are open and mapped, possibly update stale mappings
    remap_if_needed(m_log_a);
    remap_if_needed(m_log_b);
    // cerr << "get_commit_entries(" << from_version << ", " << to_version <<")" << endl;
    const char* buffer;
    const char* second_buffer;
    get_buffers_in_order(preamble, buffer, second_buffer);

    // setup local offset and version tracking variables if needed
    if (m_read_version < preamble->begin_oldest_commit_range) {
        m_read_version = preamble->begin_oldest_commit_range;
        m_read_offset = sizeof(CommitLogHeader);
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
    for (;;) {

        // switch from first to second file if needed (at most once)
        if (second_buffer && m_read_version >= preamble->begin_newest_commit_range) {
            buffer = second_buffer;
            second_buffer = 0;
            m_read_offset = sizeof(CommitLogHeader);
            // cerr << "  -- switching from first to second file" << endl;
        }

        // this condition cannot be moved to be a condition for the entire while loop,
        // because we need to do the above updates to read tracking
        if (m_read_version >= to_version)
            break;

        // follow buffer layout
        uint64_t size = *reinterpret_cast<const uint64_t*>(buffer + m_read_offset);
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
    uint64_t size = m_transact_log_free_begin - data;
    version_type from_version = internal_submit_log(data,size);
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


void WriteLogCollector::do_transact_log_reserve(std::size_t size)
{
    transact_log_reserve(size);
}


void WriteLogCollector::do_transact_log_append(const char* data, std::size_t size)
{
    transact_log_reserve(size);
    m_transact_log_free_begin = std::copy(data, data+size, m_transact_log_free_begin);
}


void WriteLogCollector::transact_log_reserve(std::size_t size)
{
    char* data = m_transact_log_buffer.data();
    std::size_t size2 = m_transact_log_free_begin - data;
    m_transact_log_buffer.reserve_extra(size2, size);
    data = m_transact_log_buffer.data();
    m_transact_log_free_begin = data + size2;
    m_transact_log_free_end = data + m_transact_log_buffer.size();
}


WriteLogCollector::WriteLogCollector(std::string database_name,
                                     bool server_synchronization_mode,
                                     const char *encryption_key)
    : m_log_a(database_name + ".log_a"), m_log_b(database_name + ".log_b")
{
    m_database_name = database_name;
    m_header_name = database_name + ".log";
    m_read_version = 0;
    m_read_offset = 0;
    m_is_persisting = server_synchronization_mode;
    m_log_a.file.set_encryption_key(encryption_key);
    m_log_b.file.set_encryption_key(encryption_key);
}

} // end _impl



Replication* makeWriteLogCollector(std::string database_name,
                                   bool server_synchronization_mode,
                                   const char *encryption_key)
{
    return new _impl::WriteLogCollector(database_name,
                                        server_synchronization_mode,
                                        encryption_key);
}



} // namespace tightdb
#endif // TIGHTDB_ENABLE_REPLICATION
