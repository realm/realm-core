/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2014] Realm Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Realm Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Realm Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Realm Incorporated.
 *
 **************************************************************************/

#include <exception>
#include <memory>
#include <string>
#include <map>

#include <realm/util/thread.hpp>
#include <realm/util/file.hpp>
#include <realm/group_shared.hpp>
#include <realm/impl/input_stream.hpp>
#include <realm/commit_log.hpp>
#include <realm/disable_sync_to_disk.hpp>

using namespace realm::util;

namespace {

class HistoryEntry {
public:
    realm::BinaryData changeset;
};

} // anonymous namespace

namespace realm {
namespace _impl {


// Design of the commit logs:
//
// We use two files to hold the commit logs. Using two files (instead of one)
// allows us to append data to the end of one of the files, instead of doing
// complex memory management. Initially, both files hold only a header, and one
// of them is designated 'active'. New commit logs are appended to the active
// file. Each file holds a consecutive range of commits, the active file holding
// the latest commits. A commit log entry is never split between the files.
//
// Calls to set_oldest_version_needed() checks if the non-active file holds
// stale commit logs only.  If so, the non-active file is reset and becomes
// active instead.
//
// Filesizes are determined by heuristics. When a file runs out of space, its
// size is doubled.  When changing the active file, the total amount memory that
// can be reached is computed, and if it is below 1/8 of the current filesize,
// the file is truncated to half its old size.  the intention is to strike a
// balance between shrinking the files, when they are much bigger than needed,
// while at the same time avoiding many repeated shrinks and expansions.
//
// Calls to get_history_entries() determine which file(s) need to be accessed,
// map them to memory and build a vector of BinaryData with pointers to the
// buffers. The pointers may end up going to both mappings/files.
//
// Access to the commit-logs metadata is protected by an inter-process mutex.
//
// FIXME: we should not use size_t for memory mapped members, but one where the
// size is guaranteed

class WriteLogCollector: public ClientHistory {
public:
    WriteLogCollector(const std::string& database_name, const char* encryption_key);
    ~WriteLogCollector() REALM_NOEXCEPT;
    std::string do_get_database_path() override { return m_database_name; }
    void do_initiate_transact(SharedGroup &, version_type) override;
    version_type do_prepare_commit(SharedGroup& sg, version_type orig_version)
    override;
    void do_finalize_commit(SharedGroup&) REALM_NOEXCEPT override;
    void do_abort_transact(SharedGroup&) REALM_NOEXCEPT override;
    BinaryData get_uncommitted_changes() REALM_NOEXCEPT override;
    void do_interrupt() REALM_NOEXCEPT override {};
    void do_clear_interrupt() REALM_NOEXCEPT override {};
    void transact_log_reserve(size_t size, char** new_begin, char** new_end) override;
    void transact_log_append(const char* data, size_t size, char** new_begin, char** new_end) override;
    void stop_logging() override;
    void reset_log_management(version_type last_version) override;
    void set_last_version_seen_locally(version_type last_seen_version_number)
    REALM_NOEXCEPT override;

    void get_changesets(version_type, version_type, BinaryData*) const REALM_NOEXCEPT override;

protected:

    // file and memory mappings are always multiples of this size
    static const size_t page_size = 4096;

    static const size_t minimal_pages = 1;

    // Layout of the commit logs preamble and header. The header contains a
    // mutex, two preambles and a flag indicating which preamble is in
    // use. Changes to the commitlogs are crash safe because of the order of
    // updates to the file. When commit logs are added, they are appended to the
    // active file, the preamble is copied, the copy is updated and sync'ed to
    // disk. Then the flag selecting which preamble to use is updated and
    // sync'ed. This way, should we crash during updates, the old preamble will
    // be in effect once we restart, and the more-or-less written changes are
    // just ignored.
    struct CommitLogPreamble {

        // indicates which file is active/being written.
        bool active_file_is_log_a;

        // The following are monotonically increasing:
        uint64_t begin_oldest_commit_range; // for commits residing in inactive file
        uint64_t begin_newest_commit_range; // for commits residing in active file
        uint64_t end_commit_range;

        // The log bringing us from state A to state A+1 is given the number A.
        // The end_commit_range is a traditional C++ limit, it points one past
        // the last number
        uint64_t write_offset; // within active file, value always kept aligned to uint64_t

        // Last seen versions by Sync and local sharing, respectively
        uint64_t last_version_seen_locally;

        // proper intialization:
        CommitLogPreamble(uint_fast64_t version)
        {
            active_file_is_log_a = true;

            // The first commit will be from version 1 -> 2, so we must set 1 initially
            begin_oldest_commit_range = begin_newest_commit_range = end_commit_range = version;
            last_version_seen_locally = version;
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

        CommitLogHeader(uint_fast64_t version):
            preamble_a(version),
            preamble_b(version)
        {
            use_preamble_a = true;
        }
    };

    // Each of the actual logs are preceded by this header,
    // and each log start aligned to uint64_t (required on some
    // architectures). The size does not count any padding needed at the end of
    // each log.
    struct EntryHeader {
        uint64_t size;
    };

    // Metadata for a file (in memory):
    struct CommitLogMetadata {
        mutable util::File file;
        std::string name;
        mutable util::File::Map<CommitLogHeader> map;
        mutable util::File::SizeType last_seen_size;
        CommitLogMetadata(std::string name): name(name) {}
    };

    class MergingIndexTranslator;

    std::string m_database_name;
    std::string m_header_name;
    CommitLogMetadata m_log_a;
    CommitLogMetadata m_log_b;
    util::Buffer<char> m_transact_log_buffer;
    mutable util::File::Map<CommitLogHeader> m_header;

    // last seen version and associated offset - 0 for invalid
    mutable uint_fast64_t m_read_version;
    mutable uint_fast64_t m_read_offset;

    // FIXME: Part of non-persisting temporary implementation
    std::map<version_type, std::string> m_reciprocal_transforms;


    // Make sure the header is available and mapped. This is required for any
    // access to metadata.  Calling the method while the mutex is locked will
    // result in undefined behavior, so DON'T.
    void map_header_if_needed() const;

    // Get the current preamble for reading only - use get_preamble_for_write()
    // if you are going to change stuff in the preamble, and remember to call
    // sync_header() to commit those changes.
    CommitLogPreamble* get_preamble() const;

    // Creates in-mapped-memory copy of the active preamble and returns a
    // pointer to it.  Allows you to do in-place updates of the preamble, then
    // commit those changes by calling sync_header().
    CommitLogPreamble* get_preamble_for_write();

    // commit any changes to the preamble obtained by get_preamble_for_writing.
    void sync_header();

    // Get the active log file. The active log file is the file to which
    // log entries are currently appended.
    CommitLogMetadata* get_active_log(CommitLogPreamble*);

    // Get the buffers pointing into the two files in order of their commits.
    // The first buffer maps the file containing log entries:
    //
    //     [ preamble->begin_oldest_commit_range .. preamble->begin_newest_commit_range [
    //
    // The second buffer maps the file containing log entries:
    //
    //     [ preamble->begin_newest_commit_range .. preamble->end_commit_range [
    void get_buffers_in_order(const CommitLogPreamble* preamble,
                              const char*& first, const char*& second) const;

    // Ensure the file is open so that it can be resized or mapped
    void open_if_needed(const CommitLogMetadata& log) const;

    // Ensure the log files memory mapping is up to date (the mapping needs to
    // be changed if the size of the file has changed since the previous
    // mapping).
    void remap_if_needed(const CommitLogMetadata& log) const;

    // Reset mapping and file
    void reset_file(CommitLogMetadata& log);

    // Reset mapping and file for the header
    void reset_header();

    // Add a single log entry to the logs. The log data is copied.
    version_type internal_submit_log(HistoryEntry);

    static void set_log_entry_internal(HistoryEntry*, const EntryHeader*, const char* log);
    static void set_log_entry_internal(BinaryData*, const EntryHeader*, const char* log);

    template<typename T>
    void get_commit_entries_internal(version_type from_version, version_type to_version,
                                     T* logs_buffer) const REALM_NOEXCEPT;

    // Determine if one of the log files hold only stale log entries.  If so,
    // recycle said log file.
    void cleanup_stale_versions(CommitLogPreamble*);
};



// little helpers:
inline uint_fast64_t aligned_to(uint_fast64_t alignment, uint_fast64_t value)
{
    return (value + alignment - 1) & ~(alignment - 1);
}


void recover_from_dead_owner()
{
    // nothing!
}



// Header access and manipulation methods:

inline WriteLogCollector::CommitLogPreamble* WriteLogCollector::get_preamble() const
{
    CommitLogHeader* header = m_header.get_addr();
    if (header->use_preamble_a)
        return &header->preamble_a;

    return &header->preamble_b;
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
    header->use_preamble_a = !header->use_preamble_a;
}


inline void WriteLogCollector::map_header_if_needed() const
{
    if (m_header.is_attached() == false) {
        File header_file(m_header_name, File::mode_Update);
        m_header.map(header_file, File::access_ReadWrite, sizeof(CommitLogHeader));
    }
}



// convenience methods for getting to buffers and logs.

void WriteLogCollector::get_buffers_in_order(const CommitLogPreamble* preamble,
                                             const char*& first, const char*& second) const
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

WriteLogCollector::CommitLogMetadata*
WriteLogCollector::get_active_log(CommitLogPreamble* preamble)
{
    if (preamble->active_file_is_log_a)
        return &m_log_a;

    return &m_log_b;
}


// File and memory mapping functions:

void WriteLogCollector::open_if_needed(const CommitLogMetadata& log) const
{
    if (log.file.is_attached() == false)
        log.file.open(log.name, File::mode_Update);
}

void WriteLogCollector::remap_if_needed(const CommitLogMetadata& log) const
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
    log.file.resize(minimal_pages * page_size); // Throws
    bool disable_sync = get_disable_sync_to_disk();
    if (!disable_sync)
        log.file.sync(); // Throws
    log.map.map(log.file, File::access_ReadWrite, minimal_pages * page_size);
    log.last_seen_size = minimal_pages * page_size;
}

void WriteLogCollector::reset_header()
{
    m_header.unmap();
    File::try_remove(m_header_name);

    File header_file(m_header_name, File::mode_Write);
    header_file.resize(sizeof(CommitLogHeader));  // Throws
    bool disable_sync = get_disable_sync_to_disk();
    if (!disable_sync)
        header_file.sync(); // Throws
    m_header.map(header_file, File::access_ReadWrite, sizeof(CommitLogHeader));
}



// Helper methods for adding and cleaning up commit log entries:

void WriteLogCollector::cleanup_stale_versions(CommitLogPreamble* preamble)
{
    // if a file holds only versions before last_seen_version_number, it can be
    // recycled.  recycling is done by updating the preamble of log file A,
    // which must be mapped by the caller.
    version_type last_seen_version_number;
    last_seen_version_number = preamble->last_version_seen_locally;

    // std::cerr << "oldest_version(" << last_seen_version_number << ")" << std::endl;
    if (last_seen_version_number >= preamble->begin_newest_commit_range) {
        // oldest file holds only stale commitlogs, so let's swap files and
        // update the range
        preamble->active_file_is_log_a = !preamble->active_file_is_log_a;
        preamble->begin_oldest_commit_range = preamble->begin_newest_commit_range;
        preamble->begin_newest_commit_range = preamble->end_commit_range;
        preamble->write_offset = 0;

        // shrink the recycled file by 1/4
        CommitLogMetadata* active_log = get_active_log(preamble);
        open_if_needed(*active_log);
        File::SizeType size = active_log->file.get_size();
        size /= page_size * minimal_pages;
        if (size > 4) {
            size -= size / 4;
            size *= page_size * minimal_pages;
            active_log->map.unmap();
            active_log->file.resize(size); // Throws
            bool disable_sync = get_disable_sync_to_disk();
            if (!disable_sync)
                active_log->file.sync(); // Throws
        }
    }
}


// returns the current "from" version
Replication::version_type
WriteLogCollector::internal_submit_log(HistoryEntry entry)
{
    map_header_if_needed();
    RobustLockGuard rlg(m_header.get_addr()->lock, &recover_from_dead_owner);
    CommitLogPreamble* preamble = get_preamble_for_write();

    CommitLogMetadata* active_log = get_active_log(preamble);

    // make sure the file is available for potential resizing
    open_if_needed(*active_log);

    // make sure we have space (allocate if not)
    File::SizeType size_needed =
        aligned_to(sizeof(uint64_t), preamble->write_offset + sizeof(EntryHeader) + entry.changeset.size());
    size_needed = aligned_to(page_size, size_needed);
    if (size_needed > active_log->file.get_size()) {
        active_log->file.resize(size_needed); // Throws
        bool disable_sync = get_disable_sync_to_disk();
        if (!disable_sync)
            active_log->file.sync(); // Throws
    }

    // create/update mapping so that we are sure it covers the file we are about
    // write:
    remap_if_needed(*active_log);

    // append data from write pointer and onwards:
    char* write_ptr = reinterpret_cast<char*>(active_log->map.get_addr()) + preamble->write_offset;
    EntryHeader hdr;
    hdr.size = entry.changeset.size();
    *reinterpret_cast<EntryHeader*>(write_ptr) = hdr;
    write_ptr += sizeof(EntryHeader);
    std::copy(entry.changeset.data(), entry.changeset.data() + entry.changeset.size(), write_ptr);
    bool disable_sync = get_disable_sync_to_disk();
    if (!disable_sync)
        active_log->map.sync(); // Throws

    // update metadata to reflect the added commit log
    preamble->write_offset += aligned_to(sizeof(uint64_t), entry.changeset.size() + sizeof(EntryHeader));
    version_type orig_version = preamble->end_commit_range;
    preamble->end_commit_range = orig_version + 1;
    sync_header();
    return orig_version;
}




// Public methods:

WriteLogCollector::~WriteLogCollector() REALM_NOEXCEPT
{
}

void WriteLogCollector::stop_logging()
{
    File::try_remove(m_log_a.name);
    File::try_remove(m_log_b.name);
    File::try_remove(m_header_name);
}

void WriteLogCollector::reset_log_management(version_type last_version)
{
    reset_header();
    reset_file(m_log_a);
    reset_file(m_log_b);
    new (m_header.get_addr())CommitLogHeader(last_version);

    // This protects us against deadlock when we restart after crash on a
    // platform without support for robust mutexes.
    new (&m_header.get_addr()->lock)RobustMutex;
    bool disable_sync = get_disable_sync_to_disk();
    if (!disable_sync)
        m_header.sync(); // Throws
}


// FIXME: Finn, `map_header_if_needed()` can throw, so it is an error to declare
// this one `noexcept`
void WriteLogCollector::set_last_version_seen_locally(version_type last_seen_version_number)
REALM_NOEXCEPT
{
    map_header_if_needed();
    RobustLockGuard rlg(m_header.get_addr()->lock, &recover_from_dead_owner);
    CommitLogPreamble* preamble = get_preamble_for_write();
    preamble->last_version_seen_locally = last_seen_version_number;
    cleanup_stale_versions(preamble);
    sync_header();
}


void WriteLogCollector::set_log_entry_internal(HistoryEntry* entry,
                                               const EntryHeader* hdr, const char* log)
{
    entry->changeset = BinaryData(log, hdr->size);
}

void WriteLogCollector::set_log_entry_internal(BinaryData* entry,
                                               const EntryHeader* hdr, const char* log)
{
    *entry = BinaryData(log, hdr->size);
}


void WriteLogCollector::get_changesets(version_type from_version, version_type to_version,
                                       BinaryData* logs_buffer) const REALM_NOEXCEPT
{
    get_commit_entries_internal(from_version, to_version, logs_buffer);
}


template<typename T>
void WriteLogCollector::get_commit_entries_internal(version_type from_version,
                                                    version_type to_version,
                                                    T*           logs_buffer) const REALM_NOEXCEPT
{
    map_header_if_needed();
    RobustLockGuard rlg(m_header.get_addr()->lock, &recover_from_dead_owner);
    const CommitLogPreamble* preamble = get_preamble();
    REALM_ASSERT_3(from_version, >=, preamble->begin_oldest_commit_range);
    REALM_ASSERT_3(to_version, <=, preamble->end_commit_range);

    // - make sure the files are open and mapped, possibly update stale mappings
    remap_if_needed(m_log_a);
    remap_if_needed(m_log_b);

    // std::cerr << "get_commit_entries(" << from_version << ", " << to_version <<")" << std::endl;
    const char* buffer;
    const char* second_buffer;
    get_buffers_in_order(preamble, buffer, second_buffer);

    // setup local offset and version tracking variables if needed
    if ((m_read_version != from_version) || (m_read_version < preamble->begin_oldest_commit_range)) {
        m_read_version = preamble->begin_oldest_commit_range;
        m_read_offset = 0;

        // std::cerr << "  -- reset tracking" << std::endl;
    }

    // switch buffer if we are starting scanning in the second file:
    if (m_read_version >= preamble->begin_newest_commit_range) {
        buffer = second_buffer;
        second_buffer = 0;

        // std::cerr << "  -- resuming directly in second file" << std::endl;
        // The saved offset (m_read_offset) should still be valid
    }

    // traverse commits:
    // FIXME: The layout of this loop is very carefully crafted to ensure proper
    // updates of read tracking (m_read_version and m_read_offset), and most
    // notably to PREVENT update of read tracking if it is unsafe, i.e. could
    // lead to problems when reading is resumed during a later call.
    for (;;) {

        // switch from first to second file if needed (at most once)
        if (second_buffer && m_read_version >= preamble->begin_newest_commit_range) {
            buffer = second_buffer;
            second_buffer = 0;
            m_read_offset = 0;

            // std::cerr << "  -- switching from first to second file\n";
        }

        // this condition cannot be moved to be a condition for the entire while
        // loop, because we need to do the above updates to read tracking
        if (m_read_version >= to_version)
            break;

        // follow buffer layout
        const EntryHeader* hdr = reinterpret_cast<const EntryHeader*>(buffer + m_read_offset);
        uint_fast64_t tmp_offset = m_read_offset + sizeof(EntryHeader);
        if (m_read_version >= from_version) {
            // std::cerr << "  --at: " << m_read_offset << ", " << size << "\n";
            set_log_entry_internal(logs_buffer, hdr, buffer + tmp_offset);
            ++logs_buffer;
        }

        // break early to avoid updating tracking information, if we've reached
        // past the final entry.. We CAN resume from the final entry, but we
        // cannot safely resume once we've read past the final entry. The reason
        // is that an intervening call to set_oldest_version could shift the
        // write point to the beginning of the other file.
        if (m_read_version + 1 >= preamble->end_commit_range)
            break;
        uint_fast64_t size = aligned_to(sizeof(uint64_t), hdr->size);
        m_read_offset = tmp_offset + size;
        m_read_version++;
    }
}


void WriteLogCollector::do_initiate_transact(SharedGroup&, version_type)
{
    char* buffer = m_transact_log_buffer.data();
    set_buffer(buffer, buffer + m_transact_log_buffer.size());
}


WriteLogCollector::version_type
WriteLogCollector::do_prepare_commit(SharedGroup&, WriteLogCollector::version_type orig_version)
{
    // Note: This function does not utilize the two-phase changeset submission
    // scheme, nor does it utilize the ability to discard a submitted changeset
    // during a subsequent call to do_initiate_transact() in case the transaction
    // ultimately fails. This means, unfortunately, that an application will
    // encounter an inconsistent state (and likely crash) it it attempts to
    // initiate a new transaction after a failed commit.
    char* data = m_transact_log_buffer.data();
    size_t size = write_position() - data;
    HistoryEntry entry;
    entry.changeset = BinaryData {
        data, size
    };
    version_type from_version = internal_submit_log(entry);
    REALM_ASSERT_3(from_version, ==, orig_version);
    static_cast<void>(from_version);
    version_type new_version = orig_version + 1;
    return new_version;
}


void WriteLogCollector::do_finalize_commit(SharedGroup&) REALM_NOEXCEPT
{
    // See note in do_prepare_commit().
}


void WriteLogCollector::do_abort_transact(SharedGroup&) REALM_NOEXCEPT
{
    // See note in do_prepare_commit().
}


BinaryData WriteLogCollector::get_uncommitted_changes() REALM_NOEXCEPT
{
    return BinaryData(m_transact_log_buffer.data(),
                      write_position() - m_transact_log_buffer.data());
}


void WriteLogCollector::transact_log_append(const char* data, size_t size,
                                            char** new_begin, char** new_end)
{
    transact_log_reserve(size, new_begin, new_end);
    *new_begin = std::copy(data, data + size, *new_begin);
}


void WriteLogCollector::transact_log_reserve(size_t size, char** new_begin, char** new_end)
{
    char* data = m_transact_log_buffer.data();
    size_t size2 = write_position() - data;
    m_transact_log_buffer.reserve_extra(size2, size);
    data = m_transact_log_buffer.data();
    *new_begin = data + size2;
    *new_end = data + m_transact_log_buffer.size();
}


WriteLogCollector::WriteLogCollector(const std::string& database_name,
                                     const char*        encryption_key):
    m_log_a(database_name + ".log_a"),
    m_log_b(database_name + ".log_b")
{
    m_database_name = database_name;
    m_header_name = database_name + ".log";
    m_read_version = 0;
    m_read_offset = 0;
    m_log_a.file.set_encryption_key(encryption_key);
    m_log_b.file.set_encryption_key(encryption_key);
}

} // end _impl


std::unique_ptr<ClientHistory> make_client_history(const std::string& database_name,
                                                   const char*        encryption_key)
{
    return std::unique_ptr<ClientHistory>(new _impl::WriteLogCollector(database_name,
                                                                       encryption_key));
}


} // namespace realm
