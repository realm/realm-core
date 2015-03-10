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

#include <sys/time.h> // FIXME!! Not portable

#include <tightdb/replication.hpp>
#include <tightdb/impl/merge_index_map.hpp>
#ifdef TIGHTDB_ENABLE_REPLICATION

using namespace util;
using namespace std;


namespace {

// Temporarily disable replication
class TempDisableReplication {
public:
    TempDisableReplication(Group& group):
        m_group(group)
    {
        typedef _impl::GroupFriend gf;
        m_temp_disabled_repl = gf::get_replication(m_group);
        gf::set_replication(m_group, 0);
    }
    ~TempDisableReplication()
    {
        typedef _impl::GroupFriend gf;
        gf::set_replication(m_group, m_temp_disabled_repl);
    }
private:
    Group& m_group;
    Replication* m_temp_disabled_repl;
};


struct SyncInfo {
    uint64_t client_file_ident;
    uint64_t server_version;
    uint64_t client_version; // this one limits cleanup of log entries
    SyncInfo():
        client_file_ident(0),
        server_version(0),
        client_version(0)
    {
    }
};

} // anonymous namespace


namespace tightdb {

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
// Calls to get_commit_entries determines which file(s) needs to be accessed,
// maps them to memory and builds a vector of BinaryData with pointers to the
// buffers. The pointers may end up going to both mappings/files.
//
// Access to the commit-logs metadata is protected by an inter-process mutex.
//
// FIXME: we should not use size_t for memory mapped members, but one where the
// size is guaranteed

class WriteLogCollector: public Replication {
public:
    WriteLogCollector(string database_name, bool server_synchronization_mode,
                      const char* encryption_key);
    ~WriteLogCollector() TIGHTDB_NOEXCEPT;
    string do_get_database_path() TIGHTDB_OVERRIDE { return m_database_name; }
    void do_begin_write_transact(SharedGroup& sg) TIGHTDB_OVERRIDE;
    version_type do_commit_write_transact(SharedGroup& sg, version_type orig_version)
        TIGHTDB_OVERRIDE;
    void do_rollback_write_transact(SharedGroup& sg) TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    void do_interrupt() TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE {};
    void do_clear_interrupt() TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE {};
    void transact_log_reserve(size_t size, char** new_begin, char** new_end) TIGHTDB_OVERRIDE;
    void transact_log_append(const char* data, size_t size, char** new_begin, char** new_end) TIGHTDB_OVERRIDE;
    virtual bool is_in_server_synchronization_mode() { return m_is_persisting; }
    version_type apply_foreign_changeset(SharedGroup&, uint_fast64_t self_peer_id, version_type, BinaryData,
                                         uint_fast64_t timestamp,
                                         uint_fast64_t peer_id, version_type peer_version,
                                         ostream*) TIGHTDB_OVERRIDE;
    version_type get_last_peer_version(uint_fast64_t peer_id) TIGHTDB_OVERRIDE;
    virtual void stop_logging() TIGHTDB_OVERRIDE;
    virtual void reset_log_management(version_type last_version) TIGHTDB_OVERRIDE;
    virtual void set_last_version_seen_locally(version_type last_seen_version_number)
        TIGHTDB_NOEXCEPT;
    void get_sync_info(uint_fast64_t&, version_type&, version_type&) TIGHTDB_OVERRIDE;
    void set_client_file_ident(uint_fast64_t) TIGHTDB_OVERRIDE;
    void set_sync_progress(version_type, version_type) TIGHTDB_OVERRIDE;
    void get_commit_entries(version_type from_version, version_type to_version,
                            Replication::CommitLogEntry* logs_buffer) TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    void get_commit_entries(version_type from_version, version_type to_version,
                            BinaryData* logs_buffer) TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;

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
        SyncInfo sync_info;

        // Last server_version, as set by calls to apply_foreign_transact_log(),
        // or 0 if never set.
        uint64_t last_server_version;
        uint64_t last_server_version_backup;

        uint64_t last_client_file_ident; // FIXME: Part of a temporary hack

        // proper intialization:
        CommitLogPreamble(uint_fast64_t version)
        {
            active_file_is_log_a = true;
            // The first commit will be from state 1 -> state 2, so we must set 1 initially
            begin_oldest_commit_range = begin_newest_commit_range = end_commit_range = version;
            last_version_seen_locally = sync_info.client_version = version;
            last_server_version_backup = last_server_version = 0;
            last_client_file_ident = 0; // FIXME: Part of a temporary hack
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
        uint64_t peer_version;
        uint64_t peer_id;
        uint64_t timestamp;
        uint64_t size;
    };

    // Metadata for a file (in memory):
    struct CommitLogMetadata {
        util::File file;
        string name;
        util::File::Map<CommitLogHeader> map;
        util::File::SizeType last_seen_size;
        CommitLogMetadata(string name): name(name) {}
    };

    class IndexTranslationMap;
    class TransformChangesetBeforeMerge;

    string m_database_name;
    string m_header_name;
    CommitLogMetadata m_log_a;
    CommitLogMetadata m_log_b;
    util::Buffer<char> m_transact_log_buffer;
    util::File::Map<CommitLogHeader> m_header;
    bool m_is_persisting;

    // last seen version and associated offset - 0 for invalid
    uint_fast64_t m_read_version;
    uint_fast64_t m_read_offset;


    // Make sure the header is available and mapped. This is required for any
    // access to metadata.  Calling the method while the mutex is locked will
    // result in undefined behavior, so DON'T.
    void map_header_if_needed();

    // Get the current preamble for reading only - use get_preamble_for_write()
    // if you are going to change stuff in the preamble, and remember to call
    // sync_header() to commit those changes.
    CommitLogPreamble* get_preamble();

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
                              const char*& first, const char*& second);

    // Ensure the file is open so that it can be resized or mapped
    void open_if_needed(CommitLogMetadata& log);

    // Ensure the log files memory mapping is up to date (the mapping needs to
    // be changed if the size of the file has changed since the previous
    // mapping).
    void remap_if_needed(CommitLogMetadata& log);

    // Reset mapping and file
    void reset_file(CommitLogMetadata& log);

    // Reset mapping and file for the header
    void reset_header();

    // Add a single log entry to the logs. The log data is copied.
    version_type internal_submit_log(const char*, uint_fast64_t size,
                                     uint_fast64_t timestamp, uint_fast64_t peer_id,
                                     version_type peer_version);



    void set_log_entry_internal(Replication::CommitLogEntry* entry,
                                const EntryHeader* hdr,
                                const char* log);

    void set_log_entry_internal(BinaryData* entry,
                                const EntryHeader* hdr,
                                const char* log);

    template<typename T>
    void get_commit_entries_internal(version_type from_version, version_type to_version,
                                     T* logs_buffer) TIGHTDB_NOEXCEPT;

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

inline WriteLogCollector::CommitLogPreamble* WriteLogCollector::get_preamble()
{
    CommitLogHeader* header = m_header.get_addr();
    if (header->use_preamble_a)
        return & header->preamble_a;
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
        m_header.map(header_file, File::access_ReadWrite, sizeof (CommitLogHeader));
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

WriteLogCollector::CommitLogMetadata*
WriteLogCollector::get_active_log(CommitLogPreamble* preamble)
{
    if (preamble->active_file_is_log_a)
        return &m_log_a;
    return &m_log_b;
}


// File and memory mapping functions:

void WriteLogCollector::open_if_needed(CommitLogMetadata& log)
{
    if (log.file.is_attached() == false)
        log.file.open(log.name, File::mode_Update);
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
    header_file.resize(sizeof (CommitLogHeader));
    m_header.map(header_file, File::access_ReadWrite, sizeof (CommitLogHeader));
}



// Helper methods for adding and cleaning up commit log entries:

void WriteLogCollector::cleanup_stale_versions(CommitLogPreamble* preamble)
{
    // if a file holds only versions before last_seen_version_number, it can be
    // recycled.  recycling is done by updating the preamble of log file A,
    // which must be mapped by the caller.
    version_type last_seen_version_number;
    last_seen_version_number = preamble->last_version_seen_locally;
    if (m_is_persisting
        && preamble->sync_info.client_version < preamble->last_version_seen_locally)
        last_seen_version_number = preamble->sync_info.client_version;

    // cerr << "oldest_version(" << last_seen_version_number << ")" << endl;
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
            size -= size/4;
            size *= page_size * minimal_pages;
            active_log->map.unmap();
            active_log->file.resize(size);
        }
    }
}


// returns the current "from" version
Replication::version_type
WriteLogCollector::internal_submit_log(const char* data, uint_fast64_t size,
                                       uint_fast64_t timestamp, uint_fast64_t peer_id,
                                       version_type peer_version)
{
    map_header_if_needed();
    RobustLockGuard rlg(m_header.get_addr()->lock, &recover_from_dead_owner);
    CommitLogPreamble* preamble = get_preamble_for_write();
    CommitLogMetadata* active_log = get_active_log(preamble);

    // for local commits, the server_version is taken from the previous commit.
    // for foreign commits, the server_version is provided by the caller and saved
    // for later use. (we need to make a backup which can be restored if the current
    // commit is discarded later)
    preamble->last_server_version_backup = preamble->last_server_version;
    if (peer_id != 0) {
        preamble->last_server_version = peer_version;
    }
    else {
        peer_version = preamble->last_server_version;
    }

    preamble->last_client_file_ident = peer_id; // FIXME: Part of a temporary hack

    // make sure the file is available for potential resizing
    open_if_needed(*active_log);

    // make sure we have space (allocate if not)
    File::SizeType size_needed =
        aligned_to(sizeof (uint64_t), preamble->write_offset + sizeof(EntryHeader) + size);
    size_needed = aligned_to(page_size, size_needed);
    if (size_needed > active_log->file.get_size())
        active_log->file.resize(size_needed);

    // create/update mapping so that we are sure it covers the file we are about
    // write:
    remap_if_needed(*active_log);

    // append data from write pointer and onwards:
    char* write_ptr = reinterpret_cast<char*>(active_log->map.get_addr()) + preamble->write_offset;
    EntryHeader hdr;
    hdr.peer_version = peer_version;
    hdr.peer_id = peer_id;
    hdr.timestamp = timestamp;
    hdr.size = size;
    *reinterpret_cast<EntryHeader*>(write_ptr) = hdr;
    write_ptr += sizeof(EntryHeader);
    copy(data, data+size, write_ptr);
    active_log->map.sync();

    // update metadata to reflect the added commit log
    preamble->write_offset += aligned_to(sizeof (uint64_t), size + sizeof(EntryHeader));
    version_type orig_version = preamble->end_commit_range;
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
        new (m_header.get_addr()) CommitLogHeader(last_version);
    }
    else {
        // for all other versions, the log files must be there:
        try {
            open_if_needed(m_log_a);
            open_if_needed(m_log_b);
            map_header_if_needed();
        }
        catch (util::File::AccessError& e) {
            throw LogFileError(m_database_name);
        }
        CommitLogPreamble* preamble = const_cast<CommitLogPreamble*>(get_preamble());

        // Verify that end of the commit range is equal to or after 'last_version'
        // TODO: This most likely should throw an exception ?
        TIGHTDB_ASSERT_3(last_version, <=, preamble->end_commit_range);

        if (last_version <= preamble->end_commit_range) {
            preamble->last_server_version = preamble->last_server_version_backup;
            if (last_version < preamble->begin_newest_commit_range) {
                // writepoint is somewhere in the in-active (oldest) file, so
                // discard data in the active file, and make the in-active file active
                preamble->end_commit_range = preamble->begin_newest_commit_range;
                preamble->begin_newest_commit_range = preamble->begin_oldest_commit_range;
                preamble->active_file_is_log_a = ! preamble->active_file_is_log_a;
            }
            // writepoint is somewhere in the active file.
            // We scan from the start to find it.
            TIGHTDB_ASSERT_3(last_version, >=, preamble->begin_newest_commit_range);
            CommitLogMetadata* active_log = get_active_log(preamble);
            remap_if_needed(*active_log);
            version_type current_version;
            const char* old_buffer;
            const char* buffer;
            get_buffers_in_order(preamble, old_buffer, buffer);
            preamble->write_offset = 0;
            for (current_version = preamble->begin_newest_commit_range;
                 current_version < last_version;
                 current_version++) {
                // advance write ptr to next buffer start:
                const EntryHeader* hdr = reinterpret_cast<const EntryHeader*>(buffer + preamble->write_offset);
                uint_fast64_t size = hdr->size;
                uint_fast64_t tmp_offset = preamble->write_offset + sizeof(EntryHeader);
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


void WriteLogCollector::get_sync_info(uint_fast64_t& client_file_ident,
                                      version_type& server_version,
                                      version_type& client_version)
{
    map_header_if_needed(); // Throws
    RobustLockGuard rlg(m_header.get_addr()->lock, &recover_from_dead_owner);
    const CommitLogPreamble* preamble = get_preamble();
    client_file_ident = preamble->sync_info.client_file_ident;
    server_version    = preamble->sync_info.server_version;
    client_version    = preamble->sync_info.client_version;
}


void WriteLogCollector::set_client_file_ident(uint_fast64_t client_file_ident)
{
    map_header_if_needed(); // Throws
    RobustLockGuard rlg(m_header.get_addr()->lock, &recover_from_dead_owner);
    CommitLogPreamble* preamble = get_preamble_for_write();
    TIGHTDB_ASSERT(client_file_ident != 0 && preamble->sync_info.client_file_ident == 0);
    preamble->sync_info.client_file_ident = client_file_ident;
    sync_header();
}


void WriteLogCollector::set_sync_progress(version_type server_version, version_type client_version)
{
    map_header_if_needed(); // Throws
    RobustLockGuard rlg(m_header.get_addr()->lock, &recover_from_dead_owner);
    CommitLogPreamble* preamble = get_preamble_for_write();
    TIGHTDB_ASSERT(server_version >= preamble->sync_info.server_version);
    TIGHTDB_ASSERT(client_version >= preamble->sync_info.client_version);
    preamble->sync_info.server_version = server_version;
    preamble->sync_info.client_version = client_version;
    cleanup_stale_versions(preamble);
    sync_header();
}


// FIXME: Finn, `map_header_if_needed()` can throw, so it is an error to declare
// this one `noexcept`
void WriteLogCollector::set_last_version_seen_locally(version_type last_seen_version_number)
    TIGHTDB_NOEXCEPT
{
    map_header_if_needed();
    RobustLockGuard rlg(m_header.get_addr()->lock, &recover_from_dead_owner);
    CommitLogPreamble* preamble = get_preamble_for_write();
    preamble->last_version_seen_locally = last_seen_version_number;
    cleanup_stale_versions(preamble);
    sync_header();
}


void WriteLogCollector::set_log_entry_internal(Replication::CommitLogEntry* entry,
                                const EntryHeader* hdr, const char* log)
{
    entry->timestamp = hdr->timestamp;
    entry->peer_id = hdr->peer_id;
    entry->peer_version = hdr->peer_version;
    entry->log_data = BinaryData(log, hdr->size);
}

void WriteLogCollector::set_log_entry_internal(BinaryData* entry,
                                const EntryHeader* hdr, const char* log)
{
    *entry = BinaryData(log, hdr->size);
}

void WriteLogCollector::get_commit_entries(version_type from_version, version_type to_version,
                                           Replication::CommitLogEntry* logs_buffer) TIGHTDB_NOEXCEPT
{
    get_commit_entries_internal(from_version, to_version, logs_buffer);
}


void WriteLogCollector::get_commit_entries(version_type from_version, version_type to_version,
                                           BinaryData* logs_buffer) TIGHTDB_NOEXCEPT
{
    get_commit_entries_internal(from_version, to_version, logs_buffer);
}


template<typename T>
void WriteLogCollector::get_commit_entries_internal(version_type from_version, version_type to_version,
                                           T* logs_buffer) TIGHTDB_NOEXCEPT
{
    map_header_if_needed();
    RobustLockGuard rlg(m_header.get_addr()->lock, &recover_from_dead_owner);
    const CommitLogPreamble* preamble = get_preamble();
    TIGHTDB_ASSERT_3(from_version, >=, preamble->begin_oldest_commit_range);
    TIGHTDB_ASSERT_3(to_version, <=, preamble->end_commit_range);

    // - make sure the files are open and mapped, possibly update stale mappings
    remap_if_needed(m_log_a);
    remap_if_needed(m_log_b);
    // cerr << "get_commit_entries(" << from_version << ", " << to_version <<")" << endl;
    const char* buffer;
    const char* second_buffer;
    get_buffers_in_order(preamble, buffer, second_buffer);

    // setup local offset and version tracking variables if needed
    if ((m_read_version != from_version) || (m_read_version < preamble->begin_oldest_commit_range)) {
        m_read_version = preamble->begin_oldest_commit_range;
        m_read_offset = 0;
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
    // updates of read tracking (m_read_version and m_read_offset), and most
    // notably to PREVENT update of read tracking if it is unsafe, i.e. could
    // lead to problems when reading is resumed during a later call.
    for (;;) {

        // switch from first to second file if needed (at most once)
        if (second_buffer && m_read_version >= preamble->begin_newest_commit_range) {
            buffer = second_buffer;
            second_buffer = 0;
            m_read_offset = 0;
            // cerr << "  -- switching from first to second file" << endl;
        }

        // this condition cannot be moved to be a condition for the entire while
        // loop, because we need to do the above updates to read tracking
        if (m_read_version >= to_version)
            break;

        // follow buffer layout
        const EntryHeader* hdr = reinterpret_cast<const EntryHeader*>(buffer + m_read_offset);
        uint_fast64_t tmp_offset = m_read_offset + sizeof(EntryHeader);
        if (m_read_version >= from_version) {
            // cerr << "  --at: " << m_read_offset << ", " << size << endl;
            set_log_entry_internal(logs_buffer, hdr, buffer+tmp_offset);
            ++logs_buffer;
        }
        // break early to avoid updating tracking information, if we've reached
        // past the final entry.. We CAN resume from the final entry, but we
        // cannot safely resume once we've read past the final entry. The reason
        // is that an intervening call to set_oldest_version could shift the
        // write point to the beginning of the other file.
        if (m_read_version+1 >= preamble->end_commit_range)
            break;
        uint_fast64_t size = aligned_to(sizeof (uint64_t), hdr->size);
        m_read_offset = tmp_offset + size;
        m_read_version++;
    }
}


class WriteLogCollector::IndexTranslationMap {
public:
    IndexTranslationMap(WriteLogCollector& log, uint64_t self_peer_id, uint_fast64_t timestamp,
        uint_fast64_t peer_id, version_type base_version, version_type current_version):
    m_log(log), m_self_peer_id(self_peer_id), m_base_version(base_version),
    m_current_version(current_version), m_timestamp(timestamp), m_peer_id(peer_id)
    {
        // Build the index maps:
        std::vector<Replication::CommitLogEntry> history(m_current_version - m_base_version);
        m_log.get_commit_entries(m_base_version, m_current_version, &*history.begin());

        for (size_t i = 0; i < history.size(); ++i) {
            const CommitLogEntry& entry = history[i];
            m_commit_log_peer_id = entry.peer_id;
            m_commit_log_timestamp = entry.timestamp;
            SimpleInputStream input(entry.log_data.data(), entry.log_data.size());
            TransactLogParser parser(input);
            parser.parse(*this);
        }
    }

    // FIXME: Translate by full path instead of just group_level_ndx
    size_t translate_row_index(size_t group_level_ndx, size_t row_ndx, size_t num_rows, bool* overwritten)
    {
        std::map<size_t, MergeIndexMap>::iterator it = m_map.find(group_level_ndx);
        size_t result = row_ndx;
        if (it == m_map.end()) {
            // Nothing happened locally to the table.
            return result;
        }

        if (overwritten) {
            *overwritten = false; // FIXME: Support sets (= detect overwritten values)
            result = it->second.transform_set(row_ndx, m_timestamp, m_peer_id);
        }
        else { // overwritten == null_ptr => // insertion
            result = it->second.transform_insert(row_ndx, num_rows, m_timestamp, m_peer_id);
        }

        return result;
    }

    void insertions(size_t row_ndx, size_t num) {
        //std::cout << "Saw insert(" << row_ndx << ", " << num << ")\n";
        std::map<size_t, MergeIndexMap>::iterator it = m_map.find(m_selected_table);
        if (it == m_map.end()) {
            it = m_map.insert(std::make_pair(m_selected_table, MergeIndexMap(m_self_peer_id))).first;
        }

        MergeIndexMap& map = it->second;
        if (m_commit_log_peer_id != m_peer_id) {
            map.unknown_insertion_at(row_ndx, num, m_timestamp, m_peer_id);
        }
        else {
            map.known_insertion_at(row_ndx, num);
        }
    }

    void update(size_t row_ndx) {
        // FIXME: TODO
        static_cast<void>(row_ndx);
    }

    bool insert_group_level_table(std::size_t, std::size_t, StringData) { return true; }
    bool erase_group_level_table(std::size_t, std::size_t) { return true; }
    bool rename_group_level_table(std::size_t, StringData) { return true; }
    bool select_table(std::size_t group_level_ndx, int levels, const std::size_t* path)
    {
        // FIXME: Translate path, and support lookup by the full path
        static_cast<void>(levels);
        static_cast<void>(path);
        m_selected_table = group_level_ndx;
        return true;
    }
    bool insert_empty_rows(std::size_t row_ndx, std::size_t num_rows, std::size_t, bool) { insertions(row_ndx, num_rows); return true; }
    bool erase_rows(std::size_t row_ndx, std::size_t num_rows, std::size_t, bool)
    {
        // FIXME: Handle deletes
        static_cast<void>(row_ndx);
        static_cast<void>(num_rows);
        return true;
    }
    bool clear_table() { return true; }
    bool insert_int(std::size_t, std::size_t row_ndx, std::size_t, int_fast64_t) { insertions(row_ndx, 1); return true; }
    bool insert_bool(std::size_t, std::size_t row_ndx, std::size_t, bool) { insertions(row_ndx, 1); return true; }
    bool insert_float(std::size_t, std::size_t row_ndx, std::size_t, float) { insertions(row_ndx, 1); return true; }
    bool insert_double(std::size_t, std::size_t row_ndx, std::size_t, double) { insertions(row_ndx, 1); return true; }
    bool insert_string(std::size_t, std::size_t row_ndx, std::size_t, StringData) { insertions(row_ndx, 1); return true; }
    bool insert_binary(std::size_t, std::size_t row_ndx, std::size_t, BinaryData) { insertions(row_ndx, 1); return true; }
    bool insert_date_time(std::size_t, std::size_t row_ndx, std::size_t, DateTime) { insertions(row_ndx, 1); return true; }
    bool insert_table(std::size_t, std::size_t row_ndx, std::size_t) { insertions(row_ndx, 1); return true; }
    bool insert_mixed(std::size_t, std::size_t row_ndx, std::size_t, const Mixed&) { insertions(row_ndx, 1); return true; }
    bool insert_link(std::size_t, std::size_t row_ndx, std::size_t, std::size_t) { insertions(row_ndx, 1); return true; }
    bool insert_link_list(std::size_t, std::size_t row_ndx, std::size_t) { insertions(row_ndx, 1); return true; }
    bool row_insert_complete() { return true; }
    bool set_int(std::size_t, std::size_t row_ndx, int_fast64_t) { update(row_ndx); return true; }
    bool set_bool(std::size_t, std::size_t row_ndx, bool) { update(row_ndx); return true; }
    bool set_float(std::size_t, std::size_t row_ndx, float) { update(row_ndx); return true; }
    bool set_double(std::size_t, std::size_t row_ndx, double) { update(row_ndx); return true; }
    bool set_string(std::size_t, std::size_t row_ndx, StringData) { update(row_ndx); return true; }
    bool set_binary(std::size_t, std::size_t row_ndx, BinaryData) { update(row_ndx); return true; }
    bool set_date_time(std::size_t, std::size_t row_ndx, DateTime) { update(row_ndx); return true; }
    bool set_table(std::size_t, std::size_t row_ndx) { update(row_ndx); return true; }
    bool set_mixed(std::size_t, std::size_t row_ndx, const Mixed&) { update(row_ndx); return true; }
    bool set_link(std::size_t, std::size_t row_ndx, std::size_t) { update(row_ndx); return true; }
    bool add_int_to_column(std::size_t, int_fast64_t) { return true; }
    bool optimize_table() { return true; }
    bool select_descriptor(int levels, const std::size_t* path)
    {
        // FIXME: Probably do something?
        static_cast<void>(levels);
        static_cast<void>(path);
        return true;
    }
    bool insert_link_column(std::size_t, DataType, StringData, std::size_t, std::size_t) { return true; }
    bool insert_column(std::size_t, DataType, StringData) { return true; }
    bool erase_link_column(std::size_t, std::size_t, std::size_t) { return true; }
    bool erase_column(std::size_t) { return true; }
    bool rename_column(std::size_t, StringData) { return true; }
    bool add_search_index(std::size_t) { return true; }
    bool add_primary_key(std::size_t) { return true; }
    bool remove_primary_key() { return true; }
    bool set_link_type(std::size_t, LinkType) { return true; }
    bool select_link_list(std::size_t, std::size_t) { return true; }
    bool link_list_set(std::size_t, std::size_t) { return true; }
    bool link_list_insert(std::size_t, std::size_t) { return true; }
    bool link_list_move(std::size_t, std::size_t) { return true; }
    bool link_list_erase(std::size_t) { return true; }
    bool link_list_clear() { return true; }
private:
    WriteLogCollector& m_log;
    uint64_t m_self_peer_id;

    // FIXME: Store maps for all paths, and handle insertions/removals
    // along the paths.
    typedef _impl::MergeIndexMap MergeIndexMap;
    std::map<size_t, MergeIndexMap> m_map;

    uint64_t m_base_version;
    uint64_t m_current_version;

    // There are the incoming values:
    uint64_t m_timestamp;
    uint64_t m_peer_id;

    // These are used for storing information about commit log entries that
    // isn't available to the parser, but needed to build the map.
    uint64_t m_commit_log_peer_id;
    uint64_t m_commit_log_timestamp;

    size_t m_selected_table; // "group_level_ndx"
};

class WriteLogCollector::TransformChangesetBeforeMerge {
public:
    TransformChangesetBeforeMerge(WriteLogCollector& log, uint64_t self_peer_id, uint_fast64_t timestamp,
        uint_fast64_t peer_id, version_type base_version, version_type current_version):
    m_map(log, self_peer_id, timestamp, peer_id, base_version, current_version),
    m_transformed(m_buffer),
    m_selected_table(-1)
    {
    }

    size_t transformed_size() const
    {
        return m_transformed.write_position() - m_buffer.transact_log_data();
    }

    BinaryData get_data() const
    {
        return BinaryData(m_buffer.transact_log_data(), transformed_size());
    }

    size_t transform_insertion(size_t row_ndx, size_t num_rows = 1)
    {
        TIGHTDB_ASSERT(m_selected_table != size_t(-1));
        size_t result = m_map.translate_row_index(m_selected_table, row_ndx, num_rows, null_ptr);
        //std::cout << "ROW INSERT TRANSLATION (table " << m_selected_table << "): " << row_ndx << " -> " << result << "\n";
        return result;
    }

    size_t transform_update(size_t row_ndx, bool* overwritten)
    {
        TIGHTDB_ASSERT(m_selected_table != size_t(-1));
        size_t result = m_map.translate_row_index(m_selected_table, row_ndx, 0, overwritten);
        //std::cout << "ROW UPDATE TRANSLATION (table " << m_selected_table << "): " << row_ndx << " -> " << result << "\n";
        return result;
    }

    /// Parser InstructionHandler:

    bool insert_group_level_table(std::size_t table_ndx, std::size_t num_tables, StringData name)
    {
        // FIXME: Transform group-level
        return m_transformed.insert_group_level_table(table_ndx, num_tables, name);
    }
    bool erase_group_level_table(std::size_t table_ndx, std::size_t num_tables)
    {
        // FIXME: Transform group-level
        return m_transformed.erase_group_level_table(table_ndx, num_tables);
    }
    bool rename_group_level_table(std::size_t table_ndx, StringData new_name)
    {
        // FIXME: Transform group-level
        return m_transformed.rename_group_level_table(table_ndx, new_name);
    }
    bool select_table(std::size_t group_level_ndx, int levels, const std::size_t* path)
    {
        // FIXME: Transform path (and use entire path for lookup)
        m_selected_table = group_level_ndx;
        TIGHTDB_ASSERT(levels == 0); // Non-group-level tables not supported!
        return m_transformed.select_table(group_level_ndx, levels, path);
    }
    bool insert_empty_rows(std::size_t row_ndx, std::size_t num_rows, std::size_t tbl_sz, bool unordered)
    {
        return m_transformed.insert_empty_rows(transform_insertion(row_ndx, num_rows), num_rows, tbl_sz, unordered);
    }
    bool erase_rows(std::size_t row_ndx, std::size_t num_rows, std::size_t tbl_sz, bool unordered)
    {
        // FIXME: We need to translate each index in the range separately.
        // FIXME: Support erase
        return m_transformed.erase_rows(row_ndx, num_rows, tbl_sz, unordered);
    }
    bool clear_table()
    {
        return m_transformed.clear_table();
    }
    bool insert_int(std::size_t col_ndx, std::size_t row_ndx, std::size_t tbl_sz, int_fast64_t value)
    {
        return m_transformed.insert_int(col_ndx, transform_insertion(row_ndx), tbl_sz, value);
    }
    bool insert_bool(std::size_t col_ndx, std::size_t row_ndx, std::size_t tbl_sz, bool value)
    {
        return m_transformed.insert_bool(col_ndx, transform_insertion(row_ndx), tbl_sz, value);
    }
    bool insert_float(std::size_t col_ndx, std::size_t row_ndx, std::size_t tbl_sz, float value)
    {
        return m_transformed.insert_float(col_ndx, transform_insertion(row_ndx), tbl_sz, value);
    }
    bool insert_double(std::size_t col_ndx, std::size_t row_ndx, std::size_t tbl_sz, double value)
    {
        return m_transformed.insert_double(col_ndx, transform_insertion(row_ndx), tbl_sz, value);
    }
    bool insert_string(std::size_t col_ndx, std::size_t row_ndx, std::size_t tbl_sz, StringData value)
    {
        return m_transformed.insert_string(col_ndx, transform_insertion(row_ndx), tbl_sz, value);
    }
    bool insert_binary(std::size_t col_ndx, std::size_t row_ndx, std::size_t tbl_sz, BinaryData value)
    {
        return m_transformed.insert_binary(col_ndx, transform_insertion(row_ndx), tbl_sz, value);
    }
    bool insert_date_time(std::size_t col_ndx, std::size_t row_ndx, std::size_t tbl_sz, DateTime value)
    {
        return m_transformed.insert_date_time(col_ndx, transform_insertion(row_ndx), tbl_sz, value);
    }
    bool insert_table(std::size_t col_ndx, std::size_t row_ndx, std::size_t tbl_sz)
    {
        return m_transformed.insert_table(col_ndx, tbl_sz, transform_insertion(row_ndx));
    }
    bool insert_mixed(std::size_t col_ndx, std::size_t row_ndx, std::size_t tbl_sz, const Mixed& value)
    {
        return m_transformed.insert_mixed(col_ndx, transform_insertion(row_ndx), tbl_sz, value);
    }
    bool insert_link(std::size_t col_ndx, std::size_t row_ndx, std::size_t tbl_sz, std::size_t value)
    {
        return m_transformed.insert_link(col_ndx, transform_insertion(row_ndx), tbl_sz, value);
    }
    bool insert_link_list(std::size_t col_ndx, std::size_t row_ndx, std::size_t tbl_sz)
    {
        return m_transformed.insert_link_list(col_ndx, transform_insertion(row_ndx), tbl_sz);
    }
    bool row_insert_complete()
    {
        return m_transformed.row_insert_complete();
    }
    bool set_int(std::size_t col_ndx, std::size_t row_ndx, int_fast64_t value)
    {
        bool overwritten = false;
        return m_transformed.set_int(col_ndx, transform_update(row_ndx, &overwritten), value);
    }
    bool set_bool(std::size_t col_ndx, std::size_t row_ndx, bool value)
    {
        bool overwritten = false;
        return m_transformed.set_bool(col_ndx, transform_update(row_ndx, &overwritten), value);
    }
    bool set_float(std::size_t col_ndx, std::size_t row_ndx, float value)
    {
        bool overwritten = false;
        return m_transformed.set_float(col_ndx, transform_update(row_ndx, &overwritten), value);
    }
    bool set_double(std::size_t col_ndx, std::size_t row_ndx, double value)
    {
        bool overwritten = false;
        return m_transformed.set_double(col_ndx, transform_update(row_ndx, &overwritten), value);
    }
    bool set_string(std::size_t col_ndx, std::size_t row_ndx, StringData value)
    {
        bool overwritten = false;
        return m_transformed.set_string(col_ndx, transform_update(row_ndx, &overwritten), value);
    }
    bool set_binary(std::size_t col_ndx, std::size_t row_ndx, BinaryData value)
    {
        bool overwritten = false;
        return m_transformed.set_binary(col_ndx, transform_update(row_ndx, &overwritten), value);
    }
    bool set_date_time(std::size_t col_ndx, std::size_t row_ndx, DateTime value)
    {
        bool overwritten = false;
        return m_transformed.set_date_time(col_ndx, transform_update(row_ndx, &overwritten), value);
    }
    bool set_table(std::size_t col_ndx, std::size_t row_ndx)
    {
        bool overwritten = false;
        return m_transformed.set_table(col_ndx, transform_update(row_ndx, &overwritten));
    }
    bool set_mixed(std::size_t col_ndx, std::size_t row_ndx, const Mixed& value)
    {
        bool overwritten = false;
        return m_transformed.set_mixed(col_ndx, transform_update(row_ndx, &overwritten), value);
    }
    bool set_link(std::size_t col_ndx, std::size_t row_ndx, std::size_t value)
    {
        bool overwritten = false;
        return m_transformed.set_link(col_ndx, transform_update(row_ndx, &overwritten), value);
    }
    bool add_int_to_column(std::size_t col_ndx, int_fast64_t value)
    {
        return m_transformed.add_int_to_column(col_ndx, value);
    }
    bool optimize_table()
    {
        return m_transformed.optimize_table();
    }
    bool select_descriptor(int levels, const std::size_t* path)
    {
        // FIXME: Record path to descriptor to prepare for lookup.
        return m_transformed.select_descriptor(levels, path);
    }
    bool insert_link_column(std::size_t col_ndx, DataType type, StringData name, std::size_t link_target_table_ndx, std::size_t backlink_col_ndx)
    {
        // FIXME: Support schema changes.
        return m_transformed.insert_link_column(col_ndx, type, name, link_target_table_ndx, backlink_col_ndx);
    }
    bool insert_column(std::size_t col_ndx, DataType type, StringData name)
    {
        // FIXME: Support schema changes.
        return m_transformed.insert_column(col_ndx, type, name);
    }
    bool erase_link_column(std::size_t col_ndx, std::size_t link_target_table_ndx, std::size_t backlink_col_ndx)
    {
        // FIXME: Support schema changes.
        return m_transformed.erase_link_column(col_ndx, link_target_table_ndx, backlink_col_ndx);
    }
    bool erase_column(std::size_t col_ndx)
    {
        // FIXME: Support schema changes.
        return m_transformed.erase_column(col_ndx);
    }
    bool rename_column(std::size_t col_ndx, StringData new_name)
    {
        // FIXME: Support schema changes.
        return m_transformed.rename_column(col_ndx, new_name);
    }
    bool add_search_index(std::size_t col_ndx)
    {
        // FIXME: Support column transform.
        return m_transformed.add_search_index(col_ndx);
    }
    bool add_primary_key(std::size_t col_ndx)
    {
        return m_transformed.add_primary_key(col_ndx);
    }
    bool remove_primary_key()
    {
        return m_transformed.remove_primary_key();
    }
    bool set_link_type(std::size_t col_ndx, LinkType type)
    {
        // FIXME: Support schema changes.
        return m_transformed.set_link_type(col_ndx, type);
    }
    bool select_link_list(std::size_t col_ndx, std::size_t row_ndx)
    {
        // FIXME: Support link lists.
        return m_transformed.select_link_list(col_ndx, row_ndx);
    }
    bool link_list_set(std::size_t link_ndx, std::size_t value)
    {
        // FIXME: Support link lists.
        return m_transformed.link_list_set(link_ndx, value);
    }
    bool link_list_insert(std::size_t link_ndx, std::size_t value)
    {
        // FIXME: Support link lists.
        return m_transformed.link_list_insert(link_ndx, value);
    }
    bool link_list_move(std::size_t old_link_ndx, std::size_t new_link_ndx)
    {
        // FIXME: Support link lists.
        return m_transformed.link_list_move(old_link_ndx, new_link_ndx);
    }
    bool link_list_erase(std::size_t link_ndx)
    {
        // FIXME: Support link lists.
        return m_transformed.link_list_erase(link_ndx);
    }
    bool link_list_clear()
    {
        // FIXME: Support link lists.
        return m_transformed.link_list_clear();
    }
private:
    IndexTranslationMap m_map;
    TransactLogBufferStream m_buffer;
    TransactLogEncoder m_transformed;

    size_t m_selected_table; // "group_level_ndx"
};


Replication::version_type
WriteLogCollector::apply_foreign_changeset(SharedGroup& sg, uint_fast64_t self_peer_id, version_type last_version_integrated_by_peer,
                                           BinaryData changeset, uint_fast64_t timestamp,
                                           uint_fast64_t peer_id, version_type peer_version,
                                           ostream* apply_log)
{
    Group& group = sg.m_group;
    TIGHTDB_ASSERT(_impl::GroupFriend::get_replication(group) == this);
    TempDisableReplication tdr(group);
    TIGHTDB_ASSERT(peer_id != 0);

    WriteTransaction transact(sg);
    version_type current_version = sg.get_current_version();

//    if (last_version_integrated_by_peer < current_version)
//        return 0;

    // FIXME: This is a temporary hack
    if (last_version_integrated_by_peer != 0) {
        if (last_version_integrated_by_peer < current_version) {
            uint_fast64_t last_client_file_ident;
            {
                map_header_if_needed(); // Throws
                RobustLockGuard rlg(m_header.get_addr()->lock, &recover_from_dead_owner);
                const CommitLogPreamble* preamble = get_preamble();
                TIGHTDB_ASSERT(preamble->end_commit_range == current_version);
                last_client_file_ident = preamble->last_client_file_ident;
            }
            // `last_client_file_ident` will be 0 if the history is empty (empty
            // database).
            bool history_ends_with_foreign_changeset = last_client_file_ident != 0;
            if (!history_ends_with_foreign_changeset)
                return 0;
        }
    }

    if (TIGHTDB_UNLIKELY(last_version_integrated_by_peer > current_version))
        throw LogicError(LogicError::bad_version_number);

    SimpleInputStream input(changeset.data(), changeset.size());
    TransformChangesetBeforeMerge transform(*this, self_peer_id, timestamp, peer_id,
        last_version_integrated_by_peer, current_version);
    TransactLogParser parser(input);
    parser.parse(transform);

    BinaryData transformed_changeset = transform.get_data();
    SimpleInputStream transformed_input(transformed_changeset.data(), transformed_changeset.size());
    apply_transact_log(transformed_input, transact.get_group(), apply_log); // Throws
    internal_submit_log(transformed_changeset.data(), transformed_changeset.size(), timestamp, peer_id, peer_version); // Throws

    return transact.commit(); // Throws
}

Replication::version_type
WriteLogCollector::get_last_peer_version(uint_fast64_t)
{
    map_header_if_needed();
    CommitLogPreamble* preamble = const_cast<WriteLogCollector*>(this)->get_preamble();
    return preamble->last_server_version;
}


WriteLogCollector::version_type
WriteLogCollector::do_commit_write_transact(SharedGroup&,
                                            WriteLogCollector::version_type orig_version)
{
    char* data = m_transact_log_buffer.data();
    uint_fast64_t size = write_position() - data;
    uint_fast64_t timestamp = get_current_timestamp();
    uint_fast64_t peer_id = 0;
    uint_fast64_t peer_version = get_last_peer_version(1);
    version_type from_version = internal_submit_log(data, size, timestamp, peer_id, peer_version);
    TIGHTDB_ASSERT_3(from_version, == , orig_version);
    static_cast<void>(from_version);
    version_type new_version = orig_version + 1;
    return new_version;
}


void WriteLogCollector::do_begin_write_transact(SharedGroup&)
{
    char* buffer = m_transact_log_buffer.data();
    set_buffer(buffer, buffer + m_transact_log_buffer.size());
}


void WriteLogCollector::do_rollback_write_transact(SharedGroup& sg) TIGHTDB_NOEXCEPT
{
    // forward transaction log buffer
    sg.do_rollback_and_continue_as_read(m_transact_log_buffer.data(), write_position());
}



void WriteLogCollector::transact_log_append(const char* data, size_t size, char** new_begin, char** new_end)
{
    transact_log_reserve(size, new_begin, new_end);
    *new_begin = copy(data, data + size, *new_begin);
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


WriteLogCollector::WriteLogCollector(string database_name,
                                     bool server_synchronization_mode,
                                     const char* encryption_key):
    m_log_a(database_name + ".log_a"),
    m_log_b(database_name + ".log_b")
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



Replication* makeWriteLogCollector(string database_name,
                                   bool server_synchronization_mode,
                                   const char* encryption_key)
{
    return new _impl::WriteLogCollector(database_name,
                                        server_synchronization_mode,
                                        encryption_key);
}

uint_fast64_t get_current_timestamp()
{
    struct timeval tv;
    gettimeofday(&tv, null_ptr);
    return tv.tv_sec * 1000000 + tv.tv_usec;
}


} // namespace tightdb

#endif // TIGHTDB_ENABLE_REPLICATION
