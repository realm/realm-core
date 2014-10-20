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
    virtual void reset_log_management() TIGHTDB_OVERRIDE;
    virtual void set_oldest_version_needed(uint_fast64_t last_seen_version_number) TIGHTDB_NOEXCEPT;
    virtual void get_commit_entries(uint_fast64_t from_version, uint_fast64_t to_version,
                                    BinaryData* logs_buffer) TIGHTDB_NOEXCEPT;
protected:
    static const size_t page_size = 4096; // file and memory mappings are always multipla of this size
    static const size_t minimal_pages = 1; 
    // Layout of the commit logs preamble. The preamble is placed at the start of the first commitlog file.
    // (it is placed at the start of both files, but only the preamble in m_log_a is used).
    struct CommitLogPreamble {
        // lock:
        RobustMutex lock;
        // indicates which file is active/beeing written.
        bool active_file_is_log_a;

        // The following are monotonically increasing:
        size_t begin_oldest_commit_range; // for commits residing in in-active file
        size_t begin_newest_commit_range; // for commits residing in the active file
        size_t end_commit_range;
        // The log bringing us from state A to state A+1 is given the number A.
        // The end_commit_range is a traditional C++ limit, it points one past the last number
        size_t write_offset; // within active file, value always kept aligned to size_t

        // proper intialization:
        CommitLogPreamble() : lock() 
        { 
            active_file_is_log_a = true;
            // The first commit will be from state 1 -> state 2, so we must set 1 initially
            begin_oldest_commit_range = begin_newest_commit_range = end_commit_range = 1;
            write_offset = sizeof(*this);
        }
    };
    // Each of the actual logs are preceded by their size (in size_t format), and each log start
    // aligned to size_t (required on some architectures). The size does not count any padding
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

    // Ensure the log files memory mapping is up to date (for full portability, the
    // mapping needs to be changed if the size of the file has changed since the previous mapping).
    void remap_if_needed(CommitLogMetadata& log);

    // Reset mapping and file
    void reset_file(CommitLogMetadata& log);

    // Get the commitlogs in order of their commits.
    void get_logs_in_order(CommitLogMetadata*& first, CommitLogMetadata*& second);
};

// little helper:
std::size_t aligned_to(std::size_t alignment, std::size_t value)
{
    return (value + alignment - 1) & ~(alignment - 1);
}

void WriteLogCollector::remap_if_needed(CommitLogMetadata& log)
{
    if (log.file.is_attached() == false) {
        log.file.open(log.name, File::mode_Update);
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

WriteLogCollector::~WriteLogCollector() TIGHTDB_NOEXCEPT 
{    
    m_log_a.map.unmap();
    m_log_a.file.close();
    m_log_b.map.unmap();
    m_log_b.file.close();
}

void WriteLogCollector::reset_log_management()
{
    // this call is only made on the replication object associated with the *first* SharedGroup
    // it must (re)initialize the log files. It does not change the content of any already existing
    // files, but instead deletes and re-creates the files.
    // it also sets the intial memory mappings for the files.
    reset_file(m_log_a);
    reset_file(m_log_b);
    new(m_log_a.map.get_addr()) CommitLogPreamble();
}

void recover_from_dead_owner()
{
    // nothing!
}

void WriteLogCollector::set_oldest_version_needed(version_type last_seen_version_number) TIGHTDB_NOEXCEPT
{
    // this call should only update in-file information, possibly recycling file usage
    // if a file holds only versions before last_seen_version_number, it can be recycled.
    // recycling is done by updating the preamble
    remap_if_needed(m_log_a);
    remap_if_needed(m_log_b);
    CommitLogPreamble* preamble = m_log_a.map.get_addr();
    {
        preamble->lock.lock(&recover_from_dead_owner);
        if (last_seen_version_number >= preamble->begin_newest_commit_range) {
            // oldest file holds only stale commitlogs, so let's swap files and update the range
            preamble->active_file_is_log_a = !preamble->active_file_is_log_a;
            preamble->begin_oldest_commit_range = preamble->begin_newest_commit_range;
            preamble->begin_newest_commit_range = preamble->end_commit_range;
            preamble->write_offset = sizeof(CommitLogPreamble);
        }
        preamble->lock.unlock();
    }
}


void WriteLogCollector::get_commit_entries(version_type from_version, version_type to_version,
                                           BinaryData* logs_buffer) TIGHTDB_NOEXCEPT
{
    // - make sure the files are open and mapped, possibly update stale mappings
    remap_if_needed(m_log_a);
    remap_if_needed(m_log_b);
    CommitLogPreamble* preamble = m_log_a.map.get_addr();
    {
        preamble->lock.lock(&recover_from_dead_owner);
        size_t version;
        char* log_buffer;
        // traverse commits in first file (first file is the opposite of the active file):
        if (preamble->active_file_is_log_a)
            log_buffer = reinterpret_cast<char*>(1+m_log_b.map.get_addr());
        else
            log_buffer = reinterpret_cast<char*>(1+m_log_a.map.get_addr());
        TIGHTDB_ASSERT(from_version >= preamble->begin_oldest_commit_range);
        TIGHTDB_ASSERT(to_version <= preamble->end_commit_range);
        for (version = preamble->begin_oldest_commit_range; version < preamble->begin_newest_commit_range; version++) {
            if (from_version >= to_version) // early out?
                break;
            if (version == from_version) {
                size_t size = *reinterpret_cast<size_t*>(log_buffer);
                *logs_buffer = BinaryData(log_buffer+sizeof(size_t), size);
                ++logs_buffer;
                // align:
                size = (size + sizeof(size_t)-1) & ~(sizeof(size_t)-1);
                log_buffer += size;
                from_version++;
            }
        }
        // then second file:
        if (preamble->active_file_is_log_a == false)
            log_buffer = reinterpret_cast<char*>(1+m_log_b.map.get_addr());
        else
            log_buffer = reinterpret_cast<char*>(1+m_log_a.map.get_addr());
        for (version = preamble->begin_newest_commit_range; version < preamble->end_commit_range; version++) {
            if (from_version >= to_version) // early out?
                break;
            if (version == from_version) {
                size_t size = *reinterpret_cast<size_t*>(log_buffer);
                *logs_buffer = BinaryData(log_buffer+sizeof(size_t), size);
                ++logs_buffer;
                // align:
                size = (size + sizeof(size_t)-1) & ~(sizeof(size_t)-1);
                log_buffer += size;
                from_version++;
            }
        }
        preamble->lock.unlock();
    }
}


WriteLogCollector::version_type 
WriteLogCollector::do_commit_write_transact(SharedGroup& sg, 
	WriteLogCollector::version_type orig_version)
{
    // IMPORTANT: To be called only under protection of the global write mutex!
    // determine which file is the one to append to.
    // resize it as needed.
    // change the mapping to match if needed.
    // copy the log to the end of memory area
    // update metadata making the new log entry visible to readers
    static_cast<void>(sg);
    char* data     = m_transact_log_buffer.release();
    std::size_t sz = m_transact_log_free_begin - data;
    version_type new_version = orig_version + 1;
    remap_if_needed(m_log_a);
    remap_if_needed(m_log_b);
    CommitLogPreamble* preamble = m_log_a.map.get_addr();
    {
        preamble->lock.lock(&recover_from_dead_owner);
        CommitLogMetadata* active_log;
        if (preamble->active_file_is_log_a)
            active_log = &m_log_a;
        else
            active_log = &m_log_b;

        // make sure we have space (allocate if not)
        File::SizeType size_needed = aligned_to(sizeof(size_t), preamble->write_offset + sizeof(size_t) + sz);
        size_needed = aligned_to(page_size, size_needed);
        if (size_needed > active_log->file.get_size()) {
            active_log->file.resize(size_needed);
        }

        // change mapping so that we can write:
        remap_if_needed(*active_log);

        // append data from write pointer and onwards:
        char* write_ptr = reinterpret_cast<char*>(active_log->map.get_addr()) + preamble->write_offset;
        *reinterpret_cast<size_t*>(write_ptr) = sz;
        write_ptr += sizeof(size_t);
        std::copy(data, data+sz, write_ptr);
        // update metadata to reflect the added commit log
        preamble->write_offset += aligned_to(sizeof(size_t), sz + sizeof(size_t));
        TIGHTDB_ASSERT(orig_version == preamble->end_commit_range);
        preamble->end_commit_range = new_version;
        preamble->lock.unlock();
    }
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
}

} // namespace _impl




Replication* makeWriteLogCollector(std::string database_name)
{
    return  new _impl::WriteLogCollector(database_name);
}



} // namespace tightdb
#endif // TIGHTDB_ENABLE_REPLICATION
