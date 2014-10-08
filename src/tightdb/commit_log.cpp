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
#include <tightdb/group_shared.hpp>
#include <map>
#include <tightdb/commit_log.hpp>

#include <tightdb/replication.hpp>
#ifdef TIGHTDB_ENABLE_REPLICATION

namespace {

using namespace tightdb;

typedef uint_fast64_t version_type;

namespace tightdb {


namespace _impl {

class WriteLogCollector : public Replication
{
public:
    WriteLogCollector(std::string database_name, WriteLogRegistry* registry);
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
    std::string m_database_name;
};


// Design of the commit logs:
//
// We use two files to hold the commit logs. Using two files allows us to append data to the
// end of one of the files, instead of doing complex memory management. Initially, both files
// hold only a header, and one of them is designated 'active'. New commit logs are appended
// to the active file. Each file holds a consecutive range of commits, the active file holding
// the latest commits.
//
// Calls to set_oldest_version_needed checks if the non-active file holds stale commit logs only.
// If so, the non-active file is reset and becomes active instead.
//
// Filesizes are determined by heuristics. When a file runs out of size, its size is doubled.
// When changing the active file, the new file is reset to 1/4 of the current total size, thus gradually
// decreasing file sizes (until a minimum limit is reached). A commit log entry is never split
// between the files.
//
// Calls to get_commit_entries determines which file(s) needs to be accessed, maps them to
// memory and builds a vector of BinaryData with pointers to the buffers. The pointers may
// end up going to both mappings/files.
//
// Layout of the commit logs preamble:
struct CommitLogPreample {
    size_t write_offset; // value always kept aligned to size_t
    size_t begin_commit;
    size_t end_commit; // if end_commit == begin_commit, the file is (logically) empty.
    size_t data_area_start;
};
// Each of the actual logs are preceded by their size (in size_t format), and each log start
// aligned to size_t (required on some architectures). The size does not count any padding
// needed at the end of each log.

WriteLogCollector::~WriteLogCollector() TIGHTDB_NOEXCEPT 
{
}

void WriteLogCollector::reset_log_management()
{
    // this call is only made on the replication object associated with the *first* SharedGroup
    // it must (re)initialize the log files. It does not change the content of any already existing
    // files, but instead deletes and re-creates the files.
    // it also clears the memory mappings set for the files.
}


void WriteLogCollector::set_oldest_version_needed(uint_fast64_t last_seen_version_number) TIGHTDB_NOEXCEPT
{
    // this call should only update in-file information, possibly recycling file usage
    // if a file holds only versions before last_seen_version_number, it can be recycled.
    // recycling is done by writing a new preamble and choosing an initial size of 1/4 of
    // the sum of the two file sizes.
    // it also clears the memory mappings set for the files.
}


void WriteLogCollector::get_commit_entries(uint_fast64_t from_version, uint_fast64_t to_version,
                                           BinaryData* logs_buffer) TIGHTDB_NOEXCEPT
{
    // - make sure the files are open and mapped, possibly update stale mappings
    // - for each requested version
    //   - add ref to a vector
}


WriteLogCollector::version_type 
WriteLogCollector::do_commit_write_transact(SharedGroup& sg, 
	WriteLogCollector::version_type orig_version)
{
    // determine which file is the one to append to.
    // resize it as needed.
    // change the mapping to match if needed.
    // copy the log to the end of memory area
    // update metadata making the new log entry visible to readers
    static_cast<void>(sg);
    char* data     = m_transact_log_buffer.release();
    std::size_t sz = m_transact_log_free_begin - data;
    version_type new_version = orig_version + 1;
    m_registry->add_commit(new_version, data, sz);
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


WriteLogCollector::WriteLogCollector(std::string database_name, WriteLogRegistry* registry)
{
    m_database_name = database_name;
    m_registry = registry;
}

} // namespace _impl




Replication* makeWriteLogCollector(std::string database_name)
{
    WriteLogRegistry* registry = globalRegistry.get(database_name);
    return  new _impl::WriteLogCollector(database_name, registry);
}



} // namespace tightdb
#endif // TIGHTDB_ENABLE_REPLICATION
