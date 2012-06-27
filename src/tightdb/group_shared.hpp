/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
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
#ifndef TIGHTDB_GROUP_SHARED_HPP
#define TIGHTDB_GROUP_SHARED_HPP

#include "group.hpp"

#ifdef TIGHTDB_ENABLE_REPLICATION
#include <tightdb/replication.hpp>
#endif

namespace tightdb {

// Pre-declarations
struct ReadCount;
struct SharedInfo;

class SharedGroup {
public:
    SharedGroup(const char* path_to_database_file);
    ~SharedGroup();

#ifdef TIGHTDB_ENABLE_REPLICATION
    struct replication_tag {};
    SharedGroup(replication_tag);
#endif

    bool is_valid() const {return m_isValid;}

    // Read transactions
    const Group& begin_read();
    void end_read();
    
    // Write transactions
    Group& begin_write();
    void commit();
    void rollback();

#ifdef _DEBUG
    void test_ringbuf();
    void zero_free_space();
#endif

private:
    // Ring buffer managment
    bool       ringbuf_is_empty() const;
    size_t     ringbuf_size() const;
    size_t     ringbuf_capacity() const;
    bool       ringbuf_is_first(size_t ndx) const;
    void       ringbuf_put(const ReadCount& v);
    void       ringbuf_remove_first();
    size_t     ringbuf_find(uint32_t version) const;
    ReadCount& ringbuf_get(size_t ndx);
    ReadCount& ringbuf_get_first();
    ReadCount& ringbuf_get_last();
    
    // Member variables
    Group       m_group;
    SharedInfo* m_info;
    size_t      m_info_len;
    bool        m_isValid;
    uint32_t    m_version;
    int         m_fd;
    const char* m_lockfile_path;

#ifdef _DEBUG
    // In debug mode we want to track state
    enum SharedState {
        SHARED_STATE_READY,
        SHARED_STATE_READING,
        SHARED_STATE_WRITING
    };
    SharedState m_state;
#endif

#ifdef TIGHTDB_ENABLE_REPLICATION
    Replication m_replication;
#endif

    void init(const char* path_to_database_file);
};

} // namespace tightdb

#endif // TIGHTDB_GROUP_SHARED_HPP
