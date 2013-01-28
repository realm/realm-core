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
#ifndef TIGHTDB_GROUP_WRITER_HPP
#define TIGHTDB_GROUP_WRITER_HPP

#include <stdint.h> // unint8_t etc
#include <cstdlib> // size_t

#include <tightdb/file.hpp>

namespace tightdb {

// Pre-declarations
class Group;
class SlabAlloc;

class GroupWriter {
public:
    GroupWriter(Group& group, bool doPersist);

    void SetVersions(std::size_t current, std::size_t readlock);

    std::size_t Commit();

    size_t write(const char* p, std::size_t n);
    void WriteAt(std::size_t pos, const char* p, std::size_t n);

#ifdef TIGHTDB_DEBUG
    void dump();
    void ZeroFreeSpace();
#endif

private:
    void DoCommit(uint64_t topPos);

    std::size_t get_free_space(size_t len);
    std::size_t reserve_free_space(size_t len, size_t start=0);
    void        add_free_space(size_t pos, size_t len, size_t version=0);
    void        merge_free_space();
    std::size_t extend_free_space(size_t len);

    Group&          m_group;
    SlabAlloc&      m_alloc;
    std::size_t     m_current_version;
    std::size_t     m_readlock_version;
    File::Map<char> m_file_map;
    bool            m_doPersist;
};


} // namespace tightdb

#endif // TIGHTDB_GROUP_WRITER_HPP
