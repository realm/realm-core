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
#include <tightdb/alloc.hpp>

namespace tightdb {

// Pre-declarations
class Group;
class SlabAlloc;

class GroupWriter {
public:
    GroupWriter(Group&);

    void set_versions(std::size_t current, std::size_t read_lock);

    /// Returns the new top ref.
    ref_type commit(bool do_sync);

    std::size_t get_file_size() const TIGHTDB_NOEXCEPT;

    /// Write the specified chunk into free space.
    ///
    /// Returns the position in the file where the first byte was
    /// written.
    std::size_t write(const char* data, std::size_t size);

    void write_at(std::size_t pos, const char* data, std::size_t size);

#ifdef TIGHTDB_DEBUG
    void dump();
#endif

private:
    Group&          m_group;
    SlabAlloc&      m_alloc;
    std::size_t     m_current_version;
    std::size_t     m_readlock_version;
    File::Map<char> m_file_map;

    // Controlled update of physical medium
    void sync(uint64_t top_pos);

    std::size_t get_free_space(std::size_t len);
    std::size_t reserve_free_space(std::size_t len, std::size_t start=0);
    void        add_free_space(std::size_t pos, std::size_t len, std::size_t version=0);
    void        merge_free_space();
    std::size_t extend_free_space(std::size_t len);
};




// Implementation:

inline std::size_t GroupWriter::get_file_size() const TIGHTDB_NOEXCEPT
{
    return m_file_map.get_size();
}

} // namespace tightdb

#endif // TIGHTDB_GROUP_WRITER_HPP
