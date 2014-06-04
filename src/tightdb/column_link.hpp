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
#ifndef TIGHTDB_COLUMN_LINK_HPP
#define TIGHTDB_COLUMN_LINK_HPP

#include <tightdb/column.hpp>
#include <tightdb/column_linkbase.hpp>
//#include <tightdb/table.hpp>
#include <tightdb/column_backlink.hpp>

namespace tightdb {

/// An link column (ColumnLink) is a single B+-tree, and the root of
/// the column is the root of the B+-tree. All leaf nodes are single
/// arrays of type Array.
///
/// The individual values in the column are row positions in the target
/// table (offset with one to allow zero to indicate null links)
class ColumnLink: public ColumnLinkBase {
public:
    ColumnLink(ref_type ref, ArrayParent* parent = 0, std::size_t ndx_in_parent = 0,
                    Allocator& alloc = Allocator::get_default()); // Throws
    ColumnLink(Allocator& alloc);
    ~ColumnLink() TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE {}

    static ref_type create(std::size_t size, Allocator&);

    // Getting and modifying links
    bool is_null_link(std::size_t row_ndx) const TIGHTDB_NOEXCEPT;
    size_t get_link(std::size_t row_ndx) const TIGHTDB_NOEXCEPT;
    void set_link(std::size_t row_ndx, std::size_t target_row_ndx);
    void insert_link(std::size_t row_ndx, std::size_t target_row_ndx);
    void nullify_link(std::size_t row_ndx);

    void clear() TIGHTDB_OVERRIDE;
    void erase(std::size_t, bool) TIGHTDB_OVERRIDE;
    void move_last_over(std::size_t, std::size_t) TIGHTDB_OVERRIDE;

    // ColumnLinkBase overrides
    void set_target_table(TableRef table) TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    TableRef get_target_table() TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    void set_backlink_column(ColumnBackLink& backlinks) TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;

protected:
    friend class ColumnBackLink;
    void do_nullify_link(std::size_t row_ndx, std::size_t old_target_row_ndx);
    void do_update_link(std::size_t row_ndx, std::size_t old_target_row_ndx, std::size_t new_target_row_ndx);

private:
    void remove_backlinks(size_t row_ndx);

    TableRef m_target_table;
    ColumnBackLink* m_backlinks;
};


// Implementation

inline ColumnLink::ColumnLink(ref_type ref, ArrayParent* parent, std::size_t ndx_in_parent, Allocator& alloc):
    ColumnLinkBase(ref, parent, ndx_in_parent, alloc), m_backlinks(null_ptr)
{
}

inline ColumnLink::ColumnLink(Allocator& alloc):
    ColumnLinkBase(alloc), m_backlinks(null_ptr)
{
}

inline ref_type ColumnLink::create(std::size_t size, Allocator& alloc)
{
    int_fast64_t value = 0;
    return Column::create(Array::type_Normal, size, value, alloc); // Throws
}

inline void ColumnLink::set_target_table(TableRef table) TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(m_target_table.get() == null_ptr);
    m_target_table = table;
}

inline TableRef ColumnLink::get_target_table() TIGHTDB_NOEXCEPT
{
    return m_target_table;
}

inline void ColumnLink::set_backlink_column(ColumnBackLink& backlinks) TIGHTDB_NOEXCEPT
{
    m_backlinks = &backlinks;
}

inline bool ColumnLink::is_null_link(std::size_t row_ndx) const TIGHTDB_NOEXCEPT
{
    // Zero indicates a missing (null) link
    return (Column::get(row_ndx) == 0);
}

inline size_t ColumnLink::get_link(std::size_t row_ndx) const TIGHTDB_NOEXCEPT
{
    // Row pos is offset by one, to allow null refs
    return Column::get(row_ndx) - 1;
}

inline void ColumnLink::insert_link(std::size_t row_ndx, std::size_t target_row_ndx)
{
    // Row pos is offsest by one, to allow null refs
    Column::insert(row_ndx, target_row_ndx + 1);

    m_backlinks->add_backlink(target_row_ndx, row_ndx);
}

inline void ColumnLink::do_nullify_link(std::size_t row_ndx, std::size_t)
{
    Column::set(row_ndx, 0);
}

inline void ColumnLink::do_update_link(std::size_t row_ndx, std::size_t, std::size_t new_target_row_ndx)
{
    // Row pos is offset by one, to allow null refs
    Column::set(row_ndx, new_target_row_ndx + 1);
}

} //namespace tightdb

#endif //TIGHTDB_COLUMN_LINK_HPP
