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
#ifndef TIGHTDB_COLUMN_BACKLINK_HPP
#define TIGHTDB_COLUMN_BACKLINK_HPP

#include <tightdb/column.hpp>
#include <tightdb/column_linkbase.hpp>
#include <tightdb/table.hpp>

namespace tightdb {

/// A column of backlinks (ColumnBackLink) is a single B+-tree, and the root of
/// the column is the root of the B+-tree. All leaf nodes are single arrays of
/// type Array with the hasRefs bit set.
///
/// The individual values in the column are either refs to Columns containing
/// the row indexes in the origin table that links to it, or in the case where
/// there is a single link, a tagged ref encoding the origin row position.
class ColumnBackLink: public Column, public ArrayParent {
public:
    ColumnBackLink(ref_type, ArrayParent* = 0, std::size_t ndx_in_parent = 0,
                             Allocator& = Allocator::get_default()); // Throws
    ~ColumnBackLink() TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE {}

    static ref_type create(std::size_t size, Allocator&);

    bool has_backlinks(std::size_t row_ndx) const TIGHTDB_NOEXCEPT;
    std::size_t get_backlink_count(std::size_t row_ndx) const TIGHTDB_NOEXCEPT;
    std::size_t get_backlink(std::size_t row_ndx, std::size_t backlink_ndx) const TIGHTDB_NOEXCEPT;

    void add_backlink(std::size_t row_ndx, std::size_t origin_row_ndx);
    void remove_backlink(std::size_t row_ndx, std::size_t origin_row_ndx);
    void update_backlink(std::size_t row_ndx, std::size_t old_row_ndx, std::size_t new_row_ndx);

    void add_row();

    void clear() TIGHTDB_OVERRIDE;
    void erase(std::size_t, bool) TIGHTDB_OVERRIDE;
    void move_last_over(std::size_t, std::size_t) TIGHTDB_OVERRIDE;

    // Link origination info
    void set_origin_table(Table&) TIGHTDB_NOEXCEPT;
    TableRef get_origin_table() const TIGHTDB_NOEXCEPT;
    void set_origin_column(ColumnLinkBase&) TIGHTDB_NOEXCEPT;

protected:
    // ArrayParent overrides
    void update_child_ref(std::size_t child_ndx, ref_type new_ref) TIGHTDB_OVERRIDE;
    ref_type get_child_ref(std::size_t child_ndx) const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
#ifdef TIGHTDB_DEBUG
    // Used only by Array::to_dot().
    std::pair<ref_type, std::size_t>
    get_to_dot_parent(std::size_t ndx_in_parent) const TIGHTDB_OVERRIDE;
#endif
    void bump_version_on_linked_table() TIGHTDB_NOEXCEPT;

private:
    void nullify_links(std::size_t row_ndx, bool do_destroy);

    TableRef        m_origin_table;
    ColumnLinkBase* m_origin_column;
};


// Implementation

inline void ColumnBackLink::bump_version_on_linked_table() TIGHTDB_NOEXCEPT
{
    _impl::TableFriend::bump_version(*m_origin_table);
}

inline ColumnBackLink::ColumnBackLink(ref_type ref, ArrayParent* parent, std::size_t ndx_in_parent,
                                      Allocator& alloc):
    Column(ref, parent, ndx_in_parent, alloc),
    m_origin_column(null_ptr)
{
}

inline ref_type ColumnBackLink::create(std::size_t size, Allocator& alloc)
{
    int_fast64_t value = 0;
    return Column::create(Array::type_HasRefs, size, value, alloc); // Throws
}

inline bool ColumnBackLink::has_backlinks(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    return Column::get(ndx) != 0;
}

inline void ColumnBackLink::set_origin_table(Table& table) TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(!m_origin_table);
    m_origin_table = table.get_table_ref();
}

inline TableRef ColumnBackLink::get_origin_table() const TIGHTDB_NOEXCEPT
{
    return m_origin_table;
}

inline void ColumnBackLink::set_origin_column(ColumnLinkBase& column) TIGHTDB_NOEXCEPT
{
    m_origin_column = &column;
}

inline void ColumnBackLink::add_row()
{
    Column::add(0);
}


} //namespace tightdb

#endif //TIGHTDB_COLUMN_BACKLINK_HPP
