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
#ifndef TIGHTDB_COLUMN_LINKBASE_HPP
#define TIGHTDB_COLUMN_LINKBASE_HPP

#include <tightdb/table.hpp>

namespace tightdb {

class ColumnBackLink;

// Abstract base class for columns containing links
class ColumnLinkBase: public Column {
public:
    explicit ColumnLinkBase(Allocator&);
    ColumnLinkBase(Array::Type, Allocator&);
    explicit ColumnLinkBase(ref_type, ArrayParent* = 0, std::size_t ndx_in_parent = 0,
                            Allocator& = Allocator::get_default());

    void set_target_table(Table&) TIGHTDB_NOEXCEPT;
    Table* get_target_table() TIGHTDB_NOEXCEPT;
    void set_backlink_column(ColumnBackLink&) TIGHTDB_NOEXCEPT;

    virtual void do_nullify_link(std::size_t row_ndx, std::size_t old_target_row_ndx) = 0;
    virtual void do_update_link(std::size_t row_ndx, std::size_t old_target_row_ndx,
                                std::size_t new_target_row_ndx) = 0;

    void adj_accessors_insert_rows(std::size_t, std::size_t) TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    void adj_accessors_erase_row(std::size_t) TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    void adj_accessors_move_last_over(std::size_t, std::size_t) TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;

protected:
    TableRef m_target_table;
    ColumnBackLink* m_backlinks;
};




// Implementation

inline ColumnLinkBase::ColumnLinkBase(Allocator& alloc):
    Column(alloc),
    m_backlinks(0)
{
}

inline ColumnLinkBase::ColumnLinkBase(Array::Type type, Allocator& alloc):
    Column(type, alloc),
    m_backlinks(0)
{
}

inline ColumnLinkBase::ColumnLinkBase(ref_type ref, ArrayParent* parent, std::size_t ndx_in_parent,
                                      Allocator& alloc):
    Column(ref, parent, ndx_in_parent, alloc),
    m_backlinks(0)
{
}

inline void ColumnLinkBase::set_target_table(Table& table) TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(!m_target_table);
    m_target_table = table.get_table_ref();
}

inline Table* ColumnLinkBase::get_target_table() TIGHTDB_NOEXCEPT
{
    return m_target_table.get();
}

inline void ColumnLinkBase::set_backlink_column(ColumnBackLink& backlinks) TIGHTDB_NOEXCEPT
{
    m_backlinks = &backlinks;
}

inline void ColumnLinkBase::adj_accessors_insert_rows(std::size_t, std::size_t) TIGHTDB_NOEXCEPT
{
    typedef _impl::TableFriend tf;
    tf::mark(*m_target_table);
}

inline void ColumnLinkBase::adj_accessors_erase_row(std::size_t) TIGHTDB_NOEXCEPT
{
    // Rows cannot be erased this way in tables with link-type columns
    TIGHTDB_ASSERT(false);
}

inline void ColumnLinkBase::adj_accessors_move_last_over(std::size_t, std::size_t) TIGHTDB_NOEXCEPT
{
    typedef _impl::TableFriend tf;
    tf::mark(*m_target_table);
}


} //namespace tightdb

#endif //TIGHTDB_COLUMN_LINKBASE_HPP
