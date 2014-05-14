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
#include <tightdb/table.hpp>

namespace tightdb {

class ColumnBackLink: public Column {
public:
    ColumnBackLink(ref_type, ArrayParent* = 0, std::size_t ndx_in_parent = 0,
                             Allocator& = Allocator::get_default()); // Throws

    static ref_type create(std::size_t size, Allocator&);

    bool has_backlinks(std::size_t row_ndx) const TIGHTDB_NOEXCEPT;
    std::size_t get_backlink_count(std::size_t row_ndx) const TIGHTDB_NOEXCEPT;
    std::size_t get_backlink(std::size_t row_ndx, std::size_t backlink_ndx) const TIGHTDB_NOEXCEPT;

    void add_backlink(std::size_t row_ndx, std::size_t source_row_ndx);
    void remove_backlink(std::size_t row_ndx, std::size_t source_row_ndx);
    void update_backlink(std::size_t row_ndx, std::size_t old_row_ndx, std::size_t new_row_ndx);


    // Source info
    void        set_source_table(TableRef table);
    TableRef    get_source_table();
    void        set_source_column(ColumnLink& column);
    //std::size_t get_source_column();

    void add_row();
    void clear() TIGHTDB_OVERRIDE;
    void move_last_over(std::size_t ndx) TIGHTDB_OVERRIDE;
    void erase(std::size_t ndx, bool is_last) TIGHTDB_OVERRIDE;

private:
    void nullify_links(std::size_t row_ndx);

    TableRef    m_source_table;
    ColumnLink* m_source_column;
};


// Implementation

inline ColumnBackLink::ColumnBackLink(ref_type ref, ArrayParent* parent, std::size_t ndx_in_parent, Allocator& alloc):
    Column(ref, parent, ndx_in_parent, alloc), m_source_column(NULL)
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

inline void ColumnBackLink::set_source_table(TableRef table)
{
    TIGHTDB_ASSERT(m_source_table.get() == NULL);
    m_source_table = table;
}

inline TableRef ColumnBackLink::get_source_table()
{
    return m_source_table;
}

inline void ColumnBackLink::set_source_column(ColumnLink& column)
{
    m_source_column = &column;
}

inline void ColumnBackLink::add_row()
{
    Column::add(0);
}
/*
inline std::size_t ColumnBackLink::get_source_column()
{
    return m_source_column;
}*/


} //namespace tightdb

#endif //TIGHTDB_COLUMN_BACKLINK_HPP
