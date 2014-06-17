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

    virtual void set_target_table(Table&) TIGHTDB_NOEXCEPT = 0;
    virtual TableRef get_target_table() TIGHTDB_NOEXCEPT = 0;
    virtual void set_backlink_column(ColumnBackLink&) TIGHTDB_NOEXCEPT = 0;

    virtual void do_nullify_link(std::size_t row_ndx, std::size_t old_target_row_ndx) = 0;
    virtual void do_update_link(std::size_t row_ndx, std::size_t old_target_row_ndx,
                                std::size_t new_target_row_ndx) = 0;
};




// Implementation

inline ColumnLinkBase::ColumnLinkBase(Allocator& alloc):
    Column(alloc)
{
}

inline ColumnLinkBase::ColumnLinkBase(Array::Type type, Allocator& alloc):
    Column(type, alloc)
{
}

inline ColumnLinkBase::ColumnLinkBase(ref_type ref, ArrayParent* parent, std::size_t ndx_in_parent,
                                      Allocator& alloc):
    Column(ref, parent, ndx_in_parent, alloc)
{
}


} //namespace tightdb

#endif //TIGHTDB_COLUMN_LINKBASE_HPP
