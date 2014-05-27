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
class ColumnLinkBase
{
public:
    virtual void set_target_table(TableRef table) = 0;
    virtual TableRef get_target_table() = 0;
    virtual void set_backlink_column(ColumnBackLink& backlinks) = 0;

    virtual void do_nullify_link(std::size_t row_ndx, std::size_t old_target_row_ndx) = 0;
    virtual void do_update_link(std::size_t row_ndx, std::size_t old_target_row_ndx, std::size_t new_target_row_ndx) = 0;
};

} //namespace tightdb

#endif //TIGHTDB_COLUMN_LINKBASE_HPP
