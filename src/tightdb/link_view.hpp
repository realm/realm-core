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
#ifndef TIGHTDB_LINK_VIEW_HPP
#define TIGHTDB_LINK_VIEW_HPP

#include <iostream>

#include <tightdb/table_view.hpp>
#include <tightdb/table.hpp>
#include <tightdb/column.hpp>
#include <tightdb/column_linklist.hpp>

namespace tightdb {


class LinkView : public TableViewBase
{
public:

    // This entire constructor/class is just an experiment to figure out how things must work    
    LinkView(ColumnLinkList& link_column, size_t link_row) :
        TableViewBase(link_column.get_target_table().get()),
        m_links_column(link_column.get_ref_column(link_row)),
        m_link_row(link_row),
        m_linklist_column(link_column)
    {
        // Make TableViewBase store separate Column with default allocator (detached from database payload)
        m_refs.clear();
        for (size_t t = 0; t < m_links_column.size(); t++)
            m_refs.add(m_links_column.get(t));
    }

    // All modifying methods must be carried out manually - something along the lines of this:
    void remove(size_t row_ndx)
    {
        m_linklist_column.remove_link(m_link_row, row_ndx);
        m_refs.erase(row_ndx, row_ndx == m_refs.size() - 1);
    }

private:
    Column m_links_column;
    size_t m_link_row;
    ColumnLinkList& m_linklist_column;

};


}

#endif
