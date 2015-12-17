/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2015] Realm Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Realm Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Realm Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Realm Incorporated.
 *
 **************************************************************************/

#include <realm/row.hpp>
#include <realm/table.hpp>
#include <realm/group.hpp>

using namespace realm;


void RowBase::attach(Table* table, size_t row_ndx) noexcept
{
    if (table) {
        table->register_row_accessor(this);
        m_table.reset(table);
        m_row_ndx = row_ndx;
    }
}

void RowBase::reattach(Table* table, size_t row_ndx) noexcept
{
    if (m_table.get() != table) {
        if (m_table)
            m_table->unregister_row_accessor(this);
        if (table)
            table->register_row_accessor(this);
        m_table.reset(table);
    }
    m_row_ndx = row_ndx;
}

void RowBase::impl_detach() noexcept
{
    if (m_table) {
        m_table->unregister_row_accessor(this);
        m_table.reset();
    }
}

RowBase::RowBase(const RowBase& source, Handover_patch& patch)
    : m_table(TableRef())
{
    patch.table_num = source.m_table->get_index_in_group();
    patch.row_ndx = source.m_row_ndx;
}

void RowBase::apply_patch(Handover_patch& patch, Group& group)
{
    m_table = group.get_table(patch.table_num);
    m_table->register_row_accessor(this);
    m_row_ndx = patch.row_ndx;
}


