/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] Realm Inc
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

using namespace std;
using namespace realm;


void RowBase::attach(Table* table, size_t row_ndx) REALM_NOEXCEPT
{
    if (table) {
        table->register_row_accessor(this);
        m_table.reset(table);
        m_row_ndx = row_ndx;
    }
}

void RowBase::reattach(Table* table, size_t row_ndx) REALM_NOEXCEPT
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

void RowBase::impl_detach() REALM_NOEXCEPT
{
    if (m_table) {
        m_table->unregister_row_accessor(this);
        m_table.reset();
    }
}


void RowBase::prepare_for_export(Handover_data& handover_data)
{
    handover_data.table_num = m_table->get_index_in_group();
    impl_detach();
}


void RowBase::prepare_for_import(Handover_data& handover_data, Group& group)
{
    TableRef table = group.get_table(handover_data.table_num);
    reattach(table.get(), m_row_ndx);
}

