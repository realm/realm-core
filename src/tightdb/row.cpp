#include <tightdb/row.hpp>
#include <tightdb/table.hpp>
#include <tightdb/group.hpp>

using namespace std;
using namespace tightdb;


void RowBase::attach(Table* table, size_t row_ndx) TIGHTDB_NOEXCEPT
{
    if (table) {
        table->register_row_accessor(this);
        m_table.reset(table);
        m_row_ndx = row_ndx;
    }
}

void RowBase::reattach(Table* table, size_t row_ndx) TIGHTDB_NOEXCEPT
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

void RowBase::impl_detach() TIGHTDB_NOEXCEPT
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

