#include <tightdb/row.hpp>
#include <tightdb/table.hpp>

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
