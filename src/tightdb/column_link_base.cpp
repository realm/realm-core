#include <tightdb/column_linkbase.hpp>
#include <tightdb/column_backlink.hpp>
#include <tightdb/table.hpp>

using namespace tightdb;


#ifdef TIGHTDB_DEBUG

void ColumnLinkBase::Verify(const Table& table, size_t col_ndx) const
{
    Column::Verify(table, col_ndx);

    // Check that the backlink column specifies the right origin
    TIGHTDB_ASSERT(&m_backlink_column->get_origin_table() == &table);
    TIGHTDB_ASSERT(&m_backlink_column->get_origin_column() == this);

    // Check that m_target_table is the table specified by the spec
    size_t target_table_ndx = m_target_table->get_index_in_parent();
    typedef _impl::TableFriend tf;
    const Spec& spec = tf::get_spec(table);
    TIGHTDB_ASSERT(target_table_ndx == spec.get_opposite_link_table_ndx(col_ndx));

    // Check that m_backlink_column is the column specified by the target table spec
    const Spec& target_spec = tf::get_spec(*m_target_table);
    size_t backlink_col_ndx =
        target_spec.find_backlink_column(table.get_index_in_parent(), col_ndx);
    TIGHTDB_ASSERT(m_backlink_column == &tf::get_column(*m_target_table, backlink_col_ndx));
}

#endif // TIGHTDB_DEBUG
