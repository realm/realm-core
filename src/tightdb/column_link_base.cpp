#include <tightdb/column_linkbase.hpp>
#include <tightdb/column_backlink.hpp>
#include <tightdb/table.hpp>

using namespace std;
using namespace tightdb;


void ColumnLinkBase::find_erase_cascade_for_target_row(size_t target_table_ndx,
                                                       size_t target_row_ndx,
                                                       size_t stop_on_table_ndx,
                                                       cascade_rowset& rows) const
{
    // Stop if there are other strong links to this row (this scheme fails to
    // discover orphaned cycles)
    typedef _impl::TableFriend tf;
    size_t num_strong_backlinks = tf::get_num_strong_backlinks(*m_target_table, target_row_ndx);
    if (num_strong_backlinks > 1)
        return;

    // Stop if the target row was already visited
    cascade_row target_row;
    target_row.table_ndx = target_table_ndx;
    target_row.row_ndx   = target_row_ndx;
    typedef cascade_rowset::iterator iter;
    iter i = ::upper_bound(rows.begin(), rows.end(), target_row);
    if (i != rows.end())
        return;

    // Recurse
    rows.insert(i, target_row); // Throws
    tf::find_erase_cascade(*m_target_table, target_row_ndx, stop_on_table_ndx, rows); // Throws
}


void ColumnLinkBase::refresh_accessor_tree(size_t col_ndx, const Spec& spec)
{
    Column::refresh_accessor_tree(col_ndx, spec); // Throws
    ColumnAttr attr = spec.get_column_attr(col_ndx);
    m_weak_links = (attr & col_attr_StrongLinks) == 0;
}


#ifdef TIGHTDB_DEBUG

void ColumnLinkBase::Verify(const Table& table, size_t col_ndx) const
{
    Column::Verify(table, col_ndx);

    // Check that the backlink column specifies the right origin
    TIGHTDB_ASSERT(&m_backlink_column->get_origin_table() == &table);
    TIGHTDB_ASSERT(&m_backlink_column->get_origin_column() == this);

    // Check that m_target_table is the table specified by the spec
    size_t target_table_ndx = m_target_table->get_index_in_group();
    typedef _impl::TableFriend tf;
    const Spec& spec = tf::get_spec(table);
    TIGHTDB_ASSERT(target_table_ndx == spec.get_opposite_link_table_ndx(col_ndx));

    // Check that m_backlink_column is the column specified by the target table spec
    const Spec& target_spec = tf::get_spec(*m_target_table);
    size_t backlink_col_ndx =
        target_spec.find_backlink_column(table.get_index_in_group(), col_ndx);
    TIGHTDB_ASSERT(m_backlink_column == &tf::get_column(*m_target_table, backlink_col_ndx));
}

#endif // TIGHTDB_DEBUG
