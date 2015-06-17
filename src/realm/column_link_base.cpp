#include <realm/column_linkbase.hpp>
#include <realm/column_backlink.hpp>
#include <realm/group.hpp>
#include <realm/table.hpp>

using namespace realm;



void ColumnLinkBase::erase(size_t, bool)
{
    // This operation is not available for unordered tables, and only unordered
    // tables may have link columns.
    REALM_ASSERT(false);
}


void ColumnLinkBase::refresh_accessor_tree(size_t col_ndx, const Spec& spec)
{
    Column::refresh_accessor_tree(col_ndx, spec); // Throws
    ColumnAttr attr = spec.get_column_attr(col_ndx);
    m_weak_links = (attr & col_attr_StrongLinks) == 0;
}


void ColumnLinkBase::check_cascade_break_backlinks_to(size_t target_table_ndx, size_t target_row_ndx,
                                                      CascadeState& state)
{
    // Stop if the target row was already visited
    CascadeState::row target_row;
    target_row.table_ndx = target_table_ndx;
    target_row.row_ndx   = target_row_ndx;
    auto i = std::upper_bound(state.rows.begin(), state.rows.end(), target_row);
    bool already_seen = i != state.rows.begin() && i[-1] == target_row;
    if (already_seen)
        return;

    // Stop if there are any remaining strong links to this row (this scheme
    // fails to discover orphaned cycles)
    typedef _impl::TableFriend tf;
    size_t num_remaining = tf::get_num_strong_backlinks(*m_target_table, target_row_ndx);
    if (num_remaining > 0)
        return;

    // Recurse
    state.rows.insert(i, target_row); // Throws
    tf::cascade_break_backlinks_to(*m_target_table, target_row_ndx, state); // Throws
}


#ifdef REALM_DEBUG

void ColumnLinkBase::Verify(const Table& table, size_t col_ndx) const
{
    Column::Verify(table, col_ndx);

    // Check that the backlink column specifies the right origin
    REALM_ASSERT(&m_backlink_column->get_origin_table() == &table);
    REALM_ASSERT(&m_backlink_column->get_origin_column() == this);

    // Check that m_target_table is the table specified by the spec
    size_t target_table_ndx = m_target_table->get_index_in_group();
    typedef _impl::TableFriend tf;
    const Spec& spec = tf::get_spec(table);
    REALM_ASSERT(target_table_ndx == spec.get_opposite_link_table_ndx(col_ndx));

    // Check that m_backlink_column is the column specified by the target table spec
    const Spec& target_spec = tf::get_spec(*m_target_table);
    size_t backlink_col_ndx =
        target_spec.find_backlink_column(table.get_index_in_group(), col_ndx);
    REALM_ASSERT(m_backlink_column == &tf::get_column(*m_target_table, backlink_col_ndx));
}

#endif // REALM_DEBUG
