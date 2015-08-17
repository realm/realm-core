#include <realm/column_linkbase.hpp>
#include <realm/column_backlink.hpp>
#include <realm/group.hpp>
#include <realm/table.hpp>

using namespace realm;



void LinkColumnBase::refresh_accessor_tree(size_t column_index, const Spec& spec)
{
    IntegerColumn::refresh_accessor_tree(column_index, spec); // Throws
    ColumnAttr attr = spec.get_column_attr(column_index);
    m_weak_links = (attr & column_attr_StrongLinks) == 0;
}


void LinkColumnBase::check_cascade_break_backlinks_to(size_t target_table_index, size_t target_row_index,
                                                      CascadeState& state)
{
    // Stop if the target row was already visited
    CascadeState::row target_row;
    target_row.table_index = target_table_index;
    target_row.row_index   = target_row_index;
    auto i = std::upper_bound(state.rows.begin(), state.rows.end(), target_row);
    bool already_seen = i != state.rows.begin() && i[-1] == target_row;
    if (already_seen)
        return;

    // Stop if there are any remaining strong links to this row (this scheme
    // fails to discover orphaned cycles)
    typedef _impl::TableFriend tf;
    size_t num_remaining = tf::get_num_strong_backlinks(*m_target_table, target_row_index);
    if (num_remaining > 0)
        return;

    // Recurse
    state.rows.insert(i, target_row); // Throws
    tf::cascade_break_backlinks_to(*m_target_table, target_row_index, state); // Throws
}


#ifdef REALM_DEBUG

void LinkColumnBase::verify(const Table& table, size_t column_index) const
{
    IntegerColumn::verify(table, column_index);

    // Check that the backlink column specifies the right origin
    REALM_ASSERT(&m_backlink_column->get_origin_table() == &table);
    REALM_ASSERT(&m_backlink_column->get_origin_column() == this);

    // Check that m_target_table is the table specified by the spec
    size_t target_table_index = m_target_table->get_index_in_group();
    typedef _impl::TableFriend tf;
    const Spec& spec = tf::get_spec(table);
    REALM_ASSERT(target_table_index == spec.get_opposite_link_table_index(column_index));

    // Check that m_backlink_column is the column specified by the target table spec
    const Spec& target_spec = tf::get_spec(*m_target_table);
    size_t backlink_column_index =
        target_spec.find_backlink_column(table.get_index_in_group(), column_index);
    REALM_ASSERT(m_backlink_column == &tf::get_column(*m_target_table, backlink_column_index));
}

#endif // REALM_DEBUG
