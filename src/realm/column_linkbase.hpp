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
#ifndef REALM_COLUMN_LINKBASE_HPP
#define REALM_COLUMN_LINKBASE_HPP

#include <realm/table.hpp>

namespace realm {

class BacklinkColumn;
class Table;

// Abstract base class for columns containing links
class LinkColumnBase: public IntegerColumn {
public:
    // Create unattached root array aaccessor.
    LinkColumnBase(Allocator& alloc, ref_type ref, Table* table, std::size_t column_ndx);
    ~LinkColumnBase() REALM_NOEXCEPT override;

    StringIndex* create_search_index() override;

    bool get_weak_links() const REALM_NOEXCEPT;
    void set_weak_links(bool) REALM_NOEXCEPT;

    Table& get_target_table() const REALM_NOEXCEPT;
    void set_target_table(Table&) REALM_NOEXCEPT;
    BacklinkColumn& get_backlink_column() const REALM_NOEXCEPT;
    void set_backlink_column(BacklinkColumn&) REALM_NOEXCEPT;

    virtual void do_nullify_link(std::size_t row_ndx, std::size_t old_target_row_ndx) = 0;
    virtual void do_update_link(std::size_t row_ndx, std::size_t old_target_row_ndx,
                                std::size_t new_target_row_ndx) = 0;

    void adj_acc_insert_rows(std::size_t, std::size_t) REALM_NOEXCEPT override;
    void adj_acc_erase_row(std::size_t) REALM_NOEXCEPT override;
    void adj_acc_move_over(std::size_t, std::size_t) REALM_NOEXCEPT override;
    void adj_acc_clear_root_table() REALM_NOEXCEPT override;
    void mark(int) REALM_NOEXCEPT override;
    void refresh_accessor_tree(std::size_t, const Spec&) override;

#ifdef REALM_DEBUG
    void Verify(const Table&, std::size_t) const override;
    using IntegerColumn::Verify;
#endif

protected:
    // A pointer to the table that this column is part of.
    Table* const m_table;

    // The index of this column within m_table.m_cols.
    std::size_t m_column_ndx;

    TableRef m_target_table;
    BacklinkColumn* m_backlink_column = nullptr;
    bool m_weak_links = false; // True if these links are weak (not strong)

    /// Call Table::cascade_break_backlinks_to() for the specified target row if
    /// it is not already in \a state.rows, and the number of strong links to it
    /// has dropped to zero.
    void check_cascade_break_backlinks_to(std::size_t target_table_ndx, std::size_t target_row_ndx,
                                          CascadeState& state);
};




// Implementation

inline LinkColumnBase::LinkColumnBase(Allocator& alloc, ref_type ref, Table* table, std::size_t column_ndx):
    IntegerColumn(alloc, ref), // Throws
    m_table(table),
    m_column_ndx(column_ndx)
{
}

inline LinkColumnBase::~LinkColumnBase() REALM_NOEXCEPT
{
}

inline StringIndex* LinkColumnBase::create_search_index()
{
    return nullptr;
}

inline bool LinkColumnBase::get_weak_links() const REALM_NOEXCEPT
{
    return m_weak_links;
}

inline void LinkColumnBase::set_weak_links(bool value) REALM_NOEXCEPT
{
    m_weak_links = value;
}

inline Table& LinkColumnBase::get_target_table() const REALM_NOEXCEPT
{
    return *m_target_table;
}

inline void LinkColumnBase::set_target_table(Table& table) REALM_NOEXCEPT
{
    REALM_ASSERT(!m_target_table);
    m_target_table = table.get_table_ref();
}

inline BacklinkColumn& LinkColumnBase::get_backlink_column() const REALM_NOEXCEPT
{
    return *m_backlink_column;
}

inline void LinkColumnBase::set_backlink_column(BacklinkColumn& column) REALM_NOEXCEPT
{
    m_backlink_column = &column;
}

inline void LinkColumnBase::adj_acc_insert_rows(std::size_t row_ndx,
                                                std::size_t num_rows) REALM_NOEXCEPT
{
    IntegerColumn::adj_acc_insert_rows(row_ndx, num_rows);

    typedef _impl::TableFriend tf;
    tf::mark(*m_target_table);
}

inline void LinkColumnBase::adj_acc_erase_row(size_t row_ndx) REALM_NOEXCEPT
{
    IntegerColumn::adj_acc_erase_row(row_ndx);

    typedef _impl::TableFriend tf;
    tf::mark(*m_target_table);
}

inline void LinkColumnBase::adj_acc_move_over(std::size_t from_row_ndx,
                                              std::size_t to_row_ndx) REALM_NOEXCEPT
{
    IntegerColumn::adj_acc_move_over(from_row_ndx, to_row_ndx);

    typedef _impl::TableFriend tf;
    tf::mark(*m_target_table);
}

inline void LinkColumnBase::adj_acc_clear_root_table() REALM_NOEXCEPT
{
    IntegerColumn::adj_acc_clear_root_table();

    typedef _impl::TableFriend tf;
    tf::mark(*m_target_table);
}

inline void LinkColumnBase::mark(int type) REALM_NOEXCEPT
{
    if (type & mark_LinkTargets) {
        typedef _impl::TableFriend tf;
        tf::mark(*m_target_table);
    }
}


} // namespace realm

#endif // REALM_COLUMN_LINKBASE_HPP
