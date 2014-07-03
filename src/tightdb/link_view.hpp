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

#include <tightdb/util/bind_ptr.hpp>
#include <tightdb/column.hpp>
#include <tightdb/column_linklist.hpp>
#include <tightdb/link_view_fwd.hpp>
#include <tightdb/table.hpp>

namespace tightdb {

class ColumnLinkList;


class LinkView {
public:
    ~LinkView() TIGHTDB_NOEXCEPT;
    bool is_attached() const TIGHTDB_NOEXCEPT;
    std::size_t get_origin_row_index() const TIGHTDB_NOEXCEPT;

    // Size info
    bool is_empty() const TIGHTDB_NOEXCEPT;
    std::size_t size() const TIGHTDB_NOEXCEPT;

    bool operator==(const LinkView&) const TIGHTDB_NOEXCEPT;
    bool operator!=(const LinkView&) const TIGHTDB_NOEXCEPT;

    // Getting links
    Table::RowExpr operator[](std::size_t link_ndx) TIGHTDB_NOEXCEPT;
    Table::RowExpr get(std::size_t link_ndx) TIGHTDB_NOEXCEPT;
    std::size_t get_target_row(std::size_t link_ndx) const TIGHTDB_NOEXCEPT;

    // Modifiers
    void add(std::size_t target_row_ndx);
    void insert(std::size_t link_ndx, std::size_t target_row_ndx);
    void set(std::size_t link_ndx, std::size_t target_row_ndx);
    void move(std::size_t old_link_ndx, std::size_t new_link_ndx);
    void remove(std::size_t link_ndx);
    void clear();

    // Find first row backed by source index
    std::size_t find_by_source_ndx(std::size_t source_ndx) const TIGHTDB_NOEXCEPT;

    Table& get_parent() TIGHTDB_NOEXCEPT;
    const Table& get_parent() const TIGHTDB_NOEXCEPT;

private:
    TableRef        m_table;
    ColumnLinkList& m_column;
    Column          m_refs;
    mutable size_t  m_ref_count;

    // constructor (protected since it can only be used by friends)
    LinkView(ColumnLinkList& column, std::size_t row_ndx);

    void detach();
    void set_origin_row_index(std::size_t row_ndx);

    void do_nullify_link(std::size_t old_target_row_ndx);
    void do_update_link(size_t old_target_row_ndx, std::size_t new_target_row_ndx);

    void bind_ref() const TIGHTDB_NOEXCEPT;
    void unbind_ref() const TIGHTDB_NOEXCEPT;

    void refresh_accessor_tree(size_t new_row_ndx);

#ifdef TIGHTDB_ENABLE_REPLICATION
    Replication* get_repl() TIGHTDB_NOEXCEPT;
    void repl_unselect() TIGHTDB_NOEXCEPT;
#endif

    friend class ColumnLinkList;
    friend class util::bind_ptr<LinkView>;
    friend class util::bind_ptr<const LinkView>;
    friend class LangBindHelper;
    friend class _impl::LinkListFriend;
};





// Implementation

inline LinkView::LinkView(ColumnLinkList& column, std::size_t row_ndx):
    m_table(column.get_target_table()->get_table_ref()),
    m_column(column),
    m_refs(&column, row_ndx, column.get_alloc()),
    m_ref_count(0)
{
    ref_type ref = column.get_row_ref(row_ndx);
    if (ref) {
        m_refs.get_root_array()->init_from_parent();
    }
}

inline LinkView::~LinkView() TIGHTDB_NOEXCEPT
{
    if (is_attached()) {
#ifdef TIGHTDB_ENABLE_REPLICATION
        repl_unselect();
#endif
        m_column.unregister_linkview(*this);
    }
}

inline void LinkView::bind_ref() const TIGHTDB_NOEXCEPT
{
    ++m_ref_count;
}

inline void LinkView::unbind_ref() const TIGHTDB_NOEXCEPT
{
    if (--m_ref_count > 0)
        return;

    delete this;
}

inline void LinkView::detach()
{
    TIGHTDB_ASSERT(is_attached());
#ifdef TIGHTDB_ENABLE_REPLICATION
    repl_unselect();
#endif
    m_table.reset();
    m_refs.detach();
}

inline bool LinkView::is_attached() const TIGHTDB_NOEXCEPT
{
    return static_cast<bool>(m_table);
}

inline std::size_t LinkView::get_origin_row_index() const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(is_attached());
    return m_refs.get_root_array()->get_ndx_in_parent();
}

inline void LinkView::set_origin_row_index(std::size_t row_ndx)
{
    TIGHTDB_ASSERT(is_attached());
    m_refs.get_root_array()->set_ndx_in_parent(row_ndx);
}

inline bool LinkView::is_empty() const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(is_attached());

    if (!m_refs.is_attached())
        return true;

    return m_refs.is_empty();
}

inline std::size_t LinkView::size() const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(is_attached());

    if (!m_refs.is_attached())
        return 0;

    return m_refs.size();
}

inline bool LinkView::operator==(const LinkView& link_list) const TIGHTDB_NOEXCEPT
{
    if (m_table->get_index_in_parent() != link_list.m_table->get_index_in_parent())
        return false;
    if (!m_refs.is_attached() || m_refs.is_empty())
        return !link_list.m_refs.is_attached() || link_list.m_refs.is_empty();
    return link_list.m_refs.is_attached() && m_refs.compare_int(link_list.m_refs);
}

inline bool LinkView::operator!=(const LinkView& link_list) const TIGHTDB_NOEXCEPT
{
    return !(*this == link_list);
}

inline Table::RowExpr LinkView::get(std::size_t row_ndx) TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(is_attached());
    TIGHTDB_ASSERT(m_refs.is_attached());
    TIGHTDB_ASSERT(row_ndx < m_refs.size());

    std::size_t real_row_ndx = to_size_t(m_refs.get(row_ndx));
    return (*m_table)[real_row_ndx];
}

inline Table::RowExpr LinkView::operator[](std::size_t row_ndx) TIGHTDB_NOEXCEPT
{
    return get(row_ndx);
}

inline std::size_t LinkView::get_target_row(std::size_t row_ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(is_attached());
    TIGHTDB_ASSERT(m_refs.is_attached());
    TIGHTDB_ASSERT(row_ndx < m_refs.size());

    return to_size_t(m_refs.get(row_ndx));
}

inline void LinkView::add(std::size_t target_row_ndx)
{
    TIGHTDB_ASSERT(is_attached());
    size_t ins_pos = (m_refs.is_attached()) ? m_refs.size() : 0;
    insert(ins_pos, target_row_ndx);
}

inline std::size_t LinkView::find_by_source_ndx(std::size_t source_ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(is_attached());
    TIGHTDB_ASSERT(source_ndx < m_table->size());

    if (!m_refs.is_attached())
        return not_found;

    return m_refs.find_first(source_ndx);
}

inline Table& LinkView::get_parent() TIGHTDB_NOEXCEPT
{
    return *m_table;
}

inline const Table& LinkView::get_parent() const TIGHTDB_NOEXCEPT
{
    return *m_table;
}


inline void LinkView::refresh_accessor_tree(size_t new_row_ndx)
{
    Array* row_indexes_root = m_refs.get_root_array();
    row_indexes_root->set_ndx_in_parent(new_row_ndx);
    row_indexes_root->init_from_parent();
}

#ifdef TIGHTDB_ENABLE_REPLICATION
inline Replication* LinkView::get_repl() TIGHTDB_NOEXCEPT
{
    typedef _impl::TableFriend tf;
    return tf::get_repl(*m_table);
}
#endif

// The purpose of this class is to give internal access to some, but
// not all of the non-public parts of the LinkView class.
class _impl::LinkListFriend {
public:
    static Table& get_table(LinkView& list) TIGHTDB_NOEXCEPT
    {
        return *list.m_table;
    }

    static const Table& get_table(const LinkView& list) TIGHTDB_NOEXCEPT
    {
        return *list.m_table;
    }

    static std::size_t get_col_ndx(const LinkView& list) TIGHTDB_NOEXCEPT
    {
        typedef _impl::TableFriend tf;
        return tf::find_column(*list.m_table, &list.m_column);
    }
};

} // namespace tightdb

#endif // TIGHTDB_LINK_VIEW_HPP
