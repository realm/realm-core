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
    std::size_t get_parent_row() const TIGHTDB_NOEXCEPT;

    // Size info
    bool is_empty() const TIGHTDB_NOEXCEPT;
    std::size_t size() const TIGHTDB_NOEXCEPT;

    bool operator==(const LinkView&) const TIGHTDB_NOEXCEPT;
    bool operator!=(const LinkView&) const TIGHTDB_NOEXCEPT;

    // Getting links
    Table::RowExpr operator[](std::size_t row_ndx) TIGHTDB_NOEXCEPT;
    Table::RowExpr get(std::size_t row_ndx) TIGHTDB_NOEXCEPT;
    std::size_t get_target_row(std::size_t row_ndx) const TIGHTDB_NOEXCEPT;

    // Modifiers
    void add(std::size_t target_row_ndx);
    void insert(std::size_t ins_pos, std::size_t target_row_ndx);
    void set(std::size_t row_ndx, std::size_t target_row_ndx);
    void move(size_t old_link_ndx, size_t new_link_ndx);
    void remove(std::size_t row_ndx);
    void clear();

private:
    // constructor (protected since it can only be used by friends)
    LinkView(ColumnLinkList& column, std::size_t row_ndx);

    void detach();
    void set_parent_row(std::size_t row_ndx);

    void do_nullify_link(std::size_t old_target_row_ndx);
    void do_update_link(size_t old_target_row_ndx, std::size_t new_target_row_ndx);

    void bind_ref() const TIGHTDB_NOEXCEPT { ++m_ref_count; }
    void unbind_ref() const TIGHTDB_NOEXCEPT;

    // Member variables
    size_t          m_row_ndx;
    TableRef        m_table;
    ColumnLinkList& m_column;
    Column          m_refs;
    mutable size_t  m_ref_count;

    friend class ColumnLinkList;
    friend class util::bind_ptr<LinkView>;
    friend class util::bind_ptr<const LinkView>;
    friend class LangBindHelper;
};

// Implementation

inline LinkView::LinkView(ColumnLinkList& column, std::size_t row_ndx):
    m_row_ndx(row_ndx),
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
        m_column.unregister_linkview(*this);
    }
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
    m_table.reset();
    m_refs.detach();
}

inline bool LinkView::is_attached() const TIGHTDB_NOEXCEPT
{
    return static_cast<bool>(m_table);
}

inline std::size_t LinkView::get_parent_row() const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(is_attached());
    return m_row_ndx;
}

inline void LinkView::set_parent_row(std::size_t row_ndx)
{
    TIGHTDB_ASSERT(is_attached());
    m_row_ndx = row_ndx;
    m_refs.set_parent(&m_column, row_ndx);
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

} // namespace tightdb

#endif // TIGHTDB_LINK_VIEW_HPP
