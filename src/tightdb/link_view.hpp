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

#include <tightdb/table.hpp>
#include <tightdb/column.hpp>
#include <tightdb/column_linklist.hpp>
#include <tightdb/util/bind_ptr.hpp>

namespace tightdb {

class ColumnLinkList;
class LinkView;

typedef util::bind_ptr<LinkView> LinkViewRef;

class LinkView {
public:
    ~LinkView() TIGHTDB_NOEXCEPT;
    bool is_attached() const TIGHTDB_NOEXCEPT;
    std::size_t get_parent_row() const TIGHTDB_NOEXCEPT;

    // Size info
    bool is_empty() const TIGHTDB_NOEXCEPT;
    std::size_t size() const TIGHTDB_NOEXCEPT;

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
    friend class ColumnLinkList;
    friend class util::bind_ptr<LinkView>;

    // constructor (protected since it can only be used by friends)
    LinkView(ColumnLinkList& column, std::size_t row_ndx);

    void detach();
    void update_column_ptr(Column* refs);
    void set_parent_row(std::size_t row_ndx);

    void do_nullify_link(std::size_t old_target_row_ndx);
    void do_update_link(size_t old_target_row_ndx, std::size_t new_target_row_ndx);

    void bind_ref() const TIGHTDB_NOEXCEPT { ++m_ref_count; }
    void unbind_ref() const TIGHTDB_NOEXCEPT;

    // Member variables
    size_t          m_row_ndx;
    TableRef        m_table;
    ColumnLinkList& m_column;
    Column*         m_refs;
    mutable size_t  m_ref_count;
};

// Implementation

inline LinkView::LinkView(ColumnLinkList& column, std::size_t row_ndx) : m_row_ndx(row_ndx), m_table(column.get_target_table()), m_column(column), m_refs(null_ptr)
{
    ref_type ref = column.get_row_ref(row_ndx);
    if (ref) {
        m_refs = new Column(ref, &m_column, m_row_ndx, m_column.get_alloc());
    }
}

inline LinkView::~LinkView() TIGHTDB_NOEXCEPT
{
    if (is_attached()) {
        m_column.unregister_linkview(*this);
        delete m_refs;
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
    delete m_refs;
}

inline void LinkView::update_column_ptr(Column* refs)
{
    // only possible update is toggle between ptr and null_ptr
    TIGHTDB_ASSERT((!refs && m_refs) || (refs && !m_refs));
    m_refs = refs;
}

inline bool LinkView::is_attached() const TIGHTDB_NOEXCEPT
{
    return bool(m_table);
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
    if (m_refs) {
        m_refs->set_parent(&m_column, row_ndx);
    }
}

inline bool LinkView::is_empty() const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(is_attached());

    if (!m_refs)
        return true;

    return m_refs->is_empty();
}

inline std::size_t LinkView::size() const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(is_attached());

    if (!m_refs)
        return 0;

    return m_refs->size();
}

inline Table::RowExpr LinkView::get(std::size_t row_ndx) TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(is_attached());
    TIGHTDB_ASSERT(m_refs);
    TIGHTDB_ASSERT(row_ndx < m_refs->size());

    std::size_t real_row_ndx = m_refs->get(row_ndx);
    return (*m_table)[real_row_ndx];
}

inline Table::RowExpr LinkView::operator[](std::size_t row_ndx) TIGHTDB_NOEXCEPT
{
    return get(row_ndx);
}

inline std::size_t LinkView::get_target_row(std::size_t row_ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(is_attached());
    TIGHTDB_ASSERT(m_refs);
    TIGHTDB_ASSERT(row_ndx < m_refs->size());

    return m_refs->get(row_ndx);
}

inline void LinkView::add(std::size_t target_row_ndx)
{
    TIGHTDB_ASSERT(is_attached());
    size_t ins_pos = (m_refs) ? m_refs->size() : 0;
    insert(ins_pos, target_row_ndx);
}

} // namespace tightdb

#endif // TIGHTDB_LINK_VIEW_HPP
