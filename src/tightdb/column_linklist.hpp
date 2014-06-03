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
#ifndef TIGHTDB_COLUMN_LINKLIST_HPP
#define TIGHTDB_COLUMN_LINKLIST_HPP

#include <tightdb/column.hpp>
#include <tightdb/column_linkbase.hpp>
#include <tightdb/table.hpp>
#include <tightdb/column_backlink.hpp>
#include <vector>

namespace tightdb {

class LinkView;
typedef util::bind_ptr<LinkView> LinkViewRef;


/// A column of link lists (ColumnLinkList) is a single B+-tree, and the root of
/// the column is the root of the B+-tree. All leaf nodes are single arrays of
/// type Array with the hasRefs bit set.
///
/// The individual values in the column are either refs to Columns containing the
/// row positions in the target table, or in the case where they are empty, a zero
/// ref.
class ColumnLinkList: public ColumnLinkBase, public ArrayParent {
public:
    ColumnLinkList(ref_type ref, ArrayParent* parent = 0, std::size_t ndx_in_parent = 0,
        Allocator& alloc = Allocator::get_default()); // Throws
    ColumnLinkList(Allocator& alloc);  // Throws
    ~ColumnLinkList() TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;

    static ref_type create(std::size_t size, Allocator&);

    bool   has_links(std::size_t row_ndx) const TIGHTDB_NOEXCEPT;
    size_t get_link_count(std::size_t row_ndx) const TIGHTDB_NOEXCEPT;

    LinkViewRef get_link_view(std::size_t row_ndx);

    void erase(std::size_t, bool) TIGHTDB_OVERRIDE;
    void move_last_over(std::size_t, std::size_t) TIGHTDB_OVERRIDE;
    void clear() TIGHTDB_OVERRIDE;

    // ColumnLinkBase overrides
    void set_target_table(TableRef table) TIGHTDB_OVERRIDE;
    TableRef get_target_table() TIGHTDB_OVERRIDE;
    void set_backlink_column(ColumnBackLink& backlinks) TIGHTDB_OVERRIDE;

private:
    friend class ColumnBackLink;
    friend class LinkView;

    void do_nullify_link(std::size_t row_ndx, std::size_t old_target_row_ndx);
    void do_update_link(std::size_t row_ndx, std::size_t old_target_row_ndx, std::size_t new_target_row_ndx);

    void unregister_linkview(const LinkView& view);
    ref_type get_row_ref(std::size_t row_ndx) const TIGHTDB_NOEXCEPT;
    void set_row_ref(std::size_t row_ndx, ref_type new_ref);
    void add_backlink(std::size_t target_row, std::size_t source_row);
    void remove_backlink(std::size_t target_row, std::size_t source_row);

    // ArrayParent overrides
    void update_child_ref(std::size_t child_ndx, ref_type new_ref) TIGHTDB_OVERRIDE;
    ref_type get_child_ref(std::size_t child_ndx) const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
#ifdef TIGHTDB_DEBUG
    // Used only by Array::to_dot().
    std::pair<ref_type, std::size_t>
    get_to_dot_parent(std::size_t ndx_in_parent) const TIGHTDB_OVERRIDE;
#endif

    TableRef m_target_table;
    ColumnBackLink* m_backlinks;
    std::vector<LinkView*> m_views;
};


// Implementation

inline ColumnLinkList::ColumnLinkList(ref_type ref, ArrayParent* parent, std::size_t ndx_in_parent, Allocator& alloc):
    ColumnLinkBase(ref, parent, ndx_in_parent, alloc), m_backlinks(null_ptr)
{
}

inline ColumnLinkList::ColumnLinkList(Allocator& alloc):
    ColumnLinkBase(Array::type_HasRefs, alloc), m_backlinks(null_ptr)
{
}

inline ref_type ColumnLinkList::create(std::size_t size, Allocator& alloc)
{
    int_fast64_t value = 0;
    return Column::create(Array::type_HasRefs, size, value, alloc); // Throws
}

inline bool ColumnLinkList::has_links(std::size_t row_ndx) const TIGHTDB_NOEXCEPT
{
    ref_type ref = Column::get_as_ref(row_ndx);
    return (ref != 0);
}

inline size_t ColumnLinkList::get_link_count(std::size_t row_ndx) const TIGHTDB_NOEXCEPT
{
    ref_type ref = Column::get_as_ref(row_ndx);
    if (ref == 0) {
        return 0;
    }
    return ColumnBase::get_size_from_ref(ref, get_alloc());
}

inline void ColumnLinkList::set_target_table(TableRef table)
{
    TIGHTDB_ASSERT(m_target_table.get() == null_ptr);
    m_target_table = table;
}

inline TableRef ColumnLinkList::get_target_table()
{
    return m_target_table;
}

inline void ColumnLinkList::set_backlink_column(ColumnBackLink& backlinks)
{
    m_backlinks = &backlinks;
}

inline void ColumnLinkList::unregister_linkview(const LinkView& view) {
    std::vector<LinkView*>::iterator p = std::find(m_views.begin(), m_views.end(), &view);
    TIGHTDB_ASSERT(p != m_views.end());
    m_views.erase(p);
}

inline ref_type ColumnLinkList::get_row_ref(std::size_t row_ndx) const TIGHTDB_NOEXCEPT
{
    return Column::get_as_ref(row_ndx);
}

inline void ColumnLinkList::set_row_ref(std::size_t row_ndx, ref_type new_ref)
{
    Column::set(row_ndx, new_ref);
}

inline void ColumnLinkList::add_backlink(std::size_t target_row, std::size_t source_row)
{
    m_backlinks->add_backlink(target_row, source_row);
}

inline void ColumnLinkList::remove_backlink(std::size_t target_row, std::size_t source_row)
{
    m_backlinks->remove_backlink(target_row, source_row);
}


} //namespace tightdb

#endif //TIGHTDB_COLUMN_LINKLIST_HPP


