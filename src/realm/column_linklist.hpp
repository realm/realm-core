/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#ifndef REALM_COLUMN_LINKLIST_HPP
#define REALM_COLUMN_LINKLIST_HPP

#include <algorithm>
#include <vector>

#include <realm/column.hpp>
#include <realm/column_linkbase.hpp>
#include <realm/table.hpp>
#include <realm/column_backlink.hpp>

namespace realm {

namespace _impl {
class TransactLogConvenientEncoder;
}


/// A column of link lists (LinkListColumn) is a single B+-tree, and the root of
/// the column is the root of the B+-tree. All leaf nodes are single arrays of
/// type Array with the hasRefs bit set.
///
/// The individual values in the column are either refs to Columns containing the
/// row positions in the target table, or in the case where they are empty, a zero
/// ref.
class LinkListColumn : public LinkColumnBase, public ArrayParent {
public:
    using LinkColumnBase::LinkColumnBase;
    LinkListColumn(Allocator& alloc, ref_type ref, Table* table, size_t column_ndx);
    ~LinkListColumn() noexcept override;

    static ref_type create(Allocator&, size_t size = 0);

    bool is_nullable() const noexcept final;

    bool has_links(size_t row_ndx) const noexcept;
    size_t get_link_count(size_t row_ndx) const noexcept;

    bool is_null(size_t row_ndx) const noexcept final;
    void set_null(size_t row_ndx) final;

    void to_json_row(size_t row_ndx, std::ostream& out) const;

    void insert_rows(size_t, size_t, size_t, bool) override;
    void erase_rows(size_t, size_t, size_t, bool) override;
    void move_last_row_over(size_t, size_t, bool) override;
    void swap_rows(size_t, size_t) override;
    void clear(size_t, bool) override;
    void update_from_parent(size_t) noexcept override;

    void verify() const override;
    void verify(const Table&, size_t) const override;

private:
    ref_type get_row_ref(size_t row_ndx) const noexcept;
    void set_row_ref(size_t row_ndx, ref_type new_ref);
    void add_backlink(size_t target_row, size_t source_row);
    void remove_backlink(size_t target_row, size_t source_row);

    // ArrayParent overrides
    void update_child_ref(size_t child_ndx, ref_type new_ref) override;
    ref_type get_child_ref(size_t child_ndx) const noexcept override;

    template <bool fix_ndx_in_parent>
    void adj_insert_rows(size_t row_ndx, size_t num_rows_inserted) noexcept;

    template <bool fix_ndx_in_parent>
    void adj_erase_rows(size_t row_ndx, size_t num_rows_erased) noexcept;

    template <bool fix_ndx_in_parent>
    void adj_move_over(size_t from_row_ndx, size_t to_row_ndx) noexcept;

    template <bool fix_ndx_in_parent>
    void adj_move(size_t from_ndx, size_t to_ndx) noexcept;

    template <bool fix_ndx_in_parent>
    void adj_swap(size_t row_ndx_1, size_t row_ndx_2) noexcept;

    std::pair<ref_type, size_t> get_to_dot_parent(size_t) const override;

    friend class BacklinkColumn;
    friend class LinkView;
    friend class _impl::TransactLogConvenientEncoder;
};


// Implementation

inline LinkListColumn::LinkListColumn(Allocator& alloc, ref_type ref, Table* table, size_t column_ndx)
    : LinkColumnBase(alloc, ref, table, column_ndx)
{
}

inline LinkListColumn::~LinkListColumn() noexcept
{
}

inline ref_type LinkListColumn::create(Allocator& alloc, size_t size)
{
    return IntegerColumn::create(alloc, Array::type_HasRefs, size); // Throws
}

inline bool LinkListColumn::is_nullable() const noexcept
{
    return false;
}

inline bool LinkListColumn::has_links(size_t row_ndx) const noexcept
{
    ref_type ref = LinkColumnBase::get_as_ref(row_ndx);
    return (ref != 0);
}

inline size_t LinkListColumn::get_link_count(size_t row_ndx) const noexcept
{
    ref_type ref = LinkColumnBase::get_as_ref(row_ndx);
    if (ref == 0)
        return 0;
    return ColumnBase::get_size_from_ref(ref, get_alloc());
}

inline bool LinkListColumn::is_null(size_t) const noexcept
{
    return false;
}

inline void LinkListColumn::set_null(size_t)
{
    throw LogicError{LogicError::column_not_nullable};
}

inline ref_type LinkListColumn::get_row_ref(size_t row_ndx) const noexcept
{
    return LinkColumnBase::get_as_ref(row_ndx);
}

inline void LinkListColumn::set_row_ref(size_t row_ndx, ref_type new_ref)
{
    LinkColumnBase::set(row_ndx, new_ref); // Throws
}

inline void LinkListColumn::add_backlink(size_t target_row, size_t source_row)
{
    m_backlink_column->add_backlink(target_row, source_row); // Throws
}

inline void LinkListColumn::remove_backlink(size_t target_row, size_t source_row)
{
    m_backlink_column->remove_one_backlink(target_row, source_row); // Throws
}


} // namespace realm

#endif // REALM_COLUMN_LINKLIST_HPP
