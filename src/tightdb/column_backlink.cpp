//
//  column_backlink.cpp
//  tightdb
//
//  Created by Alexander Stigsen on 5/12/14.
//  Copyright (c) 2014 TightDB. All rights reserved.
//

#include <tightdb/column_backlink.hpp>
#include <tightdb/column_link.hpp>

using namespace tightdb;

void ColumnBackLink::add_backlink(size_t row_ndx, size_t source_row_ndx)
{
    int64_t ref = Column::get(row_ndx);

    // If there is only a single backlink, it can be stored as
    // a tagged value
    if (ref == 0) {
        size_t tagged_ndx = (source_row_ndx << 1) + 1;
        Column::set(row_ndx, tagged_ndx);
        return;
    }

    // if there already is a tagged value stored
    // we have to convert it to a list with room for
    // new backlink
    if (ref & 1) {
        TIGHTDB_ASSERT(false);
    }

    // Otherwise just add to existing list
}

std::size_t ColumnBackLink::get_backlink_count(size_t row_ndx) const TIGHTDB_NOEXCEPT
{
    int64_t ref = Column::get(row_ndx);

    if (ref == 0)
        return 0;

    if (ref & 1)
        return 1;

    // TODO: get list size
    TIGHTDB_ASSERT(false);

    return 0;
}

std::size_t ColumnBackLink::get_backlink(size_t row_ndx, size_t backlink_ndx) const TIGHTDB_NOEXCEPT
{
    int64_t ref = Column::get(row_ndx);
    TIGHTDB_ASSERT(ref != 0);

    if (ref & 1) {
        TIGHTDB_ASSERT(backlink_ndx == 0);

        uint64_t row_ref = uint64_t(ref) >> 1;
        return row_ref;
    }

    //TODO: return ref from list
    TIGHTDB_ASSERT(false);

    return 0;
}

void ColumnBackLink::remove_backlink(size_t row_ndx, size_t source_row_ndx)
{
    int64_t ref = Column::get(row_ndx);
    TIGHTDB_ASSERT(ref != 0);

    // If there is only a single backlink, it can be stored as
    // a tagged value
    if (ref & 1) {
        TIGHTDB_ASSERT(get_backlink(row_ndx, 0) == source_row_ndx);
        Column::set(row_ndx, 0);
        return;
    }

    // TODO: if there is a list of backlinks we have to find
    // the right one and remove it.
    TIGHTDB_ASSERT(false);
}

void ColumnBackLink::update_backlink(std::size_t row_ndx, std::size_t old_row_ndx, std::size_t new_row_ndx) {
    int64_t ref = Column::get(row_ndx);
    TIGHTDB_ASSERT(ref != 0);

    if (ref & 1) {
        TIGHTDB_ASSERT((ref >> 1) == (int64_t)old_row_ndx);
        size_t tagged_ndx = (new_row_ndx << 1) + 1;
        Column::set(row_ndx, tagged_ndx);
        return;
    }

    // TODO: find match in list and replace
    TIGHTDB_ASSERT(false);
}

void ColumnBackLink::nullify_links(std::size_t row_ndx)
{
    // Nullify all links pointing to the row being deleted
    int64_t ref = Column::get(row_ndx);
    if (ref != 0) {
        if (ref & 1) {
            uint64_t row_ref = uint64_t(ref) >> 1;
            m_source_column->do_nullify_link(row_ref);
        }
        else {
            // TODO: nullify entire list of links
            TIGHTDB_ASSERT(false);

            // TODO: delete sub-tree
            //sub_column.destroy_deep();
        }
    }
}

void ColumnBackLink::move_last_over(std::size_t row_ndx)
{
    TIGHTDB_ASSERT(row_ndx+1 < size());

    // Nullify all links pointing to the row being deleted
    nullify_links(row_ndx);

    // Update all links to the last row to point to the new row instead
    size_t last_row_ndx = size()-1;
    size_t ref = Column::get(last_row_ndx);
    if (ref != 0) {
        if (ref & 1) {
            uint64_t row_ref = uint64_t(ref) >> 1;
            m_source_column->do_update_link(row_ref, row_ndx);
        }
        else {
            // TODO: update entire list of links
            TIGHTDB_ASSERT(false);
        }
    }

    // Do the actual move
    Column::set(row_ndx, ref);
    Column::erase(last_row_ndx, true);
}

void ColumnBackLink::erase(std::size_t row_ndx, bool is_last)
{
    TIGHTDB_ASSERT(is_last);

    nullify_links(row_ndx);
    Column::erase(row_ndx, true);
}

void ColumnBackLink::clear()
{
    size_t count = size();
    for (size_t i = 0; i < count; ++i) {
        nullify_links(i);
    }
    Column::clear();
    Column::clear();
    // FIXME: This one is needed because Column::clear() forgets about
    // the leaf type. A better solution should probably be found.
    m_array->set_type(Array::type_HasRefs);
}

