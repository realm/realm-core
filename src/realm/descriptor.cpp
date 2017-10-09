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

#include <memory>
#include <realm/descriptor.hpp>
#include <realm/column_string.hpp>

#include <realm/util/miscellaneous.hpp>

using namespace realm;
using namespace realm::util;

bool Descriptor::has_search_index(size_t column_ndx) const noexcept
{
    if (REALM_UNLIKELY(column_ndx >= m_spec->get_public_column_count()))
        return false;

    int attr = m_spec->get_column_attr(column_ndx);
    return attr & col_attr_Indexed;
}

void Descriptor::add_search_index(size_t column_ndx)
{
    typedef _impl::TableFriend tf;
    tf::add_search_index(*this, column_ndx); // Throws
}

void Descriptor::remove_search_index(size_t column_ndx)
{
    typedef _impl::TableFriend tf;
    tf::remove_search_index(*this, column_ndx); // Throws
}


size_t Descriptor::get_num_unique_values(size_t column_ndx) const
{
    REALM_ASSERT(is_attached());
    ColumnType col_type = m_spec->get_column_type(column_ndx);
    if (col_type != col_type_StringEnum)
        return 0;
    ref_type ref = m_spec->get_enumkeys_ref(column_ndx);
    StringColumn col(m_spec->get_alloc(), ref); // Throws
    return col.size();
}


Descriptor::~Descriptor() noexcept
{
    if (!is_attached())
        return;
    if (m_parent) {
        m_parent.reset();
    }
    m_root_table.reset();
}


void Descriptor::detach() noexcept
{
    REALM_ASSERT(is_attached());
    if (m_parent) {
        m_parent.reset();
    }
    m_root_table.reset();
}

size_t* Descriptor::record_subdesc_path(size_t*, size_t* end) const noexcept
{
    return end;
}
