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


DescriptorRef Descriptor::get_subdescriptor(size_t column_ndx)
{
    REALM_ASSERT(is_attached());

    // Reuse the the descriptor accessor if it is already in the map
    for (const auto& it: m_subdesc_map) {
        if (it.m_column_ndx == column_ndx)
            return it.m_subdesc;
    }

    // Create a new descriptor accessor
    SubspecRef subspec_ref = m_spec->get_subtable_spec(column_ndx);
    auto subspec = std::make_unique<Spec>(subspec_ref);
    auto subdesc = std::make_shared<Descriptor>(
            ConcretDescriptor{m_root_table.get(), this, subspec.get()});
    m_subdesc_map.push_back(subdesc_entry(column_ndx, subdesc));
    subspec.release();

    return subdesc;
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
    if (m_parent) {
        // Sub descriptor own the m_spec
        delete m_spec;
    }
    // Detach the sub descriptors in case there are others holding
    // refs to them.
    detach_subdesc_accessors();
    m_root_table.reset();
}

void Descriptor::detach(bool root_desc) noexcept
{
    REALM_ASSERT(is_attached());
    REALM_ASSERT((root_desc && this->is_root()) || (!root_desc && !(this->is_root())));
    (void)root_desc; // Unused warning for release build

    detach_subdesc_accessors();
    // Detach will only reset the m_root_table to make the is_attached
    // return false. It is the destructor's responsibility to free other
    // resources.
    m_root_table.reset();
}


void Descriptor::detach_subdesc_accessors() noexcept
{
    for (const auto& subdesc : m_subdesc_map) {
        subdesc.m_subdesc->detach(false);
    }
    m_subdesc_map.clear();
}


size_t* Descriptor::record_subdesc_path(size_t* begin, size_t* end) const noexcept
{
    size_t* begin_2 = end;
    const Descriptor* desc = this;
    for (;;) {
        if (desc->is_root())
            return begin_2;
        if (REALM_UNLIKELY(begin_2 == begin))
            return 0; // Not enough space in path buffer
        const Descriptor* parent = desc->m_parent;
        size_t column_ndx = not_found;

        for (const auto& subdesc : parent->m_subdesc_map) {
            if (subdesc.m_subdesc.get() == desc) {
                column_ndx = subdesc.m_column_ndx;
                break;
            }
        }

        REALM_ASSERT_3(column_ndx, !=, not_found);
        *--begin_2 = column_ndx;
        desc = parent;
    }
}

void Descriptor::adj_insert_column(size_t col_ndx) noexcept
{
    // Adjust the column indexes of subdescriptor accessors at higher
    // column indexes.
    for (auto& subdesc : m_subdesc_map) {
        if (subdesc.m_column_ndx >= col_ndx)
            ++subdesc.m_column_ndx;
    }
}


void Descriptor::adj_erase_column(size_t col_ndx) noexcept
{
    // If it exists, remove and detach the subdescriptor accessor
    // associated with the removed column. Also adjust the column
    // indexes of subdescriptor accessors at higher column indexes.
    typedef subdesc_map::iterator iter;
    iter end = m_subdesc_map.end();
    iter erase = end;
    for (iter i = m_subdesc_map.begin(); i != end; ++i) {
        if (i->m_column_ndx == col_ndx) {
            // Detach it in case there are others holding refs to it.
            i->m_subdesc->detach(false);
            erase = i;
        }
        else if (i->m_column_ndx > col_ndx) {
            --i->m_column_ndx; // Account for the removed column
        }
    }
    if (erase != end)
        m_subdesc_map.erase(erase);
}

void Descriptor::adj_move_column(size_t from, size_t to) noexcept
{
    for (auto& subdesc : m_subdesc_map) {
        if (subdesc.m_column_ndx == from) {
            subdesc.m_column_ndx = to;
        }
        else {
            if (from < to) {
                // Moving up:
                if (subdesc.m_column_ndx > from && subdesc.m_column_ndx <= to) {
                    --subdesc.m_column_ndx;
                }
            }
            else if (from > to) {
                // Moving down:
                if (subdesc.m_column_ndx < from && subdesc.m_column_ndx >= to) {
                    ++subdesc.m_column_ndx;
                }
            }
        }
    }
}
