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

#include <realm/impl/destroy_guard.hpp>
#include <realm/spec.hpp>
#include <realm/replication.hpp>
#include <realm/util/to_string.hpp>

using namespace realm;

Spec::~Spec() noexcept
{
    if (m_top.is_attached()) {
        if (Replication* repl = m_top.get_alloc().get_replication())
            repl->on_spec_destroyed(this);
    }
}

void Spec::detach() noexcept
{
    m_top.detach();
}

bool Spec::init(ref_type ref) noexcept
{
    // Needs only initialization if not previously initialized
    // or if the ref has changed
    if (!m_top.is_attached() || m_top.get_ref() != ref) {
        MemRef mem(ref, get_alloc());
        init(mem);
        return true;
    }
    return false;
}

void Spec::init(MemRef mem) noexcept
{
    m_top.init_from_mem(mem);
    size_t top_size = m_top.size();
    REALM_ASSERT(top_size >= 3 && top_size <= 5);

    m_types.init_from_ref(m_top.get_as_ref(0));
    m_types.set_parent(&m_top, 0);
    m_names.init_from_ref(m_top.get_as_ref(1));
    m_names.set_parent(&m_top, 1);
    m_attr.init_from_ref(m_top.get_as_ref(2));
    m_attr.set_parent(&m_top, 2);

    // Reset optional subarrays in the case of moving
    // from initialized children to uninitialized
    m_subspecs.detach();
    m_enumkeys.detach();

    // Subspecs array is only there and valid when there are subtables
    // if there are enumkey, but no subtables yet it will be a zero-ref
    if (has_subspec()) {
        ref_type ref = m_top.get_as_ref(3);
        m_subspecs.init_from_ref(ref);
        m_subspecs.set_parent(&m_top, 3);
    }

    // Enumkeys array is only there when there are StringEnum columns
    if (top_size >= 5) {
        m_enumkeys.init_from_ref(m_top.get_as_ref(4));
        m_enumkeys.set_parent(&m_top, 4);
    }

    update_internals();
}

void Spec::update_internals() noexcept
{
    m_has_strong_link_columns = false;
    m_num_public_columns = 0;
    size_t n = m_types.size();
    for (size_t i = 0; i < n; ++i) {
        if (ColumnAttr(m_attr.get(i)) & col_attr_StrongLinks) {
            m_has_strong_link_columns = true;
        }
        if (m_types.get(i) == col_type_BackLink) {
            // Now we have no more public columns
            return;
        }
        m_num_public_columns++;
    }
}

bool Spec::update_from_parent(size_t old_baseline) noexcept
{
    if (!m_top.update_from_parent(old_baseline))
        return false;

    m_types.update_from_parent(old_baseline);
    m_names.update_from_parent(old_baseline);
    m_attr.update_from_parent(old_baseline);

    if (has_subspec()) {
        m_subspecs.update_from_parent(old_baseline);
    }

    if (m_top.size() > 4)
        m_enumkeys.update_from_parent(old_baseline);

    update_internals();

    return true;
}


MemRef Spec::create_empty_spec(Allocator& alloc)
{
    // The 'spec_set' contains the specification (types and names) of
    // all columns and sub-tables
    Array spec_set(alloc);
    _impl::DeepArrayDestroyGuard dg(&spec_set);
    spec_set.create(Array::type_HasRefs); // Throws

    _impl::DeepArrayRefDestroyGuard dg_2(alloc);
    {
        // One type for each column
        bool context_flag = false;
        MemRef mem = Array::create_empty_array(Array::type_Normal, context_flag, alloc); // Throws
        dg_2.reset(mem.get_ref());
        int_fast64_t v(from_ref(mem.get_ref()));
        spec_set.add(v); // Throws
        dg_2.release();
    }
    {
        size_t size = 0;
        // One name for each column
        MemRef mem = ArrayStringShort::create_array(size, alloc); // Throws
        dg_2.reset(mem.get_ref());
        int_fast64_t v = from_ref(mem.get_ref());
        spec_set.add(v); // Throws
        dg_2.release();
    }
    {
        // One attrib set for each column
        bool context_flag = false;
        MemRef mem = Array::create_empty_array(Array::type_Normal, context_flag, alloc); // Throws
        dg_2.reset(mem.get_ref());
        int_fast64_t v = from_ref(mem.get_ref());
        spec_set.add(v); // Throws
        dg_2.release();
    }

    dg.release();
    return spec_set.get_mem();
}


void Spec::insert_column(size_t column_ndx, ColumnType type, StringData name, int attr)
{
    REALM_ASSERT(column_ndx <= m_types.size());

    if (REALM_UNLIKELY(name.size() > Table::max_column_name_length)) {
        throw LogicError(LogicError::column_name_too_long);
    }
    if (name.size() == 0) {
        throw LogicError(LogicError::invalid_column_name);
    }
    if (get_column_index(name) != npos) {
        throw LogicError(LogicError::column_name_in_use);
    }

    if (type != col_type_BackLink) {
        m_num_public_columns++;
    }
    m_names.insert(column_ndx, name);     // Throws
    m_types.insert(column_ndx, type);     // Throws
    // FIXME: So far, attributes are never reported to the replication system
    m_attr.insert(column_ndx, attr); // Throws

    bool is_subspec_type = type == col_type_Link || type == col_type_LinkList || type == col_type_BackLink;
    if (is_subspec_type) {
        Allocator& alloc = m_top.get_alloc();
        // `m_subspecs` array is only present when the spec contains a subtable column
        REALM_ASSERT_EX(m_subspecs.is_attached() == has_subspec(), m_subspecs.is_attached(), m_top.size());
        if (!m_subspecs.is_attached()) {
            bool context_flag = false;
            MemRef subspecs_mem = Array::create_empty_array(Array::type_HasRefs, context_flag, alloc); // Throws
            _impl::DeepArrayRefDestroyGuard dg(subspecs_mem.get_ref(), alloc);
            if (m_top.size() == 3) {
                int_fast64_t v(from_ref(subspecs_mem.get_ref()));
                m_top.add(v); // Throws
            }
            else {
                int_fast64_t v(from_ref(subspecs_mem.get_ref()));
                m_top.set(3, v); // Throws
            }
            m_subspecs.init_from_ref(subspecs_mem.get_ref());
            m_subspecs.set_parent(&m_top, 3);
            dg.release();
        }

        if (type == col_type_Link || type == col_type_LinkList) {
            // Store group-level table index of target table. When we set the
            // target it will be as a tagged integer (low bit set) Since we
            // don't know it yet we just store zero (null ref).
            size_t subspec_ndx = get_subspec_ndx(column_ndx);
            m_subspecs.insert(subspec_ndx, 0); // Throws
        }
        else if (type == col_type_BackLink) {
            // Store group-level table index of origin table and index of origin
            // column. When we set the target it will be as a tagged integer
            // (low bit set) Since we don't know it yet we just store zero (null
            // ref).
            size_t subspec_ndx = get_subspec_ndx(column_ndx);
            m_subspecs.insert(subspec_ndx, 0); // Throws
            m_subspecs.insert(subspec_ndx, 1); // Throws
        }
    }

    update_internals();
}

void Spec::erase_column(size_t column_ndx)
{
    REALM_ASSERT(column_ndx < m_types.size());
    typedef _impl::TableFriend tf;

    // If the column is a subtable column, we have to delete
    // the subspec(s) as well
    ColumnType type = ColumnType(m_types.get(column_ndx));
    if (tf::is_link_type(type)) {
        size_t subspec_ndx = get_subspec_ndx(column_ndx);
        m_subspecs.erase(subspec_ndx); // origin table index  : Throws
    }
    else if (type == col_type_BackLink) {
        size_t subspec_ndx = get_subspec_ndx(column_ndx);
        m_subspecs.erase(subspec_ndx); // origin table index  : Throws
        m_subspecs.erase(subspec_ndx); // origin column index : Throws
    }
    else if (type == col_type_StringEnum) {
        // Enum columns do also have a separate key list
        size_t keys_ndx = get_enumkeys_ndx(column_ndx);
        ref_type keys_ref = m_enumkeys.get_as_ref(keys_ndx);

        Array keys_top(m_top.get_alloc());
        keys_top.init_from_ref(keys_ref);
        keys_top.destroy_deep();
        m_enumkeys.erase(keys_ndx); // Throws
    }

    // Delete the actual name and type entries
    if (type != col_type_BackLink) {
        m_num_public_columns--;
    }
    m_names.erase(column_ndx);     // Throws
    m_types.erase(column_ndx);     // Throws
    m_attr.erase(column_ndx);      // Throws

    update_internals();
}


size_t Spec::get_subspec_ndx(size_t column_ndx) const noexcept
{
    REALM_ASSERT(column_ndx == get_column_count() || get_column_type(column_ndx) == col_type_Link ||
                 get_column_type(column_ndx) == col_type_LinkList ||
                 get_column_type(column_ndx) == col_type_BackLink);

    return get_subspec_ndx_after(column_ndx, column_ndx);
}


size_t Spec::get_subspec_ndx_after(size_t column_ndx, size_t skip_column_ndx) const noexcept
{
    REALM_ASSERT(column_ndx <= get_column_count());
    // The m_subspecs array only keep info for subtables so we need to
    // count up to it's position
    size_t subspec_ndx = 0;
    for (size_t i = 0; i != column_ndx; ++i) {
        if (i == skip_column_ndx) {
            continue;
        }

        ColumnType type = ColumnType(m_types.get(i));
        subspec_ndx += get_subspec_entries_for_col_type(type);
    }
    return subspec_ndx;
}


size_t Spec::get_subspec_entries_for_col_type(ColumnType type) const noexcept
{
    if (type == col_type_Link || type == col_type_LinkList) {
        return 1; // index of dest column
    }
    else if (type == col_type_BackLink) {
        return 2; // index of table and index of linked column
    }
    return 0; // no entries for other column types (subspec array is sparse)
}


void Spec::upgrade_string_to_enum(size_t column_ndx, ref_type keys_ref, ArrayParent*& keys_parent, size_t& keys_ndx)
{
    REALM_ASSERT(get_column_type(column_ndx) == col_type_String);

    REALM_ASSERT_EX(m_enumkeys.is_attached() == (m_top.size() > 4), m_enumkeys.is_attached(), m_top.size());
    // Create the enumkeys list if needed
    if (!m_enumkeys.is_attached()) {
        m_enumkeys.create(Array::type_HasRefs);
        if (m_top.size() == 3)
            m_top.add(0); // no subtables
        if (m_top.size() == 4) {
            m_top.add(m_enumkeys.get_ref());
        }
        else {
            m_top.set(4, m_enumkeys.get_ref());
        }
        m_enumkeys.set_parent(&m_top, 4);
    }

    // Insert the new key list
    size_t ins_pos = get_enumkeys_ndx(column_ndx);
    m_enumkeys.insert(ins_pos, keys_ref);

    set_column_type(column_ndx, col_type_StringEnum);

    // Return parent info
    keys_parent = &m_enumkeys;
    keys_ndx = ins_pos;
}


size_t Spec::get_enumkeys_ndx(size_t column_ndx) const noexcept
{
    // The enumkeys array only keep info for stringEnum columns
    // so we need to count up to it's position
    size_t enumkeys_ndx = 0;
    for (size_t i = 0; i < column_ndx; ++i) {
        if (ColumnType(m_types.get(i)) == col_type_StringEnum)
            ++enumkeys_ndx;
    }
    return enumkeys_ndx;
}


ref_type Spec::get_enumkeys_ref(size_t column_ndx, ArrayParent** keys_parent, size_t* keys_ndx) noexcept
{
    size_t enumkeys_ndx = get_enumkeys_ndx(column_ndx);

    // We may also need to return parent info
    if (keys_parent)
        *keys_parent = &m_enumkeys;
    if (keys_ndx)
        *keys_ndx = enumkeys_ndx;

    return m_enumkeys.get_as_ref(enumkeys_ndx);
}


TableKey Spec::get_opposite_link_table_key(size_t column_ndx) const noexcept
{
    REALM_ASSERT(column_ndx < get_column_count());
    REALM_ASSERT(get_column_type(column_ndx) == col_type_Link || get_column_type(column_ndx) == col_type_LinkList ||
                 get_column_type(column_ndx) == col_type_BackLink);

    // Key of opposite table is stored as tagged int in the
    // subspecs array
    size_t subspec_ndx = get_subspec_ndx(column_ndx);
    int64_t tagged_value = m_subspecs.get(subspec_ndx);
    REALM_ASSERT(tagged_value != 0); // can't retrieve it if never set

    uint64_t table_ref = uint64_t(tagged_value) >> 1;

    REALM_ASSERT(!util::int_cast_has_overflow<size_t>(table_ref));
    return TableKey(table_ref);
}


void Spec::set_opposite_link_table_key(size_t column_ndx, TableKey table_key)
{
    REALM_ASSERT(column_ndx < get_column_count());
    REALM_ASSERT(get_column_type(column_ndx) == col_type_Link || get_column_type(column_ndx) == col_type_LinkList ||
                 get_column_type(column_ndx) == col_type_BackLink);

    // key from target table is stored as tagged int
    int64_t tagged_ndx = (table_key.value << 1) + 1;

    size_t subspec_ndx = get_subspec_ndx(column_ndx);
    m_subspecs.set(subspec_ndx, tagged_ndx); // Throws
}


void Spec::set_backlink_origin_column(size_t backlink_col_ndx, size_t origin_col_ndx)
{
    REALM_ASSERT(backlink_col_ndx < get_column_count());
    REALM_ASSERT(get_column_type(backlink_col_ndx) == col_type_BackLink);

    // position of target table is stored as tagged int
    size_t tagged_ndx = (origin_col_ndx << 1) + 1;

    size_t subspec_ndx = get_subspec_ndx(backlink_col_ndx);
    m_subspecs.set(subspec_ndx + 1, tagged_ndx); // Throws
}


size_t Spec::get_origin_column_ndx(size_t backlink_col_ndx) const noexcept
{
    REALM_ASSERT(backlink_col_ndx < get_column_count());
    REALM_ASSERT(get_column_type(backlink_col_ndx) == col_type_BackLink);

    // Origin column is stored as second tagged int in the subspecs array
    size_t subspec_ndx = get_subspec_ndx(backlink_col_ndx);
    int64_t tagged_value = m_subspecs.get(subspec_ndx + 1);
    REALM_ASSERT(tagged_value != 0); // can't retrieve it if never set

    size_t origin_col_ndx = size_t(uint64_t(tagged_value) >> 1);
    return origin_col_ndx;
}


size_t Spec::find_backlink_column(TableKey origin_table_key, size_t origin_col_ndx) const noexcept
{
    size_t backlinks_column_start = m_num_public_columns;
    size_t backlinks_start = get_subspec_ndx(backlinks_column_start);
    size_t count = m_subspecs.size();

    int64_t tagged_table_ndx = (origin_table_key.value << 1) + 1;
    int64_t tagged_column_ndx = (origin_col_ndx << 1) + 1;

    for (size_t i = backlinks_start; i < count; i += 2) {
        if (m_subspecs.get(i) == tagged_table_ndx && m_subspecs.get(i + 1) == tagged_column_ndx) {
            size_t pos = (i - backlinks_start) / 2;
            return backlinks_column_start + pos;
        }
    }
    REALM_ASSERT(false);
    return not_found;
}


DataType Spec::get_public_column_type(size_t ndx) const noexcept
{
    REALM_ASSERT(ndx < get_column_count());

    ColumnType type = get_column_type(ndx);

    // Hide internal types
    if (type == col_type_StringEnum)
        return type_String;

    return DataType(type);
}


bool Spec::operator==(const Spec& spec) const noexcept
{
    if (!m_attr.compare(spec.m_attr))
        return false;
    if (!m_names.compare_string(spec.m_names))
        return false;

    // check each column's type
    const size_t column_count = get_column_count();
    for (size_t col_ndx = 0; col_ndx < column_count; ++col_ndx) {
        ColumnType col_type = ColumnType(m_types.get(col_ndx));
        switch (col_type) {
            case col_type_String:
            case col_type_StringEnum: {
                // These types are considered equal as col_type_StringEnum is used for an internal optimization only
                const int64_t rhs_type = spec.m_types.get(col_ndx);
                if (rhs_type != col_type_String && rhs_type != col_type_StringEnum)
                    return false;
                break;
            }
            case col_type_Link:
            case col_type_LinkList: {
                // In addition to name and attributes, the link target table must also be compared
                auto lhs_table_key = get_opposite_link_table_key(col_ndx);
                auto rhs_table_key = spec.get_opposite_link_table_key(col_ndx);
                if (lhs_table_key != rhs_table_key)
                    return false;
                break;
            }
            case col_type_Int:
            case col_type_Bool:
            case col_type_Binary:
            case col_type_OldTable:
            case col_type_OldMixed:
            case col_type_OldDateTime:
            case col_type_Timestamp:
            case col_type_Float:
            case col_type_Double:
            case col_type_Reserved4:
            case col_type_BackLink:
                // All other column types are compared as before
                if (m_types.get(col_ndx) != spec.m_types.get(col_ndx))
                    return false;
                break;
        }
    }

    return true;
}


#ifdef REALM_DEBUG // LCOV_EXCL_START ignore debug functions

void Spec::verify() const
{
    REALM_ASSERT(m_names.size() == get_column_count());
    REALM_ASSERT(m_types.size() == get_column_count());
    REALM_ASSERT(m_attr.size() == get_column_count());

    REALM_ASSERT(m_types.get_ref() == m_top.get_as_ref(0));
    REALM_ASSERT(m_names.get_ref() == m_top.get_as_ref(1));
    REALM_ASSERT(m_attr.get_ref() == m_top.get_as_ref(2));
}


void Spec::to_dot(std::ostream& out, StringData title) const
{
    ref_type top_ref = m_top.get_ref();

    out << "subgraph cluster_specset" << top_ref << " {" << std::endl;
    out << " label = \"specset " << title << "\";" << std::endl;

    std::string types_name = "types (" + util::to_string(m_types.size()) + ")";
    std::string names_name = "names (" + util::to_string(m_names.size()) + ")";
    std::string attr_name = "attrs (" + util::to_string(m_attr.size()) + ")";

    m_top.to_dot(out);
    m_types.to_dot(out, types_name);
    m_names.to_dot(out, names_name);
    m_attr.to_dot(out, attr_name);

    size_t num_cols = m_types.size();
    bool have_subspecs = false;
    for (size_t i = 0; i < num_cols; ++i) {
        ColumnType type = ColumnType(m_types.get(i));
        if (type == col_type_Link || type == col_type_LinkList || type == col_type_BackLink) {
            have_subspecs = true;
            break;
        }
    }

    if (have_subspecs) {
        REALM_ASSERT(m_subspecs.is_attached());
        m_subspecs.to_dot(out, "subspecs");
    }

    out << "}" << std::endl;
}

#endif // LCOV_EXCL_STOP ignore debug functions
