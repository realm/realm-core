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
#include <realm/group.hpp>
using namespace realm;

Spec::~Spec() noexcept {}

void Spec::detach() noexcept
{
    m_top.detach();
}

bool Spec::init(ref_type ref) noexcept
{
    MemRef mem(ref, get_alloc());
    init(mem);
    return true;
}

void Spec::init(MemRef mem) noexcept
{
    m_top.init_from_mem(mem);
    size_t top_size = m_top.size();
    REALM_ASSERT(top_size > 2 && top_size <= 6);

    m_types.init_from_ref(m_top.get_as_ref(0));
    m_names.init_from_ref(m_top.get_as_ref(1));
    m_attr.init_from_ref(m_top.get_as_ref(2));

    while (m_top.size() < 6) {
        m_top.add(0);
    }

    // Enumkeys array is only there when there are StringEnum columns
    if (auto ref = m_top.get_as_ref(4)) {
        m_enumkeys.init_from_ref(ref);
    }
    else {
        m_enumkeys.detach();
    }

    if (m_top.get_as_ref(5) == 0) {
        // This is an upgrade - create column key array
        MemRef mem_ref = Array::create_empty_array(Array::type_Normal, false, m_top.get_alloc()); // Throws
        m_keys.init_from_mem(mem_ref);
        m_keys.update_parent();
        size_t num_cols = m_types.size();
        for (size_t i = 0; i < num_cols; i++) {
            m_keys.add(i);
        }
    }
    else {
        m_keys.init_from_parent();
    }


    update_internals();
}

void Spec::update_internals() noexcept
{
    m_num_public_columns = 0;
    size_t n = m_types.size();
    for (size_t i = 0; i < n; ++i) {
        if (ColumnType(int(m_types.get(i))) == col_type_BackLink) {
            // Now we have no more public columns
            return;
        }
        m_num_public_columns++;
    }
}

void Spec::update_from_parent() noexcept
{
    m_top.update_from_parent();
    m_types.update_from_parent();
    m_names.update_from_parent();
    m_attr.update_from_parent();

    if (m_top.get_as_ref(4) != 0) {
        m_enumkeys.update_from_parent();
    }
    else {
        m_enumkeys.detach();
    }

    m_keys.update_from_parent();

    update_internals();
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
    spec_set.add(0); // Subspecs array
    spec_set.add(0); // Enumkeys array
    {
        // One key for each column
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

ColKey Spec::update_colkey(ColKey existing_key, size_t spec_ndx, TableKey table_key)
{
    auto attr = get_column_attr(spec_ndx);
    // index and uniqueness are not passed on to the key, so clear them
    attr.reset(col_attr_Indexed);
    attr.reset(col_attr_Unique);
    auto type = get_column_type(spec_ndx);
    if (existing_key.get_type() != type || existing_key.get_attrs() != attr) {
        unsigned upper = unsigned(table_key.value);

        return ColKey(ColKey::Idx{existing_key.get_index().val}, type, attr, upper);
    }
    // Existing key is valid
    return existing_key;
}

bool Spec::convert_column_attributes()
{
    bool changes = false;
    size_t enumkey_ndx = 0;

    for (size_t column_ndx = 0; column_ndx < m_types.size(); column_ndx++) {
        if (column_ndx < m_names.size()) {
            StringData name = m_names.get(column_ndx);
            if (name.size() == 0) {
                auto new_name = std::string("col_") + util::to_string(column_ndx);
                m_names.set(column_ndx, new_name);
                changes = true;
            }
            else if (m_names.find_first(name) != column_ndx) {
                auto new_name = std::string(name.data()) + '_' + util::to_string(column_ndx);
                m_names.set(column_ndx, new_name);
                changes = true;
            }
        }
        ColumnType type = ColumnType(int(m_types.get(column_ndx)));
        ColumnAttrMask attr = ColumnAttrMask(m_attr.get(column_ndx));
        switch (type) {
            case col_type_Link:
                if (!attr.test(col_attr_Nullable)) {
                    attr.set(col_attr_Nullable);
                    m_attr.set(column_ndx, attr.m_value);
                    changes = true;
                }
                break;
            case col_type_LinkList:
                if (!attr.test(col_attr_List)) {
                    attr.set(col_attr_List);
                    m_attr.set(column_ndx, attr.m_value);
                    changes = true;
                }
                break;
            default:
                if (type == col_type_OldTable) {
                    Array subspecs(m_top.get_alloc());
                    subspecs.set_parent(&m_top, 3);
                    subspecs.init_from_parent();

                    Spec sub_spec(get_alloc());
                    size_t subspec_ndx = get_subspec_ndx(column_ndx);
                    ref_type ref = to_ref(subspecs.get(subspec_ndx)); // Throws
                    sub_spec.init(ref);
                    REALM_ASSERT(sub_spec.get_column_count() == 1);
                    m_types.set(column_ndx, int(sub_spec.get_column_type(0)));
                    m_attr.set(column_ndx, m_attr.get(column_ndx) | sub_spec.m_attr.get(0) | col_attr_List);
                    sub_spec.destroy();

                    subspecs.erase(subspec_ndx);
                    changes = true;
                }
                else if (type == col_type_OldStringEnum) {
                    m_types.set(column_ndx, int(col_type_String));
                    // We need to padd zeroes into the m_enumkeys so that the index in
                    // m_enumkeys matches the column index.
                    for (size_t i = enumkey_ndx; i < column_ndx; i++) {
                        m_enumkeys.insert(i, 0);
                    }
                    enumkey_ndx = column_ndx + 1;
                    changes = true;
                }
                else {
                    REALM_ASSERT_RELEASE(type.is_valid());
                }
                break;
        }
    }
    if (m_enumkeys.is_attached()) {
        while (m_enumkeys.size() < m_num_public_columns) {
            m_enumkeys.add(0);
        }
    }
    return changes;
}

bool Spec::convert_column_keys(TableKey table_key)
{
    // This step will ensure that the column keys has right attribute and type info
    bool changes = false;
    auto sz = m_types.size();
    for (size_t ndx = 0; ndx < sz; ndx++) {
        ColKey existing_key = ColKey{m_keys.get(ndx)};
        ColKey col_key = update_colkey(existing_key, ndx, table_key);
        if (col_key != existing_key) {
            m_keys.set(ndx, col_key.value);
            changes = true;
        }
    }
    return changes;
}

void Spec::fix_column_keys(TableKey table_key)
{
    if (get_column_name(m_num_public_columns - 1) == "!ROW_INDEX") {
        unsigned idx = unsigned(m_types.size()) - 1;
        size_t ndx = m_num_public_columns - 1;
        // Fixing "!ROW_INDEX" column
        {
            ColKey col_key(ColKey::Idx{idx}, col_type_Int, ColumnAttrMask(), table_key.value);
            m_keys.set(ndx, col_key.value);
        }
        // Fix backlink columns
        idx = unsigned(m_num_public_columns) - 1;
        for (ndx = m_num_public_columns; ndx < m_types.size(); ndx++, idx++) {
            ColKey col_key(ColKey::Idx{idx}, col_type_BackLink, ColumnAttrMask(), table_key.value);
            m_keys.set(ndx, col_key.value);
        }
    }
}

void Spec::insert_column(size_t column_ndx, ColKey col_key, ColumnType type, StringData name, int attr)
{
    REALM_ASSERT(column_ndx <= m_types.size());

    if (REALM_UNLIKELY(name.size() > Table::max_column_name_length)) {
        throw LogicError(LogicError::column_name_too_long);
    }
    if (get_column_index(name) != npos) {
        throw LogicError(LogicError::column_name_in_use);
    }

    if (type != col_type_BackLink) {
        m_names.insert(column_ndx, name); // Throws
        m_num_public_columns++;
    }

    m_types.insert(column_ndx, int(type)); // Throws
    // FIXME: So far, attributes are never reported to the replication system
    m_attr.insert(column_ndx, attr); // Throws
    m_keys.insert(column_ndx, col_key.value);

    if (m_enumkeys.is_attached() && type != col_type_BackLink) {
        m_enumkeys.insert(column_ndx, 0);
    }

    update_internals();
}

void Spec::erase_column(size_t column_ndx)
{
    REALM_ASSERT(column_ndx < m_types.size());

    if (ColumnType(int(m_types.get(column_ndx))) != col_type_BackLink) {
        if (is_string_enum_type(column_ndx)) {
            // Enum columns do also have a separate key list
            ref_type keys_ref = m_enumkeys.get_as_ref(column_ndx);
            Array::destroy_deep(keys_ref, m_top.get_alloc());
            m_enumkeys.set(column_ndx, 0);
        }

        // Remove this column from the enum keys lookup and clean it up if it's now empty
        if (m_enumkeys.is_attached()) {
            m_enumkeys.erase(column_ndx); // Throws
            bool all_empty = true;
            for (size_t i = 0; i < m_enumkeys.size(); i++) {
                if (m_enumkeys.get(i) != 0) {
                    all_empty = false;
                    break;
                }
            }
            if (all_empty) {
                m_enumkeys.destroy_deep();
                m_top.set(4, 0);
            }
        }
        m_num_public_columns--;
        m_names.erase(column_ndx); // Throws
    }

    // Delete the entries common for all columns
    m_types.erase(column_ndx); // Throws
    m_attr.erase(column_ndx);  // Throws
    m_keys.erase(column_ndx);

    update_internals();
}


// For link and link list columns the old subspec array contain an entry which
// is the group-level table index of the target table, and for backlink
// columns the first entry is the group-level table index of the origin
// table, and the second entry is the index of the origin column in the
// origin table.
size_t Spec::get_subspec_ndx(size_t column_ndx) const noexcept
{
    REALM_ASSERT(column_ndx == get_column_count() || get_column_type(column_ndx) == col_type_Link ||
                 get_column_type(column_ndx) == col_type_LinkList ||
                 get_column_type(column_ndx) == col_type_BackLink ||
                 // col_type_OldTable is used when migrating from file format 9 to 10.
                 get_column_type(column_ndx) == col_type_OldTable);

    size_t subspec_ndx = 0;
    for (size_t i = 0; i != column_ndx; ++i) {
        ColumnType type = ColumnType(int(m_types.get(i)));
        if (type == col_type_Link || type == col_type_LinkList) {
            subspec_ndx += 1; // index of dest column
        }
        else if (type == col_type_BackLink) {
            subspec_ndx += 2; // index of table and index of linked column
        }
    }
    return subspec_ndx;
}


void Spec::upgrade_string_to_enum(size_t column_ndx, ref_type keys_ref)
{
    REALM_ASSERT(get_column_type(column_ndx) == col_type_String);

    // Create the enumkeys list if needed
    if (!m_enumkeys.is_attached()) {
        m_enumkeys.create(Array::type_HasRefs, false, m_num_public_columns);
        m_top.set(4, m_enumkeys.get_ref());
        m_enumkeys.set_parent(&m_top, 4);
    }

    // Insert the new key list
    m_enumkeys.set(column_ndx, keys_ref);
}

bool Spec::is_string_enum_type(size_t column_ndx) const noexcept
{
    return m_enumkeys.is_attached() ? (m_enumkeys.get(column_ndx) != 0) : false;
}

ref_type Spec::get_enumkeys_ref(size_t column_ndx, ArrayParent*& keys_parent) noexcept
{
    // We also need to return parent info
    keys_parent = &m_enumkeys;

    return m_enumkeys.get_as_ref(column_ndx);
}

TableKey Spec::get_opposite_link_table_key(size_t column_ndx) const noexcept
{
    REALM_ASSERT(column_ndx < get_column_count());
    REALM_ASSERT(get_column_type(column_ndx) == col_type_Link || get_column_type(column_ndx) == col_type_LinkList ||
                 get_column_type(column_ndx) == col_type_BackLink);

    // Key of opposite table is stored as tagged int in the
    // subspecs array
    size_t subspec_ndx = get_subspec_ndx(column_ndx);
    Array subspecs(m_top.get_alloc());
    subspecs.init_from_ref(m_top.get_as_ref(3));
    int64_t tagged_value = subspecs.get(subspec_ndx);
    REALM_ASSERT(tagged_value != 0); // can't retrieve it if never set

    uint64_t table_ref = uint64_t(tagged_value) >> 1;

    REALM_ASSERT(!util::int_cast_has_overflow<uint32_t>(table_ref));
    return TableKey(uint32_t(table_ref));
}

size_t Spec::get_origin_column_ndx(size_t backlink_col_ndx) const noexcept
{
    REALM_ASSERT(backlink_col_ndx < get_column_count());
    REALM_ASSERT(get_column_type(backlink_col_ndx) == col_type_BackLink);

    // Origin column is stored as second tagged int in the subspecs array
    size_t subspec_ndx = get_subspec_ndx(backlink_col_ndx);
    Array subspecs(m_top.get_alloc());
    subspecs.init_from_ref(m_top.get_as_ref(3));
    int64_t tagged_value = subspecs.get(subspec_ndx + 1);
    REALM_ASSERT(tagged_value != 0); // can't retrieve it if never set
    return size_t(tagged_value) >> 1;
}

ColKey Spec::find_backlink_column(TableKey origin_table_key, size_t spec_ndx) const noexcept
{
    size_t backlinks_column_start = m_num_public_columns;
    size_t backlinks_start = get_subspec_ndx(backlinks_column_start);
    Array subspecs(m_top.get_alloc());
    subspecs.init_from_ref(m_top.get_as_ref(3));
    size_t count = subspecs.size();

    int64_t tagged_table_ndx = (origin_table_key.value << 1) + 1;
    int64_t tagged_column_ndx = (spec_ndx << 1) + 1;

    size_t col_ndx = realm::npos;
    for (size_t i = backlinks_start; i < count; i += 2) {
        if (subspecs.get(i) == tagged_table_ndx && subspecs.get(i + 1) == tagged_column_ndx) {
            size_t pos = (i - backlinks_start) / 2;
            col_ndx = backlinks_column_start + pos;
            break;
        }
    }
    REALM_ASSERT(col_ndx != realm::npos);
    return ColKey{m_keys.get(col_ndx)};
}

namespace {

template <class T>
bool compare(const T& a, const T& b)
{
    if (a.size() != b.size())
        return false;

    for (size_t i = 0; i < a.size(); ++i) {
        if (b.get(i) != a.get(i))
            return false;
    }

    return true;
}

} // namespace

bool Spec::operator==(const Spec& spec) const noexcept
{
    if (!compare(m_attr, spec.m_attr))
        return false;
    if (!m_names.compare_string(spec.m_names))
        return false;

    // check each column's type
    const size_t column_count = get_column_count();
    for (size_t col_ndx = 0; col_ndx < column_count; ++col_ndx) {
        ColumnType col_type = ColumnType(int(m_types.get(col_ndx)));
        switch (col_type) {
            case col_type_Link:
            case col_type_TypedLink:
            case col_type_LinkList: {
                // In addition to name and attributes, the link target table must also be compared
                REALM_ASSERT(false); // We can no longer compare specs - in fact we don't want to
                /*
                auto lhs_table_key = get_opposite_link_table_key(col_ndx);
                auto rhs_table_key = spec.get_opposite_link_table_key(col_ndx);
                if (lhs_table_key != rhs_table_key)
                    return false;
                */
                break;
            }
            case col_type_Int:
            case col_type_Bool:
            case col_type_Binary:
            case col_type_String:
            case col_type_Mixed:
            case col_type_Timestamp:
            case col_type_Float:
            case col_type_Double:
            case col_type_Decimal:
            case col_type_BackLink:
            case col_type_ObjectId:
            case col_type_UUID:
                // All other column types are compared as before
                if (m_types.get(col_ndx) != spec.m_types.get(col_ndx))
                    return false;
                break;
        }
    }

    return true;
}


ColKey Spec::get_key(size_t column_ndx) const
{
    auto key = ColKey(m_keys.get(column_ndx));
    REALM_ASSERT(key.get_type().is_valid());
    return key;
}

void Spec::verify() const
{
#ifdef REALM_DEBUG
    REALM_ASSERT(m_names.size() == get_public_column_count());
    REALM_ASSERT(m_types.size() == get_column_count());
    REALM_ASSERT(m_attr.size() == get_column_count());

    REALM_ASSERT(m_types.get_ref() == m_top.get_as_ref(0));
    REALM_ASSERT(m_names.get_ref() == m_top.get_as_ref(1));
    REALM_ASSERT(m_attr.get_ref() == m_top.get_as_ref(2));
#endif
}
