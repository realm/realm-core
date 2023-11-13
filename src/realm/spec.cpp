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
    REALM_ASSERT(top_size > s_attributes_ndx && top_size <= s_spec_max_size);

    m_types.init_from_ref(m_top.get_as_ref(s_types_ndx));
    m_names.init_from_ref(m_top.get_as_ref(s_names_ndx));
    m_attr.init_from_ref(m_top.get_as_ref(s_attributes_ndx));

    while (m_top.size() < s_spec_max_size) {
        m_top.add(0);
    }

    // Enumkeys array is only there when there are StringEnum columns
    if (auto ref = m_top.get_as_ref(s_enum_keys_ndx)) {
        m_enumkeys.init_from_ref(ref);
    }
    else {
        m_enumkeys.detach();
    }

    if (m_top.get_as_ref(s_col_keys_ndx) == 0) {
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

    if (m_top.get_as_ref(s_enum_keys_ndx) != 0) {
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
    spec_set.add(0); // Nested collections array
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

bool Spec::migrate_column_keys()
{
    // Replace col_type_LinkList with col_type_Link
    constexpr int col_type_LinkList = 13;
    bool updated = false;
    auto sz = m_names.size();

    for (size_t n = 0; n < sz; n++) {
        auto t = m_types.get(n);
        if (t == col_type_LinkList) {
            auto attrs = get_column_attr(n);
            REALM_ASSERT(attrs.test(col_attr_List));
            auto col_key = ColKey(m_keys.get(n));
            ColKey new_key(col_key.get_index(), col_type_Link, attrs, col_key.get_tag());
            m_keys.set(n, new_key.value);
            updated = true;
        }
    }

    return updated;
}

void Spec::insert_column(size_t column_ndx, ColKey col_key, ColumnType type, StringData name, int attr)
{
    REALM_ASSERT(column_ndx <= m_types.size());

    if (REALM_UNLIKELY(name.size() > Table::max_column_name_length)) {
        throw InvalidArgument(ErrorCodes::InvalidName, util::format("Name too long: %1", name));
    }
    if (get_column_index(name) != npos) {
        throw InvalidArgument(ErrorCodes::InvalidName, util::format("Property name in use: %1", name));
    }

    if (type != col_type_BackLink) {
        m_names.insert(column_ndx, name); // Throws
        m_num_public_columns++;
    }

    m_types.insert(column_ndx, int(type)); // Throws
    m_attr.insert(column_ndx, attr);       // Throws
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
            case col_type_TypedLink: {
                // In addition to name and attributes, the link target table must also be compared
                REALM_ASSERT(false); // We can no longer compare specs - in fact we don't want to
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
