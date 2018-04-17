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

#include "realm/obj.hpp"
#include "realm/cluster_tree.hpp"
#include "realm/array_basic.hpp"
#include "realm/array_integer.hpp"
#include "realm/array_bool.hpp"
#include "realm/array_string.hpp"
#include "realm/array_binary.hpp"
#include "realm/array_timestamp.hpp"
#include "realm/array_key.hpp"
#include "realm/array_backlink.hpp"
#include "realm/column_type_traits.hpp"
#include "realm/index_string.hpp"
#include "realm/cluster_tree.hpp"
#include "realm/spec.hpp"
#include "realm/replication.hpp"

using namespace realm;


/********************************* ConstObj **********************************/

ConstObj::ConstObj(const ClusterTree* tree_top, ref_type ref, ObjKey key, size_t row_ndx)
    : m_tree_top(tree_top)
    , m_key(key)
    , m_valid(true)
    , m_mem(ref, tree_top->get_alloc())
    , m_row_ndx(row_ndx)
{
    m_instance_version = m_tree_top->get_instance_version();
    m_storage_version = m_tree_top->get_storage_version(m_instance_version);
}

Allocator& ConstObj::get_alloc() const
{
    return m_tree_top->get_alloc();
}

int ConstObj::cmp(const ConstObj& other, ColKey col_key) const
{
    return cmp(other, m_tree_top->get_owner()->colkey2ndx(col_key));
}


template <class T>
inline int ConstObj::cmp(const ConstObj& other, size_t col_ndx) const
{
    T val1 = _get<T>(col_ndx);
    T val2 = other._get<T>(col_ndx);
    if (val1 < val2) {
        return -1;
    }
    else if (val1 > val2) {
        return 1;
    }
    return 0;
}

int ConstObj::cmp(const ConstObj& other, size_t col_ndx) const
{
    const Spec& spec = m_tree_top->get_spec();
    ColumnAttrMask attr = spec.get_column_attr(col_ndx);
    REALM_ASSERT(!attr.test(col_attr_List)); // TODO: implement comparison of lists

    switch (spec.get_public_column_type(col_ndx)) {
        case type_Int:
            if (attr.test(col_attr_Nullable))
                return cmp<util::Optional<Int>>(other, col_ndx);
            else
                return cmp<Int>(other, col_ndx);
        case type_Bool:
            return cmp<Bool>(other, col_ndx);
        case type_Float:
            return cmp<Float>(other, col_ndx);
        case type_Double:
            return cmp<Double>(other, col_ndx);
        case type_String:
            return cmp<String>(other, col_ndx);
        case type_Binary:
            return cmp<Binary>(other, col_ndx);
        case type_Timestamp:
            return cmp<Timestamp>(other, col_ndx);
        case type_Link:
            return cmp<ObjKey>(other, col_ndx);
        case type_OldDateTime:
        case type_OldTable:
        case type_OldMixed:
        case type_LinkList:
            REALM_ASSERT(false);
            break;
    }
    return 0;
}

bool ConstObj::operator==(const ConstObj& other) const
{
    size_t col_cnt = m_tree_top->get_spec().get_public_column_count();
    while (col_cnt--) {
        if (cmp(other, col_cnt) != 0) {
            return false;
        }
    }
    return true;
}

const Table* ConstObj::get_table() const
{
    return m_tree_top->get_owner();
}

bool ConstObj::is_valid() const
{
    // Cache valid state. If once invalid, it can never become valid again
    if (m_valid)
        m_valid = get_table()->is_valid(m_key);

    return m_valid;
}

void ConstObj::remove()
{
    const_cast<Table*>(get_table())->remove_object(m_key);
}

ColKey ConstObj::get_column_key(StringData col_name) const
{
    return get_table()->get_column_key(col_name);
}

TableKey ConstObj::get_table_key() const
{
    return get_table()->get_key();
}

TableRef ConstObj::get_target_table(ColKey col_key) const
{
    return _impl::TableFriend::get_opposite_link_table(*m_tree_top->get_owner(), col_key);
}

Obj::Obj(ClusterTree* tree_top, ref_type ref, ObjKey key, size_t row_ndx)
    : ConstObj(tree_top, ref, key, row_ndx)
    , m_writeable(!tree_top->get_alloc().is_read_only(ref))
{
}

bool ConstObj::is_in_sync() const
{
    auto current_version = m_tree_top->get_storage_version(m_instance_version);
    return (current_version == m_storage_version);
}

// FIXME: Optimization - all the work needed to bump version counters
// and to check if it has changed must be optimized to avoid indirections
// and to allow inline compilation of the whole code path.
bool ConstObj::update_if_needed() const
{
    auto current_version = m_tree_top->get_storage_version(m_instance_version);
    if (current_version != m_storage_version) {
        // Get a new object from key
        ConstObj new_obj = m_tree_top->get(m_key);
        update(new_obj);

        return true;
    }
    return false;
}

template <class T>
T ConstObj::get(ColKey col_key) const
{
    return _get<T>(get_table()->colkey2ndx(col_key));
}

template <class T>
T ConstObj::_get(size_t col_ndx) const
{
    REALM_ASSERT(col_ndx < m_tree_top->get_spec().get_public_column_count());
    const Spec& spec = m_tree_top->get_spec();
    ColumnAttrMask attr = spec.get_column_attr(col_ndx);
    REALM_ASSERT(attr.test(col_attr_List) || (spec.get_column_type(col_ndx) == ColumnTypeTraits<T>::column_id));

    update_if_needed();

    typename ColumnTypeTraits<T>::cluster_leaf_type values(m_tree_top->get_alloc());
    ref_type ref = to_ref(Array::get(m_mem.get_addr(), col_ndx + 1));
    values.set_spec(const_cast<Spec*>(&spec), col_ndx);
    values.init_from_ref(ref);

    return values.get(m_row_ndx);
}

ConstObj ConstObj::get_linked_object(ColKey link_col_key) const
{
    TableRef target_table = get_target_table(link_col_key);
    ObjKey key = get<ObjKey>(link_col_key);

    return target_table->get_object(key);
}

template <class T>
inline bool ConstObj::do_is_null(size_t col_ndx) const
{
    T values(m_tree_top->get_alloc());
    ref_type ref = to_ref(Array::get(m_mem.get_addr(), col_ndx + 1));
    values.set_spec(const_cast<Spec*>(&m_tree_top->get_spec()), col_ndx);
    values.init_from_ref(ref);
    return values.is_null(m_row_ndx);
}

size_t ConstObj::get_link_count(ColKey col_key) const
{
    return get_list<ObjKey>(col_key).size();
}

bool ConstObj::is_null(ColKey col_key) const
{
    size_t col_ndx = get_table()->colkey2ndx(col_key);

    update_if_needed();
    ColumnAttrMask attr = m_tree_top->get_spec().get_column_attr(col_ndx);

    if (attr.test(col_attr_List)) {
        ArrayInteger values(m_tree_top->get_alloc());
        ref_type ref = to_ref(Array::get(m_mem.get_addr(), col_ndx + 1));
        values.init_from_ref(ref);
        return values.get(m_row_ndx) == 0;
    }

    if (attr.test(col_attr_Nullable)) {
        switch (m_tree_top->get_spec().get_column_type(col_ndx)) {
            case col_type_Int:
                return do_is_null<ArrayIntNull>(col_ndx);
            case col_type_Bool:
                return do_is_null<ArrayBoolNull>(col_ndx);
            case col_type_Float:
                return do_is_null<ArrayFloat>(col_ndx);
            case col_type_Double:
                return do_is_null<ArrayDouble>(col_ndx);
            case col_type_String:
                return do_is_null<ArrayString>(col_ndx);
            case col_type_Binary:
                return do_is_null<ArrayBinary>(col_ndx);
            case col_type_Timestamp:
                return do_is_null<ArrayTimestamp>(col_ndx);
            case col_type_Link:
                return do_is_null<ArrayKey>(col_ndx);
            default:
                break;
        }
    }
    return false;
}


// Figure out if this object has any remaining backlinkss
bool ConstObj::has_backlinks(bool only_strong_links) const
{
    const Spec& spec = m_tree_top->get_spec();
    size_t backlink_columns_begin = spec.first_backlink_column_index();
    size_t backlink_columns_end = backlink_columns_begin + spec.backlink_column_count();

    const Table* target_table = m_tree_top->get_owner();
    const Spec& target_table_spec = _impl::TableFriend::get_spec(*target_table);

    for (size_t i = backlink_columns_begin; i != backlink_columns_end; ++i) {
        ColKey backlink_col_key = target_table->ndx2colkey(i);

        // Find origin table and column for this backlink column
        TableRef origin_table = _impl::TableFriend::get_opposite_link_table(*target_table, backlink_col_key);
        auto origin_col = target_table_spec.get_origin_column_key(i);

        if (!only_strong_links || origin_table->get_link_type(origin_col) == link_Strong) {
            auto cnt = get_backlink_count(*origin_table, origin_col);
            if (cnt)
                return true;
        }
    }

    return false;
}

size_t ConstObj::get_backlink_count(const Table& origin, ColKey origin_col_key) const
{
    size_t cnt = 0;
    TableKey origin_table_key = origin.get_key();
    if (origin_table_key != TableKey()) {
        size_t backlink_col_key = m_tree_top->get_spec().find_backlink_column(origin_table_key, origin_col_key);
        cnt = get_backlink_count(backlink_col_key);
    }
    return cnt;
}

ObjKey ConstObj::get_backlink(const Table& origin, ColKey origin_col_key, size_t backlink_ndx) const
{
    TableKey origin_key = origin.get_key();
    size_t backlink_col_key = m_tree_top->get_spec().find_backlink_column(origin_key, origin_col_key);

    return get_backlink(backlink_col_key, backlink_ndx);
}

size_t ConstObj::get_backlink_count(size_t backlink_col_ndx) const
{
    Allocator& alloc = m_tree_top->get_alloc();
    Array fields(alloc);
    fields.init_from_mem(m_mem);

    ArrayBacklink backlinks(alloc);
    backlinks.set_parent(&fields, backlink_col_ndx + 1);
    backlinks.init_from_parent();

    return backlinks.get_backlink_count(m_row_ndx);
}

ObjKey ConstObj::get_backlink(size_t backlink_col_ndx, size_t backlink_ndx) const
{
    Allocator& alloc = m_tree_top->get_alloc();
    Array fields(alloc);
    fields.init_from_mem(m_mem);

    ArrayBacklink backlinks(alloc);
    backlinks.set_parent(&fields, backlink_col_ndx + 1);
    backlinks.init_from_parent();
    return backlinks.get_backlink(m_row_ndx, backlink_ndx);
}

/*********************************** Obj *************************************/

bool Obj::update_if_needed() const
{
    bool updated = ConstObj::update_if_needed();
    if (updated) {
        m_writeable = !m_tree_top->get_alloc().is_read_only(m_mem.get_ref());
    }
    return updated;
}

void Obj::ensure_writeable()
{
    if (!m_writeable) {
        m_mem = const_cast<ClusterTree*>(m_tree_top)->ensure_writeable(m_key);
        m_writeable = true;
    }
}

void Obj::bump_content_version()
{
    Allocator& alloc = m_tree_top->get_alloc();
    alloc.bump_content_version();
}

void Obj::bump_both_versions()
{
    Allocator& alloc = m_tree_top->get_alloc();
    alloc.bump_content_version();
    alloc.bump_storage_version();
}

template <>
Obj& Obj::set<int64_t>(ColKey col_key, int64_t value, bool is_default)
{
    size_t col_ndx = get_table()->colkey2ndx(col_key);
    auto& spec = m_tree_top->get_spec();
    if (spec.get_column_type(col_ndx) != ColumnTypeTraits<int64_t>::column_id)
        throw LogicError(LogicError::illegal_type);

    update_if_needed();
    ensure_writeable();

    if (StringIndex* index = m_tree_top->get_owner()->get_search_index(col_key)) {
        index->set<int64_t>(m_key, value);
    }

    Allocator& alloc = m_tree_top->get_alloc();
    alloc.bump_content_version();
    Array fallback(alloc);
    Array& fields = m_tree_top->get_fields_accessor(fallback, m_mem);
    REALM_ASSERT(col_ndx + 1 < fields.size());
    ColumnAttrMask attr = spec.get_column_attr(col_ndx);
    if (attr.test(col_attr_Nullable)) {
        ArrayIntNull values(alloc);
        values.set_parent(&fields, col_ndx + 1);
        values.init_from_parent();
        values.set(m_row_ndx, value);
    }
    else {
        ArrayInteger values(alloc);
        values.set_parent(&fields, col_ndx + 1);
        values.init_from_parent();
        values.set(m_row_ndx, value);
    }

    if (Replication* repl = alloc.get_replication()) {
        repl->set_int(m_tree_top->get_owner(), col_key, m_key, value,
                      is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws
    }

    return *this;
}

Obj& Obj::add_int(ColKey col_key, int64_t value)
{
    size_t col_ndx = get_table()->colkey2ndx(col_key);

    update_if_needed();
    ensure_writeable();

    auto add_wrap = [](int64_t a, int64_t b) -> int64_t {
        uint64_t ua = uint64_t(a);
        uint64_t ub = uint64_t(b);
        return int64_t(ua + ub);
    };

    Allocator& alloc = m_tree_top->get_alloc();
    alloc.bump_content_version();
    Array fallback(alloc);
    Array& fields = m_tree_top->get_fields_accessor(fallback, m_mem);
    REALM_ASSERT(col_ndx + 1 < fields.size());
    ColumnAttrMask attr = m_tree_top->get_spec().get_column_attr(col_ndx);
    if (attr.test(col_attr_Nullable)) {
        ArrayIntNull values(alloc);
        values.set_parent(&fields, col_ndx + 1);
        values.init_from_parent();
        Optional<int64_t> old = values.get(m_row_ndx);
        if (old) {
            values.set(m_row_ndx, add_wrap(*old, value));
        }
        else {
            throw LogicError{LogicError::illegal_combination};
        }
    }
    else {
        ArrayInteger values(alloc);
        values.set_parent(&fields, col_ndx + 1);
        values.init_from_parent();
        int64_t old = values.get(m_row_ndx);
        values.set(m_row_ndx, add_wrap(old, value));
    }

    if (Replication* repl = alloc.get_replication()) {
        repl->add_int(m_tree_top->get_owner(), col_key, m_key, value); // Throws
    }

    return *this;
}

template <>
Obj& Obj::set<ObjKey>(ColKey col_key, ObjKey target_key, bool is_default)
{
    size_t col_ndx = get_table()->colkey2ndx(col_key);
    auto& spec = m_tree_top->get_spec();
    if (spec.get_column_type(col_ndx) != ColumnTypeTraits<ObjKey>::column_id)
        throw LogicError(LogicError::illegal_type);
    TableRef target_table = get_target_table(col_key);
    if (target_key != null_key && !target_table->is_valid(target_key)) {
        throw LogicError(LogicError::target_row_index_out_of_range);
    }

    update_if_needed();
    ensure_writeable();

    Allocator& alloc = m_tree_top->get_alloc();
    alloc.bump_content_version();
    Array fallback(alloc);
    Array& fields = m_tree_top->get_fields_accessor(fallback, m_mem);
    REALM_ASSERT(col_ndx + 1 < fields.size());
    ArrayKey values(alloc);
    values.set_parent(&fields, col_ndx + 1);
    values.init_from_parent();

    ObjKey old_key = values.get(m_row_ndx);

    if (target_key != old_key) {
        CascadeState state;
        bool recurse = replace_backlink(col_key, old_key, target_key, state);

        values.set(m_row_ndx, target_key);

        if (Replication* repl = alloc.get_replication()) {
            repl->set(m_tree_top->get_owner(), col_key, m_key, target_key,
                      is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws
        }

        if (recurse)
            _impl::TableFriend::remove_recursive(*target_table, state);
    }

    return *this;
}

namespace {
template <class T>
inline bool value_is_null(const T& val)
{
    return val.is_null();
}
template <>
inline bool value_is_null(const bool&)
{
    return false;
}
template <>
inline bool value_is_null(const float& val)
{
    return null::is_null_float(val);
}
template <>
inline bool value_is_null(const double& val)
{
    return null::is_null_float(val);
}

template <class T>
inline void check_range(const T&)
{
}
template <>
inline void check_range(const StringData& val)
{
    if (REALM_UNLIKELY(val.size() > Table::max_string_size))
        throw LogicError(LogicError::string_too_big);
}
template <>
inline void check_range(const BinaryData& val)
{
    if (REALM_UNLIKELY(val.size() > ArrayBlob::max_binary_size))
        throw LogicError(LogicError::binary_too_big);
}
}

template <class T>
Obj& Obj::set(ColKey col_key, T value, bool is_default)
{
    const Spec& spec = m_tree_top->get_spec();
    size_t col_ndx = get_table()->colkey2ndx(col_key);
    if (spec.get_column_type(col_ndx) != ColumnTypeTraits<T>::column_id)
        throw LogicError(LogicError::illegal_type);
    if (value_is_null(value) && !spec.get_column_attr(col_ndx).test(col_attr_Nullable))
        throw LogicError(LogicError::column_not_nullable);
    check_range(value);

    update_if_needed();
    ensure_writeable();

    if (StringIndex* index = m_tree_top->get_owner()->get_search_index(col_key)) {
        index->set<T>(m_key, value);
    }

    Allocator& alloc = m_tree_top->get_alloc();
    alloc.bump_content_version();
    Array fallback(alloc);
    Array& fields = m_tree_top->get_fields_accessor(fallback, m_mem);
    REALM_ASSERT(col_ndx + 1 < fields.size());
    typename ColumnTypeTraits<T>::cluster_leaf_type values(alloc);
    values.set_parent(&fields, col_ndx + 1);
    values.set_spec(const_cast<Spec*>(&spec), col_ndx);
    values.init_from_parent();
    values.set(m_row_ndx, value);

    if (Replication* repl = alloc.get_replication())
        repl->set<T>(m_tree_top->get_owner(), col_key, m_key, value,
                     is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws

    return *this;
}

void Obj::set_int(ColKey col_key, int64_t value)
{
    update_if_needed();
    ensure_writeable();

    size_t col_ndx = get_table()->colkey2ndx(col_key);
    Allocator& alloc = m_tree_top->get_alloc();
    alloc.bump_content_version();
    Array fallback(alloc);
    Array& fields = m_tree_top->get_fields_accessor(fallback, m_mem);
    REALM_ASSERT(col_ndx + 1 < fields.size());
    Array values(alloc);
    values.set_parent(&fields, col_ndx + 1);
    values.init_from_parent();
    values.set(m_row_ndx, value);
}

void Obj::add_backlink(ColKey backlink_col_key, ObjKey origin_key)
{
    ensure_writeable();

    size_t backlink_col_ndx = get_table()->colkey2ndx(backlink_col_key);
    Allocator& alloc = m_tree_top->get_alloc();
    alloc.bump_content_version();
    Array fallback(alloc);
    Array& fields = m_tree_top->get_fields_accessor(fallback, m_mem);

    ArrayBacklink backlinks(alloc);
    backlinks.set_parent(&fields, backlink_col_ndx + 1);
    backlinks.init_from_parent();

    backlinks.add(m_row_ndx, origin_key);
}

bool Obj::remove_one_backlink(ColKey backlink_col_key, ObjKey origin_key)
{
    ensure_writeable();

    size_t backlink_col_ndx = get_table()->colkey2ndx(backlink_col_key);
    Allocator& alloc = m_tree_top->get_alloc();
    alloc.bump_content_version();
    Array fallback(alloc);
    Array& fields = m_tree_top->get_fields_accessor(fallback, m_mem);

    ArrayBacklink backlinks(alloc);
    backlinks.set_parent(&fields, backlink_col_ndx + 1);
    backlinks.init_from_parent();

    return backlinks.remove(m_row_ndx, origin_key);
}

void Obj::nullify_link(ColKey origin_col_key, ObjKey target_key)
{
    ensure_writeable();

    size_t origin_col_ndx = get_table()->colkey2ndx(origin_col_key);

    Allocator& alloc = m_tree_top->get_alloc();
    Array fallback(alloc);
    Array& fields = m_tree_top->get_fields_accessor(fallback, m_mem);

    const Spec& spec = m_tree_top->get_spec();
    ColumnAttrMask attr = spec.get_column_attr(origin_col_ndx);
    if (attr.test(col_attr_List)) {
        Array linklists(alloc);
        linklists.set_parent(&fields, origin_col_ndx + 1);
        linklists.init_from_parent();

        BPlusTree<ObjKey> links(alloc);
        links.set_parent(&linklists, m_row_ndx);
        links.init_from_parent();
        size_t ndx = links.find_first(target_key);
        // There must be one
        REALM_ASSERT(ndx != realm::npos);
        links.erase(ndx);

        alloc.bump_storage_version();
    }
    else {
        ArrayKey links(alloc);
        links.set_parent(&fields, origin_col_ndx + 1);
        links.init_from_parent();
        ObjKey key = links.get(m_row_ndx);
        REALM_ASSERT(key == target_key);
        links.set(m_row_ndx, ObjKey{});
        if (Replication* repl = alloc.get_replication())
            repl->set<ObjKey>(m_tree_top->get_owner(), origin_col_key, m_key, ObjKey{}, _impl::instr_Set); // Throws

        alloc.bump_content_version();
    }
}

void Obj::set_backlink(ColKey col_key, ObjKey new_key)
{
    if (new_key != realm::null_key) {
        TableRef target_table = get_target_table(col_key);
        ColKey backlink_col_key = target_table->find_backlink_column(get_table_key(), col_key);

        Obj target_obj = target_table->get_object(new_key);
        target_obj.add_backlink(backlink_col_key, m_key); // Throws
    }
}

bool Obj::replace_backlink(ColKey col_key, ObjKey old_key, ObjKey new_key, CascadeState& state)
{
    bool recurse = remove_backlink(col_key, old_key, state);
    set_backlink(col_key, new_key);

    return recurse;
}

bool Obj::remove_backlink(ColKey col_key, ObjKey old_key, CascadeState& state)
{
    bool recurse = false;

    const Table* origin_table = m_tree_top->get_owner();

    TableRef target_table = get_target_table(col_key);
    ColKey backlink_col_key = target_table->find_backlink_column(get_table_key(), col_key);

    bool strong_links = (origin_table->get_link_type(col_key) == link_Strong);
    CascadeState::Mode mode = state.m_mode;

    if (old_key != realm::null_key) {
        Obj target_obj = target_table->get_object(old_key);
        bool last_removed = target_obj.remove_one_backlink(backlink_col_key, m_key); // Throws

        // Check if the object should be cascade deleted
        if (mode != CascadeState::none && (mode == CascadeState::all || (strong_links && last_removed))) {
            bool have_backlinks = target_obj.has_backlinks(state.m_mode == CascadeState::strong);

            if (!have_backlinks) {
                state.rows.emplace_back(target_table->get_key(), old_key);
                recurse = true;
            }
        }
    }

    return recurse;
}

namespace realm {

template int64_t ConstObj::get<int64_t>(ColKey col_key) const;
template util::Optional<int64_t> ConstObj::get<util::Optional<int64_t>>(ColKey col_key) const;
template bool ConstObj::get<Bool>(ColKey col_key) const;
template util::Optional<Bool> ConstObj::get<util::Optional<Bool>>(ColKey col_key) const;
template float ConstObj::get<float>(ColKey col_key) const;
template double ConstObj::get<double>(ColKey col_key) const;
template StringData ConstObj::get<StringData>(ColKey col_key) const;
template BinaryData ConstObj::get<BinaryData>(ColKey col_key) const;
template Timestamp ConstObj::get<Timestamp>(ColKey col_key) const;
template ObjKey ConstObj::get<ObjKey>(ColKey col_key) const;

template Obj& Obj::set<bool>(ColKey, bool, bool);
template Obj& Obj::set<float>(ColKey, float, bool);
template Obj& Obj::set<double>(ColKey, double, bool);
template Obj& Obj::set<StringData>(ColKey, StringData, bool);
template Obj& Obj::set<BinaryData>(ColKey, BinaryData, bool);
template Obj& Obj::set<Timestamp>(ColKey, Timestamp, bool);
}

template <class T>
inline void Obj::do_set_null(size_t col_ndx)
{
    Allocator& alloc = m_tree_top->get_alloc();
    alloc.bump_content_version();
    Array fallback(alloc);
    Array& fields = m_tree_top->get_fields_accessor(fallback, m_mem);

    T values(alloc);
    values.set_parent(&fields, col_ndx + 1);
    values.set_spec(const_cast<Spec*>(&m_tree_top->get_spec()), col_ndx);
    values.init_from_parent();
    values.set_null(m_row_ndx);
}

Obj& Obj::set_null(ColKey col_key, bool is_default)
{
    size_t col_ndx = get_table()->colkey2ndx(col_key);
    if (REALM_UNLIKELY(col_ndx > m_tree_top->get_spec().get_public_column_count()))
        throw LogicError(LogicError::column_index_out_of_range);
    if (REALM_UNLIKELY(!m_tree_top->get_spec().get_column_attr(col_ndx).test(col_attr_Nullable))) {
        throw LogicError(LogicError::column_not_nullable);
    }

    update_if_needed();
    ensure_writeable();

    if (StringIndex* index = m_tree_top->get_owner()->get_search_index(col_key)) {
        index->set(m_key, null{});
    }

    switch (m_tree_top->get_spec().get_column_type(col_ndx)) {
        case col_type_Int:
            do_set_null<ArrayIntNull>(col_ndx);
            break;
        case col_type_Bool:
            do_set_null<ArrayBoolNull>(col_ndx);
            break;
        case col_type_Float:
            do_set_null<ArrayFloat>(col_ndx);
            break;
        case col_type_Double:
            do_set_null<ArrayDouble>(col_ndx);
            break;
        case col_type_String:
            do_set_null<ArrayString>(col_ndx);
            break;
        case col_type_Binary:
            do_set_null<ArrayBinary>(col_ndx);
            break;
        case col_type_Timestamp:
            do_set_null<ArrayTimestamp>(col_ndx);
            break;
        case col_type_Link:
            do_set_null<ArrayKey>(col_ndx);
            break;
        default:
            break;
    }

    if (Replication* repl = m_tree_top->get_alloc().get_replication())
        repl->set_null(m_tree_top->get_owner(), col_key, m_key,
                       is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws

    return *this;
}


ColKey Obj::ndx2colkey(size_t col_ndx)
{
    return get_table()->ndx2colkey(col_ndx);
}
