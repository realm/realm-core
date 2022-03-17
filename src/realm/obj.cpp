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
#include "realm/array_mixed.hpp"
#include "realm/array_timestamp.hpp"
#include "realm/array_decimal128.hpp"
#include "realm/array_key.hpp"
#include "realm/array_fixed_bytes.hpp"
#include "realm/array_backlink.hpp"
#include "realm/array_typed_link.hpp"
#include "realm/column_type_traits.hpp"
#include "realm/index_string.hpp"
#include "realm/cluster_tree.hpp"
#include "realm/spec.hpp"
#include "realm/set.hpp"
#include "realm/dictionary.hpp"
#include "realm/table_view.hpp"
#include "realm/replication.hpp"
#include "realm/util/base64.hpp"

namespace realm {

/********************************* Obj **********************************/

Obj::Obj(TableRef table, MemRef mem, ObjKey key, size_t row_ndx)
    : m_table(table)
    , m_key(key)
    , m_mem(mem)
    , m_row_ndx(row_ndx)
    , m_valid(true)
{
    m_storage_version = get_alloc().get_storage_version();
}

GlobalKey Obj::get_object_id() const
{
    return m_table->get_object_id(m_key);
}

ObjLink Obj::get_link() const
{
    return ObjLink(m_table->get_key(), m_key);
}

const TableClusterTree* Obj::get_tree_top() const
{
    if (m_key.is_unresolved()) {
        return m_table.unchecked_ptr()->m_tombstones.get();
    }
    else {
        return &m_table.unchecked_ptr()->m_clusters;
    }
}

Allocator& Obj::get_alloc() const
{
    // Do a "checked" deref to table to ensure the instance_version is correct.
    // Even if removed from the public API, this should *not* be optimized away,
    // because it is used internally in situations, where we want stale table refs
    // to be detected.
    return m_table->m_alloc;
}

Allocator& Obj::_get_alloc() const noexcept
{
    // Bypass check of table instance version. To be used only in contexts,
    // where instance version match has already been established (e.g _get<>)
    return m_table.unchecked_ptr()->m_alloc;
}

const Spec& Obj::get_spec() const
{
    return m_table.unchecked_ptr()->m_spec;
}

Replication* Obj::get_replication() const
{
    return m_table->get_repl();
}


bool Obj::operator==(const Obj& other) const
{
    for (auto ck : m_table->get_column_keys()) {
        StringData col_name = m_table->get_column_name(ck);

        auto compare_values = [&](Mixed val1, Mixed val2) {
            if (val1.is_null()) {
                if (!val2.is_null())
                    return false;
            }
            else {
                if (val1.get_type() != val2.get_type())
                    return false;
                if (val1.is_type(type_Link, type_TypedLink)) {
                    auto o1 = _get_linked_object(ck, val1);
                    auto o2 = other._get_linked_object(col_name, val2);
                    if (o1.m_table->is_embedded()) {
                        return o1 == o2;
                    }
                    else {
                        return o1.get_primary_key() == o2.get_primary_key();
                    }
                }
                else {
                    if (val1 != val2)
                        return false;
                }
            }
            return true;
        };

        if (!ck.is_collection()) {
            if (!compare_values(get_any(ck), other.get_any(col_name)))
                return false;
        }
        else {
            auto coll1 = get_collection_ptr(ck);
            auto coll2 = other.get_collection_ptr(col_name);
            size_t sz = coll1->size();
            if (coll2->size() != sz)
                return false;
            if (ck.is_list() || ck.is_set()) {
                for (size_t i = 0; i < sz; i++) {
                    if (!compare_values(coll1->get_any(i), coll2->get_any(i)))
                        return false;
                }
            }
            if (ck.is_dictionary()) {
                auto dict1 = dynamic_cast<Dictionary*>(coll1.get());
                auto dict2 = dynamic_cast<Dictionary*>(coll2.get());
                for (size_t i = 0; i < sz; i++) {
                    auto [key, value] = dict1->get_pair(i);
                    auto val2 = dict2->try_get(key);
                    if (!val2)
                        return false;
                    if (!compare_values(value, *val2))
                        return false;
                }
            }
        }
    }
    return true;
}

bool Obj::is_valid() const noexcept
{
    // Cache valid state. If once invalid, it can never become valid again
    if (m_valid)
        m_valid = bool(m_table) && (m_table.unchecked_ptr()->get_storage_version() == m_storage_version ||
                                    m_table.unchecked_ptr()->is_valid(m_key));

    return m_valid;
}

void Obj::check_valid() const
{
    if (!is_valid())
        throw std::runtime_error("Object not alive");
}

void Obj::remove()
{
    m_table.cast_away_const()->remove_object(m_key);
}

void Obj::invalidate()
{
    m_key = m_table.cast_away_const()->invalidate_object(m_key);
}

ColKey Obj::get_column_key(StringData col_name) const
{
    return get_table()->get_column_key(col_name);
}

TableKey Obj::get_table_key() const
{
    return get_table()->get_key();
}

TableRef Obj::get_target_table(ColKey col_key) const
{
    if (m_table) {
        return _impl::TableFriend::get_opposite_link_table(*m_table.unchecked_ptr(), col_key);
    }
    else {
        return TableRef();
    }
}

TableRef Obj::get_target_table(ObjLink link) const
{
    if (m_table) {
        return m_table.unchecked_ptr()->get_parent_group()->get_table(link.get_table_key());
    }
    else {
        return TableRef();
    }
}

bool Obj::update() const
{
    // Get a new object from key
    Obj new_obj = get_tree_top()->get(m_key); // Throws `KeyNotFound`

    bool changes = (m_mem.get_addr() != new_obj.m_mem.get_addr()) || (m_row_ndx != new_obj.m_row_ndx);
    if (changes) {
        m_mem = new_obj.m_mem;
        m_row_ndx = new_obj.m_row_ndx;
    }
    // Always update versions
    m_storage_version = new_obj.m_storage_version;
    m_table = new_obj.m_table;
    return changes;
}

inline bool Obj::_update_if_needed() const
{
    auto current_version = _get_alloc().get_storage_version();
    if (current_version != m_storage_version) {
        return update();
    }
    return false;
}

UpdateStatus Obj::update_if_needed_with_status() const
{
    if (!m_table) {
        // Table deleted
        return UpdateStatus::Detached;
    }

    auto current_version = get_alloc().get_storage_version();
    if (current_version != m_storage_version) {
        ClusterNode::State state = get_tree_top()->try_get(m_key);

        if (!state) {
            // Object deleted
            return UpdateStatus::Detached;
        }

        // Always update versions
        m_storage_version = current_version;
        if ((m_mem.get_addr() != state.mem.get_addr()) || (m_row_ndx != state.index)) {
            m_mem = state.mem;
            m_row_ndx = state.index;
            return UpdateStatus::Updated;
        }
    }
    return UpdateStatus::NoChange;
}

template <class T>
T Obj::get(ColKey col_key) const
{
    m_table->check_column(col_key);
    ColumnType type = col_key.get_type();
    REALM_ASSERT(type == ColumnTypeTraits<T>::column_id);

    return _get<T>(col_key.get_index());
}

template <class T>
T Obj::_get(ColKey::Idx col_ndx) const
{
    _update_if_needed();

    typename ColumnTypeTraits<T>::cluster_leaf_type values(_get_alloc());
    ref_type ref = to_ref(Array::get(m_mem.get_addr(), col_ndx.val + 1));
    values.init_from_ref(ref);

    return values.get(m_row_ndx);
}

template <>
ObjKey Obj::_get<ObjKey>(ColKey::Idx col_ndx) const
{
    _update_if_needed();

    ArrayKey values(_get_alloc());
    ref_type ref = to_ref(Array::get(m_mem.get_addr(), col_ndx.val + 1));
    values.init_from_ref(ref);

    ObjKey k = values.get(m_row_ndx);
    return k.is_unresolved() ? ObjKey{} : k;
}

bool Obj::is_unresolved(ColKey col_key) const
{
    m_table->check_column(col_key);
    ColumnType type = col_key.get_type();
    REALM_ASSERT(type == col_type_Link);

    _update_if_needed();

    return get_unfiltered_link(col_key).is_unresolved();
}

ObjKey Obj::get_unfiltered_link(ColKey col_key) const
{
    ArrayKey values(get_alloc());
    ref_type ref = to_ref(Array::get(m_mem.get_addr(), col_key.get_index().val + 1));
    values.init_from_ref(ref);

    return values.get(m_row_ndx);
}

template <>
int64_t Obj::_get<int64_t>(ColKey::Idx col_ndx) const
{
    // manual inline of _update_if_needed():
    auto& alloc = _get_alloc();
    auto current_version = alloc.get_storage_version();
    if (current_version != m_storage_version) {
        update();
    }

    ref_type ref = to_ref(Array::get(m_mem.get_addr(), col_ndx.val + 1));
    char* header = alloc.translate(ref);
    int width = Array::get_width_from_header(header);
    char* data = Array::get_data_from_header(header);
    REALM_TEMPEX(return get_direct, width, (data, m_row_ndx));
}

template <>
int64_t Obj::get<int64_t>(ColKey col_key) const
{
    m_table->check_column(col_key);
    ColumnType type = col_key.get_type();
    REALM_ASSERT(type == col_type_Int);

    if (col_key.get_attrs().test(col_attr_Nullable)) {
        auto val = _get<util::Optional<int64_t>>(col_key.get_index());
        if (!val) {
            throw std::runtime_error("Cannot return null value");
        }
        return *val;
    }
    else {
        return _get<int64_t>(col_key.get_index());
    }
}

template <>
bool Obj::get<bool>(ColKey col_key) const
{
    m_table->check_column(col_key);
    ColumnType type = col_key.get_type();
    REALM_ASSERT(type == col_type_Bool);

    if (col_key.get_attrs().test(col_attr_Nullable)) {
        auto val = _get<util::Optional<bool>>(col_key.get_index());
        if (!val) {
            throw std::runtime_error("Cannot return null value");
        }
        return *val;
    }
    else {
        return _get<bool>(col_key.get_index());
    }
}

template <>
StringData Obj::_get<StringData>(ColKey::Idx col_ndx) const
{
    // manual inline of _update_if_needed():
    auto& alloc = _get_alloc();
    auto current_version = alloc.get_storage_version();
    if (current_version != m_storage_version) {
        update();
    }

    ref_type ref = to_ref(Array::get(m_mem.get_addr(), col_ndx.val + 1));
    auto spec_ndx = m_table->leaf_ndx2spec_ndx(col_ndx);
    auto& spec = get_spec();
    if (spec.is_string_enum_type(spec_ndx)) {
        ArrayString values(get_alloc());
        values.set_spec(const_cast<Spec*>(&spec), spec_ndx);
        values.init_from_ref(ref);

        return values.get(m_row_ndx);
    }
    else {
        return ArrayString::get(alloc.translate(ref), m_row_ndx, alloc);
    }
}

template <>
BinaryData Obj::_get<BinaryData>(ColKey::Idx col_ndx) const
{
    // manual inline of _update_if_needed():
    auto& alloc = _get_alloc();
    auto current_version = alloc.get_storage_version();
    if (current_version != m_storage_version) {
        update();
    }

    ref_type ref = to_ref(Array::get(m_mem.get_addr(), col_ndx.val + 1));
    return ArrayBinary::get(alloc.translate(ref), m_row_ndx, alloc);
}

Mixed Obj::get_any(ColKey col_key) const
{
    m_table->check_column(col_key);
    auto col_ndx = col_key.get_index();
    switch (col_key.get_type()) {
        case col_type_Int:
            if (col_key.get_attrs().test(col_attr_Nullable)) {
                return Mixed{_get<util::Optional<int64_t>>(col_ndx)};
            }
            else {
                return Mixed{_get<int64_t>(col_ndx)};
            }
        case col_type_Bool:
            return Mixed{_get<util::Optional<bool>>(col_ndx)};
        case col_type_Float:
            return Mixed{_get<util::Optional<float>>(col_ndx)};
        case col_type_Double:
            return Mixed{_get<util::Optional<double>>(col_ndx)};
        case col_type_String:
            return Mixed{_get<String>(col_ndx)};
        case col_type_Binary:
            return Mixed{_get<Binary>(col_ndx)};
        case col_type_Mixed:
            return _get<Mixed>(col_ndx);
        case col_type_Timestamp:
            return Mixed{_get<Timestamp>(col_ndx)};
        case col_type_Decimal:
            return Mixed{_get<Decimal128>(col_ndx)};
        case col_type_ObjectId:
            return Mixed{_get<util::Optional<ObjectId>>(col_ndx)};
        case col_type_UUID:
            return Mixed{_get<util::Optional<UUID>>(col_ndx)};
        case col_type_Link:
            return Mixed{_get<ObjKey>(col_ndx)};
        default:
            REALM_UNREACHABLE();
            break;
    }
    return {};
}

Mixed Obj::get_any(std::vector<std::string>::iterator path_start, std::vector<std::string>::iterator path_end) const
{
    if (auto col = get_table()->get_column_key(*path_start)) {
        auto val = get_any(col);
        ++path_start;
        if (path_start == path_end)
            return val;
        if (val.is_type(type_Link, type_TypedLink)) {
            Obj obj;
            if (val.get_type() == type_Link) {
                obj = get_target_table(col)->get_object(val.get<ObjKey>());
            }
            else {
                auto obj_link = val.get<ObjLink>();
                obj = get_target_table(obj_link)->get_object(obj_link.get_obj_key());
            }
            return obj.get_any(path_start, path_end);
        }
    }
    return {};
}

Mixed Obj::get_primary_key() const
{
    auto col = m_table->get_primary_key_column();
    return col ? get_any(col) : Mixed{get_key()};
}

/* FIXME: Make this one fast too!
template <>
ObjKey Obj::_get(size_t col_ndx) const
{
    return ObjKey(_get<int64_t>(col_ndx));
}
*/

Obj Obj::_get_linked_object(ColKey link_col_key, Mixed link) const
{
    Obj obj;
    if (!link.is_null()) {
        TableRef target_table;
        if (link.is_type(type_TypedLink)) {
            target_table = m_table->get_parent_group()->get_table(link.get_link().get_table_key());
        }
        else {
            target_table = get_target_table(link_col_key);
        }
        obj = target_table->get_object(link.get<ObjKey>());
    }
    return obj;
}

template <class T>
inline bool Obj::do_is_null(ColKey::Idx col_ndx) const
{
    T values(get_alloc());
    ref_type ref = to_ref(Array::get(m_mem.get_addr(), col_ndx.val + 1));
    values.init_from_ref(ref);
    return values.is_null(m_row_ndx);
}

template <>
inline bool Obj::do_is_null<ArrayString>(ColKey::Idx col_ndx) const
{
    ArrayString values(get_alloc());
    ref_type ref = to_ref(Array::get(m_mem.get_addr(), col_ndx.val + 1));
    values.set_spec(const_cast<Spec*>(&get_spec()), m_table->leaf_ndx2spec_ndx(col_ndx));
    values.init_from_ref(ref);
    return values.is_null(m_row_ndx);
}

size_t Obj::get_link_count(ColKey col_key) const
{
    return get_list<ObjKey>(col_key).size();
}

bool Obj::is_null(ColKey col_key) const
{
    update_if_needed();
    ColumnAttrMask attr = col_key.get_attrs();
    ColKey::Idx col_ndx = col_key.get_index();
    if (attr.test(col_attr_Nullable) && !attr.test(col_attr_Collection)) {
        switch (col_key.get_type()) {
            case col_type_Int:
                return do_is_null<ArrayIntNull>(col_ndx);
            case col_type_Bool:
                return do_is_null<ArrayBoolNull>(col_ndx);
            case col_type_Float:
                return do_is_null<ArrayFloatNull>(col_ndx);
            case col_type_Double:
                return do_is_null<ArrayDoubleNull>(col_ndx);
            case col_type_String:
                return do_is_null<ArrayString>(col_ndx);
            case col_type_Binary:
                return do_is_null<ArrayBinary>(col_ndx);
            case col_type_Mixed:
                return do_is_null<ArrayMixed>(col_ndx);
            case col_type_Timestamp:
                return do_is_null<ArrayTimestamp>(col_ndx);
            case col_type_Link:
                return do_is_null<ArrayKey>(col_ndx);
            case col_type_ObjectId:
                return do_is_null<ArrayObjectIdNull>(col_ndx);
            case col_type_Decimal:
                return do_is_null<ArrayDecimal128>(col_ndx);
            case col_type_UUID:
                return do_is_null<ArrayUUIDNull>(col_ndx);
            default:
                REALM_UNREACHABLE();
        }
    }
    return false;
}


// Figure out if this object has any remaining backlinkss
bool Obj::has_backlinks(bool only_strong_links) const
{
    const Table& target_table = *m_table;

    // If we only look for strong links and the table is not embedded,
    // then there is no relevant backlinks to find.
    if (only_strong_links && !target_table.is_embedded()) {
        return false;
    }

    return m_table->for_each_backlink_column([&](ColKey backlink_col_key) {
        return get_backlink_cnt(backlink_col_key) != 0;
    });
}

size_t Obj::get_backlink_count() const
{
    update_if_needed();

    size_t cnt = 0;
    m_table->for_each_backlink_column([&](ColKey backlink_col_key) {
        cnt += get_backlink_cnt(backlink_col_key);
        return false;
    });
    return cnt;
}

size_t Obj::get_backlink_count(const Table& origin, ColKey origin_col_key) const
{
    update_if_needed();

    size_t cnt = 0;
    if (TableKey origin_table_key = origin.get_key()) {
        ColKey backlink_col_key;
        auto type = origin_col_key.get_type();
        if (type == col_type_TypedLink || type == col_type_Mixed || origin_col_key.is_dictionary()) {
            backlink_col_key = get_table()->find_backlink_column(origin_col_key, origin_table_key);
        }
        else {
            backlink_col_key = origin.get_opposite_column(origin_col_key);
        }

        cnt = get_backlink_cnt(backlink_col_key);
    }
    return cnt;
}

ObjKey Obj::get_backlink(const Table& origin, ColKey origin_col_key, size_t backlink_ndx) const
{
    ColKey backlink_col_key;
    auto type = origin_col_key.get_type();
    if (type == col_type_TypedLink || type == col_type_Mixed || origin_col_key.is_dictionary()) {
        backlink_col_key = get_table()->find_backlink_column(origin_col_key, origin.get_key());
    }
    else {
        backlink_col_key = origin.get_opposite_column(origin_col_key);
    }
    return get_backlink(backlink_col_key, backlink_ndx);
}

TableView Obj::get_backlink_view(TableRef src_table, ColKey src_col_key)
{
    TableView tv(src_table, src_col_key, *this);
    tv.do_sync();
    return tv;
}

ObjKey Obj::get_backlink(ColKey backlink_col, size_t backlink_ndx) const
{
    get_table()->check_column(backlink_col);
    Allocator& alloc = get_alloc();
    Array fields(alloc);
    fields.init_from_mem(m_mem);

    ArrayBacklink backlinks(alloc);
    backlinks.set_parent(&fields, backlink_col.get_index().val + 1);
    backlinks.init_from_parent();
    return backlinks.get_backlink(m_row_ndx, backlink_ndx);
}

std::vector<ObjKey> Obj::get_all_backlinks(ColKey backlink_col) const
{
    update_if_needed();

    get_table()->check_column(backlink_col);
    Allocator& alloc = get_alloc();
    Array fields(alloc);
    fields.init_from_mem(m_mem);

    ArrayBacklink backlinks(alloc);
    backlinks.set_parent(&fields, backlink_col.get_index().val + 1);
    backlinks.init_from_parent();

    auto cnt = backlinks.get_backlink_count(m_row_ndx);
    std::vector<ObjKey> vec;
    vec.reserve(cnt);
    for (size_t i = 0; i < cnt; i++) {
        vec.push_back(backlinks.get_backlink(m_row_ndx, i));
    }
    return vec;
}

size_t Obj::get_backlink_cnt(ColKey backlink_col) const
{
    Allocator& alloc = get_alloc();
    Array fields(alloc);
    fields.init_from_mem(m_mem);

    ArrayBacklink backlinks(alloc);
    backlinks.set_parent(&fields, backlink_col.get_index().val + 1);
    backlinks.init_from_parent();

    return backlinks.get_backlink_count(m_row_ndx);
}

void Obj::traverse_path(Visitor v, PathSizer ps, size_t path_length) const
{
    if (m_table->is_embedded()) {
        REALM_ASSERT(get_backlink_count() == 1);
        m_table->for_each_backlink_column([&](ColKey col_key) {
            std::vector<ObjKey> backlinks = get_all_backlinks(col_key);
            if (backlinks.size() == 1) {
                TableRef tr = m_table->get_opposite_table(col_key);
                Obj obj = tr->get_object(backlinks[0]); // always the first (and only)
                auto next_col_key = m_table->get_opposite_column(col_key);
                Mixed index;
                if (next_col_key.get_attrs().test(col_attr_List)) {
                    auto ll = obj.get_linklist(next_col_key);
                    auto i = ll.find_first(get_key());
                    REALM_ASSERT(i != realm::npos);
                    index = Mixed(int64_t(i));
                }
                else if (next_col_key.get_attrs().test(col_attr_Dictionary)) {
                    auto dict = obj.get_dictionary(next_col_key);
                    for (auto it : dict) {
                        if (it.second.is_type(type_TypedLink) && it.second.get_link() == get_link()) {
                            index = it.first;
                            break;
                        }
                    }
                    REALM_ASSERT(!index.is_null());
                }
                obj.traverse_path(v, ps, path_length + 1);
                v(obj, next_col_key, index);
                return true; // early out
            }
            return false; // try next column
        });
    }
    else {
        ps(path_length);
    }
}

Obj::FatPath Obj::get_fat_path() const
{
    FatPath result;
    auto sizer = [&](size_t size) {
        result.reserve(size);
    };
    auto step = [&](const Obj& o2, ColKey col, Mixed idx) -> void {
        result.push_back({o2, col, idx});
    };
    traverse_path(step, sizer);
    return result;
}

Obj::Path Obj::get_path() const
{
    Path result;
    bool top_done = false;
    auto sizer = [&](size_t size) {
        result.path_from_top.reserve(size);
    };
    auto step = [&](const Obj& o2, ColKey col, Mixed idx) -> void {
        if (!top_done) {
            top_done = true;
            result.top_table = o2.get_table()->get_key();
            result.top_objkey = o2.get_key();
        }
        result.path_from_top.push_back({col, idx});
    };
    traverse_path(step, sizer);
    return result;
}


namespace {
const char to_be_escaped[] = "\"\n\r\t\f\\\b";
const char encoding[] = "\"nrtf\\b";

template <class T>
inline void out_floats(std::ostream& out, T value)
{
    std::streamsize old = out.precision();
    out.precision(std::numeric_limits<T>::digits10 + 1);
    out << std::scientific << value;
    out.precision(old);
}

void out_string(std::ostream& out, std::string str)
{
    size_t p = str.find_first_of(to_be_escaped);
    while (p != std::string::npos) {
        char c = str[p];
        auto found = strchr(to_be_escaped, c);
        REALM_ASSERT(found);
        out << str.substr(0, p) << '\\' << encoding[found - to_be_escaped];
        str = str.substr(p + 1);
        p = str.find_first_of(to_be_escaped);
    }
    out << str;
}

void out_binary(std::ostream& out, BinaryData bin)
{
    const char* start = bin.data();
    const size_t len = bin.size();
    util::StringBuffer encode_buffer;
    encode_buffer.resize(util::base64_encoded_size(len));
    util::base64_encode(start, len, encode_buffer.data(), encode_buffer.size());
    out << encode_buffer.str();
}

void out_mixed_json(std::ostream& out, const Mixed& val)
{
    if (val.is_null()) {
        out << "null";
        return;
    }
    switch (val.get_type()) {
        case type_Int:
            out << val.get<Int>();
            break;
        case type_Bool:
            out << (val.get<bool>() ? "true" : "false");
            break;
        case type_Float:
            out_floats<float>(out, val.get<float>());
            break;
        case type_Double:
            out_floats<double>(out, val.get<double>());
            break;
        case type_String: {
            out << "\"";
            out_string(out, val.get<String>());
            out << "\"";
            break;
        }
        case type_Binary: {
            out << "\"";
            out_binary(out, val.get<Binary>());
            out << "\"";
            break;
        }
        case type_Timestamp:
            out << "\"";
            out << val.get<Timestamp>();
            out << "\"";
            break;
        case type_Decimal:
            out << "\"";
            out << val.get<Decimal128>();
            out << "\"";
            break;
        case type_ObjectId:
            out << "\"";
            out << val.get<ObjectId>();
            out << "\"";
            break;
        case type_UUID:
            out << "\"";
            out << val.get<UUID>();
            out << "\"";
            break;
        case type_TypedLink:
            out << "\"";
            out << val.get<ObjLink>();
            out << "\"";
            break;
        case type_Link:
        case type_LinkList:
        case type_Mixed:
            break;
    }
}

void out_mixed_xjson(std::ostream& out, const Mixed& val)
{
    if (val.is_null()) {
        out << "null";
        return;
    }
    switch (val.get_type()) {
        case type_Int:
            out << "{\"$numberLong\": \"";
            out << val.get<Int>();
            out << "\"}";
            break;
        case type_Bool:
            out << (val.get<bool>() ? "true" : "false");
            break;
        case type_Float:
            out << "{\"$numberDouble\": \"";
            out_floats<float>(out, val.get<float>());
            out << "\"}";
            break;
        case type_Double:
            out << "{\"$numberDouble\": \"";
            out_floats<double>(out, val.get<double>());
            out << "\"}";
            break;
        case type_String: {
            out << "\"";
            out_string(out, val.get<String>());
            out << "\"";
            break;
        }
        case type_Binary: {
            out << "{\"$binary\": {\"base64\": \"";
            out_binary(out, val.get<Binary>());
            out << "\", \"subType\": \"00\"}}";
            break;
        }
        case type_Timestamp: {
            out << "{\"$date\": {\"$numberLong\": \"";
            auto ts = val.get<Timestamp>();
            int64_t timeMillis = ts.get_seconds() * 1000 + ts.get_nanoseconds() / 1000000;
            out << timeMillis;
            out << "\"}}";
            break;
        }
        case type_Decimal:
            out << "{\"$numberDecimal\": \"";
            out << val.get<Decimal128>();
            out << "\"}";
            break;
        case type_ObjectId:
            out << "{\"$oid\": \"";
            out << val.get<ObjectId>();
            out << "\"}";
            break;
        case type_UUID:
            out << "{\"$binary\": {\"base64\": \"";
            out << val.get<UUID>().to_base64();
            out << "\", \"subType\": \"04\"}}";
            break;

        case type_TypedLink: {
            out_mixed_xjson(out, val.get<ObjLink>().get_obj_key());
            break;
        }
        case type_Link:
        case type_LinkList:
        case type_Mixed:
            break;
    }
}

void out_mixed_xjson_plus(std::ostream& out, const Mixed& val)
{
    if (val.is_null()) {
        out << "null";
        return;
    }

    // Special case for outputing a typedLink, otherwise just us out_mixed_xjson
    if (val.is_type(type_TypedLink)) {
        auto link = val.get<ObjLink>();
        out << "{ \"$link\": { \"table\": \"" << link.get_table_key() << "\", \"key\": ";
        out_mixed_xjson(out, link.get_obj_key());
        out << "}}";
        return;
    }

    out_mixed_xjson(out, val);
}

void out_mixed(std::ostream& out, const Mixed& val, JSONOutputMode output_mode)
{
    switch (output_mode) {
        case output_mode_xjson: {
            out_mixed_xjson(out, val);
            return;
        }
        case output_mode_xjson_plus: {
            out_mixed_xjson_plus(out, val);
            return;
        }
        case output_mode_json: {
            out_mixed_json(out, val);
        }
    }
}

} // anonymous namespace
void Obj::to_json(std::ostream& out, size_t link_depth, const std::map<std::string, std::string>& renames,
                  std::vector<ObjLink>& followed, JSONOutputMode output_mode) const
{
    followed.push_back(get_link());
    size_t new_depth = link_depth == not_found ? not_found : link_depth - 1;
    StringData name = "_key";
    bool prefixComma = false;
    if (renames.count(name))
        name = renames.at(name);
    out << "{";
    if (output_mode == output_mode_json) {
        prefixComma = true;
        out << "\"" << name << "\":" << this->m_key.value;
    }

    auto col_keys = m_table->get_column_keys();
    for (auto ck : col_keys) {
        name = m_table->get_column_name(ck);
        auto type = ck.get_type();
        if (type == col_type_LinkList)
            type = col_type_Link;
        if (renames.count(name))
            name = renames.at(name);

        if (prefixComma)
            out << ",";
        out << "\"" << name << "\":";
        prefixComma = true;

        TableRef target_table;
        std::string open_str;
        std::string close_str;
        ColKey pk_col_key;
        if (type == col_type_Link) {
            target_table = get_target_table(ck);
            pk_col_key = target_table->get_primary_key_column();
            bool is_embedded = target_table->is_embedded();
            bool link_depth_reached = !is_embedded && (link_depth == 0);

            if (output_mode == output_mode_xjson_plus) {
                open_str = std::string("{ ") + (is_embedded ? "\"$embedded" : "\"$link");
                if (ck.is_list())
                    open_str += "List";
                else if (ck.is_set())
                    open_str += "Set";
                else if (ck.is_dictionary())
                    open_str += "Dictionary";
                open_str += "\": ";
                close_str += " }";
            }

            if ((link_depth_reached && output_mode != output_mode_xjson) || output_mode == output_mode_xjson_plus) {
                open_str += "{ \"table\": \"" + std::string(get_target_table(ck)->get_name()) + "\", ";
                open_str += ((is_embedded || ck.is_dictionary()) ? "\"value" : "\"key");
                if (ck.is_collection())
                    open_str += "s";
                open_str += "\": ";
                close_str += "}";
            }
        }
        else {
            if (output_mode == output_mode_xjson_plus) {
                if (ck.is_set()) {
                    open_str = "{ \"$set\": ";
                    close_str = " }";
                }
                else if (ck.is_dictionary()) {
                    open_str = "{ \"$dictionary\": ";
                    close_str = " }";
                }
            }
        }

        auto print_value = [&](Mixed key, Mixed val) {
            if (!key.is_null()) {
                out_mixed(out, key, output_mode);
                out << ":";
            }
            if (val.is_type(type_Link, type_TypedLink)) {
                TableRef tt = target_table;
                auto obj_key = val.get<ObjKey>();
                std::string table_info;
                std::string table_info_close;
                if (!tt) {
                    // It must be a typed link
                    tt = m_table->get_parent_group()->get_table(val.get_link().get_table_key());
                    pk_col_key = tt->get_primary_key_column();
                    if (output_mode == output_mode_xjson_plus) {
                        table_info = std::string("{ \"$link\": ");
                        table_info_close = " }";
                    }

                    table_info += std::string("{ \"table\": \"") + std::string(tt->get_name()) + "\", \"key\": ";
                    table_info_close += " }";
                }
                if (pk_col_key && output_mode != output_mode_json) {
                    out << table_info;
                    out_mixed_xjson(out, tt->get_primary_key(obj_key));
                    out << table_info_close;
                }
                else {
                    ObjLink link(tt->get_key(), obj_key);
                    if (obj_key.is_unresolved()) {
                        out << "null";
                        return;
                    }
                    if (!tt->is_embedded()) {
                        if (link_depth == 0) {
                            out << table_info << obj_key.value << table_info_close;
                            return;
                        }
                        if ((link_depth == realm::npos &&
                             std::find(followed.begin(), followed.end(), link) != followed.end())) {
                            // We have detected a cycle in links
                            out << "{ \"table\": \"" << tt->get_name() << "\", \"key\": " << obj_key.value << " }";
                            return;
                        }
                    }

                    tt->get_object(obj_key).to_json(out, new_depth, renames, followed, output_mode);
                }
            }
            else {
                out_mixed(out, val, output_mode);
            }
        };

        if (ck.is_list() || ck.is_set()) {
            auto list = get_collection_ptr(ck);
            auto sz = list->size();

            out << open_str;
            out << "[";
            for (size_t i = 0; i < sz; i++) {
                if (i > 0)
                    out << ",";
                print_value(Mixed{}, list->get_any(i));
            }
            out << "]";
            out << close_str;
        }
        else if (ck.get_attrs().test(col_attr_Dictionary)) {
            auto dict = get_dictionary(ck);

            out << open_str;
            out << "{";

            bool first = true;
            for (auto it : dict) {
                if (!first)
                    out << ",";
                first = false;
                print_value(it.first, it.second);
            }
            out << "}";
            out << close_str;
        }
        else {
            auto val = get_any(ck);
            if (!val.is_null()) {
                out << open_str;
                print_value(Mixed{}, val);
                out << close_str;
            }
            else {
                out << "null";
            }
        }
    }
    out << "}";
    followed.pop_back();
}

std::string Obj::to_string() const
{
    std::ostringstream ostr;
    to_json(ostr, 0, {});
    return ostr.str();
}

std::ostream& operator<<(std::ostream& ostr, const Obj& obj)
{
    obj.to_json(ostr, -1, {});
    return ostr;
}

/*********************************** Obj *************************************/

bool Obj::ensure_writeable()
{
    Allocator& alloc = get_alloc();
    if (alloc.is_read_only(m_mem.get_ref())) {
        m_mem = const_cast<TableClusterTree*>(get_tree_top())->ensure_writeable(m_key);
        m_storage_version = alloc.get_storage_version();
        return true;
    }
    return false;
}

REALM_FORCEINLINE void Obj::sync(Node& arr)
{
    auto ref = arr.get_ref();
    if (arr.has_missing_parent_update()) {
        const_cast<TableClusterTree*>(get_tree_top())->update_ref_in_parent(m_key, ref);
    }
    if (m_mem.get_ref() != ref) {
        m_mem = arr.get_mem();
        m_storage_version = arr.get_alloc().get_storage_version();
    }
}

template <>
Obj& Obj::set<Mixed>(ColKey col_key, Mixed value, bool is_default)
{
    update_if_needed();
    get_table()->check_column(col_key);
    auto type = col_key.get_type();
    auto attrs = col_key.get_attrs();
    auto col_ndx = col_key.get_index();
    bool recurse = false;
    CascadeState state;

    if (type != col_type_Mixed)
        throw LogicError(LogicError::illegal_type);
    if (value_is_null(value)) {
        if (!attrs.test(col_attr_Nullable)) {
            throw LogicError(LogicError::column_not_nullable);
        }
        return set_null(col_key, is_default);
    }
    if (value.is_type(type_Link)) {
        throw LogicError(LogicError::illegal_combination);
    }

    if (value.is_type(type_TypedLink)) {
        ObjLink new_link = value.template get<ObjLink>();
        Mixed old_value = get<Mixed>(col_key);
        ObjLink old_link;
        if (old_value.is_type(type_TypedLink)) {
            old_link = old_value.get<ObjLink>();
            if (new_link == old_link)
                return *this;
        }
        m_table->get_parent_group()->validate(new_link);
        recurse = replace_backlink(col_key, old_link, new_link, state);
    }

    StringIndex* index = m_table->get_search_index(col_key);
    // The following check on unresolved is just a precaution as it should not
    // be possible to hit that while Mixed is not a supported primary key type.
    if (index && !m_key.is_unresolved()) {
        index->set<Mixed>(m_key, value);
    }

    Allocator& alloc = get_alloc();
    alloc.bump_content_version();
    Array fallback(alloc);
    Array& fields = get_tree_top()->get_fields_accessor(fallback, m_mem);
    REALM_ASSERT(col_ndx.val + 1 < fields.size());
    ArrayMixed values(alloc);
    values.set_parent(&fields, col_ndx.val + 1);
    values.init_from_parent();
    values.set(m_row_ndx, value);

    sync(fields);

    if (Replication* repl = get_replication())
        repl->set(m_table.unchecked_ptr(), col_key, m_key, value,
                  is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws

    if (recurse)
        const_cast<Table*>(m_table.unchecked_ptr())->remove_recursive(state);

    return *this;
}

Obj& Obj::set_any(ColKey col_key, Mixed value, bool is_default)
{
    if (value.is_null()) {
        REALM_ASSERT(col_key.get_attrs().test(col_attr_Nullable));
        set_null(col_key);
    }
    else {
        switch (col_key.get_type()) {
            case col_type_Int:
                if (col_key.get_attrs().test(col_attr_Nullable)) {
                    set(col_key, util::Optional<Int>(value.get_int()), is_default);
                }
                else {
                    set(col_key, value.get_int(), is_default);
                }
                break;
            case col_type_Bool:
                set(col_key, value.get_bool(), is_default);
                break;
            case col_type_Float:
                set(col_key, value.get_float(), is_default);
                break;
            case col_type_Double:
                set(col_key, value.get_double(), is_default);
                break;
            case col_type_String:
                set(col_key, value.get_string(), is_default);
                break;
            case col_type_Binary:
                set(col_key, value.get<Binary>(), is_default);
                break;
            case col_type_Mixed:
                set(col_key, value, is_default);
                break;
            case col_type_Timestamp:
                set(col_key, value.get<Timestamp>(), is_default);
                break;
            case col_type_ObjectId:
                set(col_key, value.get<ObjectId>(), is_default);
                break;
            case col_type_Decimal:
                set(col_key, value.get<Decimal128>(), is_default);
                break;
            case col_type_UUID:
                set(col_key, value.get<UUID>(), is_default);
                break;
            case col_type_Link:
                set(col_key, value.get<ObjKey>(), is_default);
                break;
            case col_type_TypedLink:
                set(col_key, value.get<ObjLink>(), is_default);
                break;
            default:
                break;
        }
    }
    return *this;
}

template <>
Obj& Obj::set<int64_t>(ColKey col_key, int64_t value, bool is_default)
{
    update_if_needed();
    get_table()->check_column(col_key);
    auto col_ndx = col_key.get_index();

    if (col_key.get_type() != ColumnTypeTraits<int64_t>::column_id)
        throw LogicError(LogicError::illegal_type);

    StringIndex* index = m_table->get_search_index(col_key);
    if (index && !m_key.is_unresolved()) {
        index->set<int64_t>(m_key, value);
    }

    Allocator& alloc = get_alloc();
    alloc.bump_content_version();
    Array fallback(alloc);
    Array& fields = get_tree_top()->get_fields_accessor(fallback, m_mem);
    REALM_ASSERT(col_ndx.val + 1 < fields.size());
    auto attr = col_key.get_attrs();
    if (attr.test(col_attr_Nullable)) {
        ArrayIntNull values(alloc);
        values.set_parent(&fields, col_ndx.val + 1);
        values.init_from_parent();
        values.set(m_row_ndx, value);
    }
    else {
        ArrayInteger values(alloc);
        values.set_parent(&fields, col_ndx.val + 1);
        values.init_from_parent();
        values.set(m_row_ndx, value);
    }

    sync(fields);

    if (Replication* repl = get_replication()) {
        repl->set(m_table.unchecked_ptr(), col_key, m_key, value,
                  is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws
    }

    return *this;
}

Obj& Obj::add_int(ColKey col_key, int64_t value)
{
    update_if_needed();
    get_table()->check_column(col_key);
    auto col_ndx = col_key.get_index();

    auto add_wrap = [](int64_t a, int64_t b) -> int64_t {
        uint64_t ua = uint64_t(a);
        uint64_t ub = uint64_t(b);
        return int64_t(ua + ub);
    };

    Allocator& alloc = get_alloc();
    alloc.bump_content_version();
    Array fallback(alloc);
    Array& fields = get_tree_top()->get_fields_accessor(fallback, m_mem);
    REALM_ASSERT(col_ndx.val + 1 < fields.size());

    if (col_key.get_type() == col_type_Mixed) {
        ArrayMixed values(alloc);
        values.set_parent(&fields, col_ndx.val + 1);
        values.init_from_parent();
        Mixed old = values.get(m_row_ndx);
        if (old.is_type(type_Int)) {
            Mixed new_val = Mixed(add_wrap(old.get_int(), value));
            if (StringIndex* index = m_table->get_search_index(col_key)) {
                index->set<Mixed>(m_key, new_val);
            }
            values.set(m_row_ndx, Mixed(new_val));
        }
        else {
            throw LogicError{LogicError::illegal_combination};
        }
    }
    else {
        if (col_key.get_type() != col_type_Int)
            throw LogicError(LogicError::illegal_type);

        auto attr = col_key.get_attrs();
        if (attr.test(col_attr_Nullable)) {
            ArrayIntNull values(alloc);
            values.set_parent(&fields, col_ndx.val + 1);
            values.init_from_parent();
            util::Optional<int64_t> old = values.get(m_row_ndx);
            if (old) {
                auto new_val = add_wrap(*old, value);
                if (StringIndex* index = m_table->get_search_index(col_key)) {
                    index->set<int64_t>(m_key, new_val);
                }
                values.set(m_row_ndx, new_val);
            }
            else {
                throw LogicError{LogicError::illegal_combination};
            }
        }
        else {
            ArrayInteger values(alloc);
            values.set_parent(&fields, col_ndx.val + 1);
            values.init_from_parent();
            int64_t old = values.get(m_row_ndx);
            auto new_val = add_wrap(old, value);
            if (StringIndex* index = m_table->get_search_index(col_key)) {
                index->set<int64_t>(m_key, new_val);
            }
            values.set(m_row_ndx, new_val);
        }
    }

    sync(fields);

    if (Replication* repl = get_replication()) {
        repl->add_int(m_table.unchecked_ptr(), col_key, m_key, value); // Throws
    }

    return *this;
}

template <>
Obj& Obj::set<ObjKey>(ColKey col_key, ObjKey target_key, bool is_default)
{
    update_if_needed();
    get_table()->check_column(col_key);
    ColKey::Idx col_ndx = col_key.get_index();
    ColumnType type = col_key.get_type();
    if (type != ColumnTypeTraits<ObjKey>::column_id)
        throw LogicError(LogicError::illegal_type);
    TableRef target_table = get_target_table(col_key);
    TableKey target_table_key = target_table->get_key();
    if (target_key) {
        ClusterTree* ct = target_key.is_unresolved() ? target_table->m_tombstones.get() : &target_table->m_clusters;
        if (!ct->is_valid(target_key)) {
            throw LogicError(LogicError::target_row_index_out_of_range);
        }
        if (target_table->is_embedded()) {
            throw LogicError(LogicError::wrong_kind_of_table);
        }
    }
    ObjKey old_key = get_unfiltered_link(col_key); // Will update if needed

    if (target_key != old_key) {
        CascadeState state(CascadeState::Mode::Strong);

        bool recurse = replace_backlink(col_key, {target_table_key, old_key}, {target_table_key, target_key}, state);
        _update_if_needed();

        Allocator& alloc = get_alloc();
        alloc.bump_content_version();
        Array fallback(alloc);
        Array& fields = get_tree_top()->get_fields_accessor(fallback, m_mem);
        REALM_ASSERT(col_ndx.val + 1 < fields.size());
        ArrayKey values(alloc);
        values.set_parent(&fields, col_ndx.val + 1);
        values.init_from_parent();

        values.set(m_row_ndx, target_key);

        sync(fields);

        if (Replication* repl = get_replication()) {
            repl->set(m_table.unchecked_ptr(), col_key, m_key, target_key,
                      is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws
        }

        if (recurse)
            target_table->remove_recursive(state);
    }

    return *this;
}

template <>
Obj& Obj::set<ObjLink>(ColKey col_key, ObjLink target_link, bool is_default)
{
    update_if_needed();
    get_table()->check_column(col_key);
    ColKey::Idx col_ndx = col_key.get_index();
    ColumnType type = col_key.get_type();
    if (type != ColumnTypeTraits<ObjLink>::column_id)
        throw LogicError(LogicError::illegal_type);
    m_table->get_parent_group()->validate(target_link);

    ObjLink old_link = get<ObjLink>(col_key); // Will update if needed

    if (target_link != old_link) {
        CascadeState state(old_link.get_obj_key().is_unresolved() ? CascadeState::Mode::All
                                                                  : CascadeState::Mode::Strong);

        bool recurse = replace_backlink(col_key, old_link, target_link, state);
        _update_if_needed();

        Allocator& alloc = get_alloc();
        alloc.bump_content_version();
        Array fallback(alloc);
        Array& fields = get_tree_top()->get_fields_accessor(fallback, m_mem);
        REALM_ASSERT(col_ndx.val + 1 < fields.size());
        ArrayTypedLink values(alloc);
        values.set_parent(&fields, col_ndx.val + 1);
        values.init_from_parent();

        values.set(m_row_ndx, target_link);

        sync(fields);

        if (Replication* repl = get_replication()) {
            repl->set(m_table.unchecked_ptr(), col_key, m_key, target_link,
                      is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws
        }

        if (recurse)
            const_cast<Table*>(m_table.unchecked_ptr())->remove_recursive(state);
    }

    return *this;
}

Obj Obj::create_and_set_linked_object(ColKey col_key, bool is_default)
{
    update_if_needed();
    get_table()->check_column(col_key);
    ColKey::Idx col_ndx = col_key.get_index();
    ColumnType type = col_key.get_type();
    if (type != col_type_Link)
        throw LogicError(LogicError::illegal_type);
    TableRef target_table = get_target_table(col_key);
    Table& t = *target_table;
    TableKey target_table_key = t.get_key();
    auto result = t.is_embedded() ? t.create_linked_object() : t.create_object();
    auto target_key = result.get_key();
    ObjKey old_key = get<ObjKey>(col_key); // Will update if needed
    if (old_key != ObjKey()) {
        if (!t.is_embedded()) {
            throw LogicError(LogicError::wrong_kind_of_table);
        }

        // If this is an embedded object and there was already an embedded object here, then we need to
        // emit an instruction to set the old embedded object to null to clear the old object on other
        // sync clients. Without this, you'll only see the Set ObjectValue instruction, which is idempotent,
        // and then array operations will have a corrupted prior_size.
        if (Replication* repl = get_replication()) {
            repl->set(m_table.unchecked_ptr(), col_key, m_key, util::none,
                      is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws
        }
    }

    if (target_key != old_key) {
        CascadeState state;

        bool recurse = replace_backlink(col_key, {target_table_key, old_key}, {target_table_key, target_key}, state);
        _update_if_needed();

        Allocator& alloc = get_alloc();
        alloc.bump_content_version();
        Array fallback(alloc);
        Array& fields = get_tree_top()->get_fields_accessor(fallback, m_mem);
        REALM_ASSERT(col_ndx.val + 1 < fields.size());
        ArrayKey values(alloc);
        values.set_parent(&fields, col_ndx.val + 1);
        values.init_from_parent();

        values.set(m_row_ndx, target_key);

        sync(fields);

        if (Replication* repl = get_replication()) {
            repl->set(m_table.unchecked_ptr(), col_key, m_key, target_key,
                      is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws
        }

        if (recurse)
            target_table->remove_recursive(state);
    }

    return result;
}

namespace {
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
} // namespace

// helper functions for filtering out calls to set_spec()
template <class T>
inline void Obj::set_spec(T&, ColKey)
{
}
template <>
inline void Obj::set_spec<ArrayString>(ArrayString& values, ColKey col_key)
{
    size_t spec_ndx = m_table->colkey2spec_ndx(col_key);
    Spec* spec = const_cast<Spec*>(&get_spec());
    values.set_spec(spec, spec_ndx);
}

template <class T>
Obj& Obj::set(ColKey col_key, T value, bool is_default)
{
    update_if_needed();
    get_table()->check_column(col_key);
    auto type = col_key.get_type();
    auto attrs = col_key.get_attrs();
    auto col_ndx = col_key.get_index();

    if (type != ColumnTypeTraits<T>::column_id)
        throw LogicError(LogicError::illegal_type);
    if (value_is_null(value) && !attrs.test(col_attr_Nullable))
        throw LogicError(LogicError::column_not_nullable);

    check_range(value);

    StringIndex* index = m_table->get_search_index(col_key);
    if (index && !m_key.is_unresolved()) {
        index->set<T>(m_key, value);
    }

    Allocator& alloc = get_alloc();
    alloc.bump_content_version();
    Array fallback(alloc);
    Array& fields = get_tree_top()->get_fields_accessor(fallback, m_mem);
    REALM_ASSERT(col_ndx.val + 1 < fields.size());
    using LeafType = typename ColumnTypeTraits<T>::cluster_leaf_type;
    LeafType values(alloc);
    values.set_parent(&fields, col_ndx.val + 1);
    set_spec<LeafType>(values, col_key);
    values.init_from_parent();
    values.set(m_row_ndx, value);

    sync(fields);

    if (Replication* repl = get_replication())
        repl->set(m_table.unchecked_ptr(), col_key, m_key, value,
                  is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws

    return *this;
}

#define INSTANTIATE_OBJ_SET(T) template Obj& Obj::set<T>(ColKey, T, bool)
INSTANTIATE_OBJ_SET(bool);
INSTANTIATE_OBJ_SET(StringData);
INSTANTIATE_OBJ_SET(float);
INSTANTIATE_OBJ_SET(double);
INSTANTIATE_OBJ_SET(Decimal128);
INSTANTIATE_OBJ_SET(Timestamp);
INSTANTIATE_OBJ_SET(BinaryData);
INSTANTIATE_OBJ_SET(ObjectId);
INSTANTIATE_OBJ_SET(UUID);

void Obj::set_int(ColKey col_key, int64_t value)
{
    update_if_needed();

    ColKey::Idx col_ndx = col_key.get_index();
    Allocator& alloc = get_alloc();
    alloc.bump_content_version();
    Array fallback(alloc);
    Array& fields = get_tree_top()->get_fields_accessor(fallback, m_mem);
    REALM_ASSERT(col_ndx.val + 1 < fields.size());
    Array values(alloc);
    values.set_parent(&fields, col_ndx.val + 1);
    values.init_from_parent();
    values.set(m_row_ndx, value);

    sync(fields);
}

void Obj::add_backlink(ColKey backlink_col_key, ObjKey origin_key)
{
    ColKey::Idx backlink_col_ndx = backlink_col_key.get_index();
    Allocator& alloc = get_alloc();
    alloc.bump_content_version();
    Array fallback(alloc);
    Array& fields = get_tree_top()->get_fields_accessor(fallback, m_mem);

    ArrayBacklink backlinks(alloc);
    backlinks.set_parent(&fields, backlink_col_ndx.val + 1);
    backlinks.init_from_parent();

    backlinks.add(m_row_ndx, origin_key);

    sync(fields);
}

bool Obj::remove_one_backlink(ColKey backlink_col_key, ObjKey origin_key)
{
    ColKey::Idx backlink_col_ndx = backlink_col_key.get_index();
    Allocator& alloc = get_alloc();
    alloc.bump_content_version();
    Array fallback(alloc);
    Array& fields = get_tree_top()->get_fields_accessor(fallback, m_mem);

    ArrayBacklink backlinks(alloc);
    backlinks.set_parent(&fields, backlink_col_ndx.val + 1);
    backlinks.init_from_parent();

    bool ret = backlinks.remove(m_row_ndx, origin_key);

    sync(fields);

    return ret;
}

namespace {
template <class T>
inline void nullify_linklist(Obj& obj, ColKey origin_col_key, T target)
{
    Lst<T> link_list(obj, origin_col_key);
    size_t ndx = link_list.find_first(target);

    REALM_ASSERT(ndx != realm::npos); // There has to be one

    if (Replication* repl = obj.get_replication()) {
        if constexpr (std::is_same_v<T, ObjKey>) {
            repl->link_list_nullify(link_list, ndx); // Throws
        }
        else {
            repl->list_erase(link_list, ndx); // Throws
        }
    }

    // We cannot just call 'remove' on link_list as it would produce the wrong
    // replication instruction and also attempt an update on the backlinks from
    // the object that we in the process of removing.
    BPlusTree<T>& tree = const_cast<BPlusTree<T>&>(link_list.get_tree());
    tree.erase(ndx);
}
template <class T>
inline void nullify_set(Obj& obj, ColKey origin_col_key, T target)
{
    Set<T> set(obj, origin_col_key);
    size_t ndx = set.find_first(target);

    REALM_ASSERT(ndx != realm::npos); // There has to be one

    if (Replication* repl = obj.get_replication()) {
        repl->set_erase(set, ndx, target); // Throws
    }

    // We cannot just call 'remove' on set as it would produce the wrong
    // replication instruction and also attempt an update on the backlinks from
    // the object that we in the process of removing.
    BPlusTree<T>& tree = const_cast<BPlusTree<T>&>(set.get_tree());
    tree.erase(ndx);
}
} // namespace

void Obj::nullify_link(ColKey origin_col_key, ObjLink target_link)
{
    // ensure_writeable();

    ColKey::Idx origin_col_ndx = origin_col_key.get_index();
    Allocator& alloc = get_alloc();

    ColumnAttrMask attr = origin_col_key.get_attrs();
    if (attr.test(col_attr_List)) {
        if (origin_col_key.get_type() == col_type_LinkList) {
            nullify_linklist(*this, origin_col_key, target_link.get_obj_key());
        }
        else if (origin_col_key.get_type() == col_type_TypedLink) {
            nullify_linklist(*this, origin_col_key, target_link);
        }
        else if (origin_col_key.get_type() == col_type_Mixed) {
            nullify_linklist(*this, origin_col_key, Mixed(target_link));
        }
        else {
            REALM_ASSERT(false);
        }
    }
    else if (attr.test(col_attr_Set)) {
        if (origin_col_key.get_type() == col_type_Link) {
            nullify_set(*this, origin_col_key, target_link.get_obj_key());
        }
        else if (origin_col_key.get_type() == col_type_TypedLink) {
            nullify_set(*this, origin_col_key, target_link);
        }
        else if (origin_col_key.get_type() == col_type_Mixed) {
            nullify_set(*this, origin_col_key, Mixed(target_link));
        }
        else {
            REALM_ASSERT(false);
        }
    }
    else if (attr.test(col_attr_Dictionary)) {
        auto dict = this->get_dictionary(origin_col_key);
        Mixed val{target_link};
        for (auto it : dict) {
            if (it.second == val) {
                dict.nullify(it.first);
            }
        }
    }
    else {
        Array fallback(alloc);
        Array& fields = get_tree_top()->get_fields_accessor(fallback, m_mem);

        if (origin_col_key.get_type() == col_type_Link) {
            ArrayKey links(alloc);
            links.set_parent(&fields, origin_col_ndx.val + 1);
            links.init_from_parent();

            // Ensure we are nullifying correct link
            REALM_ASSERT(links.get(m_row_ndx) == target_link.get_obj_key());

            links.set(m_row_ndx, ObjKey{});
        }
        else if (origin_col_key.get_type() == col_type_TypedLink) {
            ArrayTypedLink links(alloc);
            links.set_parent(&fields, origin_col_ndx.val + 1);
            links.init_from_parent();

            // Ensure we are nullifying correct link
            REALM_ASSERT(links.get(m_row_ndx) == target_link);

            links.set(m_row_ndx, ObjLink{});
        }
        else {
            ArrayMixed mixed(alloc);
            mixed.set_parent(&fields, origin_col_ndx.val + 1);
            mixed.init_from_parent();

            // Ensure we are nullifying correct link
            REALM_ASSERT(mixed.get(m_row_ndx).get<ObjLink>() == target_link);

            mixed.set(m_row_ndx, Mixed{});
        }

        sync(fields);

        if (Replication* repl = get_replication())
            repl->nullify_link(m_table.unchecked_ptr(), origin_col_key, m_key); // Throws
    }
    alloc.bump_content_version();
}

void Obj::set_backlink(ColKey col_key, ObjLink new_link) const
{
    if (new_link && new_link.get_obj_key()) {
        auto target_table = m_table->get_parent_group()->get_table(new_link.get_table_key());
        ColKey backlink_col_key;
        auto type = col_key.get_type();
        if (type == col_type_TypedLink || type == col_type_Mixed || col_key.is_dictionary()) {
            // This may modify the target table
            backlink_col_key = target_table->find_or_add_backlink_column(col_key, get_table_key());
            // it is possible that this was a link to the same table and that adding a backlink column has
            // caused the need to update this object as well.
            update_if_needed();
        }
        else {
            backlink_col_key = m_table->get_opposite_column(col_key);
        }
        auto obj_key = new_link.get_obj_key();
        auto target_obj =
            obj_key.is_unresolved() ? target_table->get_tombstone(obj_key) : target_table->get_object(obj_key);
        target_obj.add_backlink(backlink_col_key, m_key);
    }
}

bool Obj::replace_backlink(ColKey col_key, ObjLink old_link, ObjLink new_link, CascadeState& state) const
{
    bool recurse = remove_backlink(col_key, old_link, state);
    set_backlink(col_key, new_link);

    return recurse;
}

bool Obj::remove_backlink(ColKey col_key, ObjLink old_link, CascadeState& state) const
{
    if (old_link && old_link.get_obj_key()) {
        REALM_ASSERT(m_table->valid_column(col_key));
        ObjKey old_key = old_link.get_obj_key();
        auto target_obj = m_table->get_parent_group()->get_object(old_link);
        TableRef target_table = target_obj.get_table();
        ColKey backlink_col_key;
        auto type = col_key.get_type();
        if (type == col_type_TypedLink || type == col_type_Mixed || col_key.is_dictionary()) {
            backlink_col_key = target_table->find_or_add_backlink_column(col_key, get_table_key());
        }
        else {
            backlink_col_key = m_table->get_opposite_column(col_key);
        }

        bool strong_links = target_table->is_embedded();
        bool is_unres = old_key.is_unresolved();

        bool last_removed = target_obj.remove_one_backlink(backlink_col_key, m_key); // Throws
        if (is_unres) {
            if (last_removed) {
                // Check is there are more backlinks
                if (!target_obj.has_backlinks(false)) {
                    // Tombstones can be erased right away - there is no cascading effect
                    target_table->m_tombstones->erase(old_key, state);
                }
            }
        }
        else {
            return state.enqueue_for_cascade(target_obj, strong_links, last_removed);
        }
    }

    return false;
}

void Obj::assign(const Obj& other)
{
    REALM_ASSERT(get_table() == other.get_table());
    auto cols = m_table->get_column_keys();
    for (auto col : cols) {
        if (col.get_attrs().test(col_attr_List)) {
            auto src_list = other.get_listbase_ptr(col);
            auto dst_list = get_listbase_ptr(col);
            auto sz = src_list->size();
            dst_list->clear();
            for (size_t i = 0; i < sz; i++) {
                Mixed val = src_list->get_any(i);
                dst_list->insert_any(i, val);
            }
        }
        else {
            Mixed val = other.get_any(col);
            if (val.is_null()) {
                this->set_null(col);
                continue;
            }
            switch (val.get_type()) {
                case type_String: {
                    // Need to take copy. Values might be in same cluster
                    std::string str{val.get_string()};
                    this->set(col, str);
                    break;
                }
                case type_Binary: {
                    // Need to take copy. Values might be in same cluster
                    std::string str{val.get_binary()};
                    this->set(col, BinaryData(str));
                    break;
                }
                default:
                    this->set_any(col, val);
                    break;
            }
        }
    }

    auto copy_links = [this, &other](ColKey col) {
        auto t = m_table->get_opposite_table(col);
        auto c = m_table->get_opposite_column(col);
        auto backlinks = other.get_all_backlinks(col);
        for (auto bl : backlinks) {
            auto linking_obj = t->get_object(bl);
            if (c.get_type() == col_type_Link) {
                // Single link
                REALM_ASSERT(!linking_obj.get<ObjKey>(c) || linking_obj.get<ObjKey>(c) == other.get_key());
                linking_obj.set(c, get_key());
            }
            else {
                auto l = linking_obj.get_linklist(c);
                auto n = l.find_first(other.get_key());
                REALM_ASSERT(n != realm::npos);
                l.set(n, get_key());
            }
        }
        return false;
    };
    m_table->for_each_backlink_column(copy_links);
}

Dictionary Obj::get_dictionary(ColKey col_key) const
{
    REALM_ASSERT(col_key.is_dictionary());
    update_if_needed();
    return Dictionary(Obj(*this), col_key);
}

DictionaryPtr Obj::get_dictionary_ptr(ColKey col_key) const
{
    return std::make_unique<Dictionary>(Obj(*this), col_key);
}

Dictionary Obj::get_dictionary(StringData col_name) const
{
    return get_dictionary(get_column_key(col_name));
}

CollectionBasePtr Obj::get_collection_ptr(ColKey col_key) const
{
    if (col_key.is_list()) {
        return get_listbase_ptr(col_key);
    }
    else if (col_key.is_set()) {
        return get_setbase_ptr(col_key);
    }
    else if (col_key.is_dictionary()) {
        return get_dictionary_ptr(col_key);
    }
    return {};
}

CollectionBasePtr Obj::get_collection_ptr(StringData col_name) const
{
    return get_collection_ptr(get_column_key(col_name));
}

LinkCollectionPtr Obj::get_linkcollection_ptr(ColKey col_key) const
{
    if (col_key.is_list()) {
        return get_linklist_ptr(col_key);
    }
    else if (col_key.is_set()) {
        return get_linkset_ptr(col_key);
    }
    else if (col_key.is_dictionary()) {
        auto dict = get_dictionary(col_key);
        return std::make_unique<DictionaryLinkValues>(dict);
    }
    return {};
}

void Obj::assign_pk_and_backlinks(const Obj& other)
{
    REALM_ASSERT(get_table() == other.get_table());
    if (auto col_pk = m_table->get_primary_key_column()) {
        Mixed val = other.get_any(col_pk);
        this->set_any(col_pk, val);
    }
    auto nb_tombstones = m_table->m_tombstones->size();

    auto copy_links = [this, &other, nb_tombstones](ColKey col) {
        if (nb_tombstones != m_table->m_tombstones->size()) {
            // Object has been deleted - we are done
            return true;
        }
        auto t = m_table->get_opposite_table(col);
        auto c = m_table->get_opposite_column(col);
        auto backlinks = other.get_all_backlinks(col);
        for (auto bl : backlinks) {
            auto linking_obj = t->get_object(bl);
            if (c.is_dictionary()) {
                auto dict = linking_obj.get_dictionary(c);
                Mixed val(other.get_link());
                for (auto it : dict) {
                    if (it.second == val) {
                        auto link = get_link();
                        dict.insert(it.first, link);
                    }
                }
            }
            else if (c.is_set()) {
                if (c.get_type() == col_type_Link) {
                    auto set = linking_obj.get_set<ObjKey>(c);
                    set.erase(other.get_key());
                    set.insert(get_key());
                }
                else if (c.get_type() == col_type_TypedLink) {
                    auto set = linking_obj.get_set<ObjLink>(c);
                    set.erase({m_table->get_key(), other.get_key()});
                    set.insert({m_table->get_key(), get_key()});
                }
                if (c.get_type() == col_type_Mixed) {
                    auto set = linking_obj.get_set<Mixed>(c);
                    set.erase(ObjLink{m_table->get_key(), other.get_key()});
                    set.insert(ObjLink{m_table->get_key(), get_key()});
                }
            }
            else if (c.is_list()) {
                if (c.get_type() == col_type_Mixed) {
                    auto l = linking_obj.get_list<Mixed>(c);
                    auto n = l.find_first(ObjLink{m_table->get_key(), other.get_key()});
                    REALM_ASSERT(n != realm::npos);
                    l.set(n, ObjLink{m_table->get_key(), get_key()});
                }
                else if (c.get_type() == col_type_LinkList) {
                    // Link list
                    auto l = linking_obj.get_list<ObjKey>(c);
                    auto n = l.find_first(other.get_key());
                    REALM_ASSERT(n != realm::npos);
                    l.set(n, get_key());
                }
                else {
                    REALM_UNREACHABLE(); // missing type handling
                }
            }
            else {
                REALM_ASSERT(!c.is_collection());
                if (c.get_type() == col_type_Link) {
                    // Single link
                    REALM_ASSERT(!linking_obj.get<ObjKey>(c) || linking_obj.get<ObjKey>(c) == other.get_key());
                    linking_obj.set(c, get_key());
                }
                else if (c.get_type() == col_type_Mixed) {
                    // Mixed link
                    REALM_ASSERT(linking_obj.get_any(c).is_null() ||
                                 linking_obj.get_any(c).get_link().get_obj_key() == other.get_key());
                    linking_obj.set(c, Mixed{ObjLink{m_table->get_key(), get_key()}});
                }
                else {
                    REALM_UNREACHABLE(); // missing type handling
                }
            }
        }
        return false;
    };
    m_table->for_each_backlink_column(copy_links);
}

template util::Optional<int64_t> Obj::get<util::Optional<int64_t>>(ColKey col_key) const;
template util::Optional<Bool> Obj::get<util::Optional<Bool>>(ColKey col_key) const;
template float Obj::get<float>(ColKey col_key) const;
template util::Optional<float> Obj::get<util::Optional<float>>(ColKey col_key) const;
template double Obj::get<double>(ColKey col_key) const;
template util::Optional<double> Obj::get<util::Optional<double>>(ColKey col_key) const;
template StringData Obj::get<StringData>(ColKey col_key) const;
template BinaryData Obj::get<BinaryData>(ColKey col_key) const;
template Timestamp Obj::get<Timestamp>(ColKey col_key) const;
template ObjectId Obj::get<ObjectId>(ColKey col_key) const;
template util::Optional<ObjectId> Obj::get<util::Optional<ObjectId>>(ColKey col_key) const;
template ObjKey Obj::get<ObjKey>(ColKey col_key) const;
template Decimal128 Obj::get<Decimal128>(ColKey col_key) const;
template ObjLink Obj::get<ObjLink>(ColKey col_key) const;
template Mixed Obj::get<Mixed>(realm::ColKey) const;
template UUID Obj::get<UUID>(realm::ColKey) const;
template util::Optional<UUID> Obj::get<util::Optional<UUID>>(ColKey col_key) const;

template <class T>
inline void Obj::do_set_null(ColKey col_key)
{
    ColKey::Idx col_ndx = col_key.get_index();
    Allocator& alloc = get_alloc();
    alloc.bump_content_version();
    Array fallback(alloc);
    Array& fields = get_tree_top()->get_fields_accessor(fallback, m_mem);

    T values(alloc);
    values.set_parent(&fields, col_ndx.val + 1);
    values.init_from_parent();
    values.set_null(m_row_ndx);

    sync(fields);
}

template <>
inline void Obj::do_set_null<ArrayString>(ColKey col_key)
{
    ColKey::Idx col_ndx = col_key.get_index();
    size_t spec_ndx = m_table->leaf_ndx2spec_ndx(col_ndx);
    Allocator& alloc = get_alloc();
    alloc.bump_content_version();
    Array fallback(alloc);
    Array& fields = get_tree_top()->get_fields_accessor(fallback, m_mem);

    ArrayString values(alloc);
    values.set_parent(&fields, col_ndx.val + 1);
    values.set_spec(const_cast<Spec*>(&get_spec()), spec_ndx);
    values.init_from_parent();
    values.set_null(m_row_ndx);

    sync(fields);
}

Obj& Obj::set_null(ColKey col_key, bool is_default)
{
    ColumnType col_type = col_key.get_type();
    // Links need special handling
    if (col_type == col_type_Link) {
        set(col_key, null_key);
    }
    else {
        auto attrs = col_key.get_attrs();
        if (REALM_UNLIKELY(!attrs.test(col_attr_Nullable))) {
            throw LogicError(LogicError::column_not_nullable);
        }

        update_if_needed();

        StringIndex* index = m_table->get_search_index(col_key);
        if (index && !m_key.is_unresolved()) {
            index->set(m_key, null{});
        }

        switch (col_type) {
            case col_type_Int:
                do_set_null<ArrayIntNull>(col_key);
                break;
            case col_type_Bool:
                do_set_null<ArrayBoolNull>(col_key);
                break;
            case col_type_Float:
                do_set_null<ArrayFloatNull>(col_key);
                break;
            case col_type_Double:
                do_set_null<ArrayDoubleNull>(col_key);
                break;
            case col_type_ObjectId:
                do_set_null<ArrayObjectIdNull>(col_key);
                break;
            case col_type_String:
                do_set_null<ArrayString>(col_key);
                break;
            case col_type_Binary:
                do_set_null<ArrayBinary>(col_key);
                break;
            case col_type_Timestamp:
                do_set_null<ArrayTimestamp>(col_key);
                break;
            case col_type_Decimal:
                do_set_null<ArrayDecimal128>(col_key);
                break;
            case col_type_Mixed:
                do_set_null<ArrayMixed>(col_key);
                break;
            case col_type_UUID:
                do_set_null<ArrayUUIDNull>(col_key);
                break;
            default:
                REALM_UNREACHABLE();
        }
    }

    if (Replication* repl = get_replication())
        repl->set(m_table.unchecked_ptr(), col_key, m_key, util::none,
                  is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws

    return *this;
}


ColKey Obj::spec_ndx2colkey(size_t col_ndx)
{
    return get_table()->spec_ndx2colkey(col_ndx);
}

size_t Obj::colkey2spec_ndx(ColKey key)
{
    return get_table()->colkey2spec_ndx(key);
}

ColKey Obj::get_primary_key_column() const
{
    return m_table->get_primary_key_column();
}

ref_type Obj::Internal::get_ref(const Obj& obj, ColKey col_key)
{
    return to_ref(obj._get<int64_t>(col_key.get_index()));
}

} // namespace realm
