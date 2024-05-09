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
#include "realm/cluster_tree.hpp"
#include "realm/list.hpp"
#include "realm/set.hpp"
#include "realm/dictionary.hpp"
#if REALM_ENABLE_GEOSPATIAL
#include "realm/geospatial.hpp"
#endif
#include "realm/link_translator.hpp"
#include "realm/index_string.hpp"
#include "realm/object_converter.hpp"
#include "realm/replication.hpp"
#include "realm/spec.hpp"
#include "realm/table_view.hpp"
#include "realm/util/base64.hpp"
#include "realm/util/overload.hpp"

#include <ostream>

namespace realm {
namespace {

template <class T, class U>
size_t find_link_value_in_collection(T& coll, Obj& obj, ColKey origin_col_key, U link)
{
    coll.set_owner(obj, origin_col_key);
    return coll.find_first(link);
}

template <class T>
inline void nullify_linklist(Obj& obj, ColKey origin_col_key, T target)
{
    Lst<T> link_list(origin_col_key);
    size_t ndx = find_link_value_in_collection(link_list, obj, origin_col_key, target);

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
    Set<T> link_set(origin_col_key);
    size_t ndx = find_link_value_in_collection(link_set, obj, origin_col_key, target);

    REALM_ASSERT(ndx != realm::npos); // There has to be one

    if (Replication* repl = obj.get_replication()) {
        repl->set_erase(link_set, ndx, target); // Throws
    }

    // We cannot just call 'remove' on set as it would produce the wrong
    // replication instruction and also attempt an update on the backlinks from
    // the object that we in the process of removing.
    BPlusTree<T>& tree = const_cast<BPlusTree<T>&>(link_set.get_tree());
    tree.erase(ndx);
}

} // namespace

/*********************************** Obj *************************************/

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

const ClusterTree* Obj::get_tree_top() const
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

StableIndex Obj::build_index(ColKey col_key) const
{
    if (col_key.is_collection()) {
        return {col_key, 0};
    }
    REALM_ASSERT(col_key.get_type() == col_type_Mixed);
    _update_if_needed();
    ArrayMixed values(_get_alloc());
    ref_type ref = to_ref(Array::get(m_mem.get_addr(), col_key.get_index().val + 1));
    values.init_from_ref(ref);
    auto key = values.get_key(m_row_ndx);
    return {col_key, key};
}

bool Obj::check_index(StableIndex index) const
{
    if (index.is_collection()) {
        return true;
    }
    _update_if_needed();
    ArrayMixed values(_get_alloc());
    ref_type ref = to_ref(Array::get(m_mem.get_addr(), index.get_index().val + 1));
    values.init_from_ref(ref);
    auto key = values.get_key(m_row_ndx);
    return key == index.get_salt();
}

Replication* Obj::get_replication() const
{
    return m_table->get_repl();
}

bool Obj::compare_values(Mixed val1, Mixed val2, ColKey ck, Obj other, StringData col_name) const
{
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
            const auto type = val1.get_type();
            if (type == type_List) {
                Lst<Mixed> lst1(*this, ck);
                Lst<Mixed> lst2(other, other.get_column_key(col_name));
                return compare_list_in_mixed(lst1, lst2, ck, other, col_name);
            }
            else if (type == type_Set) {
                Set<Mixed> set1(*this, ck);
                Set<Mixed> set2(other, other.get_column_key(col_name));
                return set1 == set2;
            }
            else if (type == type_Dictionary) {
                Dictionary dict1(*this, ck);
                Dictionary dict2(other, other.get_column_key(col_name));
                return compare_dict_in_mixed(dict1, dict2, ck, other, col_name);
            }
            return val1 == val2;
        }
    }
    return true;
}

bool Obj::compare_list_in_mixed(Lst<Mixed>& val1, Lst<Mixed>& val2, ColKey ck, Obj other, StringData col_name) const
{
    if (val1.size() != val2.size())
        return false;

    for (size_t i = 0; i < val1.size(); ++i) {

        auto m1 = val1.get_any(i);
        auto m2 = val2.get_any(i);

        if (m1.is_type(type_List) && m2.is_type(type_List)) {
            DummyParent parent(other.get_table(), m2.get_ref());
            Lst<Mixed> list(parent, 0);
            return compare_list_in_mixed(*val1.get_list(i), list, ck, other, col_name);
        }
        else if (m1.is_type(type_Dictionary) && m2.is_type(type_Dictionary)) {
            DummyParent parent(other.get_table(), m2.get_ref());
            Dictionary dict(parent, 0);
            return compare_dict_in_mixed(*val1.get_dictionary(i), dict, ck, other, col_name);
        }
        else if (!compare_values(m1, m2, ck, other, col_name)) {
            return false;
        }
    }
    return true;
}

bool Obj::compare_dict_in_mixed(Dictionary& val1, Dictionary& val2, ColKey ck, Obj other, StringData col_name) const
{
    if (val1.size() != val2.size())
        return false;

    for (size_t i = 0; i < val1.size(); ++i) {

        auto [k1, m1] = val1.get_pair(i);
        auto [k2, m2] = val2.get_pair(i);
        if (k1 != k2)
            return false;

        if (m1.is_type(type_List) && m2.is_type(type_List)) {
            DummyParent parent(other.get_table(), m2.get_ref());
            Lst<Mixed> list(parent, 0);
            return compare_list_in_mixed(*val1.get_list(k1.get_string()), list, ck, other, col_name);
        }
        else if (m1.is_type(type_Dictionary) && m2.is_type(type_Dictionary)) {
            DummyParent parent(other.get_table(), m2.get_ref());
            Dictionary dict(parent, 0);
            return compare_dict_in_mixed(*val1.get_dictionary(k1.get_string()), dict, ck, other, col_name);
        }
        else if (!compare_values(m1, m2, ck, other, col_name)) {
            return false;
        }
    }
    return true;
}

bool Obj::operator==(const Obj& other) const
{
    for (auto ck : m_table->get_column_keys()) {
        StringData col_name = m_table->get_column_name(ck);
        auto compare = [&](Mixed m1, Mixed m2) {
            return compare_values(m1, m2, ck, other, col_name);
        };

        if (!ck.is_collection()) {
            if (!compare(get_any(ck), other.get_any(col_name)))
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
                    if (!compare(coll1->get_any(i), coll2->get_any(i)))
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
                    if (!compare(value, *val2))
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
        ++m_version_counter;
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

UpdateStatus Obj::update_if_needed() const
{
    if (!m_table) {
        // Table deleted
        return UpdateStatus::Detached;
    }

    auto current_version = _get_alloc().get_storage_version();
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
            ++m_version_counter;
            return UpdateStatus::Updated;
        }
    }
    return UpdateStatus::NoChange;
}

void Obj::checked_update_if_needed() const
{
    if (update_if_needed() == UpdateStatus::Detached) {
        m_table.check();
        get_tree_top()->get(m_key); // should always throw
    }
}

template <class T>
T Obj::get(ColKey col_key) const
{
    m_table->check_column(col_key);
    ColumnType type = col_key.get_type();
    REALM_ASSERT(type == ColumnTypeTraits<T>::column_id);

    return _get<T>(col_key.get_index());
}

template UUID Obj::_get(ColKey::Idx col_ndx) const;
template util::Optional<UUID> Obj::_get(ColKey::Idx col_ndx) const;

#if REALM_ENABLE_GEOSPATIAL

template <>
Geospatial Obj::get(ColKey col_key) const
{
    m_table->check_column(col_key);
    ColumnType type = col_key.get_type();
    REALM_ASSERT(type == ColumnTypeTraits<Link>::column_id);
    return Geospatial::from_link(get_linked_object(col_key));
}

template <>
std::optional<Geospatial> Obj::get(ColKey col_key) const
{
    m_table->check_column(col_key);
    ColumnType type = col_key.get_type();
    REALM_ASSERT(type == ColumnTypeTraits<Link>::column_id);

    auto geo = get_linked_object(col_key);
    if (!geo) {
        return {};
    }
    return Geospatial::from_link(geo);
}

#endif

template <class T>
T Obj::_get(ColKey::Idx col_ndx) const
{
    _update_if_needed();

    typename ColumnTypeTraits<T>::cluster_leaf_type values(_get_alloc());
    ref_type ref = to_ref(Array::get(m_mem.get_addr(), col_ndx.val + 1));
    values.init_from_ref(ref);

    return values.get(m_row_ndx);
}

Mixed Obj::get_unfiltered_mixed(ColKey::Idx col_ndx) const
{
    ArrayMixed values(get_alloc());
    ref_type ref = to_ref(Array::get(m_mem.get_addr(), col_ndx.val + 1));
    values.init_from_ref(ref);

    return values.get(m_row_ndx);
}

template <>
Mixed Obj::_get<Mixed>(ColKey::Idx col_ndx) const
{
    _update_if_needed();
    Mixed m = get_unfiltered_mixed(col_ndx);
    return m.is_unresolved_link() ? Mixed{} : m;
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
            throw IllegalOperation("Obj::get<int64_t> cannot return null");
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
            throw IllegalOperation("Obj::get<int64_t> cannot return null");
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
    if (col_key.is_collection()) {
        ref_type ref = to_ref(_get<int64_t>(col_ndx));
        return Mixed(ref, get_table()->get_collection_type(col_key));
    }
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

Obj Obj::get_parent_object() const
{
    Obj obj;
    checked_update_if_needed();

    if (!m_table->is_embedded()) {
        throw LogicError(ErrorCodes::TopLevelObject, "Object is not embedded");
    }
    m_table->for_each_backlink_column([&](ColKey backlink_col_key) {
        if (get_backlink_cnt(backlink_col_key) == 1) {
            auto obj_key = get_backlink(backlink_col_key, 0);
            obj = m_table->get_opposite_table(backlink_col_key)->get_object(obj_key);
            return IteratorControl::Stop;
        }
        return IteratorControl::AdvanceToNext;
    });

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
    checked_update_if_needed();
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
        return get_backlink_cnt(backlink_col_key) != 0 ? IteratorControl::Stop : IteratorControl::AdvanceToNext;
    });
}

size_t Obj::get_backlink_count() const
{
    checked_update_if_needed();

    size_t cnt = 0;
    m_table->for_each_backlink_column([&](ColKey backlink_col_key) {
        cnt += get_backlink_cnt(backlink_col_key);
        return IteratorControl::AdvanceToNext;
    });
    return cnt;
}

size_t Obj::get_backlink_count(const Table& origin, ColKey origin_col_key) const
{
    checked_update_if_needed();

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

TableView Obj::get_backlink_view(TableRef src_table, ColKey src_col_key) const
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
    checked_update_if_needed();

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

void Obj::verify_backlink(const Table& origin, ColKey origin_col_key, ObjKey origin_key) const
{
#ifdef REALM_DEBUG
    ColKey backlink_col_key;
    auto type = origin_col_key.get_type();
    if (type == col_type_TypedLink || type == col_type_Mixed || origin_col_key.is_dictionary()) {
        backlink_col_key = get_table()->find_backlink_column(origin_col_key, origin.get_key());
    }
    else {
        backlink_col_key = origin.get_opposite_column(origin_col_key);
    }

    Allocator& alloc = get_alloc();
    Array fields(alloc);
    fields.init_from_mem(m_mem);

    ArrayBacklink backlinks(alloc);
    backlinks.set_parent(&fields, backlink_col_key.get_index().val + 1);
    backlinks.init_from_parent();

    REALM_ASSERT(backlinks.verify_backlink(m_row_ndx, origin_key.value));
#else
    static_cast<void>(origin);
    static_cast<void>(origin_col_key);
    static_cast<void>(origin_key);
#endif
}

void Obj::traverse_path(Visitor v, PathSizer ps, size_t path_length) const
{
    struct BacklinkTraverser : public LinkTranslator {
        BacklinkTraverser(Obj origin, ColKey origin_col_key, Obj dest)
            : LinkTranslator(origin, origin_col_key)
            , m_dest_obj(dest)
        {
        }
        void on_list_of_links(LnkLst& ll) final
        {
            auto i = ll.find_first(m_dest_obj.get_key());
            REALM_ASSERT(i != realm::npos);
            m_index = Mixed(int64_t(i));
        }
        void on_dictionary(Dictionary& dict) final
        {
            for (auto it : dict) {
                if (it.second.is_type(type_TypedLink) && it.second.get_link() == m_dest_obj.get_link()) {
                    m_index = it.first;
                    break;
                }
            }
            REALM_ASSERT(!m_index.is_null());
        }
        void on_list_of_mixed(Lst<Mixed>&) final
        {
            REALM_UNREACHABLE(); // we don't support Mixed link to embedded object yet
        }
        void on_set_of_links(LnkSet&) final
        {
            REALM_UNREACHABLE(); // sets of embedded objects are not allowed at the schema level
        }
        void on_set_of_mixed(Set<Mixed>&) final
        {
            REALM_UNREACHABLE(); // we don't support Mixed link to embedded object yet
        }
        void on_link_property(ColKey) final {}
        void on_mixed_property(ColKey) final {}
        Mixed result()
        {
            return m_index;
        }

    private:
        Mixed m_index;
        Obj m_dest_obj;
    };

    if (m_table->is_embedded()) {
        REALM_ASSERT(get_backlink_count() == 1);
        m_table->for_each_backlink_column([&](ColKey col_key) {
            std::vector<ObjKey> backlinks = get_all_backlinks(col_key);
            if (backlinks.size() == 1) {
                TableRef tr = m_table->get_opposite_table(col_key);
                Obj obj = tr->get_object(backlinks[0]); // always the first (and only)
                auto next_col_key = m_table->get_opposite_column(col_key);
                BacklinkTraverser traverser{obj, next_col_key, *this};
                traverser.run();
                Mixed index = traverser.result();
                obj.traverse_path(v, ps, path_length + 1);
                v(obj, next_col_key, index);
                return IteratorControl::Stop; // early out
            }
            return IteratorControl::AdvanceToNext; // try next column
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

FullPath Obj::get_path() const
{
    FullPath result;
    if (m_table->is_embedded()) {
        REALM_ASSERT(get_backlink_count() == 1);
        m_table->for_each_backlink_column([&](ColKey col_key) {
            std::vector<ObjKey> backlinks = get_all_backlinks(col_key);
            if (backlinks.size() == 1) {
                TableRef origin_table = m_table->get_opposite_table(col_key);
                Obj obj = origin_table->get_object(backlinks[0]); // always the first (and only)
                auto next_col_key = m_table->get_opposite_column(col_key);

                ColumnAttrMask attr = next_col_key.get_attrs();
                Mixed index;
                if (attr.test(col_attr_List)) {
                    REALM_ASSERT(next_col_key.get_type() == col_type_Link);
                    Lst<ObjKey> link_list(next_col_key);
                    size_t i = find_link_value_in_collection(link_list, obj, next_col_key, get_key());
                    REALM_ASSERT(i != realm::not_found);
                    result = link_list.get_path();
                    result.path_from_top.emplace_back(i);
                }
                else if (attr.test(col_attr_Dictionary)) {
                    Dictionary dict(next_col_key);
                    size_t ndx = find_link_value_in_collection(dict, obj, next_col_key, get_link());
                    REALM_ASSERT(ndx != realm::not_found);
                    result = dict.get_path();
                    result.path_from_top.push_back(dict.get_key(ndx).get_string());
                }
                else {
                    result = obj.get_path();
                    if (result.path_from_top.empty()) {
                        result.path_from_top.push_back(next_col_key);
                    }
                    else {
                        result.path_from_top.push_back(obj.get_table()->get_column_name(next_col_key));
                    }
                }

                return IteratorControl::Stop; // early out
            }
            return IteratorControl::AdvanceToNext; // try next column
        });
    }
    else {
        result.top_objkey = get_key();
        result.top_table = get_table()->get_key();
    }
    return result;
}

std::string Obj::get_id() const
{
    std::ostringstream ostr;
    auto path = get_path();
    auto top_table = m_table->get_parent_group()->get_table(path.top_table);
    ostr << top_table->get_class_name() << '[';
    if (top_table->get_primary_key_column()) {
        ostr << top_table->get_primary_key(path.top_objkey);
    }
    else {
        ostr << path.top_objkey;
    }
    ostr << ']';
    if (!path.path_from_top.empty()) {
        auto prop_name = top_table->get_column_name(path.path_from_top[0].get_col_key());
        path.path_from_top[0] = PathElement(prop_name);
        ostr << path.path_from_top;
    }
    return ostr.str();
}

Path Obj::get_short_path() const noexcept
{
    return {};
}

ColKey Obj::get_col_key() const noexcept
{
    return {};
}

StablePath Obj::get_stable_path() const noexcept
{
    return {};
}

void Obj::add_index(Path& path, const CollectionParent::Index& index) const
{
    if (path.empty()) {
        path.emplace_back(get_table()->get_column_key(index));
    }
    else {
        StringData col_name = get_table()->get_column_name(index);
        path.emplace_back(col_name);
    }
}

std::string Obj::to_string() const
{
    std::ostringstream ostr;
    to_json(ostr);
    return ostr.str();
}

std::ostream& operator<<(std::ostream& ostr, const Obj& obj)
{
    obj.to_json(ostr);
    return ostr;
}

/*********************************** Obj *************************************/

bool Obj::ensure_writeable()
{
    Allocator& alloc = get_alloc();
    if (alloc.is_read_only(m_mem.get_ref())) {
        m_mem = const_cast<ClusterTree*>(get_tree_top())->ensure_writeable(m_key);
        m_storage_version = alloc.get_storage_version();
        return true;
    }
    return false;
}

REALM_FORCEINLINE void Obj::sync(Node& arr)
{
    auto ref = arr.get_ref();
    if (arr.has_missing_parent_update()) {
        const_cast<ClusterTree*>(get_tree_top())->update_ref_in_parent(m_key, ref);
    }
    if (m_mem.get_ref() != ref) {
        m_mem = arr.get_mem();
        m_storage_version = arr.get_alloc().get_storage_version();
    }
}

template <>
Obj& Obj::set<Mixed>(ColKey col_key, Mixed value, bool is_default)
{
    checked_update_if_needed();
    get_table()->check_column(col_key);
    auto type = col_key.get_type();
    auto col_ndx = col_key.get_index();
    bool recurse = false;
    CascadeState state;

    if (type != col_type_Mixed)
        throw InvalidArgument(ErrorCodes::TypeMismatch, "Property not a Mixed");
    if (value_is_null(value) && !col_key.is_nullable()) {
        throw NotNullable(Group::table_name_to_class_name(m_table->get_name()), m_table->get_column_name(col_key));
    }
    if (value.is_type(type_Link)) {
        throw InvalidArgument(ErrorCodes::TypeMismatch, "Link must be fully qualified");
    }

    Mixed old_value = get_unfiltered_mixed(col_ndx);
    if (!value.is_same_type(old_value) || value != old_value) {
        if (old_value.is_type(type_TypedLink)) {
            auto old_link = old_value.get<ObjLink>();
            recurse = remove_backlink(col_key, old_link, state);
        }
        else if (old_value.is_type(type_Dictionary)) {
            Dictionary dict(*this, col_key);
            recurse = dict.remove_backlinks(state);
        }
        else if (old_value.is_type(type_List)) {
            Lst<Mixed> list(*this, col_key);
            recurse = list.remove_backlinks(state);
        }

        if (value.is_type(type_TypedLink)) {
            if (m_table->is_asymmetric()) {
                throw IllegalOperation("Links not allowed in asymmetric tables");
            }
            auto new_link = value.get<ObjLink>();
            m_table->get_parent_group()->validate(new_link);
            set_backlink(col_key, new_link);
        }

        SearchIndex* index = m_table->get_search_index(col_key);
        // The following check on unresolved is just a precaution as it should not
        // be possible to hit that while Mixed is not a supported primary key type.
        if (index && !m_key.is_unresolved()) {
            index->set(m_key, value.is_unresolved_link() ? Mixed() : value);
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
        if (value.is_type(type_Dictionary, type_List)) {
            values.set_key(m_row_ndx, CollectionParent::generate_key(0x10));
        }

        sync(fields);
    }

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
        set_null(col_key, is_default);
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
    checked_update_if_needed();
    get_table()->check_column(col_key);
    auto col_ndx = col_key.get_index();

    if (col_key.get_type() != ColumnTypeTraits<int64_t>::column_id)
        throw InvalidArgument(ErrorCodes::TypeMismatch,
                              util::format("Property not a %1", ColumnTypeTraits<int64_t>::column_id));

    SearchIndex* index = m_table->get_search_index(col_key);
    if (index && !m_key.is_unresolved()) {
        index->set(m_key, value);
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
    checked_update_if_needed();
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
            if (SearchIndex* index = m_table->get_search_index(col_key)) {
                index->set(m_key, new_val);
            }
            values.set(m_row_ndx, Mixed(new_val));
        }
        else {
            throw IllegalOperation("Value not an int");
        }
    }
    else {
        if (col_key.get_type() != col_type_Int)
            throw IllegalOperation("Property not an int");

        auto attr = col_key.get_attrs();
        if (attr.test(col_attr_Nullable)) {
            ArrayIntNull values(alloc);
            values.set_parent(&fields, col_ndx.val + 1);
            values.init_from_parent();
            util::Optional<int64_t> old = values.get(m_row_ndx);
            if (old) {
                auto new_val = add_wrap(*old, value);
                if (SearchIndex* index = m_table->get_search_index(col_key)) {
                    index->set(m_key, new_val);
                }
                values.set(m_row_ndx, new_val);
            }
            else {
                throw IllegalOperation("No prior value");
            }
        }
        else {
            ArrayInteger values(alloc);
            values.set_parent(&fields, col_ndx.val + 1);
            values.init_from_parent();
            int64_t old = values.get(m_row_ndx);
            auto new_val = add_wrap(old, value);
            if (SearchIndex* index = m_table->get_search_index(col_key)) {
                index->set(m_key, new_val);
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
    checked_update_if_needed();
    get_table()->check_column(col_key);
    ColKey::Idx col_ndx = col_key.get_index();
    ColumnType type = col_key.get_type();
    if (type != col_type_Link)
        throw InvalidArgument(ErrorCodes::TypeMismatch, "Property not a link");
    TableRef target_table = get_target_table(col_key);
    TableKey target_table_key = target_table->get_key();
    if (target_key) {
        ClusterTree* ct = target_key.is_unresolved() ? target_table->m_tombstones.get() : &target_table->m_clusters;
        if (!ct->is_valid(target_key)) {
            InvalidArgument(ErrorCodes::KeyNotFound, "Invalid object key");
        }
        if (target_table->is_embedded()) {
            throw IllegalOperation(
                util::format("Setting not allowed on embedded object: %1", m_table->get_column_name(col_key)));
        }
    }
    ObjKey old_key = get_unfiltered_link(col_key); // Will update if needed
    CascadeState state(CascadeState::Mode::Strong);
    bool recurse = false;

    if (target_key != old_key) {
        recurse = replace_backlink(col_key, {target_table_key, old_key}, {target_table_key, target_key}, state);
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
    }

    if (Replication* repl = get_replication()) {
        repl->set(m_table.unchecked_ptr(), col_key, m_key, target_key,
                  is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws
    }

    if (recurse)
        target_table->remove_recursive(state);

    return *this;
}

template <>
Obj& Obj::set<ObjLink>(ColKey col_key, ObjLink target_link, bool is_default)
{
    checked_update_if_needed();
    get_table()->check_column(col_key);
    ColKey::Idx col_ndx = col_key.get_index();
    ColumnType type = col_key.get_type();
    if (type != col_type_TypedLink)
        throw InvalidArgument(ErrorCodes::TypeMismatch, "Property not a typed link");
    m_table->get_parent_group()->validate(target_link);

    ObjLink old_link = get<ObjLink>(col_key); // Will update if needed
    CascadeState state(old_link.get_obj_key().is_unresolved() ? CascadeState::Mode::All : CascadeState::Mode::Strong);
    bool recurse = false;

    if (target_link != old_link) {
        recurse = replace_backlink(col_key, old_link, target_link, state);
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
    }

    if (Replication* repl = get_replication()) {
        repl->set(m_table.unchecked_ptr(), col_key, m_key, target_link,
                  is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws
    }

    if (recurse)
        const_cast<Table*>(m_table.unchecked_ptr())->remove_recursive(state);

    return *this;
}

Obj Obj::create_and_set_linked_object(ColKey col_key, bool is_default)
{
    checked_update_if_needed();
    get_table()->check_column(col_key);
    ColKey::Idx col_ndx = col_key.get_index();
    ColumnType type = col_key.get_type();
    if (type != col_type_Link)
        throw InvalidArgument(ErrorCodes::TypeMismatch, "Property not a link type");
    TableRef target_table = get_target_table(col_key);
    Table& t = *target_table;
    // Only links to embedded objects are allowed.
    REALM_ASSERT(t.is_embedded() || !get_table()->is_asymmetric());
    // Incoming links to asymmetric objects are disallowed.
    REALM_ASSERT(!t.is_asymmetric());
    TableKey target_table_key = t.get_key();
    auto result = t.is_embedded() ? t.create_linked_object() : t.create_object();
    auto target_key = result.get_key();
    ObjKey old_key = get<ObjKey>(col_key); // Will update if needed
    if (old_key != ObjKey()) {
        if (t.is_embedded()) {
            // If this is an embedded object and there was already an embedded object here, then we need to
            // emit an instruction to set the old embedded object to null to clear the old object on other
            // sync clients. Without this, you'll only see the Set ObjectValue instruction, which is idempotent,
            // and then array operations will have a corrupted prior_size.
            if (Replication* repl = get_replication()) {
                repl->set(m_table.unchecked_ptr(), col_key, m_key, util::none,
                          is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws
            }
        }
    }

    REALM_ASSERT(target_key != old_key); // We will always create a new object
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
        throw LogicError(ErrorCodes::LimitExceeded, "String too big");
}
template <>
inline void check_range(const BinaryData& val)
{
    if (REALM_UNLIKELY(val.size() > ArrayBlob::max_binary_size))
        throw LogicError(ErrorCodes::LimitExceeded, "Binary too big");
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

#if REALM_ENABLE_GEOSPATIAL

template <>
Obj& Obj::set(ColKey col_key, Geospatial value, bool is_default)
{
    checked_update_if_needed();
    get_table()->check_column(col_key);
    auto type = col_key.get_type();

    if (type != ColumnTypeTraits<Link>::column_id)
        throw InvalidArgument(ErrorCodes::TypeMismatch,
                              util::format("Property '%1' must be a link to set a Geospatial value",
                                           get_table()->get_column_name(col_key)));

    Obj geo = get_linked_object(col_key);
    if (!geo) {
        geo = create_and_set_linked_object(col_key, is_default);
    }
    value.assign_to(geo);
    return *this;
}

template <>
Obj& Obj::set(ColKey col_key, std::optional<Geospatial> value, bool is_default)
{
    checked_update_if_needed();
    auto table = get_table();
    table->check_column(col_key);
    auto type = col_key.get_type();
    auto attrs = col_key.get_attrs();

    if (type != ColumnTypeTraits<Link>::column_id)
        throw InvalidArgument(ErrorCodes::TypeMismatch,
                              util::format("Property '%1' must be a link to set a Geospatial value",
                                           get_table()->get_column_name(col_key)));
    if (!value && !attrs.test(col_attr_Nullable))
        throw NotNullable(Group::table_name_to_class_name(table->get_name()), table->get_column_name(col_key));

    if (!value) {
        set_null(col_key, is_default);
    }
    else {
        Obj geo = get_linked_object(col_key);
        if (!geo) {
            geo = create_and_set_linked_object(col_key, is_default);
        }
        value->assign_to(geo);
    }
    return *this;
}

#endif

template <class T>
Obj& Obj::set(ColKey col_key, T value, bool is_default)
{
    checked_update_if_needed();
    get_table()->check_column(col_key);
    auto type = col_key.get_type();
    auto attrs = col_key.get_attrs();
    auto col_ndx = col_key.get_index();

    if (type != ColumnTypeTraits<T>::column_id)
        throw InvalidArgument(ErrorCodes::TypeMismatch,
                              util::format("Property not a %1", ColumnTypeTraits<T>::column_id));
    if (value_is_null(value) && !attrs.test(col_attr_Nullable))
        throw NotNullable(Group::table_name_to_class_name(m_table->get_name()), m_table->get_column_name(col_key));

    check_range(value);

    SearchIndex* index = m_table->get_search_index(col_key);
    if (index && !m_key.is_unresolved()) {
        index->set(m_key, value);
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

void Obj::set_int(ColKey::Idx col_ndx, int64_t value)
{
    checked_update_if_needed();

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

void Obj::set_ref(ColKey::Idx col_ndx, ref_type value, CollectionType type)
{
    checked_update_if_needed();

    Allocator& alloc = get_alloc();
    alloc.bump_content_version();
    Array fallback(alloc);
    Array& fields = get_tree_top()->get_fields_accessor(fallback, m_mem);
    REALM_ASSERT(col_ndx.val + 1 < fields.size());
    ArrayMixed values(alloc);
    values.set_parent(&fields, col_ndx.val + 1);
    values.init_from_parent();
    values.set(m_row_ndx, Mixed(value, type));

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

template <class ValueType>
inline void Obj::nullify_single_link(ColKey col, ValueType target)
{
    ColKey::Idx origin_col_ndx = col.get_index();
    Allocator& alloc = get_alloc();
    Array fallback(alloc);
    Array& fields = get_tree_top()->get_fields_accessor(fallback, m_mem);
    using ArrayType = typename ColumnTypeTraits<ValueType>::cluster_leaf_type;
    ArrayType links(alloc);
    links.set_parent(&fields, origin_col_ndx.val + 1);
    links.init_from_parent();
    // Ensure we are nullifying correct link
    REALM_ASSERT(links.get(m_row_ndx) == target);
    links.set(m_row_ndx, ValueType{});
    sync(fields);

    if (Replication* repl = get_replication())
        repl->nullify_link(m_table.unchecked_ptr(), col,
                           m_key); // Throws
}

template <>
inline void Obj::nullify_single_link<Mixed>(ColKey col, Mixed target)
{
    ColKey::Idx origin_col_ndx = col.get_index();
    Allocator& alloc = get_alloc();
    Array fallback(alloc);
    Array& fields = get_tree_top()->get_fields_accessor(fallback, m_mem);
    ArrayMixed mixed(alloc);
    mixed.set_parent(&fields, origin_col_ndx.val + 1);
    mixed.init_from_parent();
    auto val = mixed.get(m_row_ndx);
    bool result = false;
    if (val.is_type(type_TypedLink)) {
        // Ensure we are nullifying correct link
        result = (val == target);
        mixed.set(m_row_ndx, Mixed{});
        sync(fields);

        if (Replication* repl = get_replication())
            repl->nullify_link(m_table.unchecked_ptr(), col,
                               m_key); // Throws
    }
    else if (val.is_type(type_Dictionary)) {
        Dictionary dict(*this, col);
        result = dict.nullify(target.get_link());
    }
    else if (val.is_type(type_List)) {
        Lst<Mixed> list(*this, col);
        result = list.nullify(target.get_link());
    }
    REALM_ASSERT(result);
    static_cast<void>(result);
}

void Obj::nullify_link(ColKey origin_col_key, ObjLink target_link) &&
{
    REALM_ASSERT(get_alloc().get_storage_version() == m_storage_version);

    struct LinkNullifier : public LinkTranslator {
        LinkNullifier(Obj origin_obj, ColKey origin_col, ObjLink target)
            : LinkTranslator(origin_obj, origin_col)
            , m_target_link(target)
        {
        }
        void on_list_of_links(LnkLst&) final
        {
            nullify_linklist(m_origin_obj, m_origin_col_key, m_target_link.get_obj_key());
        }
        void on_list_of_mixed(Lst<Mixed>& list) final
        {
            list.nullify(m_target_link);
        }
        void on_set_of_links(LnkSet&) final
        {
            nullify_set(m_origin_obj, m_origin_col_key, m_target_link.get_obj_key());
        }
        void on_set_of_mixed(Set<Mixed>&) final
        {
            nullify_set(m_origin_obj, m_origin_col_key, Mixed(m_target_link));
        }
        void on_dictionary(Dictionary& dict) final
        {
            dict.nullify(m_target_link);
        }
        void on_link_property(ColKey origin_col_key) final
        {
            m_origin_obj.nullify_single_link<ObjKey>(origin_col_key, m_target_link.get_obj_key());
        }
        void on_mixed_property(ColKey origin_col_key) final
        {
            m_origin_obj.nullify_single_link<Mixed>(origin_col_key, Mixed{m_target_link});
        }

    private:
        ObjLink m_target_link;
    } nullifier{*this, origin_col_key, target_link};

    nullifier.run();

    get_alloc().bump_content_version();
}


struct EmbeddedObjectLinkMigrator : public LinkTranslator {
    EmbeddedObjectLinkMigrator(Obj origin, ColKey origin_col, Obj dest_orig, Obj dest_replace)
        : LinkTranslator(origin, origin_col)
        , m_dest_orig(dest_orig)
        , m_dest_replace(dest_replace)
    {
    }
    void on_list_of_links(LnkLst& list) final
    {
        auto n = list.find_first(m_dest_orig.get_key());
        REALM_ASSERT(n != realm::npos);
        list.set(n, m_dest_replace.get_key());
    }
    void on_dictionary(Dictionary& dict) final
    {
        auto pos = dict.find_any(m_dest_orig.get_link());
        REALM_ASSERT(pos != realm::npos);
        Mixed key = dict.get_key(pos);
        dict.insert(key, m_dest_replace.get_link());
    }
    void on_link_property(ColKey col) final
    {
        REALM_ASSERT(!m_origin_obj.get<ObjKey>(col) || m_origin_obj.get<ObjKey>(col) == m_dest_orig.get_key());
        m_origin_obj.set(col, m_dest_replace.get_key());
    }
    void on_set_of_links(LnkSet&) final
    {
        // this should never happen because sets of embedded objects are not allowed at the schema level
        REALM_UNREACHABLE();
    }
    // The following cases have support here but are expected to fail later on in the
    // migration due to core not yet supporting untyped Mixed links to embedded objects.
    void on_set_of_mixed(Set<Mixed>& set) final
    {
        auto did_erase_pair = set.erase(m_dest_orig.get_link());
        REALM_ASSERT(did_erase_pair.second);
        set.insert(m_dest_replace.get_link());
    }
    void on_list_of_mixed(Lst<Mixed>& list) final
    {
        auto n = list.find_any(m_dest_orig.get_link());
        REALM_ASSERT(n != realm::npos);
        list.insert_any(n, m_dest_replace.get_link());
    }
    void on_mixed_property(ColKey col) final
    {
        REALM_ASSERT(m_origin_obj.get<Mixed>(col).is_null() ||
                     m_origin_obj.get<Mixed>(col) == m_dest_orig.get_link());
        m_origin_obj.set_any(col, m_dest_replace.get_link());
    }

private:
    Obj m_dest_orig;
    Obj m_dest_replace;
};

void Obj::handle_multiple_backlinks_during_schema_migration()
{
    REALM_ASSERT(!m_table->get_primary_key_column());
    converters::EmbeddedObjectConverter embedded_obj_tracker;
    auto copy_links = [&](ColKey col) {
        auto opposite_table = m_table->get_opposite_table(col);
        auto opposite_column = m_table->get_opposite_column(col);
        auto backlinks = get_all_backlinks(col);
        for (auto backlink : backlinks) {
            // create a new obj
            auto obj = m_table->create_object();
            embedded_obj_tracker.track(*this, obj);
            auto linking_obj = opposite_table->get_object(backlink);
            // change incoming links to point to the newly created object
            EmbeddedObjectLinkMigrator{linking_obj, opposite_column, *this, obj}.run();
        }
        embedded_obj_tracker.process_pending();
        return IteratorControl::AdvanceToNext;
    };
    m_table->for_each_backlink_column(copy_links);
}

LstBasePtr Obj::get_listbase_ptr(ColKey col_key) const
{
    auto list = CollectionParent::get_listbase_ptr(col_key, 0);
    list->set_owner(*this, col_key);
    return list;
}

SetBasePtr Obj::get_setbase_ptr(ColKey col_key) const
{
    auto set = CollectionParent::get_setbase_ptr(col_key, 0);
    set->set_owner(*this, col_key);
    return set;
}

Dictionary Obj::get_dictionary(ColKey col_key) const
{
    REALM_ASSERT(col_key.is_dictionary() || col_key.get_type() == col_type_Mixed);
    checked_update_if_needed();
    return Dictionary(Obj(*this), col_key);
}

Obj& Obj::set_collection(ColKey col_key, CollectionType type)
{
    REALM_ASSERT(col_key.get_type() == col_type_Mixed);
    if ((col_key.is_dictionary() && type == CollectionType::Dictionary) ||
        (col_key.is_list() && type == CollectionType::List)) {
        return *this;
    }
    if (type == CollectionType::Set) {
        throw IllegalOperation("Set nested in Mixed is not supported");
    }
    set(col_key, Mixed(0, type));

    return *this;
}

DictionaryPtr Obj::get_dictionary_ptr(ColKey col_key) const
{
    return std::make_shared<Dictionary>(get_dictionary(col_key));
}

DictionaryPtr Obj::get_dictionary_ptr(const Path& path) const
{
    return std::dynamic_pointer_cast<Dictionary>(get_collection_ptr(path));
}

Dictionary Obj::get_dictionary(StringData col_name) const
{
    return get_dictionary(get_column_key(col_name));
}

CollectionPtr Obj::get_collection_ptr(const Path& path) const
{
    REALM_ASSERT(path.size() > 0);
    // First element in path must be column name
    auto col_key = path[0].is_col_key() ? path[0].get_col_key() : m_table->get_column_key(path[0].get_key());
    REALM_ASSERT(col_key);
    size_t level = 1;
    CollectionBasePtr collection = get_collection_ptr(col_key);

    while (level < path.size()) {
        auto& path_elem = path[level];
        Mixed ref;
        if (collection->get_collection_type() == CollectionType::List) {
            ref = collection->get_any(path_elem.get_ndx());
        }
        else {
            ref = dynamic_cast<Dictionary*>(collection.get())->get(path_elem.get_key());
        }
        if (ref.is_type(type_List)) {
            collection = collection->get_list(path_elem);
        }
        else if (ref.is_type(type_Dictionary)) {
            collection = collection->get_dictionary(path_elem);
        }
        else {
            throw InvalidArgument("Wrong path");
        }
        level++;
    }

    return collection;
}

CollectionPtr Obj::get_collection_by_stable_path(const StablePath& path) const
{
    // First element in path is phony column key
    ColKey col_key = m_table->get_column_key(path[0]);
    size_t level = 1;
    CollectionBasePtr collection = get_collection_ptr(col_key);

    while (level < path.size()) {
        auto& index = path[level];
        auto get_ref = [&]() -> std::pair<Mixed, PathElement> {
            Mixed ref;
            PathElement path_elem;
            if (collection->get_collection_type() == CollectionType::List) {
                auto list_of_mixed = dynamic_cast<Lst<Mixed>*>(collection.get());
                size_t ndx = list_of_mixed->find_index(index);
                if (ndx != realm::not_found) {
                    ref = list_of_mixed->get(ndx);
                    path_elem = ndx;
                }
            }
            else {
                auto dict = dynamic_cast<Dictionary*>(collection.get());
                size_t ndx = dict->find_index(index);
                if (ndx != realm::not_found) {
                    ref = dict->get_any(ndx);
                    path_elem = dict->get_key(ndx).get_string();
                }
            }
            return {ref, path_elem};
        };
        auto [ref, path_elem] = get_ref();
        if (ref.is_type(type_List)) {
            collection = collection->get_list(path_elem);
        }
        else if (ref.is_type(type_Dictionary)) {
            collection = collection->get_dictionary(path_elem);
        }
        else {
            return nullptr;
        }
        level++;
    }

    return collection;
}

CollectionBasePtr Obj::get_collection_ptr(ColKey col_key) const
{
    if (col_key.is_collection()) {
        auto collection = CollectionParent::get_collection_ptr(col_key, 0);
        collection->set_owner(*this, col_key);
        return collection;
    }
    REALM_ASSERT(col_key.get_type() == col_type_Mixed);
    auto val = get<Mixed>(col_key);
    if (val.is_type(type_List)) {
        return std::make_shared<Lst<Mixed>>(*this, col_key);
    }
    REALM_ASSERT(val.is_type(type_Dictionary));
    return std::make_shared<Dictionary>(*this, col_key);
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

template <class T>
inline void replace_in_linkset(Obj& obj, ColKey origin_col_key, T target, T replacement)
{
    Set<T> link_set(origin_col_key);
    size_t ndx = find_link_value_in_collection(link_set, obj, origin_col_key, target);

    REALM_ASSERT(ndx != realm::npos); // There has to be one

    link_set.erase(target);
    link_set.insert(replacement);
}

inline void replace_in_dictionary(Obj& obj, ColKey origin_col_key, Mixed target, Mixed replacement)
{
    Dictionary dict(origin_col_key);
    size_t ndx = find_link_value_in_collection(dict, obj, origin_col_key, target);

    REALM_ASSERT(ndx != realm::npos); // There has to be one

    auto key = dict.get_key(ndx);
    dict.insert(key, replacement);
}

void Obj::assign_pk_and_backlinks(Obj& other)
{
    struct LinkReplacer : LinkTranslator {
        LinkReplacer(Obj origin, ColKey origin_col_key, const Obj& dest_orig, const Obj& dest_replace)
            : LinkTranslator(origin, origin_col_key)
            , m_dest_orig(dest_orig)
            , m_dest_replace(dest_replace)
        {
        }
        void on_list_of_links(LnkLst&) final
        {
            auto linklist = m_origin_obj.get_linklist(m_origin_col_key);
            linklist.replace_link(m_dest_orig.get_key(), m_dest_replace.get_key());
        }
        void on_list_of_mixed(Lst<Mixed>& list) final
        {
            list.replace_link(m_dest_orig.get_link(), m_dest_replace.get_link());
        }
        void on_set_of_links(LnkSet&) final
        {
            replace_in_linkset(m_origin_obj, m_origin_col_key, m_dest_orig.get_key(), m_dest_replace.get_key());
        }
        void on_set_of_mixed(Set<Mixed>&) final
        {
            replace_in_linkset<Mixed>(m_origin_obj, m_origin_col_key, m_dest_orig.get_link(),
                                      m_dest_replace.get_link());
        }
        void on_dictionary(Dictionary& dict) final
        {
            dict.replace_link(m_dest_orig.get_link(), m_dest_replace.get_link());
        }
        void on_link_property(ColKey col) final
        {
            REALM_ASSERT(!m_origin_obj.get<ObjKey>(col) || m_origin_obj.get<ObjKey>(col) == m_dest_orig.get_key());
            // Handle links as plain integers. Backlinks has been taken care of.
            // Be careful here - links are stored as value + 1 so that null link (-1) will be 0
            auto new_key = m_dest_replace.get_key();
            m_origin_obj.set_int(col.get_index(), new_key.value + 1);
            if (Replication* repl = m_origin_obj.get_replication())
                repl->set(m_origin_obj.get_table().unchecked_ptr(), col, m_origin_obj.get_key(), new_key);
        }
        void on_mixed_property(ColKey col) final
        {
            auto val = m_origin_obj.get_any(col);
            if (val.is_type(type_Dictionary)) {
                Dictionary dict(m_origin_obj, m_origin_col_key);
                dict.replace_link(m_dest_orig.get_link(), m_dest_replace.get_link());
            }
            else if (val.is_type(type_List)) {
                Lst<Mixed> list(m_origin_obj, m_origin_col_key);
                list.replace_link(m_dest_orig.get_link(), m_dest_replace.get_link());
            }
            else {
                REALM_ASSERT(val.is_null() || val.get_link().get_obj_key() == m_dest_orig.get_key());
                m_origin_obj.set(col, Mixed{m_dest_replace.get_link()});
            }
        }

    private:
        const Obj& m_dest_orig;
        const Obj& m_dest_replace;
    };

    REALM_ASSERT(get_table() == other.get_table());
    if (auto col_pk = m_table->get_primary_key_column()) {
        Mixed val = other.get_any(col_pk);
        this->set_any(col_pk, val);
    }
    auto nb_tombstones = m_table->m_tombstones->size();

    auto copy_links = [this, &other, nb_tombstones](ColKey col) {
        if (nb_tombstones != m_table->m_tombstones->size()) {
            // Object has been deleted - we are done
            return IteratorControl::Stop;
        }

        auto t = m_table->get_opposite_table(col);
        auto c = m_table->get_opposite_column(col);
        auto backlinks = other.get_all_backlinks(col);

        if (c.get_type() == col_type_Link && !(c.is_dictionary() || c.is_set())) {
            auto idx = col.get_index();
            // Transfer the backlinks from tombstone to live object
            REALM_ASSERT(_get<int64_t>(idx) == 0);
            auto other_val = other._get<int64_t>(idx);
            set_int(idx, other_val);
            other.set_int(idx, 0);
        }

        for (auto bl : backlinks) {
            auto linking_obj = t->get_object(bl);
            LinkReplacer replacer{linking_obj, c, other, *this};
            replacer.run();
        }
        return IteratorControl::AdvanceToNext;
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
        return set(col_key, null_key, is_default);
    }
    if (col_type == col_type_Mixed) {
        return set(col_key, Mixed{}, is_default);
    }

    auto attrs = col_key.get_attrs();
    if (REALM_UNLIKELY(!attrs.test(col_attr_Nullable))) {
        throw NotNullable(Group::table_name_to_class_name(m_table->get_name()), m_table->get_column_name(col_key));
    }

    checked_update_if_needed();

    SearchIndex* index = m_table->get_search_index(col_key);
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
        case col_type_UUID:
            do_set_null<ArrayUUIDNull>(col_key);
            break;
        case col_type_Mixed:
        case col_type_Link:
        case col_type_BackLink:
        case col_type_TypedLink:
            REALM_UNREACHABLE();
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

ref_type Obj::get_collection_ref(StableIndex index, CollectionType type) const
{
    if (index.is_collection()) {
        return to_ref(_get<int64_t>(index.get_index()));
    }
    if (check_index(index)) {
        auto val = _get<Mixed>(index.get_index());
        if (val.is_type(DataType(int(type)))) {
            return val.get_ref();
        }
        throw realm::IllegalOperation(util::format("Not a %1", type));
    }
    throw StaleAccessor("This collection is no more");
}

bool Obj::check_collection_ref(StableIndex index, CollectionType type) const noexcept
{
    if (index.is_collection()) {
        return true;
    }
    if (check_index(index)) {
        return _get<Mixed>(index.get_index()).is_type(DataType(int(type)));
    }
    return false;
}

void Obj::set_collection_ref(StableIndex index, ref_type ref, CollectionType type)
{
    if (index.is_collection()) {
        set_int(index.get_index(), from_ref(ref));
        return;
    }
    set_ref(index.get_index(), ref, type);
}

void Obj::set_backlink(ColKey col_key, ObjLink new_link) const
{
    if (!new_link) {
        return;
    }

    auto target_table = m_table->get_parent_group()->get_table(new_link.get_table_key());
    ColKey backlink_col_key;
    auto type = col_key.get_type();
    if (type == col_type_TypedLink || type == col_type_Mixed || col_key.is_dictionary()) {
        // This may modify the target table
        backlink_col_key = target_table->find_or_add_backlink_column(col_key, m_table->get_key());
        // it is possible that this was a link to the same table and that adding a backlink column has
        // caused the need to update this object as well.
        update_if_needed();
    }
    else {
        backlink_col_key = m_table->get_opposite_column(col_key);
    }
    auto obj_key = new_link.get_obj_key();
    auto target_obj =
        obj_key.is_unresolved() ? target_table->try_get_tombstone(obj_key) : target_table->try_get_object(obj_key);
    if (!target_obj) {
        throw InvalidArgument(ErrorCodes::KeyNotFound, "Target object not found");
    }
    target_obj.add_backlink(backlink_col_key, m_key);
}

bool Obj::replace_backlink(ColKey col_key, ObjLink old_link, ObjLink new_link, CascadeState& state) const
{
    bool recurse = remove_backlink(col_key, old_link, state);
    set_backlink(col_key, new_link);
    return recurse;
}

bool Obj::remove_backlink(ColKey col_key, ObjLink old_link, CascadeState& state) const
{
    if (!old_link) {
        return false;
    }

    REALM_ASSERT(m_table->valid_column(col_key));
    ObjKey old_key = old_link.get_obj_key();
    auto target_obj = m_table->get_parent_group()->get_object(old_link);
    TableRef target_table = target_obj.get_table();
    ColKey backlink_col_key;
    auto type = col_key.get_type();
    if (type == col_type_TypedLink || type == col_type_Mixed || col_key.is_dictionary()) {
        backlink_col_key = target_table->find_or_add_backlink_column(col_key, m_table->get_key());
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

    return false;
}

} // namespace realm
