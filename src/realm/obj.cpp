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
#include "realm/table_view.hpp"
#include "realm/replication.hpp"

namespace realm {

/********************************* ConstObj **********************************/

ConstObj::ConstObj(const ClusterTree* tree_top, MemRef mem, ObjKey key, size_t row_ndx)
    : m_table(tree_top->get_owner())
    , m_key(key)
    , m_mem(mem)
    , m_row_ndx(row_ndx)
    , m_valid(true)
{
    m_instance_version = tree_top->get_instance_version();
    m_storage_version = tree_top->get_storage_version(m_instance_version);
}

const ClusterTree* ConstObj::get_tree_top() const
{
    return &m_table->m_clusters;
}

Allocator& ConstObj::get_alloc() const
{
    return m_table->m_alloc;
}

const Spec& ConstObj::get_spec() const
{
    return m_table->m_spec;
}

Replication* ConstObj::get_replication() const
{
    return m_table->get_repl();
}

template <class T>
inline int ConstObj::cmp(const ConstObj& other, ColKey::Idx col_ndx) const
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

int ConstObj::cmp(const ConstObj& other, ColKey col_key) const
{
    ColKey::Idx col_ndx = col_key.get_index();
    ColumnAttrMask attr = col_key.get_attrs();
    REALM_ASSERT(!attr.test(col_attr_List)); // TODO: implement comparison of lists

    switch (DataType(col_key.get_type())) {
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
    size_t col_cnt = get_spec().get_public_column_count();
    while (col_cnt--) {
        ColKey key = m_table->spec_ndx2colkey(col_cnt);
        if (cmp(other, key) != 0) {
            return false;
        }
    }
    return true;
}

bool ConstObj::is_valid() const
{
    // Cache valid state. If once invalid, it can never become valid again
    if (m_valid)
        m_valid = (m_table->get_instance_version() == m_instance_version) && m_table->is_valid(m_key);

    return m_valid;
}

void ConstObj::remove()
{
    const_cast<Table*>(get_table())->remove_object(m_key);
}

Mixed ConstObj::get_any(ColKey col_key) const
{
    switch (col_key.get_type()) {
        case col_type_Int:
            if (col_key.get_attrs().test(col_attr_Nullable)) {
                return Mixed{get<util::Optional<int64_t>>(col_key)};
            }
            else {
                return Mixed{get<int64_t>(col_key)};
            }
        case col_type_Bool:
            return Mixed{get<util::Optional<bool>>(col_key)};
        case col_type_Float:
            return Mixed{get<util::Optional<float>>(col_key)};
        case col_type_Double:
            return Mixed{get<util::Optional<double>>(col_key)};
        case col_type_String:
            return Mixed{get<String>(col_key)};
        case col_type_Binary:
            return Mixed{get<Binary>(col_key)};
        case col_type_Timestamp:
            return Mixed{get<Timestamp>(col_key)};
        case col_type_Link:
            return Mixed{get<ObjKey>(col_key)};
        default:
            REALM_UNREACHABLE();
            break;
    }
    return {};
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
    return _impl::TableFriend::get_opposite_link_table(*m_table, col_key);
}

Obj::Obj(ClusterTree* tree_top, MemRef mem, ObjKey key, size_t row_ndx)
    : ConstObj(tree_top, mem, key, row_ndx)
{
}

bool ConstObj::update() const
{
    // Get a new object from key
    ConstObj new_obj = get_tree_top()->get(m_key);

    bool changes = (m_mem.get_addr() != new_obj.m_mem.get_addr()) || (m_row_ndx != new_obj.m_row_ndx);
    if (changes) {
        m_mem = new_obj.m_mem;
        m_row_ndx = new_obj.m_row_ndx;
    }
    // Always update versions
    m_storage_version = new_obj.m_storage_version;
    m_instance_version = new_obj.m_instance_version;

    return changes;
}

bool ConstObj::update_if_needed() const
{
    auto current_version = get_alloc().get_storage_version(m_instance_version);
    if (current_version != m_storage_version) {
        return update();
    }
    return false;
}

template <class T>
T ConstObj::get(ColKey col_key) const
{
    m_table->report_invalid_key(col_key);
    ColumnType type = col_key.get_type();
    REALM_ASSERT(type == ColumnTypeTraits<T>::column_id);

    return _get<T>(col_key.get_index());
}

template <class T>
T ConstObj::_get(ColKey::Idx col_ndx) const
{
    update_if_needed();

    typename ColumnTypeTraits<T>::cluster_leaf_type values(get_alloc());
    ref_type ref = to_ref(Array::get(m_mem.get_addr(), col_ndx.val + 1));
    values.init_from_ref(ref);

    return values.get(m_row_ndx);
}

template <>
int64_t ConstObj::_get<int64_t>(ColKey::Idx col_ndx) const
{
    // manual inline of is_in_sync():
    auto& alloc = get_alloc();
    auto current_version = alloc.get_storage_version(m_instance_version);
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
StringData ConstObj::_get<StringData>(ColKey::Idx col_ndx) const
{
    // manual inline of is_in_sync():
    auto& alloc = get_alloc();
    auto current_version = alloc.get_storage_version(m_instance_version);
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
BinaryData ConstObj::_get<BinaryData>(ColKey::Idx col_ndx) const
{
    // manual inline of is_in_sync():
    auto& alloc = get_alloc();
    auto current_version = alloc.get_storage_version(m_instance_version);
    if (current_version != m_storage_version) {
        update();
    }

    ref_type ref = to_ref(Array::get(m_mem.get_addr(), col_ndx.val + 1));
    return ArrayBinary::get(alloc.translate(ref), m_row_ndx, alloc);
}

/* FIXME: Make this one fast too!
template <>
ObjKey ConstObj::_get(size_t col_ndx) const
{
    return ObjKey(_get<int64_t>(col_ndx));
}
*/

ConstObj ConstObj::get_linked_object(ColKey link_col_key) const
{
    TableRef target_table = get_target_table(link_col_key);
    ObjKey key = get<ObjKey>(link_col_key);

    return target_table->get_object(key);
}

template <class T>
inline bool ConstObj::do_is_null(ColKey::Idx col_ndx) const
{
    T values(get_alloc());
    ref_type ref = to_ref(Array::get(m_mem.get_addr(), col_ndx.val + 1));
    values.init_from_ref(ref);
    return values.is_null(m_row_ndx);
}

template <>
inline bool ConstObj::do_is_null<ArrayString>(ColKey::Idx col_ndx) const
{
    ArrayString values(get_alloc());
    ref_type ref = to_ref(Array::get(m_mem.get_addr(), col_ndx.val + 1));
    values.set_spec(const_cast<Spec*>(&get_spec()), m_table->leaf_ndx2spec_ndx(col_ndx));
    values.init_from_ref(ref);
    return values.is_null(m_row_ndx);
}

size_t ConstObj::get_link_count(ColKey col_key) const
{
    return get_list<ObjKey>(col_key).size();
}

bool ConstObj::is_null(ColKey col_key) const
{
    update_if_needed();
    ColumnAttrMask attr = col_key.get_attrs();
    ColKey::Idx col_ndx = col_key.get_index();
    if (attr.test(col_attr_Nullable) && !attr.test(col_attr_List)) {
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
    const Table* target_table = m_table;
    auto look_for_backlinks = [&](ColKey backlink_col_key) {
        // Find origin table and column for this backlink column
        TableRef origin_table = target_table->get_opposite_table(backlink_col_key);
        ColKey origin_col = target_table->get_opposite_column(backlink_col_key);
        if (!only_strong_links || origin_table->get_link_type(origin_col) == link_Strong) {
            auto cnt = get_backlink_count(*origin_table, origin_col);
            if (cnt)
                return true;
        }
        return false;
    };
    return m_table->for_each_backlink_column(look_for_backlinks);
}

size_t ConstObj::get_backlink_count(bool only_strong_links) const
{
    const Table* target_table = m_table;
    size_t cnt = 0;
    auto look_for_backlinks = [&](ColKey backlink_col_key) {
        // Find origin table and column for this backlink column
        TableRef origin_table = target_table->get_opposite_table(backlink_col_key);
        ColKey origin_col = target_table->get_opposite_column(backlink_col_key);
        if (!only_strong_links || origin_table->get_link_type(origin_col) == link_Strong) {
            cnt += get_backlink_count(*origin_table, origin_col);
        }
        return false;
    };
    m_table->for_each_backlink_column(look_for_backlinks);
    return cnt;
}

size_t ConstObj::get_backlink_count(const Table& origin, ColKey origin_col_key) const
{
    size_t cnt = 0;
    TableKey origin_table_key = origin.get_key();
    if (origin_table_key != TableKey()) {
        ColKey backlink_col_key = origin.get_opposite_column(origin_col_key);
        cnt = get_backlink_count(backlink_col_key);
    }
    return cnt;
}

ObjKey ConstObj::get_backlink(const Table& origin, ColKey origin_col_key, size_t backlink_ndx) const
{
    ColKey backlink_col_key = origin.get_opposite_column(origin_col_key);
    return get_backlink(backlink_col_key, backlink_ndx);
}

TableView ConstObj::get_backlink_view(Table* src_table, ColKey src_col_key)
{
    TableView tv(src_table, src_col_key, *this);
    tv.do_sync();
    return tv;
}

size_t ConstObj::get_backlink_count(ColKey backlink_col) const
{
    get_table()->report_invalid_key(backlink_col);
    Allocator& alloc = get_alloc();
    Array fields(alloc);
    fields.init_from_mem(m_mem);

    ArrayBacklink backlinks(alloc);
    backlinks.set_parent(&fields, backlink_col.get_index().val + 1);
    backlinks.init_from_parent();

    return backlinks.get_backlink_count(m_row_ndx);
}

ObjKey ConstObj::get_backlink(ColKey backlink_col, size_t backlink_ndx) const
{
    get_table()->report_invalid_key(backlink_col);
    Allocator& alloc = get_alloc();
    Array fields(alloc);
    fields.init_from_mem(m_mem);

    ArrayBacklink backlinks(alloc);
    backlinks.set_parent(&fields, backlink_col.get_index().val + 1);
    backlinks.init_from_parent();
    return backlinks.get_backlink(m_row_ndx, backlink_ndx);
}

namespace {

template <class T>
inline void out_floats(std::ostream& out, T value)
{
    std::streamsize old = out.precision();
    out.precision(std::numeric_limits<T>::digits10 + 1);
    out << std::scientific << value;
    out.precision(old);
}

void out_mixed(std::ostream& out, const Mixed& val)
{
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
        case type_String:
            out << "\"" << val.get<String>() << "\"";
            break;
        case type_Binary: {
            out << "\"";
            auto bin = val.get<Binary>();
            const char* p = bin.data();
            for (size_t i = 0; i < bin.size(); ++i) {
                out << std::setw(2) << std::setfill('0') << std::hex << static_cast<unsigned int>(p[i]) << std::dec;
            }
            out << "\"";
            break;
        }
        case type_Timestamp:
            out << "\"";
            out << val.get<Timestamp>();
            out << "\"";
            break;
        case type_Link:
        case type_LinkList:
        case type_OldDateTime:
        case type_OldMixed:
        case type_OldTable:
            break;
    }
}

} // anonymous namespace

void ConstObj::to_json(std::ostream& out, size_t link_depth, std::map<std::string, std::string>& renames,
                       std::vector<ColKey>& followed) const
{
    out << "{";
    bool first = true;
    auto col_keys = m_table->get_column_keys();
    for (auto ck : col_keys) {
        if (is_null(ck))
            continue;

        if (first) {
            first = false;
        }
        else {
            out << ",";
        }

        StringData name = m_table->get_column_name(ck);
        DataType type = DataType(ck.get_type());
        if (renames[name] != "")
            name = renames[name];

        out << "\"" << name << "\":";

        if (ck.get_attrs().test(col_attr_List)) {
            if (type == type_LinkList) {
                auto ll = get_linklist(ck);
                auto sz = ll.size();

                if ((link_depth == 0) ||
                    (link_depth == not_found && std::find(followed.begin(), followed.end(), ck) != followed.end())) {
                    out << "{\"table\": \"" << get_target_table(ck)->get_name() << "\", \"rows\": [";
                    for (size_t i = 0; i < sz; i++) {
                        if (i > 0)
                            out << ", ";
                        out << ll.get(i).value;
                    }
                    out << "]}";
                }
                else {
                    out << "[";
                    for (size_t i = 0; i < sz; i++) {
                        if (i > 0)
                            out << ", ";
                        followed.push_back(ck);
                        size_t new_depth = link_depth == not_found ? not_found : link_depth - 1;
                        ll.get_object(i).to_json(out, new_depth, renames, followed);
                    }
                    out << "]";
                }

            }
            else {
                auto list = get_listbase_ptr(ck);
                auto sz = list->size();

                out << "[";
                for (size_t i = 0; i < sz; i++) {
                    auto val = list->get_any(i);
                    if (val.is_null())
                        continue;

                    if (i > 0)
                        out << ", ";

                    out_mixed(out, val);
                }
                out << "]";
            }
        }
        else {
            if (type == type_Link) {
                auto obj = get_linked_object(ck);
                if ((link_depth == 0) ||
                    (link_depth == not_found && std::find(followed.begin(), followed.end(), ck) != followed.end())) {
                    out << "\"" << obj.get_key().value << "\"";
                    break;
                }
                else {
                    out << "[";
                    followed.push_back(ck);
                    size_t new_depth = link_depth == not_found ? not_found : link_depth - 1;
                    obj.to_json(out, new_depth, renames, followed);
                    out << "]";
                }
            }
            else {
                out_mixed(out, get_any(ck));
            }
        }
    }
    out << "}";
}

/*********************************** Obj *************************************/

bool Obj::ensure_writeable()
{
    Allocator& alloc = get_alloc();
    if (alloc.is_read_only(m_mem.get_ref())) {
        m_mem = const_cast<ClusterTree*>(get_tree_top())->ensure_writeable(m_key);
        m_storage_version = alloc.get_storage_version(m_instance_version);
        return true;
    }
    return false;
}

void Obj::bump_content_version()
{
    Allocator& alloc = get_alloc();
    alloc.bump_content_version();
}

void Obj::bump_both_versions()
{
    Allocator& alloc = get_alloc();
    alloc.bump_content_version();
    alloc.bump_storage_version();
}

Obj& Obj::set(ColKey col_key, Mixed value)
{
    if (value.is_null()) {
        REALM_ASSERT(col_key.get_attrs().test(col_attr_Nullable));
        set_null(col_key);
    }
    else {
        REALM_ASSERT(value.get_type() == DataType(col_key.get_type()));
        switch (col_key.get_type()) {
            case col_type_Int:
                if (col_key.get_attrs().test(col_attr_Nullable)) {
                    set(col_key, util::Optional<Int>(value.get_int()));
                }
                else {
                    set(col_key, value.get_int());
                }
                break;
            case col_type_Bool:
                set(col_key, value.get_bool());
                break;
            case col_type_Float:
                set(col_key, value.get_float());
                break;
            case col_type_Double:
                set(col_key, value.get_double());
                break;
            case col_type_String:
                set(col_key, value.get_string());
                break;
            case col_type_Binary:
                set(col_key, value.get<Binary>());
                break;
            case col_type_Timestamp:
                set(col_key, value.get<Timestamp>());
                break;
            case col_type_Link:
                set(col_key, value.get<ObjKey>());
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
    get_table()->report_invalid_key(col_key);
    auto col_ndx = col_key.get_index();

    if (col_key.get_type() != ColumnTypeTraits<int64_t>::column_id)
        throw LogicError(LogicError::illegal_type);

    ensure_writeable();

    if (StringIndex* index = m_table->get_search_index(col_key)) {
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

    if (Replication* repl = get_replication()) {
        repl->set_int(m_table, col_key, m_key, value,
                      is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws
    }

    return *this;
}

Obj& Obj::add_int(ColKey col_key, int64_t value)
{
    update_if_needed();
    get_table()->report_invalid_key(col_key);
    auto col_ndx = col_key.get_index();

    ensure_writeable();

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
    auto attr = col_key.get_attrs();
    if (attr.test(col_attr_Nullable)) {
        ArrayIntNull values(alloc);
        values.set_parent(&fields, col_ndx.val + 1);
        values.init_from_parent();
        Optional<int64_t> old = values.get(m_row_ndx);
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

    if (Replication* repl = get_replication()) {
        repl->add_int(m_table, col_key, m_key, value); // Throws
    }

    return *this;
}

template <>
Obj& Obj::set<ObjKey>(ColKey col_key, ObjKey target_key, bool is_default)
{
    update_if_needed();
    get_table()->report_invalid_key(col_key);
    ColKey::Idx col_ndx = col_key.get_index();
    ColumnType type = col_key.get_type();
    if (type != ColumnTypeTraits<ObjKey>::column_id)
        throw LogicError(LogicError::illegal_type);
    TableRef target_table = get_target_table(col_key);
    if (target_key != null_key && !target_table->is_valid(target_key)) {
        throw LogicError(LogicError::target_row_index_out_of_range);
    }

    ObjKey old_key = get<ObjKey>(col_key); // Will update if needed

    if (target_key != old_key) {
        CascadeState state;

        ensure_writeable();
        bool recurse = replace_backlink(col_key, old_key, target_key, state);

        Allocator& alloc = get_alloc();
        alloc.bump_content_version();
        Array fallback(alloc);
        Array& fields = get_tree_top()->get_fields_accessor(fallback, m_mem);
        REALM_ASSERT(col_ndx.val + 1 < fields.size());
        ArrayKey values(alloc);
        values.set_parent(&fields, col_ndx.val + 1);
        values.init_from_parent();

        values.set(m_row_ndx, target_key);

        if (Replication* repl = get_replication()) {
            repl->set(m_table, col_key, m_key, target_key,
                      is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws
        }

        if (recurse)
            target_table->remove_recursive(state);
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
    get_table()->report_invalid_key(col_key);
    auto type = col_key.get_type();
    auto attrs = col_key.get_attrs();
    auto col_ndx = col_key.get_index();

    if (type != ColumnTypeTraits<T>::column_id)
        throw LogicError(LogicError::illegal_type);
    if (value_is_null(value) && !attrs.test(col_attr_Nullable))
        throw LogicError(LogicError::column_not_nullable);
    check_range(value);

    ensure_writeable();

    if (StringIndex* index = m_table->get_search_index(col_key)) {
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

    if (Replication* repl = get_replication())
        repl->set<T>(m_table, col_key, m_key, value,
                     is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws

    return *this;
}

void Obj::set_int(ColKey col_key, int64_t value)
{
    update_if_needed();
    ensure_writeable();

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
}

void Obj::add_backlink(ColKey backlink_col_key, ObjKey origin_key)
{
    ensure_writeable();

    ColKey::Idx backlink_col_ndx = backlink_col_key.get_index();
    Allocator& alloc = get_alloc();
    alloc.bump_content_version();
    Array fallback(alloc);
    Array& fields = get_tree_top()->get_fields_accessor(fallback, m_mem);

    ArrayBacklink backlinks(alloc);
    backlinks.set_parent(&fields, backlink_col_ndx.val + 1);
    backlinks.init_from_parent();

    backlinks.add(m_row_ndx, origin_key);
}

bool Obj::remove_one_backlink(ColKey backlink_col_key, ObjKey origin_key)
{
    ensure_writeable();

    ColKey::Idx backlink_col_ndx = backlink_col_key.get_index();
    Allocator& alloc = get_alloc();
    alloc.bump_content_version();
    Array fallback(alloc);
    Array& fields = get_tree_top()->get_fields_accessor(fallback, m_mem);

    ArrayBacklink backlinks(alloc);
    backlinks.set_parent(&fields, backlink_col_ndx.val + 1);
    backlinks.init_from_parent();

    return backlinks.remove(m_row_ndx, origin_key);
}

void Obj::nullify_link(ColKey origin_col_key, ObjKey target_key)
{
    ensure_writeable();

    ColKey::Idx origin_col_ndx = origin_col_key.get_index();

    Allocator& alloc = get_alloc();
    Array fallback(alloc);
    Array& fields = get_tree_top()->get_fields_accessor(fallback, m_mem);

    ColumnAttrMask attr = origin_col_key.get_attrs();
    if (attr.test(col_attr_List)) {
        Lst<ObjKey> link_list(*this, origin_col_key);
        size_t ndx = link_list.find_first(target_key);

        REALM_ASSERT(ndx != realm::npos); // There has to be one

        if (Replication* repl = get_replication()) {
            repl->link_list_nullify(link_list, ndx); // Throws
        }

        // We cannot just call 'remove' on link_list as it would produce the wrong
        // replication instruction and also attempt an update on the backlinks from
        // the object that we in the process of removing.
        BPlusTree<ObjKey>& tree = const_cast<BPlusTree<ObjKey>&>(link_list.get_tree());
        tree.erase(ndx);
    }
    else {
        ArrayKey links(alloc);
        links.set_parent(&fields, origin_col_ndx.val + 1);
        links.init_from_parent();

        // Ensure we are nullifying correct link
        ObjKey key = links.get(m_row_ndx);
        REALM_ASSERT(key == target_key);

        links.set(m_row_ndx, ObjKey{});

        if (Replication* repl = get_replication())
            repl->nullify_link(m_table, origin_col_key, m_key); // Throws
    }
    alloc.bump_content_version();
}

void Obj::set_backlink(ColKey col_key, ObjKey new_key)
{
    if (new_key != realm::null_key) {
        REALM_ASSERT(m_table->valid_column(col_key));
        TableRef target_table = get_target_table(col_key);
        ColKey backlink_col_key = m_table->get_opposite_column(col_key);
        REALM_ASSERT(target_table->valid_column(backlink_col_key));

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

    const Table* origin_table = m_table;

    REALM_ASSERT(origin_table->valid_column(col_key));
    TableRef target_table = get_target_table(col_key);
    ColKey backlink_col_key = m_table->get_opposite_column(col_key);
    REALM_ASSERT(target_table->valid_column(backlink_col_key));

    bool strong_links = (origin_table->get_link_type(col_key) == link_Strong);
    CascadeState::Mode mode = state.m_mode;

    if (old_key != realm::null_key) {
        Obj target_obj = target_table->get_object(old_key);
        bool last_removed = target_obj.remove_one_backlink(backlink_col_key, m_key); // Throws

        // Check if the object should be cascade deleted
        if (mode != CascadeState::none && (mode == CascadeState::all || (strong_links && last_removed))) {
            bool have_backlinks = target_obj.has_backlinks(state.m_mode == CascadeState::strong);

            if (!have_backlinks) {
                state.m_to_be_deleted.emplace_back(target_table->get_key(), old_key);
                recurse = true;
            }
        }
    }

    return recurse;
}


template int64_t ConstObj::get<int64_t>(ColKey col_key) const;
template util::Optional<int64_t> ConstObj::get<util::Optional<int64_t>>(ColKey col_key) const;
template bool ConstObj::get<Bool>(ColKey col_key) const;
template util::Optional<Bool> ConstObj::get<util::Optional<Bool>>(ColKey col_key) const;
template float ConstObj::get<float>(ColKey col_key) const;
template util::Optional<float> ConstObj::get<util::Optional<float>>(ColKey col_key) const;
template double ConstObj::get<double>(ColKey col_key) const;
template util::Optional<double> ConstObj::get<util::Optional<double>>(ColKey col_key) const;
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
}

Obj& Obj::set_null(ColKey col_key, bool is_default)
{
    ColumnType col_type = col_key.get_type();
    // Links need special handling
    if (col_type == col_type_Link) {
        return set(col_key, null_key);
    }
    auto attrs = col_key.get_attrs();
    if (REALM_UNLIKELY(!attrs.test(col_attr_Nullable))) {
        throw LogicError(LogicError::column_not_nullable);
    }

    update_if_needed();
    ensure_writeable();

    if (StringIndex* index = m_table->get_search_index(col_key)) {
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
        case col_type_String:
            do_set_null<ArrayString>(col_key);
            break;
        case col_type_Binary:
            do_set_null<ArrayBinary>(col_key);
            break;
        case col_type_Timestamp:
            do_set_null<ArrayTimestamp>(col_key);
            break;
        default:
            break;
    }

    if (Replication* repl = get_replication())
        repl->set_null(m_table, col_key, m_key, is_default ? _impl::instr_SetDefault : _impl::instr_Set); // Throws

    return *this;
}


ColKey Obj::spec_ndx2colkey(size_t col_ndx)
{
    return get_table()->spec_ndx2colkey(col_ndx);
}

} // namespace realm
