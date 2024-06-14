/*************************************************************************
 *
 * Copyright 2018 Realm Inc.
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

#include <realm/array_mixed.hpp>
#include <realm/array_basic.hpp>

using namespace realm;

ArrayMixed::ArrayMixed(Allocator& a)
    : Array(a)
    , m_composite(a)
    , m_ints(a)
    , m_int_pairs(a)
    , m_strings(a)
    , m_refs(a)
{
    m_composite.set_parent(this, payload_idx_type);
}

void ArrayMixed::create()
{
    Array::create(type_HasRefs, false, payload_idx_size);
    m_composite.create(type_Normal);
    m_composite.update_parent();
}

void ArrayMixed::init_from_mem(MemRef mem) noexcept
{
    Array::init_from_mem(mem);
    m_composite.init_from_parent();
    m_ints.detach();
    m_int_pairs.detach();
    m_strings.detach();
    m_refs.detach();
}

void ArrayMixed::add(Mixed value)
{
    if (value.is_null()) {
        m_composite.add(0);
        return;
    }
    m_composite.add(store(value));
}


void ArrayMixed::set(size_t ndx, Mixed value)
{
    auto old_type = get_type(ndx);
    // If we replace a collections ref value with one of the
    // same type, then it is just an update of of the
    // ref stored in the parent. If the new type is a different
    // type then it means that we are overwriting a collection
    // with some other value and hence the collection must be
    // destroyed as well as the possible key.
    bool destroy_collection = !value.is_type(old_type);

    if (value.is_null()) {
        set_null(ndx);
    }
    else {
        erase_linked_payload(ndx, destroy_collection);
        m_composite.set(ndx, store(value));
    }

    if (destroy_collection && Array::size() > payload_idx_key) {
        if (auto ref = Array::get_as_ref(payload_idx_key)) {
            Array keys(Array::get_alloc());
            keys.set_parent(const_cast<ArrayMixed*>(this), payload_idx_key);
            keys.init_from_ref(ref);
            if (ndx < keys.size())
                keys.set(ndx, 0);
        }
    }
}

void ArrayMixed::insert(size_t ndx, Mixed value)
{
    if (value.is_null()) {
        m_composite.insert(ndx, 0);
    }
    else {
        m_composite.insert(ndx, store(value));
    }
    if (Array::size() > payload_idx_key) {
        if (auto ref = Array::get_as_ref(payload_idx_key)) {
            Array keys(Array::get_alloc());
            keys.set_parent(const_cast<ArrayMixed*>(this), payload_idx_key);
            keys.init_from_ref(ref);
            keys.insert(ndx, 0);
        }
    }
}

void ArrayMixed::set_null(size_t ndx)
{
    auto val = m_composite.get(ndx);
    if (val) {
        erase_linked_payload(ndx, true);
        m_composite.set(ndx, 0);
    }
}

Mixed ArrayMixed::get(size_t ndx) const
{
    int64_t val = m_composite.get(ndx);

    if (val) {
        int64_t int_val = val >> s_data_shift;
        size_t payload_ndx = size_t(int_val);
        DataType type = DataType((val & s_data_type_mask) - 1);
        switch (type) {
            case type_Int: {
                if (val & s_payload_idx_mask) {
                    ensure_int_array();
                    return Mixed(m_ints.get(payload_ndx));
                }
                return Mixed(int_val);
            }
            case type_Bool:
                return Mixed(int_val != 0);
            case type_Float:
                ensure_int_array();
                return Mixed(type_punning<float>(m_ints.get(payload_ndx)));
            case type_Double:
                ensure_int_array();
                return Mixed(type_punning<double>(m_ints.get(payload_ndx)));
            case type_String: {
                ensure_string_array();
                REALM_ASSERT(size_t(int_val) < m_strings.size());
                return Mixed(m_strings.get(payload_ndx));
            }
            case type_Binary: {
                ensure_string_array();
                REALM_ASSERT(size_t(int_val) < m_strings.size());
                auto s = m_strings.get(payload_ndx);
                return Mixed(BinaryData(s.data(), s.size()));
            }
            case type_Timestamp: {
                ensure_int_pair_array();
                payload_ndx <<= 1;
                REALM_ASSERT(payload_ndx + 1 < m_int_pairs.size());
                return Mixed(Timestamp(m_int_pairs.get(payload_ndx), int32_t(m_int_pairs.get(payload_ndx + 1))));
            }
            case type_ObjectId: {
                ensure_string_array();
                REALM_ASSERT(size_t(int_val) < m_strings.size());
                auto s = m_strings.get(payload_ndx);
                ObjectId id;
                memcpy(&id, s.data(), sizeof(ObjectId));
                return Mixed(id);
            }
            case type_Decimal: {
                ensure_int_pair_array();
                Decimal128::Bid128 raw;
                payload_ndx <<= 1;
                REALM_ASSERT(payload_ndx + 1 < m_int_pairs.size());
                raw.w[0] = m_int_pairs.get(payload_ndx);
                raw.w[1] = m_int_pairs.get(payload_ndx + 1);
                return Mixed(Decimal128(raw));
            }
            case type_Link:
                ensure_int_array();
                return Mixed(ObjKey(m_ints.get(payload_ndx)));
            case type_TypedLink: {
                ensure_int_pair_array();
                payload_ndx <<= 1;
                REALM_ASSERT(payload_ndx + 1 < m_int_pairs.size());
                ObjLink ret{TableKey(uint32_t(m_int_pairs.get(payload_ndx))),
                            ObjKey(m_int_pairs.get(payload_ndx + 1))};
                return Mixed(ret);
            }
            case type_UUID: {
                ensure_string_array();
                REALM_ASSERT(size_t(int_val) < m_strings.size());
                auto s = m_strings.get(payload_ndx);
                UUID::UUIDBytes bytes{};
                std::copy_n(s.data(), bytes.size(), bytes.begin());
                return Mixed(UUID(bytes));
            }
            default:
                if (size_t((val & s_payload_idx_mask) >> s_payload_idx_shift) == payload_idx_ref) {
                    ensure_ref_array();
                    return Mixed(m_refs.get(payload_ndx), CollectionType(int(type)));
                }
                break;
        }
    }

    return {};
}

void ArrayMixed::clear()
{
    m_composite.clear();
    m_ints.destroy();
    m_int_pairs.destroy();
    m_strings.destroy();
    m_refs.destroy_deep();
    Array::set(payload_idx_int, 0);
    Array::set(payload_idx_pair, 0);
    Array::set(payload_idx_str, 0);
    if (Array::size() > payload_idx_ref) {
        Array::set(payload_idx_ref, 0);
    }
    if (Array::size() > payload_idx_key) {
        if (auto ref = Array::get_as_ref(payload_idx_key)) {
            Array::destroy(ref, m_composite.get_alloc());
            Array::set(payload_idx_key, 0);
        }
    }
}

void ArrayMixed::erase(size_t ndx)
{
    erase_linked_payload(ndx, true);
    m_composite.erase(ndx);
    if (Array::size() > payload_idx_key) {
        if (auto ref = Array::get_as_ref(payload_idx_key)) {
            Array keys(Array::get_alloc());
            keys.set_parent(const_cast<ArrayMixed*>(this), payload_idx_key);
            keys.init_from_ref(ref);
            keys.erase(ndx);
        }
    }
}

void ArrayMixed::move(ArrayMixed& dst, size_t ndx)
{
    auto sz = size();
    size_t i = ndx;
    while (i < sz) {
        auto val = get(i++);
        dst.add(val);
    }
    if (Array::size() > payload_idx_key) {
        if (auto ref = Array::get_as_ref(payload_idx_key)) {
            dst.ensure_keys();
            Array keys(Array::get_alloc());
            keys.set_parent(const_cast<ArrayMixed*>(this), payload_idx_key);
            keys.init_from_ref(ref);
            for (size_t j = 0, i = ndx; i < sz; i++, j++) {
                dst.set_key(j, keys.get(i));
            }
            keys.truncate(ndx);
        }
    }
    while (i > ndx) {
        erase_linked_payload(--i, false);
    }
    m_composite.truncate(ndx);
}

size_t ArrayMixed::find_first(Mixed value, size_t begin, size_t end) const noexcept
{
    if (value.is_null()) {
        return m_composite.find_first(0, begin, end);
    }
    DataType type = value.get_type();
    if (end == realm::npos)
        end = size();
    for (size_t i = begin; i < end; i++) {
        if (Mixed::data_types_are_comparable(this->get_type(i), type) && get(i) == value) {
            return i;
        }
    }
    return realm::npos;
}

bool ArrayMixed::ensure_keys()
{
    if (Array::size() < payload_idx_key + 1 || Array::get(payload_idx_key) == 0) {
        Array keys(Array::get_alloc());
        keys.set_parent(const_cast<ArrayMixed*>(this), payload_idx_key);
        keys.create(type_Normal, false, size(), 0);
        keys.update_parent();

        return false;
    }
    return true;
}

size_t ArrayMixed::find_key(int64_t key) const noexcept
{
    if (ref_type ref = get_as_ref(payload_idx_key)) {
        Array keys(Array::get_alloc());
        keys.init_from_ref(ref);
        return keys.find_first(key);
    }
    return realm::not_found;
}

void ArrayMixed::set_key(size_t ndx, int64_t key)
{
    Array keys(Array::get_alloc());
    ensure_array_accessor(keys, payload_idx_key);
    while (keys.size() <= ndx) {
        keys.add(0);
    }
    keys.set(ndx, key);
}

int64_t ArrayMixed::get_key(size_t ndx) const
{
    Array keys(Array::get_alloc());
    if (ref_type ref = get_as_ref(payload_idx_key)) {
        keys.init_from_ref(ref);
        return (ndx < keys.size()) ? keys.get(ndx) : 0;
    }
    return 0;
}

void ArrayMixed::verify() const
{
    // TODO: Implement
}

void ArrayMixed::ensure_array_accessor(Array& arr, size_t ndx_in_parent) const
{
    if (!arr.is_attached()) {
        ref_type ref = get_as_ref(ndx_in_parent);
        arr.set_parent(const_cast<ArrayMixed*>(this), ndx_in_parent);
        if (ref) {
            arr.init_from_ref(ref);
        }
        else {
            arr.create(ndx_in_parent == payload_idx_ref ? type_HasRefs : type_Normal);
            arr.update_parent();
        }
    }
}

void ArrayMixed::ensure_int_array() const
{
    ensure_array_accessor(m_ints, payload_idx_int);
}

void ArrayMixed::ensure_int_pair_array() const
{
    ensure_array_accessor(m_int_pairs, payload_idx_pair);
}

void ArrayMixed::ensure_string_array() const
{
    if (!m_strings.is_attached()) {
        ref_type ref = get_as_ref(payload_idx_str);
        m_strings.set_parent(const_cast<ArrayMixed*>(this), payload_idx_str);
        if (ref) {
            m_strings.init_from_ref(ref);
        }
        else {
            m_strings.create();
            m_strings.update_parent();
        }
    }
}

void ArrayMixed::ensure_ref_array() const
{
    while (Array::size() < payload_idx_ref + 1) {
        const_cast<ArrayMixed*>(this)->Array::add(0);
    }
    ensure_array_accessor(m_refs, payload_idx_ref);
}

void ArrayMixed::replace_index(size_t old_ndx, size_t new_ndx, size_t payload_arr_index)
{
    if (old_ndx != new_ndx) {
        size_t sz = m_composite.size();
        for (size_t i = 0; i != sz; i++) {
            int64_t val = m_composite.get(i);
            if (size_t((val & s_payload_idx_mask) >> s_payload_idx_shift) == payload_arr_index &&
                (val >> s_data_shift) == int64_t(old_ndx)) {
                m_composite.set(i, int64_t(new_ndx << s_data_shift) | (val & 0xff));
                return;
            }
        }
    }
}

void ArrayMixed::erase_linked_payload(size_t ndx, bool free_linked_arrays)
{
    auto val = m_composite.get(ndx);
    auto payload_arr_index = size_t((val & s_payload_idx_mask) >> s_payload_idx_shift);

    if (payload_arr_index) {
        // A value is stored in one of the payload arrays
        size_t last_ndx = 0;
        size_t erase_ndx = size_t(val >> s_data_shift);
        // Clean up current value by moving last over
        switch (payload_arr_index) {
            case payload_idx_int: {
                ensure_int_array();
                last_ndx = m_ints.size() - 1;
                if (erase_ndx != last_ndx) {
                    m_ints.set(erase_ndx, m_ints.get(last_ndx));
                    replace_index(last_ndx, erase_ndx, payload_arr_index);
                }
                m_ints.erase(last_ndx);
                break;
            }
            case payload_idx_str: {
                ensure_string_array();
                last_ndx = m_strings.size() - 1;
                if (erase_ndx != last_ndx) {
                    StringData tmp = m_strings.get(last_ndx);
                    std::string tmp_val(tmp.data(), tmp.size());
                    m_strings.set(erase_ndx, StringData(tmp_val));
                    replace_index(last_ndx, erase_ndx, payload_arr_index);
                }
                m_strings.erase(last_ndx);
                break;
            }
            case payload_idx_pair: {
                ensure_int_pair_array();
                last_ndx = m_int_pairs.size() - 2;
                erase_ndx <<= 1;
                if (erase_ndx != last_ndx) {
                    m_int_pairs.set(erase_ndx, m_int_pairs.get(last_ndx));
                    m_int_pairs.set(erase_ndx + 1, m_int_pairs.get(last_ndx + 1));
                    replace_index(last_ndx >> 1, erase_ndx >> 1, payload_arr_index);
                }
                m_int_pairs.truncate(last_ndx);
                break;
            }
            case payload_idx_ref: {
                ensure_ref_array();
                last_ndx = m_refs.size() - 1;
                auto old_ref = m_refs.get(erase_ndx);
                if (erase_ndx != last_ndx) {
                    m_refs.set(erase_ndx, m_refs.get(last_ndx));
                    replace_index(last_ndx, erase_ndx, payload_arr_index);
                }
                m_refs.erase(last_ndx);
                if (old_ref && free_linked_arrays)
                    Array::destroy_deep(old_ref, m_composite.get_alloc());
                break;
            }
            default:
                break;
        }
    }
}

int64_t ArrayMixed::store(const Mixed& value)
{
    DataType type = value.get_type();
    int64_t val;
    switch (type) {
        case type_Int: {
            int64_t int_val = value.get_int();
            if (std::numeric_limits<int32_t>::min() <= int_val && int_val <= std::numeric_limits<int32_t>::max()) {
                val = (static_cast<uint64_t>(int_val) << s_data_shift);
            }
            else {
                ensure_int_array();
                size_t ndx = m_ints.size();
                m_ints.add(int_val);
                val = int64_t(ndx << s_data_shift) | (payload_idx_int << s_payload_idx_shift);
            }
            break;
        }
        case type_Bool:
            val = (value.get_bool() << s_data_shift);
            break;
        case type_Float: {
            ensure_int_array();
            size_t ndx = m_ints.size();
            m_ints.add(type_punning<int64_t>(value.get_float()));
            val = int64_t(ndx << s_data_shift) | (payload_idx_int << s_payload_idx_shift);
            break;
        }
        case type_Double: {
            ensure_int_array();
            size_t ndx = m_ints.size();
            m_ints.add(type_punning<int64_t>(value.get_double()));
            val = int64_t(ndx << s_data_shift) | (payload_idx_int << s_payload_idx_shift);
            break;
        }
        case type_String: {
            ensure_string_array();
            size_t ndx = m_strings.size();
            m_strings.add(value.get_string());
            val = int64_t(ndx << s_data_shift) | (payload_idx_str << s_payload_idx_shift);
            break;
        }
        case type_Binary: {
            ensure_string_array();
            size_t ndx = m_strings.size();
            auto bin = value.get<Binary>();
            m_strings.add(StringData(bin.data(), bin.size()));
            val = int64_t(ndx << s_data_shift) | (payload_idx_str << s_payload_idx_shift);
            break;
        }
        case type_Timestamp: {
            ensure_int_pair_array();
            size_t ndx = m_int_pairs.size() / 2;
            auto t = value.get_timestamp();
            m_int_pairs.add(t.get_seconds());
            m_int_pairs.add(t.get_nanoseconds());
            val = int64_t(ndx << s_data_shift) | (payload_idx_pair << s_payload_idx_shift);
            break;
        }
        case type_ObjectId: {
            ensure_string_array();
            size_t ndx = m_strings.size();
            auto id = value.get<ObjectId>();
            char buffer[sizeof(ObjectId)];
            memcpy(buffer, &id, sizeof(ObjectId));
            m_strings.add(StringData(buffer, sizeof(ObjectId)));
            val = int64_t(ndx << s_data_shift) | (payload_idx_str << s_payload_idx_shift);
            break;
        }
        case type_Decimal: {
            ensure_int_pair_array();
            size_t ndx = m_int_pairs.size() / 2;
            auto t = value.get<Decimal128>();
            m_int_pairs.add(t.raw()->w[0]);
            m_int_pairs.add(t.raw()->w[1]);
            val = int64_t(ndx << s_data_shift) | (payload_idx_pair << s_payload_idx_shift);
            break;
        }
        case type_Link: {
            ensure_int_array();
            size_t ndx = m_ints.size();
            m_ints.add(value.get<ObjKey>().value);
            val = int64_t(ndx << s_data_shift) | (payload_idx_int << s_payload_idx_shift);
            break;
        }
        case type_TypedLink: {
            ensure_int_pair_array();
            size_t ndx = m_int_pairs.size() / 2;
            auto t = value.get<ObjLink>();
            m_int_pairs.add(int64_t(t.get_table_key().value));
            m_int_pairs.add(t.get_obj_key().value);
            val = int64_t(ndx << s_data_shift) | (payload_idx_pair << s_payload_idx_shift);
            break;
        }
        case type_UUID: {
            ensure_string_array();
            size_t ndx = m_strings.size();
            auto id = value.get<UUID>();
            const auto uuid_bytes = id.to_bytes();
            m_strings.add(StringData(reinterpret_cast<const char*>(uuid_bytes.data()), uuid_bytes.size()));
            val = int64_t(ndx << s_data_shift) | (payload_idx_str << s_payload_idx_shift);
            break;
        }
        default:
            REALM_ASSERT(type == type_List || type == type_Dictionary);
            ensure_ref_array();
            size_t ndx = m_refs.size();
            m_refs.add(value.get_ref());
            val = int64_t(ndx << s_data_shift) | (payload_idx_ref << s_payload_idx_shift);
            break;
    }
    return val + int(type) + 1;
}
