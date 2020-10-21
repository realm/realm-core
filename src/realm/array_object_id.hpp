/*************************************************************************
 *
 * Copyright 2019 Realm Inc.
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

#ifndef REALM_ARRAY_OBJECT_ID_HPP
#define REALM_ARRAY_OBJECT_ID_HPP

#include <realm/array.hpp>
#include <realm/object_id.hpp>

namespace realm {

class ArrayObjectId : public ArrayPayload, protected Array {
public:
    using value_type = ObjectId;

    using Array::Array;
    using Array::destroy;
    using Array::get_ref;
    using Array::init_from_mem;
    using Array::init_from_parent;
    using Array::update_parent;
    using Array::verify;

    static ObjectId default_value(bool nullable)
    {
        REALM_ASSERT(!nullable);
        return ObjectId();
    }

    void create()
    {
        auto mem = Array::create(type_Normal, false, wtype_Multiply, 0, 0, m_alloc); // Throws
        Array::init_from_mem(mem);
    }

    void init_from_ref(ref_type ref) noexcept override
    {
        Array::init_from_ref(ref);
    }

    void set_parent(ArrayParent* parent, size_t ndx_in_parent) noexcept override
    {
        Array::set_parent(parent, ndx_in_parent);
    }

    size_t size() const
    {
        auto data_bytes = m_size - div_round_up<s_block_size>(m_size); // remove one byte per block.
        return data_bytes / s_width;
    }

    bool is_null(size_t ndx) const
    {
        return this->get_width() == 0 || get_pos(ndx).is_null(this);
    }

    ObjectId get(size_t ndx) const
    {
        REALM_ASSERT(is_valid_ndx(ndx));
        REALM_ASSERT(!is_null(ndx));
        return get_pos(ndx).get_value(this);
    }

    void add(const ObjectId& value)
    {
        insert(size(), value);
    }

    void set(size_t ndx, const ObjectId& value);
    void insert(size_t ndx, const ObjectId& value);
    void erase(size_t ndx);
    void move(ArrayObjectId& dst, size_t ndx);
    void clear()
    {
        truncate(0);
    }
    void truncate(size_t ndx)
    {
        Array::truncate(calc_required_bytes(ndx));
    }

    size_t find_first(const ObjectId& value, size_t begin = 0, size_t end = npos) const noexcept;

protected:
    static constexpr size_t s_width = sizeof(ObjectId); // Size of each element
    static_assert(s_width == 12, "Size of ObjectId must be 12");

    // A block is a byte bitvector indicating null entries and 8 ObjectIds.
    static constexpr size_t s_block_size = s_width * 8 + 1; // 97

    template <size_t div>
    static size_t div_round_up(size_t num)
    {
        return (num + div - 1) / div;
    }

    // An accessor for the data at a given index. All casting and offset calculation should be kept here.
    struct Pos {
        size_t base_byte;
        size_t offset;

        void set_value(ArrayObjectId* arr, const ObjectId& val) const
        {
            reinterpret_cast<ObjectId*>(arr->m_data + base_byte + 1 /*null bit byte*/)[offset] = val;
        }
        const ObjectId& get_value(const ArrayObjectId* arr) const
        {
            return reinterpret_cast<const ObjectId*>(arr->m_data + base_byte + 1 /*null bit byte*/)[offset];
        }

        void set_null(ArrayObjectId* arr, bool new_is_null) const
        {
            auto& bitvec = arr->m_data[base_byte];
            if (new_is_null) {
                bitvec |= 1 << offset;
            }
            else {
                bitvec &= ~(1 << offset);
            }
        }
        bool is_null(const ArrayObjectId* arr) const
        {
            return arr->m_data[base_byte] & (1 << offset);
        }
    };

    static Pos get_pos(size_t ndx)
    {

        return Pos{(ndx / 8) * s_block_size, ndx % 8};
    }

    static size_t calc_required_bytes(size_t num_items)
    {
        return (num_items * s_width) +       // ObjectId data
               (div_round_up<8>(num_items)); // null bitvectors (1 byte per 8 oids, rounded up)
    }

    size_t calc_byte_len(size_t num_items, size_t /*unused width*/ = 0) const override
    {
        return num_items + Node::header_size;
    }

    bool is_valid_ndx(size_t ndx) const
    {
        return calc_byte_len(ndx) <= m_size;
    }
};

// The nullable ObjectId array uses the same layout and is compatible with the non-nullable one. It adds support for
// operations on null. Because the base class maintains null markers, we are able to defer to it for many operations.
class ArrayObjectIdNull : public ArrayObjectId {
public:
    using ArrayObjectId::ArrayObjectId;
    static util::Optional<ObjectId> default_value(bool nullable)
    {
        if (nullable)
            return util::none;
        return ObjectId();
    }

    void set(size_t ndx, const util::Optional<ObjectId>& value)
    {
        if (value) {
            ArrayObjectId::set(ndx, *value);
        }
        else {
            set_null(ndx);
        }
    }
    void add(const util::Optional<ObjectId>& value)
    {
        insert(size(), value);
    }
    void insert(size_t ndx, const util::Optional<ObjectId>& value)
    {
        if (value) {
            ArrayObjectId::insert(ndx, *value);
        }
        else {
            ArrayObjectId::insert(ndx, null_oid);
            set_null(ndx);
        }
    }
    void set_null(size_t ndx)
    {
        copy_on_write();
        auto pos = get_pos(ndx);
        pos.set_value(this, null_oid);
        pos.set_null(this, true);
    }
    util::Optional<ObjectId> get(size_t ndx) const noexcept
    {
        auto pos = get_pos(ndx);
        if (pos.is_null(this)) {
            return util::none;
        }
        return pos.get_value(this);
    }
    size_t find_first(const util::Optional<ObjectId>& value, size_t begin = 0, size_t end = npos) const
    {
        if (value) {
            return ArrayObjectId::find_first(*value, begin, end);
        }
        else {
            return find_first_null(begin, end);
        }
    }
    size_t find_first_null(size_t begin = 0, size_t end = npos) const;

private:
    static const ObjectId null_oid;
};


} // namespace realm

#endif /* REALM_ARRAY_OBJECT_ID_HPP */
