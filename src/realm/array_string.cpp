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

#include <realm/array_string.hpp>

using namespace realm;

ArrayString::ArrayString(Allocator& a)
    : m_alloc(a)
{
    m_arr = new (&m_storage.m_string_short) ArrayStringShort(a, true);
}

void ArrayString::create()
{
    static_cast<ArrayStringShort*>(m_arr)->create();
}

void ArrayString::init_from_ref(ref_type ref)
{
    char* header = m_alloc.translate(ref);

    ArrayParent* parent = m_arr->get_parent();
    size_t ndx_in_parent = m_arr->get_ndx_in_parent();

    bool long_strings = Array::get_hasrefs_from_header(header);
    if (!long_strings) {
        // Small strings
        auto arr = new (&m_storage.m_string_short) ArrayStringShort(m_alloc, true);
        arr->init_from_mem(MemRef(header, ref, m_alloc));
        m_type = Type::small;
    }
    else {
        bool is_big = Array::get_context_flag_from_header(header);
        if (!is_big) {
            auto arr = new (&m_storage.m_string_long) ArrayBinary(m_alloc);
            arr->init_from_mem(MemRef(header, ref, m_alloc));
            m_type = Type::medium;
        }
        else {
            auto arr = new (&m_storage.m_big_blobs) ArrayBigBlobs(m_alloc, true);
            arr->init_from_mem(MemRef(header, ref, m_alloc));
            m_type = Type::big;
        }
    }
    m_arr->set_parent(parent, ndx_in_parent);
}

void ArrayString::init_from_parent()
{
    ref_type ref = m_arr->get_ref_from_parent();
    init_from_ref(ref);
}

size_t ArrayString::size() const
{
    switch (m_type) {
        case Type::small:
            return static_cast<ArrayStringShort*>(m_arr)->size();
        case Type::medium:
            return static_cast<ArrayBinary*>(m_arr)->size();
        case Type::big:
            return static_cast<ArrayBigBlobs*>(m_arr)->size();
    }
    return {};
}

void ArrayString::add(StringData value)
{
    switch (upgrade_leaf(value.size())) {
        case Type::small:
            static_cast<ArrayStringShort*>(m_arr)->add(value);
            break;
        case Type::medium:
            static_cast<ArrayBinary*>(m_arr)->add_string(value);
            break;
        case Type::big:
            static_cast<ArrayBigBlobs*>(m_arr)->add_string(value);
            break;
    }
}

void ArrayString::set(size_t ndx, StringData value)
{
    switch (upgrade_leaf(value.size())) {
        case Type::small:
            static_cast<ArrayStringShort*>(m_arr)->set(ndx, value);
            break;
        case Type::medium:
            static_cast<ArrayBinary*>(m_arr)->set_string(ndx, value);
            break;
        case Type::big:
            static_cast<ArrayBigBlobs*>(m_arr)->set_string(ndx, value);
            break;
    }
}

void ArrayString::insert(size_t ndx, StringData value)
{
    switch (upgrade_leaf(value.size())) {
        case Type::small:
            static_cast<ArrayStringShort*>(m_arr)->insert(ndx, value);
            break;
        case Type::medium:
            static_cast<ArrayBinary*>(m_arr)->insert_string(ndx, value);
            break;
        case Type::big:
            static_cast<ArrayBigBlobs*>(m_arr)->insert_string(ndx, value);
            break;
    }
}

StringData ArrayString::get(size_t ndx) const
{
    switch (m_type) {
        case Type::small:
            return static_cast<ArrayStringShort*>(m_arr)->get(ndx);
        case Type::medium:
            return static_cast<ArrayBinary*>(m_arr)->get_string(ndx);
        case Type::big:
            return static_cast<ArrayBigBlobs*>(m_arr)->get_string(ndx);
    }
    return {};
}

bool ArrayString::is_null(size_t ndx) const
{
    switch (m_type) {
        case Type::small:
            return static_cast<ArrayStringShort*>(m_arr)->is_null(ndx);
        case Type::medium:
            return static_cast<ArrayBinary*>(m_arr)->is_null(ndx);
        case Type::big:
            return static_cast<ArrayBigBlobs*>(m_arr)->is_null(ndx);
    }
    return {};
}

void ArrayString::erase(size_t ndx)
{
    switch (m_type) {
        case Type::small:
            static_cast<ArrayStringShort*>(m_arr)->erase(ndx);
            break;
        case Type::medium:
            static_cast<ArrayBinary*>(m_arr)->erase(ndx);
            break;
        case Type::big:
            static_cast<ArrayBigBlobs*>(m_arr)->erase(ndx);
            break;
    }
}

void ArrayString::truncate_and_destroy_children(size_t ndx)
{
    switch (m_type) {
        case Type::small:
            static_cast<ArrayStringShort*>(m_arr)->truncate(ndx);
            break;
        case Type::medium:
            static_cast<ArrayBinary*>(m_arr)->truncate(ndx);
            break;
        case Type::big:
            static_cast<ArrayBigBlobs*>(m_arr)->truncate(ndx);
            break;
    }
}

ArrayString::Type ArrayString::upgrade_leaf(size_t value_size)
{
    if (m_type == Type::big)
        return Type::big;

    if (m_type == Type::medium) {
        if (value_size <= medium_string_max_size)
            return Type::medium;

        // Upgrade root leaf from medium to big strings
        auto string_long = static_cast<ArrayBinary*>(m_arr);
        ArrayBigBlobs big_blobs(m_alloc, true);
        big_blobs.create(); // Throws

        size_t n = string_long->size();
        for (size_t i = 0; i < n; i++) {
            big_blobs.add_string(string_long->get_string(i)); // Throws
        }
        big_blobs.set_parent(string_long->get_parent(), string_long->get_ndx_in_parent());
        big_blobs.update_parent(); // Throws
        string_long->destroy();
        auto arr = new (&m_storage.m_big_blobs) ArrayBigBlobs(m_alloc, true);
        arr->init_from_mem(big_blobs.get_mem());

        m_type = Type::big;
        return Type::big;
    }

    // m_type == Type::small
    if (value_size <= small_string_max_size)
        return Type::small;

    if (value_size <= medium_string_max_size) {
        // Upgrade root leaf from small to medium strings
        auto string_short = static_cast<ArrayStringShort*>(m_arr);
        ArrayBinary string_long(m_alloc);
        string_long.create(); // Throws

        size_t n = string_short->size();
        for (size_t i = 0; i < n; i++) {
            string_long.add_string(string_short->get(i)); // Throws
        }
        string_long.set_parent(string_short->get_parent(), string_short->get_ndx_in_parent());
        string_long.update_parent(); // Throws
        string_short->destroy();
        auto arr = new (&m_storage.m_string_short) ArrayBinary(m_alloc);
        arr->init_from_mem(string_long.get_mem());

        m_type = Type::medium;
    }
    else {
        // Upgrade root leaf from small to big strings
        auto string_short = static_cast<ArrayStringShort*>(m_arr);
        ArrayBigBlobs big_blobs(m_alloc, true);
        big_blobs.create(); // Throws

        size_t n = string_short->size();
        for (size_t i = 0; i < n; i++) {
            big_blobs.add_string(string_short->get(i)); // Throws
        }
        big_blobs.set_parent(string_short->get_parent(), string_short->get_ndx_in_parent());
        big_blobs.update_parent(); // Throws
        string_short->destroy();
        auto arr = new (&m_storage.m_big_blobs) ArrayBigBlobs(m_alloc, true);
        arr->init_from_mem(big_blobs.get_mem());

        m_type = Type::big;
    }

    return m_type;
}
