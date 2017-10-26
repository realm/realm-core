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

#include <realm/array_binary.hpp>

using namespace realm;

ArrayBinary::ArrayBinary(Allocator& a)
    : m_alloc(a)
{
    m_arr = new (&m_storage.m_small_blobs) ArraySmallBlobs(a);
}

void ArrayBinary::create()
{
    static_cast<ArraySmallBlobs*>(m_arr)->create();
}

void ArrayBinary::init_from_ref(ref_type ref)
{
    char* header = m_alloc.translate(ref);

    ArrayParent* parent = m_arr->get_parent();
    size_t ndx_in_parent = m_arr->get_ndx_in_parent();

    m_is_big = Array::get_context_flag_from_header(header);
    if (!m_is_big) {
        auto arr = new (&m_storage.m_small_blobs) ArraySmallBlobs(m_alloc);
        arr->init_from_mem(MemRef(header, ref, m_alloc));
    }
    else {
        auto arr = new (&m_storage.m_big_blobs) ArrayBigBlobs(m_alloc, true);
        arr->init_from_mem(MemRef(header, ref, m_alloc));
    }

    m_arr->set_parent(parent, ndx_in_parent);
}


void ArrayBinary::init_from_parent()
{
    ref_type ref = m_arr->get_ref_from_parent();
    init_from_ref(ref);
}

size_t ArrayBinary::size() const
{
    if (!m_is_big) {
        return static_cast<ArraySmallBlobs*>(m_arr)->size();
    }
    else {
        return static_cast<ArrayBigBlobs*>(m_arr)->size();
    }
}

void ArrayBinary::add(BinaryData value)
{
    bool is_big = upgrade_leaf(value.size());
    if (!is_big) {
        static_cast<ArraySmallBlobs*>(m_arr)->add(value);
    }
    else {
        static_cast<ArrayBigBlobs*>(m_arr)->add(value);
    }
}

void ArrayBinary::set(size_t ndx, BinaryData value)
{
    bool is_big = upgrade_leaf(value.size());
    if (!is_big) {
        static_cast<ArraySmallBlobs*>(m_arr)->set(ndx, value);
    }
    else {
        static_cast<ArrayBigBlobs*>(m_arr)->set(ndx, value);
    }
}

void ArrayBinary::insert(size_t ndx, BinaryData value)
{
    bool is_big = upgrade_leaf(value.size());
    if (!is_big) {
        static_cast<ArraySmallBlobs*>(m_arr)->insert(ndx, value);
    }
    else {
        static_cast<ArrayBigBlobs*>(m_arr)->insert(ndx, value);
    }
}

BinaryData ArrayBinary::get(size_t ndx) const
{
    if (!m_is_big) {
        return static_cast<ArraySmallBlobs*>(m_arr)->get(ndx);
    }
    else {
        return static_cast<ArrayBigBlobs*>(m_arr)->get(ndx);
    }
}

bool ArrayBinary::is_null(size_t ndx) const
{
    if (!m_is_big) {
        return static_cast<ArraySmallBlobs*>(m_arr)->is_null(ndx);
    }
    else {
        return static_cast<ArrayBigBlobs*>(m_arr)->is_null(ndx);
    }
}

void ArrayBinary::erase(size_t ndx)
{
    if (!m_is_big) {
        return static_cast<ArraySmallBlobs*>(m_arr)->erase(ndx);
    }
    else {
        return static_cast<ArrayBigBlobs*>(m_arr)->erase(ndx);
    }
}

void ArrayBinary::truncate_and_destroy_children(size_t ndx)
{
    if (!m_is_big) {
        return static_cast<ArraySmallBlobs*>(m_arr)->truncate(ndx);
    }
    else {
        return static_cast<ArrayBigBlobs*>(m_arr)->truncate(ndx);
    }
}

bool ArrayBinary::upgrade_leaf(size_t value_size)
{
    if (m_is_big)
        return true;

    if (value_size <= small_blob_max_size)
        return false;

    // Upgrade root leaf from small to big blobs
    auto small_blobs = static_cast<ArraySmallBlobs*>(m_arr);
    ArrayBigBlobs big_blobs(m_alloc, true);
    big_blobs.create(); // Throws

    size_t n = small_blobs->size();
    for (size_t i = 0; i < n; i++) {
        big_blobs.add(small_blobs->get(i)); // Throws
    }
    big_blobs.set_parent(small_blobs->get_parent(), small_blobs->get_ndx_in_parent());
    big_blobs.update_parent(); // Throws
    small_blobs->destroy();
    auto arr = new (&m_storage.m_big_blobs) ArrayBigBlobs(m_alloc, true);
    arr->init_from_mem(big_blobs.get_mem());

    m_is_big = true;
    return true;
}
