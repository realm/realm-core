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

#include <utility> // pair

#include <realm/array_blobs_small.hpp>
#include <realm/array_blob.hpp>
#include <realm/array_integer.hpp>
#include <realm/impl/destroy_guard.hpp>

using namespace realm;

void ArraySmallBlobs::init_from_mem(MemRef mem) noexcept
{
    Array::init_from_mem(mem);
    ref_type offsets_ref = get_as_ref(0);
    ref_type blob_ref = get_as_ref(1);

    m_offsets.init_from_ref(offsets_ref);
    m_blob.init_from_ref(blob_ref);

    if (!legacy_array_type()) {
        ref_type nulls_ref = get_as_ref(2);
        m_nulls.init_from_ref(nulls_ref);
    }
}

void ArraySmallBlobs::add(BinaryData value, bool add_zero_term)
{
    REALM_ASSERT_7(value.size(), ==, 0, ||, value.data(), !=, 0);

    if (value.is_null() && legacy_array_type())
        throw LogicError(LogicError::column_not_nullable);

    m_blob.add(value.data(), value.size(), add_zero_term);
    size_t end = value.size();
    if (add_zero_term)
        ++end;
    if (!m_offsets.is_empty())
        end += to_size_t(m_offsets.back());
    m_offsets.add(end);

    if (!legacy_array_type())
        m_nulls.add(value.is_null());
}

void ArraySmallBlobs::set(size_t ndx, BinaryData value, bool add_zero_term)
{
    REALM_ASSERT_3(ndx, <, m_offsets.size());
    REALM_ASSERT_3(value.size(), == 0 ||, value.data());

    if (value.is_null() && legacy_array_type())
        throw LogicError(LogicError::column_not_nullable);

    int_fast64_t start = ndx ? m_offsets.get(ndx - 1) : 0;
    int_fast64_t current_end = m_offsets.get(ndx);
    size_t stored_size = value.size();
    if (add_zero_term)
        ++stored_size;
    int_fast64_t diff = (start + stored_size) - current_end;
    m_blob.replace(to_size_t(start), to_size_t(current_end), value.data(), value.size(), add_zero_term);
    m_offsets.adjust(ndx, m_offsets.size(), diff);

    if (!legacy_array_type())
        m_nulls.set(ndx, value.is_null());
}

void ArraySmallBlobs::insert(size_t ndx, BinaryData value, bool add_zero_term)
{
    REALM_ASSERT_3(ndx, <=, m_offsets.size());
    REALM_ASSERT_3(value.size(), == 0 ||, value.data());

    if (value.is_null() && legacy_array_type())
        throw LogicError(LogicError::column_not_nullable);

    size_t pos = ndx ? to_size_t(m_offsets.get(ndx - 1)) : 0;
    m_blob.insert(pos, value.data(), value.size(), add_zero_term);

    size_t stored_size = value.size();
    if (add_zero_term)
        ++stored_size;
    m_offsets.insert(ndx, pos + stored_size);
    m_offsets.adjust(ndx + 1, m_offsets.size(), stored_size);

    if (!legacy_array_type())
        m_nulls.insert(ndx, value.is_null());
}

void ArraySmallBlobs::erase(size_t ndx)
{
    REALM_ASSERT_3(ndx, <, m_offsets.size());

    size_t start = ndx ? to_size_t(m_offsets.get(ndx - 1)) : 0;
    size_t end = to_size_t(m_offsets.get(ndx));

    m_blob.erase(start, end);
    m_offsets.erase(ndx);
    m_offsets.adjust(ndx, m_offsets.size(), int64_t(start) - end);

    if (!legacy_array_type())
        m_nulls.erase(ndx);
}

BinaryData ArraySmallBlobs::get(const char* header, size_t ndx, Allocator& alloc) noexcept
{
    // Column *may* be nullable if top has 3 refs (3'rd being m_nulls). Else, if it has 2, it's non-nullable
    // See comment in legacy_array_type() and also in array_binary.hpp.
    size_t siz = Array::get_size_from_header(header);
    REALM_ASSERT_7(siz, ==, 2, ||, siz, ==, 3);

    if (siz == 3) {
        std::pair<int64_t, int64_t> p = get_two(header, 1);
        const char* nulls_header = alloc.translate(to_ref(p.second));
        int64_t n = ArrayInteger::get(nulls_header, ndx);
        // 0 or 1 is all that is ever written to m_nulls; any other content would be a bug
        REALM_ASSERT_3(n == 1, ||, n == 0);
        bool null = (n != 0);
        if (null)
            return BinaryData{};
    }

    std::pair<int64_t, int64_t> p = get_two(header, 0);
    const char* offsets_header = alloc.translate(to_ref(p.first));
    const char* blob_header = alloc.translate(to_ref(p.second));
    size_t begin, end;
    if (ndx) {
        p = get_two(offsets_header, ndx - 1);
        begin = to_size_t(p.first);
        end = to_size_t(p.second);
    }
    else {
        begin = 0;
        end = to_size_t(Array::get(offsets_header, ndx));
    }
    BinaryData bd = BinaryData(ArrayBlob::get(blob_header, begin), end - begin);
    return bd;
}

MemRef ArraySmallBlobs::create_array(size_t size, Allocator& alloc, BinaryData values)
{
    // Only null and zero-length non-null allowed as initialization value
    REALM_ASSERT(values.size() == 0);
    Array top(alloc);
    _impl::DeepArrayDestroyGuard dg(&top);
    top.create(type_HasRefs); // Throws

    _impl::DeepArrayRefDestroyGuard dg_2(alloc);
    {
        bool context_flag = false;
        int64_t value = 0;
        MemRef mem = ArrayInteger::create_array(type_Normal, context_flag, size, value, alloc); // Throws
        dg_2.reset(mem.get_ref());
        int64_t v = from_ref(mem.get_ref());
        top.add(v); // Throws
        dg_2.release();
    }
    {
        size_t blobs_size = 0;
        MemRef mem = ArrayBlob::create_array(blobs_size, alloc); // Throws
        dg_2.reset(mem.get_ref());
        int64_t v = from_ref(mem.get_ref());
        top.add(v); // Throws
        dg_2.release();
    }
    {
        // Always create a m_nulls array, regardless if its column is marked as nullable or not. NOTE: This is new
        // - existing binary arrays from earier versions of core will not have this third array. All methods on
        // ArraySmallBlobs must thus check if this array exists before trying to access it. If it doesn't, it must be
        // interpreted as if its column isn't nullable.
        bool context_flag = false;
        int64_t value = values.is_null() ? 1 : 0;
        MemRef mem = ArrayInteger::create_array(type_Normal, context_flag, size, value, alloc); // Throws
        dg_2.reset(mem.get_ref());
        int64_t v = from_ref(mem.get_ref());
        top.add(v); // Throws
        dg_2.release();
    }

    dg.release();
    return top.get_mem();
}

size_t ArraySmallBlobs::find_first(BinaryData value, bool is_string, size_t begin, size_t end) const noexcept
{
    size_t sz = size();
    if (end == npos)
        end = sz;
    REALM_ASSERT_11(begin, <=, sz, &&, end, <=, sz, &&, begin, <=, end);

    if (value.is_null()) {
        for (size_t i = begin; i != end; ++i) {
            if (m_nulls.get(i))
                return i;
        }
    }
    else {
        // When strings are stored as blobs, they are always zero-terminated
        // but the value we get as input might not be.
        size_t value_size = value.size();
        size_t full_size = is_string ? value_size + 1 : value_size;

        size_t start_ofs = begin ? to_size_t(m_offsets.get(begin - 1)) : 0;
        for (size_t i = begin; i != end; ++i) {
            size_t end_ofs = to_size_t(m_offsets.get(i));
            size_t blob_size = end_ofs - start_ofs;
            if (!m_nulls.get(i) && blob_size == full_size) {
                const char* blob_value = m_blob.get(start_ofs);
                if (std::equal(blob_value, blob_value + value_size, value.data()))
                    return i;
            }
            start_ofs = end_ofs;
        }
    }

    return not_found;
}

MemRef ArraySmallBlobs::slice(size_t offset, size_t slice_size, Allocator& target_alloc) const
{
    REALM_ASSERT(is_attached());

    ArraySmallBlobs array_slice(target_alloc);
    _impl::ShallowArrayDestroyGuard dg(&array_slice);
    array_slice.create(); // Throws
    size_t begin = offset;
    size_t end = offset + slice_size;
    for (size_t i = begin; i != end; ++i) {
        BinaryData value = get(i);
        array_slice.add(value); // Throws
    }
    dg.release();
    return array_slice.get_mem();
}


#ifdef REALM_DEBUG // LCOV_EXCL_START ignore debug functions

void ArraySmallBlobs::to_dot(std::ostream& out, bool, StringData title) const
{
    ref_type ref = get_ref();

    out << "subgraph cluster_binary" << ref << " {" << std::endl;
    out << " label = \"ArrayBinary";
    if (title.size() != 0)
        out << "\\n'" << title << "'";
    out << "\";" << std::endl;

    Array::to_dot(out, "binary_top");
    m_offsets.to_dot(out, "offsets");
    m_blob.to_dot(out, "blob");

    out << "}" << std::endl;
}

#endif // LCOV_EXCL_STOP ignore debug functions
