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

#include <algorithm>

#include <realm/array_blobs_big.hpp>
#include <realm/column_integer.hpp>


using namespace realm;

BinaryData ArrayBigBlobs::get_at(size_t ndx, size_t& pos) const noexcept
{
    ref_type ref = get_as_ref(ndx);
    if (ref == 0)
        return {}; // realm::null();

    ArrayBlob blob(m_alloc);
    blob.init_from_ref(ref);

    return blob.get_at(pos);
}


void ArrayBigBlobs::add(BinaryData value, bool add_zero_term)
{
    REALM_ASSERT_7(value.size(), ==, 0, ||, value.data(), !=, 0);

    if (value.is_null()) {
        Array::add(0); // Throws
    }
    else {
        ArrayBlob new_blob(m_alloc);
        new_blob.create();                                                      // Throws
        ref_type ref = new_blob.add(value.data(), value.size(), add_zero_term); // Throws
        Array::add(from_ref(ref));                                              // Throws
    }
}


void ArrayBigBlobs::set(size_t ndx, BinaryData value, bool add_zero_term)
{
    REALM_ASSERT_3(ndx, <, size());
    REALM_ASSERT_7(value.size(), ==, 0, ||, value.data(), !=, 0);

    ref_type ref = get_as_ref(ndx);

    if (ref == 0 && value.is_null()) {
        return;
    }
    else if (ref == 0 && value.data() != nullptr) {
        ArrayBlob new_blob(m_alloc);
        new_blob.create();                                             // Throws
        ref = new_blob.add(value.data(), value.size(), add_zero_term); // Throws
        Array::set_as_ref(ndx, ref);
        return;
    }
    else if (ref != 0 && value.data() != nullptr) {
        char* header = m_alloc.translate(ref);
        if (Array::get_context_flag_from_header(header)) {
            Array arr(m_alloc);
            arr.init_from_mem(MemRef(header, ref, m_alloc));
            arr.set_parent(this, ndx);
            ref_type new_ref =
                arr.blob_replace(0, arr.blob_size(), value.data(), value.size(), add_zero_term); // Throws
            if (new_ref != ref) {
                Array::set_as_ref(ndx, new_ref);
            }
        }
        else {
            ArrayBlob blob(m_alloc);
            blob.init_from_mem(MemRef(header, ref, m_alloc));
            blob.set_parent(this, ndx);
            ref_type new_ref = blob.replace(0, blob.blob_size(), value.data(), value.size(), add_zero_term); // Throws
            if (new_ref != ref) {
                Array::set_as_ref(ndx, new_ref);
            }
        }
        return;
    }
    else if (ref != 0 && value.is_null()) {
        Array::destroy_deep(ref, get_alloc());
        Array::set(ndx, 0);
        return;
    }
    REALM_ASSERT(false);
}


void ArrayBigBlobs::insert(size_t ndx, BinaryData value, bool add_zero_term)
{
    REALM_ASSERT_3(ndx, <=, size());
    REALM_ASSERT_7(value.size(), ==, 0, ||, value.data(), !=, 0);

    if (value.is_null()) {
        Array::insert(ndx, 0); // Throws
    }
    else {
        ArrayBlob new_blob(m_alloc);
        new_blob.create();                                                      // Throws
        ref_type ref = new_blob.add(value.data(), value.size(), add_zero_term); // Throws

        Array::insert(ndx, int64_t(ref)); // Throws
    }
}


size_t ArrayBigBlobs::count(BinaryData value, bool is_string, size_t begin, size_t end) const noexcept
{
    size_t num_matches = 0;

    size_t begin_2 = begin;
    for (;;) {
        size_t ndx = find_first(value, is_string, begin_2, end);
        if (ndx == not_found)
            break;
        ++num_matches;
        begin_2 = ndx + 1;
    }

    return num_matches;
}


size_t ArrayBigBlobs::find_first(BinaryData value, bool is_string, size_t begin, size_t end) const noexcept
{
    if (end == npos)
        end = m_size;
    REALM_ASSERT_11(begin, <=, m_size, &&, end, <=, m_size, &&, begin, <=, end);

    // When strings are stored as blobs, they are always zero-terminated
    // but the value we get as input might not be.
    size_t value_size = value.size();
    size_t full_size = is_string ? value_size + 1 : value_size;

    if (value.is_null()) {
        for (size_t i = begin; i != end; ++i) {
            ref_type ref = get_as_ref(i);
            if (ref == 0)
                return i;
        }
    }
    else {
        for (size_t i = begin; i != end; ++i) {
            ref_type ref = get_as_ref(i);
            if (ref) {
                const char* blob_header = get_alloc().translate(ref);
                size_t sz = get_size_from_header(blob_header);
                if (sz == full_size) {
                    const char* blob_value = ArrayBlob::get(blob_header, 0);
                    if (std::equal(blob_value, blob_value + value_size, value.data()))
                        return i;
                }
            }
        }
    }

    return not_found;
}


void ArrayBigBlobs::find_all(IntegerColumn& result, BinaryData value, bool is_string, size_t add_offset, size_t begin,
                             size_t end)
{
    size_t begin_2 = begin;
    for (;;) {
        size_t ndx = find_first(value, is_string, begin_2, end);
        if (ndx == not_found)
            break;
        result.add(add_offset + ndx); // Throws
        begin_2 = ndx + 1;
    }
}


#ifdef REALM_DEBUG // LCOV_EXCL_START ignore debug functions

void ArrayBigBlobs::verify() const
{
    REALM_ASSERT(has_refs());
    for (size_t i = 0; i < size(); ++i) {
        ref_type blob_ref = Array::get_as_ref(i);
        // 0 is used to indicate realm::null()
        if (blob_ref != 0) {
            ArrayBlob blob(m_alloc);
            blob.init_from_ref(blob_ref);
            blob.verify();
        }
    }
}


void ArrayBigBlobs::to_dot(std::ostream& out, bool, StringData title) const
{
    ref_type ref = get_ref();

    out << "subgraph cluster_binary" << ref << " {" << std::endl;
    out << " label = \"ArrayBinary";
    if (title.size() != 0)
        out << "\\n'" << title << "'";
    out << "\";" << std::endl;

    Array::to_dot(out, "big_blobs_leaf");

    for (size_t i = 0; i < size(); ++i) {
        ref_type blob_ref = Array::get_as_ref(i);
        ArrayBlob blob(m_alloc);
        blob.init_from_ref(blob_ref);
        blob.set_parent(const_cast<ArrayBigBlobs*>(this), i);
        blob.to_dot(out);
    }

    out << "}" << std::endl;

    to_dot_parent_edge(out);
}

#endif // LCOV_EXCL_STOP ignore debug functions
