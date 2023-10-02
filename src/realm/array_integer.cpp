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

#include <vector>
#include <set>

#include <realm/array_integer_tpl.hpp>
#include <realm/impl/destroy_guard.hpp>
#include <realm/column_integer.hpp>

using namespace realm;

Mixed ArrayInteger::get_any(size_t ndx) const
{
    return Mixed(get(ndx));
}

int64_t ArrayInteger::get(size_t ndx) const noexcept
{
    if (is_in_compressed_format()) {
        return get_compressed_value(ndx);
    }
    return Array::get(ndx);
}

void ArrayInteger::set(size_t ndx, int64_t value)
{
    if (is_in_compressed_format())
        decompress();
    Array::set(ndx, value);
}

void ArrayInteger::insert(size_t ndx, int_fast64_t value)
{
    if (is_in_compressed_format())
        decompress();
    Array::insert(ndx, value);
}

void ArrayInteger::add(int_fast64_t value)
{
    if (is_in_compressed_format())
        decompress();
    Array::add(value);
}

void ArrayInteger::move(Array& dst, size_t ndx)
{
    if (is_in_compressed_format())
        decompress();
    Array::move(dst, ndx);
}

size_t ArrayInteger::size() const noexcept
{
    size_t value_width, index_width, value_size, index_size;
    if (get_compressed_header_info(value_width, index_width, value_size, index_size)) {
        return index_size;
    }
    return Array::size();
}

bool ArrayInteger::is_empty() const noexcept
{
    return size() == 0;
}

bool ArrayInteger::try_compress()
{
    std::vector<int64_t> values;
    std::vector<size_t> indices;
    if (!is_in_compressed_format() && try_compress(values, indices)) {
        size_t value_width, index_width, value_size, index_size;
        if (get_compressed_header_info(value_width, index_width, value_size, index_size)) {
            const auto data = (uint64_t*)get_data_from_header(m_compressed_array.get_addr());
            const auto offset = value_size * value_width;
            bf_iterator it_value{data, 0, value_width, value_width, 0};
            bf_iterator it_index{data, offset, index_width, index_width, 0};
            for (size_t i = 0; i < values.size(); ++i) {
                *it_value = values[i];
                ++it_value;
            }
            for (size_t i = 0; i < indices.size(); ++i) {
                *it_index = indices[i];
                ++it_index;
            }
            return true;
        }
    }
    return false;
}

bool ArrayInteger::is_in_compressed_format() const
{
    const auto header = m_compressed_array.get_addr();
    if (header) {
        return get_wtype_from_header(header) >= wtype_extend;
    }
    return false;
}

bool ArrayInteger::get_compressed_header_info(size_t& value_width, size_t& index_width, size_t& value_size,
                                              size_t& index_size) const
{
    if (is_in_compressed_format()) {
        const auto addr = m_compressed_array.get_addr();
        value_size = get_size_A_from_header(addr);
        index_size = get_size_B_from_header(addr);
        value_width = get_width_A_from_header(addr);
        index_width = get_width_B_from_header(addr);
        return true;
    }
    return false;
}


bool ArrayInteger::try_compress(std::vector<int64_t>& values, std::vector<size_t>& indices)
{
    const auto sz = size();
    values.reserve(sz);
    indices.reserve(sz);

    for (size_t i = 0; i < sz; ++i) {
        auto item = get(i);
        values.push_back(item);
        indices.push_back(item);
    }

    std::sort(values.begin(), values.end());
    auto last = std::unique(values.begin(), values.end());
    values.erase(last, values.end());

    for (auto& v : indices) {
        auto pos = std::lower_bound(values.begin(), values.end(), v);
        v = std::distance(values.begin(), pos);
    }

    const auto max_value = std::max_element(values.begin(), values.end());
    const auto max_index = std::max_element(indices.begin(), indices.end());
    const auto compressed_values_size = bit_width(*max_value) * values.size();
    const auto compressed_indices_size = bit_width(*max_index) * indices.size();
    const auto compressed_size = compressed_values_size + compressed_indices_size;
    const auto uncompressed_size = bit_width(*max_value) * size();

    // compress array only if there is some gain
    if (compressed_size < uncompressed_size) {
        m_compressed_array = Array::create_flex_array(Type::type_Normal, false, values.size(), *max_value,
                                                      indices.size(), *max_index, m_alloc);
        // release memory allocated for the array.
        Array::destroy();
        return true;
    }
    return false;
}

bool ArrayInteger::decompress()
{
    size_t value_width, index_width, value_size, index_size;
    if (get_compressed_header_info(value_width, index_width, value_size, index_size)) {
        create(); // recreate the array
        const auto data = (uint64_t*)get_data_from_header(m_compressed_array.get_addr());
        const auto offset = value_size * value_width;
        bf_iterator index_iterator{data, offset, index_width, index_width, 0};
        for (size_t i = 0; i < index_size; ++i) {
            const auto index = (int)index_iterator.get_value();
            const auto value = read_bitfield(data, index * value_width, value_width);
            Array::insert(i, value);
            ++index_iterator;
        }
        // free compressed array
        m_alloc.free_(m_compressed_array);
        m_compressed_array.set_addr(nullptr);
        return true;
    }
    return false;
}

int64_t ArrayInteger::get_compressed_value(size_t ndx) const
{
    size_t value_width, index_width, value_size, index_size;
    if (get_compressed_header_info(value_width, index_width, value_size, index_size)) {

        if (ndx >= index_size)
            return realm::not_found;

        const auto data = (uint64_t*)get_data_from_header(m_compressed_array.get_addr());
        const auto offset = (value_size * value_width) + (ndx * index_width);
        const auto index = read_bitfield(data, offset, index_width);
        return read_bitfield(data, index * value_width, value_width);
    }
    return realm::not_found;
}

Mixed ArrayIntNull::get_any(size_t ndx) const
{
    return Mixed(get(ndx));
}

MemRef ArrayIntNull::create_array(Type type, bool context_flag, size_t size, Allocator& alloc)
{
    // Create an array with null value as the first element
    return Array::create(type, context_flag, wtype_Bits, size + 1, 0, alloc); // Throws
}


void ArrayIntNull::init_from_ref(ref_type ref) noexcept
{
    REALM_ASSERT_DEBUG(ref);
    char* header = m_alloc.translate(ref);
    init_from_mem(MemRef{header, ref, m_alloc});
}

void ArrayIntNull::init_from_mem(MemRef mem) noexcept
{
    Array::init_from_mem(mem);

    // We always have the null value stored at position 0
    REALM_ASSERT(m_size > 0);
}

void ArrayIntNull::init_from_parent() noexcept
{
    init_from_ref(get_ref_from_parent());
}

namespace {
int64_t next_null_candidate(int64_t previous_candidate)
{
    uint64_t x = static_cast<uint64_t>(previous_candidate);
    // Increment by a prime number. This guarantees that we will
    // eventually hit every possible integer in the 2^64 range.
    x += 0xfffffffbULL;
    return int64_t(x);
}
}

int_fast64_t ArrayIntNull::choose_random_null(int64_t incoming) const
{
    // We just need any number -- it could have been `rand()`, but
    // random numbers are hard, and we don't want to risk locking mutices
    // or saving state. The top of the stack should be "random enough".
    int64_t candidate = reinterpret_cast<int64_t>(&candidate);

    while (true) {
        candidate = next_null_candidate(candidate);
        if (candidate == incoming) {
            continue;
        }
        if (can_use_as_null(candidate)) {
            return candidate;
        }
    }
}

bool ArrayIntNull::can_use_as_null(int64_t candidate) const
{
    return find_first(candidate) == npos;
}

void ArrayIntNull::replace_nulls_with(int64_t new_null)
{
    int64_t old_null = null_value();
    Array::set(0, new_null);
    size_t i = 1;
    while (true) {
        size_t found = Array::find_first(old_null, i);
        if (found < Array::size()) {
            Array::set(found, new_null);
            i = found + 1;
        }
        else {
            break;
        }
    }
}


void ArrayIntNull::avoid_null_collision(int64_t value)
{
    if (m_width == 64) {
        if (value == null_value()) {
            int_fast64_t new_null = choose_random_null(value);
            replace_nulls_with(new_null);
        }
    }
    else {
        if (value < m_lbound || value >= m_ubound) {
            size_t new_width = bit_width(value);
            int64_t new_upper_bound = Array::ubound_for_width(new_width);

            // We're using upper bound as magic NULL value, so we have to check
            // explicitly that the incoming value doesn't happen to be the new
            // NULL value. If it is, we upgrade one step further.
            if (new_width < 64 && value == new_upper_bound) {
                new_width = (new_width == 0 ? 1 : new_width * 2);
                new_upper_bound = Array::ubound_for_width(new_width);
            }

            int64_t new_null;
            if (new_width == 64) {
                // Width will be upgraded to 64, so we need to pick a random NULL.
                new_null = choose_random_null(value);
            }
            else {
                new_null = new_upper_bound;
            }

            replace_nulls_with(new_null); // Expands array
        }
    }
}

void ArrayIntNull::find_all(IntegerColumn* result, value_type value, size_t col_offset, size_t begin,
                            size_t end) const
{
    // FIXME: We can't use the fast Array::find_all here, because it would put the wrong indices
    // in the result column. Since find_all may be invoked many times for different leaves in the
    // B+tree with the same result column, we also can't simply adjust indices after finding them
    // (because then the first indices would be adjusted multiple times for each subsequent leaf)

    if (end == npos) {
        end = size();
    }

    for (size_t i = begin; i < end; ++i) {
        if (get(i) == value) {
            result->add(col_offset + i);
        }
    }
}

bool ArrayIntNull::find(int cond, value_type value, size_t start, size_t end, QueryStateBase* state) const
{
    return find_impl(cond, value, start, end, state, nullptr);
}

size_t ArrayIntNull::find_first(value_type value, size_t begin, size_t end) const
{
    return find_first<Equal>(value, begin, end);
}

void ArrayIntNull::get_chunk(size_t ndx, value_type res[8]) const noexcept
{
    // FIXME: Optimize this
    int64_t tmp[8];
    Array::get_chunk(ndx + 1, tmp);
    int64_t null = null_value();
    for (size_t i = 0; i < 8; ++i) {
        res[i] = tmp[i] == null ? util::Optional<int64_t>() : tmp[i];
    }
}

void ArrayIntNull::move(ArrayIntNull& dst, size_t ndx)
{
    size_t sz = size();
    for (size_t i = ndx; i < sz; i++) {
        dst.add(get(i));
    }
    truncate(ndx + 1);
}
