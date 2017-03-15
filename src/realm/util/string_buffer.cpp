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

#include <new>
#include <limits>
#include <algorithm>
#include <stdexcept>

#include <realm/util/safe_int_ops.hpp>
#include <realm/util/string_buffer.hpp>
#include <realm/utilities.hpp>

using namespace realm;
using namespace realm::util;


void StringBuffer::append(const char* append_data, size_t append_data_size)
{
    size_t new_size = m_size;
    if (int_add_with_overflow_detect(new_size, append_data_size))
        throw util::BufferSizeOverflow();
    reserve(new_size); // Throws
    realm::safe_copy_n(append_data, append_data_size, m_buffer.data() + m_size);
    m_size = new_size;
    m_buffer[new_size] = 0; // Add zero termination
}


void StringBuffer::reallocate(size_t min_capacity)
{
    size_t min_capacity_2 = min_capacity;
    // Make space for zero termination
    if (int_add_with_overflow_detect(min_capacity_2, 1))
        throw util::BufferSizeOverflow();
    size_t new_capacity = m_buffer.size();
    if (int_multiply_with_overflow_detect(new_capacity, 2))
        new_capacity = std::numeric_limits<size_t>::max(); // LCOV_EXCL_LINE
    if (new_capacity < min_capacity_2)
        new_capacity = min_capacity_2;
    m_buffer.resize(new_capacity, 0, m_size, 0); // Throws
}
