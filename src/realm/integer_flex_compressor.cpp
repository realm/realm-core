/*************************************************************************
 *
 * Copyright 2023 Realm Inc.
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

#include <realm/integer_flex_compressor.hpp>
#include <realm/node_header.hpp>
#include <realm/array_direct.hpp>

#include <vector>
#include <algorithm>

#ifdef REALM_DEBUG
#include <iostream>
#include <sstream>
#endif

using namespace realm;

void FlexCompressor::init_array(char* h, uint8_t flags, uint8_t v_width, uint8_t ndx_width, size_t v_size,
                                size_t ndx_size) const
{
    using Encoding = NodeHeader::Encoding;
    init_header(h, Encoding::Flex, flags, v_width, ndx_width, v_size, ndx_size);
}

void FlexCompressor::copy_data(const Array& arr, const std::vector<int64_t>& values,
                               const std::vector<size_t>& indices) const
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(arr.is_attached());
    const auto& compressor = arr.integer_compressor();
    REALM_ASSERT_DEBUG(compressor.get_encoding() == Encoding::Flex);
    const auto v_width = compressor.v_width();
    const auto ndx_width = compressor.ndx_width();
    const auto v_size = values.size();
    const auto data = (uint64_t*)arr.m_data;
    const auto offset = static_cast<size_t>(v_size * v_width);
    BfIterator it_value{data, 0, v_width, v_width, 0};
    BfIterator it_index{data, offset, ndx_width, ndx_width, 0};
    for (size_t i = 0; i < v_size; ++i) {
        it_value.set_value(values[i]);
        REALM_ASSERT_DEBUG(sign_extend_value(v_width, it_value.get_value()) == values[i]);
        ++it_value;
    }
    for (size_t i = 0; i < indices.size(); ++i) {
        REALM_ASSERT_DEBUG(values[indices[i]] ==
                           sign_extend_value(v_width, read_bitfield(data, indices[i] * v_width, v_width)));
        it_index.set_value(indices[i]);
        REALM_ASSERT_DEBUG(indices[i] == it_index.get_value());
        REALM_ASSERT_DEBUG(values[indices[i]] ==
                           sign_extend_value(v_width, read_bitfield(data, indices[i] * v_width, v_width)));
        ++it_index;
    }
}

bool FlexCompressor::find_all_match(size_t start, size_t end, size_t baseindex, QueryStateBase* state)
{
    REALM_ASSERT_DEBUG(state->match_count() < state->limit());
    const auto process = state->limit() - state->match_count();
    const auto end2 = end - start > process ? start + process : end;
    for (; start < end2; start++)
        if (!state->match(start + baseindex))
            return false;
    return true;
}
