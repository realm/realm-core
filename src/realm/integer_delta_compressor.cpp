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

#include <realm/integer_delta_compressor.hpp>
#include <realm/node_header.hpp>
#include <realm/array_direct.hpp>

#include <vector>
#include <algorithm>

#ifdef REALM_DEBUG
#include <iostream>
#include <sstream>
#endif

using namespace realm;

void DeltaCompressor::init_header(char* h, uint8_t flags, uint8_t v_width, uint8_t ndx_width, size_t v_size,
                                  size_t ndx_size)
{
    using Encoding = NodeHeader::Encoding;
    ::init_header(h, Encoding::Delta, flags, v_width, ndx_width, v_size, ndx_size);
}

void DeltaCompressor::copy_data(const Array& origin, Array& arr, const std::vector<int64_t>& values)
{
    // this can be boosted a little bit, with and size should be known at this stage.
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(arr.is_attached());
    const auto& compressor = arr.integer_compressor();
    REALM_ASSERT_DEBUG(compressor.get_encoding() == Encoding::Delta);
    const auto v_width = arr.m_width;
    const auto v_size = arr.m_size;
    const auto ndx_width = compressor.ndx_width();

    auto data = (uint64_t*)arr.m_data;
    const auto offset = 2 * size_t(v_width);

    BfIterator it_value{data, 0, v_width, v_width, 0};
    BfIterator it_delta{data, offset, ndx_width, ndx_width, 0};

    auto max_val = values.back();
    auto min_val = values.front();
    it_value.set_value(max_val);
    ++it_value;
    it_value.set_value(min_val);

    for (size_t i = 0; i < v_size; ++i) {
        auto orig_val = origin.get(i);
        if (orig_val == max_val) {
            it_delta.set_value(0);
        }
        else {
            it_delta.set_value(orig_val - min_val + 1);
        }
        ++it_delta;
    }
}
