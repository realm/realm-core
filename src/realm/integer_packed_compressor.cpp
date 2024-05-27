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

#include <realm/integer_packed_compressor.hpp>
#include <realm/integer_compressor.hpp>
#include <realm/node_header.hpp>
#include <realm/array_direct.hpp>
#include <realm/array.hpp>

#include <vector>
#include <algorithm>

#ifdef REALM_DEBUG
#include <iostream>
#include <sstream>
#endif

using namespace realm;

void PackedCompressor::init_array(char* h, uint8_t flags, uint8_t v_width, size_t v_size) const
{
    using Encoding = NodeHeader::Encoding;
    init_header((char*)h, Encoding::Packed, flags, static_cast<uint8_t>(v_width), v_size);
}

void PackedCompressor::copy_data(const Array& origin, Array& arr) const
{
    // this can be boosted a little bit, with and size should be known at this stage.
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(arr.is_attached());
    REALM_ASSERT_DEBUG(arr.integer_compressor().get_encoding() == Encoding::Packed);
    // we don't need to access the header, init from mem must have been called
    const auto v_width = arr.m_width;
    const auto v_size = arr.m_size;
    auto data = (uint64_t*)arr.m_data;
    BfIterator it_value{data, 0, v_width, v_width, 0};
    for (size_t i = 0; i < v_size; ++i) {
        it_value.set_value(origin.get(i));
        REALM_ASSERT_DEBUG(sign_extend_value(v_width, it_value.get_value()) == origin.get(i));
        ++it_value;
    }
}

bool PackedCompressor::find_all_match(size_t start, size_t end, size_t baseindex, QueryStateBase* state)
{
    REALM_ASSERT_DEBUG(state->match_count() < state->limit());
    const auto process = state->limit() - state->match_count();
    const auto end2 = end - start > process ? start + process : end;
    for (; start < end2; start++)
        if (!state->match(start + baseindex))
            return false;
    return true;
}
