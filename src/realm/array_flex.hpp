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

#ifndef REALM_ARRAY_FLEX_HPP
#define REALM_ARRAY_FLEX_HPP

#include <realm/array_encode.hpp>
#include <realm/node_header.hpp>

namespace realm {
//
// Compress array in Flex format
// Decompress array in WTypeBits formats
//
class Array;
class ArrayFlex {
public:
    // encoding/decoding
    void init_array(char* h, uint8_t flags, size_t v_width, size_t ndx_width, size_t v_size, size_t ndx_size) const;
    void copy_data(const Array&, const std::vector<int64_t>&, const std::vector<size_t>&) const;
    std::vector<int64_t> fetch_all_values(const Array& h) const;
    // getters/setters
    int64_t get(const Array&, size_t) const;
    int64_t get(const char*, size_t, size_t, size_t, size_t, size_t, size_t) const;
    void get_chunk(const Array& h, size_t ndx, int64_t res[8]) const;
    void set_direct(const Array&, size_t, int64_t) const;

    template <typename Cond>
    bool find_all(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const
    {
        // TODO: implement this
        return true;
    }

private:
    static int64_t do_get(uint64_t*, size_t, size_t, size_t, size_t, size_t, size_t);
};
} // namespace realm
#endif // REALM_ARRAY_COMPRESS_HPP
