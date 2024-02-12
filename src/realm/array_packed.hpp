/*************************************************************************
 *
 * Copyright 2024 Realm Inc.
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

#ifndef REALM_ARRAY_PACKED_HPP
#define REALM_ARRAY_PACKED_HPP

#include <realm/array_encode.hpp>
#include <realm/node_header.hpp>
#include <realm/array.hpp>

namespace realm {

//
// Compress array in Packed format
// Decompress array in WTypeBits formats
//
class Array;
class ArrayPacked {
public:
    // encoding/decoding
    void init_array(char* h, uint8_t flags, size_t v_width, size_t v_size) const;
    void copy_data(const Array&, Array&) const;
    std::vector<int64_t> fetch_signed_values_from_encoded_array(const Array&) const;
    NodeHeader::Encoding get_encoding() const;
    // get or set
    int64_t get(const Array& arr, size_t ndx) const;
    static int64_t get(const char*, size_t);
    void get_chunk(const Array&, size_t ndx, int64_t res[8]) const;
    void set_direct(const Array&, size_t, int64_t) const;
    // query
    int64_t sum(const Array&, size_t start, size_t end) const;
    //    template <typename F>
    //    size_t find_first(const Array&, int64_t, size_t, size_t, F f);

private:
    static void get_encode_info(const char*, size_t&, size_t&);
    static int64_t do_get(uint64_t*, size_t, size_t, size_t); // do not expose this!
};


} // namespace realm

#endif // REALM_ARRAY_PACKED_HPP
