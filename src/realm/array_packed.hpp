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
    void init_array(char*, uint8_t, size_t, size_t) const;
    void copy_data(const Array&, Array&) const;
    // get or set
    int64_t get(const Array&, size_t) const;
    int64_t get(const char*, size_t, size_t, size_t, size_t) const;
    void get_chunk(const Array&, size_t, int64_t res[8]) const;
    void set_direct(const Array&, size_t, int64_t) const;

    template <typename Cond>
    bool find_all(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;
    int64_t sum(const Array&, size_t, size_t) const;

private:
    int64_t do_get(uint64_t*, size_t, size_t, size_t, size_t) const;
    bool find_all_match(size_t start, size_t end, size_t baseindex, QueryStateBase* state) const;

    template <typename Cond>
    size_t parallel_subword_find(const Array&, int64_t, size_t, size_t) const;
};
} // namespace realm

#endif // REALM_ARRAY_PACKED_HPP
