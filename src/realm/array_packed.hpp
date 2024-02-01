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

namespace realm {

//
// Compress array in Packed format
// Decompress array in WTypeBits formats
//
class Array;
class ArrayPacked : public ArrayEncode {
public:
    explicit ArrayPacked() = default;
    virtual ~ArrayPacked() = default;

    static bool encode(const Array&, Array&, size_t, size_t);

    // compressing/decompressing logic
    // bool encode(const Array&, Array&) const final override;
    bool decode(Array&) final override;

    // basic query support
    int64_t sum(const Array&, size_t start, size_t end) const final override;

    // setters
    void set_direct(const Array&, size_t, int64_t) const final override;

    // getters
    int64_t get(const Array&, size_t) const final override;
    void get_chunk(const Array&, size_t ndx, int64_t res[8]) const final override;

    // static getters based on header (to be used carefully since they assume the array is a B array).
    static int64_t get(const char*, size_t);

private:
    // read info about the encoded array from header
    static bool get_encode_info(const Array&, size_t&, size_t&);
    static void setup_array_packed_format(const Array&, Array&, size_t, size_t);
    static void copy_into_packed_array(const Array&, Array&);

    std::vector<int64_t> fetch_signed_values_from_packed_array(const Array&, size_t, size_t) const;
    void restore_array(Array&, const std::vector<int64_t>&) const;
};


} // namespace realm

#endif // REALM_ARRAY_PACKED_HPP
