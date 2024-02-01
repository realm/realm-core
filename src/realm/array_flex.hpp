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

namespace realm {
//
// Compress array in Flex format
// Decompress array in WTypeBits formats
//
class Array;
class ArrayFlex : public ArrayEncode {
public:
    explicit ArrayFlex() = default;
    virtual ~ArrayFlex() = default;

    static bool encode(const Array&, Array&, size_t, const std::vector<int64_t>&, const std::vector<size_t>&, size_t,
                       size_t);

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
    // decode array methods
    std::vector<int64_t> fetch_signed_values_from_encoded_array(const Array&, size_t, size_t, size_t, size_t) const;
    void restore_array(Array&, const std::vector<int64_t>&) const;


    static bool get_encode_info(const Array&, size_t&, size_t&, size_t&, size_t&);
    static void setup_array_flex_format(const Array&, Array&, size_t, const std::vector<int64_t>&,
                                        const std::vector<size_t>&, size_t, size_t);
    static void copy_into_flex_array(Array&, const std::vector<int64_t>&, const std::vector<size_t>&);
};
} // namespace realm
#endif // REALM_ARRAY_COMPRESS_HPP
