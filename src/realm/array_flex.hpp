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
    explicit ArrayFlex(Array& array);
    virtual ~ArrayFlex() = default;
    bool encode() const final override;
    bool decode() const final override;
    bool is_encoded() const final override;
    size_t size() const final override;
    int64_t get(size_t) const final override;

private:
    // read info about the encoded array from header
    bool get_encode_info(size_t& value_width, size_t& index_width, size_t& value_size, size_t& index_size) const;

    // encode array methods
    bool try_encode(std::vector<int64_t>&, std::vector<size_t>&) const;
    void do_encode_array(std::vector<int64_t>&, std::vector<size_t>&) const;
    bool check_gain(std::vector<int64_t>&, std::vector<size_t>&, int&, int&) const;
    void setup_array_in_flex_format(std::vector<int64_t>&, std::vector<size_t>&, int, int) const;
    void copy_into_encoded_array(std::vector<int64_t>&, std::vector<size_t>&) const;
    std::vector<int64_t> fetch_values(size_t, size_t, size_t, size_t) const;
    void restore_array(const std::vector<int64_t>&) const;

    Array& m_array;
};
} // namespace realm
#endif // REALM_ARRAY_COMPRESS_HPP
