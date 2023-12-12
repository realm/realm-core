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
    bool encode(const Array&, Array&) const final override;
    bool decode(Array&) final override;
    bool is_encoded(const Array&) const final override;
    size_t size(const Array&) const final override;
    int64_t get(const Array&, size_t) const final override;
    size_t find_first(const Array&, int64_t value) const final override;
    // this is supposed to be used only if the underlying type is uint.
    uint64_t get_unsigned(const Array&, size_t, size_t&) const final override;
    // these methods are used by ArrayUnsigned, and have a huge impact on how fast we traverse the ClusterTree
    size_t lower_bound(const Array&, uint64_t) const final override;
    size_t upper_bound(const Array&, uint64_t) const final override;

    static int64_t get(const char* header, size_t ndx);
    static uint64_t get_unsigned(const char* header, size_t ndx, size_t&);

private:
    // read info about the encoded array from header
    static bool get_encode_info(const char* header, size_t& value_width, size_t& index_width, size_t& value_size,
                                size_t& index_size);
    // encode array methods
    bool try_encode(const Array&, Array&, std::vector<int64_t>&, std::vector<size_t>&) const;
    void arrange_data_in_flex_format(const Array&, std::vector<int64_t>&, std::vector<size_t>&) const;
    bool check_gain(const Array&, std::vector<int64_t>&, std::vector<size_t>&, int&, int&) const;
    void setup_array_in_flex_format(const Array&, Array&, std::vector<int64_t>&, std::vector<size_t>&, int,
                                    int) const;
    void copy_into_encoded_array(Array&, std::vector<int64_t>&, std::vector<size_t>&) const;
    // decode array methods
    std::vector<int64_t> fetch_signed_values_from_encoded_array(const Array&, size_t, size_t, size_t, size_t) const;
    std::vector<uint64_t> fetch_unsigned_values_from_encoded_array(const Array&, size_t, size_t, size_t,
                                                                   size_t) const;
    void restore_array(Array&, const std::vector<int64_t>&) const;
};
} // namespace realm
#endif // REALM_ARRAY_COMPRESS_HPP
