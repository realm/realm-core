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

#ifndef REALM_ARRAY_COMPRESS_HPP
#define REALM_ARRAY_COMPRESS_HPP

#include <realm/alloc.hpp>

namespace realm {
//
// This class represents the basic interface that every compressed array must implement.
//
class Array;
class ArrayEncode {
public:
    explicit ArrayEncode() = default;
    virtual ~ArrayEncode() = default;
    virtual bool encode(const Array&, Array&) const = 0;
    virtual bool decode(Array&) = 0;
    virtual bool is_encoded(const Array&) const = 0;
    virtual size_t size(const Array&) const = 0;
    virtual int64_t get(const Array&, size_t) const = 0;
    // this needs to be used carefully, only if you know that the underline data is unsigned.
    virtual uint64_t get_unsigned(const Array&, size_t, size_t&) const = 0;
    // these methods are used by ArrayUnsigned, and have a huge impact on how fast we traverse the ClusterTree
    // Note: These methods are expecting array to be storing only unsigned int values.
    virtual size_t lower_bound(const Array&, uint64_t) const = 0;
    virtual size_t upper_bound(const Array&, uint64_t) const = 0;
    // query mappers
    virtual size_t find_first(const Array&, int64_t value) const = 0;
    virtual int64_t sum(const Array&, size_t start, size_t end) const = 0;
};

} // namespace realm
#endif // REALM_ARRAY_COMPRESS_HPP
