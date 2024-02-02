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

#ifndef REALM_ARRAY_ENCODE_HPP
#define REALM_ARRAY_ENCODE_HPP

#include <realm/alloc.hpp>

namespace realm {
//
// This class represents the basic interface that every compressed array must implement.
//
class Array;
class ArrayEncode {
public:
    // TODO move all of this in some other class that only dispatches
    static bool encode(const Array&, Array&);
    static int64_t get(const char*, size_t);
    static int64_t find_first(const char*, size_t);
    static size_t size(const char*);
    static bool is_packed(const char*);
    static void set_direct(char* data, size_t w, size_t ndx, int64_t v);
    static size_t find(const Array&, int64_t);

    // this is the interface that every encoding array must adhere to
    // Generic interface for an encoding Array.
    explicit ArrayEncode() = default;
    virtual ~ArrayEncode() = default;
    // encode/decode arrays (encoding happens during the Array::write machinery, decoding happens during COW and
    // before to allocate) virtual bool encode(const Array&, Array&) const = 0;
    virtual bool decode(Array&) = 0;
    // getting and setting values in encoded arrays
    virtual int64_t get(const Array&, size_t) const = 0;
    virtual void get_chunk(const Array&, size_t ndx, int64_t res[8]) const = 0;
    virtual void set_direct(const Array&, size_t, int64_t) const = 0;
    // query interface
    virtual int64_t sum(const Array&, size_t start, size_t end) const = 0;
};

} // namespace realm
#endif // REALM_ARRAY_ENCODE_HPP
