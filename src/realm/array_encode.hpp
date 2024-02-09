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

#include <cstdint>
#include <cstddef>
#include <vector>

namespace realm {

class Array;
class ArrayEncode {
public:
    // commit => encode, COW/insert => decode
    bool encode(const Array&, Array&) const;
    bool decode(Array&) const;

    // init from mem B
    size_t size(const char* header);

    // get/set
    int64_t get(const Array&, size_t) const;
    static int64_t get(const char*, size_t);
    void get_chunk(const Array&, size_t ndx, int64_t res[8]) const;
    void set_direct(const Array&, size_t, int64_t) const;

    // query interface
    template <typename F>
    size_t find_first(const Array&, int64_t, size_t, size_t, F) const;
    int64_t sum(const Array&, size_t start, size_t end) const;

private:
    inline bool is_packed(const Array&) const;
    inline bool is_flex(const Array&) const;

    void set(char* data, size_t w, size_t ndx, int64_t v) const;
    size_t flex_encoded_array_size(const std::vector<int64_t>&, const std::vector<size_t>&, size_t&, size_t&) const;
    size_t packed_encoded_array_size(std::vector<int64_t>&, size_t, size_t&) const;
    void try_encode(const Array&, std::vector<int64_t>&, std::vector<size_t>&) const;
    bool always_encode(const Array&, Array&, bool) const; // for testing
};

} // namespace realm
#endif // REALM_ARRAY_ENCODE_HPP
