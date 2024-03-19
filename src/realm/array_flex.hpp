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

#include <realm/array.hpp>

#include <cstdint>
#include <stddef.h>
#include <vector>


namespace realm {

struct WordTypeValue {};
struct WordTypeIndex {};

//
// Compress array in Flex format
// Decompress array in WTypeBits formats
//
class ArrayFlex {
public:
    // encoding/decoding
    void init_array(char* h, uint8_t flags, size_t v_width, size_t ndx_width, size_t v_size, size_t ndx_size) const;
    void copy_data(const Array&, const std::vector<int64_t>&, const std::vector<size_t>&) const;
    // getters/setters
    int64_t get(const Array&, size_t) const;
    int64_t get(const char*, size_t, const ArrayEncode&) const;
    void get_chunk(const Array& h, size_t ndx, int64_t res[8]) const;
    void set_direct(const Array&, size_t, int64_t) const;

    template <typename Cond>
    inline bool find_all(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;

    int64_t sum(const Array&, size_t, size_t) const;

private:
    int64_t do_get(uint64_t*, size_t, size_t, size_t, size_t, size_t, uint64_t) const;
    bool find_all_match(size_t, size_t, size_t, QueryStateBase*) const;
    
    template<typename Cond>
    inline bool find_linear(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;
    
    template<typename CondVal, typename CondIndex>
    inline bool find_parallel(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;
    
    bool find_eq(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;
    bool find_neq(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;
    bool find_lt(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;
    bool find_gt(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;
};


template<typename Cond>
inline bool ArrayFlex::find_all(const Array& arr, int64_t value, size_t start, size_t end, size_t baseindex, QueryStateBase* state) const
{
    REALM_ASSERT_DEBUG(start <= arr.m_size && (end <= arr.m_size || end == size_t(-1)) && start <= end);
    Cond c;

    if (end == npos)
        end = arr.m_size;

    if (!(arr.m_size > start && start < end))
        return true;

    const auto lbound = arr.m_lbound;
    const auto ubound = arr.m_ubound;

    if (!c.can_match(value, lbound, ubound))
        return true;

    if (c.will_match(value, lbound, ubound)) {
        return find_all_match(start, end, baseindex, state);
    }

    REALM_ASSERT_3(arr.m_width, !=, 0);
    
    if constexpr (std::is_same_v<Equal, Cond>) {
        return find_eq(arr, value, start, end, baseindex, state);
    }
    else if constexpr (std::is_same_v<NotEqual, Cond>) {
        return find_neq(arr, value, start, end, baseindex, state);
    }
    else if constexpr (std::is_same_v<Less, Cond>) {
        return find_lt(arr, value, start, end, baseindex, state);
    }
    else if constexpr (std::is_same_v<Greater, Cond>) {
        return find_gt(arr, value, start, end, baseindex, state);
    }
    return true;
}

template<typename Cond>
inline bool ArrayFlex::find_linear(const Array& arr, int64_t value, size_t start, size_t end, size_t baseindex, QueryStateBase* state) const
{
    const auto cmp = [](int64_t item, int64_t key){
        if constexpr(std::is_same_v<Cond, Equal>)
            return item == key;
        if constexpr(std::is_same_v<Cond, NotEqual>) {
            return item != key;
        }
        if constexpr(std::is_same_v<Cond, Less>) {
            return item < key;
        }
        if constexpr(std::is_same_v<Cond, Greater>) {
            return item > key;
        }
        REALM_UNREACHABLE();
    };
    
    auto data = (uint64_t*)arr.m_data;
    const auto& encoder = arr.get_encoder();
    const auto offset = encoder.width() * encoder.v_size();
    const auto v_width = encoder.width();
    const auto ndx_width = encoder.ndx_width();
    
    bf_iterator ndx_it((uint64_t*)data, offset, ndx_width, ndx_width, start);
    bf_iterator val_it((uint64_t*)data, 0, v_width, v_width, *ndx_it);
    while(start < end) {
        const auto sv = sign_extend_field_by_mask(encoder.width_mask(), *val_it);
        if(cmp(sv, value) && !state->match(start + baseindex))
            return false;
        ++start;
        ++ndx_it;
        val_it.move(*ndx_it);
    }
    return true;
}

template <typename Cond, typename Type = WordTypeValue>
inline uint64_t vector_compare(uint64_t MSBs, uint64_t a, uint64_t b)
{
    if constexpr (std::is_same_v<Cond, Equal>)
        return find_all_fields_EQ(MSBs, a, b);
    if constexpr (std::is_same_v<Cond, NotEqual>)
        return find_all_fields_NE(MSBs, a, b);
    
    if constexpr (std::is_same_v<Cond, Greater>){
        if(std::is_same_v<Type, WordTypeValue>)
            return find_all_fields_signed_GT(MSBs, a, b);
        if(std::is_same_v<Type, WordTypeIndex>)
            return find_all_fields_unsigned_GT(MSBs,a, b);
        REALM_UNREACHABLE();
    }
    if constexpr (std::is_same_v<Cond, GreaterEqual>) {
        if constexpr (std::is_same_v<Type, WordTypeValue>)
            return find_all_fields_signed_GE(MSBs, a, b);
        if constexpr (std::is_same_v<Type, WordTypeIndex>)
            return find_all_fields_unsigned_GE(MSBs, a, b);
        REALM_UNREACHABLE();
    }
    if constexpr (std::is_same_v<Cond, Less>) {
        if constexpr (std::is_same_v<Type, WordTypeValue>)
            return find_all_fields_signed_LT(MSBs, a, b);
        if constexpr (std::is_same_v<Type, WordTypeIndex>)
            return find_all_fields_unsigned_LT(MSBs, a, b);
        REALM_UNREACHABLE();
    }
    if constexpr (std::is_same_v<Cond, LessEqual>) {
        if constexpr (std::is_same_v<Type, WordTypeValue>)
            return find_all_fields_signed_LT(MSBs, a, b);
        if constexpr (std::is_same_v<Type, WordTypeIndex>)
            return find_all_fields_unsigned_LE(MSBs, a, b);
        REALM_UNREACHABLE();
    }
    
}

template<typename CondVal, typename CondIndex>
inline bool ArrayFlex::find_parallel(const Array& arr, int64_t value, size_t start, size_t end, size_t baseindex, QueryStateBase* state) const
{
    const auto& encoder = arr.m_encoder;
    const auto v_width = encoder.width();
    const auto v_size = encoder.v_size();
    const auto ndx_width = encoder.ndx_width();
    const auto offset = v_size * v_width;
    uint64_t* data = (uint64_t*)arr.m_data;
    
    auto MSBs = encoder.msb();
    auto search_vector = populate(v_width, value);
    auto v_start = parallel_subword_find(vector_compare<CondVal>, data, 0, v_width, MSBs, search_vector, 0, v_size);
    if (v_start == v_size)
        return true;
    
    MSBs = encoder.ndx_msb();
    search_vector = populate(ndx_width, v_start);
    while (start < end) {
        start =
        parallel_subword_find(vector_compare<CondIndex, WordTypeIndex>, data, offset, ndx_width, MSBs, search_vector, start, end);
        if (start < end)
            if (!state->match(start + baseindex))
                return false;
        
        ++start;
    }
    return true;
}

} // namespace realm
#endif // REALM_ARRAY_COMPRESS_HPP
