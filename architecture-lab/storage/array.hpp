/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
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

#ifndef __ARRAY_HPP__
#define __ARRAY_HPP__

#include <cassert>
#include <stdexcept>

#include "refs.hpp"
#include "memory.hpp"


/*
  Array encoding: The least 11 bits of a ref is used to encode element size
  and capacity. 
  * Bits 2-0: element size (1,2,4 bits, 1,2,4,8 bytes)
  * Bits 10-3: size (0..255, both included)
  * Bits 63-11: Data. For refs, which always have lowest 3 bits 0. These are not stored
  * If the element size and capacity allows, data are stored inline
  */

// describes the encoding/decoding of single elements
template<typename T> struct Encoding {
    static T get_from_quad(uint64_t data, int sz, int index);
    static int get_encoding_size(T);
    static bool is_null(T value) { return value == 0; }
    static uint64_t set_in_quad(uint64_t quad, int esz, int index, T value);
    static uint64_t encode(T value) { return value; }
    static T decode(uint64_t enc) { return enc; }
    static void commit_from_quad(Memory& mem, uint64_t& quad) {};
};

template<typename T>
struct _Array {
    uint64_t data;
    // helper methods:
    inline bool is_all_zero() { return data == 0; }
    inline int get_esz() { return data & 0x7; }
    inline unsigned int get_cap() { return (data >> 3) & 0xFF; }
    inline uint64_t get_data() { return data >> 11; }
    void set_data(uint64_t val) { uint64_t l = data & 0x7FF; data = l | (val << 11); }
    static int bits_required(int esz, int cap) { uint64_t sz = cap; return sz << esz; }
    int quads_required() { return (63 + bits_required(get_esz(), get_cap())) / 64; }
    static inline bool can_be_inlined(int esz, int cap) {
        return false; // don't use inlining for now, current implementation is inefficient
        switch(esz) {
            case 0: return cap <= 52;
            case 1: return cap <= 26;
            case 2: return cap <= 13;
            case 3: return cap <= 6;
            case 4: return cap <= 3;
            case 5: return cap <= 1;
            default: return false;
        }
    }
    bool inline is_inlined() { return can_be_inlined(get_esz(), get_cap()); }
    void inline init(int esz, int cap, uint64_t value) { data = esz | (cap << 3) | (value << 11); }
    _Array() { data = 0; }

    Ref<uint64_t> get_ref() { Ref<uint64_t> res; res.r = get_data() << 3; return res; }
    void set_ref(Ref<uint64_t> ref) { set_data(ref.r >> 3); }

    static _Array<T> commit(Memory& mem, _Array<T> from);
    void alloc(Memory& mem) { 
        if (!is_inlined()) {
            uint64_t* dummy;
            set_ref(mem.alloc<uint64_t>(dummy, 8 * quads_required()));
        }
    }
    void free(Memory& mem) {
        if (!is_inlined()) {
            // TODO: Handle freeing of individual elements of lists!
            mem.free(get_ref(), 8 * quads_required());
            set_data(0);
        }
    }
    bool is_writable(Memory& mem) {
        return mem.is_writable(get_ref());
    }
    inline T get(Memory& mem, int index);
    void set(Memory& mem, int index, T value, int capacity);
    void set_unchecked(Memory& mem, int index, T value);
};

static inline int get_shift_in_quad(int sz, int index) {
    int index_sz_mask = 0x3F;
    return (index & (index_sz_mask >> sz)) << sz;
}

static inline int get_quad_index(int sz, int index) {
    return index >> (6-sz);
}

// get a mask covering an object of 'sz'
static inline uint64_t get_mask(int sz) {
    if (sz == 6) return 0xFFFFFFFFULL;
    uint64_t res = (1ULL << (1 << sz)) - 1;
    return res;
}

// get mask matching sign bit
static inline uint64_t get_sign_mask(int sz) {
    return 1ULL << ((1 << sz) - 1);
}

template<>
inline uint64_t Encoding<uint64_t>::get_from_quad(uint64_t data, int sz, int index) {
    if (sz == 6) return data;
    uint64_t shifted = data >> get_shift_in_quad(sz, index);
    return shifted & get_mask(sz);
}

// assume 2-cpl representation
template<>
inline int64_t Encoding<int64_t>::get_from_quad(uint64_t data, int sz, int index) {
    if (sz == 6) return (int64_t) data;
    uint64_t shifted = data >> get_shift_in_quad(sz, index);
    uint64_t extended_sign = 0 - ((shifted & get_sign_mask(sz)) << 1);
    extended_sign |= (shifted & get_mask(sz));
    return (int64_t) extended_sign;
}

template<>
inline float Encoding<float>::get_from_quad(uint64_t data, int sz, int index) {
    assert(sz == 5);
    uint32_t v = Encoding<uint64_t>::get_from_quad(data, sz, index);
    return static_cast<float>(v);
}

template<>
inline double Encoding<double>::get_from_quad(uint64_t data, int sz, int index) {
    assert(sz == 6);
    uint64_t v = Encoding<uint64_t>::get_from_quad(data, sz, index);
    return static_cast<double>(v);
}

template<>
inline char Encoding<char>::get_from_quad(uint64_t data, int sz, int index) {
    assert(sz <= 3);
    return Encoding<uint64_t>::get_from_quad(data, sz, index);
}

template<typename T>
inline T _Array<T>::get(Memory& mem, int index) {
    if (is_all_zero()) return T(0);
    if (is_inlined()) {
        return Encoding<T>::get_from_quad(get_data(), get_esz(), index);
    }
    uint64_t idx = get_quad_index(get_esz(), index);
    uint64_t* array = mem.txl(get_ref());
    uint64_t quad = array[idx];
    return Encoding<T>::get_from_quad(quad, get_esz(), index);
}

template<typename T>
struct _List { //: public _Array<T> { FIXME: better to use inheritance?
    _Array<T> array;
    _List(int j) { assert(j==0); }
    uint64_t get_size() { return array.get_cap(); }
    void set_size(Memory& mem, uint64_t size);
    T get(Memory& mem, uint64_t index) { return array.get(mem, index); }
    void set(Memory& mem, uint64_t index, T value) { array.set(mem, index, value, 0); }
};



// Specialization for all arrays with lists as elements: a list always requires a full quad.
template<typename T> struct Encoding<_List<T>> {
    static _List<T> get_from_quad(uint64_t data, int sz, int index) { return decode(data); }
    static int get_encoding_size(_List<T>) { return 6; }
    static bool is_null(_List<T> value) { return value.array.data == 0; }
    static uint64_t set_in_quad(uint64_t quad, int esz, int index, _List<T> value) { return encode(value); }
    static uint64_t encode(_List<T> value) { return value.array.data; }
    static _List<T> decode(uint64_t enc) { _List<T> res(0); res.array.data = enc; return res; }
    static void commit_from_quad(Memory& mem, uint64_t& quad);
};

#endif

