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

#include "array.hpp"
#include <cassert>

template<>
inline int Encoding<uint64_t>::get_encoding_size(uint64_t data) {
    /*
    if (data < 0x2ULL) return 0;
    if (data < 0x4ULL) return 1;
    if (data < 0x10ULL) return 2;
    */
    if (data < 0x100ULL) return 3;
    if (data < 0x10000ULL) return 4;
    if (data < 0x100000000ULL) return 5;
    return 6;
}

template<>
inline int Encoding<int64_t>::get_encoding_size(int64_t data) {
    if (data < 0) {
        data = ~data; // tricky.
    }
    uint64_t data2 = static_cast<uint64_t>(data);
    return Encoding<uint64_t>::get_encoding_size(data2 << 1);
}

template<>
inline int Encoding<float>::get_encoding_size(float) {
    return 5;
}

template<>
inline int Encoding<double>::get_encoding_size(double) {
    return 6;
}

template<>
inline int Encoding<char>::get_encoding_size(char) {
    return 3;
}

template<typename T>
_Array<T> _Array<T>::commit(Memory& mem, _Array<T> from) {
    if (from.is_inlined()) return from;
    Ref<uint64_t> from_ref = from.get_ref();
    if (mem.is_writable(from_ref)) {
        int quads = from.quads_required();
        uint64_t* to_ptr;
        Ref<uint64_t> to = mem.alloc_in_file<uint64_t>(to_ptr, 8 * quads);
        uint64_t* from_ptr = mem.txl(from_ref);
        for (int j=0; j<quads; ++j) {
            to_ptr[j] = from_ptr[j];
            Encoding<T>::commit_from_quad(mem, to_ptr[j]);
        }
        _Array<T> result = from;
        mem.free(from_ref);
        result.set_ref(to);
        return result;
    }
    return from;
}

template<typename T>
void Encoding<_List<T>>::commit_from_quad(Memory& mem, uint64_t& quad) {
    _List<T> list = decode(quad);
    list.array = _Array<T>::commit(mem, list.array);
    quad = encode(list);
}


template<typename T>
void ensure_storage(Memory& mem, _Array<T>& a, int index, int e_sz, int capacity) {
    // make room for non-zero value of e_sz at index. Also, as this
    // always happen in preparation for a later write, make sure the
    // array is writable.
    int old_cap = a.get_cap();
    int new_cap = old_cap;
    if (index >= new_cap)
        new_cap = index + 1;
    _Array<T> b(a);
    // check for early out:
    if (e_sz <= b.get_esz() && index < old_cap) {
        if (a.is_inlined()) return;
        if (a.is_writable(mem)) return;
    }
    if (e_sz < b.get_esz()) 
        e_sz = b.get_esz();
    if (!_Array<T>::can_be_inlined(e_sz, new_cap)) {
        // TODO:
        // this is a fine optimization for simple arrays, but it is wrong
        // for lists....instead, a different path for writing into the arrays
        // in the top of the cluster should be provided, where the true capacity
        // could be propagated from the Leaf of the Cuckoo tree.... this cannot
        // be done easily, however, before the Leaf and the Cluster is fused
        // into one:
        // new_cap = (new_cap + 15) & 0x1F0; // align
        if (capacity > new_cap)
            new_cap = capacity;
    }
    assert(new_cap <= 255);
    a.init(e_sz, new_cap, 0);
    if (!a.is_inlined()) {
        a.alloc(mem);
    }
    for (int j=0; j<old_cap; ++j) {
        T tmp = b.get(mem, j);
        a.set_unchecked(mem, j, tmp);
    }
    for (int j=old_cap; j < new_cap; ++j) {
        a.set_unchecked(mem, j, 0);
    }
    if (!b.is_inlined()) {
        b.free(mem);
    }
}

template<typename T>
uint64_t Encoding<T>::set_in_quad(uint64_t quad, int esz, int index, T value) {
    if (esz == 6) return value;
    uint64_t mask = get_mask(esz);
    mask <<= get_shift_in_quad(esz, index);
    uint64_t inv_mask = ~mask;
    uint64_t v = value;
    v <<= get_shift_in_quad(esz, index);
    quad &= inv_mask;
    quad |= v;
    return quad;
}

template<>
uint64_t Encoding<float>::set_in_quad(uint64_t quad, int esz, int index, float value) {
    assert(esz == 5);
    uint32_t v = static_cast<uint32_t>(value);
    return Encoding<uint64_t>::set_in_quad(quad, esz, index, v);
}

template<>
uint64_t Encoding<double>::set_in_quad(uint64_t quad, int esz, int index, double value) {
    assert(esz == 6);
    uint32_t v = static_cast<uint64_t>(value);
    return Encoding<uint64_t>::set_in_quad(quad, esz, index, v);
}

template<typename T>
void _Array<T>::set_unchecked(Memory& mem, int index, T value) {
    int e_sz = get_esz();
    if (is_inlined()) {
        uint64_t q = Encoding<T>::set_in_quad(get_data(), e_sz, index, value);
        set_data(q);
    } else {
        uint64_t idx = get_quad_index(e_sz, index);
        uint64_t* array = mem.txl(get_ref());
        uint64_t quad = array[idx];
        quad = Encoding<T>::set_in_quad(quad, e_sz, index, value);
        array[idx] = quad;
    }
}

template<typename T>
void _Array<T>::set(Memory& mem, int index, T value, int capacity) {
    // extend storage if necessary before performing write
    if (Encoding<T>::is_null(value) && is_all_zero()) return;
    int e_sz = Encoding<T>::get_encoding_size(value);
    ensure_storage(mem, *this, index, e_sz, capacity);
    set_unchecked(mem, index, value);
}

template<typename T>
void _List<T>::set_size(Memory& mem, uint64_t size) {
    if (size < array.get_cap()) {
        // TODO truncate
        throw std::runtime_error("truncating a list is unimplemented");
    }
    else if (size > array.get_cap()) {
        ensure_storage(mem, array, size-1, array.get_esz(), 0);
    }
}

// explicit instantiation
template class _Array<uint64_t>;
template class _Array<int64_t>;
template class _Array<float>;
template class _Array<double>;
template class _Array<char>;
template class _Array<_List<uint64_t>>;
template class _Array<_List<int64_t>>;
template class _Array<_List<float>>;
template class _Array<_List<double>>;
template class _Array<_List<char>>;

template class _List<uint64_t>;
template class _List<int64_t>;
template class _List<float>;
template class _List<double>;
template class _List<char>;
