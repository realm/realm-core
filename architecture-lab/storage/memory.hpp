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

/* Memory manager
 */

#ifndef __MEMORY_HPP__
#define __MEMORY_HPP__

#include <cstddef>
#include <vector>

#include "refs.hpp"

class Memory {
public:
    static const int chunk_shift = 20;
    static const int num_chunks = 64*1024;
    static const uint64_t chunk_size = 1ULL << chunk_shift;
    static const uint64_t chunk_offset_mask = chunk_size - 1;
    static const int num_size_bins = 500;
    Memory();
    ~Memory();
    void reset_freelists();

    template <typename T> T* txl(Ref<T> ref) const {
        return internal_txl<T>(ref.r);
    }

    template <typename T> T* in_file_txl(Ref<T> ref) const {
        return reinterpret_cast<T*>(internal_in_file_txl(ref.r));
    }

    template <typename T>
    inline bool is_writable(Ref<T> some_ref) const { return some_ref.r >= scratch_ref_start; }

    template <typename T>
    inline bool is_valid(Ref<T> some_ref) const { return some_ref.r != 0; }

    template <typename T>
    Ref<T> alloc(T*& ptr, size_t real_size = sizeof(T)) { 
        Ref<T> res; res.r = internal_alloc(real_size); ptr = txl(res); 
        return res; 
    }

    template <typename T>
    void free(Ref<T> ref, size_t real_size = sizeof(T)) { 
        internal_free(ref.r, real_size); 
    }

    template <typename T>
    Ref<T> alloc_in_file(T*& ptr, size_t real_size = sizeof(T)) {
        void* p;
        Ref<T> res; res.r = internal_alloc_in_file(p, real_size);
        ptr = reinterpret_cast<T*>(p);
        return res;
    }

    uint64_t get_footprint() { return last_valid_ref - scratch_ref_start; }
    uint64_t get_recycled() { return recycled; }
    uint64_t get_freed() { return freed; }
    void open_for_write(int fd, uint64_t in_file_allocation_start_ref);
    void finish_writing(uint64_t& file_size, uint64_t& in_file_allocation_point);
    void prepare_mapping(int fd, uint64_t file_size);
private:
    uint64_t internal_alloc_in_file(void*& ptr, size_t real_size);
    uint64_t internal_alloc(size_t real_size);
    void internal_free(uint64_t ref, size_t real_size);
    template <typename T> T* internal_txl(uint64_t ref) const {
        return reinterpret_cast<T*>(txl_table[ref >> chunk_shift] + (ref & chunk_offset_mask));
    }

    char* null_page;
    char* txl_table[num_chunks];
    uint64_t free_lists[num_size_bins];

    const uint64_t first_valid_ref = chunk_size;
    // refs inside chunk from which allocation takes place.
    // all chunks below memory_ref_start are read-only mappings of the file,
    // all chunks above or equal to memory_ref_start are read-write scratch space
    uint64_t scratch_ref_start = first_valid_ref;
    uint64_t allocation_ref = scratch_ref_start;
    uint64_t last_valid_ref = scratch_ref_start;
    uint64_t recycled = 0;
    uint64_t freed = 0;

    int fd = -1;
    uint64_t file_size = 0;
    uint64_t file_alloc_start = 0;
    uint64_t file_alloc_ref = 0;
    uint64_t file_alloc_limit = 0;
    char* file_alloc_base_ptr = 0;

    std::vector<char*> write_maps;
};

#endif
