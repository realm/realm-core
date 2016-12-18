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

#include <sys/types.h>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <stdexcept>

#include <cstdint>
#include <iostream>
#include <cassert>

#include "memory.hpp"

struct OutOfMemory {};

size_t bin_to_size(int bin);
int size_to_bin(size_t size);

Memory::Memory() {
    // set up everything to lead to the null_page.
    // encodings of tree_node and tree_leaf must support
    // searching without having to check if they hit the null-page
    // also - all Ref in objects in the null-page must always
    // refer back to the null page
    null_page = new char[chunk_size];
    for (size_t j=0; j<chunk_size; ++j) null_page[j] = 0;
    for (int j=0; j<num_chunks; ++j) txl_table[j] = null_page;
    reset_freelists();
    // internal checks:
    assert(size_to_bin(7) == 0);
    assert(size_to_bin(8) == 1);
    assert(bin_to_size(1) == 8);
    assert(bin_to_size(0) == 0);
}

void Memory::reset_freelists() {
    for (int j=0; j<num_size_bins;++j) free_lists[j] = 0;
    recycled = freed = 0;
}

Memory::~Memory() {
    reset_freelists();
    // unmap both file and scratch space
    for (size_t j = first_valid_ref >> chunk_shift; j < last_valid_ref >> chunk_shift; ++j) {
        munmap(txl_table[j], chunk_size);
    }
    delete null_page;
}

size_t align_to_next(size_t arg, size_t alignment) {
    size_t offset_mask = alignment - 1;
    size_t alignment_mask = ~offset_mask;
    return alignment_mask & (arg + offset_mask);
}

size_t bin_to_size(int bin) {
    if (bin < 64) {
        size_t size = bin;
        return size << 3;
    }
    int shifts = (bin >> 5) -1;
    size_t size = 32 + (bin & 0x1F);
    size <<= shifts;
    size <<= 3;
    return size;
}

int size_to_bin(size_t size) {
    int sz = size >> 3; // get rid of alignment
    int bin = 0;
    // go for ~3% internal fragmentation
    while (sz >= 64) {
        sz = sz >> 1;
        bin = bin + 32;
    }
    bin += sz;
    assert(bin_to_size(bin) <= size);
    assert(size < bin_to_size(bin+1));
    if (sz >= 63) assert(bin_to_size(bin) > 0.95 * bin_to_size(bin+1));
    return bin;
}

uint64_t Memory::internal_alloc(size_t sz) {
    int bin = size_to_bin(sz - 1);
    sz = bin_to_size(bin + 1);
    assert(bin < 500);
    if (free_lists[bin]) {
        uint64_t head = free_lists[bin];
        uint64_t* ptr = internal_txl<uint64_t>(head);
        free_lists[bin] = *ptr;
        recycled += sz;
        //std::cout << "  recycled " << head << std::endl;
        return head;
    }
    if (allocation_ref + sz > last_valid_ref) {
        uint64_t chunk_index = last_valid_ref >> chunk_shift;
        void* addr = mmap(0, chunk_size, PROT_READ | PROT_WRITE, 
                          MAP_PRIVATE | MAP_ANONYMOUS, 0, 0);
        if (addr == nullptr)
            throw OutOfMemory();
        txl_table[chunk_index] = (char*)addr;
        allocation_ref = last_valid_ref;
        last_valid_ref += chunk_size;
    }
    uint64_t res = allocation_ref;
    allocation_ref += sz;
    return res;
}

void Memory::internal_free(uint64_t ref, size_t sz) {
    // ignore null refs:
    if (ref == 0) return;
    // (for now) ignore refs into the read-only part
    if (ref < scratch_ref_start) return;

    // otherwise add to free list
    int bin = size_to_bin(sz - 1);
    sz = bin_to_size(bin + 1);
    freed += sz;
    assert(bin < 500);
    uint64_t* ptr = internal_txl<uint64_t>(ref);
    *ptr = free_lists[bin];
    free_lists[bin] = ref;
    //std::cout << "  freelisted " << ref << std::endl;
}

uint64_t Memory::internal_alloc_in_file(void*&p, size_t real_size) {
    real_size = align_to_next(real_size, 8);
    if (file_alloc_ref + real_size > file_alloc_limit) {
        // save old chunk!
        write_maps.push_back(file_alloc_base_ptr);
        uint64_t new_file_size = file_size + chunk_size;
        int status = ftruncate(fd, new_file_size);
        if (status < 0)
            throw std::runtime_error("failed to extend file for writing");
        file_alloc_base_ptr = 
            reinterpret_cast<char*>( mmap(0, chunk_size, PROT_READ|PROT_WRITE, MAP_SHARED, 
                                          fd, file_size));
        if (file_alloc_base_ptr == MAP_FAILED)
            throw std::runtime_error("failed to mmap file for writing");
        file_size = new_file_size;
        file_alloc_start = file_alloc_limit;
        file_alloc_ref = file_alloc_start;
        file_alloc_limit = file_alloc_start + chunk_size;
    }
    uint64_t res = file_alloc_ref;
    file_alloc_ref += real_size;
    p = file_alloc_base_ptr + (res - file_alloc_start);
    return res;
}

void Memory::open_for_write(int new_fd, uint64_t in_file_allocation_start_ref) {
    fd = new_fd;
    if (in_file_allocation_start_ref == 0)
        in_file_allocation_start_ref = chunk_size;
    file_alloc_ref = in_file_allocation_start_ref;
    file_alloc_start = (file_alloc_ref >> chunk_shift) << chunk_shift;
    file_alloc_limit = file_alloc_start + chunk_size;
    // setup mapping for very first chunk:
    uint64_t new_file_size = file_alloc_limit - chunk_size + 64 * 1024;
    int status = ftruncate(fd, new_file_size);
    if (status < 0)
        throw std::runtime_error("failed to prepare for write of initial chunk");
    file_alloc_base_ptr = 
        reinterpret_cast<char*>( mmap(0, chunk_size, PROT_READ|PROT_WRITE, MAP_SHARED, 
                                      fd, new_file_size - chunk_size));
    if (file_alloc_base_ptr == MAP_FAILED)
        throw std::runtime_error("failed to setup mmap for write of initial chunk");
    file_size = new_file_size;

}

void Memory::finish_writing(uint64_t& _file_size, uint64_t& _in_file_allocation_point) {
    for (auto e: write_maps) {
        msync(e, chunk_size, MS_SYNC);
        munmap(e, chunk_size);
    }
    if (file_alloc_ref != file_alloc_start) {
        msync(file_alloc_base_ptr, file_alloc_ref - file_alloc_start, MS_SYNC);
        munmap(file_alloc_base_ptr, chunk_size);
    }
    _file_size = file_size;
    _in_file_allocation_point = file_alloc_ref;
}

void Memory::prepare_mapping(int new_fd, uint64_t new_file_size) {
    // chunks from scratch_ref_start to last_valid_ref are scratchpad and must be freed:
    for (uint64_t j = scratch_ref_start >> chunk_shift; j < (last_valid_ref >> chunk_shift); j++) {
        munmap(txl_table[j], chunk_size);
        txl_table[j] = 0;
    }
    // all chunks mapping refs from first_valid_ref to scratch_ref_start are already mapped
    file_size = new_file_size;
    uint64_t file_size_as_ref = file_size - 64*1024 + chunk_size;
    uint64_t new_scratch_ref_start = align_to_next(file_size_as_ref, chunk_size);
    if (new_scratch_ref_start > scratch_ref_start) {
        // growing the memory mapping to match file
        for (uint64_t j = scratch_ref_start; j < new_scratch_ref_start; j += chunk_size) {
            uint64_t file_pos = j - chunk_size + 64 * 1024;
            void* p = mmap(0, chunk_size, PROT_READ, MAP_SHARED, fd, file_pos);
            if (p==nullptr)
                throw std::runtime_error("failed to setup mapping for reading file");
            txl_table[j >> chunk_shift] = reinterpret_cast<char*>(p);
        }
        scratch_ref_start = new_scratch_ref_start;
    } else if (new_scratch_ref_start < scratch_ref_start) {
        // shrinking the memory mapping to match file
        assert(!"not possible");
    }
    // prepare scratchpad operation
    allocation_ref = scratch_ref_start;
    last_valid_ref = scratch_ref_start;
}
