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

#ifndef __DIRECT_MAP_HPP__
#define __DIRECT_MAP_HPP__

// A direct map with *very* fast lookup, but little concern
// for cost of inserts/changes or space consumption

#include "refs.hpp"
#include "memory.hpp"
#include "tree.hpp"

template<typename _TLeaf, typename _TEntry>
struct _DirectMap {

    _TreeTop<_TLeaf> tree;

    // The map was copied to file, so recursively copy all changed leafs to file
    // _TEntry must support 'copied_to_file'
    void copied_to_file(Memory& mem);

    // make entry at index writable (assumes map is already writable):
    // _TEntry must support 'copied_from_file'
    void cow_path(Memory& mem, uint64_t index);

    // entries must be default constructible
    uint64_t insert(Memory& mem);
    void set(Memory& mem, uint64_t key, _TEntry entry);
    _TEntry get(Memory& mem, uint64_t key) const;
    // inplace access:
    _TEntry* get_ref(Memory& mem, uint64_t key);
    bool find(Memory& mem, uint64_t key) const;
    void init(size_t initial_size);
};


#endif

