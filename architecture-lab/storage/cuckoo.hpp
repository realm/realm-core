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

#ifndef __CUCKOO_HPP__
#define __CUCKOO_HPP__

#include "memory.hpp"
#include "tree.hpp"
#include "payload.hpp"
#include "object.hpp"

struct TreeLeaf;

struct _Cuckoo {
    _TreeTop<TreeLeaf> primary_tree;
    _TreeTop<TreeLeaf> secondary_tree; // not used atm

    // update internals as we've been copied into file:
    void copied_to_file(Memory& mem, PayloadMgr& pm);

    // get a ref to any payload and the row index into it
    bool find(Memory& mem, uint64_t key, Ref<DynType>& payload, int& index, uint8_t& size);

    void init();

    // get a null ref back if key could not be found. If found, cow the path to the payload
    // and return a pointer allowing for later update of the payload ref.
    bool find_and_cow_path(Memory& mem, PayloadMgr& pm, uint64_t key, 
                           Ref<DynType>& payload, int& index, uint8_t& size);

    void insert(Memory& mem, uint64_t key, PayloadMgr& pm);

    bool first_access(Memory& mem, ObjectIterator& oc);
private:
    void rehash_tree(Memory& mem, _TreeTop<TreeLeaf>& tree, PayloadMgr& pm);
    void grow_tree(Memory& mem, PayloadMgr& pm);

};


#endif
