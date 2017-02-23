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

#include <iostream>
#include <cassert>

#include "payload.hpp"
#include "cuckoo.hpp"
#include "hash.hpp"


void _Cuckoo::init() {
    primary_tree.init(256);
    secondary_tree.init(0);
}

struct CondensationEntry { uint8_t idx; uint8_t quick_key; };

struct TreeLeaf {
    uint16_t sz;
    uint16_t capacity;
    uint32_t reserved;
    CondensationEntry condenser[256];
    Ref<DynType> payload;
    uint64_t keys[256]; // <-- must come last
};


size_t get_leaf_size(int sz) {
    size_t res = sizeof(TreeLeaf) + (sz - 256) * sizeof(uint64_t);
    assert(res > 0);
    assert(res < 10000);
    return res;
}

void clone_leaf(TreeLeaf* from, TreeLeaf* to, int to_capacity) {
    //std::cout << " - clone_leaf" << std::endl;
    to->sz = from->sz;
    to->payload = from->payload;
    to->capacity = to_capacity;
    for (int j=0; j<256; ++j) to->condenser[j] = from->condenser[j];
    for (int j=0; j<from->sz; ++j) to->keys[j] = from->keys[j];
    for (int j=from->sz; j<to_capacity; ++j) to->keys[j] = 0;
}

struct CuckooLeafCommitter : public _TreeTop::LeafCommitter {
    virtual Ref<DynType> commit(Ref<DynType> from);
    Memory& mem;
    PayloadMgr& pmgr;
    CuckooLeafCommitter(Memory& mem, PayloadMgr& pmgr) : mem(mem), pmgr(pmgr) {}
};

Ref<DynType> CuckooLeafCommitter::commit(Ref<DynType> from) {
    if (is_null(from)) return from;
    if (mem.is_writable(from)) {
        TreeLeaf* from_ptr = mem.txl(from.as<TreeLeaf>());
        TreeLeaf* to_ptr;
        Ref<TreeLeaf> to = mem.alloc_in_file<TreeLeaf>(to_ptr, get_leaf_size(from_ptr->sz));
        clone_leaf(from_ptr, to_ptr, from_ptr->sz);
        to_ptr->payload = pmgr.commit(to_ptr->payload);
        mem.free(from, get_leaf_size(from_ptr->capacity));
        return to;
    }
    return from;
}

void _Cuckoo::copied_to_file(Memory& mem, PayloadMgr& pmgr) {
    CuckooLeafCommitter cmt(mem, pmgr);
    primary_tree.copied_to_file(mem, cmt);
    secondary_tree.copied_to_file(mem, cmt);
}


// return index with matching key, or -1 if no match found
int find_in_leaf(Memory& mem, TreeLeaf* leaf_ptr, uint64_t hash, uint64_t key) {
    int subhash = hash & 0xFF; // cut off all above one byte
    int subhash_limit = (subhash + 4) & 0xFF;
    key >>= 1; // shift out hash indicator prior to key comparions
    while (subhash != subhash_limit) {
        uint8_t idx = leaf_ptr->condenser[subhash].idx;
        uint8_t quick_key = leaf_ptr->condenser[subhash].quick_key;
        subhash++;
        subhash &= 0xFF;
        if (idx == 0)
            continue;
        if (quick_key != (key & 0xFF))
            continue;
        --idx; // indices wrapped by one to make 0 be last possible index
        if ((leaf_ptr->keys[idx] >> 1) == key)
            return idx;
    }
    return -1;
}

// return subhash for empty index within search range of hash, -1 if no match
int find_empty_in_leaf(Memory& mem, TreeLeaf* leaf_ptr, uint64_t hash) {
    int subhash = hash & 0xFF; // cut off all above one byte
    int subhash_limit = (subhash + 4) & 0xFF;
    while (subhash != subhash_limit) {
        uint8_t idx = leaf_ptr->condenser[subhash].idx;
        if (idx == 0 && leaf_ptr->sz < 255)
            return subhash;
        --idx; // indices wrapped by one to make 0 be last possible index
        subhash++;
        subhash &= 0xFF;
    }
    return -1;
}

bool _Cuckoo::find(Memory& mem, uint64_t key, Ref<DynType>& payload, int& index, uint8_t& size) {
    key <<= 1;
    uint64_t h_1 = hash_a(key);
    Ref<DynType> leaf = primary_tree.lookup(mem, h_1);
    Ref<TreeLeaf> leaf_ref = leaf.as<TreeLeaf>();
    TreeLeaf* leaf_ptr = mem.txl(leaf_ref);
    int in_leaf_idx = find_in_leaf(mem, leaf_ptr, h_1, key);
    if (in_leaf_idx >= 0) {
        index = in_leaf_idx;
        size = leaf_ptr->sz;
        payload = leaf_ptr->payload;
        return true;
    }
    key |= 1;
    uint64_t h_2 = hash_b(key);
    leaf = primary_tree.lookup(mem, h_2);
    leaf_ref = leaf.as<TreeLeaf>();
    leaf_ptr = mem.txl(leaf_ref);
    in_leaf_idx = find_in_leaf(mem, leaf_ptr, h_2, key);
    if (in_leaf_idx >= 0) {
        index = in_leaf_idx;
        size = leaf_ptr->sz;
        payload = leaf_ptr->payload;
        return true;
    }
    return false;
}





// get a null ref back if key could not be found. If found, cow the path to the payload
// and return a pointer allowing for later update of the payload ref.
bool _Cuckoo::find_and_cow_path(Memory& mem, PayloadMgr& pm, uint64_t key, 
                                Ref<DynType>& payload, int& index, uint8_t& size) {
    key <<= 1;
    uint64_t h_1 = hash_a(key);
    Ref<DynType> leaf = primary_tree.lookup(mem, h_1);
    Ref<TreeLeaf> leaf_ref = leaf.as<TreeLeaf>();
    TreeLeaf* leaf_ptr = mem.txl(leaf_ref);
    int in_leaf_idx = find_in_leaf(mem, leaf_ptr, h_1, key);
    if (in_leaf_idx < 0) {
        key |= 1;
        h_1 = hash_b(key);
        leaf = primary_tree.lookup(mem, h_1);
        leaf_ref = leaf.as<TreeLeaf>();
        leaf_ptr = mem.txl(leaf_ref);
        in_leaf_idx = find_in_leaf(mem, leaf_ptr, h_1, key);
        if (in_leaf_idx < 0) {
            return false;
        }
    }
    if (!mem.is_writable(leaf)) {
        TreeLeaf* new_leaf_ptr;
        int capacity = leaf_ptr->capacity;
        int size = get_leaf_size(capacity);
        Ref<TreeLeaf> new_leaf = mem.alloc<TreeLeaf>(new_leaf_ptr, size);
        clone_leaf(leaf_ptr, new_leaf_ptr, capacity);
        pm.cow(new_leaf_ptr->payload, capacity, capacity);
        mem.free(leaf, size);
        primary_tree.cow_path(mem, h_1, new_leaf);
        payload = new_leaf_ptr->payload;
    } else {
        payload = leaf_ptr->payload;
    }
    index = in_leaf_idx;
    size = leaf_ptr->sz;
    return true;
}

struct KeyInUse {};

bool insert_in_leaf(Memory& mem, Ref<DynType> leaf, _TreeTop* tree_ptr, 
		    uint64_t hash, uint64_t &key, PayloadMgr& pm) {

    Ref<TreeLeaf> typed_leaf = leaf.as<TreeLeaf>();
    TreeLeaf* leaf_ptr = mem.txl(typed_leaf);
    if (find_in_leaf(mem, leaf_ptr, hash, key) >= 0)
        throw KeyInUse();
    bool conflict = false;
    int subhash = find_empty_in_leaf(mem, leaf_ptr, hash);
    if (subhash < 0) {
        conflict = true;
        subhash = hash & 0xffULL;
    }

    // Need room for one more key, only if there was no conflict
    size_t needed = leaf_ptr->sz + (conflict ? 0:1);
    size_t old_capacity = leaf_ptr->capacity;
    //std::cout << "    - capacity: " << capacity << " needed: " << needed << std::endl;
    if (!mem.is_writable(leaf) || needed > old_capacity) {
        // need to cow leaf and path to it:
        assert(needed <= 256);
        size_t new_capacity = (needed + 15) & ~15ULL;
        Ref<DynType> old_leaf = leaf;
        TreeLeaf* old_leaf_ptr = leaf_ptr;
        leaf = mem.alloc<TreeLeaf>(leaf_ptr, get_leaf_size(new_capacity));
        clone_leaf(old_leaf_ptr, leaf_ptr, new_capacity);
        mem.free(old_leaf, get_leaf_size(old_capacity));
        pm.cow(leaf_ptr->payload, old_capacity, new_capacity);
        tree_ptr->cow_path(mem, hash, leaf);
    }
    // we now have a writable leaf with sufficient capacity, update it:
    if (conflict) { // we're reusing the spot of an old key:
        uint8_t idx = leaf_ptr->condenser[subhash].idx;
        --idx;
        uint64_t old_key = leaf_ptr->keys[idx];
        leaf_ptr->keys[idx] = key;
        leaf_ptr->condenser[subhash].quick_key = key >> 1;
        pm.swap_internalbuffer(leaf_ptr->payload, idx, leaf_ptr->sz);
        key = old_key;
    } else { // we're adding a new key:
        uint8_t idx = leaf_ptr->sz;
        leaf_ptr->keys[idx] = key;
        leaf_ptr->condenser[subhash].quick_key = key >> 1;
        pm.write_internalbuffer(leaf_ptr->payload, idx, leaf_ptr->sz);
        ++leaf_ptr->sz;
        ++idx;
        leaf_ptr->condenser[subhash].idx = idx;
        tree_ptr->count++;
    }
    return conflict;
}


bool _Cuckoo::first_access(Memory& mem, ObjectIterator& oi) {

    uint64_t tree_index = oi.tree_index;
    while (tree_index < primary_tree.mask) {
        Ref<DynType> leaf = primary_tree.lookup(mem, tree_index);
        if (mem.is_valid(leaf)) {
            Ref<TreeLeaf> leaf_ref = leaf.as<TreeLeaf>();
            TreeLeaf* leaf_ptr = mem.txl(leaf_ref);
            if (leaf_ptr->sz != 0) {
                oi.tree_index = tree_index;
                oi.leaf = leaf_ptr;
                oi.o.r.key = leaf_ptr->keys[0] >> 1;
                oi.o.index = 0;
                oi.o.size = leaf_ptr->sz;
                Ref<DynType> payload = leaf_ptr->payload;
                oi.o.cluster = mem.txl(payload.as<_Cluster>());
                return true;
            }
        }
        tree_index += 256;
    }
    return false;
}

// a bit cowboy-like to put the ObjectIterator method here, but we want
// access to the TreeLeaf definition
bool ObjectIterator::next_access() {
    o.index++;
    if (o.index < leaf->sz) {
        o.r.key = leaf->keys[o.index] >> 1;
        return true;
    }
    return false;
}

void _Cuckoo::rehash_tree(Memory& mem, _TreeTop& tree, PayloadMgr& pm) {
    for (uint64_t index = 0; index < tree.mask; index += 256) {
        Ref<DynType> leaf = tree.lookup(mem, index);
        if (mem.is_valid(leaf)) {
            Ref<TreeLeaf> leaf_ref = leaf.as<TreeLeaf>();
            TreeLeaf* leaf_ptr = mem.txl(leaf_ref);
            for (int j = 0; j < leaf_ptr->sz; ++j) {
                pm.read_internalbuffer(leaf_ptr->payload, j);
                insert(mem, leaf_ptr->keys[j] & ~1ULL, pm);
            }
            size_t capacity = leaf_ptr->capacity;
            pm.free(leaf_ptr->payload, capacity);
            mem.free(leaf, get_leaf_size(capacity));
        }
    }
    tree.free(mem);
}

void _Cuckoo::grow_tree(Memory& mem, PayloadMgr& pm) {
    // make a backup and set up new tree:
    _TreeTop t_p = primary_tree;
    uint64_t sz1 = 1 + 2*t_p.mask;
    primary_tree.init(sz1);
    // iterate through old tree, rehashing everything
    // even though rehash calls insert, it cannot overflow
    // and recurse... formal proof??
    rehash_tree(mem, t_p, pm);
}

const int max_collisions = 20;

void _Cuckoo::insert(Memory& mem, uint64_t key, PayloadMgr& pm) {
    int collision_count = 1;
    while (collision_count < max_collisions) {
        uint64_t hash;
        _TreeTop* tree_ptr;
        if ((key & 1) == 0) { // key encodes which hash was used
            hash = hash_a(key);
        } else {
            hash = hash_b(key);
        }
        tree_ptr = &primary_tree;
        Ref<DynType> leaf = tree_ptr->lookup(mem, hash);
        // insert, potentially update 'key' with value to move
        //uint64_t old_key = key;
        bool conflict = insert_in_leaf(mem, leaf, tree_ptr, hash, key, pm);
        if (!conflict) {
            //std::cout << " - added key: " << old_key << std::endl;
            break;
        }
        //std::cout << " - added key: " << old_key << " and has to move " << key << std::endl;
        key = key ^ 1; // switch hash functions for the key
        ++collision_count;
    }
    if (collision_count == max_collisions) {
        std::cout << "*** Overflow during insert, forced to grow tree " << std::endl;
        grow_tree(mem, pm);
        insert(mem, key, pm);
    }
    // fast way of multiplying by 1.5:
    if ((primary_tree.count + (primary_tree.count >> 1)) > primary_tree.mask)
        grow_tree(mem, pm);
}

