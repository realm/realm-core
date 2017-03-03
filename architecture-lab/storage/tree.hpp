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

#ifndef __TREE_HPP__
#define __TREE_HPP__

#include "refs.hpp"
#include "memory.hpp"

template <typename TLeaf>
struct _TreeTop {
    uint64_t mask;
    uint64_t count;
    char levels;
    Ref<DynType> top_level;

    struct LeafCommitter {
        virtual Ref<TLeaf> commit(Ref<TLeaf> from) = 0;
    };

    void copied_to_file(Memory& mem, LeafCommitter& lc);
    void cow_path(Memory& mem, uint64_t index, Ref<TLeaf> leaf);

    uint64_t allocate_free_index(Memory& mem);
    void release_index(Memory& mem, uint64_t index);

    // get leaf at index
    Ref<TLeaf> lookup(const Memory& mem, uint64_t index) const;

    void init(uint64_t capacity);

    void free(Memory& mem);

    static Ref<DynType> dispatch_commit(Memory& mem, Ref<DynType> from, int levels, LeafCommitter& lc);
};



/*
  Implementation - possibly move to dedicated impl header file
*/

#include <cstdint>


// interior node of radix tree
struct _TreeNode {
    // bitmap indicating if underlying subtree is completely filled.
    uint64_t next_level_filled[8];

    // refs to next level in tree.
    Ref<DynType> next_level[256];

    // Fixme: put cow operation here!
    template <typename TLeaf>
    static Ref<DynType> commit(Memory& mem, Ref<DynType> from, int levels, 
                               typename _TreeTop<TLeaf>::LeafCommitter& lc);
};

template<typename TLeaf>
void _TreeTop<TLeaf>::copied_to_file(Memory& mem, LeafCommitter& lc) {
    // this is sort the inverse of cow_path
    top_level = dispatch_commit(mem, top_level, levels, lc);
}

template<typename TLeaf>
Ref<DynType> _TreeTop<TLeaf>::dispatch_commit(Memory& mem, Ref<DynType> from, int levels, 
                                              LeafCommitter& lc) {
    if (is_null(from)) return from;
    if (levels == 1) {
        return lc.commit(from.as<TLeaf>());
    } else {
        return _TreeNode::commit<TLeaf>(mem, from, levels, lc);
    }
}

template<typename TLeaf>
Ref<DynType> _TreeNode::commit(Memory& mem, Ref<DynType> from, int levels, 
                               typename _TreeTop<TLeaf>::LeafCommitter& lc) {
    if (mem.is_writable(from)) {
        _TreeNode* to_ptr;
        Ref<_TreeNode> to = mem.alloc_in_file<_TreeNode>(to_ptr);
        _TreeNode* from_ptr = mem.txl(from.as<_TreeNode>());
        for (int i=0; i<256; ++i) {
            to_ptr->next_level[i] = _TreeTop<TLeaf>::dispatch_commit(mem, from_ptr->next_level[i], levels-1, lc);
        }
        mem.free(from);
        return to;
    }
    return from;
}

template<typename TLeaf>
void _TreeTop<TLeaf>::init(uint64_t capacity) {
    int bits = 4; //minimal size of tree is 16
    while ((1ULL<<bits) < capacity) ++bits;
    mask = (1ULL << bits) - 1;
    count = 0;
    levels = 1 + ((bits-1)/8);
    top_level = Ref<DynType>();
}

inline Ref<DynType> step(const Memory& mem, Ref<DynType> ref, uint64_t masked_index, int shift) {
    Ref<_TreeNode> r = ref.as<_TreeNode>();
    unsigned char c = masked_index >> shift;
    ref = mem.txl(r)->next_level[c];
    return ref;
}

inline Ref<DynType> step_with_trace(const Memory& mem, Ref<DynType> ref, uint64_t masked_index,
				    int shift, Ref<DynType>*& tracking) {
    Ref<_TreeNode> r = ref.as<_TreeNode>();
    unsigned char c = masked_index >> shift;
    _TreeNode* ptr = mem.txl(r);
    ref = ptr->next_level[c];
    tracking = &ptr->next_level[c];
    return ref;
}

// Get leaf at index:
template<typename TLeaf>
Ref<TLeaf> _TreeTop<TLeaf>::lookup(const Memory& mem, uint64_t index) const {
    Ref<DynType> ref = top_level;
    uint64_t masked_index = index & mask;
    switch (levels) {
        case 8:
            ref = step(mem, ref, masked_index, 56);
        case 7:
            ref = step(mem, ref, masked_index, 48);
        case 6:
            ref = step(mem, ref, masked_index, 40);
        case 5:
            ref = step(mem, ref, masked_index, 32);
        case 4:
            ref = step(mem, ref, masked_index, 24);
        case 3:
            ref = step(mem, ref, masked_index, 16);
        case 2:
            ref = step(mem, ref, masked_index, 8);
        case 1:
            return ref.as<TLeaf>();
        case 0:
            return Ref<TLeaf>(); // empty tree <-- this should assert!
    }
    return Ref<TLeaf>(); // not possible
}

// Set leaf at index:
// copy-on-write the path from the tree top to the leaf, but not the top or leaf themselves.
// caller is responsible for copy-on-writing the leaf PRIOR to the call, and for
// copy-on-writing the top PRIOR to the call so that it can be updated.
template<typename TLeaf>
void _TreeTop<TLeaf>::cow_path(Memory& mem, uint64_t index, Ref<TLeaf> leaf) {
    Ref<DynType> ref = top_level;
    Ref<DynType>* tracking_ref = &top_level;
    uint64_t masked_index = index & mask;
    int _levels = levels;
    int shifts = _levels * 8 - 8;
    while (_levels > 1) {
        // copy on write each interior node
        if (!mem.is_writable(ref)) {
            Ref<_TreeNode> old_ref = ref.as<_TreeNode>();
            _TreeNode* new_node;
            ref = mem.alloc<_TreeNode>(new_node);
            _TreeNode* old_node = mem.txl(old_ref);
            *new_node = *old_node;
            *tracking_ref = ref;
            mem.free(old_ref);
        }
        ref = step_with_trace(mem, ref, masked_index, shifts, tracking_ref);
        shifts -= 8;
        --_levels;
    }
    // leaf node:
    *tracking_ref = leaf;
}

// release all interior nodes of tree - leafs should have been
// removed/released before calling free_tree. The tree must
// be made writable before calling free_tree.
inline void free_tree_internal(int level, Memory& mem, Ref<DynType> ref) {
    Ref<_TreeNode> tree_node = ref.as<_TreeNode>();
    _TreeNode* node_ptr = mem.txl(tree_node);
    if (level > 2) {
        for (int j=0; j<256; ++j) {
            if (node_ptr)
                free_tree_internal(level - 1, mem, node_ptr->next_level[j]);
        }
    }
    mem.free(ref);
}

template<typename TLeaf>
void _TreeTop<TLeaf>::free(Memory& mem) {
    if (levels > 1)
        free_tree_internal(levels, mem, top_level);
    top_level = Ref<DynType>();
}





#endif
