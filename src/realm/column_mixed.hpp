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

#ifndef REALM_COLUMN_MIXED_HPP
#define REALM_COLUMN_MIXED_HPP

#include <realm/bplustree.hpp>
#include <realm/array_mixed.hpp>

namespace realm {

class BPlusTreeMixed : public BPlusTree<Mixed> {
public:
    BPlusTreeMixed(Allocator& alloc)
        : BPlusTree<Mixed>(alloc)
    {
    }

    void ensure_keys()
    {
        auto func = [&](BPlusTreeNode* node, size_t) {
            return static_cast<LeafNode*>(node)->ensure_keys() ? IteratorControl::Stop
                                                               : IteratorControl::AdvanceToNext;
        };

        m_root->bptree_traverse(func);
    }
    size_t find_key(int64_t key) const noexcept
    {
        size_t ret = realm::npos;
        auto func = [&](BPlusTreeNode* node, size_t offset) {
            LeafNode* leaf = static_cast<LeafNode*>(node);
            auto pos = leaf->find_key(key);
            if (pos != realm::not_found) {
                ret = pos + offset;
                return IteratorControl::Stop;
            }
            else {
                return IteratorControl::AdvanceToNext;
            }
        };

        m_root->bptree_traverse(func);
        return ret;
    }

    void set_key(size_t ndx, int64_t key) const noexcept
    {
        auto func = [key](BPlusTreeNode* node, size_t ndx) {
            LeafNode* leaf = static_cast<LeafNode*>(node);
            leaf->set_key(ndx, key);
        };

        m_root->bptree_access(ndx, func);
    }

    int64_t get_key(size_t ndx) const noexcept
    {
        int64_t ret = 0;
        auto func = [&ret](BPlusTreeNode* node, size_t ndx) {
            LeafNode* leaf = static_cast<LeafNode*>(node);
            ret = leaf->get_key(ndx);
        };

        m_root->bptree_access(ndx, func);
        return ret;
    }
};

} // namespace realm

#endif /* REALM_COLUMN_MIXED_HPP */
