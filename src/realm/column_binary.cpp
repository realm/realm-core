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

#include <realm/column_binary.hpp>
#include <realm/array_key.hpp>

#include <algorithm>
#include <iomanip>
#include <memory>

using namespace realm;

BinaryData BinaryColumn::get_at(size_t ndx, size_t& pos) const noexcept
{
    REALM_ASSERT_3(ndx, <, size());
    if (m_cached_leaf_begin <= ndx && ndx < m_cached_leaf_end) {
        return m_leaf_cache.get_at(ndx - m_cached_leaf_begin, pos);
    }
    else {
        BinaryData value;

        auto func = [&value, &pos](BPlusTreeNode* node, size_t ndx_in_leaf) {
            LeafNode* leaf = static_cast<LeafNode*>(node);
            value = leaf->get_at(ndx_in_leaf, pos);
        };

        m_root->bptree_access(ndx, func);

        return value;
    }
}
