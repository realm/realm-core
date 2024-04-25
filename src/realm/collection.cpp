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

#include <realm/group.hpp>
#include <realm/collection.hpp>
#include <realm/bplustree.hpp>
#include <realm/array_key.hpp>
#include <realm/array_string.hpp>
#include <realm/array_mixed.hpp>

namespace realm {

namespace _impl {
size_t virtual2real(const std::vector<size_t>& vec, size_t ndx) noexcept
{
    for (auto i : vec) {
        if (i > ndx)
            break;
        ndx++;
    }
    return ndx;
}

size_t virtual2real(const BPlusTree<ObjKey>* tree, size_t ndx) noexcept
{
    // Only translate if context flag is set.
    if (tree->get_context_flag()) {
        size_t adjust = 0;
        auto func = [&adjust, ndx](BPlusTreeNode* node, size_t offset) {
            auto leaf = static_cast<BPlusTree<ObjKey>::LeafNode*>(node);
            size_t sz = leaf->size();
            for (size_t i = 0; i < sz; i++) {
                if (i + offset == ndx) {
                    return IteratorControl::Stop;
                }
                auto k = leaf->get(i);
                if (k.is_unresolved()) {
                    adjust++;
                }
            }
            return IteratorControl::AdvanceToNext;
        };

        tree->traverse(func);
        ndx -= adjust;
    }
    return ndx;
}

size_t real2virtual(const std::vector<size_t>& vec, size_t ndx) noexcept
{
    // Subtract the number of tombstones below ndx.
    auto it = std::lower_bound(vec.begin(), vec.end(), ndx);
    // A tombstone index has no virtual mapping. This is an error.
    REALM_ASSERT_DEBUG_EX(it == vec.end() || *it != ndx, ndx, vec.size());
    auto n = it - vec.begin();
    return ndx - n;
}

void update_unresolved(std::vector<size_t>& vec, const BPlusTree<ObjKey>* tree)
{
    vec.clear();

    // Only do the scan if context flag is set.
    if (tree && tree->is_attached() && tree->get_context_flag()) {
        auto func = [&vec](BPlusTreeNode* node, size_t offset) {
            auto leaf = static_cast<BPlusTree<ObjKey>::LeafNode*>(node);
            size_t sz = leaf->size();
            for (size_t i = 0; i < sz; i++) {
                auto k = leaf->get(i);
                if (k.is_unresolved()) {
                    vec.push_back(i + offset);
                }
            }
            return IteratorControl::AdvanceToNext;
        };

        tree->traverse(func);
    }
}

void check_for_last_unresolved(BPlusTree<ObjKey>* tree)
{
    if (tree) {
        bool no_more_unresolved = true;
        size_t sz = tree->size();
        for (size_t n = 0; n < sz; n++) {
            if (tree->get(n).is_unresolved()) {
                no_more_unresolved = false;
                break;
            }
        }
        if (no_more_unresolved)
            tree->set_context_flag(false);
    }
}

size_t get_collection_size_from_ref(ref_type ref, Allocator& alloc)
{
    size_t ret = 0;
    if (ref) {
        Array arr(alloc);
        arr.init_from_ref(ref);
        if (arr.is_inner_bptree_node()) {
            // This is a BPlusTree
            ret = size_t(arr.back()) >> 1;
        }
        else if (arr.has_refs()) {
            // This is a dictionary
            auto key_ref = arr.get_as_ref(0);
            ret = get_collection_size_from_ref(key_ref, alloc);
        }
        else {
            ret = arr.size();
        }
    }
    return ret;
}

} // namespace _impl

Collection::~Collection() {}

void Collection::get_any(QueryCtrlBlock& ctrl, Mixed val, size_t index)
{
    auto path_size = ctrl.path.size() - index;
    PathElement& pe = ctrl.path[index];
    bool end_of_path = path_size == 1;

    if (end_of_path) {
        ctrl.matches.emplace_back();
    }

    if (val.is_type(type_Dictionary) && (pe.is_key() || pe.is_all())) {
        auto ref = val.get_ref();
        if (!ref)
            return;
        Array top(*ctrl.alloc);
        top.init_from_ref(ref);

        BPlusTree<StringData> keys(*ctrl.alloc);
        keys.set_parent(&top, 0);
        keys.init_from_parent();
        size_t start = 0;
        if (size_t finish = keys.size()) {
            if (pe.is_key()) {
                start = keys.find_first(StringData(pe.get_key()));
                if (start == realm::not_found) {
                    if (pe.get_key() == "@keys") {
                        ctrl.path_only_unary_keys = false;
                        REALM_ASSERT(end_of_path);
                        keys.for_all([&](const auto& k) {
                            ctrl.matches.back().push_back(k);
                        });
                    }
                    else if (end_of_path) {
                        ctrl.matches.back().push_back(Mixed());
                    }
                    return;
                }
                finish = start + 1;
            }
            BPlusTree<Mixed> values(*ctrl.alloc);
            values.set_parent(&top, 1);
            values.init_from_parent();
            for (; start < finish; start++) {
                val = values.get(start);
                if (end_of_path) {
                    ctrl.matches.back().push_back(val);
                }
                else {
                    Collection::get_any(ctrl, val, index + 1);
                }
            }
        }
    }
    else if (val.is_type(type_List) && (pe.is_ndx() || pe.is_all())) {
        auto ref = val.get_ref();
        if (!ref)
            return;
        BPlusTree<Mixed> list(*ctrl.alloc);
        list.init_from_ref(ref);
        if (size_t sz = list.size()) {
            size_t start = 0;
            size_t finish = sz;
            if (pe.is_ndx()) {
                start = pe.get_ndx();
                if (start == size_t(-1)) {
                    start = sz - 1;
                }
                if (start < sz) {
                    finish = start + 1;
                }
            }
            for (; start < finish; start++) {
                val = list.get(start);
                if (end_of_path) {
                    ctrl.matches.back().push_back(val);
                }
                else {
                    Collection::get_any(ctrl, val, index + 1);
                }
            }
        }
    }
    else if (val.is_type(type_TypedLink) && pe.is_key()) {
        auto link = val.get_link();
        Obj obj = ctrl.group->get_object(link);
        auto col = obj.get_table()->get_column_key(pe.get_key());
        if (col) {
            val = obj.get_any(col);
            if (end_of_path) {
                ctrl.matches.back().push_back(val);
            }
            else {
                if (val.is_type(type_Link)) {
                    val = ObjLink(obj.get_target_table(col)->get_key(), val.get<ObjKey>());
                }
                Collection::get_any(ctrl, val, index + 1);
            }
        }
    }
}

UpdateStatus CollectionBase::do_init_from_parent(BPlusTreeBase* tree, ref_type ref, bool allow_create)
{
    if (ref) {
        tree->init_from_ref(ref);
    }
    else {
        if (!allow_create) {
            tree->detach();
            return UpdateStatus::Detached;
        }
        // The ref in the column was NULL, create the tree in place.
        tree->create();
        REALM_ASSERT(tree->is_attached());
    }
    return UpdateStatus::Updated;
}

void CollectionBase::out_of_bounds(const char* msg, size_t index, size_t size) const
{
    auto path = get_short_path();
    path.erase(path.begin());
    throw OutOfBounds(util::format("%1 on %2 '%3.%4%5'", msg, collection_type_name(get_collection_type()),
                                   get_table()->get_class_name(), get_property_name(), path),
                      index, size);
}

} // namespace realm
