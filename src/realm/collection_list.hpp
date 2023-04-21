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

#ifndef REALM_COLLECTION_LIST_HPP
#define REALM_COLLECTION_LIST_HPP

#include <realm/collection.hpp>
#include <realm/collection_parent.hpp>
#include <realm/bplustree.hpp>
#include <realm/array_ref.hpp>

namespace realm {

using CollectionListPtr = std::shared_ptr<CollectionList>;

/*
 * A CollectionList can hold other collections. The nested collections can be referred to
 * by either an integer index or a string key.
 */

class CollectionList final : public Collection,
                             public CollectionParent,
                             protected ArrayParent,
                             public std::enable_shared_from_this<CollectionList> {
public:
    [[nodiscard]] static CollectionListPtr create(std::shared_ptr<CollectionParent> parent, ColKey col_key,
                                                  Index index, CollectionType coll_type)
    {
        return std::shared_ptr<CollectionList>(new CollectionList(parent, col_key, index, coll_type));
    }
    [[nodiscard]] static CollectionListPtr create(CollectionParent& parent, ColKey col_key)
    {
        return std::shared_ptr<CollectionList>(new CollectionList(&parent, col_key));
    }
    CollectionList(const CollectionList&) = delete;

    ~CollectionList() final;
    size_t size() const final
    {
        return update_if_needed() ? m_refs.size() : 0;
    }

    Mixed get_any(size_t ndx) const final;

    bool init_from_parent(bool allow_create) const;

    size_t get_level() const noexcept final
    {
        return m_level;
    }
    UpdateStatus update_if_needed_with_status() const final;
    bool update_if_needed() const final
    {
        return update_if_needed_with_status() != UpdateStatus::Detached;
    }
    TableRef get_table() const noexcept final
    {
        return m_parent->get_table();
    }
    const Obj& get_object() const noexcept final
    {
        return m_parent->get_object();
    }

    Index get_index(size_t ndx) const noexcept;

    ref_type get_collection_ref(Index index) const noexcept final;
    void set_collection_ref(Index index, ref_type ref) final;

    // If this list is at the outermost nesting level, use these functions to
    // get the leaf collections
    CollectionBasePtr insert_collection(size_t ndx);
    CollectionBasePtr insert_collection(StringData key);
    CollectionBasePtr get_collection(size_t ndx) const;

    // If this list is at an intermediate nesting level, use these functions to
    // get a CollectionList at next level
    CollectionListPtr insert_collection_list(size_t ndx);
    CollectionListPtr insert_collection_list(StringData key);
    CollectionListPtr get_collection_list(size_t ndx) const;

    void remove(size_t ndx);
    void remove(StringData key);

    ref_type get_child_ref(size_t child_ndx) const noexcept final;
    void update_child_ref(size_t child_ndx, ref_type new_ref) final;

private:
    friend class Cluster;
    friend class ClusterTree;

    std::shared_ptr<CollectionParent> m_owned_parent;
    CollectionParent* m_parent;
    CollectionParent::Index m_index;
    size_t m_level = 0;
    Allocator* m_alloc;
    ColKey m_col_key;
    mutable Array m_top;
    mutable std::unique_ptr<BPlusTreeBase> m_keys;
    mutable BPlusTree<ref_type> m_refs;
    DataType m_key_type;
    mutable uint_fast64_t m_content_version = 0;


    CollectionList(std::shared_ptr<CollectionParent> parent, ColKey col_key, Index index, CollectionType coll_type);
    CollectionList(CollectionParent*, ColKey col_key);

    UpdateStatus ensure_created()
    {
        auto status = m_parent->update_if_needed_with_status();
        switch (status) {
            case UpdateStatus::Detached:
                break; // Not possible (would have thrown earlier).
            case UpdateStatus::NoChange: {
                if (m_top.is_attached()) {
                    return UpdateStatus::NoChange;
                }
                // The tree has not been initialized yet for this accessor, so
                // perform lazy initialization by treating it as an update.
                [[fallthrough]];
            }
            case UpdateStatus::Updated: {
                bool attached = init_from_parent(true);
                REALM_ASSERT(attached);
                return attached ? UpdateStatus::Updated : UpdateStatus::Detached;
            }
        }

        REALM_UNREACHABLE();
    }
    void get_all_keys(size_t levels, std::vector<ObjKey>&) const;
};

} // namespace realm

#endif
