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

#include <realm/collection_parent.hpp>
#include <realm/bplustree.hpp>
#include <realm/array_ref.hpp>

namespace realm {

using CollectionListPtr = std::shared_ptr<CollectionList>;

class CollectionList : public CollectionParent,
                       protected ArrayParent,
                       public std::enable_shared_from_this<CollectionList> {
public:
    [[nodiscard]] static CollectionListPtr create(std::shared_ptr<CollectionParent> parent, ColKey col_key,
                                                  Index index, CollectionType coll_type)
    {
        return std::shared_ptr<CollectionList>(new CollectionList(parent, col_key, index, coll_type));
    }
    CollectionList(const CollectionList&) = delete;

    ~CollectionList();

    bool init_from_parent(bool allow_create) const;

    Replication* get_replication() const final
    {
        return m_parent->get_replication();
    }
    size_t get_level() const noexcept final
    {
        return m_level;
    }
    UpdateStatus update_if_needed_with_status() const final
    {
        auto status = m_parent->update_if_needed_with_status();
        switch (status) {
            case UpdateStatus::Detached: {
                m_top.detach();
                return UpdateStatus::Detached;
            }
            case UpdateStatus::NoChange:
                if (m_top.is_attached()) {
                    return UpdateStatus::NoChange;
                }
                // The tree has not been initialized yet for this accessor, so
                // perform lazy initialization by treating it as an update.
                [[fallthrough]];
            case UpdateStatus::Updated: {
                bool attached = init_from_parent(false);
                return attached ? UpdateStatus::Updated : UpdateStatus::Detached;
            }
        }
        REALM_UNREACHABLE();
    }
    bool update_if_needed() const final
    {
        return update_if_needed_with_status() != UpdateStatus::Detached;
    }
    int_fast64_t bump_content_version() final
    {
        return m_parent->bump_content_version();
    }
    void bump_both_versions() override
    {
        m_parent->bump_both_versions();
    }
    TableRef get_table() const noexcept final
    {
        return m_parent->get_table();
    }
    ColKey get_col_key() const noexcept final
    {
        return m_col_key;
    }
    const Obj& get_object() const noexcept final
    {
        return m_parent->get_object();
    }

    Index get_index(size_t ndx) const noexcept;

    ref_type get_collection_ref(Index index) const noexcept final;
    void set_collection_ref(Index index, ref_type ref) final;

    bool is_empty() const
    {
        return size() == 0;
    }

    size_t size() const
    {
        return update_if_needed() ? m_refs.size() : 0;
    }
    CollectionBasePtr insert_collection(size_t ndx);
    CollectionBasePtr insert_collection(StringData key);
    CollectionBasePtr get_collection_ptr(size_t ndx) const;

    CollectionListPtr insert_collection_list(size_t ndx);
    CollectionListPtr insert_collection_list(StringData key);
    CollectionListPtr get_collection_list(size_t ndx) const;

    void remove(size_t ndx);
    void remove(StringData key);

    ref_type get_child_ref(size_t child_ndx) const noexcept final;
    void update_child_ref(size_t child_ndx, ref_type new_ref) final;

private:
    std::shared_ptr<CollectionParent> m_parent;
    CollectionParent::Index m_index;
    size_t m_level;
    Allocator* m_alloc;
    ColKey m_col_key;
    mutable Array m_top;
    mutable std::unique_ptr<BPlusTreeBase> m_keys;
    mutable BPlusTree<ref_type> m_refs;
    DataType m_key_type;

    CollectionList(std::shared_ptr<CollectionParent> parent, ColKey col_key, Index index, CollectionType coll_type);

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
};

} // namespace realm

#endif
