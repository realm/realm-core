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

class CollectionList final : public Collection, public CollectionParent, protected ArrayParent {
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
        return update() ? m_refs.size() : 0;
    }

    Mixed get_any(size_t ndx) const final;

    bool init_from_parent(bool allow_create) const;

    UpdateStatus update_if_needed_with_status() const noexcept final;
    bool update_if_needed() const final;

    FullPath get_path() const noexcept final;
    Path get_short_path() const noexcept final;
    void add_index(Path& path, Index ndx) const noexcept final;

    TableRef get_table() const noexcept final
    {
        return m_parent->get_table();
    }
    const Obj& get_object() const noexcept final
    {
        return m_parent->get_object();
    }

    Index get_index(size_t ndx) const noexcept;

    ref_type get_collection_ref(Index index, CollectionType) const noexcept final;
    void set_collection_ref(Index index, ref_type ref, CollectionType) final;

    // If this list is at the outermost nesting level, use these functions to
    // get the leaf collections
    void insert_collection(const PathElement& index, CollectionType = CollectionType::Dictionary) override;
    CollectionBasePtr get_collection(const PathElement& index) const;

    // If this list is at an intermediate nesting level, use these functions to
    // get a CollectionList at next level
    CollectionListPtr get_collection_list(const PathElement&) const;

    void remove(size_t ndx);
    void remove(StringData key);

    ref_type get_child_ref(size_t child_ndx) const noexcept final;
    void update_child_ref(size_t child_ndx, ref_type new_ref) final;

    CollectionType get_collection_type() const noexcept override
    {
        return m_coll_type;
    }
    void to_json(std::ostream&, size_t, JSONOutputMode, util::FunctionRef<void(const Mixed&)>) const override;

private:
    friend class Cluster;
    friend class ClusterTree;

    std::shared_ptr<CollectionParent> m_owned_parent;
    CollectionParent* m_parent;
    CollectionParent::Index m_index;
    Allocator* m_alloc;
    ColKey m_col_key;
    mutable Array m_top;
    mutable std::unique_ptr<BPlusTreeBase> m_keys;
    mutable BPlusTree<ref_type> m_refs;
    CollectionType m_coll_type;
    mutable uint_fast64_t m_content_version = 0;


    CollectionList(std::shared_ptr<CollectionParent> parent, ColKey col_key, Index index, CollectionType coll_type);
    CollectionList(CollectionParent*, ColKey col_key);

    void bump_content_version()
    {
        m_content_version = m_alloc->bump_content_version();
    }
    void ensure_created();
    bool update() const
    {
        return update_if_needed_with_status() != UpdateStatus::Detached;
    }
    void get_all_keys(size_t levels, std::vector<ObjKey>&) const;

    Index get_index(const PathElement&) const;
};

} // namespace realm

#endif
