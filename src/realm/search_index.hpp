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

#ifndef REALM_SEARCH_INDEX_HPP
#define REALM_SEARCH_INDEX_HPP

#include <realm/cluster_tree.hpp>

namespace realm {

// The purpose of this class is to get easy access to fields in a specific column in the
// cluster. When you have an object like this, you can get a string version of the relevant
// field based on the key for the object.
class ClusterColumn {
public:
    ClusterColumn(const ClusterTree* cluster_tree, ColKey column_key, IndexType type)
        : m_cluster_tree(cluster_tree)
        , m_column_key(column_key)
        , m_tokenize(type == IndexType::Fulltext)
        , m_full_word(m_tokenize | column_key.is_collection())
    {
    }
    size_t size() const
    {
        return m_cluster_tree->size();
    }
    ClusterTree::Iterator begin() const
    {
        return ClusterTree::Iterator(*m_cluster_tree, 0);
    }

    ClusterTree::Iterator end() const
    {
        return ClusterTree::Iterator(*m_cluster_tree, size());
    }


    DataType get_data_type() const;
    ColKey get_column_key() const
    {
        return m_column_key;
    }
    bool is_nullable() const
    {
        return m_column_key.is_nullable();
    }
    bool tokenize() const
    {
        return m_tokenize;
    }
    bool full_word() const
    {
        return m_full_word;
    }
    Mixed get_value(ObjKey key) const;
    Lst<String> get_list(ObjKey key) const;
    std::vector<ObjKey> get_all_keys() const;

private:
    const ClusterTree* m_cluster_tree;
    ColKey m_column_key;
    bool m_tokenize;
    bool m_full_word;
};


class SearchIndex {
public:
    SearchIndex(const ClusterColumn& target_column, Array* root)
        : m_target_column(target_column)
        , m_root_array(root)
    {
    }
    virtual ~SearchIndex() = default;

    // Search Index API:
    virtual void insert(ObjKey value, const Mixed& key) = 0;
    virtual void set(ObjKey value, const Mixed& key) = 0;
    virtual ObjKey find_first(const Mixed&) const = 0;
    virtual void find_all(std::vector<ObjKey>& result, Mixed value, bool case_insensitive = false) const = 0;
    virtual FindRes find_all_no_copy(Mixed value, InternalFindResult& result) const = 0;
    virtual size_t count(const Mixed&) const = 0;
    virtual void erase(ObjKey) = 0;
    virtual void clear() = 0;
    virtual bool has_duplicate_values() const noexcept = 0;
    virtual bool is_empty() const = 0;
    virtual void insert_bulk(const ArrayUnsigned* keys, uint64_t key_offset, size_t num_values,
                             ArrayPayload& values) = 0;
    virtual void insert_bulk_list(const ArrayUnsigned* keys, uint64_t key_offset, size_t num_values,
                                  ArrayInteger& ref_array) = 0;
    virtual void verify() const = 0;

#ifdef REALM_DEBUG
    virtual void print() const = 0;
#endif // REALM_DEBUG

    // Accessor concept:
    Allocator& get_alloc() const noexcept;
    void destroy() noexcept;
    void detach();
    bool is_attached() const noexcept;
    void set_parent(ArrayParent* parent, size_t ndx_in_parent) noexcept;
    size_t get_ndx_in_parent() const noexcept;
    void update_from_parent() noexcept;
    void refresh_accessor_tree(const ClusterColumn& target_column);
    ref_type get_ref() const noexcept;

    // SearchIndex common base methods
    ColKey get_column_key() const
    {
        return m_target_column.get_column_key();
    }

    void set_target(const ClusterColumn& target_column) noexcept
    {
        m_target_column = target_column;
    }

protected:
    ClusterColumn m_target_column;
    Array* m_root_array;
};


inline Allocator& SearchIndex::get_alloc() const noexcept
{
    return m_root_array->get_alloc();
}

inline void SearchIndex::destroy() noexcept
{
    return m_root_array->destroy_deep();
}

inline bool SearchIndex::is_attached() const noexcept
{
    return m_root_array->is_attached();
}

inline void SearchIndex::refresh_accessor_tree(const ClusterColumn& target_column)
{
    m_root_array->init_from_parent();
    m_target_column = target_column;
}

inline ref_type SearchIndex::get_ref() const noexcept
{
    return m_root_array->get_ref();
}

inline void SearchIndex::set_parent(ArrayParent* parent, size_t ndx_in_parent) noexcept
{
    m_root_array->set_parent(parent, ndx_in_parent);
}

inline size_t SearchIndex::get_ndx_in_parent() const noexcept
{
    return m_root_array->get_ndx_in_parent();
}

inline void SearchIndex::update_from_parent() noexcept
{
    m_root_array->init_from_parent();
}

} // namespace realm

#endif // REALM_SEARCH_INDEX_HPP
