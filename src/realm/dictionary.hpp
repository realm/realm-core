/*************************************************************************
 *
 * Copyright 2019 Realm Inc.
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

#ifndef REALM_DICTIONARY_HPP
#define REALM_DICTIONARY_HPP

#include <realm/collection.hpp>
#include <realm/obj.hpp>
#include <realm/mixed.hpp>
#include <realm/array_mixed.hpp>
#include <realm/dictionary_cluster_tree.hpp>

namespace realm {

class DictionaryClusterTree;

class Dictionary : public CollectionBase, private ArrayParent {
public:
    using Iterator = CollectionIterator<Dictionary>;

    Dictionary() {}
    ~Dictionary();

    Dictionary(const Obj& obj, ColKey col_key);
    Dictionary(const Dictionary& other)
        : m_obj(other.m_obj)
        , m_col_key(other.m_col_key)
        , m_key_type(other.m_key_type)
    {
        *this = other;
    }
    Dictionary& operator=(const Dictionary& other);

    DataType get_key_data_type() const;
    DataType get_value_data_type() const;

    // Overriding members of CollectionBase:
    std::unique_ptr<CollectionBase> clone_collection() const;
    size_t size() const final;
    bool is_null(size_t ndx) const final;
    Mixed get_any(size_t ndx) const final;

    Mixed min(size_t* return_ndx = nullptr) const final;
    Mixed max(size_t* return_ndx = nullptr) const final;
    Mixed sum(size_t* return_cnt = nullptr) const final;
    Mixed avg(size_t* return_cnt = nullptr) const final;

    void sort(std::vector<size_t>& indices, bool ascending = true) const final;
    void distinct(std::vector<size_t>& indices, util::Optional<bool> sort_order = util::none) const final;
    TableRef get_target_table() const final;
    const Obj& get_obj() const noexcept final;
    ColKey get_col_key() const final;
    ObjKey get_key() const final;
    bool is_attached() const final;
    bool has_changed() const final;
    ConstTableRef get_table() const final;


    void create();

    // first points to inserted/updated element.
    // second is true if the element was inserted
    std::pair<Iterator, bool> insert(Mixed key, Mixed value);
    std::pair<Iterator, bool> insert(Mixed key, const Obj& obj);

    // throws std::out_of_range if key is not found
    Mixed get(Mixed key) const;
    // Noexcept version
    util::Optional<Mixed> try_get(Mixed key) const noexcept;
    // adds entry if key is not found
    const Mixed operator[](Mixed key);

    Iterator find(Mixed key);

    void erase(Mixed key);
    void erase(Iterator it);

    void nullify(Mixed);

    void clear() final;

    template <class T>
    void for_all_values(T&& f)
    {
        if (m_clusters) {
            ArrayMixed leaf(m_obj.get_alloc());
            // Iterate through cluster and call f on each value
            auto trv_func = [&leaf, &f](const Cluster* cluster) {
                size_t e = cluster->node_size();
                cluster->init_leaf(DictionaryClusterTree::s_values_col, &leaf);
                for (size_t i = 0; i < e; i++) {
                    f(leaf.get(i));
                }
                // Continue
                return false;
            };
            m_clusters->traverse(trv_func);
        }
    }

    Iterator begin() const;
    Iterator end() const;

protected:
    bool update_if_needed() const final;

private:
    mutable DictionaryClusterTree* m_clusters = nullptr;
    Obj m_obj;
    ColKey m_col_key;
    DataType m_key_type = type_String;

    mutable uint_fast64_t m_content_version = 0;
    mutable uint_fast64_t m_last_content_version = 0;

    bool init_from_parent() const final;
    Mixed do_get(ClusterNode::State&&) const;

    // Overriding ArrayParent interface:
    ref_type get_child_ref(size_t child_ndx) const noexcept final
    {
        static_cast<void>(child_ndx);
        try {
            return to_ref(m_obj._get<int64_t>(m_col_key.get_index()));
        }
        catch (const KeyNotFound&) {
            return ref_type(0);
        }
    }

    void update_child_ref(size_t child_ndx, ref_type new_ref) final
    {
        REALM_ASSERT(child_ndx == 0);
        m_obj.set_int(m_col_key, from_ref(new_ref));
    }

    void update_content_version() const
    {
        m_content_version = m_obj.get_alloc().get_content_version();
    }

    friend class CollectionIterator<Dictionary>;
};

template <>
class CollectionIterator<Dictionary> : public ClusterTree::Iterator {
public:
    typedef std::forward_iterator_tag iterator_category;
    typedef std::pair<const Mixed, Mixed> value_type;
    typedef ptrdiff_t difference_type;
    typedef const value_type* pointer;
    typedef const value_type& reference;

    value_type operator*() const;

private:
    friend class Dictionary;
    using ClusterTree::Iterator::get_position;

    DataType m_key_type;

    CollectionIterator(const Dictionary* dict, size_t pos);
};

inline std::pair<Dictionary::Iterator, bool> Dictionary::insert(Mixed key, const Obj& obj)
{
    return insert(key, Mixed(obj.get_link()));
}

inline std::unique_ptr<CollectionBase> Dictionary::clone_collection() const
{
    return m_obj.get_dictionary_ptr(m_col_key);
}

inline TableRef Dictionary::get_target_table() const
{
    return m_obj.get_target_table(m_col_key);
}

inline const Obj& Dictionary::get_obj() const noexcept
{
    return m_obj;
}

inline ColKey Dictionary::get_col_key() const
{
    return m_col_key;
}

inline ObjKey Dictionary::get_key() const
{
    return m_obj.get_key();
}

inline bool Dictionary::is_attached() const
{
    return m_obj.is_valid();
}

inline bool Dictionary::has_changed() const
{
    update_if_needed();
    if (m_last_content_version != m_content_version) {
        m_last_content_version = m_content_version;
        return true;
    }
    return false;
}

inline ConstTableRef Dictionary::get_table() const
{
    return m_obj.get_table();
}

inline bool Dictionary::update_if_needed() const
{
    auto content_version = m_obj.get_alloc().get_content_version();
    if (m_obj.update_if_needed() || content_version != m_content_version) {
        init_from_parent();
        return true;
    }
    return false;
}


} // namespace realm

#endif /* SRC_REALM_DICTIONARY_HPP_ */
