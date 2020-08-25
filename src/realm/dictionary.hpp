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
#include <realm/cluster_tree.hpp>

namespace realm {

class DictionaryClusterTree;

class Dictionary : public CollectionBase {
public:
    class Iterator;

    Dictionary() {}
    ~Dictionary();

    Dictionary(const Obj& obj, ColKey col_key);
    Dictionary(const Dictionary& other)
        : CollectionBase(other)
    {
        *this = other;
    }
    Dictionary& operator=(const Dictionary& other);


    // Overriding members of CollectionBase:
    size_t size() const final;
    DataType get_value_data_type() const;
    bool is_null(size_t ndx) const final;
    Mixed get_any(size_t ndx) const final;

    Mixed min(size_t* return_ndx = nullptr) const final;
    Mixed max(size_t* return_ndx = nullptr) const final;
    Mixed sum(size_t* return_cnt = nullptr) const final;
    Mixed avg(size_t* return_cnt = nullptr) const final;

    void sort(std::vector<size_t>& indices, bool ascending = true) const final;
    void distinct(std::vector<size_t>& indices, util::Optional<bool> sort_order = util::none) const final;


    void create();

    // first points to inserted/updated element.
    // second is true if the element was inserted
    std::pair<Iterator, bool> insert(Mixed key, Mixed value);
    std::pair<Iterator, bool> insert(Mixed key, const Obj& obj);

    // throws std::out_of_range if key is not found
    Mixed get(Mixed key) const;
    // adds entry if key is not found
    const Mixed operator[](Mixed key);

    Iterator find(Mixed key);

    void erase(Mixed key);
    void erase(Iterator it);

    void nullify(Mixed);

    void clear();

    Iterator begin() const;
    Iterator end() const;

private:
    mutable DictionaryClusterTree* m_clusters = nullptr;

    bool init_from_parent() const final;
    Mixed do_get(ClusterNode::State&&) const;
};

class Dictionary::Iterator : public ClusterTree::Iterator {
public:
    typedef std::forward_iterator_tag iterator_category;
    typedef std::pair<const Mixed, Mixed> value_type;
    typedef ptrdiff_t difference_type;
    typedef const value_type* pointer;
    typedef const value_type& reference;

    value_type operator*();

private:
    friend class Dictionary;
    using ClusterTree::Iterator::get_position;

    ColumnType m_key_type;

    Iterator(const Dictionary* dict, size_t pos);
};

inline std::pair<Dictionary::Iterator, bool> Dictionary::insert(Mixed key, const Obj& obj)
{
    return insert(key, Mixed(obj.get_link()));
}

} // namespace realm

#endif /* SRC_REALM_DICTIONARY_HPP_ */
