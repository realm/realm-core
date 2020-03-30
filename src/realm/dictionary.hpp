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

#include <realm/array.hpp>
#include <realm/obj.hpp>
#include <realm/mixed.hpp>
#include <realm/cluster_tree.hpp>

namespace realm {

class DictionaryClusterTree;

class Dictionary : public ArrayParent {
public:
    class Iterator;
    class MixedRef;

    Dictionary() {}
    ~Dictionary();

    Dictionary(const ConstObj& obj, ColKey col_key);
    Dictionary(const Dictionary& other)
    {
        *this = other;
    }
    Dictionary& operator=(const Dictionary& other);

    bool is_attached() const
    {
        return m_obj.is_valid();
    }

    size_t size() const;

    void create();

    // throws std::out_of_range if key is not found
    Mixed get(Mixed key) const;
    // first points to inserted/updated element.
    // second is true if the element was inserted
    std::pair<Dictionary::Iterator, bool> insert(Mixed key, Mixed value);
    // adds entry if key is not found
    MixedRef operator[](Mixed key);

    void clear();

    Iterator begin() const;
    Iterator end() const;

private:
    friend class MixedRef;
    DictionaryClusterTree* m_clusters = nullptr;
    Obj m_obj;
    ColKey m_col_key;
    mutable bool m_valid = false;
    mutable uint_fast64_t m_content_version = 0;

    void update_content_version() const
    {
        m_content_version = m_obj.get_alloc().get_content_version();
    }

    void update_if_needed() const
    {
        auto content_version = m_obj.get_alloc().get_content_version();
        if (m_obj.update_if_needed() || content_version != m_content_version) {
            init_from_parent();
        }
    }
    void init_from_parent() const;

    void update_child_ref(size_t ndx, ref_type new_ref) override;
    ref_type get_child_ref(size_t ndx) const noexcept override;
    std::pair<ref_type, size_t> get_to_dot_parent(size_t) const override;
};

class DictionaryClusterTree : public ClusterTree {
public:
    static constexpr ColKey s_col_key = ColKey(ColKey::Idx{0}, col_type_OldMixed, ColumnAttrMask(), 0);
    static constexpr ColKey s_col_value = ColKey(ColKey::Idx{1}, col_type_OldMixed, ColumnAttrMask(), 0);

    DictionaryClusterTree(Dictionary* owner, Allocator& alloc, size_t ndx);
    ~DictionaryClusterTree() override;

    ref_type create()
    {
        MemRef mem = create_empty_cluster(m_alloc);
        init_from_ref(mem.get_ref());
        insert_column(s_col_key);
        insert_column(s_col_value);
        return mem.get_ref();
    }
    const Table* get_owner() const override
    {
        return nullptr;
    }
    void for_each_and_every_column(ColIterateFunction func) const override
    {
        func(s_col_key);
        func(s_col_value);
    }
    void set_spec(ArrayPayload&, ColKey::Idx) const override {}
    bool is_string_enum_type(ColKey::Idx) const override
    {
        return false;
    }
    size_t get_ndx_in_parent() const override
    {
        return m_ndx_in_cluster;
    }
    ArrayParent* get_parent() const override
    {
        return m_owner;
    }
    size_t num_leaf_cols() const override
    {
        return 2;
    }

private:
    Dictionary* m_owner;
    size_t m_ndx_in_cluster;
};

class Dictionary::Iterator : public ClusterTree::Iterator {
public:
    typedef std::forward_iterator_tag iterator_category;
    typedef std::pair<Mixed, Mixed> value_type;
    typedef ptrdiff_t difference_type;
    typedef const value_type* pointer;
    typedef const value_type& reference;

    value_type operator*();

private:
    friend class Dictionary;

    Iterator(const Dictionary* dict, size_t pos)
        : ClusterTree::Iterator(*dict->m_clusters, pos)
    {
    }
};

class Dictionary::MixedRef {
public:
    operator Mixed();
    MixedRef& operator=(Mixed val);

private:
    friend class Dictionary;
    MixedRef(Allocator& alloc, MemRef mem, size_t ndx)
        : m_alloc(alloc)
        , m_mem(mem)
        , m_ndx(ndx)
    {
    }
    Allocator& m_alloc;
    MemRef m_mem;
    size_t m_ndx;
};
} // namespace realm

#endif /* SRC_REALM_DICTIONARY_HPP_ */
