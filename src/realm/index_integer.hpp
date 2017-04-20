/*************************************************************************
 *
 * Copyright 2017 Realm Inc.
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

#ifndef REALM_INDEX_INTEGER_HPP_
#define REALM_INDEX_INTEGER_HPP_

#include "realm/index_string.hpp"

namespace realm {

class IntegerIndex : public SearchIndex {
public:
    IntegerIndex(ColumnBase* target_column, Allocator&);
    IntegerIndex(ref_type, ArrayParent*, size_t ndx_in_parent, ColumnBase* target_column, Allocator&);
    ~IntegerIndex() noexcept;

    void update_from_parent(size_t old_baseline) noexcept override;
    void refresh_accessor_tree(size_t, const Spec&) override;

    void clear() override;

    void insert(size_t row_ndx, int64_t value, size_t num_rows, bool is_append);
    void insert(size_t row_ndx, util::Optional<int64_t> value, size_t num_rows, bool is_append);
    void insert_null(size_t row_ndx, size_t num_rows, bool is_append);

    void update_ref(int64_t value, size_t old_row_ndx, size_t new_row_ndx);
    void update_ref(util::Optional<int64_t> value, size_t old_row_ndx, size_t new_row_ndx);

    void erase(size_t row_ndx, bool is_last);

    size_t count(int64_t value) const;

    size_t find_first(int64_t value) const;
    size_t find_first(util::Optional<int64_t> value) const;
    void find_all(IntegerColumn& result, int64_t value) const;
    void find_all(IntegerColumn& result, util::Optional<int64_t> value) const;

    void distinct(IntegerColumn& result) const override;

private:
    struct Treetop;
    struct TreeLeaf : private Array {
        IntegerIndex* m_index;
        Array m_condenser;
        Array m_values;

        TreeLeaf(Allocator&, IntegerIndex* = nullptr);

        static ref_type create(Allocator&);

        void init(ref_type ref);
        void clear();

        size_t size() const noexcept
        {
            return m_values.size();
        }
        ref_type get_ref() const noexcept
        {
            return Array::get_ref();
        }
        void set_parent(ArrayParent* parent, size_t ndx_in_parent) noexcept
        {
            Array::set_parent(parent, ndx_in_parent);
        }
        bool has_parent()
        {
            return Array::get_parent() != nullptr;
        }
        void init_from_parent()
        {
            init(get_ref_from_parent());
        }
        void detach()
        {
            Array::detach();
        }

        bool insert(Treetop* treeTop, uint64_t hash, int64_t key, int64_t& value);
        void erase(Treetop* treeTop, uint64_t hash, int idx, int64_t value);
        void update_ref(Treetop* treeTop, uint64_t hash, int i, size_t old_row_ndx, size_t new_row_ndx);

        int find(uint64_t hash, int64_t key) const;
        int find_empty(uint64_t hash, int64_t key) const;
        size_t count(int i) const;
        int64_t get_first_value(int i) const;
        void get_all_values(int i, std::vector<int64_t>& values) const;
        void adjust_row_indexes(size_t min_row_ndx, int diff);

    private:
        void ensure_writeable(Treetop* treeTop, uint64_t hash);
    };
    struct Treetop : public Array {
        size_t m_count;
        uint64_t m_mask;
        int m_levels;

        Treetop(Allocator& alloc);
        Treetop(Treetop&& other);
        void init(size_t capacity);
        void clear(IntegerIndex::TreeLeaf& leaf);
        size_t get_count() const noexcept;
        void incr_count();
        void decr_count();
        bool ready_to_grow() const noexcept;
        void cow_path(uint64_t hash, ref_type leaf_ref);
        bool lookup(uint64_t, TreeLeaf& leaf);
        void lookup_or_create(uint64_t hash, TreeLeaf& leaf);
        void adjust_row_indexes(size_t min_row_ndx, int diff);
        void for_each(std::function<void(TreeLeaf*)>);

    private:
        static void cow(Array& ref, uint64_t masked_index, int levels, ref_type leaf_ref);
        static void for_each(Array& arr, std::function<void(TreeLeaf*)>);
    };

    ColumnBase* m_target_column;
    mutable Treetop m_top;
    mutable TreeLeaf m_current_leaf;

    int64_t get_key_value(size_t row);
    int64_t get_leaf_index(int64_t value, uint64_t* hash = nullptr) const;
    void do_delete(size_t row_ndx, int64_t value);
    void grow_tree();
};

class SortedListComparator {
public:
    SortedListComparator(ColumnBase& column_values);
    bool operator()(int64_t ndx, StringData needle);
    bool operator()(StringData needle, int64_t ndx);

private:
    ColumnBase& values;
};

} // namespace realm

#endif /* REALM_INDEX_INTEGER_HPP_ */
