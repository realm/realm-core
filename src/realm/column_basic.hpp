/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] Realm Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Realm Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Realm Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Realm Incorporated.
 *
 **************************************************************************/
#ifndef REALM_COLUMN_BASIC_HPP
#define REALM_COLUMN_BASIC_HPP

#include <realm/column.hpp>
#include <realm/column_tpl.hpp>
#include <realm/array_basic.hpp>

namespace realm {

template<class T>
struct AggReturnType {
    typedef T sum_type;
};
template<>
struct AggReturnType<float>
{
    typedef double sum_type;
};

// FIXME: Remove this - it's unused except in tests.
template<>
struct ColumnTypeTraits<int> {
    using column_type = BasicColumn<int>;
    using leaf_type = ArrayInteger;
    using sum_type = int;
    static const DataType id = type_Int;
    static const ColumnType column_id = col_type_Int;
    static const ColumnType real_column_type = col_type_Int;
};



/// A basic column (BasicColumn<T>) is a single B+-tree, and the root
/// of the column is the root of the B+-tree. All leaf nodes are
/// single arrays of type BasicArray<T>.
///
/// A basic column can currently only be used for simple unstructured
/// types like float, double.
template<class T>
class BasicColumn : public ColumnBaseSimple, public ColumnTemplate<T> {
public:
    using LeafType = typename ColumnTypeTraits<T>::leaf_type;
    using value_type = T;

    // The FloatColumn and DoubleColumn only exists as class types that support
    // null (there is no separate typed nullable and non-nullable versions).
    // Both have a `bool m_nullable` flag which is set in their constructor
    // according to the m_spec
    static const bool nullable = true;

    BasicColumn(Allocator&, ref_type, bool nullable);

    size_t size() const noexcept final;
    bool is_empty() const noexcept { return size() == 0; }

    bool is_nullable() const noexcept override
    {
        return m_nullable;
    }
    bool is_null(size_t index) const noexcept override
    {
        if (!m_nullable)
            return false;

        return null::is_null_float(get(index));
    }

    void set_null(size_t index) override
    {
        REALM_ASSERT(m_nullable);
        if (!m_array->is_inner_bptree_node()) {
            static_cast<BasicArray<T>*>(m_array.get())->set(index, null::get_null_float<T>()); // Throws
            return;
        }
        SetLeafElem set_leaf_elem(m_array->get_alloc(), null::get_null_float<T>());
        m_array->update_bptree_elem(index, set_leaf_elem); // Throws
    }

    struct LeafInfo {
        const BasicArray<T>** out_leaf_ptr;
        BasicArray<T>* in_fallback;
    };

    void get_leaf(size_t ndx, size_t& ndx_in_leaf,
                          LeafInfo& inout_leaf_info) const noexcept;

    T get(size_t ndx) const noexcept;
    StringData get_index_data(size_t, StringIndex::StringConversionBuffer& buffer) const noexcept final;
    void add(T value = T());
    void set(size_t ndx, T value);
    void insert(size_t ndx, T value = T());
    void erase(size_t row_ndx);
    void erase(size_t row_ndx, bool is_last);
    void move_last_over(size_t row_ndx);
    void swap_rows(size_t row_ndx_1, size_t row_ndx_2) override;
    void clear();

    size_t count(T value) const;

    typedef typename AggReturnType<T>::sum_type SumType;
    SumType sum(size_t begin = 0, size_t end = npos,
                size_t limit = size_t(-1), size_t* return_ndx = nullptr) const;

    double average(size_t begin = 0, size_t end = npos,
                   size_t limit = size_t(-1), size_t* return_ndx = nullptr) const;

    T maximum(size_t begin = 0, size_t end = npos,
              size_t limit = size_t(-1), size_t* return_ndx = nullptr) const;

    T minimum(size_t begin = 0, size_t end = npos,
              size_t limit = size_t(-1), size_t* return_ndx = nullptr) const;

    size_t find_first(T value, size_t begin = 0 , size_t end = npos) const;

    void find_all(IntegerColumn& result, T value, size_t begin = 0, size_t end = npos) const;

    //@{
    /// Find the lower/upper bound for the specified value assuming
    /// that the elements are already sorted in ascending order.
    size_t lower_bound(T value) const noexcept;
    size_t upper_bound(T value) const noexcept;
    //@{

    /// Compare two columns for equality.
    bool compare(const BasicColumn&) const;

    static ref_type create(Allocator&, size_t size = 0);

    // Overrriding method in ColumnBase
    ref_type write(size_t, size_t, size_t,
                   _impl::OutputStream&) const override;

    void insert_rows(size_t, size_t, size_t) override;
    void erase_rows(size_t, size_t, size_t, bool) override;
    void move_last_row_over(size_t, size_t, bool) override;
    void clear(size_t, bool) override;
    void refresh_accessor_tree(size_t, const Spec&) override;

#ifdef REALM_DEBUG
    void verify() const override;
    void to_dot(std::ostream&, StringData title) const override;
    void do_dump_node_structure(std::ostream&, int) const override;
#endif

protected:
    T get_val(size_t row) const override { return get(row); }

private:
    /// \param row_ndx Must be `realm::npos` if appending.
    void do_insert(size_t row_ndx, T value, size_t num_rows);

    // Called by Array::bptree_insert().
    static ref_type leaf_insert(MemRef leaf_mem, ArrayParent&, size_t ndx_in_parent,
                                Allocator&, size_t insert_ndx,
                                Array::TreeInsert<BasicColumn<T>>&);

    class SetLeafElem;
    class EraseLeafElem;
    class CreateHandler;
    class SliceHandler;

    void do_move_last_over(size_t row_ndx, size_t last_row_ndx);
    void do_swap_rows(size_t row_ndx_1, size_t row_ndx_2);
    void do_clear();

    bool m_nullable;

#ifdef REALM_DEBUG
    static size_t verify_leaf(MemRef, Allocator&);
    void leaf_to_dot(MemRef, ArrayParent*, size_t ndx_in_parent,
                     std::ostream&) const override;
    static void leaf_dumper(MemRef, Allocator&, std::ostream&, int level);
#endif

    friend class Array;
    friend class ColumnBase;
};

template<class T>
void BasicColumn<T>::get_leaf(size_t ndx, size_t& ndx_in_leaf,
                                         LeafInfo& leaf) const noexcept
{
    if (!m_array->is_inner_bptree_node()) {
        ndx_in_leaf = ndx;
        *leaf.out_leaf_ptr = static_cast<const BasicArray<T>*>(m_array.get());
        return;
    }
    std::pair<MemRef, size_t> p = m_array->get_bptree_leaf(ndx);
    leaf.in_fallback->init_from_mem(p.first);
    *leaf.out_leaf_ptr = leaf.in_fallback;
    ndx_in_leaf = p.second;
}

template<class T>
StringData BasicColumn<T>::get_index_data(size_t, StringIndex::StringConversionBuffer&) const noexcept
{
    REALM_ASSERT(false && "Index not supported for floating-point columns yet.");
    REALM_UNREACHABLE();
}


} // namespace realm


// template implementation
#include <realm/column_basic_tpl.hpp>


#endif // REALM_COLUMN_BASIC_HPP
