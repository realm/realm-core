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
#ifndef REALM_COLUMN_TABLE_HPP
#define REALM_COLUMN_TABLE_HPP

#include <vector>

#include <realm/util/features.h>
#include <memory>
#include <realm/column.hpp>
#include <realm/table.hpp>

namespace realm {


/// Base class for any type of column that can contain subtables.
// FIXME: Don't derive from IntegerColumn, but define a BpTree<ref_type> specialization.
class SubtableColumnParent: public IntegerColumn, public Table::Parent {
public:
    void discard_child_accessors() REALM_NOEXCEPT;

    ~SubtableColumnParent() REALM_NOEXCEPT override;

    static ref_type create(Allocator&, std::size_t size = 0);

    Table* get_subtable_accessor(std::size_t) const REALM_NOEXCEPT override;

    void insert_rows(size_t, size_t, size_t) override;
    void erase_rows(size_t, size_t, size_t, bool) override;
    void move_last_row_over(size_t, size_t, bool) override;
    void clear(std::size_t, bool) override;
    void discard_subtable_accessor(std::size_t) REALM_NOEXCEPT override;
    void update_from_parent(std::size_t) REALM_NOEXCEPT override;
    void adj_acc_insert_rows(std::size_t, std::size_t) REALM_NOEXCEPT override;
    void adj_acc_erase_row(std::size_t) REALM_NOEXCEPT override;
    void adj_acc_move_over(std::size_t, std::size_t) REALM_NOEXCEPT override;
    void adj_acc_clear_root_table() REALM_NOEXCEPT override;
    void mark(int) REALM_NOEXCEPT override;
    void refresh_accessor_tree(std::size_t, const Spec&) override;

#ifdef REALM_DEBUG
    void verify() const override;
    void verify(const Table&, std::size_t) const override;
#endif

protected:
    /// A pointer to the table that this column is part of. For a free-standing
    /// column, this pointer is null.
    Table* const m_table;

    /// The index of this column within m_table.m_cols. For a free-standing
    /// column, this index is zero.
    std::size_t m_column_index;

    struct SubtableMap {
        ~SubtableMap() REALM_NOEXCEPT {}
        bool empty() const REALM_NOEXCEPT { return m_entries.empty(); }
        Table* find(std::size_t subtable_index) const REALM_NOEXCEPT;
        void add(std::size_t subtable_index, Table*);
        // Returns true if, and only if at least one entry was detached and
        // removed from the map.
        bool detach_and_remove_all() REALM_NOEXCEPT;
        // Returns true if, and only if the entry was found and removed, and it
        // was the last entry in the map.
        bool detach_and_remove(std::size_t subtable_index) REALM_NOEXCEPT;
        // Returns true if, and only if the entry was found and removed, and it
        // was the last entry in the map.
        bool remove(Table*) REALM_NOEXCEPT;
        void update_from_parent(std::size_t old_baseline) const REALM_NOEXCEPT;
        template<bool fix_index_in_parent>
        void adj_insert_rows(size_t row_index, size_t num_rows_inserted) REALM_NOEXCEPT;
        // Returns true if, and only if an entry was found and removed, and it
        // was the last entry in the map.
        template<bool fix_index_in_parent>
        bool adj_erase_rows(size_t row_index, size_t num_rows_erased) REALM_NOEXCEPT;
        // Returns true if, and only if an entry was found and removed, and it
        // was the last entry in the map.
        template<bool fix_index_in_parent>
        bool adj_move_over(std::size_t from_row_index, std::size_t to_row_index)
            REALM_NOEXCEPT;
        void update_accessors(const std::size_t* column_path_begin, const std::size_t* column_path_end,
                              _impl::TableFriend::AccessorUpdater&);
        void recursive_mark() REALM_NOEXCEPT;
        void refresh_accessor_tree(std::size_t spec_index_in_parent);
    private:
        struct entry {
            std::size_t m_subtable_index;
            Table* m_table;
        };
        typedef std::vector<entry> entries;
        entries m_entries;
    };

    /// Contains all existing accessors that are attached to a subtable in this
    /// column. It can map a row index into a pointer to the corresponding
    /// accessor when it exists.
    ///
    /// There is an invariant in force: Either `m_table` is null, or there is an
    /// additional referece count on `*m_table` when, and only when the map is
    /// non-empty.
    mutable SubtableMap m_subtable_map;

//    SubtableColumnParent(Allocator&, Table*, std::size_t column_index);

    SubtableColumnParent(Allocator&, ref_type, Table*, std::size_t column_index);

    /// Get a pointer to the accessor of the specified subtable. The
    /// accessor will be created if it does not already exist.
    ///
    /// The returned table pointer must **always** end up being
    /// wrapped in some instantiation of BasicTableRef<>.
    ///
    /// NOTE: This method must be used only for subtables with
    /// independent specs, i.e. for elements of a MixedColumn.
    Table* get_subtable_ptr(std::size_t subtable_index);

    // Overriding method in ArrayParent
    void update_child_ref(std::size_t, ref_type) override;

    // Overriding method in ArrayParent
    ref_type get_child_ref(std::size_t) const REALM_NOEXCEPT override;

    // Overriding method in Table::Parent
    Table* get_parent_table(std::size_t*) REALM_NOEXCEPT override;

    // Overriding method in Table::Parent
    void child_accessor_destroyed(Table*) REALM_NOEXCEPT override;

    /// Assumes that the two tables have the same spec.
    static bool compare_subtable_rows(const Table&, const Table&);

    /// Construct a copy of the columns array of the specified table
    /// and return just the ref to that array.
    ///
    /// In the clone, no string column will be of the enumeration
    /// type.
    ref_type clone_table_columns(const Table*);

    std::size_t* record_subtable_path(std::size_t* begin,
                                      std::size_t* end) REALM_NOEXCEPT override;

    void update_table_accessors(const std::size_t* column_path_begin, const std::size_t* column_path_end,
                                _impl::TableFriend::AccessorUpdater&);

    /// \param row_index Must be `realm::npos` if appending.
    void do_insert(std::size_t row_index, int_fast64_t value, std::size_t num_rows);

#ifdef REALM_DEBUG
    std::pair<ref_type, std::size_t>
    get_to_dot_parent(std::size_t index_in_parent) const override;
#endif

    friend class Table;
};



class SubtableColumn: public SubtableColumnParent {
public:
    /// Create a subtable column accessor and attach it to a
    /// preexisting underlying structure of arrays.
    ///
    /// \param table If this column is used as part of a table you must
    /// pass a pointer to that table. Otherwise you must pass null.
    ///
    /// \param column_index If this column is used as part of a table
    /// you must pass the logical index of the column within that
    /// table. Otherwise you should pass zero.
    SubtableColumn(Allocator&, ref_type, Table* table, std::size_t column_index);

    ~SubtableColumn() REALM_NOEXCEPT override {}

    std::size_t get_subtable_size(std::size_t index) const REALM_NOEXCEPT;

    /// Get a pointer to the accessor of the specified subtable. The
    /// accessor will be created if it does not already exist.
    ///
    /// The returned table pointer must **always** end up being
    /// wrapped in some instantiation of BasicTableRef<>.
    Table* get_subtable_ptr(std::size_t subtable_index);

    const Table* get_subtable_ptr(std::size_t subtable_index) const;

    // When passing a table to add() or insert() it is assumed that
    // the table spec is compatible with this column. The number of
    // columns must be the same, and the corresponding columns must
    // have the same data type (as returned by
    // Table::get_column_type()).

    void add(const Table* value = 0);
    void insert(std::size_t index, const Table* value = 0);
    void set(std::size_t index, const Table*);
    void clear_table(std::size_t index);

    using SubtableColumnParent::insert;

    void erase_rows(size_t, size_t, size_t, bool) override;
    void move_last_row_over(size_t, size_t, bool) override;

    /// Compare two subtable columns for equality.
    bool compare_table(const SubtableColumn&) const;

    void refresh_accessor_tree(std::size_t, const Spec&) override;

#ifdef REALM_DEBUG
    void verify(const Table&, std::size_t) const override;
    void do_dump_node_structure(std::ostream&, int) const override;
    void to_dot(std::ostream&, StringData title) const override;
#endif

private:
    mutable std::size_t m_subspec_index; // Unknown if equal to `npos`

    std::size_t get_subspec_index() const REALM_NOEXCEPT;

    void destroy_subtable(std::size_t index) REALM_NOEXCEPT;

    void do_discard_child_accessors() REALM_NOEXCEPT override;
};





// Implementation

// Overriding virtual method of Column.
inline void SubtableColumnParent::insert_rows(size_t row_index, size_t num_rows_to_insert,
                                              size_t prior_num_rows)
{
    REALM_ASSERT_DEBUG(prior_num_rows == size());
    REALM_ASSERT(row_index <= prior_num_rows);

    size_t row_index_2 = (row_index == prior_num_rows ? realm::npos : row_index);
    int_fast64_t value = 0;
    do_insert(row_index_2, value, num_rows_to_insert); // Throws
}

// Overriding virtual method of Column.
inline void SubtableColumnParent::erase_rows(size_t row_index, size_t num_rows_to_erase,
                                             size_t prior_num_rows,
                                             bool broken_reciprocal_backlinks)
{
    IntegerColumn::erase_rows(row_index, num_rows_to_erase, prior_num_rows,
                       broken_reciprocal_backlinks); // Throws

    const bool fix_index_in_parent = true;
    bool last_entry_removed =
        m_subtable_map.adj_erase_rows<fix_index_in_parent>(row_index, num_rows_to_erase);
    typedef _impl::TableFriend tf;
    if (last_entry_removed)
        tf::unbind_ref(*m_table);
}

// Overriding virtual method of Column.
inline void SubtableColumnParent::move_last_row_over(size_t row_index, size_t prior_num_rows,
                                                     bool broken_reciprocal_backlinks)
{
    IntegerColumn::move_last_row_over(row_index, prior_num_rows, broken_reciprocal_backlinks); // Throws

    const bool fix_index_in_parent = true;
    size_t last_row_index = prior_num_rows - 1;
    bool last_entry_removed =
        m_subtable_map.adj_move_over<fix_index_in_parent>(last_row_index, row_index);
    typedef _impl::TableFriend tf;
    if (last_entry_removed)
        tf::unbind_ref(*m_table);
}

inline void SubtableColumnParent::clear(std::size_t, bool)
{
    discard_child_accessors();
    clear_without_updating_index(); // Throws
    // FIXME: This one is needed because
    // IntegerColumn::clear_without_updating_index() forgets about the
    // leaf type. A better solution should probably be sought after.
    get_root_array()->set_type(Array::type_HasRefs);
}

inline void SubtableColumnParent::mark(int type) REALM_NOEXCEPT
{
    if (type & mark_Recursive)
        m_subtable_map.recursive_mark();
}

inline void SubtableColumnParent::refresh_accessor_tree(std::size_t column_index, const Spec& spec)
{
    IntegerColumn::refresh_accessor_tree(column_index, spec); // Throws
    m_column_index = column_index;
}

inline void SubtableColumnParent::adj_acc_insert_rows(std::size_t row_index,
                                                      std::size_t num_rows) REALM_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    const bool fix_index_in_parent = false;
    m_subtable_map.adj_insert_rows<fix_index_in_parent>(row_index, num_rows);
}

inline void SubtableColumnParent::adj_acc_erase_row(std::size_t row_index) REALM_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    const bool fix_index_in_parent = false;
    size_t num_rows_erased = 1;
    bool last_entry_removed =
        m_subtable_map.adj_erase_rows<fix_index_in_parent>(row_index, num_rows_erased);
    typedef _impl::TableFriend tf;
    if (last_entry_removed)
        tf::unbind_ref(*m_table);
}

inline void SubtableColumnParent::adj_acc_move_over(std::size_t from_row_index,
                                                    std::size_t to_row_index) REALM_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    const bool fix_index_in_parent = false;
    bool last_entry_removed =
        m_subtable_map.adj_move_over<fix_index_in_parent>(from_row_index, to_row_index);
    typedef _impl::TableFriend tf;
    if (last_entry_removed)
        tf::unbind_ref(*m_table);
}

inline void SubtableColumnParent::adj_acc_clear_root_table() REALM_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    IntegerColumn::adj_acc_clear_root_table();
    discard_child_accessors();
}

inline Table* SubtableColumnParent::get_subtable_accessor(std::size_t row_index) const
    REALM_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    Table* subtable = m_subtable_map.find(row_index);
    return subtable;
}

inline void SubtableColumnParent::discard_subtable_accessor(std::size_t row_index) REALM_NOEXCEPT
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    bool last_entry_removed = m_subtable_map.detach_and_remove(row_index);
    typedef _impl::TableFriend tf;
    if (last_entry_removed)
        tf::unbind_ref(*m_table);
}

inline void SubtableColumnParent::SubtableMap::add(std::size_t subtable_index, Table* table)
{
    entry e;
    e.m_subtable_index = subtable_index;
    e.m_table        = table;
    m_entries.push_back(e);
}

template<bool fix_index_in_parent>
void SubtableColumnParent::SubtableMap::adj_insert_rows(size_t row_index, size_t num_rows_inserted)
    REALM_NOEXCEPT
{
    typedef entries::iterator iter;
    iter end = m_entries.end();
    for (iter i = m_entries.begin(); i != end; ++i) {
        if (i->m_subtable_index >= row_index) {
            i->m_subtable_index += num_rows_inserted;
            typedef _impl::TableFriend tf;
            if (fix_index_in_parent)
                tf::set_index_in_parent(*(i->m_table), i->m_subtable_index);
        }
    }
}

template<bool fix_index_in_parent>
bool SubtableColumnParent::SubtableMap::adj_erase_rows(size_t row_index, size_t num_rows_erased)
    REALM_NOEXCEPT
{
    if (m_entries.empty())
        return false;
    typedef _impl::TableFriend tf;
    auto end = m_entries.end();
    auto i = m_entries.begin();
    do {
        if (i->m_subtable_index >= row_index + num_rows_erased) {
            i->m_subtable_index -= num_rows_erased;
            if (fix_index_in_parent)
                tf::set_index_in_parent(*(i->m_table), i->m_subtable_index);
        }
        else if (i->m_subtable_index >= row_index) {
            // Must hold a counted reference while detaching
            TableRef table(i->m_table);
            tf::detach(*table);
            // Move last over
            *i = *--end;
            continue;
        }
        ++i;
    }
    while (i != end);
    m_entries.erase(end, m_entries.end());
    return m_entries.empty();
}


template<bool fix_index_in_parent>
bool SubtableColumnParent::SubtableMap::adj_move_over(std::size_t from_row_index,
                                                      std::size_t to_row_index) REALM_NOEXCEPT
{
    typedef _impl::TableFriend tf;

    std::size_t i = 0, n = m_entries.size();
    // We return true if, and only if we remove the last entry in the map.  We
    // need special handling for the case, where the set of entries are already
    // empty, otherwise the final return statement would return true in this
    // case, even though we didn't actually remove an entry.
    if (i == n)
        return false;

    while (i < n) {
        entry& e = m_entries[i];
        if (REALM_UNLIKELY(e.m_subtable_index == to_row_index)) {
            // Must hold a counted reference while detaching
            TableRef table(e.m_table);
            tf::detach(*table);
            // Delete entry by moving last over (faster and avoids invalidating
            // iterators)
            e = m_entries[--n];
            m_entries.pop_back();
        }
        else {
            if (REALM_UNLIKELY(e.m_subtable_index == from_row_index)) {
                e.m_subtable_index = to_row_index;
                if (fix_index_in_parent)
                    tf::set_index_in_parent(*(e.m_table), e.m_subtable_index);
            }
            ++i;
        }
    }
    return m_entries.empty();
}

inline SubtableColumnParent::SubtableColumnParent(Allocator& alloc, ref_type ref,
                                                  Table* table, std::size_t column_index):
    IntegerColumn(alloc, ref), // Throws
    m_table(table),
    m_column_index(column_index)
{
}

inline void SubtableColumnParent::update_child_ref(std::size_t child_index, ref_type new_ref)
{
    set(child_index, new_ref);
}

inline ref_type SubtableColumnParent::get_child_ref(std::size_t child_index) const REALM_NOEXCEPT
{
    return get_as_ref(child_index);
}

inline void SubtableColumnParent::discard_child_accessors() REALM_NOEXCEPT
{
    bool last_entry_removed = m_subtable_map.detach_and_remove_all();
    if (last_entry_removed && m_table)
        _impl::TableFriend::unbind_ref(*m_table);
}

inline SubtableColumnParent::~SubtableColumnParent() REALM_NOEXCEPT
{
    discard_child_accessors();
}

inline bool SubtableColumnParent::compare_subtable_rows(const Table& a, const Table& b)
{
    return _impl::TableFriend::compare_rows(a,b);
}

inline ref_type SubtableColumnParent::clone_table_columns(const Table* t)
{
    return _impl::TableFriend::clone_columns(*t, get_root_array()->get_alloc());
}

inline ref_type SubtableColumnParent::create(Allocator& alloc, std::size_t size)
{
    return IntegerColumn::create(alloc, Array::type_HasRefs, size); // Throws
}

inline std::size_t* SubtableColumnParent::record_subtable_path(std::size_t* begin,
                                                               std::size_t* end) REALM_NOEXCEPT
{
    if (end == begin)
        return 0; // Error, not enough space in buffer
    *begin++ = m_column_index;
    if (end == begin)
        return 0; // Error, not enough space in buffer
    return _impl::TableFriend::record_subtable_path(*m_table, begin, end);
}

inline void SubtableColumnParent::
update_table_accessors(const std::size_t* column_path_begin, const std::size_t* column_path_end,
                       _impl::TableFriend::AccessorUpdater& updater)
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    m_subtable_map.update_accessors(column_path_begin, column_path_end, updater); // Throws
}

inline void SubtableColumnParent::do_insert(std::size_t row_index, int_fast64_t value,
                                            std::size_t num_rows)
{
    IntegerColumn::insert_without_updating_index(row_index, value, num_rows); // Throws
    bool is_append = row_index == realm::npos;
    if (!is_append) {
        const bool fix_index_in_parent = true;
        m_subtable_map.adj_insert_rows<fix_index_in_parent>(row_index, num_rows);
    }
}


inline SubtableColumn::SubtableColumn(Allocator& alloc, ref_type ref,
                                      Table* table, std::size_t column_index):
    SubtableColumnParent(alloc, ref, table, column_index),
    m_subspec_index(realm::npos)
{
}

inline const Table* SubtableColumn::get_subtable_ptr(std::size_t subtable_index) const
{
    return const_cast<SubtableColumn*>(this)->get_subtable_ptr(subtable_index);
}

inline void SubtableColumn::refresh_accessor_tree(std::size_t column_index, const Spec& spec)
{
    SubtableColumnParent::refresh_accessor_tree(column_index, spec); // Throws
    m_subspec_index = spec.get_subspec_index(column_index);
    m_subtable_map.refresh_accessor_tree(m_subspec_index); // Throws
}

inline std::size_t SubtableColumn::get_subspec_index() const REALM_NOEXCEPT
{
    if (REALM_UNLIKELY(m_subspec_index == realm::npos)) {
        typedef _impl::TableFriend tf;
        const Spec& spec = tf::get_spec(*m_table);
        m_subspec_index = spec.get_subspec_index(m_column_index);
    }
    return m_subspec_index;
}


} // namespace realm

#endif // REALM_COLUMN_TABLE_HPP
