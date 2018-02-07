/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
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

#ifndef REALM_TABLE_HPP
#define REALM_TABLE_HPP

#include <algorithm>
#include <map>
#include <utility>
#include <typeinfo>
#include <memory>
#include <mutex>

#include <realm/util/features.h>
#include <realm/util/thread.hpp>
#include <realm/table_ref.hpp>
#include <realm/list.hpp>
#include <realm/spec.hpp>
#include <realm/query.hpp>
#include <realm/cluster_tree.hpp>
#include <realm/keys.hpp>

namespace realm {

class BacklinkColumn;
class BinaryColumy;
class ConstTableView;
class Group;
class LinkColumn;
class LinkColumnBase;
class LinkListColumn;
class LinkView;
class SortDescriptor;
class StringIndex;
class TableView;
class TableViewBase;
class TimestampColumn;
template <class>
class Columns;
template <class>
class SubQuery;
struct LinkTargetInfo;

struct Link {
};
typedef Link BackLink;


namespace _impl {
class TableFriend;
}
namespace metrics {
class QueryInfo;
}

class Replication;

class Table {
public:
    /// Construct a new freestanding top-level table with static
    /// lifetime.
    ///
    /// This constructor should be used only when placing a table
    /// instance on the stack, and it is then the responsibility of
    /// the application that there are no objects of type TableRef or
    /// ConstTableRef that refer to it, or to any of its subtables,
    /// when it goes out of scope.
    Table(Allocator& = Allocator::get_default());

    /// Construct a copy of the specified table as a new freestanding
    /// top-level table with static lifetime.
    ///
    /// This constructor should be used only when placing a table
    /// instance on the stack, and it is then the responsibility of
    /// the application that there are no objects of type TableRef or
    /// ConstTableRef that refer to it, or to any of its subtables,
    /// when it goes out of scope.
    Table(const Table&, Allocator& = Allocator::get_default());
    void revive(Allocator& new_allocator);

    ~Table() noexcept;

    Allocator& get_alloc() const;

    /// Construct a copy of the specified table as a new freestanding top-level
    /// table with dynamic lifetime. This method is deprecated.
    TableRef copy(Allocator& = Allocator::get_default()) const;

    /// Get the name of this table, if it has one. Only group-level tables have
    /// names. For a table of any other kind, this function returns the empty
    /// string.
    StringData get_name() const noexcept;

    // Whether or not elements can be null.
    bool is_nullable(ColKey col_key) const;

    //@{
    /// Conventience functions for inspecting the dynamic table type.
    ///
    size_t get_column_count() const noexcept;
    DataType get_column_type(ColKey column_key) const noexcept;
    StringData get_column_name(ColKey column_key) const noexcept;
    ColumnAttrMask get_column_attr(ColKey column_key) const noexcept;
    ColKey get_column_key(StringData name) const noexcept;
    ColKey find_backlink_column(TableKey origin_table_key, ColKey origin_col_key) const noexcept;
    //@}

    //@{
    /// Convenience functions for manipulating the dynamic table type.
    ///
    static const size_t max_column_name_length = 63;
    static const uint64_t max_num_columns = 0xFFFFUL; // <-- must be power of two -1
    ColKey add_column(DataType type, StringData name, bool nullable = false);
    ColKey add_column_list(DataType type, StringData name);
    ColKey add_column_link(DataType type, StringData name, Table& target, LinkType link_type = link_Weak);
    std::vector<ColKey> get_col_keys() const;

    // Pass a ColKey() as first argument to have a new colkey generated
    // Requesting a specific ColKey may fail with invalidkey exception, if the key is already in use
    // We recommend allowing Core to choose the ColKey.
    ColKey insert_column(ColKey col_key, DataType type, StringData name, bool nullable = false);
    ColKey insert_column_link(ColKey col_key, DataType type, StringData name, Table& target,
                              LinkType link_type = link_Weak);
    void remove_column(ColKey col_key);
    void rename_column(ColKey col_key, StringData new_name);
    bool valid_column(ColKey col_key) const;
    //@}

    /// There are two kinds of links, 'weak' and 'strong'. A strong link is one
    /// that implies ownership, i.e., that the origin object (parent) owns the
    /// target parent (child). Simply stated, this means that when the origin object
    /// (parent) is removed, so is the target object (child). If there are multiple
    /// strong links to an object, the origin objects share ownership, and the
    /// target object is removed when the last owner disappears. Weak links do not
    /// imply ownership, and will be nullified or removed when the target object
    /// disappears.
    ///
    /// To put this in precise terms; when a strong link is broken, and the
    /// target object has no other strong links to it, the target object is removed. A
    /// object that is implicitly removed in this way, is said to be
    /// *cascade-removed*. When a weak link is broken, nothing is
    /// cascade-removed.
    ///
    /// A link is considered broken if
    ///
    ///  - the link is nullified, removed, or replaced by a different link
    ///
    ///  - the origin object is explicitly removed
    ///
    ///  - the origin object is cascade-removed, or if
    ///
    ///  - the origin field is removed from the table (Table::remove_column()),
    ///    or if
    ///
    ///  - the origin table is removed from the group.
    ///
    /// Note that a link is *not* considered broken when it is replaced by a
    /// link to the same target object. I.e., no objects will be cascade-removed
    /// due to such an operation.
    ///
    /// When a object is explicitly removed (such as by Table::move_last_over()),
    /// all links to it are automatically removed or nullified. For single link
    /// columns (type_Link), links to the removed object are nullified. For link
    /// list columns (type_LinkList), links to the removed object are removed from
    /// the list.
    ///
    /// When a object is cascade-removed there can no longer be any strong links to
    /// it, but if there are any weak links, they will be removed or nullified.
    ///
    /// It is important to understand that this cascade-removal scheme is too
    /// simplistic to enable detection and removal of orphaned link-cycles. In
    /// this respect, it suffers from the same limitations as a reference
    /// counting scheme generally does.
    ///
    /// It is also important to understand, that the possible presence of a link
    /// cycle can cause a object to be cascade-removed as a consequence of being
    /// modified. This happens, for example, if two objects, A and B, have strong
    /// links to each other, and there are no other strong links to either of
    /// them. In this case, if A->B is changed to A->C, then both A and B will
    /// be cascade-removed. This can lead to obscure bugs in some applications.
    ///
    /// \param col_key The key of the link column (`type_Link` or
    /// `type_LinkList`) to be modified.
    ///
    /// \param link_type The type of links the column should store.
    void set_link_type(ColKey col_key, LinkType);
    LinkType get_link_type(ColKey col_key) const;

    //@{

    /// has_search_index() returns true if, and only if a search index has been
    /// added to the specified column. Rather than throwing, it returns false if
    /// the table accessor is detached or the specified index is out of range.
    ///
    /// add_search_index() adds a search index to the specified column of the
    /// table. It has no effect if a search index has already been added to the
    /// specified column (idempotency).
    ///
    /// remove_search_index() removes the search index from the specified column
    /// of the table. It has no effect if the specified column has no search
    /// index. The search index cannot be removed from the primary key of a
    /// table.
    ///
    /// \param col_key The key of a column of the table.

    bool has_search_index(ColKey col_key) const noexcept;
    void add_search_index(ColKey col_key);
    void remove_search_index(ColKey col_key);

    //@}

    /// If the specified column is optimized to store only unique values, then
    /// this function returns the number of unique values currently
    /// stored. Otherwise it returns zero. This function is mainly intended for
    /// debugging purposes.
    size_t get_num_unique_values(ColKey col_key) const;

    bool has_clusters() const
    {
        return m_clusters.is_attached();
    }

    template <class T>
    Columns<T> column(ColKey col_key); // FIXME: Should this one have been declared noexcept?
    template <class T>
    Columns<T> column(const Table& origin, ColKey origin_col_key);

    template <class T>
    SubQuery<T> column(ColKey col_key, Query subquery);
    template <class T>
    SubQuery<T> column(const Table& origin, ColKey origin_col_key, Query subquery);

    // Table size and deletion
    bool is_empty() const noexcept;
    size_t size() const noexcept;

    //@{

    /// Object handling.

    // Create an object with key. If the key is omitted, a key will be generated by the system
    Obj create_object(ObjKey key = {});
    /// Create a number of objects and add corresponding keys to a vector
    void create_objects(size_t number, std::vector<ObjKey>& keys);
    /// Create a number of objects with keys supplied
    void create_objects(const std::vector<ObjKey>& keys);
    /// Does the key refer to an object within the table?
    bool is_valid(ObjKey key) const
    {
        return m_clusters.is_valid(key);
    }
    Obj get_object(ObjKey key)
    {
        return m_clusters.get(key);
    }
    ConstObj get_object(ObjKey key) const
    {
        return m_clusters.get(key);
    }
    void dump_objects()
    {
        return m_clusters.dump_objects();
    }

    bool traverse_clusters(ClusterTree::TraverseFunction func) const
    {
        return m_clusters.traverse(func);
    }

    /// remove_object() removes the specified object from the table.
    /// The removal of an object a table may cause other linked objects to be
    /// cascade-removed. The clearing of a table may also cause linked objects
    /// to be cascade-removed, but in this respect, the effect is exactly as if
    /// each object had been removed individually. See set_link_type() for details.
    void remove_object(ObjKey key);
    /// remove_object_recursive() will delete linked rows if the removed link was the
    /// last one holding on to the row in question. This will be done recursively.
    void remove_object_recursive(ObjKey key);
    void clear();
    using Iterator = ClusterTree::Iterator;
    using ConstIterator = ClusterTree::ConstIterator;
    ConstIterator begin() const;
    ConstIterator end() const;
    Iterator begin();
    Iterator end();
    void remove_object(const ConstIterator& it)
    {
        remove_object(it->get_key());
    }
    //@}


    TableRef get_link_target(ColKey column_key) noexcept;
    ConstTableRef get_link_target(ColKey column_key) const noexcept;

    static const size_t max_string_size = 0xFFFFF8 - Array::header_size - 1;
    static const size_t max_binary_size = 0xFFFFF8 - Array::header_size;

    // FIXME: These limits should be chosen independently of the underlying
    // platform's choice to define int64_t and independent of the integer
    // representation. The current values only work for 2's complement, which is
    // not guaranteed by the standard.
    static constexpr int_fast64_t max_integer = std::numeric_limits<int64_t>::max();
    static constexpr int_fast64_t min_integer = std::numeric_limits<int64_t>::min();

    //@{

    /// Deprecated:

    /// If this accessor is attached to a subtable, then that subtable has a
    /// parent table, and the subtable either resides in a column of type
    /// `table` or of type `mixed` in that parent. In that case
    /// get_parent_table() returns a reference to the accessor associated with
    /// the parent, and get_parent_row_index() returns the index of the row in
    /// which the subtable resides. In all other cases (free-standing and
    /// group-level tables), get_parent_table() returns null and
    /// get_parent_row_index() returns realm::npos.
    ///
    /// If this accessor is attached to a subtable, and \a col_key_out is
    /// specified, then `*col_key_out` is set to the index of the column of
    /// the parent table in which the subtable resides. If this accessor is not
    /// attached to a subtable, then `*col_key_out` will retain its original
    /// value upon return.

    TableRef get_parent_table(size_t* col_key_out = nullptr) noexcept;
    ConstTableRef get_parent_table(size_t* col_key_out = nullptr) const noexcept;
    size_t get_parent_row_index() const noexcept;

    //@}


    /// Only group-level unordered tables can be used as origins or targets of
    /// links.
    bool is_group_level() const noexcept;

    /// If this table is a group-level table, then this function returns the
    /// index of this table within the group. Otherwise it returns realm::npos.
    size_t get_index_in_group() const noexcept;
    TableKey get_key() const noexcept;
    // Get the key of this table directly, without needing a Table accessor.
    static TableKey get_key_direct(Allocator& alloc, ref_type top_ref);

    // Aggregate functions
    size_t count_int(ColKey col_key, int64_t value) const;
    size_t count_string(ColKey col_key, StringData value) const;
    size_t count_float(ColKey col_key, float value) const;
    size_t count_double(ColKey col_key, double value) const;

    int64_t sum_int(ColKey col_key) const;
    double sum_float(ColKey col_key) const;
    double sum_double(ColKey col_key) const;
    int64_t maximum_int(ColKey col_key, ObjKey* return_ndx = nullptr) const;
    float maximum_float(ColKey col_key, ObjKey* return_ndx = nullptr) const;
    double maximum_double(ColKey col_key, ObjKey* return_ndx = nullptr) const;
    Timestamp maximum_timestamp(ColKey col_key, ObjKey* return_ndx = nullptr) const;
    int64_t minimum_int(ColKey col_key, ObjKey* return_ndx = nullptr) const;
    float minimum_float(ColKey col_key, ObjKey* return_ndx = nullptr) const;
    double minimum_double(ColKey col_key, ObjKey* return_ndx = nullptr) const;
    Timestamp minimum_timestamp(ColKey col_key, ObjKey* return_ndx = nullptr) const;
    double average_int(ColKey col_key, size_t* value_count = nullptr) const;
    double average_float(ColKey col_key, size_t* value_count = nullptr) const;
    double average_double(ColKey col_key, size_t* value_count = nullptr) const;

    // Will return pointer to search index accessor. Will return nullptr if no index
    StringIndex* get_search_index(ColKey col) const noexcept
    {
        size_t column_ndx = colkey2ndx(col);
        REALM_ASSERT(column_ndx < m_index_accessors.size());
        return m_index_accessors[column_ndx];
    }

    template <class T>
    ObjKey find_first(ColKey col_key, T value) const;

    //    Key find_first_link(Key target_key) const;
    ObjKey find_first_int(ColKey col_key, int64_t value) const;
    ObjKey find_first_bool(ColKey col_key, bool value) const;
    ObjKey find_first_timestamp(ColKey col_key, Timestamp value) const;
    ObjKey find_first_float(ColKey col_key, float value) const;
    ObjKey find_first_double(ColKey col_key, double value) const;
    ObjKey find_first_string(ColKey col_key, StringData value) const;
    ObjKey find_first_binary(ColKey col_key, BinaryData value) const;
    ObjKey find_first_null(ColKey col_key) const;

    //    TableView find_all_link(Key target_key);
    //    ConstTableView find_all_link(Key target_key) const;
    TableView find_all_int(ColKey col_key, int64_t value);
    ConstTableView find_all_int(ColKey col_key, int64_t value) const;
    TableView find_all_bool(ColKey col_key, bool value);
    ConstTableView find_all_bool(ColKey col_key, bool value) const;
    TableView find_all_float(ColKey col_key, float value);
    ConstTableView find_all_float(ColKey col_key, float value) const;
    TableView find_all_double(ColKey col_key, double value);
    ConstTableView find_all_double(ColKey col_key, double value) const;
    TableView find_all_string(ColKey col_key, StringData value);
    ConstTableView find_all_string(ColKey col_key, StringData value) const;
    TableView find_all_binary(ColKey col_key, BinaryData value);
    ConstTableView find_all_binary(ColKey col_key, BinaryData value) const;
    TableView find_all_null(ColKey col_key);
    ConstTableView find_all_null(ColKey col_key) const;

    /// The following column types are supported: String, Integer, OldDateTime, Bool
    TableView get_distinct_view(ColKey col_key);
    ConstTableView get_distinct_view(ColKey col_key) const;

    TableView get_sorted_view(ColKey col_key, bool ascending = true);
    ConstTableView get_sorted_view(ColKey col_key, bool ascending = true) const;

    TableView get_sorted_view(SortDescriptor order);
    ConstTableView get_sorted_view(SortDescriptor order) const;

    TableView get_backlink_view(ObjKey key, Table* src_table, ColKey src_col_key);

    // Report the current content version. This is a 64-bit value which is bumped whenever
    // the content in the table changes.
    uint_fast64_t get_content_version() const noexcept;

    // Report the current instance version. This is a 64-bit value which is bumped
    // whenever the table accessor is recycled.
    uint_fast64_t get_instance_version() const noexcept;

    // Report the current storage version. This is a 64-bit value which is bumped
    // whenever the location in memory of any part of the table changes.
    uint_fast64_t get_storage_version(uint64_t instance_version) const;
    void bump_storage_version() const noexcept;
    void bump_content_version() const noexcept;

private:
    template <class T>
    TableView find_all(ColKey col_key, T value);
    // insert a mapping, moving all later mappings to a higher index
    void insert_col_mapping(size_t ndx, ColKey key);
    // remove a mapping, moving all later mappings to a lower index
    void remove_col_mapping(size_t ndx);
    ColKey generate_col_key();

public:
    size_t colkey2ndx(ColKey key) const;
    ColKey ndx2colkey(size_t ndx) const;
    void verify_inv() const;
    //@{
    /// Find the lower/upper bound according to a column that is
    /// already sorted in ascending order.
    ///
    /// For an integer column at index 0, and an integer value '`v`',
    /// lower_bound_int(0,v) returns the index '`l`' of the first row
    /// such that `get_int(0,l) &ge; v`, and upper_bound_int(0,v)
    /// returns the index '`u`' of the first row such that
    /// `get_int(0,u) &gt; v`. In both cases, if no such row is found,
    /// the returned value is the number of rows in the table.
    ///
    ///     3 3 3 4 4 4 5 6 7 9 9 9
    ///     ^     ^     ^     ^     ^
    ///     |     |     |     |     |
    ///     |     |     |     |      -- Lower and upper bound of 15
    ///     |     |     |     |
    ///     |     |     |      -- Lower and upper bound of 8
    ///     |     |     |
    ///     |     |      -- Upper bound of 4
    ///     |     |
    ///     |      -- Lower bound of 4
    ///     |
    ///      -- Lower and upper bound of 1
    ///
    /// These functions are similar to std::lower_bound() and
    /// std::upper_bound().
    ///
    /// The string versions assume that the column is sorted according
    /// to StringData::operator<().
    ///
    /// FIXME: Deprecate or change to return ObjKey.
    size_t lower_bound_int(ColKey col_key, int64_t value) const noexcept;
    size_t upper_bound_int(ColKey col_key, int64_t value) const noexcept;
    size_t lower_bound_bool(ColKey col_key, bool value) const noexcept;
    size_t upper_bound_bool(ColKey col_key, bool value) const noexcept;
    size_t lower_bound_float(ColKey col_key, float value) const noexcept;
    size_t upper_bound_float(ColKey col_key, float value) const noexcept;
    size_t lower_bound_double(ColKey col_key, double value) const noexcept;
    size_t upper_bound_double(ColKey col_key, double value) const noexcept;
    size_t lower_bound_string(ColKey col_key, StringData value) const noexcept;
    size_t upper_bound_string(ColKey col_key, StringData value) const noexcept;
    //@}

    // Queries
    // Using where(tv) is the new method to perform queries on TableView. The 'tv' can have any order; it does not
    // need to be sorted, and, resulting view retains its order.
    Query where(TableViewBase* tv = nullptr)
    {
        return Query(*this, tv);
    }

    // FIXME: We need a ConstQuery class or runtime check against modifications in read transaction.
    Query where(TableViewBase* tv = nullptr) const
    {
        return Query(*this, tv);
    }

    // Perform queries on a LinkView. The returned Query holds a reference to list.
    Query where(const LinkListPtr& list)
    {
        return Query(*this, list);
    }

    Table& link(ColKey link_column);
    Table& backlink(const Table& origin, ColKey origin_col_key);

    // Optimizing. enforce == true will enforce enumeration of all string columns;
    // enforce == false will auto-evaluate if they should be enumerated or not
    void optimize(bool enforce = false);

    // Conversion
    void to_json(std::ostream& out, size_t link_depth = 0,
                 std::map<std::string, std::string>* renames = nullptr) const;
    void to_string(std::ostream& out, size_t limit = 500) const;
    void row_to_string(ObjKey key, std::ostream& out) const;

    // Get a reference to this table
    TableRef get_table_ref()
    {
        return TableRef(this);
    }
    ConstTableRef get_table_ref() const
    {
        return ConstTableRef(this);
    }

    /// \brief Compare two tables for equality.
    ///
    /// Two tables are equal if they have equal descriptors
    /// (`Descriptor::operator==()`) and equal contents. Equal descriptors imply
    /// that the two tables have the same columns in the same order. Equal
    /// contents means that the two tables must have the same number of rows,
    /// and that for each row index, the two rows must have the same values in
    /// each column.
    ///
    /// In mixed columns, both the value types and the values are required to be
    /// equal.
    ///
    /// For a particular row and column, if the two values are themselves tables
    /// (subtable and mixed columns) value equality implies a recursive
    /// invocation of `Table::operator==()`.
    bool operator==(const Table&) const;

    /// \brief Compare two tables for inequality.
    ///
    /// See operator==().
    bool operator!=(const Table& t) const;

    /// Compute the sum of the sizes in number of bytes of all the array nodes
    /// that currently make up this table. See also
    /// Group::compute_aggregate_byte_size().
    ///
    /// If this table accessor is the detached state, this function returns
    /// zero.
    size_t compute_aggregated_byte_size() const noexcept;

    // Debug
    void verify() const;
#ifdef REALM_DEBUG
    void to_dot(std::ostream&, StringData title = StringData()) const;
    void print() const;
    MemStats stats() const;
    void dump_node_structure() const; // To std::cerr (for GDB)
    void dump_node_structure(std::ostream&, int level) const;
#endif

    class Parent;
    using HandoverPatch = TableHandoverPatch;
    static void generate_patch(const Table* ref, std::unique_ptr<HandoverPatch>& patch);
    static TableRef create_from_and_consume_patch(std::unique_ptr<HandoverPatch>& patch, Group& group);

protected:
    /// Compare the objects of two tables under the assumption that the two tables
    /// have the same number of columns, and the same data type at each column
    /// index (as expressed through the DataType enum).
    bool compare_objects(const Table&) const;

    void check_lists_are_empty(size_t row_ndx) const;

private:
    class SliceWriter;

    mutable WrappedAllocator m_alloc;
    Array m_top;

    using SpecPtr = std::unique_ptr<Spec>;
    SpecPtr m_spec;         // 1st slot in m_top
    ClusterTree m_clusters; // 3rd slot in m_top
    int64_t m_next_key_value = -1;
    TableKey m_key;     // 4th slot in m_top
    Array m_index_refs; // 5th slot in m_top
    std::vector<StringIndex*> m_index_accessors;

    // Used for queries: Items are added with link() method during buildup of query
    mutable std::vector<ColKey> m_link_chain;

    void batch_erase_rows(const KeyColumn& keys);
    void do_remove_object(ObjKey key);
    size_t do_set_link(ColKey col_key, size_t row_ndx, size_t target_row_ndx);

    void populate_search_index(ColKey column_ndx);
    void rebuild_search_index(size_t current_file_format_version);

    /// Disable copying assignment.
    ///
    /// It could easily be implemented by calling assign(), but the
    /// non-checking nature of the low-level dynamically typed API
    /// makes it too risky to offer this feature as an
    /// operator.
    ///
    /// FIXME: assign() has not yet been implemented, but the
    /// intention is that it will copy the rows of the argument table
    /// into this table after clearing the original contents, and for
    /// target tables without a shared spec, it would also copy the
    /// spec. For target tables with shared spec, it would be an error
    /// to pass an argument table with an incompatible spec, but
    /// assign() would not check for spec compatibility. This would
    /// make it ideal as a basis for implementing operator=() for
    /// typed tables.
    Table& operator=(const Table&) = delete;

    /// Used when constructing an accessor whose lifetime is going to be managed
    /// by reference counting. The lifetime of accessors of free-standing tables
    /// allocated on the stack by the application is not managed by reference
    /// counting, so that is a case where this tag must **not** be specified.
    class ref_count_tag {
    };

    /// Create an uninitialized accessor whose lifetime is managed by Group
    Table(ref_count_tag, Allocator&);

    void init(ref_type top_ref, ArrayParent*, size_t ndx_in_parent, bool skip_create_column_accessors = false);

    ColKey do_insert_column(ColKey col_key, DataType type, StringData name, LinkTargetInfo& link_target_info,
                            bool nullable = false, bool listtype = false);
    ColKey do_insert_column_unless_exists(ColKey col_key, DataType type, StringData name, LinkTargetInfo& link,
                                          bool nullable = false, bool listtype = false, bool* was_inserted = nullptr);

    struct InsertSubtableColumns;
    struct EraseSubtableColumns;
    struct RenameSubtableColumns;

    ColKey insert_root_column(ColKey col_key, DataType type, StringData name, LinkTargetInfo& link_target,
                              bool nullable = false, bool linktype = false);
    void erase_root_column(ColKey col_key);
    ColKey do_insert_root_column(ColKey col_key, ColumnType, StringData name, bool nullable = false,
                                 bool listtype = false);
    void do_erase_root_column(ColKey col_key);
    ColKey insert_backlink_column(TableKey origin_table_key, ColKey origin_col_key, ColKey backlink_col_key);
    void erase_backlink_column(TableKey origin_table_key, ColKey origin_col_key);

    struct AccessorUpdater {
        virtual void update(Table&) = 0;
        virtual void update_parent(Table&) = 0;
        virtual ~AccessorUpdater()
        {
        }
    };
    void update_accessors(AccessorUpdater&);

    /// Called in the context of Group::commit() to ensure that
    /// attached table accessors stay valid across a commit. Please
    /// note that this works only for non-transactional commits. Table
    /// accessors obtained during a transaction are always detached
    /// when the transaction ends.
    void update_from_parent(size_t old_baseline) noexcept;

    // Support function for conversions
    void to_string_header(std::ostream& out, std::vector<size_t>& widths) const;
    void to_string_row(ObjKey key, std::ostream& out, const std::vector<size_t>& widths) const;

    // recursive methods called by to_json, to follow links
    void to_json(std::ostream& out, size_t link_depth, std::map<std::string, std::string>& renames,
                 std::vector<ref_type>& followed) const;
    void to_json_row(size_t row_ndx, std::ostream& out, size_t link_depth,
                     std::map<std::string, std::string>& renames, std::vector<ref_type>& followed) const;
    void to_json_row(size_t row_ndx, std::ostream& out, size_t link_depth = 0,
                     std::map<std::string, std::string>* renames = nullptr) const;

    // Detach accessor. This recycles the Table accessor and all subordinate
    // accessors become invalid.
    void detach() noexcept;

    /// Detach and remove all attached row, link list, and subtable
    /// accessors. This function does not discard the descriptor accessor, if
    /// any, and it does not discard column accessors either.
    void discard_child_accessors() noexcept;

    ColumnType get_real_column_type(ColKey col_key) const noexcept;

    /// If this table is a group-level table, the parent group is returned,
    /// otherwise null is returned.
    Group* get_parent_group() const noexcept;

    static size_t get_size_from_ref(ref_type top_ref, Allocator&) noexcept;
    static size_t get_size_from_ref(ref_type spec_ref, ref_type columns_ref, Allocator&) noexcept;

    const Table* get_parent_table_ptr(size_t* col_key_out = nullptr) const noexcept;
    Table* get_parent_table_ptr(size_t* col_key_out = nullptr) noexcept;

    /// Create an empty table with independent spec and return just
    /// the reference to the underlying memory.
    static ref_type create_empty_table(Allocator&, TableKey = TableKey());

    /// True for `col_type_Link` and `col_type_LinkList`.
    static bool is_link_type(ColumnType) noexcept;

    void connect_opposite_link_columns(ColKey link_col_key, Table& target_table, ColKey backlink_col_key) noexcept;

    void remove_recursive(CascadeState&);
    //@{

    /// Cascading removal of strong links.
    ///
    /// FIXME: Update this explanation
    ///
    /// cascade_break_backlinks_to() removes all backlinks pointing to the row
    /// at \a row_ndx. Additionally, if this causes the number of **strong**
    /// backlinks originating from a particular opposite row (target row of
    /// corresponding forward link) to drop to zero, and that row is not already
    /// in \a state.rows, then that row is added to \a state.rows, and
    /// cascade_break_backlinks_to() is called recursively for it. This
    /// operation is the first half of the cascading row removal operation. The
    /// second half is performed by passing the resulting contents of \a
    /// state.rows to remove_backlink_broken_rows().
    ///
    /// Operations that trigger cascading row removal due to explicit removal of
    /// one or more rows (the *initiating rows*), should add those rows to \a
    /// rows initially, and then call cascade_break_backlinks_to() once for each
    /// of them in turn. This is opposed to carrying out the explicit row
    /// removals independently, which is also possible, but does require that
    /// any initiating rows, that end up in \a state.rows due to link cycles,
    /// are removed before passing \a state.rows to
    /// remove_backlink_broken_rows(). In the case of clear(), where all rows of
    /// a table are explicitly removed, it is better to use
    /// cascade_break_backlinks_to_all_rows(), and then carry out the table
    /// clearing as an independent step. For operations that trigger cascading
    /// row removal for other reasons than explicit row removal, \a state.rows
    /// must be empty initially, but cascade_break_backlinks_to() must still be
    /// called for each of the initiating rows.
    ///
    /// When the last non-recursive invocation of cascade_break_backlinks_to()
    /// returns, all forward links originating from a row in \a state.rows have
    /// had their reciprocal backlinks removed, so remove_backlink_broken_rows()
    /// does not perform reciprocal backlink removal at all. Additionally, all
    /// remaining backlinks originating from rows in \a state.rows are
    /// guaranteed to point to rows that are **not** in \a state.rows. This is
    /// true because any backlink that was pointing to a row in \a state.rows
    /// has been removed by one of the invocations of
    /// cascade_break_backlinks_to(). The set of forward links, that correspond
    /// to these remaining backlinks, is precisely the set of forward links that
    /// need to be removed/nullified by remove_backlink_broken_rows(), which it
    /// does by way of reciprocal forward link removal. Note also, that while
    /// all the rows in \a state.rows can have remaining **weak** backlinks
    /// originating from them, only the initiating rows in \a state.rows can
    /// have remaining **strong** backlinks originating from them. This is true
    /// because a non-initiating row is added to \a state.rows only when the
    /// last backlink originating from it is lost.
    ///
    /// Each row removal is replicated individually (as opposed to one
    /// replication instruction for the entire cascading operation). This is
    /// done because it provides an easy way for Group::advance_transact() to
    /// know which tables are affected by the cascade. Note that this has
    /// several important consequences: First of all, the replication log
    /// receiver must execute the row removal instructions in a non-cascading
    /// fashion, meaning that there will be an asymmetry between the two sides
    /// in how the effect of the cascade is brought about. While this is fine
    /// for simple 1-to-1 replication, it may end up interfering badly with
    /// *transaction merging*, when that feature is introduced. Imagine for
    /// example that the cascade initiating operation gets canceled during
    /// conflict resolution, but some, or all of the induced row removals get to
    /// stay. That would break causal consistency. It is important, however, for
    /// transaction merging that the cascaded row removals are explicitly
    /// mentioned in the replication log, such that they can be used to adjust
    /// row indexes during the *operational transform*.
    ///
    /// cascade_break_backlinks_to_all_rows() has the same affect as calling
    /// cascade_break_backlinks_to() once for each row in the table. When
    /// calling this function, \a state.stop_on_table must be set to the origin
    /// table (origin table of corresponding forward links), and \a
    /// state.stop_on_link_list_column must be null.
    ///
    /// It is immaterial which table remove_backlink_broken_rows() is called on,
    /// as long it that table is in the same group as the removed rows.

    void cascade_break_backlinks_to(size_t, CascadeState&)
    {
        REALM_ASSERT(false); // unimplemented
    }

    void cascade_break_backlinks_to_all_rows(CascadeState&)
    {
        REALM_ASSERT(false); // unimplemented
    }

    void remove_backlink_broken_rows(const CascadeState&)
    {
        REALM_ASSERT(false); // unimplemented
    }

    //@}

    /// Used by query. Follows chain of link columns and returns final target table
    const Table* get_link_chain_target(const std::vector<ColKey>&) const;

    // Precondition: 1 <= end - begin
    size_t* record_subtable_path(size_t* begin, size_t* end) const noexcept;

    Replication* get_repl() noexcept;

    void set_ndx_in_parent(size_t ndx_in_parent) noexcept;

    /// Refresh the part of the accessor tree that is rooted at this
    /// table.
    void refresh_accessor_tree();
    void refresh_index_accessors();

    bool is_cross_table_link_target() const noexcept;
    std::recursive_mutex* get_parent_accessor_management_lock() const;
#ifdef REALM_DEBUG
    void to_dot_internal(std::ostream&) const;
#endif
    template <Action action, typename T, typename R>
    R aggregate(ColKey col_key, T value = {}, size_t* resultcount = nullptr, ObjKey* return_ndx = nullptr) const;
    template <typename T>
    double average(ColKey col_key, size_t* resultcount) const;

    std::vector<ColKey> m_ndx2colkey;
    // holds the ndx in the lower bits, the tag in the higher bit to save an indirection
    // when validating colkeys.
    std::vector<uint64_t> m_colkey2ndx;

    static constexpr int top_position_for_spec = 0;
    static constexpr int top_position_for_columns = 1;
    static constexpr int top_position_for_cluster_tree = 2;
    static constexpr int top_position_for_key = 3;
    static constexpr int top_position_for_search_indexes = 4;
    static constexpr int top_position_for_column_key = 5;

    friend class SubtableNode;
    friend class _impl::TableFriend;
    friend class Query;
    friend class metrics::QueryInfo;
    template <class>
    friend class SimpleQuerySupport;
    friend class LangBindHelper;
    friend class TableViewBase;
    template <class T>
    friend class Columns;
    friend class Columns<StringData>;
    friend class ParentNode;
    template <class>
    friend class SequentialGetter;
    friend class RowBase;
    friend class LinksToNode;
    friend class LinkMap;
    friend class LinkView;
    friend class Group;
    friend class ClusterTree;
    friend class ArrayBacklink;
};

class Table::Parent : public ArrayParent {
public:
    ~Parent() noexcept override
    {
    }

protected:
    virtual StringData get_child_name(size_t child_ndx) const noexcept;

    /// If children are group-level tables, then this function returns the
    /// group. Otherwise it returns null.
    virtual Group* get_parent_group() noexcept;

    /// If children are subtables, then this function returns the
    /// parent table. Otherwise it returns null.
    ///
    /// If \a col_key_out is not null, this function must assign the index of
    /// the column within the parent table to `*col_key_out` when , and only
    /// when this table parent is a column in a parent table.
    virtual Table* get_parent_table(size_t* col_key_out = nullptr) noexcept;

    virtual Spec* get_subtable_spec() noexcept;

    /// Must be called whenever a child table accessor is about to be destroyed.
    ///
    /// Note that the argument is a pointer to the child Table rather than its
    /// `ndx_in_parent` property. This is because only minimal accessor
    /// consistency can be assumed by this function.
    virtual void child_accessor_destroyed(Table* child) noexcept = 0;


    virtual size_t* record_subtable_path(size_t* begin, size_t* end) noexcept;
    virtual std::recursive_mutex* get_accessor_management_lock() noexcept = 0;

    friend class Table;
};


// Implementation:


inline uint_fast64_t Table::get_content_version() const noexcept
{
    return m_alloc.get_content_version();
}

inline uint_fast64_t Table::get_instance_version() const noexcept
{
    return m_alloc.get_instance_version();
}


inline uint_fast64_t Table::get_storage_version(uint64_t instance_version) const
{
    return m_alloc.get_storage_version(instance_version);
}


inline void Table::bump_storage_version() const noexcept
{
    return m_alloc.bump_storage_version();
}

inline void Table::bump_content_version() const noexcept
{
    m_alloc.bump_content_version();
}



inline StringData Table::get_name() const noexcept
{
    const Array& real_top = m_top;
    ArrayParent* parent = real_top.get_parent();
    if (!parent)
        return StringData("");
    size_t index_in_parent = real_top.get_ndx_in_parent();
    REALM_ASSERT(dynamic_cast<Parent*>(parent));
    return static_cast<Parent*>(parent)->get_child_name(index_in_parent);
}

inline size_t Table::get_column_count() const noexcept
{
    return m_spec->get_public_column_count();
}

inline StringData Table::get_column_name(ColKey column_key) const noexcept
{
    auto ndx = colkey2ndx(column_key);
    REALM_ASSERT_3(ndx, <, get_column_count());
    return m_spec->get_column_name(ndx);
}

inline ColKey Table::get_column_key(StringData name) const noexcept
{
    size_t ndx = m_spec->get_column_index(name);
    if (ndx == npos)
        return ColKey();
    return ndx2colkey(ndx);
}

inline ColumnType Table::get_real_column_type(ColKey col_key) const noexcept
{
    size_t ndx = colkey2ndx(col_key);
    REALM_ASSERT_3(ndx, <, m_spec->get_column_count());
    return m_spec->get_column_type(ndx);
}

inline DataType Table::get_column_type(ColKey column_key) const noexcept
{
    auto ndx = colkey2ndx(column_key);
    REALM_ASSERT_3(ndx, <, m_spec->get_column_count());
    return m_spec->get_public_column_type(ndx);
}


inline Table::Table(Allocator& alloc)
    : m_alloc(alloc)
    , m_top(m_alloc)
    , m_clusters(this, m_alloc)
    , m_index_refs(m_alloc)
{
    ref_type ref = create_empty_table(alloc); // Throws
    Parent* parent = nullptr;
    size_t ndx_in_parent = 0;
    init(ref, parent, ndx_in_parent);
}

inline Table::Table(ref_count_tag, Allocator& alloc)
    : m_alloc(alloc)
    , m_top(m_alloc)
    , m_clusters(this, m_alloc)
    , m_index_refs(m_alloc)
{
}

inline void Table::revive(Allocator& alloc)
{
    m_alloc.switch_underlying_allocator(alloc);
    // since we're rebinding to a new table, we'll bump version counters
    // FIXME
    // this can be optimized if version counters are saved along with the
    // table data.
    bump_content_version();
    bump_storage_version();
    // we assume all other accessors are detached, so we're done.
}

inline Allocator& Table::get_alloc() const
{
    return m_alloc;
}

// For use by queries
template <class T>
inline Columns<T> Table::column(ColKey col_key)
{
    std::vector<ColKey> link_chain = std::move(m_link_chain);
    m_link_chain.clear();

    // Check if user-given template type equals Realm type. Todo, we should clean up and reuse all our
    // type traits (all the is_same() cases below).
    const Table* table = get_link_chain_target(link_chain);

    realm::DataType ct = table->get_column_type(col_key);
    if (std::is_same<T, int64_t>::value && ct != type_Int)
        throw(LogicError::type_mismatch);
    else if (std::is_same<T, bool>::value && ct != type_Bool)
        throw(LogicError::type_mismatch);
    else if (std::is_same<T, float>::value && ct != type_Float)
        throw(LogicError::type_mismatch);
    else if (std::is_same<T, double>::value && ct != type_Double)
        throw(LogicError::type_mismatch);

    if (std::is_same<T, Link>::value || std::is_same<T, LinkList>::value || std::is_same<T, BackLink>::value) {
        link_chain.push_back(col_key);
    }

    return Columns<T>(col_key, this, std::move(link_chain));
}

template <class T>
inline Columns<T> Table::column(const Table& origin, ColKey origin_col_key)
{
    static_assert(std::is_same<T, BackLink>::value, "");

    auto origin_table_key = origin.get_key();
    const Table& current_target_table = *get_link_chain_target(m_link_chain);
    size_t backlink_col_ndx = current_target_table.m_spec->find_backlink_column(origin_table_key, origin_col_key);
    ColKey backlink_col_key = current_target_table.ndx2colkey(backlink_col_ndx);

    std::vector<ColKey> link_chain = std::move(m_link_chain);
    m_link_chain.clear();
    link_chain.push_back(backlink_col_key);

    return Columns<T>(backlink_col_key, this, std::move(link_chain));
}

template <class T>
SubQuery<T> Table::column(ColKey col_key, Query subquery)
{
    static_assert(std::is_same<T, Link>::value, "A subquery must involve a link list or backlink column");
    return SubQuery<T>(column<T>(col_key), std::move(subquery));
}

template <class T>
SubQuery<T> Table::column(const Table& origin, ColKey origin_col_key, Query subquery)
{
    static_assert(std::is_same<T, BackLink>::value, "A subquery must involve a link list or backlink column");
    return SubQuery<T>(column<T>(origin, origin_col_key), std::move(subquery));
}

// For use by queries
inline Table& Table::link(ColKey link_column)
{
    m_link_chain.push_back(link_column);
    return *this;
}

inline Table& Table::backlink(const Table& origin, ColKey origin_col_key)
{
    auto origin_table_key = origin.get_key();
    const Table& current_target_table = *get_link_chain_target(m_link_chain);
    size_t backlink_col_ndx = current_target_table.m_spec->find_backlink_column(origin_table_key, origin_col_key);
    ColKey backlink_col_key = current_target_table.ndx2colkey(backlink_col_ndx);
    return link(backlink_col_key);
}

inline bool Table::is_empty() const noexcept
{
    return size() == 0;
}

inline size_t Table::size() const noexcept
{
    return m_clusters.size();
}


inline ConstTableRef Table::get_link_target(ColKey col_key) const noexcept
{
    return const_cast<Table*>(this)->get_link_target(col_key);
}

inline ConstTableRef Table::get_parent_table(size_t* col_key_out) const noexcept
{
    return ConstTableRef(get_parent_table_ptr(col_key_out));
}

inline TableRef Table::get_parent_table(size_t* col_key_out) noexcept
{
    return TableRef(get_parent_table_ptr(col_key_out));
}

inline bool Table::is_group_level() const noexcept
{
    return bool(get_parent_group());
}

inline bool Table::operator==(const Table& t) const
{
    return *m_spec == *t.m_spec && compare_objects(t); // Throws
}

inline bool Table::operator!=(const Table& t) const
{
    return !(*this == t); // Throws
}

inline size_t Table::get_size_from_ref(ref_type top_ref, Allocator& alloc) noexcept
{
    const char* top_header = alloc.translate(top_ref);
    std::pair<int_least64_t, int_least64_t> p = Array::get_two(top_header, 0);
    ref_type spec_ref = to_ref(p.first), columns_ref = to_ref(p.second);
    return get_size_from_ref(spec_ref, columns_ref, alloc);
}

inline Table* Table::get_parent_table_ptr(size_t* col_key_out) noexcept
{
    const Table* parent = const_cast<const Table*>(this)->get_parent_table_ptr(col_key_out);
    return const_cast<Table*>(parent);
}

inline bool Table::is_link_type(ColumnType col_type) noexcept
{
    return col_type == col_type_Link || col_type == col_type_LinkList;
}

inline size_t* Table::record_subtable_path(size_t* b, size_t* e) const noexcept
{
    const Array& real_top = m_top;
    size_t index_in_parent = real_top.get_ndx_in_parent();
    REALM_ASSERT_3(b, <, e);
    *b++ = index_in_parent;
    ArrayParent* parent = real_top.get_parent();
    REALM_ASSERT(parent);
    REALM_ASSERT(dynamic_cast<Parent*>(parent));
    return static_cast<Parent*>(parent)->record_subtable_path(b, e);
}

inline size_t* Table::Parent::record_subtable_path(size_t* b, size_t*) noexcept
{
    return b;
}

inline Replication* Table::get_repl() noexcept
{
    return m_top.get_alloc().get_replication();
}

inline void Table::set_ndx_in_parent(size_t ndx_in_parent) noexcept
{
    REALM_ASSERT(m_top.is_attached());
    m_top.set_ndx_in_parent(ndx_in_parent);
}

inline size_t Table::colkey2ndx(ColKey key) const
{
    size_t idx = key.value & max_num_columns;
    if (idx >= m_colkey2ndx.size())
        throw InvalidKey("Nonexisting column key");
    // FIXME: Optimization! There are many scenarios where this test may be avoided.
    uint64_t ndx_and_tag = m_colkey2ndx[idx];
    size_t ndx = ndx_and_tag & max_num_columns;
    if ((ndx == max_num_columns) || ((ndx_and_tag ^ key.value) > max_num_columns))
        throw InvalidKey("Nonexisting column key");
    return ndx;
}

inline ColKey Table::ndx2colkey(size_t ndx) const
{
    REALM_ASSERT(ndx < m_ndx2colkey.size());
    return m_ndx2colkey[ndx];
}

bool inline Table::valid_column(ColKey col_key) const
{
    size_t idx = col_key.value & max_num_columns;
    if (idx >= m_colkey2ndx.size())
        return false;
    uint64_t ndx_and_tag = m_colkey2ndx[idx];
    if ((ndx_and_tag ^ col_key.value) > max_num_columns)
        return false;
    if ((ndx_and_tag & max_num_columns) == max_num_columns)
        return false;
    return true;
}


// This class groups together information about the target of a link column
// This is not a valid link if the target table == nullptr
struct LinkTargetInfo {
    LinkTargetInfo(Table* target = nullptr, ColKey backlink_key = ColKey())
        : m_target_table(target)
        , m_backlink_col_key(backlink_key)
    {
    }
    bool is_valid() const
    {
        return (m_target_table != nullptr);
    }
    Table* m_target_table;
    ColKey m_backlink_col_key; // a value of ColKey() indicates the backlink should be appended
};

// The purpose of this class is to give internal access to some, but
// not all of the non-public parts of the Table class.
class _impl::TableFriend {
public:
    static ref_type create_empty_table(Allocator& alloc, TableKey key = TableKey())
    {
        return Table::create_empty_table(alloc, key); // Throws
    }

    static Table* create_accessor(Allocator& alloc, ref_type top_ref, Table::Parent* parent, size_t ndx_in_parent)
    {
        std::unique_ptr<Table> table(new Table(Table::ref_count_tag(), alloc)); // Throws
        table->init(top_ref, parent, ndx_in_parent);                            // Throws
        return table.release();
    }

    // Intended to be used only by Group::create_table_accessor()
    static Table* create_incomplete_accessor(Allocator& alloc, ref_type top_ref, Table::Parent* parent,
                                             size_t ndx_in_parent)
    {
        std::unique_ptr<Table> table(new Table(Table::ref_count_tag(), alloc)); // Throws
        bool skip_create_column_accessors = true;
        table->init(top_ref, parent, ndx_in_parent, skip_create_column_accessors); // Throws
        return table.release();
    }

    // Intended to be used only by Group::create_table_accessor()
    static void complete_accessor(Table& table)
    {
        table.refresh_index_accessors(); // Throws
    }

    static void set_top_parent(Table& table, ArrayParent* parent, size_t ndx_in_parent) noexcept
    {
        table.m_top.set_parent(parent, ndx_in_parent);
    }

    static void update_from_parent(Table& table, size_t old_baseline) noexcept
    {
        table.update_from_parent(old_baseline);
    }

    static void detach(Table& table) noexcept
    {
        table.detach();
    }

    static ColKey find_backlink_column(Table& table, TableKey origin_key, ColKey col_key)
    {
        return table.find_backlink_column(origin_key, col_key);
    }

    static TableKey get_opposite_link_table_key(const Table& table, ColKey col_key)
    {
        return get_spec(table).get_opposite_link_table_key(table.colkey2ndx(col_key));
    }

    static void discard_child_accessors(Table& table) noexcept
    {
        table.discard_child_accessors();
    }

    static bool compare_objects(const Table& a, const Table& b)
    {
        return a.compare_objects(b); // Throws
    }

    static size_t get_size_from_ref(ref_type ref, Allocator& alloc) noexcept
    {
        return Table::get_size_from_ref(ref, alloc);
    }

    static size_t get_size_from_ref(ref_type spec_ref, ref_type columns_ref, Allocator& alloc) noexcept
    {
        return Table::get_size_from_ref(spec_ref, columns_ref, alloc);
    }

    static Spec& get_spec(Table& table) noexcept
    {
        return *table.m_spec;
    }

    static const Spec& get_spec(const Table& table) noexcept
    {
        return *table.m_spec;
    }

    static TableRef get_opposite_link_table(const Table& table, ColKey col_key);

    static void do_remove_object(Table& table, ObjKey key)
    {
        table.do_remove_object(key); // Throws
    }

    static void do_set_link(Table& table, ColKey col_key, size_t row_ndx, size_t target_row_ndx)
    {
        table.do_set_link(col_key, row_ndx, target_row_ndx); // Throws
    }

    static void remove_recursive(Table& table, CascadeState& rows)
    {
        table.remove_recursive(rows); // Throws
    }

    static size_t* record_subtable_path(const Table& table, size_t* b, size_t* e) noexcept
    {
        return table.record_subtable_path(b, e);
    }
    static void insert_column_unless_exists(Table& table, ColKey column_key, DataType type, StringData name,
                                            LinkTargetInfo link, bool nullable = false, bool listtype = false,
                                            bool* was_inserted = nullptr)
    {
        table.do_insert_column_unless_exists(column_key, type, name, link, nullable, listtype,
                                             was_inserted); // Throws
    }

    static void erase_column(Table& table, ColKey col_key)
    {
        table.remove_column(col_key); // Throws
    }

    static void rename_column(Table& table, ColKey col_key, StringData name)
    {
        table.rename_column(col_key, name); // Throws
    }

    static void set_link_type(Table& table, ColKey column_key, LinkType link_type)
    {
        table.set_link_type(column_key, link_type); // Throws
    }

    static void batch_erase_rows(Table& table, const KeyColumn& keys)
    {
        table.batch_erase_rows(keys); // Throws
    }

    typedef Table::AccessorUpdater AccessorUpdater;
    static void update_accessors(Table& table, AccessorUpdater& updater)
    {
        table.update_accessors(updater); // Throws
    }

    static void refresh_accessor_tree(Table& table)
    {
        table.refresh_accessor_tree(); // Throws
    }

    static void set_ndx_in_parent(Table& table, size_t ndx_in_parent) noexcept
    {
        table.set_ndx_in_parent(ndx_in_parent);
    }

    static bool is_link_type(ColumnType type) noexcept
    {
        return Table::is_link_type(type);
    }

    static void bump_content_version(Table& table) noexcept
    {
        table.bump_content_version();
    }

    static bool is_cross_table_link_target(const Table& table)
    {
        return table.is_cross_table_link_target();
    }

    static Group* get_parent_group(const Table& table) noexcept
    {
        return table.get_parent_group();
    }

    static Replication* get_repl(Table& table) noexcept
    {
        return table.get_repl();
    }
};


} // namespace realm

#endif // REALM_TABLE_HPP
