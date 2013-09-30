/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/
#ifndef TIGHTDB_TABLE_HPP
#define TIGHTDB_TABLE_HPP

#include <utility>

#include <tightdb/column_fwd.hpp>
#include <tightdb/table_ref.hpp>
#include <tightdb/spec.hpp>
#include <tightdb/mixed.hpp>
#include <tightdb/query.hpp>

//#include "query_expression.h"


#ifdef TIGHTDB_ENABLE_REPLICATION
#  include <tightdb/replication.hpp>
#endif

namespace tightdb {

class TableView;
class ConstTableView;
class StringIndex;

template <class T> class Columns;

/// The Table class is non-polymorphic, that is, it has no virtual
/// functions. This is important because it ensures that there is no
/// run-time distinction between a Table instance and an instance of
/// any variation of BasicTable<T>, and this, in turn, makes it valid
/// to cast a pointer from Table to BasicTable<T> even when the
/// instance is constructed as a Table. Of couse, this also assumes
/// that BasicTable<> is non-polymorphic, has no destructor, and adds
/// no extra data members.
///
/// FIXME: Table assignment (from any group to any group) could be made
/// aliasing safe as follows: Start by cloning source table into
/// target allocator. On success, assign, and then deallocate any
/// previous structure at the target.
///
/// FIXME: It might be desirable to have a 'table move' feature
/// between two places inside the same group (say from a subtable or a
/// mixed column to group level). This could be done in a very
/// efficient manner.
///
/// FIXME: When compiling in debug mode, all public table methods
/// should TIGHTDB_ASSERT(is_attached()).
class Table {
public:
    /// Construct a new freestanding top-level table with static
    /// lifetime.
    ///
    /// This constructor should be used only when placing a table
    /// instance on the stack, and it is then the responsibility of
    /// the application that there are no objects of type TableRef or
    /// ConstTableRef that refer to it, or to any of its subtables,
    /// when it goes out of scope. To create a top-level table with
    /// dynamic lifetime, use Table::create() instead.
    Table(Allocator& = Allocator::get_default());

    /// Construct a copy of the specified table as a new freestanding
    /// top-level table with static lifetime.
    ///
    /// This constructor should be used only when placing a table
    /// instance on the stack, and it is then the responsibility of
    /// the application that there are no objects of type TableRef or
    /// ConstTableRef that refer to it, or to any of its subtables,
    /// when it goes out of scope. To create a top-level table with
    /// dynamic lifetime, use Table::copy() instead.
    Table(const Table&, Allocator& = Allocator::get_default());

    ~Table() TIGHTDB_NOEXCEPT;

    /// Construct a new freestanding top-level table with dynamic
    /// lifetime.
    static TableRef create(Allocator& = Allocator::get_default());

    /// Construct a copy of the specified table as a new freestanding
    /// top-level table with dynamic lifetime.
    TableRef copy(Allocator& = Allocator::get_default()) const;

    /// A table accessor that is no longer attached must not be
    /// accessed in any way except by calling is_attached(). A table
    /// accessor that is obtained from a Group becomes detached if its
    /// group accessor is destroyed. This is also true for any
    /// subtable accessor that is obtained indirectly from a group. A
    /// subtable accessor will generally become detached if its parent
    /// table is modified. Calling a const member function on a parent
    /// table accessor, will never detach its subtable accessors,
    /// though. An accessor for a freestanding table will never become
    /// detached. An accessor for a subtable of a freestanding table
    /// may become detached.
    ///
    /// FIXME: High level language bindings will probably want to be
    /// able to explicitely detach a group and all tables of that
    /// group if any modifying operation fails (e.g. memory allocation
    /// failure) (and something similar for freestanding tables) since
    /// that leaves the group in state where any further access is
    /// disallowed. This way they will be able to reliably intercept
    /// any attempt at accessing such a failed group.
    ///
    /// FIXME: The C++ documentation must state that if any modifying
    /// operation on a group (incl. tables, subtables, and specs) or
    /// on a free standing table (incl. subtables and specs) fails,
    /// then any further access to that group (except ~Group()) or
    /// freestanding table (except ~Table()) has undefined behaviour
    /// and is considered an error on behalf of the application. Note
    /// that even Table::is_attached() is disallowed in this case.
    bool is_attached() const TIGHTDB_NOEXCEPT;

    // A degenerate table is a subtable which isn't instantiated in the
    // database file yet because there has not yet been write-access to 
    // it. Avoiding instantiation is an optimization to save space, etc.
    bool is_degenerate() const TIGHTDB_NOEXCEPT { return m_columns.m_data == NULL; }

    /// A shared spec is a column specification that in general
    /// applies to many tables. A table is not allowed to directly
    /// modify its own spec if it is shared. A shared spec may only be
    /// modified via the closest ancestor table that has a nonshared
    /// spec. Such an ancestor will always exist.
    bool has_shared_spec() const;


    //@{
    /// Schema handling.
    ///
    /// Only the const qualified get_spec() and has_index() may be
    /// called for a table with shared spec. All the other methods may
    /// be called only for a top-level table or a subtable in a mixed
    /// column.
    ///
    /// With the exception of get_spec() (with and without const
    /// qualification,) and has_index(), all of these methods will
    /// detach all subtable accessors (instances of Table) having this
    /// table as direct or indirect ancestor.
    ///
    /// \param column_path A list of column indexes. For
    /// add_subcolumn() this list must contain at least one
    /// element. For remove_subcolumn() and rename_subcolumn() this
    /// list must contain at least two element. The first index
    /// specifies a column, C1, of this table, the next index
    /// specifies a column, C2, of the spec of C1, and so forth.
    ///
    /// \sa Spec
    Spec& get_spec() TIGHTDB_NOEXCEPT;
    const Spec& get_spec() const TIGHTDB_NOEXCEPT;
    void update_from_spec(); // Must not be called for a table with shared spec
    std::size_t add_column(DataType type, StringData name); // Add a column dynamically
    std::size_t add_subcolumn(const std::vector<std::size_t>& column_path, DataType type,
                              StringData name);
    void remove_column(std::size_t column_ndx);
    void remove_subcolumn(const std::vector<std::size_t>& column_path);
    void rename_column(std::size_t column_ndx, StringData name);
    void rename_subcolumn(const std::vector<std::size_t>& column_path, StringData name);
    bool has_index(std::size_t column_ndx) const;
    void set_index(std::size_t column_ndx);
    //@}

    template <class T> Columns<T> column(size_t column)
    {
        return Columns<T>(column, this);
    }

    // Table size and deletion
    bool        is_empty() const TIGHTDB_NOEXCEPT { return m_size == 0; }
    std::size_t size() const TIGHTDB_NOEXCEPT { return m_size; }
    void        clear();

    // Column information
    std::size_t get_column_count() const TIGHTDB_NOEXCEPT;
    StringData  get_column_name(std::size_t column_ndx) const TIGHTDB_NOEXCEPT;
    std::size_t get_column_index(StringData name) const;
    DataType    get_column_type(std::size_t column_ndx) const TIGHTDB_NOEXCEPT;

    // Row handling
    std::size_t add_empty_row(std::size_t num_rows = 1);
    void        insert_empty_row(std::size_t row_ndx, std::size_t num_rows = 1);
    void        remove(std::size_t row_ndx);
    void        remove_last() { if (!is_empty()) remove(m_size-1); }
    void        move_last_over(std::size_t ndx);

    // Insert row
    // NOTE: You have to insert values in ALL columns followed by insert_done().
    void insert_int(std::size_t column_ndx, std::size_t row_ndx, int64_t value);
    void insert_bool(std::size_t column_ndx, std::size_t row_ndx, bool value);
    void insert_date(std::size_t column_ndx, std::size_t row_ndx, Date value);
    template<class E> void insert_enum(std::size_t column_ndx, std::size_t row_ndx, E value);
    void insert_float(std::size_t column_ndx, std::size_t row_ndx, float value);
    void insert_double(std::size_t column_ndx, std::size_t row_ndx, double value);
    void insert_string(std::size_t column_ndx, std::size_t row_ndx, StringData value);
    void insert_binary(std::size_t column_ndx, std::size_t row_ndx, BinaryData value);
    void insert_subtable(std::size_t column_ndx, std::size_t row_ndx); // Insert empty table
    void insert_mixed(std::size_t column_ndx, std::size_t row_ndx, Mixed value);
    void insert_done();

    // Get cell values
    int64_t     get_int(std::size_t column_ndx, std::size_t row_ndx) const TIGHTDB_NOEXCEPT;
    bool        get_bool(std::size_t column_ndx, std::size_t row_ndx) const TIGHTDB_NOEXCEPT;
    Date        get_date(std::size_t column_ndx, std::size_t row_ndx) const TIGHTDB_NOEXCEPT;
    float       get_float(std::size_t column_ndx, std::size_t row_ndx) const TIGHTDB_NOEXCEPT;
    double      get_double(std::size_t column_ndx, std::size_t row_ndx) const TIGHTDB_NOEXCEPT;
    StringData  get_string(std::size_t column_ndx, std::size_t row_ndx) const TIGHTDB_NOEXCEPT;
    BinaryData  get_binary(std::size_t column_ndx, std::size_t row_ndx) const TIGHTDB_NOEXCEPT;
    Mixed       get_mixed(std::size_t column_ndx, std::size_t row_ndx) const TIGHTDB_NOEXCEPT;
    DataType    get_mixed_type(std::size_t column_ndx, std::size_t row_ndx) const TIGHTDB_NOEXCEPT;

    // Set cell values
    void set_int(std::size_t column_ndx, std::size_t row_ndx, int64_t value);
    void set_bool(std::size_t column_ndx, std::size_t row_ndx, bool value);
    void set_date(std::size_t column_ndx, std::size_t row_ndx, Date value);
    template<class E> void set_enum(std::size_t column_ndx, std::size_t row_ndx, E value);
    void set_float(std::size_t column_ndx, std::size_t row_ndx, float value);
    void set_double(std::size_t column_ndx, std::size_t row_ndx, double value);
    void set_string(std::size_t column_ndx, std::size_t row_ndx, StringData value);
    void set_binary(std::size_t column_ndx, std::size_t row_ndx, BinaryData value);
    void set_mixed(std::size_t column_ndx, std::size_t row_ndx, Mixed value);


    void add_int(std::size_t column_ndx, int64_t value);

    /// Like insert_subtable(std::size_t, std::size_t, const Table*)
    /// but overwrites the specified cell rather than inserting a new
    /// one.
    void set_subtable(std::size_t col_ndx, std::size_t row_ndx, const Table*);

    // Sub-tables (works on columns whose type is either 'subtable' or
    // 'mixed', for a value in a mixed column that is not a subtable,
    // get_subtable() returns null, get_subtable_size() returns zero,
    // and clear_subtable() replaces the value with an empty table.)
    TableRef       get_subtable(std::size_t column_ndx, std::size_t row_ndx);
    ConstTableRef  get_subtable(std::size_t column_ndx, std::size_t row_ndx) const;
    size_t         get_subtable_size(std::size_t column_ndx, std::size_t row_ndx) const TIGHTDB_NOEXCEPT;
    void           clear_subtable(std::size_t column_ndx, std::size_t row_ndx);

    // Aggregate functions
    std::size_t  count_int(std::size_t column_ndx, int64_t value) const;
    std::size_t  count_string(std::size_t column_ndx, StringData value) const;
    std::size_t  count_float(std::size_t column_ndx, float value) const;
    std::size_t  count_double(std::size_t column_ndx, double value) const;

    int64_t sum(std::size_t column_ndx) const;
    double  sum_float(std::size_t column_ndx) const;
    double  sum_double(std::size_t column_ndx) const;
    // FIXME: What to return for below when table empty? 0?
    int64_t maximum(std::size_t column_ndx) const;
    float   maximum_float(std::size_t column_ndx) const;
    double  maximum_double(std::size_t column_ndx) const;
    int64_t minimum(std::size_t column_ndx) const;
    float   minimum_float(std::size_t column_ndx) const;
    double  minimum_double(std::size_t column_ndx) const;
    double  average(std::size_t column_ndx) const;
    double  average_float(std::size_t column_ndx) const;
    double  average_double(std::size_t column_ndx) const;

    // Searching
    std::size_t    lookup(StringData value) const;
    std::size_t    find_first_int(std::size_t column_ndx, int64_t value) const;
    std::size_t    find_first_bool(std::size_t column_ndx, bool value) const;
    std::size_t    find_first_date(std::size_t column_ndx, Date value) const;
    std::size_t    find_first_float(std::size_t column_ndx, float value) const;
    std::size_t    find_first_double(std::size_t column_ndx, double value) const;
    std::size_t    find_first_string(std::size_t column_ndx, StringData value) const;
    std::size_t    find_first_binary(std::size_t column_ndx, BinaryData value) const;

    TableView      find_all_int(std::size_t column_ndx, int64_t value);
    ConstTableView find_all_int(std::size_t column_ndx, int64_t value) const;
    TableView      find_all_bool(std::size_t column_ndx, bool value);
    ConstTableView find_all_bool(std::size_t column_ndx, bool value) const;
    TableView      find_all_date(std::size_t column_ndx, Date value);
    ConstTableView find_all_date(std::size_t column_ndx, Date value) const;
    TableView      find_all_float(std::size_t column_ndx, float value);
    ConstTableView find_all_float(std::size_t column_ndx, float value) const;
    TableView      find_all_double(std::size_t column_ndx, double value);
    ConstTableView find_all_double(std::size_t column_ndx, double value) const;
    TableView      find_all_string(std::size_t column_ndx, StringData value);
    ConstTableView find_all_string(std::size_t column_ndx, StringData value) const;
    TableView      find_all_binary(std::size_t column_ndx, BinaryData value);
    ConstTableView find_all_binary(std::size_t column_ndx, BinaryData value) const;

    TableView      distinct(std::size_t column_ndx);
    ConstTableView distinct(std::size_t column_ndx) const;

    TableView      get_sorted_view(std::size_t column_ndx, bool ascending = true);
    ConstTableView get_sorted_view(std::size_t column_ndx, bool ascending = true) const;

private:
    template <class T> std::size_t find_first(std::size_t column_ndx, T value) const; // called by above methods
    template <class T> ConstTableView find_all(size_t column_ndx, T value) const;
public:


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
    std::size_t lower_bound_int(std::size_t column_ndx, int64_t value) const TIGHTDB_NOEXCEPT;
    std::size_t upper_bound_int(std::size_t column_ndx, int64_t value) const TIGHTDB_NOEXCEPT;
    std::size_t lower_bound_bool(std::size_t column_ndx, bool value) const TIGHTDB_NOEXCEPT;
    std::size_t upper_bound_bool(std::size_t column_ndx, bool value) const TIGHTDB_NOEXCEPT;
    std::size_t lower_bound_float(std::size_t column_ndx, float value) const TIGHTDB_NOEXCEPT;
    std::size_t upper_bound_float(std::size_t column_ndx, float value) const TIGHTDB_NOEXCEPT;
    std::size_t lower_bound_double(std::size_t column_ndx, double value) const TIGHTDB_NOEXCEPT;
    std::size_t upper_bound_double(std::size_t column_ndx, double value) const TIGHTDB_NOEXCEPT;
    std::size_t lower_bound_string(std::size_t column_ndx, StringData value) const TIGHTDB_NOEXCEPT;
    std::size_t upper_bound_string(std::size_t column_ndx, StringData value) const TIGHTDB_NOEXCEPT;
    //@}

    // Queries
    Query       where()       { return Query(*this); }

    // FIXME: We need a ConstQuery class or runtime check against modifications in read transaction.
    Query where() const { return Query(*this); } 

    // Optimizing
    void optimize();

    // Conversion
    void to_json(std::ostream& out) const;
    void to_string(std::ostream& out, std::size_t limit = 500) const;
    void row_to_string(std::size_t row_ndx, std::ostream& out) const;

    // Get a reference to this table
    TableRef get_table_ref() { return TableRef(this); }
    ConstTableRef get_table_ref() const { return ConstTableRef(this); }

    /// Compare two tables for equality. Two tables are equal if, and
    /// only if, they contain the same columns and rows in the same
    /// order, that is, for each value V of type T at column index C
    /// and row index R in one of the tables, there is a value of type
    /// T at column index C and row index R in the other table that
    /// is equal to V.
    bool operator==(const Table&) const;

    /// Compare two tables for inequality. See operator==().
    bool operator!=(const Table& t) const;

    // Debug
#ifdef TIGHTDB_DEBUG
    void Verify() const; // Must be upper case to avoid conflict with macro in ObjC
    void to_dot(std::ostream&, StringData title = StringData()) const;
    void print() const;
    MemStats stats() const;
#endif

    class Parent;

protected:
    /// Get the subtable at the specified column and row index.
    ///
    /// The returned table pointer must always end up being wrapped in
    /// a TableRef.
    Table* get_subtable_ptr(std::size_t col_idx, std::size_t row_idx);

    /// Get the subtable at the specified column and row index.
    ///
    /// The returned table pointer must always end up being wrapped in
    /// a ConstTableRef.
    const Table* get_subtable_ptr(std::size_t col_idx, std::size_t row_idx) const;

    /// Compare the rows of two tables under the assumption that the
    /// two tables have the same spec, and therefore the same sequence
    /// of columns.
    bool compare_rows(const Table&) const;

    void insert_into(Table* parent, std::size_t col_ndx, std::size_t row_ndx) const;

    void set_into_mixed(Table* parent, std::size_t col_ndx, std::size_t row_ndx) const;

private:
    // Number of rows in this table
    std::size_t m_size;

    // On-disk format
    Array m_top;
    Array m_columns;
    Spec m_spec;

    // Column accessor instances
    Array m_cols;
    mutable std::size_t m_ref_count;
    mutable const StringIndex* m_lookup_index;

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
    Table& operator=(const Table&);

    /// Used when the lifetime of a table is managed by reference
    /// counting. The lifetime of free-standing tables allocated on
    /// the stack by the application is not managed by reference
    /// counting, so that is a case where this tag must not be
    /// specified.
    class ref_count_tag {};

    /// Construct a wrapper for a table with independent spec, and
    /// whose lifetime is managed by reference counting.
    Table(ref_count_tag, Allocator&, ref_type top_ref, Parent*, std::size_t ndx_in_parent);

    /// Construct a wrapper for a table with shared spec, and whose
    /// lifetime is managed by reference counting.
    ///
    /// It is possible to construct a 'null' table by passing zero for
    /// \a columns_ref, in this case the columns will be created on
    /// demand.
    Table(ref_count_tag, Allocator&, ref_type spec_ref, ref_type columns_ref,
          Parent*, std::size_t ndx_in_parent);

    void init_from_ref(ref_type top_ref, ArrayParent*, std::size_t ndx_in_parent);
    void init_from_ref(ref_type spec_ref, ref_type columns_ref,
                       ArrayParent*, std::size_t ndx_in_parent);

    void create_columns();
    void cache_columns();
    void destroy_column_accessors() TIGHTDB_NOEXCEPT;

    /// Called in the context of Group::commit() to ensure that
    /// attached table accessors stay valid across a commit. Please
    /// note that this works only for non-transactional commits. Table
    /// accessors obtained during a transaction are always detached
    /// when the transaction ends.
    void update_from_parent(std::size_t old_baseline) TIGHTDB_NOEXCEPT;

    // Specification
    void adjust_column_ndx_in_parent(std::size_t column_ndx_begin, int diff) TIGHTDB_NOEXCEPT;
    std::size_t do_add_column(DataType);
    void do_add_subcolumn(const std::vector<std::size_t>& column_path, std::size_t pos, DataType);
    static void do_remove_column(Array& column_refs, const Spec::ColumnInfo&);
    void do_remove_subcolumn(const std::vector<std::size_t>& column_path,
                             std::size_t column_path_ndx, const Spec::ColumnInfo&);

    void set_index(std::size_t column_ndx, bool update_spec);

    // Support function for conversions
    void to_json_row(std::size_t row_ndx, std::ostream& out) const;
    void to_string_header(std::ostream& out, std::vector<std::size_t>& widths) const;
    void to_string_row(std::size_t row_ndx, std::ostream& out, const std::vector<std::size_t>& widths) const;

    /// Assumes that the specified column is a subtable column (in
    /// particular, not a mixed column) and that the specified table
    /// has a spec that is compatible with that column, that is, the
    /// number of columns must be the same, and corresponding columns
    /// must have identical data types (as returned by
    /// get_column_type()).
    void insert_subtable(std::size_t col_ndx, std::size_t row_ndx, const Table*);

    void insert_mixed_subtable(std::size_t col_ndx, std::size_t row_ndx, const Table*);

    void set_mixed_subtable(std::size_t col_ndx, std::size_t row_ndx, const Table*);

    /// Put this table accessor into the detached state. This detaches
    /// it from the underlying structure of array nodes. Also do this
    /// recursively for subtables. When this function returns,
    /// is_attached() will return false.
    ///
    /// This function may be called for a table accessor that is
    /// already in the detached state (idempotency).
    ///
    /// It is also valid to call this function for a table accessor
    /// that has not yet been detached, but whose underlying structure
    /// of arrays have changed in an unpredictable/unknown way. This
    /// kind of change generally happens when a modifying table
    /// operation fails, and also when one transaction is ended and a
    /// new one is started.
    void detach() TIGHTDB_NOEXCEPT;

    /// Detach all attached subtable accessors.
    void detach_subtable_accessors() TIGHTDB_NOEXCEPT;

    void bind_ref() const TIGHTDB_NOEXCEPT { ++m_ref_count; }
    void unbind_ref() const TIGHTDB_NOEXCEPT { if (--m_ref_count == 0) delete this; }

    class UnbindGuard;

    ColumnType get_real_column_type(std::size_t column_ndx) const TIGHTDB_NOEXCEPT;

    const Array* get_column_root(std::size_t col_ndx) const TIGHTDB_NOEXCEPT;
    std::pair<const Array*, const Array*> get_string_column_roots(std::size_t col_ndx) const
        TIGHTDB_NOEXCEPT;

    const ColumnBase& get_column_base(std::size_t column_ndx) const TIGHTDB_NOEXCEPT;
    ColumnBase& get_column_base(std::size_t column_ndx);
    template <class T, ColumnType col_type> T& get_column(std::size_t ndx);
    template <class T, ColumnType col_type> const T& get_column(std::size_t ndx) const TIGHTDB_NOEXCEPT;
    Column& get_column(std::size_t column_ndx);
    const Column& get_column(std::size_t column_ndx) const TIGHTDB_NOEXCEPT;
    ColumnFloat& get_column_float(std::size_t column_ndx);
    const ColumnFloat& get_column_float(std::size_t column_ndx) const TIGHTDB_NOEXCEPT;
    ColumnDouble& get_column_double(std::size_t column_ndx);
    const ColumnDouble& get_column_double(std::size_t column_ndx) const TIGHTDB_NOEXCEPT;
    AdaptiveStringColumn& get_column_string(std::size_t column_ndx);
    const AdaptiveStringColumn& get_column_string(std::size_t column_ndx) const TIGHTDB_NOEXCEPT;
    ColumnBinary& get_column_binary(std::size_t column_ndx);
    const ColumnBinary& get_column_binary(std::size_t column_ndx) const TIGHTDB_NOEXCEPT;
    ColumnStringEnum& get_column_string_enum(std::size_t column_ndx);
    const ColumnStringEnum& get_column_string_enum(std::size_t column_ndx) const TIGHTDB_NOEXCEPT;
    ColumnTable& get_column_table(std::size_t column_ndx);
    const ColumnTable& get_column_table(std::size_t column_ndx) const TIGHTDB_NOEXCEPT;
    ColumnMixed& get_column_mixed(std::size_t column_ndx);
    const ColumnMixed& get_column_mixed(std::size_t column_ndx) const TIGHTDB_NOEXCEPT;

    void instantiate_before_change();
    void validate_column_type(const ColumnBase& column, ColumnType expected_type, std::size_t ndx) const;

    /// Create an empty table with independent spec and return just
    /// the reference to the underlying memory.
    static ref_type create_empty_table(Allocator&);

    /// Create a column of the specified type, fill it with the
    /// specified number of default values, and return just the
    /// reference to the underlying memory.
    static ref_type create_column(DataType column_type, size_t num_default_values, Allocator&);

    /// Construct a copy of the columns array of this table using the
    /// specified allocator and return just the ref to that array.
    ///
    /// In the clone, no string column will be of the enumeration
    /// type.
    ref_type clone_columns(Allocator&) const;

    /// Construct a complete copy of this table (including its spec)
    /// using the specified allocator and return just the ref to the
    /// new top array.
    ref_type clone(Allocator&) const;

#ifdef TIGHTDB_ENABLE_REPLICATION
    struct LocalTransactLog;
    LocalTransactLog transact_log() TIGHTDB_NOEXCEPT;
    // Condition: 1 <= end - begin
    std::size_t* record_subspec_path(const Spec*, std::size_t* begin, std::size_t* end) const TIGHTDB_NOEXCEPT;
    // Condition: 1 <= end - begin
    std::size_t* record_subtable_path(std::size_t* begin, std::size_t* end) const TIGHTDB_NOEXCEPT;
    friend class Replication;
#endif

#ifdef TIGHTDB_DEBUG
    void to_dot_internal(std::ostream&) const;
#endif

    friend class Group;
    friend class Query;
    friend class ColumnMixed;
    template<class> friend class bind_ptr;
    friend class ColumnSubtableParent;
    friend class LangBindHelper;
    friend class TableViewBase;
    template<class T> friend class Columns;
    friend class ParentNode;
    template<class> friend class SequentialGetter;
};



class Table::Parent: public ArrayParent {
public:
    ~Parent() TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE {}

protected:
    /// Must be called whenever a child table accessor is destroyed.
    virtual void child_accessor_destroyed(std::size_t child_ndx) TIGHTDB_NOEXCEPT = 0;

#ifdef TIGHTDB_ENABLE_REPLICATION
    virtual std::size_t* record_subtable_path(std::size_t* begin, std::size_t* end) TIGHTDB_NOEXCEPT;
#endif

    friend class Table;
};





// Implementation:

inline bool Table::is_attached() const TIGHTDB_NOEXCEPT
{
    // Note that it is not possible to link the state of attachment of
    // a table to the state of attachment of m_top, because tables
    // with shared spec do not have a 'top' array. Neither is it
    // possible to link it to the state of attachment of m_columns,
    // because subtables with shared spec start out in a degenerate
    // form where they do not have a 'columns' array. For these
    // reasons, it is neccessary to define the state of attachment of
    // a table as follows: A table is attached if, and ony if m_column
    // stores a non-null parent pointer. This works because even for
    // degenerate subtables, m_columns is initialized with the correct
    // parent pointer.
    return m_columns.has_parent();
}

inline std::size_t Table::get_column_count() const TIGHTDB_NOEXCEPT
{
    return m_spec.get_column_count();
}

inline StringData Table::get_column_name(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < get_column_count());
    return m_spec.get_column_name(ndx);
}

inline std::size_t Table::get_column_index(StringData name) const
{
    return m_spec.get_column_index(name);
}

inline ColumnType Table::get_real_column_type(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < get_column_count());
    return m_spec.get_real_column_type(ndx);
}

inline DataType Table::get_column_type(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < get_column_count());
    return m_spec.get_column_type(ndx);
}

template <class C, ColumnType coltype>
C& Table::get_column(std::size_t ndx)
{
    ColumnBase& column = get_column_base(ndx);
#ifdef TIGHTDB_DEBUG
    validate_column_type(column, coltype, ndx);
#endif
    return static_cast<C&>(column);
}

template <class C, ColumnType coltype>
const C& Table::get_column(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    const ColumnBase& column = get_column_base(ndx);
#ifdef TIGHTDB_DEBUG
    validate_column_type(column, coltype, ndx);
#endif
    return static_cast<const C&>(column);
}


inline bool Table::has_shared_spec() const
{
    return !m_top.is_attached();
}

inline Spec& Table::get_spec() TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(!has_shared_spec()); // you can only change specs on top-level tables
    return m_spec;
}

inline const Spec& Table::get_spec() const TIGHTDB_NOEXCEPT
{
    return m_spec;
}

class Table::UnbindGuard {
public:
    UnbindGuard(Table* table) TIGHTDB_NOEXCEPT: m_table(table)
    {
    }

    ~UnbindGuard() TIGHTDB_NOEXCEPT
    {
        if (m_table)
            m_table->unbind_ref();
    }

    Table& operator*() const TIGHTDB_NOEXCEPT
    {
        return *m_table;
    }

    Table* operator->() const TIGHTDB_NOEXCEPT
    {
        return m_table;
    }

    Table* get() const TIGHTDB_NOEXCEPT
    {
        return m_table;
    }

    Table* release() TIGHTDB_NOEXCEPT
    {
        Table* table = m_table;
        m_table = 0;
        return table;
    }

private:
    Table* m_table;
};

inline ref_type Table::create_empty_table(Allocator& alloc)
{
    Array top(Array::type_HasRefs, 0, 0, alloc);
    top.add(Spec::create_empty_spec(alloc));
    top.add(Array::create_empty_array(Array::type_HasRefs, alloc)); // Columns
    return top.get_ref();
}

#ifdef _MSC_VER
#pragma warning(push)
// Disable the Microsoft warning about passing "this" as a parameter to another constructor. Here it's safe.
#pragma warning(disable: 4355)
#endif

inline Table::Table(Allocator& alloc):
    m_size(0), m_top(alloc), m_columns(alloc), m_spec(this, alloc), m_ref_count(1),
    m_lookup_index(0)
{
    ref_type ref = create_empty_table(alloc); // Throws
    init_from_ref(ref, 0, 0);
}

inline Table::Table(const Table& t, Allocator& alloc):
    m_size(0), m_top(alloc), m_columns(alloc), m_spec(this, alloc), m_ref_count(1),
    m_lookup_index(0)
{
    ref_type ref = t.clone(alloc); // Throws
    init_from_ref(ref, 0, 0);
}

inline Table::Table(ref_count_tag, Allocator& alloc, ref_type top_ref,
                    Parent* parent, std::size_t ndx_in_parent):
    m_size(0), m_top(alloc), m_columns(alloc), m_spec(this, alloc), m_ref_count(0),
    m_lookup_index(0)
{
    init_from_ref(top_ref, parent, ndx_in_parent);
}

inline Table::Table(ref_count_tag, Allocator& alloc, ref_type spec_ref, ref_type columns_ref,
                    Parent* parent, std::size_t ndx_in_parent):
    m_size(0), m_top(alloc), m_columns(alloc), m_spec(this, alloc), m_ref_count(0),
    m_lookup_index(0)
{
    init_from_ref(spec_ref, columns_ref, parent, ndx_in_parent);
}

#ifdef _MSC_VER
#pragma warning(pop)
#endif


inline void Table::set_index(std::size_t column_ndx)
{
    detach_subtable_accessors();
    set_index(column_ndx, true);
}

inline TableRef Table::create(Allocator& alloc)
{
    ref_type ref = create_empty_table(alloc); // Throws
    Table* table = new Table(ref_count_tag(), alloc, ref, 0, 0); // Throws
    return table->get_table_ref();
}

inline TableRef Table::copy(Allocator& alloc) const
{
    ref_type ref = clone(alloc); // Throws
    Table* table = new Table(ref_count_tag(), alloc, ref, 0, 0); // Throws
    return table->get_table_ref();
}


inline void Table::insert_bool(std::size_t column_ndx, std::size_t row_ndx, bool value)
{
    insert_int(column_ndx, row_ndx, value);
}

inline void Table::insert_date(std::size_t column_ndx, std::size_t row_ndx, Date value)
{
    insert_int(column_ndx, row_ndx, value.get_date());
}

template<class E>
inline void Table::insert_enum(std::size_t column_ndx, std::size_t row_ndx, E value)
{
    insert_int(column_ndx, row_ndx, value);
}

inline void Table::insert_subtable(std::size_t col_ndx, std::size_t row_ndx)
{
    insert_subtable(col_ndx, row_ndx, 0); // Null stands for an empty table
}

template<class E>
inline void Table::set_enum(std::size_t column_ndx, std::size_t row_ndx, E value)
{
    set_int(column_ndx, row_ndx, value);
}

inline TableRef Table::get_subtable(std::size_t column_ndx, std::size_t row_ndx)
{
    return TableRef(get_subtable_ptr(column_ndx, row_ndx));
}

inline ConstTableRef Table::get_subtable(std::size_t column_ndx, std::size_t row_ndx) const
{
    return ConstTableRef(get_subtable_ptr(column_ndx, row_ndx));
}

inline bool Table::operator==(const Table& t) const
{
    return m_spec == t.m_spec && compare_rows(t);
}

inline bool Table::operator!=(const Table& t) const
{
    return m_spec != t.m_spec || !compare_rows(t);
}

inline void Table::insert_into(Table* parent, std::size_t col_ndx, std::size_t row_ndx) const
{
    parent->insert_subtable(col_ndx, row_ndx, this);
}

inline void Table::set_into_mixed(Table* parent, std::size_t col_ndx, std::size_t row_ndx) const
{
    parent->insert_mixed_subtable(col_ndx, row_ndx, this);
}


#ifdef TIGHTDB_ENABLE_REPLICATION

struct Table::LocalTransactLog {
    template<class T> void set_value(std::size_t column_ndx, std::size_t row_ndx, const T& value)
    {
        if (m_repl)
            m_repl->set_value(m_table, column_ndx, row_ndx, value); // Throws
    }

    template<class T> void insert_value(std::size_t column_ndx, std::size_t row_ndx, const T& value)
    {
        if (m_repl)
            m_repl->insert_value(m_table, column_ndx, row_ndx, value); // Throws
    }

    void row_insert_complete()
    {
        if (m_repl)
            m_repl->row_insert_complete(m_table); // Throws
    }

    void insert_empty_rows(std::size_t row_ndx, std::size_t num_rows)
    {
        if (m_repl)
            m_repl->insert_empty_rows(m_table, row_ndx, num_rows); // Throws
    }

    void remove_row(std::size_t row_ndx)
    {
        if (m_repl)
            m_repl->remove_row(m_table, row_ndx); // Throws
    }

    void add_int_to_column(std::size_t column_ndx, int64_t value)
    {
        if (m_repl)
            m_repl->add_int_to_column(m_table, column_ndx, value); // Throws
    }

    void add_index_to_column(std::size_t column_ndx)
    {
        if (m_repl)
            m_repl->add_index_to_column(m_table, column_ndx); // Throws
    }

    void clear_table()
    {
        if (m_repl)
            m_repl->clear_table(m_table); // Throws
    }

    void optimize_table()
    {
        if (m_repl)
            m_repl->optimize_table(m_table); // Throws
    }

    void add_column(DataType type, StringData name)
    {
        if (m_repl)
            m_repl->add_column(m_table, &m_table->m_spec, type, name); // Throws
    }

    void on_table_destroyed() TIGHTDB_NOEXCEPT
    {
        if (m_repl)
            m_repl->on_table_destroyed(m_table);
    }

private:
    Replication* const m_repl;
    Table* const m_table;
    LocalTransactLog(Replication* r, Table* t) TIGHTDB_NOEXCEPT: m_repl(r), m_table(t) {}
    friend class Table;
};

inline Table::LocalTransactLog Table::transact_log() TIGHTDB_NOEXCEPT
{
    return LocalTransactLog(m_top.get_alloc().get_replication(), this);
}

inline std::size_t* Table::record_subspec_path(const Spec* spec, std::size_t* begin,
                                               std::size_t* end) const TIGHTDB_NOEXCEPT
{
    if (spec != &m_spec) {
        TIGHTDB_ASSERT(m_spec.m_subspecs.is_attached());
        return spec->record_subspec_path(&m_spec.m_subspecs, begin, end);
    }
    return begin;
}

inline std::size_t* Table::record_subtable_path(std::size_t* begin,
                                                std::size_t* end) const TIGHTDB_NOEXCEPT
{
    const Array& real_top = m_top.is_attached() ? m_top : m_columns;
    std::size_t index_in_parent = real_top.get_ndx_in_parent();
    TIGHTDB_ASSERT(begin < end);
    *begin++ = index_in_parent;
    ArrayParent* parent = real_top.get_parent();
    TIGHTDB_ASSERT(parent);
    TIGHTDB_ASSERT(dynamic_cast<Parent*>(parent));
    return static_cast<Parent*>(parent)->record_subtable_path(begin, end);
}

inline std::size_t* Table::Parent::record_subtable_path(std::size_t* begin,
                                                        std::size_t*) TIGHTDB_NOEXCEPT
{
    return begin;
}

#endif // TIGHTDB_ENABLE_REPLICATION


} // namespace tightdb

#endif // TIGHTDB_TABLE_HPP
