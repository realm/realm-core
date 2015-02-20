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
#ifndef TIGHTDB_TABLE_VIEW_HPP
#define TIGHTDB_TABLE_VIEW_HPP

#include <iostream>

#include <tightdb/views.hpp>
#include <tightdb/table.hpp>
#include <tightdb/column.hpp>
#include <tightdb/util/features.h>
#include <tightdb/group_shared.hpp>
namespace tightdb {

using std::size_t;

// Views, tables and synchronization between them:
//
// Views are built through queries against either tables or another view.
// Views may be restricted to only hold entries provided by another view.
// this other view is called the "restricting view".
// Views may be sorted in ascending or descending order of values in one ore more columns.
//
// Views remember the query from which it was originally built.
// Views remember the table from which it was originally built.
// Views remember a restricting view if one was used when it was originally built.
// Views remember the last sorting criteria (column and direction)
//
// A view may be operated in one of two distinct modes: *reflective* and *imperative*.
// Sometimes the term "reactive" is used instead of "reflective" with the same meaning.
//
// Reflective views:
// - A reflective view *always* *reflect* the result of running the query.
//   If the underlying tables or tableviews change, the reflective view changes as well.
//   A reflective view may need to rerun the query it was generated from, a potentially
//   costly operation which happens on demand.
// - It does not matter whether changes are explicitly done within the transaction, or
//   occur implicitly as part of advance_read() or promote_to_write().
//
// Imperative views:
// - An imperative view only *initially* holds the result of the query. An imperative
//   view *never* reruns the query. To force the view to match it's query (by rerunning it),
//   the view must be operated in reflective mode.
//   An imperative view can be modified explicitly. References can be added, removed or
//   changed.
//
// - In imperative mode, the references in the view tracks movement of the referenced data:
//   If you delete an entry which is referenced from a view, said reference is detached,
//   not removed.
// - It does not matter whether the delete is done in-line (as part of the current transaction),
//   or if it is done implicitly as part of advance_read() or promote_to_write().
//
// Use cases:
//
// 1. Presenting data
// The first use case (and primary motivator behind the reflective view) is to just track
// and present the state of the database. In this case, the view is operated in reflective
// mode, it is not modified within the transaction, and it does not cause modification in
// other parts of the database.
//
// 2. Handover
// The second use case is "handover." The implicit rerun of the query in our first use case
// may be too costly to be acceptable on the main thread. Instead you want to run the query
// on a worker thread, but display it on the main thread. To achieve this, you need two
// SharedGroups locked on to the same version of the database. If you have that, you can
// *handover* a view from one thread/SharedGroup to the other. Currently, Handover is limited
// to views without a restricting view (see above) and to views created from a toplevel table
// (not a LinkView).
//
// You can handover both reflective and imperative views. But most often it will only make
// sense to handover an imperative view, because it is guaranteed not to rerun its query.
//
// Handover is expressed using a new type, HandoverTableView, which can be produced calling
// TableViewBase::export_for_handover(). After the call to export_for_handover(), the table view
// becomes detached to prevent misuse. On the receiving side, a new TableView is produced from
// HandoverTableView::import_from_handover(SharedGroup& sg). Export will fail if the TableView
// has a restricting view, or is created from a LinkView. 
// Import will fail if the importing and exporting SharedGroups are observing different 
// versions of the database.
//
// 3. Iterating a view and changing data
// The third use case (and a motivator behind the imperative view) is when you want
// to make changes to the database in accordance with a query result. Imagine you want to 
// find all employees with a salary below a limit and raise their salaries to the limit (pseudocode):
//
//    promote_to_write();
//    view = table.where().less_than(salary_column,limit).find_all();
//    for (size_t i = 0; i < view.size(); ++i)
//        view.set_int(salary_column, i, limit);
//    commit_and_continue_as_read();
//
// This is idiomatic imperative code and it works if the view is operated in imperative mode.
//
// If the view is operated in reflective mode, the behaviour surprises most people: When the
// first salary is changed, the entry no longer fullfills the query, so it is dropped from the
// view implicitly. view[0] is removed, view[1] moves to view[0] and so forth. But the next
// loop iteration has i=1 and refers to view[1], thus skipping view[0]. The end result is that
// every other employee get a raise, while the others don't.
//
// One way of dealing with use case 3 without supporting imperative views, is to *require* that
// the loop body is expressed as a lambda, which we'll call. This allows us to ensure that the
// lambda is only called with a valid iterator. Unfortunately, it also adds restrictions on what
// can be allowed inside the lambda. One of the things you can not easily allow is calling
// advance_read() or promote_to_write().
//
// 4. Iterating intermixed with implicit updates
// This leads us to use case 4, which is similar to use case 3, but uses promote_to_write()
// intermixed with iterating a view. This is actually quite important to some, who do not want
// to end up with a large write transaction.
//
//    view = table.where().less_than(salary_column,limit).find_all();
//    for (size_t i = 0; i < view.size(); ++i) {
//        promote_to_write();
//        view.set_int(salary_column, i, limit);
//        commit_and_continue_as_write();
//    }
//
// Anything can happen at the call to promote_to_write(). The key question then becomes: how
// do we support a safe way of realising the original goal (raising salaries) ?
//
// using the imperative operating mode:
//
//    view = table.where().less_than(salary_column,limit).find_all();
//    for (size_t i = 0; i < view.size(); ++i) {
//        promote_to_write();
//        Row r = view[i];
//        if (r.is_attached())
//            r.set_int(salary_column, limit);
//        commit_and_continue_as_write();
//    }
//
// This is safe, and we just aim for providing low level safety: is_attached() can tell
// if the reference is valid, and the references in the view continue to point to the
// same object at all times, also following implicit updates. The rest is up to the
// application logic.
//
// It is important to see, that there is no guarantee that all relevant employees get
// their raise in cases whith concurrent updates. At every call to promote_to_write() new
// employees may be added to the underlying table, but as the view is in imperative mode,
// these new employees are not added to the view. Also at promote_to_write() an existing
// employee could recieve a (different, larger) raise which would then be overwritten and lost.
// However, these are problems that you should expect, since the activity is spread over multiple
// transactions.


/// Common base class for TableView and ConstTableView.
class TableViewBase : public RowIndexes {
public:
// - not in use / implemented yet:
//    enum mode { mode_Reflective, mode_Imperative };
//    void set_operating_mode(mode);
//    mode get_operating_mode();
    bool is_empty() const TIGHTDB_NOEXCEPT;
    bool is_attached() const TIGHTDB_NOEXCEPT;
    std::size_t size() const TIGHTDB_NOEXCEPT;

    // Column information
    const ColumnBase& get_column_base(size_t index) const;

    size_t      get_column_count() const TIGHTDB_NOEXCEPT;
    StringData  get_column_name(size_t column_ndx) const TIGHTDB_NOEXCEPT;
    size_t      get_column_index(StringData name) const;
    DataType    get_column_type(size_t column_ndx) const TIGHTDB_NOEXCEPT;

    // Getting values
    int64_t     get_int(size_t column_ndx, size_t row_ndx) const TIGHTDB_NOEXCEPT;
    bool        get_bool(size_t column_ndx, size_t row_ndx) const TIGHTDB_NOEXCEPT;
    DateTime    get_datetime(size_t column_ndx, size_t row_ndx) const TIGHTDB_NOEXCEPT;
    float       get_float(size_t column_ndx, size_t row_ndx) const TIGHTDB_NOEXCEPT;
    double      get_double(size_t column_ndx, size_t row_ndx) const TIGHTDB_NOEXCEPT;
    StringData  get_string(size_t column_ndx, size_t row_ndx) const TIGHTDB_NOEXCEPT;
    BinaryData  get_binary(size_t column_ndx, size_t row_ndx) const TIGHTDB_NOEXCEPT;
    Mixed       get_mixed(size_t column_ndx, size_t row_ndx) const TIGHTDB_NOEXCEPT;
    DataType    get_mixed_type(size_t column_ndx, size_t row_ndx) const TIGHTDB_NOEXCEPT;
    std::size_t get_link(std::size_t column_ndx, std::size_t row_ndx) const TIGHTDB_NOEXCEPT;

    // Links
    bool is_null_link(std::size_t column_ndx, std::size_t row_ndx) const TIGHTDB_NOEXCEPT;

    // Subtables
    size_t get_subtable_size(size_t column_ndx, size_t row_ndx) const TIGHTDB_NOEXCEPT;

    // Searching (Int and String)
    size_t find_first_int(size_t column_ndx, int64_t value) const;
    size_t find_first_bool(size_t column_ndx, bool value) const;
    size_t find_first_datetime(size_t column_ndx, DateTime value) const;
    size_t find_first_float(size_t column_ndx, float value) const;
    size_t find_first_double(size_t column_ndx, double value) const;
    size_t find_first_string(size_t column_ndx, StringData value) const;
    size_t find_first_binary(size_t column_ndx, BinaryData value) const;

    // Aggregate functions. count_target is ignored by all <int
    // function> except Count. Hack because of bug in optional
    // arguments in clang and vs2010 (fixed in 2012)
    template <int function, typename T, typename R, class ColType>
    R aggregate(R (ColType::*aggregateMethod)(size_t, size_t, size_t, size_t*) const,
        size_t column_ndx, T count_target, size_t* return_ndx = null_ptr) const;

    int64_t sum_int(size_t column_ndx) const;
    int64_t maximum_int(size_t column_ndx, size_t* return_ndx = 0) const;
    int64_t minimum_int(size_t column_ndx, size_t* return_ndx = 0) const;
    double average_int(size_t column_ndx) const;
    size_t count_int(size_t column_ndx, int64_t target) const;

    double sum_float(size_t column_ndx) const;
    float maximum_float(size_t column_ndx, size_t* return_ndx = 0) const;
    float minimum_float(size_t column_ndx, size_t* return_ndx = 0) const;
    double average_float(size_t column_ndx) const;
    size_t count_float(size_t column_ndx, float target) const;

    double sum_double(size_t column_ndx) const;
    double maximum_double(size_t column_ndx, size_t* return_ndx = 0) const;
    double minimum_double(size_t column_ndx, size_t* return_ndx = 0) const;
    double average_double(size_t column_ndx) const;
    size_t count_double(size_t column_ndx, double target) const;

    DateTime maximum_datetime(size_t column_ndx, size_t* return_ndx = 0) const;
    DateTime minimum_datetime(size_t column_ndx, size_t* return_ndx = 0) const;

    void apply_same_order(TableViewBase& order);

    // Simple pivot aggregate method. Experimental! Please do not
    // document method publicly.
    void aggregate(size_t group_by_column, size_t aggr_column,
                   Table::AggrType op, Table& result) const;

    // Get row index in the source table this view is "looking" at.
    std::size_t get_source_ndx(std::size_t row_ndx) const TIGHTDB_NOEXCEPT;

    /// Search this view for the specified source table row (specified by its
    /// index in the source table). If found, the index of that row within this
    /// view is returned, otherwise `tightdb::not_found` is returned.
    std::size_t find_by_source_ndx(std::size_t source_ndx) const TIGHTDB_NOEXCEPT;

    // Conversion
    void to_json(std::ostream&) const;
    void to_string(std::ostream&, std::size_t limit = 500) const;
    void row_to_string(std::size_t row_ndx, std::ostream&) const;

#ifdef TIGHTDB_ENABLE_REPLICATION
    // Determine if the view is 'in sync' with the underlying table
    // as well as other views used to generate the view. Note that updates
    // through views maintains synchronization between view and table.
    // It doesnt by itself maintain other views as well. So if a view
    // is generated from another view (not a table), updates may cause
    // that view to be outdated, AND as the generated view depends upon
    // it, it too will become outdated.
    bool is_in_sync() const TIGHTDB_NOEXCEPT;

    // Synchronize a view to match a table or tableview from which it
    // has been derived. Synchronization is achieved by rerunning the
    // query used to generate the view. If derived from another view, that
    // view will be synchronized as well.
    //
    // "live" or "reactive" views are implemented by calling sync_if_needed
    // before any of the other access-methods whenever the view may have become
    // outdated.
    uint_fast64_t sync_if_needed() const;
#else
    uint_fast64_t sync_if_needed() const;
#endif

    // Set this undetached TableView to be a distinct view, and sync immediately.
    void sync_distinct_view(size_t column_ndx);

    // This TableView can be "born" from 4 different sources : LinkView, Table::get_distinct_view(),
    // Table::find_all() or Query. Return the version of the source it was created from.
    uint64_t outside_version() const;

protected:
#ifdef TIGHTDB_ENABLE_REPLICATION
    void do_sync();
#endif

    // Null if, and only if, the view is detached.
    mutable TableRef m_table;

    // If this TableView was created from a LinkView, then this reference points to it. Otherwise it's 0
    mutable ConstLinkViewRef m_linkview_source;

    mutable uint_fast64_t m_last_seen_version;

    // m_distinct_column_source != npos if this view was created from distinct values in a column of m_table.
    size_t m_distinct_column_source;

    // A valid query holds a reference to it's table which must match our m_table.
    // hence we can use A query with a null table reference to indicate that the view
    // was NOT generated by a query, but follows a table directly.
    Query m_query;
    // parameters for findall, needed to rerun the query
    size_t m_start;
    size_t m_end;
    size_t m_limit;

    /// Construct null view (no memory allocated).
    TableViewBase();

    /// Construct empty view, ready for addition of row indices.
    TableViewBase(Table* parent);
    TableViewBase(Table* parent, Query& query, size_t start, size_t end, size_t limit);

    /// Copy constructor.
    TableViewBase(const TableViewBase&);

    /// Moving constructor.
    TableViewBase(TableViewBase*) TIGHTDB_NOEXCEPT;

    ~TableViewBase() TIGHTDB_NOEXCEPT;

    void move_assign(TableViewBase*) TIGHTDB_NOEXCEPT;

    template<class R, class V> static R find_all_integer(V*, std::size_t, int64_t);
    template<class R, class V> static R find_all_float(V*, std::size_t, float);
    template<class R, class V> static R find_all_double(V*, std::size_t, double);
    template<class R, class V> static R find_all_string(V*, std::size_t, StringData);

    void prepare_for_export(std::size_t& table_num)
    {
        table_num = m_table->get_index_in_group();
        // must be group level table!
        TIGHTDB_ASSERT(table_num != npos);
        // FIXME: might need to propagate into base class and members

        detach();
    }
    void prepare_for_import(TableRef tr)
    {
        m_table = tr;
        m_last_seen_version = tr->m_version;
        tr->register_view(this);
        // FIXME: propagate into base class and members
    }
private:
    void detach() TIGHTDB_NOEXCEPT;
    std::size_t find_first_integer(std::size_t column_ndx, int64_t value) const;
    friend class Table;
    friend class Query;
    friend class SharedGroup;

    // Called by table to adjust any row references:
    void adj_row_acc_insert_rows(std::size_t row_ndx, std::size_t num_rows) TIGHTDB_NOEXCEPT;
    void adj_row_acc_erase_row(std::size_t row_ndx) TIGHTDB_NOEXCEPT;
    void adj_row_acc_move_over(std::size_t from_row_ndx, std::size_t to_row_ndx) TIGHTDB_NOEXCEPT;

    template<typename Tab> friend class BasicTableView;
};


inline void TableViewBase::detach() TIGHTDB_NOEXCEPT
{
    m_table = TableRef();
}


class ConstTableView;


/// A TableView gives read and write access to the parent table.
///
/// A 'const TableView' cannot be changed (e.g. sorted), nor can the
/// parent table be modified through it.
///
/// A TableView is both copyable and movable. Copying a TableView
/// makes a proper copy. Copying a temporary TableView is optimized
/// away on all modern compilers due to such things as 'return value
/// optimization'. Move semantics is accessed using the move()
/// function.
///
/// You should use 'return tv' whenever the type of 'tv' matches the
/// return type in the function signature exactly, such as
/// `T fun() { return T(...); }` or `T fun() { T tv; return tv }` to
/// enable return-value-optimization and named-return-value-optimization
/// respectively.
///
/// You should use 'return move(tv)' whenever the type of 'tv' mismatch
/// the signature (where 'tv' needs conversion to return type), such as
/// `ConstTableView fun() {TableView tv; return move(tv);}` to enable
/// move-semantics.
///
/// Avoid return(tv) whenever possible because it inhibits rvo and nrvo.
/// `return tv` has been benchmarked to be slower than `return move(tv)`
/// for both VC2012 and GCC 4.7 in many cases but never the opposite.
//
/// Note that move(tv) removes the contents from tv and leaves it
/// truncated.
///
/// FIXME: Add general documentation about move semantics, and refer
/// to it from here.
class TableView: public TableViewBase {
public:
    TableView();
    ~TableView() TIGHTDB_NOEXCEPT;
    TableView& operator=(TableView);
    friend TableView move(TableView& tv) { return TableView(&tv); }

    // Rows
    typedef BasicRowExpr<Table> RowExpr;
    typedef BasicRowExpr<const Table> ConstRowExpr;
    RowExpr get(std::size_t row_ndx) TIGHTDB_NOEXCEPT;
    ConstRowExpr get(std::size_t row_ndx) const TIGHTDB_NOEXCEPT;
    RowExpr front() TIGHTDB_NOEXCEPT;
    ConstRowExpr front() const TIGHTDB_NOEXCEPT;
    RowExpr back() TIGHTDB_NOEXCEPT;
    ConstRowExpr back() const TIGHTDB_NOEXCEPT;
    RowExpr operator[](std::size_t row_ndx) TIGHTDB_NOEXCEPT;
    ConstRowExpr operator[](std::size_t row_ndx) const TIGHTDB_NOEXCEPT;

    // Setting values
    void set_int(size_t column_ndx, size_t row_ndx, int64_t value);
    void set_bool(size_t column_ndx, size_t row_ndx, bool value);
    void set_datetime(size_t column_ndx, size_t row_ndx, DateTime value);
    template<class E> void set_enum(size_t column_ndx, size_t row_ndx, E value);
    void set_float(size_t column_ndx, size_t row_ndx, float value);
    void set_double(size_t column_ndx, size_t row_ndx, double value);
    void set_string(size_t column_ndx, size_t row_ndx, StringData value);
    void set_binary(size_t column_ndx, size_t row_ndx, BinaryData value);
    void set_mixed(size_t column_ndx, size_t row_ndx, Mixed value);
    void set_subtable(size_t column_ndx,size_t row_ndx, const Table* table);
    void set_link(std::size_t column_ndx, std::size_t row_ndx, std::size_t target_row_ndx);
    void add_int(size_t column_ndx, int64_t value);

    // Subtables
    TableRef      get_subtable(size_t column_ndx, size_t row_ndx);
    ConstTableRef get_subtable(size_t column_ndx, size_t row_ndx) const;
    void          clear_subtable(size_t column_ndx, size_t row_ndx);

    // Links
    TableRef get_link_target(std::size_t column_ndx) TIGHTDB_NOEXCEPT;
    ConstTableRef get_link_target(std::size_t column_ndx) const TIGHTDB_NOEXCEPT;
    void nullify_link(std::size_t column_ndx, std::size_t row_ndx);

    // Deleting
    void clear();
    void remove(std::size_t row_ndx);
    void remove_last();

    // Searching (Int and String)
    TableView       find_all_int(size_t column_ndx, int64_t value);
    ConstTableView  find_all_int(size_t column_ndx, int64_t value) const;
    TableView       find_all_bool(size_t column_ndx, bool value);
    ConstTableView  find_all_bool(size_t column_ndx, bool value) const;
    TableView       find_all_datetime(size_t column_ndx, DateTime value);
    ConstTableView  find_all_datetime(size_t column_ndx, DateTime value) const;
    TableView       find_all_float(size_t column_ndx, float value);
    ConstTableView  find_all_float(size_t column_ndx, float value) const;
    TableView       find_all_double(size_t column_ndx, double value);
    ConstTableView  find_all_double(size_t column_ndx, double value) const;
    TableView       find_all_string(size_t column_ndx, StringData value);
    ConstTableView  find_all_string(size_t column_ndx, StringData value) const;
    // FIXME: Need: TableView find_all_binary(size_t column_ndx, BinaryData value);
    // FIXME: Need: ConstTableView find_all_binary(size_t column_ndx, BinaryData value) const;

    Table& get_parent() TIGHTDB_NOEXCEPT;
    const Table& get_parent() const TIGHTDB_NOEXCEPT;

private:
    TableView(Table& parent);
    TableView(Table& parent, Query& query, size_t start, size_t end, size_t limit);
    TableView(TableView* tv) TIGHTDB_NOEXCEPT;

    TableView find_all_integer(size_t column_ndx, int64_t value);
    ConstTableView find_all_integer(size_t column_ndx, int64_t value) const;

    friend class ConstTableView;
    friend class Table;
    friend class Query;
    friend class TableViewBase;
    friend class ListviewNode;
    friend class LinkView;
};




/// A ConstTableView gives read access to the parent table, but no
/// write access. The view itself, though, can be changed, for
/// example, it can be sorted.
///
/// Note that methods are declared 'const' if, and only if they leave
/// the view unmodified, and this is irrespective of whether they
/// modify the parent table.
///
/// A ConstTableView has both copy and move semantics. See TableView
/// for more on this.
class ConstTableView: public TableViewBase {
public:
    ConstTableView();
    ~ConstTableView() TIGHTDB_NOEXCEPT;
    ConstTableView& operator=(ConstTableView);
    friend ConstTableView move(ConstTableView& tv) { return ConstTableView(&tv); }

    ConstTableView(TableView);
    ConstTableView& operator=(TableView);

    // Rows
    typedef BasicRowExpr<const Table> ConstRowExpr;
    ConstRowExpr get(std::size_t row_ndx) const TIGHTDB_NOEXCEPT;
    ConstRowExpr front() const TIGHTDB_NOEXCEPT;
    ConstRowExpr back() const TIGHTDB_NOEXCEPT;
    ConstRowExpr operator[](std::size_t row_ndx) const TIGHTDB_NOEXCEPT;

    // Subtables
    ConstTableRef get_subtable(size_t column_ndx, size_t row_ndx) const;

    // Links
    ConstTableRef get_link_target(std::size_t column_ndx) const TIGHTDB_NOEXCEPT;

    // Searching (Int and String)
    ConstTableView find_all_int(size_t column_ndx, int64_t value) const;
    ConstTableView find_all_bool(size_t column_ndx, bool value) const;
    ConstTableView find_all_datetime(size_t column_ndx, DateTime value) const;
    ConstTableView find_all_float(size_t column_ndx, float value) const;
    ConstTableView find_all_double(size_t column_ndx, double value) const;
    ConstTableView find_all_string(size_t column_ndx, StringData value) const;

    const Table& get_parent() const TIGHTDB_NOEXCEPT;

private:
    ConstTableView(const Table& parent);
    ConstTableView(ConstTableView*) TIGHTDB_NOEXCEPT;

    ConstTableView find_all_integer(size_t column_ndx, int64_t value) const;

    friend class TableView;
    friend class Table;
    friend class Query;
    friend class TableViewBase;
};

class HandoverTableView {
public:
    TableView import_from_handover(SharedGroup&);
    ConstTableView const_import_from_handover(SharedGroup&);
};


// ================================================================================================
// TableViewBase Implementation:


inline bool TableViewBase::is_empty() const TIGHTDB_NOEXCEPT
{
    return m_row_indexes.is_empty();
}

inline bool TableViewBase::is_attached() const TIGHTDB_NOEXCEPT
{
    return bool(m_table);
}

inline std::size_t TableViewBase::size() const TIGHTDB_NOEXCEPT
{
    return m_row_indexes.size();
}

inline std::size_t TableViewBase::get_source_ndx(std::size_t row_ndx) const TIGHTDB_NOEXCEPT
{
    return to_size_t(m_row_indexes.get(row_ndx));
}

inline std::size_t TableViewBase::find_by_source_ndx(std::size_t source_ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(source_ndx < m_table->size());
    return m_row_indexes.find_first(source_ndx);
}

inline TableViewBase::TableViewBase():
    RowIndexes(Column::unattached_root_tag(), Allocator::get_default()), // Throws
    m_distinct_column_source(npos)
{
#ifdef TIGHTDB_ENABLE_REPLICATION
    m_last_seen_version = 0;
    m_auto_sort = false;
#endif
    ref_type ref = Column::create(m_row_indexes.get_alloc()); // Throws
    m_row_indexes.get_root_array()->init_from_ref(ref);
}

inline TableViewBase::TableViewBase(Table* parent):
    RowIndexes(Column::unattached_root_tag(), Allocator::get_default()), 
    m_table(parent->get_table_ref()), // Throws
    m_distinct_column_source(npos)
    {
#ifdef TIGHTDB_ENABLE_REPLICATION
    m_last_seen_version = m_table ? m_table->m_version : 0;
    m_auto_sort = false;
#endif
    // FIXME: This code is unreasonably complicated because it uses `Column` as
    // a free-standing container, and beause `Column` does not conform to the
    // RAII idiom (nor should it).
    Allocator& alloc = m_row_indexes.get_alloc();
    _impl::DeepArrayRefDestroyGuard ref_guard(alloc);
    ref_guard.reset(Column::create(alloc)); // Throws
    parent->register_view(this); // Throws
    m_row_indexes.get_root_array()->init_from_ref(ref_guard.release());
}

inline TableViewBase::TableViewBase(Table* parent, Query& query, size_t start, size_t end, size_t limit):
    RowIndexes(Column::unattached_root_tag(), Allocator::get_default()), // Throws
    m_table(parent->get_table_ref()),
    m_distinct_column_source(npos),
    m_query(query, Query::TCopyExpressionTag())
{
#ifdef TIGHTDB_ENABLE_REPLICATION    
    m_last_seen_version = m_table ? m_table->m_version : 0;
    m_auto_sort = false;
#endif
    m_start = start;
    m_end = end;
    m_limit = limit;
    // FIXME: This code is unreasonably complicated because it uses `Column` as
    // a free-standing container, and beause `Column` does not conform to the
    // RAII idiom (nor should it).
    Allocator& alloc = m_row_indexes.get_alloc();
    _impl::DeepArrayRefDestroyGuard ref_guard(alloc);
    ref_guard.reset(Column::create(alloc)); // Throws
    parent->register_view(this); // Throws
    m_row_indexes.get_root_array()->init_from_ref(ref_guard.release());
}

inline TableViewBase::TableViewBase(const TableViewBase& tv):
    RowIndexes(Column::unattached_root_tag(), Allocator::get_default()),
    m_table(tv.m_table),
    m_distinct_column_source(tv.m_distinct_column_source),
    m_query(tv.m_query, Query::TCopyExpressionTag())
    {
#ifdef TIGHTDB_ENABLE_REPLICATION
    m_last_seen_version = tv.m_last_seen_version;
    m_auto_sort = tv.m_auto_sort;
    m_start = tv.m_start;
    m_end = tv.m_end;
    m_limit = tv.m_limit;
    m_linkview_source = tv.m_linkview_source;
    m_sorting_predicate = tv.m_sorting_predicate;
#endif
    // FIXME: This code is unreasonably complicated because it uses `Column` as
    // a free-standing container, and beause `Column` does not conform to the
    // RAII idiom (nor should it).
    Allocator& alloc = m_row_indexes.get_alloc();
    MemRef mem = tv.m_row_indexes.get_root_array()->clone_deep(alloc); // Throws
    _impl::DeepArrayRefDestroyGuard ref_guard(mem.m_ref, alloc);
    if (m_table)
        m_table->register_view(this); // Throws
    m_row_indexes.get_root_array()->init_from_mem(mem);
    ref_guard.release();
}

inline TableViewBase::TableViewBase(TableViewBase* tv) TIGHTDB_NOEXCEPT:
    RowIndexes(Column::move_tag(), tv->m_row_indexes),
    m_table(move(tv->m_table)),
    m_distinct_column_source(tv->m_distinct_column_source)
{
#ifdef TIGHTDB_ENABLE_REPLICATION
    // if we are created from a table view which is outdated, take care to use the outdated
    // version number so that we can later trigger a sync if needed.
    m_last_seen_version = tv->m_last_seen_version;
    m_auto_sort = tv->m_auto_sort;
    m_start = tv->m_start;
    m_end = tv->m_end;
    m_limit = tv->m_limit;
    m_linkview_source = tv->m_linkview_source;
    m_sorting_predicate = tv->m_sorting_predicate;
#endif
    if (m_table)
        m_table->move_registered_view(tv, this);
}

inline TableViewBase::~TableViewBase() TIGHTDB_NOEXCEPT
{
    if (m_table) {
        m_table->unregister_view(this);
        m_table = TableRef();
    }
    m_row_indexes.destroy(); // Shallow
}

inline void TableViewBase::move_assign(TableViewBase* tv) TIGHTDB_NOEXCEPT
{
    if (m_table)
        m_table->unregister_view(this);
    m_table = move(tv->m_table);
    if (m_table)
        m_table->move_registered_view(tv, this);

    m_row_indexes.move_assign(tv->m_row_indexes);
    m_query = tv->m_query;
#ifdef TIGHTDB_ENABLE_REPLICATION
    m_last_seen_version = tv->m_last_seen_version;
    m_auto_sort = tv->m_auto_sort;
    m_start = tv->m_start;
    m_end = tv->m_end;
    m_limit = tv->m_limit;
    m_linkview_source = tv->m_linkview_source;
    m_sorting_predicate = tv->m_sorting_predicate;
#endif
}

#define TIGHTDB_ASSERT_COLUMN(column_ndx)                                   \
    TIGHTDB_ASSERT(m_table);                                                \
    TIGHTDB_ASSERT(column_ndx < m_table->get_column_count());

#define TIGHTDB_ASSERT_ROW(row_ndx)                                         \
    TIGHTDB_ASSERT(m_table);                                                \
    TIGHTDB_ASSERT(row_ndx < m_row_indexes.size());

#define TIGHTDB_ASSERT_COLUMN_AND_TYPE(column_ndx, column_type)             \
    TIGHTDB_ASSERT_COLUMN(column_ndx)                                       \
    TIGHTDB_ASSERT(m_table->get_column_type(column_ndx) == column_type ||   \
                  (m_table->get_column_type(column_ndx) == type_DateTime && column_type == type_Int));

#define TIGHTDB_ASSERT_INDEX(column_ndx, row_ndx)                           \
    TIGHTDB_ASSERT_COLUMN(column_ndx)                                       \
    TIGHTDB_ASSERT(row_ndx < m_row_indexes.size());

#define TIGHTDB_ASSERT_INDEX_AND_TYPE(column_ndx, row_ndx, column_type)     \
    TIGHTDB_ASSERT_COLUMN_AND_TYPE(column_ndx, column_type)                 \
    TIGHTDB_ASSERT(row_ndx < m_row_indexes.size());

#define TIGHTDB_ASSERT_INDEX_AND_TYPE_TABLE_OR_MIXED(column_ndx, row_ndx)   \
    TIGHTDB_ASSERT_COLUMN(column_ndx)                                       \
    TIGHTDB_ASSERT(m_table->get_column_type(column_ndx) == type_Table ||    \
                   (m_table->get_column_type(column_ndx) == type_Mixed));   \
    TIGHTDB_ASSERT(row_ndx < m_row_indexes.size());

// Column information

inline const ColumnBase& TableViewBase::get_column_base(size_t index) const
{
    return m_table->get_column_base(index);
}

inline size_t TableViewBase::get_column_count() const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(m_table);
    return m_table->get_column_count();
}

inline StringData TableViewBase::get_column_name(size_t column_ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(m_table);
    return m_table->get_column_name(column_ndx);
}

inline size_t TableViewBase::get_column_index(StringData name) const
{
    TIGHTDB_ASSERT(m_table);
    return m_table->get_column_index(name);
}

inline DataType TableViewBase::get_column_type(size_t column_ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(m_table);
    return m_table->get_column_type(column_ndx);
}


// Getters


inline int64_t TableViewBase::get_int(size_t column_ndx, size_t row_ndx) const
    TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT_INDEX(column_ndx, row_ndx);

    const size_t real_ndx = size_t(m_row_indexes.get(row_ndx));
    return m_table->get_int(column_ndx, real_ndx);
}

inline bool TableViewBase::get_bool(size_t column_ndx, size_t row_ndx) const
    TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT_INDEX_AND_TYPE(column_ndx, row_ndx, type_Bool);

    const size_t real_ndx = size_t(m_row_indexes.get(row_ndx));
    return m_table->get_bool(column_ndx, real_ndx);
}

inline DateTime TableViewBase::get_datetime(size_t column_ndx, size_t row_ndx) const
    TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT_INDEX_AND_TYPE(column_ndx, row_ndx, type_DateTime);

    const size_t real_ndx = size_t(m_row_indexes.get(row_ndx));
    return m_table->get_datetime(column_ndx, real_ndx);
}

inline float TableViewBase::get_float(size_t column_ndx, size_t row_ndx) const
    TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT_INDEX_AND_TYPE(column_ndx, row_ndx, type_Float);

    const size_t real_ndx = size_t(m_row_indexes.get(row_ndx));
    return m_table->get_float(column_ndx, real_ndx);
}

inline double TableViewBase::get_double(size_t column_ndx, size_t row_ndx) const
    TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT_INDEX_AND_TYPE(column_ndx, row_ndx, type_Double);

    const size_t real_ndx = size_t(m_row_indexes.get(row_ndx));
    return m_table->get_double(column_ndx, real_ndx);
}

inline StringData TableViewBase::get_string(size_t column_ndx, size_t row_ndx) const
    TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT_INDEX_AND_TYPE(column_ndx, row_ndx, type_String);

    const size_t real_ndx = size_t(m_row_indexes.get(row_ndx));
    return m_table->get_string(column_ndx, real_ndx);
}

inline BinaryData TableViewBase::get_binary(size_t column_ndx, size_t row_ndx) const
    TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT_INDEX_AND_TYPE(column_ndx, row_ndx, type_Binary);

    const size_t real_ndx = size_t(m_row_indexes.get(row_ndx));
    return m_table->get_binary(column_ndx, real_ndx); // Throws
}

inline Mixed TableViewBase::get_mixed(size_t column_ndx, size_t row_ndx) const
    TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT_INDEX_AND_TYPE(column_ndx, row_ndx, type_Mixed);

    const size_t real_ndx = size_t(m_row_indexes.get(row_ndx));
    return m_table->get_mixed(column_ndx, real_ndx); // Throws
}

inline DataType TableViewBase::get_mixed_type(size_t column_ndx, size_t row_ndx) const
    TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT_INDEX_AND_TYPE(column_ndx, row_ndx, type_Mixed);

    const size_t real_ndx = size_t(m_row_indexes.get(row_ndx));
    return m_table->get_mixed_type(column_ndx, real_ndx);
}

inline size_t TableViewBase::get_subtable_size(size_t column_ndx, size_t row_ndx) const
    TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT_INDEX_AND_TYPE_TABLE_OR_MIXED(column_ndx, row_ndx);

    const size_t real_ndx = size_t(m_row_indexes.get(row_ndx));
    return m_table->get_subtable_size(column_ndx, real_ndx);
}

inline std::size_t TableViewBase::get_link(std::size_t column_ndx, std::size_t row_ndx) const
    TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT_INDEX_AND_TYPE(column_ndx, row_ndx, type_Link);

    const size_t real_ndx = size_t(m_row_indexes.get(row_ndx));
    return m_table->get_link(column_ndx, real_ndx);
}

inline TableRef TableView::get_link_target(std::size_t column_ndx) TIGHTDB_NOEXCEPT
{
    return m_table->get_link_target(column_ndx);
}

inline ConstTableRef TableView::get_link_target(std::size_t column_ndx) const TIGHTDB_NOEXCEPT
{
    return m_table->get_link_target(column_ndx);
}

inline ConstTableRef ConstTableView::get_link_target(std::size_t column_ndx) const TIGHTDB_NOEXCEPT
{
    return m_table->get_link_target(column_ndx);
}

inline bool TableViewBase::is_null_link(std::size_t column_ndx, std::size_t row_ndx) const
    TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT_INDEX_AND_TYPE(column_ndx, row_ndx, type_Link);

    const size_t real_ndx = size_t(m_row_indexes.get(row_ndx));
    return m_table->is_null_link(column_ndx, real_ndx);
}


// Searching


inline size_t TableViewBase::find_first_int(size_t column_ndx, int64_t value) const
{
    TIGHTDB_ASSERT_COLUMN_AND_TYPE(column_ndx, type_Int);
    return find_first_integer(column_ndx, value);
}

inline size_t TableViewBase::find_first_bool(size_t column_ndx, bool value) const
{
    TIGHTDB_ASSERT_COLUMN_AND_TYPE(column_ndx, type_Bool);
    return find_first_integer(column_ndx, value ? 1 : 0);
}

inline size_t TableViewBase::find_first_datetime(size_t column_ndx, DateTime value) const
{
    TIGHTDB_ASSERT_COLUMN_AND_TYPE(column_ndx, type_DateTime);
    return find_first_integer(column_ndx, int64_t(value.get_datetime()));
}


template <class R, class V>
R TableViewBase::find_all_integer(V* view, size_t column_ndx, int64_t value)
{
    typedef typename util::RemoveConst<V>::type TNonConst;
    return view->m_table->where(const_cast<TNonConst*>(view)).equal(column_ndx, value).find_all();
}

template <class R, class V>
R TableViewBase::find_all_float(V* view, size_t column_ndx, float value)
{
    typedef typename util::RemoveConst<V>::type TNonConst;
    return view->m_table->where(const_cast<TNonConst*>(view)).equal(column_ndx, value).find_all();
}

template <class R, class V>
R TableViewBase::find_all_double(V* view, size_t column_ndx, double value)
{
    typedef typename util::RemoveConst<V>::type TNonConst;
    return view->m_table->where(const_cast<TNonConst*>(view)).equal(column_ndx, value).find_all();
}

template <class R, class V>
R TableViewBase::find_all_string(V* view, size_t column_ndx, StringData value)
{
    typedef typename util::RemoveConst<V>::type TNonConst;
    return view->m_table->where(const_cast<TNonConst*>(view)).equal(column_ndx, value).find_all();
}


//-------------------------- TableView, ConstTableView implementation:

inline TableView::TableView()
{
}

inline ConstTableView::ConstTableView()
{
}

inline ConstTableView::ConstTableView(TableView tv):
    TableViewBase(&tv)
{
}

inline TableView::~TableView() TIGHTDB_NOEXCEPT
{
}

inline ConstTableView::~ConstTableView() TIGHTDB_NOEXCEPT
{
}

inline TableView& TableView::operator=(TableView tv)
{
    move_assign(&tv);
    return *this;
}

inline ConstTableView& ConstTableView::operator=(ConstTableView tv)
{
    move_assign(&tv);
    return *this;
}

inline ConstTableView& ConstTableView::operator=(TableView tv)
{
    move_assign(&tv);
    return *this;
}

inline void TableView::remove_last()
{
    if (!is_empty())
        remove(size()-1);
}

inline Table& TableView::get_parent() TIGHTDB_NOEXCEPT
{
    return *m_table;
}

inline const Table& TableView::get_parent() const TIGHTDB_NOEXCEPT
{
    return *m_table;
}

inline const Table& ConstTableView::get_parent() const TIGHTDB_NOEXCEPT
{
    return *m_table;
}

inline TableView::TableView(Table& parent):
    TableViewBase(&parent)
{
}

inline TableView::TableView(Table& parent, Query& query, size_t start, size_t end, size_t limit):
    TableViewBase(&parent, query, start, end, limit)
{
}

inline ConstTableView::ConstTableView(const Table& parent):
    TableViewBase(const_cast<Table*>(&parent))
{
}

inline TableView::TableView(TableView* tv) TIGHTDB_NOEXCEPT:
    TableViewBase(tv)
{
}

inline ConstTableView::ConstTableView(ConstTableView* tv) TIGHTDB_NOEXCEPT:
    TableViewBase(tv)
{
}



// - string
inline TableView TableView::find_all_string(size_t column_ndx, StringData value)
{
    return TableViewBase::find_all_string<TableView>(this, column_ndx, value);
}

inline ConstTableView TableView::find_all_string(size_t column_ndx, StringData value) const
{
    return TableViewBase::find_all_string<ConstTableView>(this, column_ndx, value);
}

inline ConstTableView ConstTableView::find_all_string(size_t column_ndx, StringData value) const
{
    return TableViewBase::find_all_string<ConstTableView>(this, column_ndx, value);
}

// - float
inline TableView TableView::find_all_float(size_t column_ndx, float value)
{
    return TableViewBase::find_all_float<TableView>(this, column_ndx, value);
}

inline ConstTableView TableView::find_all_float(size_t column_ndx, float value) const
{
    return TableViewBase::find_all_float<ConstTableView>(this, column_ndx, value);
}

inline ConstTableView ConstTableView::find_all_float(size_t column_ndx, float value) const
{
    return TableViewBase::find_all_float<ConstTableView>(this, column_ndx, value);
}


// - double
inline TableView TableView::find_all_double(size_t column_ndx, double value)
{
    return TableViewBase::find_all_double<TableView>(this, column_ndx, value);
}

inline ConstTableView TableView::find_all_double(size_t column_ndx, double value) const
{
    return TableViewBase::find_all_double<ConstTableView>(this, column_ndx, value);
}

inline ConstTableView ConstTableView::find_all_double(size_t column_ndx, double value) const
{
    return TableViewBase::find_all_double<ConstTableView>(this, column_ndx, value);
}



// -- 3 variants of the 3 find_all_{int, bool, date} all based on integer

inline TableView TableView::find_all_integer(size_t column_ndx, int64_t value)
{
    return TableViewBase::find_all_integer<TableView>(this, column_ndx, value);
}

inline ConstTableView TableView::find_all_integer(size_t column_ndx, int64_t value) const
{
    return TableViewBase::find_all_integer<ConstTableView>(this, column_ndx, value);
}

inline ConstTableView ConstTableView::find_all_integer(size_t column_ndx, int64_t value) const
{
    return TableViewBase::find_all_integer<ConstTableView>(this, column_ndx, value);
}


inline TableView TableView::find_all_int(size_t column_ndx, int64_t value)
{
    TIGHTDB_ASSERT_COLUMN_AND_TYPE(column_ndx, type_Int);
    return find_all_integer(column_ndx, value);
}

inline TableView TableView::find_all_bool(size_t column_ndx, bool value)
{
    TIGHTDB_ASSERT_COLUMN_AND_TYPE(column_ndx, type_Bool);
    return find_all_integer(column_ndx, value ? 1 : 0);
}

inline TableView TableView::find_all_datetime(size_t column_ndx, DateTime value)
{
    TIGHTDB_ASSERT_COLUMN_AND_TYPE(column_ndx, type_DateTime);
    return find_all_integer(column_ndx, int64_t(value.get_datetime()));
}


inline ConstTableView TableView::find_all_int(size_t column_ndx, int64_t value) const
{
    TIGHTDB_ASSERT_COLUMN_AND_TYPE(column_ndx, type_Int);
    return find_all_integer(column_ndx, value);
}

inline ConstTableView TableView::find_all_bool(size_t column_ndx, bool value) const
{
    TIGHTDB_ASSERT_COLUMN_AND_TYPE(column_ndx, type_Bool);
    return find_all_integer(column_ndx, value ? 1 : 0);
}

inline ConstTableView TableView::find_all_datetime(size_t column_ndx, DateTime value) const
{
    TIGHTDB_ASSERT_COLUMN_AND_TYPE(column_ndx, type_DateTime);
    return find_all_integer(column_ndx, int64_t(value.get_datetime()));
}


inline ConstTableView ConstTableView::find_all_int(size_t column_ndx, int64_t value) const
{
    TIGHTDB_ASSERT_COLUMN_AND_TYPE(column_ndx, type_Int);
    return find_all_integer(column_ndx, value);
}

inline ConstTableView ConstTableView::find_all_bool(size_t column_ndx, bool value) const
{
    TIGHTDB_ASSERT_COLUMN_AND_TYPE(column_ndx, type_Bool);
    return find_all_integer(column_ndx, value ? 1 : 0);
}

inline ConstTableView ConstTableView::find_all_datetime(size_t column_ndx, DateTime value) const
{
    TIGHTDB_ASSERT_COLUMN_AND_TYPE(column_ndx, type_DateTime);
    return find_all_integer(column_ndx, int64_t(value.get_datetime()));
}


// Rows


inline TableView::RowExpr TableView::get(std::size_t row_ndx) TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT_ROW(row_ndx);
    std::size_t real_ndx = std::size_t(m_row_indexes.get(row_ndx));
    return m_table->get(real_ndx);
}

inline TableView::ConstRowExpr TableView::get(std::size_t row_ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT_ROW(row_ndx);
    std::size_t real_ndx = std::size_t(m_row_indexes.get(row_ndx));
    return m_table->get(real_ndx);
}

inline ConstTableView::ConstRowExpr ConstTableView::get(std::size_t row_ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT_ROW(row_ndx);
    std::size_t real_ndx = std::size_t(m_row_indexes.get(row_ndx));
    return m_table->get(real_ndx);
}

inline TableView::RowExpr TableView::front() TIGHTDB_NOEXCEPT
{
    return get(0);
}

inline TableView::ConstRowExpr TableView::front() const TIGHTDB_NOEXCEPT
{
    return get(0);
}

inline ConstTableView::ConstRowExpr ConstTableView::front() const TIGHTDB_NOEXCEPT
{
    return get(0);
}

inline TableView::RowExpr TableView::back() TIGHTDB_NOEXCEPT
{
    std::size_t last_row_ndx = size() - 1;
    return get(last_row_ndx);
}

inline TableView::ConstRowExpr TableView::back() const TIGHTDB_NOEXCEPT
{
    std::size_t last_row_ndx = size() - 1;
    return get(last_row_ndx);
}

inline ConstTableView::ConstRowExpr ConstTableView::back() const TIGHTDB_NOEXCEPT
{
    std::size_t last_row_ndx = size() - 1;
    return get(last_row_ndx);
}

inline TableView::RowExpr TableView::operator[](std::size_t row_ndx) TIGHTDB_NOEXCEPT
{
    return get(row_ndx);
}

inline TableView::ConstRowExpr TableView::operator[](std::size_t row_ndx) const TIGHTDB_NOEXCEPT
{
    return get(row_ndx);
}

inline ConstTableView::ConstRowExpr
ConstTableView::operator[](std::size_t row_ndx) const TIGHTDB_NOEXCEPT
{
    return get(row_ndx);
}


// Subtables


inline TableRef TableView::get_subtable(size_t column_ndx, size_t row_ndx)
{
    TIGHTDB_ASSERT_INDEX_AND_TYPE_TABLE_OR_MIXED(column_ndx, row_ndx);

    const size_t real_ndx = size_t(m_row_indexes.get(row_ndx));
    return m_table->get_subtable(column_ndx, real_ndx);
}

inline ConstTableRef TableView::get_subtable(size_t column_ndx, size_t row_ndx) const
{
    TIGHTDB_ASSERT_INDEX_AND_TYPE_TABLE_OR_MIXED(column_ndx, row_ndx);

    const size_t real_ndx = size_t(m_row_indexes.get(row_ndx));
    return m_table->get_subtable(column_ndx, real_ndx);
}

inline ConstTableRef ConstTableView::get_subtable(size_t column_ndx, size_t row_ndx) const
{
    TIGHTDB_ASSERT_INDEX_AND_TYPE_TABLE_OR_MIXED(column_ndx, row_ndx);

    const size_t real_ndx = size_t(m_row_indexes.get(row_ndx));
    return m_table->get_subtable(column_ndx, real_ndx);
}

inline void TableView::clear_subtable(size_t column_ndx, size_t row_ndx)
{
    TIGHTDB_ASSERT_INDEX_AND_TYPE_TABLE_OR_MIXED(column_ndx, row_ndx);

    const size_t real_ndx = size_t(m_row_indexes.get(row_ndx));
    return m_table->clear_subtable(column_ndx, real_ndx);
}


// Setters


inline void TableView::set_int(size_t column_ndx, size_t row_ndx, int64_t value)
{
    TIGHTDB_ASSERT_INDEX_AND_TYPE(column_ndx, row_ndx, type_Int);

    const size_t real_ndx = size_t(m_row_indexes.get(row_ndx));
    m_table->set_int(column_ndx, real_ndx, value);
}

inline void TableView::set_bool(size_t column_ndx, size_t row_ndx, bool value)
{
    TIGHTDB_ASSERT_INDEX_AND_TYPE(column_ndx, row_ndx, type_Bool);

    const size_t real_ndx = size_t(m_row_indexes.get(row_ndx));
    m_table->set_bool(column_ndx, real_ndx, value);
}

inline void TableView::set_datetime(size_t column_ndx, size_t row_ndx, DateTime value)
{
    TIGHTDB_ASSERT_INDEX_AND_TYPE(column_ndx, row_ndx, type_DateTime);

    const size_t real_ndx = size_t(m_row_indexes.get(row_ndx));
    m_table->set_datetime(column_ndx, real_ndx, value);
}

inline void TableView::set_float(size_t column_ndx, size_t row_ndx, float value)
{
    TIGHTDB_ASSERT_INDEX_AND_TYPE(column_ndx, row_ndx, type_Float);

    const size_t real_ndx = size_t(m_row_indexes.get(row_ndx));
    m_table->set_float(column_ndx, real_ndx, value);
}

inline void TableView::set_double(size_t column_ndx, size_t row_ndx, double value)
{
    TIGHTDB_ASSERT_INDEX_AND_TYPE(column_ndx, row_ndx, type_Double);

    const size_t real_ndx = size_t(m_row_indexes.get(row_ndx));
    m_table->set_double(column_ndx, real_ndx, value);
}

template<class E> inline void TableView::set_enum(size_t column_ndx, size_t row_ndx, E value)
{
    const size_t real_ndx = size_t(m_row_indexes.get(row_ndx));
    m_table->set_int(column_ndx, real_ndx, value);
}

inline void TableView::set_string(size_t column_ndx, size_t row_ndx, StringData value)
{
    TIGHTDB_ASSERT_INDEX_AND_TYPE(column_ndx, row_ndx, type_String);

    const size_t real_ndx = size_t(m_row_indexes.get(row_ndx));
    m_table->set_string(column_ndx, real_ndx, value);
}

inline void TableView::set_binary(size_t column_ndx, size_t row_ndx, BinaryData value)
{
    TIGHTDB_ASSERT_INDEX_AND_TYPE(column_ndx, row_ndx, type_Binary);

    const size_t real_ndx = size_t(m_row_indexes.get(row_ndx));
    m_table->set_binary(column_ndx, real_ndx, value);
}

inline void TableView::set_mixed(size_t column_ndx, size_t row_ndx, Mixed value)
{
    TIGHTDB_ASSERT_INDEX_AND_TYPE(column_ndx, row_ndx, type_Mixed);

    const size_t real_ndx = size_t(m_row_indexes.get(row_ndx));
    m_table->set_mixed(column_ndx, real_ndx, value);
}

inline void TableView::set_subtable(size_t column_ndx, size_t row_ndx, const Table* value)
{
    TIGHTDB_ASSERT_INDEX_AND_TYPE_TABLE_OR_MIXED(column_ndx, row_ndx);
    const size_t real_ndx = size_t(m_row_indexes.get(row_ndx));
    m_table->set_subtable(column_ndx, real_ndx, value);
}

inline void TableView::set_link(std::size_t column_ndx, std::size_t row_ndx, std::size_t target_row_ndx)
{
    TIGHTDB_ASSERT_INDEX_AND_TYPE(column_ndx, row_ndx, type_Link);
    const size_t real_ndx = size_t(m_row_indexes.get(row_ndx));
    m_table->set_link(column_ndx, real_ndx, target_row_ndx);
}

inline void TableView::nullify_link(std::size_t column_ndx, std::size_t row_ndx)
{
    TIGHTDB_ASSERT_INDEX_AND_TYPE(column_ndx, row_ndx, type_Link);
    const size_t real_ndx = size_t(m_row_indexes.get(row_ndx));
    m_table->nullify_link(column_ndx, real_ndx);
}


inline void TableView::add_int(size_t column_ndx, int64_t value)
{
    m_table->add_int(column_ndx, value);
}

} // namespace tightdb

#endif // TIGHTDB_TABLE_VIEW_HPP
