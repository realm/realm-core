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

#include <realm/table_view.hpp>


#include <realm/column.hpp>
#include <realm/query_conditions.hpp>
#include <realm/column_basic.hpp>
#include <realm/util/utf8.hpp>
#include <realm/index_string.hpp>

using namespace realm;

TableViewBase::TableViewBase(TableViewBase& src, Handover_patch& patch, 
                             MutableSourcePayload mode)
    : RowIndexes(src, mode), 
      m_linkview_source(LinkViewRef()),
      m_query(src.m_query, patch.query_patch, mode)
{
    patch.was_in_sync = src.is_in_sync();
    patch.table_num = src.m_table->get_index_in_group();
    // must be group level table!
    if (patch.table_num == npos) {
        throw std::runtime_error("TableView handover failed: not a group level table");
    }
    LinkView::generate_patch(src.m_linkview_source, patch.linkview_patch);
    m_table = TableRef();
    src.m_last_seen_version = -1; // bring source out-of-sync, now that it has lost its data
    m_last_seen_version = 0;
    m_distinct_column_source = src.m_distinct_column_source;
    m_sorting_predicate = src.m_sorting_predicate;
    m_auto_sort = src.m_auto_sort;
    m_start = src.m_start;
    m_end = src.m_end;
    m_limit = src.m_limit;
    m_num_detached_refs = 0;
}

TableViewBase::TableViewBase(const TableViewBase& src, Handover_patch& patch, 
                             ConstSourcePayload mode)
    : RowIndexes(src, mode), 
      m_linkview_source(LinkViewRef()),
      m_query(src.m_query, patch.query_patch, mode)
{
    if (mode == ConstSourcePayload::Stay)
        patch.was_in_sync = false;
    else
        patch.was_in_sync = src.is_in_sync();
    patch.table_num = src.m_table->get_index_in_group();
    // must be group level table!
    if (patch.table_num == npos) {
        throw std::runtime_error("TableView handover failed: not a group level table");
    }
    LinkView::generate_patch(src.m_linkview_source, patch.linkview_patch);
    m_table = TableRef();
    m_last_seen_version = 0;
    m_distinct_column_source = src.m_distinct_column_source;
    m_sorting_predicate = src.m_sorting_predicate;
    m_auto_sort = src.m_auto_sort;
    m_start = src.m_start;
    m_end = src.m_end;
    m_limit = src.m_limit;
    m_num_detached_refs = 0;
}

void TableViewBase::apply_patch(Handover_patch& patch, Group& group)
{
    TableRef tr = group.get_table(patch.table_num);
    m_table = tr;
    if (patch.was_in_sync)
        m_last_seen_version = tr->m_version;
    else
        m_last_seen_version = -1;
    tr->register_view(this);
    m_query.apply_patch(patch.query_patch, group);
    m_linkview_source = LinkView::create_from_and_consume_patch(patch.linkview_patch, group);
}

// Searching

// find_*_integer() methods are used for all "kinds" of integer values (bool, int, DateTime)

size_t TableViewBase::find_first_integer(size_t column_ndx, int64_t value) const
{
    check_cookie();

    for (size_t i = 0; i < m_row_indexes.size(); i++)
        if (is_row_attached(i) && get_int(column_ndx, i) == value)
            return i;
    return size_t(-1);
}

size_t TableViewBase::find_first_float(size_t column_ndx, float value) const
{
    check_cookie();

    for (size_t i = 0; i < m_row_indexes.size(); i++)
        if (is_row_attached(i) && get_float(column_ndx, i) == value)
            return i;
    return size_t(-1);
}

size_t TableViewBase::find_first_double(size_t column_ndx, double value) const
{
    check_cookie();

    for (size_t i = 0; i < m_row_indexes.size(); i++)
        if (is_row_attached(i) && get_double(column_ndx, i) == value)
            return i;
    return size_t(-1);
}

size_t TableViewBase::find_first_string(size_t column_ndx, StringData value) const
{
    check_cookie();

    REALM_ASSERT_COLUMN_AND_TYPE(column_ndx, type_String);

    for (size_t i = 0; i < m_row_indexes.size(); i++)
        if (is_row_attached(i) && get_string(column_ndx, i) == value) return i;
    return size_t(-1);
}

size_t TableViewBase::find_first_binary(size_t column_ndx, BinaryData value) const
{
    check_cookie();

    REALM_ASSERT_COLUMN_AND_TYPE(column_ndx, type_Binary);

    for (size_t i = 0; i < m_row_indexes.size(); i++)
        if (is_row_attached(i) && get_binary(column_ndx, i) == value) return i;
    return size_t(-1);
}


// Aggregates ----------------------------------------------------

// count_target is ignored by all <int function> except Count. Hack because of bug in optional
// arguments in clang and vs2010 (fixed in 2012)
template <int function, typename T, typename R, class ColType>
R TableViewBase::aggregate(R(ColType::*aggregateMethod)(size_t, size_t, size_t, size_t*) const, size_t column_ndx, T count_target, size_t* return_ndx) const
{
    check_cookie();

    using ColTypeTraits = ColumnTypeTraits<T, ColType::nullable>;
    REALM_ASSERT_COLUMN_AND_TYPE(column_ndx, ColTypeTraits::id);
    REALM_ASSERT(function == act_Sum || function == act_Max || function == act_Min || function == act_Count);
    REALM_ASSERT(m_table);
    REALM_ASSERT(column_ndx < m_table->get_column_count());
    if ((m_row_indexes.size() - m_num_detached_refs) == 0)
        return 0;

    typedef typename ColumnTypeTraits<T, ColType::nullable>::leaf_type ArrType;
    const ColType* column = static_cast<ColType*>(&m_table->get_column_base(column_ndx));

    if (m_num_detached_refs == 0 && m_row_indexes.size() == column->size()) {
        // direct aggregate on the column
        if(function == act_Count)
            return static_cast<R>(column->count(count_target));
        else
            return (column->*aggregateMethod)(0, size_t(-1), size_t(-1), return_ndx); // end == limit == -1
    }

    // Array object instantiation must NOT allocate initial memory (capacity)
    // with 'new' because it will lead to mem leak. The column keeps ownership
    // of the payload in array and will free it itself later, so we must not call destroy() on array.
    ArrType arr(column->get_alloc());
    const ArrType* arrp = nullptr;
    size_t leaf_start = 0;
    size_t leaf_end = 0;
    size_t row_ndx;

    R res = static_cast<R>(0);
    T first = column->get(to_size_t(m_row_indexes.get(0)));

    if (return_ndx)
        *return_ndx = 0;
    
    if(function == act_Count)
        res = static_cast<R>((first == count_target ? 1 : 0));
    else
        res = static_cast<R>(first);

    for (size_t ss = 1; ss < m_row_indexes.size(); ++ss) {
        row_ndx = to_size_t(m_row_indexes.get(ss));

        // skip detached references:
        if (row_ndx == detached_ref) continue;

        if (row_ndx < leaf_start || row_ndx >= leaf_end) {
            size_t ndx_in_leaf;
            typename ColType::LeafInfo leaf { &arrp, &arr };
            column->get_leaf(row_ndx, ndx_in_leaf, leaf);
            leaf_start = row_ndx - ndx_in_leaf;
            leaf_end = leaf_start + arrp->size();
        }

        T v = arrp->get(row_ndx - leaf_start);

        if (function == act_Sum)
            res += static_cast<R>(v);
        else if (function == act_Max && v > static_cast<T>(res)) {
            res = static_cast<R>(v);
            if (return_ndx)
                *return_ndx = ss;
        }
        else if (function == act_Min && v < static_cast<T>(res)) {
            res = static_cast<R>(v);
            if (return_ndx)
                *return_ndx = ss;
        }
        else if (function == act_Count && v == count_target)
            res++;

    }

    return res;
}

// sum 

int64_t TableViewBase::sum_int(size_t column_ndx) const
{
    return aggregate<act_Sum, int64_t>(&Column::sum, column_ndx, 0);
}
double TableViewBase::sum_float(size_t column_ndx) const
{
    return aggregate<act_Sum, float>(&ColumnFloat::sum, column_ndx, 0.0);
}
double TableViewBase::sum_double(size_t column_ndx) const
{
    return aggregate<act_Sum, double>(&ColumnDouble::sum, column_ndx, 0.0);
}

// Maximum

int64_t TableViewBase::maximum_int(size_t column_ndx, size_t* return_ndx) const
{
    return aggregate<act_Max, int64_t>(&Column::maximum, column_ndx, 0, return_ndx);
}
float TableViewBase::maximum_float(size_t column_ndx, size_t* return_ndx) const
{
    return aggregate<act_Max, float>(&ColumnFloat::maximum, column_ndx, 0.0, return_ndx);
}
double TableViewBase::maximum_double(size_t column_ndx, size_t* return_ndx) const
{
    return aggregate<act_Max, double>(&ColumnDouble::maximum, column_ndx, 0.0, return_ndx);
}
DateTime TableViewBase::maximum_datetime(size_t column_ndx, size_t* return_ndx) const
{
    return aggregate<act_Max, int64_t>(&Column::maximum, column_ndx, 0, return_ndx);
}

// Minimum

int64_t TableViewBase::minimum_int(size_t column_ndx, size_t* return_ndx) const
{
    return aggregate<act_Min, int64_t>(&Column::minimum, column_ndx, 0, return_ndx);
}
float TableViewBase::minimum_float(size_t column_ndx, size_t* return_ndx) const
{
    return aggregate<act_Min, float>(&ColumnFloat::minimum, column_ndx, 0.0, return_ndx);
}
double TableViewBase::minimum_double(size_t column_ndx, size_t* return_ndx) const
{
    return aggregate<act_Min, double>(&ColumnDouble::minimum, column_ndx, 0.0, return_ndx);
}
DateTime TableViewBase::minimum_datetime(size_t column_ndx, size_t* return_ndx) const
{
    return aggregate<act_Min, int64_t>(&Column::minimum, column_ndx, 0, return_ndx);
}

// Average

double TableViewBase::average_int(size_t column_ndx) const
{
    return aggregate<act_Sum, int64_t>(&Column::sum, column_ndx, 0) / static_cast<double>(num_attached_rows());
}
double TableViewBase::average_float(size_t column_ndx) const
{
    return aggregate<act_Sum, float>(&ColumnFloat::sum, column_ndx, 0.0) 
        / static_cast<double>(num_attached_rows());
}
double TableViewBase::average_double(size_t column_ndx) const
{
    return aggregate<act_Sum, double>(&ColumnDouble::sum, column_ndx, 0.0) 
        / static_cast<double>(num_attached_rows());
}

// Count
size_t TableViewBase::count_int(size_t column_ndx, int64_t target) const
{
    return aggregate<act_Count, int64_t, size_t, Column>(nullptr, column_ndx, target);
}
size_t TableViewBase::count_float(size_t column_ndx, float target) const
{
    return aggregate<act_Count, float, size_t, ColumnFloat>(nullptr, column_ndx, target);
}
size_t TableViewBase::count_double(size_t column_ndx, double target) const
{
    return aggregate<act_Count, double, size_t, ColumnDouble>(nullptr, column_ndx, target);
}

// Simple pivot aggregate method. Experimental! Please do not document method publicly.
void TableViewBase::aggregate(size_t group_by_column, size_t aggr_column, Table::AggrType op, Table& result) const
{
    m_table->aggregate(group_by_column, aggr_column, op, result, &m_row_indexes);
}

void TableViewBase::to_json(std::ostream& out) const
{
    check_cookie();

    // Represent table as list of objects
    out << "[";

    const size_t row_count = size();
    for (size_t r = 0; r < row_count; ++r) {
        const size_t real_row_index = get_source_ndx(r);
        if (real_row_index != detached_ref) {
            if (r > 0)
                out << ",";
            m_table->to_json_row(real_row_index, out);
        }
    }

    out << "]";
}

void TableViewBase::to_string(std::ostream& out, size_t limit) const
{
    check_cookie();

    // Print header (will also calculate widths)
    std::vector<size_t> widths;
    m_table->to_string_header(out, widths);

    // Set limit=-1 to print all rows, otherwise only print to limit
    const size_t row_count = num_attached_rows();
    const size_t out_count = (limit == size_t(-1)) 
        ? row_count
        : (row_count < limit) ? row_count : limit;

    // Print rows
    size_t i = 0;
    size_t count = out_count;
    while (count) {
        const size_t real_row_index = get_source_ndx(i);
        if (real_row_index != detached_ref) {
            m_table->to_string_row(real_row_index, out, widths);
            --count;
        }
        ++i;
    }

    if (out_count < row_count) {
        const size_t rest = row_count - out_count;
        out << "... and " << rest << " more rows (total " << row_count << ")";
    }
}

void TableViewBase::row_to_string(size_t row_ndx, std::ostream& out) const
{
    check_cookie();

    REALM_ASSERT(row_ndx < m_row_indexes.size());

    // Print header (will also calculate widths)
    std::vector<size_t> widths;
    m_table->to_string_header(out, widths);

    // Print row contents
    size_t real_ndx = get_source_ndx(row_ndx);
    REALM_ASSERT(real_ndx != detached_ref);
    m_table->to_string_row(real_ndx, out, widths);
}

#ifdef REALM_ENABLE_REPLICATION

uint64_t TableViewBase::outside_version() const
{
    check_cookie();

    // Return version of whatever this TableView depends on
    LinkView* lvp = dynamic_cast<LinkView*>(m_query.m_view);
    if (lvp) {
        // This TableView was created by a Query that had a LinkViewRef inside its .where() clause
        return lvp->get_origin_table().m_version;
    }

    if (m_linkview_source) {
        // m_linkview_source is set if-and-only-if this TableView was created by LinkView::get_as_sorted_view()
        return m_linkview_source->get_origin_table().m_version;
    }
    else {
        // This TableView was created by a method directly on Table, such as Table::find_all(int64_t)
        return m_table->m_version;
    }
}

bool TableViewBase::is_in_sync() const REALM_NOEXCEPT
{
    check_cookie();

    bool table = bool(m_table);
    bool version = bool(m_last_seen_version == outside_version());
    bool view = bool(m_query.m_view);

    return table && version && (view ? m_query.m_view->is_in_sync() : true);
}

uint_fast64_t TableViewBase::sync_if_needed() const
{
    if (!is_in_sync()) {
        // FIXME: Is this a reasonable handling of constness?
        const_cast<TableViewBase*>(this)->do_sync();
    }
    return m_last_seen_version;
}
#else
uint_fast64_t sync_if_needed() const { return 0; };
#endif



void TableViewBase::adj_row_acc_insert_rows(std::size_t row_ndx, std::size_t num_rows) REALM_NOEXCEPT
{
    m_row_indexes.adjust_ge(int_fast64_t(row_ndx), num_rows);
}


void TableViewBase::adj_row_acc_erase_row(std::size_t row_ndx) REALM_NOEXCEPT
{
    std::size_t it = 0;
    for (;;) {
        it = m_row_indexes.find_first(row_ndx, it);
        if (it == not_found) 
            break;
        ++m_num_detached_refs;
        m_row_indexes.set(it, -1);
    }
    m_row_indexes.adjust_ge(int_fast64_t(row_ndx)+1, -1);
}


void TableViewBase::adj_row_acc_move_over(std::size_t from_row_ndx, std::size_t to_row_ndx) REALM_NOEXCEPT
{
    std::size_t it = 0;
    // kill any refs to the target row ndx
    for (;;) {
        it = m_row_indexes.find_first(to_row_ndx, it);
        if (it == not_found) 
            break;
        ++m_num_detached_refs;
        m_row_indexes.set(it, -1);
    }
    // adjust any refs to the source row ndx to point to the target row ndx.
    it = 0;
    for (;;) {
        it = m_row_indexes.find_first(from_row_ndx, it);
        if (it == not_found)
            break;
        m_row_indexes.set(it, to_row_ndx);
    }
}





// O(n) for n = this->size()
void TableView::remove(size_t ndx)
{
    check_cookie();

    REALM_ASSERT(m_table);
    REALM_ASSERT(ndx < m_row_indexes.size());

#ifdef REALM_ENABLE_REPLICATION
    bool sync_to_keep = m_last_seen_version == outside_version();
#endif

    // Delete row in source table
    const size_t real_ndx = size_t(m_row_indexes.get(ndx));
    m_table->remove(real_ndx);

#ifdef REALM_ENABLE_REPLICATION
    // It is important to not accidentally bring us in sync, if we were
    // not in sync to start with:
    if (sync_to_keep)
        m_last_seen_version = outside_version();
#endif

    // Update refs
    m_row_indexes.erase(ndx, ndx == size() - 1);

    // Adjustment of row indexes greater than the removed index is done by 
    // adj_row_acc_move_over or adj_row_acc_erase_row as sideeffect of the actual
    // update of the table, so we don't need to do it here (it has already been done)
}


void TableView::clear()
{
    REALM_ASSERT(m_table);

#ifdef REALM_ENABLE_REPLICATION
    bool sync_to_keep = m_last_seen_version == outside_version();
#endif

    // If m_table is unordered we must use move_last_over(). Fixme/todo: To test if it's unordered we currently
    // see if we have any link or backlink columns. This is bad becuase in the future we could have unordered 
    // tables with no links - and then this test will break.
    bool is_ordered = true;
    for (size_t c = 0; c < m_table->m_spec.get_column_count(); c++) {
        ColumnType t = m_table->m_spec.get_column_type(c);
        if (t == col_type_Link || t == col_type_LinkList || t == col_type_BackLink) {
            is_ordered = false;
            break;
        }
    }

    if (is_ordered)
        m_table->batch_remove(m_row_indexes);
    else
        m_table->batch_move_last_over(m_row_indexes);

    m_row_indexes.clear();
    m_num_detached_refs = 0;

#ifdef REALM_ENABLE_REPLICATION
    // It is important to not accidentally bring us in sync, if we were
    // not in sync to start with:
    if (sync_to_keep)
        m_last_seen_version = outside_version();
#endif
}

void TableViewBase::sync_distinct_view(size_t column)
{
    m_row_indexes.clear();
    m_num_detached_refs = 0;
    m_distinct_column_source = column;
    if (m_distinct_column_source != npos) {
        REALM_ASSERT(m_table);
        REALM_ASSERT(m_table->has_search_index(m_distinct_column_source));
        if (!m_table->is_degenerate()) {
            const ColumnBase& col = m_table->get_column_base(m_distinct_column_source);
            col.get_search_index()->distinct(m_row_indexes);
        }
    }
}

#ifdef REALM_ENABLE_REPLICATION
// Sort according to one column
void TableViewBase::sort(size_t column, bool ascending)
{
    std::vector<size_t> c;
    std::vector<bool> a;
    c.push_back(column);
    a.push_back(ascending);
    sort(c, a);
}

// Sort according to multiple columns, user specified order on each column
void TableViewBase::sort(std::vector<size_t> columns, std::vector<bool> ascending)
{
    REALM_ASSERT(columns.size() == ascending.size());
    m_auto_sort = true;
    m_sorting_predicate = Sorter(columns, ascending);
    sort(m_sorting_predicate);
}

void TableViewBase::re_sort()
{
    sort(m_sorting_predicate);
}


void TableViewBase::do_sync() 
{
    // A TableView can be "born" from 4 different sources: LinkView, Table::get_distinct_view(),
    // Table::find_all() or Query. Here we sync with the respective source.
    
    if (m_linkview_source) {
        m_row_indexes.clear();
        for (size_t t = 0; t < m_linkview_source->size(); t++)
            m_row_indexes.add(m_linkview_source->get(t).get_index());
    }
    else if (m_table && m_distinct_column_source != npos) {
        sync_distinct_view(m_distinct_column_source);
    }
    // precondition: m_table is attached
    else if (!m_query.m_table) {
        // This case gets invoked if the TableView origined from Table::find_all(T value). It is temporarely disabled 
        // because it doesn't take the search parameter in count. FIXME/Todo
        REALM_ASSERT(false);
        // no valid query
        m_row_indexes.clear();
        for (size_t i = 0; i < m_table->size(); i++)
            m_row_indexes.add(i);
    }
    else  {
        // valid query, so clear earlier results and reexecute it.
        if (m_row_indexes.is_attached())
            m_row_indexes.clear();
        else
            m_row_indexes.init_from_ref(Allocator::get_default(), 
                                        Column::create(Allocator::get_default()));
        // if m_query had a TableView filter, then sync it. If it had a LinkView filter, no sync is needed
        if (m_query.m_view)
            m_query.m_view->sync_if_needed();

        // find_all needs to call size() on the tableview. But if we're
        // out of sync, size() will then call do_sync and we'll have an infinite regress
        // SO: fake that we're up to date BEFORE calling find_all.
        m_query.find_all(*(const_cast<TableViewBase*>(this)), m_start, m_end, m_limit);
    }
    if (m_auto_sort)
        re_sort();

    m_last_seen_version = outside_version();
}
#endif // REALM_ENABLE_REPLICATION
