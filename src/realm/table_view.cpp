#include <realm/table_view.hpp>


#include <realm/column.hpp>
#include <realm/query_conditions.hpp>
#include <realm/column_basic.hpp>
#include <realm/util/utf8.hpp>
#include <realm/index_string.hpp>

using namespace std;
using namespace realm;

// Searching

// find_*_integer() methods are used for all "kinds" of integer values (bool, int, DateTime)

size_t TableViewBase::find_first_integer(size_t column_ndx, int64_t value) const
{
    check_cookie();

    for (size_t i = 0; i < m_row_indexes.size(); i++)
        if (get_int(column_ndx, i) == value)
            return i;
    return size_t(-1);
}

size_t TableViewBase::find_first_float(size_t column_ndx, float value) const
{
    check_cookie();

    for (size_t i = 0; i < m_row_indexes.size(); i++)
        if (get_float(column_ndx, i) == value)
            return i;
    return size_t(-1);
}

size_t TableViewBase::find_first_double(size_t column_ndx, double value) const
{
    check_cookie();

    for (size_t i = 0; i < m_row_indexes.size(); i++)
        if (get_double(column_ndx, i) == value)
            return i;
    return size_t(-1);
}

size_t TableViewBase::find_first_string(size_t column_ndx, StringData value) const
{
    check_cookie();

    REALM_ASSERT_COLUMN_AND_TYPE(column_ndx, type_String);

    for (size_t i = 0; i < m_row_indexes.size(); i++)
        if (get_string(column_ndx, i) == value) return i;
    return size_t(-1);
}

size_t TableViewBase::find_first_binary(size_t column_ndx, BinaryData value) const
{
    check_cookie();

    REALM_ASSERT_COLUMN_AND_TYPE(column_ndx, type_Binary);

    for (size_t i = 0; i < m_row_indexes.size(); i++)
        if (get_binary(column_ndx, i) == value) return i;
    return size_t(-1);
}


// Aggregates ----------------------------------------------------

// count_target is ignored by all <int function> except Count. Hack because of bug in optional
// arguments in clang and vs2010 (fixed in 2012)
template <int function, typename T, typename R, class ColType>
R TableViewBase::aggregate(R(ColType::*aggregateMethod)(size_t, size_t, size_t, size_t*) const, size_t column_ndx, T count_target, size_t* return_ndx) const
{
    check_cookie();

    REALM_ASSERT_COLUMN_AND_TYPE(column_ndx, ColumnTypeTraits<T>::id);
    REALM_ASSERT(function == act_Sum || function == act_Max || function == act_Min || function == act_Count);
    REALM_ASSERT(m_table);
    REALM_ASSERT(column_ndx < m_table->get_column_count());
    if (m_row_indexes.size() == 0)
        return 0;

    typedef typename ColumnTypeTraits<T>::array_type ArrType;
    const ColType* column = static_cast<ColType*>(&m_table->get_column_base(column_ndx));

    if (m_row_indexes.size() == column->size()) {
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
    return aggregate<act_Sum, int64_t>(&Column::sum, column_ndx, 0) / static_cast<double>(size());
}
double TableViewBase::average_float(size_t column_ndx) const
{
    return aggregate<act_Sum, float>(&ColumnFloat::sum, column_ndx, 0.0) / static_cast<double>(size());
}
double TableViewBase::average_double(size_t column_ndx) const
{
    return aggregate<act_Sum, double>(&ColumnDouble::sum, column_ndx, 0.0) / static_cast<double>(size());
}

// Count
size_t TableViewBase::count_int(size_t column_ndx, int64_t target) const
{
    return aggregate<act_Count, int64_t, size_t, Column>(NULL, column_ndx, target);
}
size_t TableViewBase::count_float(size_t column_ndx, float target) const
{
    return aggregate<act_Count, float, size_t, ColumnFloat>(NULL, column_ndx, target);
}
size_t TableViewBase::count_double(size_t column_ndx, double target) const
{
    return aggregate<act_Count, double, size_t, ColumnDouble>(NULL, column_ndx, target);
}

// Simple pivot aggregate method. Experimental! Please do not document method publicly.
void TableViewBase::aggregate(size_t group_by_column, size_t aggr_column, Table::AggrType op, Table& result) const
{
    m_table->aggregate(group_by_column, aggr_column, op, result, &m_row_indexes);
}

void TableViewBase::to_json(ostream& out) const
{
    check_cookie();

    // Represent table as list of objects
    out << "[";

    const size_t row_count = size();
    for (size_t r = 0; r < row_count; ++r) {
        if (r > 0)
            out << ",";
        const size_t real_row_index = get_source_ndx(r);
        m_table->to_json_row(real_row_index, out);
    }

    out << "]";
}

void TableViewBase::to_string(ostream& out, size_t limit) const
{
    check_cookie();

    // Print header (will also calculate widths)
    vector<size_t> widths;
    m_table->to_string_header(out, widths);

    // Set limit=-1 to print all rows, otherwise only print to limit
    const size_t row_count = size();
    const size_t out_count = (limit == size_t(-1)) ? row_count
                                                   : (row_count < limit) ? row_count : limit;

    // Print rows
    for (size_t i = 0; i < out_count; ++i) {
        const size_t real_row_index = get_source_ndx(i);
        m_table->to_string_row(real_row_index, out, widths);
    }

    if (out_count < row_count) {
        const size_t rest = row_count - out_count;
        out << "... and " << rest << " more rows (total " << row_count << ")";
    }
}

void TableViewBase::row_to_string(size_t row_ndx, ostream& out) const
{
    check_cookie();

    REALM_ASSERT(row_ndx < m_row_indexes.size());

    // Print header (will also calculate widths)
    vector<size_t> widths;
    m_table->to_string_header(out, widths);

    // Print row contents
    m_table->to_string_row(get_source_ndx(row_ndx), out, widths);
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

    // Decrement row indexes greater than or equal to ndx
    //
    // O(n) for n = this->size(). FIXME: Dangerous cast below: unsigned -> signed
    m_row_indexes.adjust_ge(int_fast64_t(real_ndx), -1);
}


void TableView::clear()
{
    REALM_ASSERT(m_table);

#ifdef REALM_ENABLE_REPLICATION
    bool sync_to_keep = m_last_seen_version == outside_version();
#endif

    // If m_table is unordered we must use move_last_over(). Fixme/todo: To test if it's unordered we currently
    // see if we have any link or backlink columns. This is bad becuase in the future we could have unordered tables
    // with no links
    bool is_ordered = true;
    for (size_t c = 0; c < m_table->m_spec.get_column_count(); c++) {
        ColumnType t = m_table->m_spec.get_column_type(c);
        if (t == col_type_Link || t == col_type_LinkList || t == col_type_BackLink) {
            is_ordered = false;
            break;
        }
    }

    // Test if tableview is sorted ascendingly
    bool is_sorted = true;
    for (size_t t = 1; t < size(); t++) {
        if (m_row_indexes.get(t) < m_row_indexes.get(t - 1)) {
            is_sorted = false;
            break;
        }
    }

    if (is_sorted) {
        // Delete all referenced rows in source table
        // (in reverse order to avoid index drift)
        for (size_t i = m_row_indexes.size(); i != 0; --i) {
            size_t ndx = size_t(m_row_indexes.get(i - 1));

            // If table is unordered, we must use move_last_over()
            if (is_ordered)
                m_table->remove(ndx);
            else
                m_table->move_last_over(ndx);
        }
    }
    else {
        // sort tableview
        vector<size_t> v;
        for (size_t t = 0; t < size(); t++)
            v.push_back(to_size_t(m_row_indexes.get(t)));
        std::sort(v.begin(), v.end());

        for (size_t i = m_row_indexes.size(); i != 0; --i) {
            size_t ndx = size_t(v[i - 1]);

            // If table is unordered, we must use move_last_over()
            if (is_ordered)
                m_table->remove(ndx);
            else
                m_table->move_last_over(ndx);
        }
    }

    m_row_indexes.clear();

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
        m_row_indexes.clear();
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
