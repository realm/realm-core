#include <tightdb/table_view.hpp>
#include <tightdb/column.hpp>
#include <tightdb/column_basic.hpp>
#include <tightdb/util/utf8.hpp>

using namespace std;
using namespace tightdb;

// Searching

// find_*_integer() methods are used for all "kinds" of integer values (bool, int, DateTime)

size_t TableViewBase::find_first_integer(size_t column_ndx, int64_t value) const
{
    for (size_t i = 0; i < m_refs.size(); i++)
        if (get_int(column_ndx, i) == value)
            return i;
    return size_t(-1);
}

size_t TableViewBase::find_first_float(size_t column_ndx, float value) const
{
    for (size_t i = 0; i < m_refs.size(); i++)
        if (get_float(column_ndx, i) == value)
            return i;
    return size_t(-1);
}

size_t TableViewBase::find_first_double(size_t column_ndx, double value) const
{
    for (size_t i = 0; i < m_refs.size(); i++)
        if (get_double(column_ndx, i) == value)
            return i;
    return size_t(-1);
}

size_t TableViewBase::find_first_string(size_t column_ndx, StringData value) const
{
    TIGHTDB_ASSERT_COLUMN_AND_TYPE(column_ndx, type_String);

    for (size_t i = 0; i < m_refs.size(); i++)
        if (get_string(column_ndx, i) == value) return i;
    return size_t(-1);
}

size_t TableViewBase::find_first_binary(size_t column_ndx, BinaryData value) const
{
    TIGHTDB_ASSERT_COLUMN_AND_TYPE(column_ndx, type_Binary);

    for (size_t i = 0; i < m_refs.size(); i++)
        if (get_binary(column_ndx, i) == value) return i;
    return size_t(-1);
}


// Aggregates ----------------------------------------------------

// count_target is ignored by all <int function> except Count. Hack because of bug in optional
// arguments in clang and vs2010 (fixed in 2012)
template <int function, typename T, typename R, class ColType>
R TableViewBase::aggregate(R (ColType::*aggregateMethod)(size_t, size_t, size_t) const, size_t column_ndx, T count_target) const
{
    TIGHTDB_ASSERT_COLUMN_AND_TYPE(column_ndx, ColumnTypeTraits<T>::id);
    TIGHTDB_ASSERT(function == act_Sum || function == act_Max || function == act_Min || function == act_Count);
    TIGHTDB_ASSERT(m_table);
    TIGHTDB_ASSERT(column_ndx < m_table->get_column_count());
    if (m_refs.size() == 0)
        return 0;

    typedef typename ColumnTypeTraits<T>::array_type ArrType;
    const ColType* column = static_cast<ColType*>(&m_table->get_column_base(column_ndx));

    if (m_refs.size() == column->size()) {
        // direct aggregate on the column
        if(function == act_Count)
            return static_cast<R>(column->count(count_target));
        else
            return (column->*aggregateMethod)(0, size_t(-1), size_t(-1)); // end == limit == -1
    }

    // Array object instantiation must NOT allocate initial memory (capacity)
    // with 'new' because it will lead to mem leak. The column keeps ownership
    // of the payload in array and will free it itself later, so we must not call destroy() on array.
    ArrType arr((Array::no_prealloc_tag()));
    size_t leaf_start = 0;
    size_t leaf_end = 0;
    size_t row_ndx;

    R res = static_cast<R>(0);

    T first = column->get(to_size_t(m_refs.get(0)));

    if(function == act_Count)
        res = static_cast<R>((first == count_target ? 1 : 0));
    else
        res = static_cast<R>(first);

    for (size_t ss = 1; ss < m_refs.size(); ++ss) {
        row_ndx = to_size_t(m_refs.get(ss));
        if (row_ndx >= leaf_end) {
            column->GetBlock(row_ndx, arr, leaf_start);
            const size_t leaf_size = arr.size();
            leaf_end = leaf_start + leaf_size;
        }

        T v = arr.get(row_ndx - leaf_start);

        if (function == act_Sum)
            res += static_cast<R>(v);
        else if (function == act_Max && v > static_cast<T>(res))
            res = static_cast<R>(v);
        else if (function == act_Min && v < static_cast<T>(res))
            res = static_cast<R>(v);
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

int64_t TableViewBase::maximum_int(size_t column_ndx) const
{
    return aggregate<act_Max, int64_t>(&Column::maximum, column_ndx, 0);
}
float TableViewBase::maximum_float(size_t column_ndx) const
{
    return aggregate<act_Max, float>(&ColumnFloat::maximum, column_ndx, 0.0);
}
double TableViewBase::maximum_double(size_t column_ndx) const
{
    return aggregate<act_Max, double>(&ColumnDouble::maximum, column_ndx, 0.0);
}
DateTime TableViewBase::maximum_datetime(size_t column_ndx) const
{
    return aggregate<act_Max, int64_t>(&Column::maximum, column_ndx, 0);
}

// Minimum

int64_t TableViewBase::minimum_int(size_t column_ndx) const
{
    return aggregate<act_Min, int64_t>(&Column::minimum, column_ndx, 0);
}
float TableViewBase::minimum_float(size_t column_ndx) const
{
    return aggregate<act_Min, float>(&ColumnFloat::minimum, column_ndx, 0.0);
}
double TableViewBase::minimum_double(size_t column_ndx) const
{
    return aggregate<act_Min, double>(&ColumnDouble::minimum, column_ndx, 0.0);
}
DateTime TableViewBase::minimum_datetime(size_t column_ndx) const
{
    return aggregate<act_Min, int64_t>(&Column::minimum, column_ndx, 0);
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

namespace tightdb {

    template <> StringData TableViewBase::GetValue<StringData>(size_t row, size_t column) const
    {
        return get_string(column, row);
    }

    template <> float TableViewBase::GetValue<float>(size_t row, size_t column) const
    {
        return get_float(column, row);
    }

    template <> double TableViewBase::GetValue<double>(size_t row, size_t column) const
    {
        return get_double(column, row);
    }
}

// Fixme, 'compare' is workaround because using utf8.hpp inside '<' operator of StringData gives circular reference
template <class T> bool compare(T v1, T v2)
{
    return v1 < v2;
}

template <> bool compare<StringData>(StringData v1, StringData v2)
{
    bool ret = utf8_compare(v1.data(), v2.data());
    return ret;
}

template <class T> struct Comparer
{
    Comparer(size_t column, bool ascend, TableViewBase& tv) : m_column(column), m_ascending(ascend), m_tv(tv) {}
    bool operator() (size_t i, size_t j) const {
        T v1 = m_tv.GetValue<T>(i, m_column);
        T v2 = m_tv.GetValue<T>(j, m_column);
        bool b = compare(v1, v2);
        return m_ascending ? b : !b;
    }

    size_t m_column;
    bool m_ascending;
    TableViewBase& m_tv;
};

// Sort m_refs with std::sort using Comparer as predicate
template <class T> void TableViewBase::sort(size_t column, bool ascending)
{
    vector<size_t> v, v2;
    for (size_t t = 0; t < size(); t++) {
        v.push_back(t);
        v2.push_back(get_source_ndx(t));
    }
    std::stable_sort(v.begin(), v.end(), Comparer<T>(column, ascending, *this));
    m_refs.clear();
    for (size_t t = 0; t < v.size(); t++)
        m_refs.add(v2[v[t]]);
}

void TableViewBase::sort(size_t column, bool Ascending)
{
    TIGHTDB_ASSERT(m_table);

    m_auto_sort = true;
    m_ascending = Ascending;
    m_sort_index = column;

    DataType type = m_table->get_column_type(column);

    TIGHTDB_ASSERT(type == type_Int  ||
                   type == type_DateTime ||
                   type == type_Bool ||
                   type == type_Float ||
                   type == type_Double ||
                   type == type_String);

    if (m_refs.size() == 0)
        return;
    
    Array result;

    if (type == type_Float) {
        sort<float>(column, Ascending);
    }
    else if (type == type_Double) {
        sort<double>(column, Ascending);
    }
    else if (type == type_String) {
        sort<tightdb::StringData>(column, Ascending);
    }
    else {

        Array vals;
        Array ref;

        //ref.Preset(0, m_refs.size() - 1, m_refs.size());
        for (size_t t = 0; t < m_refs.size(); t++)
            ref.add(t);

        // Extract all values from the Column and put them in an Array because Array is much faster to operate on
        // with rand access (we have ~log(n) accesses to each element, so using 1 additional read to speed up the rest is faster)
        if (type == type_Int) {
            for (size_t t = 0; t < m_refs.size(); t++) {
                int64_t v = m_table->get_int(column, size_t(m_refs.get(t)));
                vals.add(v);
            }
        }
        else if (type == type_DateTime) {
            for (size_t t = 0; t < m_refs.size(); t++) {
                size_t idx = size_t(m_refs.get(t));
                int64_t v = int64_t(m_table->get_datetime(column, idx).get_datetime());
                vals.add(v);
            }
        }
        else if (type == type_Bool) {
            for (size_t t = 0; t < m_refs.size(); t++) {
                size_t idx = size_t(m_refs.get(t));
                int64_t v = int64_t(m_table->get_bool(column, idx));
                vals.add(v);
            }
        }

        vals.ReferenceSort(ref);
        vals.destroy();

        for (size_t t = 0; t < m_refs.size(); t++) {
            size_t r = to_size_t(ref.get(t));
            size_t rr = to_size_t(m_refs.get(r));
            result.add(rr);
        }

        // Copy result to m_refs (todo, there might be a shortcut)
        m_refs.clear();
        if (Ascending) {
            for (size_t t = 0; t < ref.size(); t++) {
                size_t v = to_size_t(result.get(t));
                m_refs.add(v);
            }
        }
        else {
            for (size_t t = 0; t < ref.size(); t++) {
                size_t v = to_size_t(result.get(ref.size() - t - 1));
                m_refs.add(v);
            }
        }
        ref.destroy();
    }
    result.destroy();
}

// Simple pivot aggregate method. Experimental! Please do not document method publicly.
void TableViewBase::aggregate(size_t group_by_column, size_t aggr_column, Table::AggrType op, Table& result) const
{
    m_table->aggregate(group_by_column, aggr_column, op, result, &m_refs);
}




void TableViewBase::to_json(ostream& out) const
{
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
    TIGHTDB_ASSERT(row_ndx < m_refs.size());

    // Print header (will also calculate widths)
    vector<size_t> widths;
    m_table->to_string_header(out, widths);

    // Print row contents
    m_table->to_string_row(get_source_ndx(row_ndx), out, widths);
}

// O(n) for n = this->size()
void TableView::remove(size_t ndx)
{
    TIGHTDB_ASSERT(m_table);
    TIGHTDB_ASSERT(ndx < m_refs.size());
    bool sync_to_keep = m_last_seen_version == m_table->m_version;

    // Delete row in source table
    const size_t real_ndx = size_t(m_refs.get(ndx));
    m_table->from_view_remove(real_ndx, this);
    // It is important to not accidentally bring us in sync, if we were
    // not in sync to start with:
    if (sync_to_keep)
        m_last_seen_version = m_table->m_version;
    // Update refs
    m_refs.erase(ndx, ndx == size() - 1);

    // Decrement row indexes greater than or equal to ndx
    //
    // O(n) for n = this->size(). FIXME: Dangerous cast below: unsigned -> signed
    m_refs.adjust_ge(int_fast64_t(real_ndx), -1);

}


void TableView::clear()
{
    TIGHTDB_ASSERT(m_table);
    // sort m_refs
    vector<size_t> v;
    for (size_t t = 0; t < size(); t++)
        v.push_back(to_size_t(m_refs.get(t)));
    std::sort(v.begin(), v.end());
    bool sync_to_keep = m_last_seen_version == m_table->m_version;

    // Delete all referenced rows in source table
    // (in reverse order to avoid index drift)
    for (size_t i = m_refs.size(); i != 0; --i) {
        size_t ndx = size_t(v[i-1]);
        m_table->from_view_remove(ndx, this);
    }

    m_refs.clear();
    // It is important to not accidentally bring us in sync, if we were
    // not in sync to start with:
    if (sync_to_keep)
        m_last_seen_version = m_table->m_version;
}

#ifdef TIGHTDB_ENABLE_REPLICATION

void TableViewBase::do_sync() 
{
    // precondition: m_table is attached
    if (!m_query.m_table) {
        // no valid query
        m_last_seen_version = m_table->m_version;
        m_refs.clear();
        for (size_t i=0; i<m_table->size(); i++)
            m_refs.add(i);
        cerr << "ref to entire table" << endl;
    }
    else  {
        // valid query, so clear earlier results and reexecute it.
        m_refs.clear();
        // now we are going to do something naughty!
        if (m_query.m_tableview)
            m_query.m_tableview->sync_if_needed();
        // and more:
        // find_all needs to call size() on the tableview. But if we're
        // out of sync, size() will then call do_sync and we'll have an infinite regress
        // SO: fake that we're up to date BEFORE calling find_all.
        m_last_seen_version = m_table->m_version;
        m_query.find_all(*(const_cast<TableViewBase*>(this)), m_start, m_end, m_limit);
    }
    if (m_auto_sort)
        sort(m_sort_index, m_ascending);
}
#endif // TIGHTDB_ENABLE_REPLICATION
