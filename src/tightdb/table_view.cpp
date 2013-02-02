#include <tightdb/table_view.hpp>
#include <tightdb/column.hpp>
#include <tightdb/column_basic.hpp>

namespace tightdb {

// Searching

// find_*_integer() methods are used for all "kinds" of integer values (bool, int, Date)

size_t TableViewBase::find_first_integer(size_t column_ndx, int64_t value) const
{
    for (size_t i = 0; i < m_refs.Size(); i++)
        if (get_int(column_ndx, i) == value)
            return i;
    return size_t(-1);
}

size_t TableViewBase::find_first_string(size_t column_ndx, const char* value) const
{
    TIGHTDB_ASSERT_COLUMN_AND_TYPE(column_ndx, COLUMN_TYPE_STRING);

    for (size_t i = 0; i < m_refs.Size(); i++)
        if (strcmp(get_string(column_ndx, i), value) == 0)
            return i;
    return size_t(-1);
}

size_t TableViewBase::find_first_float(size_t column_ndx, float value) const
{
    for (size_t i = 0; i < m_refs.Size(); i++)
        if (get_float(column_ndx, i) == value)
            return i;
    return size_t(-1);
}

size_t TableViewBase::find_first_double(size_t column_ndx, double value) const
{
    for (size_t i = 0; i < m_refs.Size(); i++)
        if (get_double(column_ndx, i) == value)
            return i;
    return size_t(-1);
}


// Aggregates ----------------------------------------------------


template <int function, typename T, typename R, class ColType> 
R TableViewBase::aggregate(R (ColType::*aggregateMethod)(size_t, size_t) const, size_t column_ndx) const
{
    TIGHTDB_ASSERT_COLUMN_AND_TYPE(column_ndx, ColumnTypeTraits<T>::id);
    TIGHTDB_ASSERT(function == TDB_SUM || function == TDB_MAX || function == TDB_MIN);
    TIGHTDB_ASSERT(m_table);
    TIGHTDB_ASSERT(column_ndx < m_table->get_column_count());
    if (m_refs.Size() == 0) 
        return 0;

    typedef typename ColumnTypeTraits<T>::array_type ArrType;
    const ColType* column = (ColType*)&m_table->GetColumnBase(column_ndx);

    if (m_refs.Size() == column->Size()) {
        // direct aggregate on the column
        return (column->*aggregateMethod)(0, size_t(-1));
    }

    // Array object instantiation must NOT allocate initial memory (capacity) 
    // with 'new' because it will lead to mem leak. The column keeps ownership 
    // of the payload in array and will free it itself later, so we must not call Destroy() on array.
    ArrType arr((Array::no_prealloc_tag()));
    size_t leaf_start = 0;
    size_t leaf_end = 0;
    size_t row_ndx;

    R res = static_cast<R>(column->template TreeGet<T,ColType>(0));

    for (size_t ss = 1; ss < m_refs.Size(); ++ss) {
        row_ndx = m_refs.GetAsSizeT(ss);
        if (row_ndx >= leaf_end) {
            ((Column*)column)->GetBlock(row_ndx, arr, leaf_start);
            const size_t leaf_size = arr.Size();
            leaf_end = leaf_start + leaf_size;
        }    

        T v = arr.Get(row_ndx - leaf_start);

        if (function == TDB_SUM)
            res += v;
        else if (function == TDB_MAX ? v > res : v < res)
            res = v;
    }

    return res;
}

// sum

int64_t TableViewBase::sum(size_t column_ndx) const
{
    return aggregate<TDB_SUM, int64_t>(&Column::sum, column_ndx);
}
double TableViewBase::sum_float(size_t column_ndx) const
{
    return aggregate<TDB_SUM, float>(&ColumnFloat::sum, column_ndx);
}
double TableViewBase::sum_double(size_t column_ndx) const
{
    return aggregate<TDB_SUM, double>(&ColumnDouble::sum, column_ndx);
}

// Maximum

int64_t TableViewBase::maximum(size_t column_ndx) const
{
    return aggregate<TDB_MAX, int64_t>(&Column::maximum, column_ndx);
}
float TableViewBase::maximum_float(size_t column_ndx) const
{
    return aggregate<TDB_MAX, float>(&ColumnFloat::maximum, column_ndx);
}
double TableViewBase::maximum_double(size_t column_ndx) const
{
    return aggregate<TDB_MAX, double>(&ColumnDouble::maximum, column_ndx);
}

// Minimum

int64_t TableViewBase::minimum(size_t column_ndx) const
{
    return aggregate<TDB_MIN, int64_t>(&Column::minimum, column_ndx);
}
float TableViewBase::minimum_float(size_t column_ndx) const
{
    return aggregate<TDB_MIN, float>(&ColumnFloat::minimum, column_ndx);
}
double TableViewBase::minimum_double(size_t column_ndx) const
{
    return aggregate<TDB_MIN, double>(&ColumnDouble::minimum, column_ndx);
}

//

void TableViewBase::sort(size_t column, bool Ascending)
{
    TIGHTDB_ASSERT(m_table);
    TIGHTDB_ASSERT(m_table->get_column_type(column) == COLUMN_TYPE_INT  ||
                   m_table->get_column_type(column) == COLUMN_TYPE_DATE ||
                   m_table->get_column_type(column) == COLUMN_TYPE_BOOL);

    if (m_refs.Size() == 0)
        return;

    Array vals;
    Array ref;
    Array result;

    //ref.Preset(0, m_refs.Size() - 1, m_refs.Size());
    for (size_t t = 0; t < m_refs.Size(); t++)
        ref.add(t);

    // Extract all values from the Column and put them in an Array because Array is much faster to operate on
    // with rand access (we have ~log(n) accesses to each element, so using 1 additional read to speed up the rest is faster)
    if (m_table->get_column_type(column) == COLUMN_TYPE_INT) {
        for (size_t t = 0; t < m_refs.Size(); t++) {
            const int64_t v = m_table->get_int(column, size_t(m_refs.Get(t)));
            vals.add(v);
        }
    }
    else if (m_table->get_column_type(column) == COLUMN_TYPE_DATE) {
        for (size_t t = 0; t < m_refs.Size(); t++) {
            const size_t idx = size_t(m_refs.Get(t));
            const int64_t v = int64_t(m_table->get_date(column, idx));
            vals.add(v);
        }
    }
    else if (m_table->get_column_type(column) == COLUMN_TYPE_BOOL) {
        for (size_t t = 0; t < m_refs.Size(); t++) {
            const size_t idx = size_t(m_refs.Get(t));
            const int64_t v = int64_t(m_table->get_bool(column, idx));
            vals.add(v);
        }
    }

    vals.ReferenceSort(ref);
    vals.Destroy();

    for (size_t t = 0; t < m_refs.Size(); t++) {
        const size_t r  = (size_t)ref.Get(t);
        const size_t rr = (size_t)m_refs.Get(r);
        result.add(rr);
    }

    ref.Destroy();

    // Copy result to m_refs (todo, there might be a shortcut)
    m_refs.Clear();
    if (Ascending) {
        for (size_t t = 0; t < ref.Size(); t++) {
            const size_t v = (size_t)result.Get(t);
            m_refs.add(v);
        }
    }
    else {
        for (size_t t = 0; t < ref.Size(); t++) {
            const size_t v = (size_t)result.Get(ref.Size() - t - 1);
            m_refs.add(v);
        }
    }
    result.Destroy();
}

void TableViewBase::to_json(std::ostream& out)
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

void TableViewBase::to_string(std::ostream& out, size_t limit) const
{
    // Print header (will also calculate widths)
    std::vector<size_t> widths;
    m_table->to_string_header(out, widths);

    // Set limit=-1 to print all rows, otherwise only print to limit
    const size_t row_count = size();
    const size_t out_count = (limit == (size_t)-1) ? row_count
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

void TableView::remove(size_t ndx)
{
    TIGHTDB_ASSERT(m_table);
    TIGHTDB_ASSERT(ndx < m_refs.Size());

    // Delete row in source table
    const size_t real_ndx = size_t(m_refs.Get(ndx));
    m_table->remove(real_ndx);

    // Update refs
    m_refs.Delete(ndx);
    m_refs.IncrementIf(ndx, -1);
}


void TableView::clear()
{
    TIGHTDB_ASSERT(m_table);
    m_refs.sort();

    // Delete all referenced rows in source table
    // (in reverse order to avoid index drift)
    const size_t count = m_refs.Size();
    for (size_t i = count; i; --i) {
        const size_t ndx = size_t(m_refs.Get(i-1));
        m_table->remove(ndx);
    }

    m_refs.Clear();
}


} // namespace tightdb
