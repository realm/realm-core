#include <tightdb/table_view.hpp>
#include <tightdb/column.hpp>
#include <assert.h>

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


template <int function>int64_t TableViewBase::aggregate(size_t column_ndx) const
{
    TIGHTDB_ASSERT_COLUMN_AND_TYPE(column_ndx, COLUMN_TYPE_INT);
    TIGHTDB_ASSERT(function == TDB_SUM || function == TDB_MAX || function == TDB_MIN);
    TIGHTDB_ASSERT(m_table);
    TIGHTDB_ASSERT(column_ndx < m_table->get_column_count());
    if (m_refs.Size() == 0) 
        return 0;

    int64_t res = 0;
    Column& m_column = m_table->GetColumn(column_ndx);

    if (m_refs.Size() == m_column.Size()) {
        if (function == TDB_MAX)
            return m_column.maximum();
        if (function == TDB_MIN)
            return m_column.minimum();
        if (function == TDB_SUM)
            return m_column.sum();
    }

    Array m_array;
    size_t m_leaf_start = 0;
    size_t m_leaf_end = 0;
    size_t s;

    res = get_int(column_ndx, 0);

    for (size_t ss = 1; ss < m_refs.Size(); ++ss) {
        s = m_refs.GetAsRef(ss);
        if (s >= m_leaf_end) {
            m_column.GetBlock(s, m_array, m_leaf_start);
            const size_t leaf_size = m_array.Size();
            m_leaf_end = m_leaf_start + leaf_size;
        }    

        int64_t v = m_array.Get(s - m_leaf_start);

        if (function == TDB_SUM)
            res += v;
        else if (function == TDB_MAX ? v > res : v < res)
            res = v;
    }
    
    return res;
}

int64_t TableViewBase::sum(size_t column_ndx) const
{
    return aggregate<TDB_SUM>(column_ndx);
}

int64_t TableViewBase::maximum(size_t column_ndx) const
{
    return aggregate<TDB_MAX>(column_ndx);
}

int64_t TableViewBase::minimum(size_t column_ndx) const
{
    return aggregate<TDB_MIN>(column_ndx);
}

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
