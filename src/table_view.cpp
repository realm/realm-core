#include "table.hpp"
#include "column.hpp"
#include <assert.h>

namespace tightdb {

TableView::TableView(Table& source): m_table(source) {}

TableView::TableView(const TableView& v): m_table(v.m_table), m_refs(v.m_refs) {}

TableView::~TableView()
{
    m_refs.Destroy();
}

Table* TableView::get_subtable()
{
    return &m_table;
}

// Searching
size_t TableView::Find(size_t column_ndx, int64_t value) const
{
    assert(column_ndx < m_table.get_column_count());
    assert(m_table.get_column_type(column_ndx) == COLUMN_TYPE_INT);

    for(size_t i = 0; i < m_refs.Size(); i++)
        if(get_int(column_ndx, i) == value)
            return i;

    return (size_t)-1;
}

void TableView::FindAll(TableView& tv, size_t column_ndx, int64_t value)
{
    assert(column_ndx < m_table.get_column_count());
    assert(m_table.get_column_type(column_ndx) == COLUMN_TYPE_INT);

    for(size_t i = 0; i < m_refs.Size(); i++)
        if(get_int(column_ndx, i) == value)
            tv.GetRefColumn().add(i);
}

size_t TableView::FindString(size_t column_ndx, const char* value) const
{
    assert(column_ndx < m_table.get_column_count());
    assert(m_table.get_column_type(column_ndx) == COLUMN_TYPE_STRING);

    for(size_t i = 0; i < m_refs.Size(); i++)
    if(strcmp(get_string(column_ndx, i), value) == 0)
        return i;

    return (size_t)-1;
}


void TableView::FindAllString(TableView& tv, size_t column_ndx, const char* value)
{
    assert(column_ndx < m_table.get_column_count());
    assert(m_table.get_column_type(column_ndx) == COLUMN_TYPE_STRING);

    for(size_t i = 0; i < m_refs.Size(); i++)
    if(strcmp(get_string(column_ndx, i), value) == 0)
        tv.GetRefColumn().add(i);
}

int64_t TableView::sum(size_t column_ndx) const
{
    assert(column_ndx < m_table.get_column_count());
    assert(m_table.get_column_type(column_ndx) == COLUMN_TYPE_INT);
    int64_t sum = 0;

    for(size_t i = 0; i < m_refs.Size(); i++)
        sum += get_int(column_ndx, i);

    return sum;
}

int64_t TableView::maximum(size_t column_ndx) const
{
    if (is_empty()) return 0;
    if (m_refs.Size() == 0) return 0;

    int64_t mv = get_int(column_ndx, 0);
    for (size_t i = 1; i < m_refs.Size(); ++i) {
        const int64_t v = get_int(column_ndx, i);
        if (v > mv) {
            mv = v;
        }
    }
    return mv;
}

int64_t TableView::minimum(size_t column_ndx) const
{
    if (is_empty()) return 0;
    if (m_refs.Size() == 0) return 0;

    int64_t mv = get_int(column_ndx, 0);
    for (size_t i = 1; i < m_refs.Size(); ++i) {
        const int64_t v = get_int(column_ndx, i);
        if (v < mv) {
            mv = v;
        }
    }
    return mv;
}

int64_t TableView::get_int(size_t column_ndx, size_t ndx) const
{
    assert(column_ndx < m_table.get_column_count());
    assert(m_table.get_column_type(column_ndx) == COLUMN_TYPE_INT);
    assert(ndx < m_refs.Size());

    const size_t real_ndx = m_refs.GetAsRef(ndx);
    return m_table.get_int(column_ndx, real_ndx);
}

bool TableView::get_bool(size_t column_ndx, size_t ndx) const
{
    assert(column_ndx < m_table.get_column_count());
    assert(m_table.get_column_type(column_ndx) == COLUMN_TYPE_BOOL);
    assert(ndx < m_refs.Size());

    const size_t real_ndx = m_refs.GetAsRef(ndx);
    return m_table.get_bool(column_ndx, real_ndx);
}

time_t TableView::get_date(size_t column_ndx, size_t ndx) const
{
    assert(column_ndx < m_table.get_column_count());
    assert(m_table.get_column_type(column_ndx) == COLUMN_TYPE_DATE);
    assert(ndx < m_refs.Size());

    const size_t real_ndx = m_refs.GetAsRef(ndx);
    return m_table.get_date(column_ndx, real_ndx);
}

const char* TableView::get_string(size_t column_ndx, size_t ndx) const
{
    assert(column_ndx < m_table.get_column_count());
    assert(m_table.get_column_type(column_ndx) == COLUMN_TYPE_STRING);
    assert(ndx < m_refs.Size());

    const size_t real_ndx = m_refs.GetAsRef(ndx);
    return m_table.get_string(column_ndx, real_ndx);
}

BinaryData TableView::get_binary(std::size_t column_ndx, std::size_t ndx) const
{
    assert(ndx < m_refs.Size());
    const size_t real_ndx = m_refs.GetAsRef(ndx);
    return m_table.get_binary(column_ndx, real_ndx);
}

Mixed TableView::get_mixed(std::size_t column_ndx, std::size_t ndx) const
{
    assert(ndx < m_refs.Size());
    const size_t real_ndx = m_refs.GetAsRef(ndx);
    return m_table.get_mixed(column_ndx, real_ndx);
}

TableRef TableView::get_subtable(size_t column_ndx, size_t ndx)
{
    assert(column_ndx < m_table.get_column_count());
    assert(m_table.get_column_type(column_ndx) == COLUMN_TYPE_TABLE);
    assert(ndx < m_refs.Size());

    const size_t real_ndx = m_refs.GetAsRef(ndx);
    return m_table.get_subtable(column_ndx, real_ndx);
}

void TableView::set_int(size_t column_ndx, size_t ndx, int64_t value)
{
    assert(column_ndx < m_table.get_column_count());
    assert(m_table.get_column_type(column_ndx) == COLUMN_TYPE_INT);
    assert(ndx < m_refs.Size());

    const size_t real_ndx = m_refs.GetAsRef(ndx);
    m_table.set_int(column_ndx, real_ndx, value);
}

void TableView::set_bool(size_t column_ndx, size_t ndx, bool value)
{
    assert(column_ndx < m_table.get_column_count());
    assert(m_table.get_column_type(column_ndx) == COLUMN_TYPE_BOOL);
    assert(ndx < m_refs.Size());

    const size_t real_ndx = m_refs.GetAsRef(ndx);
    m_table.set_bool(column_ndx, real_ndx, value);
}

void TableView::set_date(size_t column_ndx, size_t ndx, time_t value)
{
    assert(column_ndx < m_table.get_column_count());
    assert(m_table.get_column_type(column_ndx) == COLUMN_TYPE_DATE);
    assert(ndx < m_refs.Size());

    const size_t real_ndx = m_refs.GetAsRef(ndx);
    m_table.set_date(column_ndx, real_ndx, value);
}

void TableView::set_string(size_t column_ndx, size_t ndx, const char* value)
{
    assert(column_ndx < m_table.get_column_count());
    assert(m_table.get_column_type(column_ndx) == COLUMN_TYPE_STRING);
    assert(ndx < m_refs.Size());

    const size_t real_ndx = m_refs.GetAsRef(ndx);
    m_table.set_string(column_ndx, real_ndx, value);
}

void TableView::set_binary(std::size_t column_ndx, std::size_t ndx, const char* value, std::size_t len)
{
    assert(ndx < m_refs.Size());
    const size_t real_ndx = m_refs.GetAsRef(ndx);
    m_table.set_binary(column_ndx, real_ndx, value, len);
}

void TableView::set_mixed(std::size_t column_ndx, std::size_t ndx, Mixed value)
{
    assert(ndx < m_refs.Size());
    const size_t real_ndx = m_refs.GetAsRef(ndx);
    m_table.set_mixed(column_ndx, real_ndx, value);
}


void TableView::Sort(size_t column, bool Ascending)
{
    assert(m_table.get_column_type(column) == COLUMN_TYPE_INT || m_table.get_column_type(column) == COLUMN_TYPE_DATE || m_table.get_column_type(column) == COLUMN_TYPE_BOOL);

    if(m_refs.Size() == 0)
        return;

    Array vals;
    Array ref;
    Array result;

    //ref.Preset(0, m_refs.Size() - 1, m_refs.Size());
    for(size_t t = 0; t < m_refs.Size(); t++)
        ref.add(t);

    // Extract all values from the Column and put them in an Array because Array is much faster to operate on
    // with rand access (we have ~log(n) accesses to each element, so using 1 additional read to speed up the rest is faster)
    if(m_table.get_column_type(column) == COLUMN_TYPE_INT) {
        for(size_t t = 0; t < m_refs.Size(); t++) {
            int64_t v = m_table.get_int(column, m_refs.GetAsRef(t));
            vals.add(v);
        }
    }
    else if(m_table.get_column_type(column) == COLUMN_TYPE_DATE) {
        for(size_t t = 0; t < m_refs.Size(); t++) {
            size_t idx = m_refs.GetAsRef(t);
            int64_t v = (int64_t)m_table.get_date(column, idx);
            vals.add(v);
        }
    }
    else if(m_table.get_column_type(column) == COLUMN_TYPE_BOOL) {
        for(size_t t = 0; t < m_refs.Size(); t++) {
            size_t idx = m_refs.GetAsRef(t);
            int64_t v = (int64_t)m_table.get_bool(column, idx);
            vals.add(v);
        }
    }

    vals.ReferenceSort(ref);
    vals.Destroy();

    for(size_t t = 0; t < m_refs.Size(); t++) {
        size_t r = ref.GetAsRef(t);
        size_t rr = m_refs.GetAsRef(r);
        result.add(rr);
    }

    ref.Destroy();

    // Copy result to m_refs (todo, there might be a shortcut)
    m_refs.Clear();
    if(Ascending) {
        for(size_t t = 0; t < ref.Size(); t++) {
            size_t v = result.GetAsRef(t);
            m_refs.add(v);
        }
    }
    else {
        for(size_t t = 0; t < ref.Size(); t++) {
            size_t v = result.GetAsRef(ref.Size() - t - 1);
                m_refs.add(v);
        }
    }
    result.Destroy();
}

void TableView::remove(size_t ndx)
{
    assert(ndx < m_refs.Size());

    // Delete row in source table
    const size_t real_ndx = m_refs.GetAsRef(ndx);
    m_table.remove(real_ndx);

    // Update refs
    m_refs.Delete(ndx);
    m_refs.IncrementIf(ndx, -1);
}

void TableView::clear()
{
    m_refs.Sort();

    // Delete all referenced rows in source table
    // (in reverse order to avoid index drift)
    const size_t count = m_refs.Size();
    for (size_t i = count; i; --i) {
        const size_t ndx = m_refs.GetAsRef(i-1);
        m_table.remove(ndx);
    }

    m_refs.Clear();
}

}
