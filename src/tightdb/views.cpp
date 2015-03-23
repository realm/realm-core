#include <tightdb/views.hpp>

using namespace tightdb;

// Sort according to one column
void RowIndexes::sort(size_t column, bool ascending)
{
    std::vector<size_t> c;
    std::vector<bool> a;
    c.push_back(column);
    a.push_back(ascending);
    sort(c, a);
}

// Sort according to multiple columns, user specified order on each column
void RowIndexes::sort(std::vector<size_t> columns, std::vector<bool> ascending)
{
    REALM_ASSERT(columns.size() == ascending.size());
    m_auto_sort = true;
    m_sorting_predicate = Sorter(columns, ascending);
    re_sort();
}

// Re-sort view according to last used criterias
void RowIndexes::re_sort()
{
    size_t sz = size();
    if (sz == 0)
        return;

    std::vector<size_t> v;
    v.reserve(sz);
    for (size_t t = 0; t < sz; t++)
        v.push_back(m_row_indexes.get(t));

    m_sorting_predicate.init(this);
    std::stable_sort(v.begin(), v.end(), m_sorting_predicate);

    m_row_indexes.clear();
    for (size_t t = 0; t < sz; t++)
        m_row_indexes.add(v[t]);
}
