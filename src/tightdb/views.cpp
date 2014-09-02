
#include <tightdb/views.hpp>

using namespace tightdb;

// Sort according to one column
void RowIndexes::sort(size_t column, bool ascending)
{
    std::vector<size_t> v;
    v.push_back(column);
    sort(v, ascending);
}

// Sort according to multiple columns
void RowIndexes::sort(std::vector<size_t> columns, bool ascending)
{
    m_auto_sort = true;
    m_sorting_predicate = Sorter(columns, ascending);
    re_sort();
}

// Re-sort view according to last used criterias
void RowIndexes::re_sort()
{
    std::vector<size_t> v;
    for (size_t t = 0; t < size(); t++)
        v.push_back(m_row_indexes.get(t));
    m_sorting_predicate.m_row_indexes_class = this;
    std::stable_sort(v.begin(), v.end(), m_sorting_predicate);
    m_row_indexes.clear();
    for (size_t t = 0; t < v.size(); t++)
        m_row_indexes.add(v[t]);
}
