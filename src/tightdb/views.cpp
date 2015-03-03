#include <tightdb/views.hpp>

using namespace tightdb;

// Re-sort view according to last used criterias
void RowIndexes::sort(Sorter& sorting_predicate)
{
    std::vector<size_t> v;
    for (size_t t = 0; t < size(); t++)
        v.push_back(m_row_indexes.get(t));
    sorting_predicate.m_row_indexes_class = this;
    std::stable_sort(v.begin(), v.end(), sorting_predicate);
    m_row_indexes.clear();
    for (size_t t = 0; t < v.size(); t++)
        m_row_indexes.add(v[t]);
}

