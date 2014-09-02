#ifndef TIGHTDB_VIEWS_HPP
#define TIGHTDB_VIEWS_HPP

#include <tightdb/column.hpp>

using namespace tightdb;

// This class is for common functionality of ListView and LinkView which inherit from it. Currently it only 
// supports sorting.
class RowIndexes
{
public:
    RowIndexes(Column::unattached_root_tag urt, tightdb::Allocator& alloc) : m_row_indexes(urt, alloc), 
        m_auto_sort(false)  {}
    RowIndexes(Column::move_tag mt, Column& col) : m_row_indexes(mt, col), m_auto_sort(false) {}
    virtual ~RowIndexes() {};

    // Return a column of the table that m_row_indexes are pointing at (which is the target table for LinkList and
    // parent table for TableView)
    virtual ColumnBase& get_column_base(size_t index) = 0;

    virtual size_t size() const = 0;

    // Predicate for std::sort
    struct Sorter
    {
        Sorter(){}
        Sorter(std::vector<size_t> columns, bool ascending) : m_columns(columns), m_ascending(ascending) {};
        bool operator()(size_t i, size_t j) const
        {
            TIGHTDB_ASSERT(m_columns.size() == m_column_pointers.size());
            for (size_t t = 0; t < m_column_pointers.size(); t++) {
                int c = m_column_pointers[t]->compare_values(i, j);
                if (c != 0)
                    return m_ascending ? c > 0 : c < 0;
            }
            return false; // row i == row j
        }
        std::vector<size_t> m_columns;
        std::vector<ColumnTemplateBase*> m_column_pointers;
        RowIndexes* m_row_indexes_class;
        bool m_ascending;
    };

    // Sort m_row_indexes according to one column
    void sort(size_t column, bool ascending = true);

    // Sort m_row_indexes according to multiple columns
    void sort(std::vector<size_t> columns, bool ascending = true);

    // Re-sort view according to last used criterias
    void re_sort();

    Column m_row_indexes;
    Sorter m_sorting_predicate; // Stores sorting criterias (columns + ascending)
    bool m_auto_sort;
};

#endif // TIGHTDB_VIEWS_HPP
