#include <tightdb/column_table.hpp>
#include <tightdb/lang_bind_helper.hpp>

using namespace std;

namespace tightdb {

Table* LangBindHelper::get_subtable_ptr_during_insert(Table* t, size_t col_ndx, size_t row_ndx)
{
    TIGHTDB_ASSERT(col_ndx < t->get_column_count());
    ColumnTable& subtables =  t->GetColumnTable(col_ndx);
    TIGHTDB_ASSERT(row_ndx < subtables.size());
    Table* subtab = subtables.get_subtable_ptr(row_ndx);
    subtab->bind_ref();
    return subtab;
}

} // namespace tightdb
