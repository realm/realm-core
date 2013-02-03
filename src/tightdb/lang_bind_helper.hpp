/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/
#ifndef TIGHTDB_LANG_BIND_HELPER_HPP
#define TIGHTDB_LANG_BIND_HELPER_HPP

#include <cstddef>

#include <tightdb/table.hpp>
#include <tightdb/column_table.hpp>
#include <tightdb/table_view.hpp>
#include <tightdb/group.hpp>

namespace tightdb {


/// These functions are only to be used by language bindings to gain
/// access to certain otherwise private memebers.
///
/// \note An application must never call these functions directly.
///
/// All the get_*_ptr() functions as well as new_table() in this class
/// will return a Table pointer where the reference count has already been
/// incremented.
///
/// The application must make sure that the unbind_table_ref() function is
/// called to decrement the reference count when it no longer needs
/// access to that table.
class LangBindHelper {
public:
    /// Construct a freestanding table.
    static Table* new_table();

    static Table* get_subtable_ptr(Table*, std::size_t column_ndx, std::size_t row_ndx);
    static const Table* get_subtable_ptr(const Table*, std::size_t column_ndx,
                                         std::size_t row_ndx);

    // FIXME: He who added this one, please provide a comment that explains why it is necessary!
    static Table* get_subtable_ptr_during_insert(Table*, std::size_t col_ndx,
                                                 std::size_t row_ndx);

    static Table* get_subtable_ptr(TableView*, std::size_t column_ndx, std::size_t row_ndx);
    static const Table* get_subtable_ptr(const TableView*, std::size_t column_ndx,
                                         std::size_t row_ndx);
    static const Table* get_subtable_ptr(const ConstTableView*, std::size_t column_ndx,
                                         std::size_t row_ndx);

    static Table* get_table_ptr(Group* grp, const char* name);
    static Table* get_table_ptr(Group* grp, const char* name, bool& was_created);
    static const Table* get_table_ptr(const Group* grp, const char* name);

    static void unbind_table_ref(const Table*);
    static void bind_table_ref(const Table*);
};


// Implementation:

inline Table* LangBindHelper::new_table()
{
    Allocator& alloc = Allocator::get_default();
    const std::size_t ref = Table::create_empty_table(alloc); // Throws
    Table* const table = new Table(Table::RefCountTag(), alloc, ref, 0, 0); // Throws
    table->bind_ref();
    return table;
}

inline Table* LangBindHelper::get_subtable_ptr(Table* t, std::size_t column_ndx,
                                               std::size_t row_ndx)
{
    Table* subtab = t->get_subtable_ptr(column_ndx, row_ndx);
    subtab->bind_ref();
    return subtab;
}

inline const Table* LangBindHelper::get_subtable_ptr(const Table* t, std::size_t column_ndx,
                                                     std::size_t row_ndx)
{
    const Table* subtab = t->get_subtable_ptr(column_ndx, row_ndx);
    subtab->bind_ref();
    return subtab;
}

inline Table* LangBindHelper::get_subtable_ptr_during_insert(Table* t, std::size_t col_ndx,
                                                             std::size_t row_ndx)
{
    TIGHTDB_ASSERT(col_ndx < t->get_column_count());
    ColumnTable& subtables =  t->GetColumnTable(col_ndx);
    TIGHTDB_ASSERT(row_ndx < subtables.Size());
    Table* subtab = subtables.get_subtable_ptr(row_ndx);
    subtab->bind_ref();
    return subtab;
}

inline Table* LangBindHelper::get_subtable_ptr(TableView* tv, std::size_t column_ndx,
                                               std::size_t row_ndx)
{
    return get_subtable_ptr(&tv->get_parent(), column_ndx, tv->get_source_ndx(row_ndx));
}

inline const Table* LangBindHelper::get_subtable_ptr(const TableView* tv, std::size_t column_ndx,
                                                     std::size_t row_ndx)
{
    return get_subtable_ptr(&tv->get_parent(), column_ndx, tv->get_source_ndx(row_ndx));
}

inline const Table* LangBindHelper::get_subtable_ptr(const ConstTableView* tv,
                                                     std::size_t column_ndx, std::size_t row_ndx)
{
    return get_subtable_ptr(&tv->get_parent(), column_ndx, tv->get_source_ndx(row_ndx));
}

inline Table* LangBindHelper::get_table_ptr(Group* grp, const char* name)
{
    Table* subtab = grp->get_table_ptr(name);
    subtab->bind_ref();
    return subtab;
}

inline Table* LangBindHelper::get_table_ptr(Group* grp, const char* name, bool& was_created)
{
    Table* subtab = grp->get_table_ptr(name, was_created);
    subtab->bind_ref();
    return subtab;
}

inline const Table* LangBindHelper::get_table_ptr(const Group* grp, const char* name)
{
    const Table* subtab = grp->get_table_ptr(name);
    subtab->bind_ref();
    return subtab;
}

inline void LangBindHelper::unbind_table_ref(const Table* t)
{
   t->unbind_ref();
}

inline void LangBindHelper::bind_table_ref(const Table* t)
{
   t->bind_ref();
}


} // namespace tightdb

#endif // TIGHTDB_LANG_BIND_HELPER_HPP
