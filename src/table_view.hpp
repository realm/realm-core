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
#ifndef __TIGHTDB_TABLE_VIEW_H
#define __TIGHTDB_TABLE_VIEW_H

#include "array.hpp"
#include "table_ref.hpp"

namespace tightdb {
using std::size_t;
using std::time_t;

class TableView {
public:
    TableView(Table& source);
    TableView(const TableView& v);
    ~TableView();

    bool is_empty() const {return m_refs.is_empty();}
    size_t size() const {return m_refs.Size();}

    // Getting values
    int64_t     get_int(size_t column_ndx, size_t ndx) const;
    bool        get_bool(size_t column_ndx, size_t ndx) const;
    std::time_t get_date(size_t column_ndx, size_t ndx) const;
    const char* get_string(size_t column_ndx, size_t ndx) const;
    BinaryData  get_binary(size_t column_ndx, size_t ndx) const;
    Mixed       get_mixed(size_t column_ndx, size_t ndx) const;
    TableRef    get_subtable(size_t column_ndx, size_t ndx);

    // Setting values
    void set_int(size_t column_ndx, size_t ndx, int64_t value);
    void set_bool(size_t column_ndx, size_t ndx, bool value);
    void set_date(size_t column_ndx, size_t ndx, std::time_t value);
    void set_string(size_t column_ndx, size_t ndx, const char* value);
    void set_binary(size_t column_ndx, size_t ndx, const char* value, size_t len);
    void set_mixed(size_t column_ndx, size_t ndx, Mixed value);

    // Deleting
    void clear();
    void remove(size_t ndx);
    void remove_last() { if (!is_empty()) remove(size()-1); }

    // Searching (Int and String)
    size_t  find_first(size_t column_ndx, int64_t value) const;
    size_t  find_first(size_t column_ndx, const char* value) const;
    void    find_all(TableView& tv, size_t column_ndx, int64_t value);
    void    find_all(TableView& tv, size_t column_ndx, const char *value);

    // Aggregate functions
    int64_t sum(size_t column_ndx) const;
    int64_t maximum(size_t column_ndx) const;
    int64_t minimum(size_t column_ndx) const;

    void sort(size_t column, bool Ascending = true);

    // Get row index in the source table this view is "looking" at.
    size_t get_source_ndx(size_t row_ndx) const {return m_refs.GetAsRef(row_ndx);}


//protected: DON'T USE.
//    friend Query;

    Table *get_table(); // todo, temporary for tests FIXME: Is this still needed????
    Table& get_parent() {return m_table;}
    Array& get_ref_column() {return m_refs;}
    
private:
    // Don't allow copying
    TableView& operator=(const TableView&) {return *this;}

    Table& m_table;
    Array m_refs;
};


} // namespace tightdb

#endif // __TIGHTDB_TABLE_VIEW_H
