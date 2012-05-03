#ifndef TIGHTDB_TABLE_VIEW_H
#define TIGHTDB_TABLE_VIEW_H

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

    Table& GetParent() {return m_table;}
    Array& GetRefColumn() {return m_refs;}
    std::size_t GetRef(std::size_t ndx) const {return m_refs.GetAsRef(ndx);}

    bool is_empty() const {return m_refs.is_empty();}
    std::size_t size() const {return m_refs.Size();}

    // Getting values
    int64_t Get(std::size_t column_id, std::size_t ndx) const;
    bool get_bool(std::size_t column_id, std::size_t ndx) const;
    std::time_t get_date(std::size_t column_id, std::size_t ndx) const;
    const char* get_string(std::size_t column_id, std::size_t ndx) const;
    BinaryData get_binary(std::size_t column_id, std::size_t ndx) const;
    Mixed get_mixed(std::size_t column_id, std::size_t ndx) const;
    TableRef GetTable(std::size_t column_id, std::size_t ndx);

    // Setting values
    void Set(std::size_t column_id, std::size_t ndx, int64_t value);
    void set_bool(std::size_t column_id, std::size_t ndx, bool value);
    void set_date(std::size_t column_id, std::size_t ndx, std::time_t value);
    void set_string(std::size_t column_id, std::size_t ndx, const char* value);
    void set_binary(std::size_t column_id, std::size_t ndx, const char* value, std::size_t len);
    void set_mixed(std::size_t column_id, std::size_t ndx, Mixed value);

    void Sort(std::size_t column, bool Ascending = true);

    // Deleting
    void clear();
    void remove(std::size_t ndx);
    void pop_back() { if (!is_empty()) remove(size()-1); }

    // Finding
    std::size_t Find(std::size_t column_id, int64_t value) const;
    void FindAll(TableView& tv, std::size_t column_id, int64_t value);
    std::size_t FindString(std::size_t column_id, const char* value) const;
    void FindAllString(TableView& tv, std::size_t column_id, const char *value);

    // Aggregate functions
    int64_t sum(std::size_t column_id) const;
    int64_t Max(std::size_t column_id) const;
    int64_t Min(std::size_t column_id) const;

    Table *GetTable(); // todo, temporary for tests FIXME: Is this still needed????

private:
    // Don't allow copying
    TableView& operator=(const TableView&) {return *this;}

    Table& m_table;
    Array m_refs;
};


} // namespace tightdb

#endif // TIGHTDB_TABLE_VIEW_H
