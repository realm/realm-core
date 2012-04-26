#ifndef TIGHTDB_TABLE_VIEW_H
#define TIGHTDB_TABLE_VIEW_H

#include "Array.hpp"
#include "table_ref.hpp"

namespace tightdb {


class TableView {
public:
    TableView(Table& source);
    TableView(const TableView& v);
    ~TableView();

    Table& GetParent() {return m_table;}
    Array& GetRefColumn() {return m_refs;}
    std::size_t GetRef(std::size_t ndx) const {return m_refs.GetAsRef(ndx);}

    bool IsEmpty() const {return m_refs.IsEmpty();}
    std::size_t GetSize() const {return m_refs.Size();}

    // Getting values
    int64_t Get(std::size_t column_id, std::size_t ndx) const;
    bool GetBool(std::size_t column_id, std::size_t ndx) const;
    std::time_t GetDate(std::size_t column_id, std::size_t ndx) const;
    const char* GetString(std::size_t column_id, std::size_t ndx) const;

    // Setting values
    void Set(std::size_t column_id, std::size_t ndx, int64_t value);
    void SetBool(std::size_t column_id, std::size_t ndx, bool value);
    void SetDate(std::size_t column_id, std::size_t ndx, std::time_t value);
    void SetString(std::size_t column_id, std::size_t ndx, const char* value);
    void Sort(std::size_t column, bool Ascending = true);
    // Sub-tables
    TableRef GetTable(std::size_t column_id, std::size_t ndx);

    // Deleting
    void Delete(std::size_t ndx);
    void Clear();

    // Finding
    std::size_t Find(std::size_t column_id, int64_t value) const;
    void FindAll(TableView& tv, std::size_t column_id, int64_t value);
    std::size_t FindString(std::size_t column_id, const char* value) const;
    void FindAllString(TableView& tv, std::size_t column_id, const char *value);

    // Aggregate functions
    int64_t Sum(std::size_t column_id) const;
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
