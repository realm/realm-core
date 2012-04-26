#ifndef TIGHTDB_SPEC_H
#define TIGHTDB_SPEC_H

#include "Array.hpp"

namespace tightdb {


class Table;
class ColumnTable;



class TableColumnFactory {
public:
    /**
     * Create a new table column by calling the constructor of the
     * appropriate table column type with the same arguments.
     */
    virtual ColumnTable* create(std::size_t schema_ref,
                                ArrayParent* parent, std::size_t idx_in_parent,
                                Allocator& alloc, const Table* tab) = 0;

    /**
     * Create a table column and attach it to an already existing
     * array structure. This is done by calling the constructor of the
     * appropriate table column type with the same arguments.
     */
    virtual ColumnTable *create(std::size_t columns_ref, std::size_t schema_ref,
                                ArrayParent* parent, size_t idx_in_parent,
                                Allocator& alloc, const Table* tab) = 0;

    virtual ~TableColumnFactory() {}
};



class Spec {
public:
    Spec(Allocator& alloc, size_t ref, ArrayParent *parent, size_t pndx);
    Spec(const Spec& s);

    void AddColumn(ColumnType type, const char* name);
    Spec AddColumnTable(const char* name, TableColumnFactory* = 0);

    Spec GetSpec(size_t column_id);
    const Spec GetSpec(size_t column_id) const;

    size_t GetColumnCount() const;
    ColumnType GetColumnType(size_t ndx) const;
    const char* GetColumnName(size_t ndx) const;
    size_t GetColumnIndex(const char* name) const;

    // Column Attributes
    ColumnType GetColumnAttr(size_t ndx) const;
    void SetColumnAttr(size_t ndx, ColumnType attr);

    size_t GetRef() const {return m_specSet.GetRef();}

    // Serialization
    template<class S> size_t Write(S& out, size_t& pos) const;

#ifdef _DEBUG
    void ToDot(std::ostream& out, const char* title=NULL) const;
#endif //_DEBUG

private:
    void Create(size_t ref, ArrayParent *parent, size_t pndx);

    Array m_specSet;
    Array m_spec;
    ArrayString m_names;
    Array m_subSpecs;
};


} // namespace tightdb

#endif // TIGHTDB_SPEC_H
