#ifndef TIGHTDB_SPEC_H
#define TIGHTDB_SPEC_H

#include "Array.hpp"

namespace tightdb {


class Table;



class Spec {
public:
    Spec(Allocator& alloc, std::size_t ref, ArrayParent *parent, std::size_t pndx);
    Spec(const Spec& s);

    void AddColumn(ColumnType type, const char* name);
    Spec AddColumnTable(const char* name);

    Spec GetSpec(std::size_t column_id);
    const Spec GetSpec(std::size_t column_id) const;

    std::size_t GetColumnCount() const;
    ColumnType GetColumnType(std::size_t ndx) const;
    const char* GetColumnName(std::size_t ndx) const;
    std::size_t GetColumnIndex(const char* name) const;

    // Column Attributes
    ColumnType GetColumnAttr(std::size_t ndx) const;
    void SetColumnAttr(std::size_t ndx, ColumnType attr);

    std::size_t GetRef() const {return m_specSet.GetRef();}

    // Serialization
    template<class S> std::size_t Write(S& out, std::size_t& pos) const;

#ifdef _DEBUG
    void ToDot(std::ostream& out, const char* title=NULL) const;
#endif //_DEBUG

private:
    void Create(std::size_t ref, ArrayParent *parent, std::size_t pndx);

    Array m_specSet;
    Array m_spec;
    ArrayString m_names;
    Array m_subSpecs;
};


} // namespace tightdb

#endif // TIGHTDB_SPEC_H
