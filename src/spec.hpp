#ifndef TIGHTDB_SPEC_H
#define TIGHTDB_SPEC_H

#include "Array.hpp"
#include "array_string.hpp"
#include "ColumnType.hpp"

namespace tightdb {


class Table;



class Spec {
public:
    Spec(Allocator& alloc);
    Spec(Allocator& alloc, ArrayParent* parent, size_t pndx);
    Spec(Allocator& alloc, std::size_t ref, ArrayParent *parent, std::size_t pndx);
    Spec(const Spec& s);
    
    std::size_t GetRef() const;
    void UpdateRef(size_t ref, ArrayParent* parent=NULL, size_t pndx=0);
    void SetParent(ArrayParent* parent, size_t pndx);
    bool UpdateFromParent();
    void Destroy();

    void AddColumn(ColumnType type, const char* name);
    Spec AddColumnTable(const char* name);

    Spec GetSpec(std::size_t column_id);
    const Spec GetSpec(std::size_t column_id) const;
    std::size_t GetSubSpecRef(std::size_t column_ndx) const;

    // Direct access to type and attribute list
    std::size_t GetTypeAttrCount() const;
    ColumnType GetTypeAttr(size_t ndx) const;
    
    // Column info
    std::size_t GetColumnCount() const;
    ColumnType GetColumnType(std::size_t ndx) const;
    ColumnType GetRealColumnType(std::size_t ndx) const;
    void SetColumnType(std::size_t ndx, ColumnType type);
    const char* GetColumnName(std::size_t ndx) const;
    std::size_t GetColumnIndex(const char* name) const;

    // Column Attributes
    ColumnType GetColumnAttr(std::size_t ndx) const;
    void SetColumnAttr(std::size_t ndx, ColumnType attr);

    // Serialization
    template<class S> std::size_t Write(S& out, std::size_t& pos) const;

#ifdef _DEBUG
    bool Compare(const Spec& spec) const;
    void Verify() const;
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
