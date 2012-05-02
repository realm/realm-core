#ifndef TIGHTDB_SPEC_H
#define TIGHTDB_SPEC_H

#include "Array.hpp"
#include "array_string.hpp"
#include "ColumnType.hpp"

namespace tightdb {


class Spec {
public:
    Spec(Allocator& alloc);
    Spec(Allocator& alloc, ArrayParent* parent, size_t pndx);
    Spec(Allocator& alloc, std::size_t ref, ArrayParent *parent, std::size_t pndx);
    Spec(const Spec& s);

private:
    std::size_t get_ref() const;
public:
    void update_ref(size_t ref, ArrayParent* parent=NULL, size_t pndx=0);
    void set_parent(ArrayParent* parent, size_t pndx);
    bool update_from_parent();
    void destroy();

    void add_column(ColumnType type, const char* name);
    Spec add_subtable_column(const char* name);

    Spec get_subspec(std::size_t column_id);
    const Spec get_subspec(std::size_t column_id) const;
    std::size_t get_subspec_ref(std::size_t column_ndx) const;

    // Direct access to type and attribute list
    std::size_t get_type_attr_count() const;
    ColumnType get_type_attr(size_t ndx) const;
    
    // Column info
    std::size_t get_column_count() const;
    ColumnType get_column_type(std::size_t ndx) const;
    ColumnType get_real_column_type(std::size_t ndx) const;
    void set_column_type(std::size_t ndx, ColumnType type);
    const char* get_column_name(std::size_t ndx) const;
    std::size_t get_column_index(const char* name) const;

    // Column Attributes
    ColumnType get_column_attr(std::size_t ndx) const;
    void set_column_attr(std::size_t ndx, ColumnType attr);

private:
    // Serialization
    template<class S> std::size_t write(S& out, std::size_t& pos) const;
public:

#ifdef _DEBUG
    bool compare(const Spec& spec) const;
    void verify() const;
    void to_dot(std::ostream& out, const char* title=NULL) const;
#endif //_DEBUG

private:
    friend class Table;

    void create(std::size_t ref, ArrayParent *parent, std::size_t pndx);

    Array m_specSet;
    Array m_spec;
    ArrayString m_names;
    Array m_subSpecs;
};


} // namespace tightdb

#endif // TIGHTDB_SPEC_H
