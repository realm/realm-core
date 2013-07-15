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
#ifndef TIGHTDB_SPEC_HPP
#define TIGHTDB_SPEC_HPP

#include <tightdb/array.hpp>
#include <tightdb/array_string.hpp>
#include <tightdb/data_type.hpp>
#include <tightdb/column_type.hpp>

namespace tightdb {

using std::size_t;
using std::vector;

class Table;

class Spec {
public:
    Spec(const Spec& s);
    ~Spec();

    size_t add_column(DataType type, StringData name, ColumnType attr=col_attr_None);
    size_t add_subcolumn(const vector<size_t>& column_path, DataType type, StringData name);
    Spec add_subtable_column(StringData name);

    void rename_column(size_t column_ndx, StringData newname);
    void rename_column(const vector<size_t>& column_ids, StringData newname);
    void remove_column(size_t column_ndx);
    void remove_column(const vector<size_t>& column_ids);

    // FIXME: It seems that the application must make sure that the
    // parent Spec object is kept alive for at least as long as the
    // spec that is returned. This also has implications for language
    // bindings such as Java.
    Spec get_subtable_spec(size_t column_ndx);
    // FIXME: Returning a const Spec is futile since Spec has a public
    // copy constructor.
    const Spec get_subtable_spec(size_t column_ndx) const;

    // Direct access to type and attribute list
    size_t get_type_attr_count() const;
    ColumnType get_type_attr(size_t column_ndx) const;

    // Column info
    size_t get_column_count() const TIGHTDB_NOEXCEPT;
    DataType get_column_type(size_t column_ndx) const TIGHTDB_NOEXCEPT;
    ColumnType get_real_column_type(size_t column_ndx) const TIGHTDB_NOEXCEPT;
    StringData get_column_name(size_t column_ndx) const TIGHTDB_NOEXCEPT;

    /// Returns std::size_t(-1) if the specified column is not found.
    size_t get_column_index(StringData name) const;

    // Column Attributes
    ColumnType get_column_attr(size_t column_ndx) const;

    /// Compare two table specs for equality.
    bool operator==(const Spec&) const;

    /// Compare two tables specs for inequality. See operator==().
    bool operator!=(const Spec& s) const { return !(*this == s); }

#ifdef TIGHTDB_DEBUG
    void Verify() const; // Must be upper case to avoid conflict with macro in ObjC
    void to_dot(std::ostream& out, StringData title = StringData()) const;
#endif // TIGHTDB_DEBUG

private:
    friend class Table;

    Spec(const Table*, Allocator&); // Uninitialized
    Spec(const Table*, Allocator&, ArrayParent* parent, size_t ndx_in_parent);
    Spec(const Table*, Allocator&, size_t ref, ArrayParent *parent, size_t ndx_in_parent);

    void init_from_ref(size_t ref, ArrayParent *parent, size_t pndx);
    void destroy();

    size_t get_ref() const;
    void update_ref(size_t ref, ArrayParent* parent=NULL, size_t pndx=0);

    bool update_from_parent();
    void set_parent(ArrayParent* parent, size_t pndx);

    void set_column_type(size_t column_ndx, ColumnType type);
    void set_column_attr(size_t column_ndx, ColumnType attr);

    // Serialization
    template<class S> size_t write(S& out, size_t& pos) const;

    size_t get_column_type_pos(size_t column_ndx) const TIGHTDB_NOEXCEPT;
    size_t get_subspec_ndx(size_t column_ndx) const;
    size_t get_subspec_ref(size_t subspec_ndx) const;
    size_t get_num_subspecs() const { return m_subSpecs.IsValid() ? m_subSpecs.size() : 0; }
    Spec get_subspec_by_ndx(size_t subspec_ndx);

    /// Construct an empty spec and return just the reference to the
    /// underlying memory.
    static size_t create_empty_spec(Allocator&);

    size_t do_add_subcolumn(const vector<size_t>& column_ids, size_t pos, DataType type, StringData name);
    void do_remove_column(const vector<size_t>& column_ids, size_t pos);
    void do_rename_column(const vector<size_t>& column_ids, size_t pos, StringData name);

#ifdef TIGHTDB_ENABLE_REPLICATION
    // Precondition: 1 <= end - begin
    size_t* record_subspec_path(const Array* root_subspecs, size_t* begin,
                                size_t* end) const TIGHTDB_NOEXCEPT;
    friend class Replication;
#endif

    // Member variables
    const Table* const m_table;
    Array m_specSet;
    Array m_spec;
    ArrayString m_names;
    Array m_subSpecs;
};




// Implementation:

inline size_t Spec::create_empty_spec(Allocator& alloc)
{
    // The 'spec_set' contains the specification (types and names) of
    // all columns and sub-tables
    Array spec_set(Array::coldef_HasRefs, 0, 0, alloc);
    spec_set.add(Array::create_empty_array(Array::coldef_Normal, alloc)); // One type for each column
    spec_set.add(ArrayString::create_empty_string_array(alloc)); // One name for each column
    return spec_set.get_ref();
}


// Uninitialized Spec (call update_ref() to init)
inline Spec::Spec(const Table* table, Allocator& alloc):
    m_table(table), m_specSet(alloc), m_spec(alloc), m_names(alloc), m_subSpecs(alloc) {}

// Create a new Spec
inline Spec::Spec(const Table* table, Allocator& alloc, ArrayParent* parent, size_t ndx_in_parent):
    m_table(table), m_specSet(alloc), m_spec(alloc), m_names(alloc), m_subSpecs(alloc)
{
    const size_t ref = create_empty_spec(alloc); // Throws
    init_from_ref(ref, parent, ndx_in_parent);
}

// Create Spec from ref
inline Spec::Spec(const Table* table, Allocator& alloc, size_t ref, ArrayParent* parent, size_t pndx):
    m_table(table), m_specSet(alloc), m_spec(alloc), m_names(alloc), m_subSpecs(alloc)
{
    init_from_ref(ref, parent, pndx);
}

inline Spec::Spec(const Spec& s):
    m_table(s.m_table), m_specSet(s.m_specSet.GetAllocator()), m_spec(s.m_specSet.GetAllocator()),
    m_names(s.m_specSet.GetAllocator()), m_subSpecs(s.m_specSet.GetAllocator())
{
    const size_t ref    = s.m_specSet.get_ref();
    ArrayParent *parent = s.m_specSet.GetParent();
    const size_t pndx   = s.m_specSet.GetParentNdx();

    init_from_ref(ref, parent, pndx);
}


inline Spec Spec::get_subspec_by_ndx(size_t subspec_ndx)
{
    Allocator& alloc = m_specSet.GetAllocator();
    const size_t ref = m_subSpecs.get_as_ref(subspec_ndx);
    return Spec(m_table, alloc, ref, &m_subSpecs, subspec_ndx);
}


} // namespace tightdb

#endif // TIGHTDB_SPEC_HPP
