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

#include <tightdb/util/features.h>
#include <tightdb/array.hpp>
#include <tightdb/array_string.hpp>
#include <tightdb/data_type.hpp>
#include <tightdb/column_type.hpp>

namespace tightdb {

class Table;

class Spec {
public:
    Spec(const Spec&);
    ~Spec() TIGHTDB_NOEXCEPT;

    std::size_t add_column(DataType type, StringData name, ColumnAttr attr = col_attr_None);
    std::size_t add_subcolumn(const std::vector<std::size_t>& column_path, DataType type,
                              StringData name);
    Spec add_subtable_column(StringData name);

    void rename_column(std::size_t column_ndx, StringData new_name);
    void rename_column(const std::vector<std::size_t>& column_ids, StringData new_name);
    void remove_column(std::size_t column_ndx);
    void remove_column(const std::vector<std::size_t>& column_ids);

    // FIXME: It seems that the application must make sure that the
    // parent Spec object is kept alive for at least as long as the
    // spec that is returned. This also has implications for language
    // bindings such as Java. The reason is that the parent pointers
    // must stay valid.
    Spec get_subtable_spec(std::size_t column_ndx);
    // FIXME: Returning a const Spec is futile since Spec has a public
    // copy constructor.
    const Spec get_subtable_spec(std::size_t column_ndx) const;

    // Column info
    std::size_t get_column_count() const TIGHTDB_NOEXCEPT;
    DataType get_column_type(std::size_t column_ndx) const TIGHTDB_NOEXCEPT;
    ColumnType get_real_column_type(std::size_t column_ndx) const TIGHTDB_NOEXCEPT;
    StringData get_column_name(std::size_t column_ndx) const TIGHTDB_NOEXCEPT;

    /// Returns std::size_t(-1) if the specified column is not found.
    std::size_t get_column_index(StringData name) const;

    // Column Attributes
    ColumnAttr get_column_attr(std::size_t column_ndx) const;

    // Auto Enumerated string columns
    void upgrade_string_to_enum(size_t column_ndx, ref_type keys_ref,
                                ArrayParent*& keys_parent, size_t& keys_ndx);
    ref_type get_enumkeys_ref(size_t column_ndx,
                              ArrayParent** keys_parent=null_ptr, size_t* keys_ndx=null_ptr);

    // Get position in column list adjusted for indexes
    // (since index refs are stored alongside column refs in
    //  m_columns, this may differ from the logical position)
    size_t get_column_pos(size_t column_ndx) const;

    /// Compare two table specs for equality.
    bool operator==(const Spec&) const;

    /// Compare two tables specs for inequality. See operator==().
    bool operator!=(const Spec& s) const { return !(*this == s); }

#ifdef TIGHTDB_DEBUG
    void Verify() const; // Must be upper case to avoid conflict with macro in ObjC
    void to_dot(std::ostream&, StringData title = StringData()) const;
#endif

private:
    // Member variables
    const Table* const m_table;
    Array m_top;
    Array m_spec;
    ArrayString m_names;
    Array m_attr;
    Array m_subspecs;
    Array m_enumkeys;

    Spec(const Table*, Allocator&); // Uninitialized
    Spec(const Table*, Allocator&, ArrayParent*, std::size_t ndx_in_parent);
    Spec(const Table*, Allocator&, ref_type, ArrayParent*, std::size_t ndx_in_parent);

    void init_from_ref(ref_type, ArrayParent*, std::size_t ndx_in_parent) TIGHTDB_NOEXCEPT;
    void destroy() TIGHTDB_NOEXCEPT;

    ref_type get_ref() const TIGHTDB_NOEXCEPT;

    /// Called in the context of Group::commit() to ensure that
    /// attached table accessors stay valid across a commit. Please
    /// note that this works only for non-transactional commits. Table
    /// accessors obtained during a transaction are always detached
    /// when the transaction ends.
    void update_from_parent(std::size_t old_baseline) TIGHTDB_NOEXCEPT;

    void set_parent(ArrayParent*, std::size_t ndx_in_parent) TIGHTDB_NOEXCEPT;

    void set_column_type(std::size_t column_ndx, ColumnType type);
    void set_column_attr(std::size_t column_ndx, ColumnAttr attr);

    std::size_t get_subspec_ndx(std::size_t column_ndx) const;
    std::size_t get_subspec_ref(std::size_t subspec_ndx) const;
    std::size_t get_num_subspecs() const TIGHTDB_NOEXCEPT;
    Spec get_subspec_by_ndx(std::size_t subspec_ndx);

    size_t get_enumkeys_ndx(size_t column_ndx) const;

    /// Construct an empty spec and return just the reference to the
    /// underlying memory.
    static ref_type create_empty_spec(Allocator&);

    std::size_t do_add_subcolumn(const std::vector<std::size_t>& column_ids, std::size_t pos,
                                 DataType type, StringData name);
    void do_remove_column(const std::vector<std::size_t>& column_ids, std::size_t pos);
    void do_rename_column(const std::vector<std::size_t>& column_ids, std::size_t pos,
                          StringData name);

    struct ColumnInfo {
        std::size_t m_column_ref_ndx; ///< Index within Table::m_columns
        bool m_has_index;
        ColumnInfo(): m_column_ref_ndx(0), m_has_index(false) {}
    };

    void get_column_info(std::size_t column_ndx, ColumnInfo&) const;
    void get_subcolumn_info(const std::vector<std::size_t>& column_path,
                            std::size_t column_path_ndx, ColumnInfo&) const;

#ifdef TIGHTDB_ENABLE_REPLICATION
    // Precondition: 1 <= end - begin
    std::size_t* record_subspec_path(const Array* root_subspecs, std::size_t* begin,
                                     std::size_t* end) const TIGHTDB_NOEXCEPT;
    friend class Replication;
#endif

    friend class Table;
};




// Implementation:

inline std::size_t Spec::get_num_subspecs() const TIGHTDB_NOEXCEPT
{
    return m_subspecs.is_attached() ? m_subspecs.size() : 0;
}

inline ref_type Spec::create_empty_spec(Allocator& alloc)
{
    // The 'spec_set' contains the specification (types and names) of
    // all columns and sub-tables
    Array spec_set(Array::type_HasRefs, null_ptr, 0, alloc);
    std::size_t size = 0;
    spec_set.add(Array::create_array(Array::type_Normal, size, alloc)); // One type for each column
    spec_set.add(ArrayString::create_empty_array(alloc)); // One name for each column
    spec_set.add(ArrayString::create_empty_array(alloc)); // One attr set for each column
    return spec_set.get_ref();
}


// Uninitialized Spec (call init_from_ref() to init)
inline Spec::Spec(const Table* table, Allocator& alloc):
    m_table(table), m_top(alloc), m_spec(alloc), m_names(alloc), m_attr(alloc), m_subspecs(alloc), m_enumkeys(alloc) {}

// Create a new Spec
inline Spec::Spec(const Table* table, Allocator& alloc, ArrayParent* parent,
                  std::size_t ndx_in_parent):
    m_table(table), m_top(alloc), m_spec(alloc), m_names(alloc), m_attr(alloc), m_subspecs(alloc), m_enumkeys(alloc)
{
    ref_type ref = create_empty_spec(alloc); // Throws
    init_from_ref(ref, parent, ndx_in_parent);
}

// Create Spec from ref
inline Spec::Spec(const Table* table, Allocator& alloc, ref_type ref, ArrayParent* parent,
                  std::size_t ndx_in_parent):
    m_table(table), m_top(alloc), m_spec(alloc), m_names(alloc), m_attr(alloc), m_subspecs(alloc), m_enumkeys(alloc)
{
    init_from_ref(ref, parent, ndx_in_parent);
}

inline Spec::Spec(const Spec& s):
    m_table(s.m_table), m_top(s.m_top.get_alloc()), m_spec(s.m_top.get_alloc()),
    m_names(s.m_top.get_alloc()), m_attr(s.m_top.get_alloc()), m_subspecs(s.m_top.get_alloc()), m_enumkeys(s.m_top.get_alloc())
{
    ref_type ref        = s.m_top.get_ref();
    ArrayParent* parent = s.m_top.get_parent();
    std::size_t pndx    = s.m_top.get_ndx_in_parent();

    init_from_ref(ref, parent, pndx);
}


inline Spec Spec::get_subspec_by_ndx(std::size_t subspec_ndx)
{
    Allocator& alloc = m_top.get_alloc();
    ref_type ref = m_subspecs.get_as_ref(subspec_ndx);
    return Spec(m_table, alloc, ref, &m_subspecs, subspec_ndx);
}

inline void Spec::destroy() TIGHTDB_NOEXCEPT
{
    m_top.destroy();
}

inline ref_type Spec::get_ref() const TIGHTDB_NOEXCEPT
{
    return m_top.get_ref();
}

inline void Spec::set_parent(ArrayParent* parent, std::size_t ndx_in_parent) TIGHTDB_NOEXCEPT
{
    m_top.set_parent(parent, ndx_in_parent);
}

inline void Spec::rename_column(std::size_t column_ndx, StringData new_name)
{
    TIGHTDB_ASSERT(column_ndx < m_spec.size());

    //TODO: Verify that new name is valid

    m_names.set(column_ndx, new_name);
}

inline void Spec::rename_column(const std::vector<std::size_t>& column_ids, StringData name)
{
    do_rename_column(column_ids, 0, name);
}

inline void Spec::remove_column(const std::vector<std::size_t>& column_ids)
{
    do_remove_column(column_ids, 0);
}

inline std::size_t Spec::get_column_count() const TIGHTDB_NOEXCEPT
{
    return m_names.size();
}

inline ColumnType Spec::get_real_column_type(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < get_column_count());
    return ColumnType(m_spec.get(ndx));
}

inline void Spec::set_column_type(std::size_t column_ndx, ColumnType type)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());

    // At this point we only support upgrading to string enum
    TIGHTDB_ASSERT(ColumnType(m_spec.get(column_ndx)) == col_type_String);
    TIGHTDB_ASSERT(type == col_type_StringEnum);

    m_spec.set(column_ndx, type);
}

inline ColumnAttr Spec::get_column_attr(std::size_t ndx) const
{
    TIGHTDB_ASSERT(ndx < get_column_count());
    return ColumnAttr(m_attr.get(ndx));
}

inline void Spec::set_column_attr(std::size_t column_ndx, ColumnAttr attr)
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());

    // At this point we only allow one attr at a time
    // so setting it will overwrite existing. In the future
    // we will allow combinations.
    m_attr.set(column_ndx, attr);
}

inline StringData Spec::get_column_name(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < get_column_count());
    return m_names.get(ndx);
}

inline std::size_t Spec::get_column_index(StringData name) const
{
    return m_names.find_first(name);
}


} // namespace tightdb

#endif // TIGHTDB_SPEC_HPP
