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
class SubspecRef;
class ConstSubspecRef;

class Spec {
public:
    Spec(SubspecRef) TIGHTDB_NOEXCEPT;
    ~Spec() TIGHTDB_NOEXCEPT;

    std::size_t add_column(const Table* table, DataType type, StringData name,
                           ColumnAttr attr = col_attr_None);
    std::size_t add_subcolumn(const Table* table, const std::vector<std::size_t>& column_path,
                              DataType type, StringData name);
    SubspecRef add_subtable_column(const Table* table, StringData name);

    void rename_column(std::size_t column_ndx, StringData new_name);
    void rename_column(const std::vector<std::size_t>& column_ids, StringData new_name);
    void remove_column(std::size_t column_ndx);
    void remove_column(const std::vector<std::size_t>& column_ids);

    //@{
    // If a new Spec is constructed from the returned subspec
    // reference, it is the responsibility of the application that the
    // parent Spec object (this) is kept alive for at least as long as
    // the new Spec object.
    SubspecRef get_subtable_spec(std::size_t column_ndx) TIGHTDB_NOEXCEPT;
    ConstSubspecRef get_subtable_spec(std::size_t column_ndx) const TIGHTDB_NOEXCEPT;
    //@}

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
    void upgrade_string_to_enum(std::size_t column_ndx, ref_type keys_ref,
                                ArrayParent*& keys_parent, std::size_t& keys_ndx);
    ref_type get_enumkeys_ref(std::size_t column_ndx,
                              ArrayParent** keys_parent = 0, std::size_t* keys_ndx = 0);

    // Get position in column list adjusted for indexes
    // (since index refs are stored alongside column refs in
    //  m_columns, this may differ from the logical position)
    std::size_t get_column_pos(std::size_t column_ndx) const;

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
    Array m_top;
    Array m_spec;
    ArrayString m_names;
    Array m_attr;
    Array m_subspecs;
    Array m_enumkeys;

    Spec(Allocator&) TIGHTDB_NOEXCEPT; // Uninitialized
    Spec(Allocator&, ArrayParent*, std::size_t ndx_in_parent);
    Spec(Allocator&, ref_type, ArrayParent*, std::size_t ndx_in_parent);

    void init(ref_type, ArrayParent*, std::size_t ndx_in_parent) TIGHTDB_NOEXCEPT;
    void init(SubspecRef) TIGHTDB_NOEXCEPT;
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

    std::size_t get_subspec_ndx(std::size_t column_ndx) const TIGHTDB_NOEXCEPT;
    std::size_t get_subspec_ref(std::size_t subspec_ndx) const TIGHTDB_NOEXCEPT;
    std::size_t get_num_subspecs() const TIGHTDB_NOEXCEPT;
    SubspecRef get_subspec_by_ndx(std::size_t subspec_ndx) TIGHTDB_NOEXCEPT;

    size_t get_enumkeys_ndx(size_t column_ndx) const;

    /// Construct an empty spec and return just the reference to the
    /// underlying memory.
    static ref_type create_empty_spec(Allocator&);

    std::size_t do_add_subcolumn(const Table* table, const std::vector<std::size_t>& column_ids,
                                 std::size_t pos, DataType type, StringData name);
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



class SubspecRef {
public:
    struct const_cast_tag {};
    SubspecRef(const_cast_tag, ConstSubspecRef r) TIGHTDB_NOEXCEPT;
    ~SubspecRef() TIGHTDB_NOEXCEPT {}
    Allocator& get_alloc() const TIGHTDB_NOEXCEPT { return m_parent->get_alloc(); }

private:
    Array* const m_parent;
    std::size_t const m_ndx_in_parent;

    SubspecRef(Array* parent, std::size_t ndx_in_parent) TIGHTDB_NOEXCEPT;

    friend class Spec;
    friend class ConstSubspecRef;
};

class ConstSubspecRef {
public:
    ConstSubspecRef(SubspecRef r) TIGHTDB_NOEXCEPT;
    ~ConstSubspecRef() TIGHTDB_NOEXCEPT {}
    Allocator& get_alloc() const TIGHTDB_NOEXCEPT { return m_parent->get_alloc(); }

private:
    const Array* const m_parent;
    std::size_t const m_ndx_in_parent;

    ConstSubspecRef(const Array* parent, std::size_t ndx_in_parent) TIGHTDB_NOEXCEPT;

    friend class Spec;
    friend class SubspecRef;
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
    Array spec_set(alloc);
    spec_set.create(Array::type_HasRefs); // Throws
    try {
        std::size_t size = 0;
        int_fast64_t value = 0;
        // One type for each column
        ref_type types_ref = Array::create_array(Array::type_Normal, size, value, alloc); // Throws
        try {
            int_fast64_t v = types_ref; // FIXME: Dangerous case: unsigned -> signed
            spec_set.add(v); // Throws
        }
        catch (...) {
            Array::destroy(types_ref, alloc);
            throw;
        }
        // One name for each column
        ref_type names_ref = ArrayString::create_array(size, alloc); // Throws
        try {
            int_fast64_t v = names_ref; // FIXME: Dangerous case: unsigned -> signed
            spec_set.add(v); // Throws
        }
        catch (...) {
            Array::destroy(names_ref, alloc);
            throw;
        }
        // One attrib set for each column
        ref_type attribs_ref = ArrayString::create_array(size, alloc); // Throws
        try {
            int_fast64_t v = attribs_ref; // FIXME: Dangerous case: unsigned -> signed
            spec_set.add(v); // Throws
        }
        catch (...) {
            Array::destroy(attribs_ref, alloc);
            throw;
        }
    }
    catch (...) {
        spec_set.destroy();
        throw;
    }
    return spec_set.get_ref();
}


inline Spec::Spec(SubspecRef r) TIGHTDB_NOEXCEPT:
    m_top(r.m_parent->get_alloc()), m_spec(r.m_parent->get_alloc()),
    m_names(r.m_parent->get_alloc()), m_attr(r.m_parent->get_alloc()),
    m_subspecs(r.m_parent->get_alloc()), m_enumkeys(r.m_parent->get_alloc())
{
    init(r);
}

// Uninitialized Spec (call init() to init)
inline Spec::Spec(Allocator& alloc) TIGHTDB_NOEXCEPT:
    m_top(alloc), m_spec(alloc), m_names(alloc), m_attr(alloc), m_subspecs(alloc),
    m_enumkeys(alloc)
{
}

// Create a new Spec
inline Spec::Spec(Allocator& alloc, ArrayParent* parent, std::size_t ndx_in_parent):
    m_top(alloc), m_spec(alloc), m_names(alloc), m_attr(alloc), m_subspecs(alloc),
    m_enumkeys(alloc)
{
    ref_type ref = create_empty_spec(alloc); // Throws
    init(ref, parent, ndx_in_parent);
}

// Create Spec from ref
inline Spec::Spec(Allocator& alloc, ref_type ref, ArrayParent* parent, std::size_t ndx_in_parent):
    m_top(alloc), m_spec(alloc), m_names(alloc), m_attr(alloc), m_subspecs(alloc),
    m_enumkeys(alloc)
{
    init(ref, parent, ndx_in_parent);
}


inline SubspecRef Spec::get_subtable_spec(std::size_t column_ndx) TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(get_column_type(column_ndx) == type_Table);
    std::size_t subspec_ndx = get_subspec_ndx(column_ndx);
    return SubspecRef(&m_subspecs, subspec_ndx);
}

inline ConstSubspecRef Spec::get_subtable_spec(std::size_t column_ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(column_ndx < get_column_count());
    TIGHTDB_ASSERT(get_column_type(column_ndx) == type_Table);
    std::size_t subspec_ndx = get_subspec_ndx(column_ndx);
    return ConstSubspecRef(&m_subspecs, subspec_ndx);
}

inline SubspecRef Spec::get_subspec_by_ndx(std::size_t subspec_ndx) TIGHTDB_NOEXCEPT
{
    return SubspecRef(&m_subspecs, subspec_ndx);
}

inline void Spec::init(SubspecRef r) TIGHTDB_NOEXCEPT
{
    ref_type ref = r.m_parent->get_as_ref(r.m_ndx_in_parent);
    init(ref, r.m_parent, r.m_ndx_in_parent);
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


inline SubspecRef::SubspecRef(Array* parent, std::size_t ndx_in_parent) TIGHTDB_NOEXCEPT:
    m_parent(parent), m_ndx_in_parent(ndx_in_parent)
{
}

inline SubspecRef::SubspecRef(const_cast_tag, ConstSubspecRef r) TIGHTDB_NOEXCEPT:
    m_parent(const_cast<Array*>(r.m_parent)), m_ndx_in_parent(r.m_ndx_in_parent)
{
}

inline ConstSubspecRef::ConstSubspecRef(const Array* parent, std::size_t ndx_in_parent) TIGHTDB_NOEXCEPT:
        m_parent(parent), m_ndx_in_parent(ndx_in_parent)
{
}

inline ConstSubspecRef::ConstSubspecRef(SubspecRef r) TIGHTDB_NOEXCEPT:
        m_parent(r.m_parent), m_ndx_in_parent(r.m_ndx_in_parent)
{
}


} // namespace tightdb

#endif // TIGHTDB_SPEC_HPP
