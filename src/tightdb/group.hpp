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

#ifndef TIGHTDB_GROUP_HPP
#define TIGHTDB_GROUP_HPP

#include <string>

#include <tightdb/exceptions.hpp>
#include <tightdb/table.hpp>
#include <tightdb/table_basic_fwd.hpp>
#include <tightdb/alloc_slab.hpp>

namespace tightdb {


// Pre-declarations
class SharedGroup;


class Group: private Table::Parent {
public:
    /// Construct a free-standing group. This group instance will be
    /// in the attached state, but neither associated with a file, nor
    /// with an external memory buffer.
    Group();

    enum OpenMode {
        mode_Normal,   ///< Open in read/write mode, create the file if it does not already exist.
        mode_ReadOnly, ///< Open in read-only mode, fail if the file does not already exist.
        mode_NoCreate  ///< Open in read/write mode, fail if the file does not already exist.
    };

    /// Equivalent to calling open(const std::string&, OpenMode) on a
    /// default constructed instance.
    explicit Group(const std::string& file, OpenMode = mode_Normal);

    /// Equivalent to calling open(BinaryData, bool) on a default
    /// constructed instance.
    explicit Group(BinaryData, bool take_ownership = true);

    struct unattached_tag {};

    /// Create a Group instance in its unattached state. It may then
    /// be attached to a database file later by calling one of the
    /// open() methods. You may test whether this instance is
    /// currently in its attached state by calling
    /// is_attached(). Calling any other method (except the
    /// destructor) while in the unattached state has undefined
    /// behavior.
    Group(unattached_tag) TIGHTDB_NOEXCEPT;

    ~Group();

    /// Attach this Group instance to the specified database file.
    ///
    /// If the specified file exists in the file system, it must
    /// contain a valid TightDB database. If the file does not exist,
    /// it will be created (unless mode_NoCreate is specified). While
    /// a group constructed this way, can be used to access and
    /// manipulate a TightDB database, it is generally better to use a
    /// SharedGroup instance along with proper transactions.
    ///
    /// Changes made to the database via a Group instance are not
    /// automatically committed to the specified file. You may,
    /// however, at any time, explicitely commit your changes by
    /// calling the commit() method. Alternatively you may call
    /// write() to write the entire database to a new file.
    ///
    /// A file that is opened this way, may not be modified until
    /// after the Group object is destroyed. Behaviour is undefined if
    /// a file is modified while any Group instance refers to it.
    ///
    /// Calling open() on a Group instance that is already in the
    /// attached state has undefined behavior.
    ///
    /// \param file Filesystem path to a TightDB database file.
    ///
    /// \throw File::AccessError If the file could not be opened. If
    /// the reason corresponds to one of the exception types that are
    /// derived from File::AccessError, the derived exception type is
    /// thrown. Note that InvalidDatabase is among these derived
    /// exception types.
    void open(const std::string& file, OpenMode = mode_Normal);

    /// Attach this Group instance to the specified memory buffer.
    ///
    /// This is similar to constructing a group from a file except
    /// that in this case the database is assumed to be stored in the
    /// specified memory buffer.
    ///
    /// If \a take_ownership is <tt>true</tt>, you pass the ownership
    /// of the specified buffer to the group. In this case the buffer
    /// will eventually be freed using std::free(), so the buffer you
    /// pass, must have been allocated using std::malloc().
    ///
    /// On the other hand, if \a take_ownership is set to
    /// <tt>false</tt>, it is your responsibility to keep the memory
    /// buffer alive during the lifetime of the group, and in case the
    /// buffer needs to be deallocated afterwards, that is your
    /// responsibility too.
    ///
    /// Calling open() on a Group instance that is already in the
    /// attached state has undefined behavior.
    ///
    /// \throw InvalidDatabase If the specified buffer does not appear
    /// to contain a valid database.
    void open(BinaryData, bool take_ownership = true);

    /// A group may be created in the unattached state, and then later
    /// attached to a file with a call to open(). Calling any method
    /// other than open(), is_attached(), and ~Group() on an
    /// unattached instance results in undefined behavior.
    bool is_attached() const TIGHTDB_NOEXCEPT;

    /// Returns true if, and only if the number of tables in this
    /// group is zero.
    bool is_empty() const TIGHTDB_NOEXCEPT;

    /// Returns the number of tables in this group.
    size_t size() const;

    StringData get_table_name(size_t table_ndx) const;
    bool has_table(StringData name) const;

    /// Check whether this group has a table with the specified name
    /// and type.
    template<class T> bool has_table(StringData name) const;

    TableRef      get_table(StringData name);
    ConstTableRef get_table(StringData name) const;
    template<class T> typename T::Ref      get_table(StringData name);
    template<class T> typename T::ConstRef get_table(StringData name) const;

    // Serialization

    /// Write this database to a new file. It is an error to specify a
    /// file that already exists. This is to protect against
    /// overwriting a database file that is currently open, which
    /// would cause undefined behaviour.
    ///
    /// \param file A filesystem path.
    ///
    /// \throw File::AccessError If the file could not be opened. If
    /// the reason corresponds to one of the exception types that are
    /// derived from File::AccessError, the derived exception type is
    /// thrown. In particular, File::Exists will be thrown if the file
    /// exists already.
    void write(const std::string& file) const;

    /// Write this database to a memory buffer.
    ///
    /// Ownership of the returned buffer is transferred to the
    /// caller. The memory will have been allocated using
    /// std::malloc().
    BinaryData write_to_mem() const;

    // FIXME: What does this one do? Exactly how is it different from
    // calling write()? There is no documentation to be found anywhere
    // and it looks like it leaves the group in an invalid state. You
    // should probably not use it.
    void commit();

    // Conversion
    template<class S> void to_json(S& out) const;
    void to_string(std::ostream& out) const;

    /// Compare two groups for equality. Two groups are equal if, and
    /// only if, they contain the same tables in the same order, that
    /// is, for each table T at index I in one of the groups, there is
    /// a table at index I in the other group that is equal to T.
    bool operator==(const Group&) const;

    /// Compare two groups for inequality. See operator==().
    bool operator!=(const Group& g) const { return !(*this == g); }

#ifdef TIGHTDB_DEBUG
    void Verify() const; // Uncapitalized 'verify' cannot be used due to conflict with macro in Obj-C
    void print() const;
    void print_free() const;
    MemStats stats();
    void enable_mem_diagnostics(bool enable=true) {m_alloc.EnableDebug(enable);}
    void to_dot(std::ostream& out) const;
    void to_dot() const; // For GDB
    void zero_free_space(size_t file_size, size_t readlock_version);
#endif // TIGHTDB_DEBUG

protected:
    friend class GroupWriter;
    friend class SharedGroup;

    void create_from_file(const std::string& filename, OpenMode, bool do_init);

    void invalidate();
    bool in_initial_state() const;
    void init_shared();
    size_t commit(size_t current_version, size_t readlock_version, bool doPersist);
    void rollback();

    SlabAlloc& get_allocator() {return m_alloc;}
    Array& get_top_array() {return m_top;}

    // Recursively update all internal refs after commit
    void update_refs(size_t top_ref);

    void update_from_shared(size_t top_ref, size_t len);
    void reset_to_new();

    void update_child_ref(size_t subtable_ndx, size_t new_ref) TIGHTDB_OVERRIDE
    {
        m_tables.Set(subtable_ndx, new_ref);
    }

    void child_destroyed(std::size_t) TIGHTDB_OVERRIDE {} // Ignore

    size_t get_child_ref(size_t subtable_ndx) const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE
    {
        return m_tables.GetAsRef(subtable_ndx);
    }

    void create(); // FIXME: Could be private
    void create_from_ref(size_t top_ref);

    // May throw WriteError
    template<class S> size_t write_to_stream(S& out) const;

    // Member variables
    SlabAlloc m_alloc;
    Array m_top;
    Array m_tables;
    ArrayString m_tableNames;
    Array m_freePositions;
    Array m_freeLengths;
    Array m_freeVersions;
    mutable Array m_cachedtables;
    const bool m_is_shared;
    size_t m_readlock_version;

private:
    struct shared_tag {};
    Group(shared_tag) TIGHTDB_NOEXCEPT;

    // FIXME: Implement a proper copy constructor (fairly trivial).
    Group(const Group&); // Disable copying

    Table* get_table_ptr(StringData name);
    Table* get_table_ptr(StringData name, bool& was_created);
    const Table* get_table_ptr(StringData name) const;
    template<class T> T* get_table_ptr(StringData name);
    template<class T> const T* get_table_ptr(StringData name) const;

    Table* get_table_ptr(size_t ndx);
    const Table* get_table_ptr(size_t ndx) const;
    Table* create_new_table(StringData name);

    void clear_cache();

    friend class LangBindHelper;

#ifdef TIGHTDB_ENABLE_REPLICATION
    friend class Replication;
    Replication* get_replication() const TIGHTDB_NOEXCEPT { return m_alloc.get_replication(); }
    void set_replication(Replication* r) TIGHTDB_NOEXCEPT { m_alloc.set_replication(r); }
#endif
};





// Implementation

// FIXME: Successfully constructed arrays are leaked if one array
// constructor fails. The core problem here and in many other places
// is that ~Array() does not deallocate memory. More generally, the
// fact that the Array class violates the RAII idiom in multiple ways,
// coupled with the fact that we still use it in ways that involves
// ownership of the underlying memory, causes lots of problems with
// robustness both here and in other places.
inline Group::Group():
    m_top(Array::coldef_HasRefs, NULL, 0, m_alloc), m_tables(m_alloc), m_tableNames(NULL, 0, m_alloc),
    m_freePositions(Array::coldef_Normal, NULL, 0, m_alloc),
    m_freeLengths(Array::coldef_Normal, NULL, 0, m_alloc),
    m_freeVersions(Array::coldef_Normal, NULL, 0, m_alloc), m_is_shared(false)
{
    // FIXME: Arrays are leaked when create() throws
    create();
}

inline Group::Group(const std::string& file, OpenMode mode):
    m_top(m_alloc), m_tables(m_alloc), m_tableNames(m_alloc), m_freePositions(m_alloc),
    m_freeLengths(m_alloc), m_freeVersions(m_alloc), m_is_shared(false)
{
    // FIXME: The try-cache is required because of the unfortunate
    // fact that Array violates the RAII idiom while still being used
    // to own memory. We must find a way to improve Array.
    try {
        open(file, mode); // Throws
    }
    catch (...) {
        m_cachedtables.Destroy();
        throw;
    }
}

inline Group::Group(BinaryData buffer, bool take_ownership):
    m_top(m_alloc), m_tables(m_alloc), m_tableNames(m_alloc), m_freePositions(m_alloc),
    m_freeLengths(m_alloc), m_freeVersions(m_alloc), m_is_shared(false)
{
    // FIXME: The try-cache is required because of the unfortunate
    // fact that Array violates the RAII idiom while still being used
    // to own memory. We must find a way to improve Array.
    try {
        open(buffer, take_ownership); // Throws
    }
    catch (...) {
        m_cachedtables.Destroy();
        throw;
    }
}

inline Group::Group(unattached_tag) TIGHTDB_NOEXCEPT:
    m_top(m_alloc), m_tables(m_alloc), m_tableNames(m_alloc), m_freePositions(m_alloc),
    m_freeLengths(m_alloc), m_freeVersions(m_alloc), m_is_shared(false) {}

inline Group::Group(shared_tag) TIGHTDB_NOEXCEPT:
    m_top(m_alloc), m_tables(m_alloc), m_tableNames(m_alloc), m_freePositions(m_alloc),
    m_freeLengths(m_alloc), m_freeVersions(m_alloc), m_is_shared(true) {}

inline void Group::open(const std::string& file, OpenMode mode)
{
    TIGHTDB_ASSERT(!is_attached());
    create_from_file(file, mode, true);
}

inline void Group::open(BinaryData buffer, bool take_ownership)
{
    TIGHTDB_ASSERT(!is_attached());
    TIGHTDB_ASSERT(buffer.data());
    m_alloc.attach_buffer(buffer.data(), buffer.size(), take_ownership);
    create_from_ref(m_alloc.GetTopRef()); // FIXME: Throws and leaves the Group in peril
}

inline bool Group::is_attached() const TIGHTDB_NOEXCEPT
{
    return m_alloc.is_attached();
}

inline bool Group::is_empty() const TIGHTDB_NOEXCEPT
{
    if (!m_top.IsValid()) return true;
    return m_tableNames.is_empty();
}

inline bool Group::in_initial_state() const
{
    return !m_top.IsValid();
}

inline std::size_t Group::size() const
{
    if (!m_top.IsValid()) return 0;
    return m_tableNames.size();
}

inline StringData Group::get_table_name(std::size_t table_ndx) const
{
    TIGHTDB_ASSERT(m_top.IsValid());
    TIGHTDB_ASSERT(table_ndx < m_tableNames.size());
    return m_tableNames.get(table_ndx);
}

inline const Table* Group::get_table_ptr(std::size_t ndx) const
{
    return const_cast<Group*>(this)->get_table_ptr(ndx);
}

inline bool Group::has_table(StringData name) const
{
    if (!m_top.IsValid()) return false;
    const size_t i = m_tableNames.find_first(name);
    return i != size_t(-1);
}

template<class T> inline bool Group::has_table(StringData name) const
{
    if (!m_top.IsValid()) return false;
    const size_t i = m_tableNames.find_first(name);
    if (i == size_t(-1)) return false;
    const Table* const table = get_table_ptr(i);
    return T::matches_dynamic_spec(&table->get_spec());
}

inline Table* Group::get_table_ptr(StringData name)
{
    TIGHTDB_ASSERT(m_top.IsValid());
    const size_t ndx = m_tableNames.find_first(name);
    if (ndx != size_t(-1)) {
        // Get table from cache
        return get_table_ptr(ndx);
    }

    return create_new_table(name);
}

inline Table* Group::get_table_ptr(StringData name, bool& was_created)
{
    TIGHTDB_ASSERT(m_top.IsValid());
    const size_t ndx = m_tableNames.find_first(name);
    if (ndx != size_t(-1)) {
        was_created = false;
        // Get table from cache
        return get_table_ptr(ndx);
    }

    was_created = true;
    return create_new_table(name);
}

inline const Table* Group::get_table_ptr(StringData name) const
{
    TIGHTDB_ASSERT(has_table(name));
    return const_cast<Group*>(this)->get_table_ptr(name);
}

template<class T> inline T* Group::get_table_ptr(StringData name)
{
    TIGHTDB_STATIC_ASSERT(IsBasicTable<T>::value, "Invalid table type");
    TIGHTDB_ASSERT(!has_table(name) || has_table<T>(name));

    TIGHTDB_ASSERT(m_top.IsValid());
    const size_t ndx = m_tableNames.find_first(name);
    if (ndx != size_t(-1)) {
        // Get table from cache
        return static_cast<T*>(get_table_ptr(ndx));
    }

    T* const table = static_cast<T*>(create_new_table(name));
    table->set_dynamic_spec(); // FIXME: May fail
    return table;
}

template<class T> inline const T* Group::get_table_ptr(StringData name) const
{
    TIGHTDB_ASSERT(has_table(name));
    return const_cast<Group*>(this)->get_table_ptr<T>(name);
}

inline TableRef Group::get_table(StringData name)
{
    return get_table_ptr(name)->get_table_ref();
}

inline ConstTableRef Group::get_table(StringData name) const
{
    return get_table_ptr(name)->get_table_ref();
}

template<class T> inline typename T::Ref Group::get_table(StringData name)
{
    return get_table_ptr<T>(name)->get_table_ref();
}

template<class T> inline typename T::ConstRef Group::get_table(StringData name) const
{
    return get_table_ptr<T>(name)->get_table_ref();
}

inline void Group::commit()
{
    commit(-1, -1, true);
}

template<class S> size_t Group::write_to_stream(S& out) const
{
    // Space for file header
    out.write(SlabAlloc::default_header, sizeof SlabAlloc::default_header);

    // When serializing to disk we dont want
    // to include free space tracking as serialized
    // files are written without any free space.
    Array top(Array::coldef_HasRefs, NULL, 0, const_cast<SlabAlloc&>(m_alloc)); // FIXME: Another aspect of the poor constness behavior in Array class. What can we do?
    top.add(m_top.Get(0));
    top.add(m_top.Get(1));

    // Recursively write all arrays
    const uint64_t topPos = top.Write(out); // FIXME: Why does this not return char*?
    const size_t byte_size = out.getpos();

    // Write top ref
    // (since we initially set the last bit in the file header to
    //  zero, it is the first ref block that is valid)
    out.seek(0);
    out.write(reinterpret_cast<const char*>(&topPos), 8);

    // FIXME: To be 100% robust with respect to being able to detect
    // afterwards whether the file was completely written, we would
    // have to put a sync() here and then proceed to write the T-DB
    // bytes into the header. Also, if it is possible that the file is
    // left with random contents if the host looses power before our
    // call to sync() has completed, then we must initially resize the
    // file to header_len - 1, fill with zeroes, and call sync(). If
    // the file is then found later with size header_len - 1, it will
    // be considered invalid.

    // Clean up temporary top
    top.Set(0, 0); // reset to avoid recursive delete
    top.Set(1, 0); // reset to avoid recursive delete
    top.Destroy();

    // return bytes written
    return byte_size;
}

template<class S>
void Group::to_json(S& out) const
{
    if (!m_top.IsValid()) {
        out << "{}";
        return;
    }

    out << "{";

    for (size_t i = 0; i < m_tables.size(); ++i) {
        StringData name = m_tableNames.get(i);
        const Table* table = get_table_ptr(i);

        if (i) out << ",";
        out << "\"" << name << "\"";
        out << ":";
        table->to_json(out);
    }

    out << "}";
}


inline void Group::clear_cache()
{
    const size_t count = m_cachedtables.size();
    for (size_t i = 0; i < count; ++i) {
        if (Table* const t = reinterpret_cast<Table*>(m_cachedtables.Get(i))) {
            t->invalidate();
            t->unbind_ref();
        }
    }
    m_cachedtables.Clear();
}


} // namespace tightdb

#endif // TIGHTDB_GROUP_HPP
