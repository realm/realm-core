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
        /// Open in read-only mode. Fail if the file does not already exist.
        mode_ReadOnly,
        /// Open in read/write mode. Create the file if it doesn't exist.
        mode_ReadWrite,
        /// Open in read/write mode. Fail if the file does not already exist.
        mode_ReadWriteNoCreate
    };

    /// Equivalent to calling open(const std::string&, OpenMode) on a
    /// default constructed instance.
    explicit Group(const std::string& file, OpenMode = mode_ReadOnly);

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
    /// By default, the specified file is opened in read-only mode
    /// (mode_ReadOnly). This allows opening a file even when the
    /// caller lacks permission to write to that file. The opened
    /// group may still be modified freely, but the changes cannot be
    /// written back to the same file using the commit() function. An
    /// attempt to do that, will cause an exception to be thrown. When
    /// opening in read-only mode, it is an error if the specified
    /// file does not already exist in the file system.
    ///
    /// Alternatively, the file can be opened in read/write mode
    /// (mode_ReadWrite). This allows use of the commit() function,
    /// but, of course, it also requires that the caller has
    /// permission to write to the specified file. When opening in
    /// read-write mode, an attempt to create the specified file will
    /// be made, if it does not already exist in the file system.
    ///
    /// In any case, if the file already exists, it must contain a
    /// valid TightDB database. In many cases invalidity will be
    /// detected and cause the InvalidDatabase exception to be thrown,
    /// but you should not rely on it.
    ///
    /// Note that changes made to the database via a Group instance
    /// are not automatically committed to the specified file. You
    /// may, however, at any time, explicitly commit your changes by
    /// calling the commit() method, provided that the specified
    /// open-mode is not mode_ReadOnly. Alternatively, you may call
    /// write() to write the entire database to a new file. Writing
    /// the database to a new file does not end, or in any other way
    /// change the association between the Group instance and the file
    /// that was specified in the call to open().
    ///
    /// A file that is passed to Group::open(), may not be modified by
    /// a third party until after the Group object is
    /// destroyed. Behavior is undefined if a file is modified by a
    /// third party while any Group object is associated with it.
    ///
    /// Calling open() on a Group instance that is already in the
    /// attached state has undefined behavior.
    ///
    /// Accessing a TightDB database file through manual construction
    /// of a Group object does not offer any level of thread safety or
    /// transaction safety. When any of those kinds of safety are a
    /// concern, consider using a SharedGroup instead. When accessing
    /// a database file in read/write mode through a manually
    /// constructed Group object, it is entirely the responsibility of
    /// the application that the file is not accessed in any way by a
    /// third party during the life-time of that group object. It is,
    /// on the other hand, safe to concurrently access a database file
    /// by multiple manually created Group objects, as long as all of
    /// them are opened in read-only mode, and there is no other party
    /// that modifies the file concurrently.
    ///
    /// Do not call this function on a group instance that is managed
    /// by a shared group. Doing so will result in undefined behavior.
    ///
    /// \param file File system path to a TightDB database file.
    ///
    /// \param mode Specifying a mode that is not mode_ReadOnly
    /// requires that the specified file can be opened in read/write
    /// mode. In general there is no reason to open a group in
    /// read/write mode unless you want to be able to call
    /// Group::commit().
    ///
    /// \throw File::AccessError If the file could not be opened. If
    /// the reason corresponds to one of the exception types that are
    /// derived from File::AccessError, the derived exception type is
    /// thrown. Note that InvalidDatabase is among these derived
    /// exception types.
    void open(const std::string& file, OpenMode mode = mode_ReadOnly);

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
    /// Do not call this function on a group instance that is managed
    /// by a shared group. Doing so will result in undefined behavior.
    ///
    /// \throw InvalidDatabase If the specified buffer does not appear
    /// to contain a valid database.
    void open(BinaryData, bool take_ownership = true);

    /// A group may be created in the unattached state, and then later
    /// attached to a file with a call to open(). Calling any method
    /// other than open(), and is_attached() on an unattached instance
    /// results in undefined behavior.
    bool is_attached() const TIGHTDB_NOEXCEPT;

    /// Returns true if, and only if the number of tables in this
    /// group is zero.
    bool is_empty() const TIGHTDB_NOEXCEPT;

    /// Returns the number of tables in this group.
    std::size_t size() const;

    StringData get_table_name(std::size_t table_ndx) const;
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

    /// Commit changes to the attached file. This requires that the
    /// attached file is opened in read/write mode.
    ///
    /// Calling this function on an unattached group, a free-standing
    /// group, a group whose attached file is opened in read-only
    /// mode, a group that is attached to a memory buffer, or a group
    /// that is managed by a shared group, is an error and will result
    /// in undefined behavior.
    ///
    /// Table accesors will remain valid across the commit. Note that
    /// this is not the case when working with proper transactions.
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
    void enable_mem_diagnostics(bool enable = true) { m_alloc.enable_debug(enable); }
    void to_dot(std::ostream&) const;
    void to_dot() const; // For GDB
    void to_dot(const char* file_path) const;
    void zero_free_space(std::size_t file_size, std::size_t readlock_version);
#endif

private:
    SlabAlloc m_alloc;
    Array m_top;
    Array m_tables;
    ArrayString m_table_names;
    Array m_free_positions;
    Array m_free_lengths;
    Array m_free_versions;
    mutable Array m_cached_tables;
    const bool m_is_shared;
    std::size_t m_readlock_version;

    struct shared_tag {};
    Group(shared_tag) TIGHTDB_NOEXCEPT;

    // FIXME: Implement a proper copy constructor (fairly trivial).
    Group(const Group&); // Disable copying

    void init_array_parents();
    void invalidate();
    void init_shared();

    /// Recursively update refs stored in all cached array
    /// accessors. This includes cached array accessors in any
    /// currently attached table accessors. This ensures that the
    /// group instance itself, as well as any attached table accessor
    /// that exists across Group::commit() will remain valid. This
    /// function is not appropriate for use in conjunction with
    /// commits via shared group.
    void update_refs(ref_type top_ref, std::size_t old_baseline);

    void update_from_shared(ref_type new_top_ref, std::size_t new_file_size);

    void update_child_ref(std::size_t subtable_ndx, ref_type new_ref) TIGHTDB_OVERRIDE
    {
        m_tables.set(subtable_ndx, new_ref);
    }

    void child_destroyed(std::size_t) TIGHTDB_OVERRIDE {} // Ignore

    ref_type get_child_ref(std::size_t subtable_ndx) const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE
    {
        return m_tables.get_as_ref(subtable_ndx);
    }

    // May throw WriteError
    template<class S> std::size_t write_to_stream(S& out) const;

    /// Create a new underlying node structure and attach this
    /// accessor instance to it
    void create();

    /// Attach this accessor instance to a preexisting underlying node
    /// structure. This function also initializes the table accessor
    /// cache.
    void init_from_ref(ref_type top_ref);

    Table* get_table_ptr(StringData name);
    Table* get_table_ptr(StringData name, bool& was_created);
    const Table* get_table_ptr(StringData name) const;
    template<class T> T* get_table_ptr(StringData name);
    template<class T> const T* get_table_ptr(StringData name) const;

    Table* get_table_ptr(std::size_t ndx);
    const Table* get_table_ptr(std::size_t ndx) const;
    Table* create_new_table(StringData name);

    void clear_cache();

    friend class GroupWriter;
    friend class SharedGroup;
    friend class LangBindHelper;

#ifdef TIGHTDB_ENABLE_REPLICATION
    friend class Replication;
    Replication* get_replication() const TIGHTDB_NOEXCEPT { return m_alloc.get_replication(); }
    void set_replication(Replication* r) TIGHTDB_NOEXCEPT { m_alloc.set_replication(r); }
#endif
};





// Implementation

inline Group::Group():
    m_top(m_alloc), m_tables(m_alloc), m_table_names(m_alloc), m_free_positions(m_alloc),
    m_free_lengths(m_alloc), m_free_versions(m_alloc), m_is_shared(false)
{
    init_array_parents();

    // FIXME: The try-catch is required because of the unfortunate
    // fact that Array violates the RAII idiom by allocating memory in
    // the constructor and not freeing it in the destructor. We must
    // find a way to improve Array.
    try {
        create(); // Throws
    }
    catch (...) {
        m_cached_tables.destroy();
        throw;
    }
}

inline Group::Group(const std::string& file, OpenMode mode):
    m_top(m_alloc), m_tables(m_alloc), m_table_names(m_alloc), m_free_positions(m_alloc),
    m_free_lengths(m_alloc), m_free_versions(m_alloc), m_is_shared(false)
{
    init_array_parents();

    // FIXME: The try-catch is required because of the unfortunate
    // fact that Array violates the RAII idiom by allocating memory in
    // the constructor and not freeing it in the destructor. We must
    // find a way to improve Array.
    try {
        open(file, mode); // Throws
    }
    catch (...) {
        m_cached_tables.destroy();
        throw;
    }
}

inline Group::Group(BinaryData buffer, bool take_ownership):
    m_top(m_alloc), m_tables(m_alloc), m_table_names(m_alloc), m_free_positions(m_alloc),
    m_free_lengths(m_alloc), m_free_versions(m_alloc), m_is_shared(false)
{
    init_array_parents();

    // FIXME: The try-catch is required because of the unfortunate
    // fact that Array violates the RAII idiom by allocating memory in
    // the constructor and not freeing it in the destructor. We must
    // find a way to improve Array.
    try {
        open(buffer, take_ownership); // Throws
    }
    catch (...) {
        m_cached_tables.destroy();
        throw;
    }
}

inline Group::Group(unattached_tag) TIGHTDB_NOEXCEPT:
    m_top(m_alloc), m_tables(m_alloc), m_table_names(m_alloc), m_free_positions(m_alloc),
    m_free_lengths(m_alloc), m_free_versions(m_alloc), m_is_shared(false)
{
    init_array_parents();
}

inline Group::Group(shared_tag) TIGHTDB_NOEXCEPT:
    m_top(m_alloc), m_tables(m_alloc), m_table_names(m_alloc), m_free_positions(m_alloc),
    m_free_lengths(m_alloc), m_free_versions(m_alloc), m_is_shared(true)
{
    init_array_parents();
}

inline void Group::init_array_parents()
{
    m_table_names.set_parent(&m_top, 0);
    m_tables.set_parent(&m_top, 1);
    m_free_positions.set_parent(&m_top, 2);
    m_free_lengths.set_parent(&m_top, 3);
    m_free_versions.set_parent(&m_top, 4);
}

inline bool Group::is_attached() const TIGHTDB_NOEXCEPT
{
    return m_alloc.is_attached();
}

inline bool Group::is_empty() const TIGHTDB_NOEXCEPT
{
    if (!m_top.is_attached()) return true;
    return m_table_names.is_empty();
}

inline std::size_t Group::size() const
{
    if (!m_top.is_attached()) return 0;
    return m_table_names.size();
}

inline StringData Group::get_table_name(std::size_t table_ndx) const
{
    TIGHTDB_ASSERT(m_top.is_attached());
    TIGHTDB_ASSERT(table_ndx < m_table_names.size());
    return m_table_names.get(table_ndx);
}

inline const Table* Group::get_table_ptr(std::size_t ndx) const
{
    return const_cast<Group*>(this)->get_table_ptr(ndx);
}

inline bool Group::has_table(StringData name) const
{
    if (!m_top.is_attached()) return false;
    std::size_t i = m_table_names.find_first(name);
    return i != std::size_t(-1);
}

template<class T> inline bool Group::has_table(StringData name) const
{
    if (!m_top.is_attached()) return false;
    std::size_t i = m_table_names.find_first(name);
    if (i == std::size_t(-1)) return false;
    const Table* table = get_table_ptr(i);
    return T::matches_dynamic_spec(&table->get_spec());
}

inline Table* Group::get_table_ptr(StringData name)
{
    TIGHTDB_ASSERT(m_top.is_attached());
    std::size_t ndx = m_table_names.find_first(name);
    if (ndx != std::size_t(-1)) {
        // Get table from cache
        return get_table_ptr(ndx);
    }

    return create_new_table(name);
}

inline Table* Group::get_table_ptr(StringData name, bool& was_created)
{
    TIGHTDB_ASSERT(m_top.is_attached());
    std::size_t ndx = m_table_names.find_first(name);
    if (ndx != std::size_t(-1)) {
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

    TIGHTDB_ASSERT(m_top.is_attached());
    std::size_t ndx = m_table_names.find_first(name);
    if (ndx != std::size_t(-1)) {
        // Get table from cache
        return static_cast<T*>(get_table_ptr(ndx));
    }

    T* table = static_cast<T*>(create_new_table(name));
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

template<class S> std::size_t Group::write_to_stream(S& out) const
{
    // Space for file header
    out.write(SlabAlloc::default_header, sizeof SlabAlloc::default_header);

    // When serializing to disk we dont want
    // to include free space tracking as serialized
    // files are written without any free space.
    Array top(Array::type_HasRefs, 0, 0, const_cast<SlabAlloc&>(m_alloc)); // FIXME: Another aspect of the poor constness behavior in Array class. What can we do?
    top.add(m_top.get(0));
    top.add(m_top.get(1));

    // Recursively write all arrays
    const uint64_t top_pos = top.write(out);
    const std::size_t byte_size = out.getpos();

    // Write top ref
    // (since we initially set the last bit in the file header to
    //  zero, it is the first ref block that is valid)
    out.seek(0);
    out.write(reinterpret_cast<const char*>(&top_pos), 8);

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
    top.set(0, 0); // reset to avoid recursive delete
    top.set(1, 0); // reset to avoid recursive delete
    top.destroy();

    // return bytes written
    return byte_size;
}

template<class S>
void Group::to_json(S& out) const
{
    if (!m_top.is_attached()) {
        out << "{}";
        return;
    }

    out << "{";

    for (std::size_t i = 0; i < m_tables.size(); ++i) {
        StringData name = m_table_names.get(i);
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
    std::size_t n = m_cached_tables.size();
    for (std::size_t i = 0; i < n; ++i) {
        if (Table* t = reinterpret_cast<Table*>(m_cached_tables.get(i))) {
            t->invalidate();
            t->unbind_ref();
        }
    }
    m_cached_tables.clear();
}


} // namespace tightdb

#endif // TIGHTDB_GROUP_HPP
