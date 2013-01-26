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
#ifndef TIGHTDB_REPLICATION_HPP
#define TIGHTDB_REPLICATION_HPP

#include <cstddef>
#include <cstring>
#include <new>
#include <algorithm>
#include <limits>

#include <tightdb/meta.hpp>
#include <tightdb/tuple.hpp>
#include <tightdb/error.hpp>
#include <tightdb/mixed.hpp>

namespace tightdb {


class Spec;
class Table;
class Group;


// FIXME: Be careful about the possibility of one modification functions being called by another where both do transaction logging.

// FIXME: How to handle unordered tables. Will it introduce nondeterminism in actual order such that indices cannot be considered equal across replication instances?

// FIXME: The current table/subtable selection scheme assumes that a TableRef of a subtable is not accessed after any modification of one of its ancestor tables.

// FIXME: Checking on same Table* requires that ~Table checks and nullifies on match. Another option would be to store m_selected_table as a TableRef. Yet another option would be to assign unique identifiers to each Table instance vial Allocator. Yet another option would be to explicitely invalidate subtables recursively when parent is modified.

// FIXME: Consider opportunistic transmission of the transaction log while it is being built.



/// Manage replication for a client or for the local coordinator.
///
/// When replication is enabled, a directory named "/var/lib/tightdb/"
/// must exist, and the user running the client and the user running
/// the local coordinator must both have write permission to that
/// directory.
///
/// Replication is enabled by creating an instance of SharedGroup and
/// passing SharedGroup::replication_tag() to the constructor.
///
/// Replication also requires that a local coordinator process is
/// running on the same host as the client. At most one local
/// coordinator process may run on each host.
struct Replication {
    /// Create a Replication instance in its unattached state. To
    /// attach it to a replication coordination buffer, call attach().
    /// You may test whether this instance is currently in its
    /// attached state by calling is_attach(). Calling any other
    /// method while in the unattached state has undefined behaviour.
    Replication() TIGHTDB_NOEXCEPT;

    ~Replication();

    static std::string get_path_to_database_file() { return "/var/lib/tightdb/replication.db"; }

    /// Attach this instance to the replication coordination buffer
    /// associated with the specified database file.
    ///
    /// \param The file system path to the database file. This is used
    /// only to derive a path for a replication specific shared memory
    /// file object. Its path is derived by appending ".repl" to the
    /// specified path.
    ///
    /// \param map_transact_log_buf When true, all of the replication
    /// specific shared memory is mapped. When false, the transaction
    /// log buffer is not mapped. When used in conjunction with an
    /// instance of SharedGroup, this must always be true.
    void attach(const std::string& path_to_database_file = "", bool map_transact_log_buf = true);

    bool is_attached() const TIGHTDB_NOEXCEPT;

    /// Interrupt any blocking call to a function in this class. This
    /// function may be called asyncronously from any thread, but it
    /// may not be called from a system signal handler.
    ///
    /// A function in this class may block if it is explicitely stated
    /// in its documention, or if it returns \c error_code and its
    /// documentation does not explicitely state that it does not
    /// block.
    ///
    /// After any function has returned with an interruption
    /// indication, the only functions that may safely be called are
    /// rollback_write_transact() and the destructor. If a client,
    /// after having received an interruption indication, calls
    /// rollback_write_transact() and then clear_interrupt(), it may
    /// then resume normal operation on this object.
    ///
    /// This function can never fail.
    void interrupt();

    /// Acquire permision to start a new 'write' transaction. This
    /// function must be called by a client before it requests a
    /// 'write' transaction. This ensures that the local shared
    /// database is up-to-date. During the transaction, all
    /// modifications must be posted to this Replication instance as
    /// calls to set_value() and friends. After the completion of the
    /// transaction, the client must call either
    /// commit_write_transact() or rollback_write_transact().
    ///
    /// This function returns ERROR_INTERRUPTED if it was interrupted
    /// by an asynchronous call to shutdown().
    error_code begin_write_transact();

    /// Called by a client to commit the accumulated transaction
    /// log. The transaction log may not be committed if any of the
    /// functions that submit data to it, have failed or been
    /// interrupted. This operation will block until the local
    /// coordinator reports that the transaction log has been dealt
    /// with in a manner that makes the transaction persistent. This
    /// operation may be interrupted by an asynchronous call to
    /// interrupt().
    ///
    /// \return True on success and false if interrupted.
    ///
    /// FIXME: In general the transaction will be considered complete
    /// even if this operation is interrupted. Is that ok?
    bool commit_write_transact();

    /// Called by a client to discard the accumulated transaction
    /// log. This function must be called if a write transaction was
    /// successfully initiated, but one of the functions that submit
    /// data to the transaction log has failed or has been
    /// interrupted. It must also be called after a failed or
    /// interrupted call to commit_write_transact(). This function can
    /// never fail.
    void rollback_write_transact();

    /// May be called by a client to reset this replication instance
    /// after an interrupted transaction. It is not an error to call
    /// this function in a situation where no interruption has
    /// occured. This function can never fail.
    void clear_interrupt();

    /// Called by the local coordinator to wait for the next client
    /// that wants to start a new 'write' transaction.
    ///
    /// \return False if the operation was aborted by an asynchronous
    /// call to shutdown().
    bool wait_for_write_request();

    typedef int db_version_type;

    struct TransactLog {
        db_version_type m_db_version;
        size_t m_offset1, m_size1, m_offset2, m_size2;
    };

    /// Called by the local coordinator to grant permission for one
    /// client to start a new 'write' transaction. This function also
    /// waits for the client to signal completion of the write
    /// transaction.
    ///
    /// At entry, \c transact_log.db_version must be set to the
    /// version of the database as it will be after completion of the
    /// 'write' transaction that is granted. At exit, the offsets and
    /// the sizes in \a transact_log will have been properly
    /// initialized.
    ///
    /// \return False if the operation was aborted by an asynchronous
    /// call to shutdown().
    bool grant_write_access_and_wait_for_completion(TransactLog& transact_log);

    /// Called by the local coordinator to gain access to the memory
    /// that corresponds to the specified transaction log. The memory
    /// mapping of the underlying file is expanded as necessary. This
    /// function cannot block, and can therefore not be interrupted.
    error_code map_transact_log(const TransactLog&, const char** addr1, const char** addr2);

    /// Called by the local coordinator to signal to clients that a
    /// new version of the database has been persisted. Presumably,
    /// one of the clients is blocked in a call to
    /// commit_write_transact() waiting for this signal. This function
    /// can never fail.
    void update_persisted_db_version(db_version_type);

    /// Called by the local coordinator to release space in the
    /// transaction log buffer corresponding to a previously completed
    /// transaction log that is no longer needed. This function may be
    /// called asynchronously with respect to wait_for_write_request()
    /// and grant_write_access_and_wait_for_completion(). This
    /// function can never fail.
    void transact_log_consumed(size_t size);


    // Transaction log instruction encoding:
    //
    //   N  New top level table
    //   T  Select table
    //   S  Select spec for currently selected table
    //   A  Add column to selected spec
    //   s  Set value
    //   i  Insert value
    //   c  Row insert complete
    //   I  Insert empty rows
    //   R  Remove row
    //   a  Add int to column
    //   x  Add index to column
    //   C  Clear table
    //   Z  Optimize table

    struct subtable_tag {};

    error_code new_top_level_table(const char* name);
    error_code add_column(const Table*, const Spec*, ColumnType, const char* name);

    template<class T>
    error_code set_value(const Table*, std::size_t column_ndx, std::size_t ndx, T value);
    error_code set_value(const Table*, std::size_t column_ndx, std::size_t ndx, BinaryData value);
    error_code set_value(const Table*, std::size_t column_ndx, std::size_t ndx, const Mixed& value);
    error_code set_value(const Table*, std::size_t column_ndx, std::size_t ndx, subtable_tag);

    template<class T>
    error_code insert_value(const Table*, std::size_t column_ndx, std::size_t ndx, T value);
    error_code insert_value(const Table*, std::size_t column_ndx, std::size_t ndx, BinaryData value);
    error_code insert_value(const Table*, std::size_t column_ndx, std::size_t ndx, const Mixed& value);
    error_code insert_value(const Table*, std::size_t column_ndx, std::size_t ndx, subtable_tag);

    error_code row_insert_complete(const Table*);
    error_code insert_empty_rows(const Table*, std::size_t row_ndx, std::size_t num_rows);
    error_code remove_row(const Table*, std::size_t row_ndx);
    error_code add_int_to_column(const Table*, std::size_t column_ndx, int64_t value);
    error_code add_index_to_column(const Table*, std::size_t column_ndx);
    error_code clear_table(const Table*);
    error_code optimize_table(const Table*);

    void on_table_destroyed(const Table* t) { if (m_selected_table == t) m_selected_table = 0; }
    void on_spec_destroyed(const Spec* s)   { if (m_selected_spec  == s) m_selected_spec  = 0; }


    struct InputStream {
        /// \return The number of extracted bytes. This will always be
        /// less than or equal to \a size. A value of zero indicates
        /// end-of-input unless \a size was zero.
        virtual std::size_t read(char* buffer, std::size_t size) = 0;

        virtual ~InputStream() {}
    };

    /// Called by the local coordinator to apply a transaction log
    /// received from another local coordinator.
    ///
    /// \param apply_log If specified, and the library was compiled in
    /// debug mode, then a line describing each individual operation
    /// is writted to the specified stream.
    ///
    /// \return ERROR_IO if the transaction log could not be
    /// successfully parsed, or ended prematurely.
#ifdef TIGHTDB_DEBUG
    static error_code apply_transact_log(InputStream& transact_log, Group& target,
                                         std::ostream* apply_log = 0);
#else
    static error_code apply_transact_log(InputStream& transact_log, Group& target);
#endif

private:
    struct SharedState;
    struct TransactLogApplier;

    int m_fd; // Memory mapped file descriptor
    SharedState* m_shared_state;
    // Invariant: m_shared_state_size <= std::numeric_limits<std::ptrdiff_t>::max()
    std::size_t m_shared_state_mapped_size;
    bool m_interrupt; // Protected by m_shared_state->m_mutex

    // Set by begin_write_transact() to the new database version
    // created by the initiated 'write' transaction.
    db_version_type m_write_transact_db_version;

    // These two delimit a contiguous region of free space in the
    // buffer following the last written data. It may be empty.
    char* m_transact_log_free_begin;
    char* m_transact_log_free_end;

    template<class T> struct Buffer {
        T* m_data;
        std::size_t m_size;
        T& operator[](std::size_t i) { return m_data[i]; }
        const T& operator[](std::size_t i) const { return m_data[i]; }
        Buffer(): m_data(0), m_size(0) {}
        ~Buffer() { delete[] m_data; }
        bool set_size(std::size_t);
    };
    Buffer<std::size_t> m_subtab_path_buf;

    const Table* m_selected_table;
    const Spec*  m_selected_spec;

    /// This function must be called before using an instance of this
    /// class. It must not be called more than once. It is legal to
    /// destroy an instance of this class without this function having
    /// been called.
    error_code init(std::string path_to_database_file, bool map_transact_log_buf);

    /// \param n Must be small (probably not greater than 1024)
    error_code transact_log_reserve(char** buf, int n);

    void transact_log_advance(char* ptr);

    error_code transact_log_append(const char* data, std::size_t size);

    /// \param n Must be small (probably not greater than 1024)
    error_code transact_log_reserve_contig(std::size_t n);

    error_code transact_log_append_overflow(const char* data, std::size_t size);

    /// Must be called only by a client that has 'write' access and
    /// only when there are no transaction logs in the transaction log
    /// buffer beyond the one being created.
    error_code transact_log_expand(std::size_t free, bool contig);

    error_code remap_file(std::size_t size);

    error_code check_table(const Table*);
    error_code select_table(const Table*);

    error_code check_spec(const Table*, const Spec*);
    error_code select_spec(const Table*, const Spec*);

    error_code string_cmd(char cmd, std::size_t column_ndx, std::size_t ndx,
                          const char* data,std::size_t size);

    error_code mixed_cmd(char cmd, std::size_t column_ndx, std::size_t ndx, const Mixed& value);

    template<class L> error_code simple_cmd(char cmd, const Tuple<L>& integers);

    template<class T> struct EncodeInt { void operator()(T value, char** ptr); };

    template<class T> static char* encode_int(char* ptr, T value);

    // Make sure this is in agreement with the actual integer encoding scheme
    static const int max_enc_bytes_per_int = (std::numeric_limits<int64_t>::digits+1+6)/7;
};





// Implementation:

inline Replication::Replication() TIGHTDB_NOEXCEPT:
    m_shared_state(0), m_interrupt(false), m_selected_table(0), m_selected_spec(0) {}


inline void Replication::attach(const std::string& path_to_database_file,
                                bool map_transact_log_buf)
{
    error_code err = init(path_to_database_file, map_transact_log_buf);
    if (err) throw_error(err);
}


inline bool Replication::is_attached() const TIGHTDB_NOEXCEPT
{
    return m_shared_state;
}


inline error_code Replication::transact_log_reserve(char** buf, int n)
{
    if (std::size_t(m_transact_log_free_end - m_transact_log_free_begin) < n) {
        const error_code err = transact_log_reserve_contig(n);
        if (err) return err;
    }
    *buf = m_transact_log_free_begin;
    return ERROR_NONE;
}


inline void Replication::transact_log_advance(char* ptr)
{
    m_transact_log_free_begin = ptr;
}


inline error_code Replication::transact_log_append(const char* data, std::size_t size)
{
    if (std::size_t(m_transact_log_free_end - m_transact_log_free_begin) < size) {
        return transact_log_append_overflow(data, size);
    }
    m_transact_log_free_begin = std::copy(data, data+size, m_transact_log_free_begin);
    return ERROR_NONE;
}


template<class T> inline char* Replication::encode_int(char* ptr, T value)
{
    bool negative = false;
    if (is_negative(value)) {
        negative = true;
        value = -(value + 1);
    }
    // At this point 'value' is always a positive number, also, small
    // negative numbers have been converted to small positive numbers.
    const int max_bytes = (std::numeric_limits<T>::digits+1+6)/7;
    for (int i=0; i<max_bytes; ++i) {
        if (value >> 6 == 0) break;
        *reinterpret_cast<unsigned char*>(ptr) = 0x80 | int(value & 0x7F);
        ++ptr;
        value >>= 7;
    }
    *reinterpret_cast<unsigned char*>(ptr) = negative ? 0x40 | int(value) : value;
    return ++ptr;
}


template<class T> inline void Replication::EncodeInt<T>::operator()(T value, char** ptr)
{
    *ptr = encode_int(*ptr, value);
}


template<class L> inline error_code Replication::simple_cmd(char cmd, const Tuple<L>& integers)
{
    char* buf;
    error_code err = transact_log_reserve(&buf, 1 + TypeCount<L>::value*max_enc_bytes_per_int);
    if (err) return err;
    *buf++ = cmd;
    for_each<EncodeInt>(integers, &buf);
    transact_log_advance(buf);
    return ERROR_NONE;
}


inline error_code Replication::check_table(const Table* t)
{
    if (t != m_selected_table) {
        return select_table(t);
    }
    return ERROR_NONE;
}


inline error_code Replication::check_spec(const Table* t, const Spec* s)
{
    if (s != m_selected_spec) {
        return select_spec(t,s);
    }
    return ERROR_NONE;
}


inline error_code Replication::string_cmd(char cmd, std::size_t column_ndx,
                                          std::size_t ndx, const char* data, std::size_t size)
{
    error_code err = simple_cmd(cmd, tuple(column_ndx, ndx, size));
    if (err) return err;
    return transact_log_append(data, size);
}


inline error_code Replication::mixed_cmd(char cmd, std::size_t column_ndx,
                                         std::size_t ndx, const Mixed& value)
{
    char* buf;
    error_code err = transact_log_reserve(&buf, 1 + 4*max_enc_bytes_per_int);
    if (err) return err;
    *buf++ = cmd;
    buf = encode_int(buf, column_ndx);
    buf = encode_int(buf, ndx);
    buf = encode_int(buf, int(value.get_type()));
    switch (value.get_type()) {
    case COLUMN_TYPE_INT:
        buf = encode_int(buf, value.get_int());
        transact_log_advance(buf);
        break;
    case COLUMN_TYPE_BOOL:
        buf = encode_int(buf, int(value.get_bool()));
        transact_log_advance(buf);
        break;
    case COLUMN_TYPE_DATE:
        buf = encode_int(buf, value.get_date());
        transact_log_advance(buf);
        break;
    case COLUMN_TYPE_STRING:
        {
            const char* data = value.get_string();
            std::size_t size = std::strlen(data);
            buf = encode_int(buf, size);
            transact_log_advance(buf);
            err = transact_log_append(data, size);
            if (err) return err;
        }
        break;
    case COLUMN_TYPE_BINARY:
        {
            BinaryData data = value.get_binary();
            buf = encode_int(buf, data.len);
            transact_log_advance(buf);
            err = transact_log_append(data.pointer, data.len);
            if (err) return err;
        }
        break;
    case COLUMN_TYPE_TABLE:
        transact_log_advance(buf);
        break;
    default:
        TIGHTDB_ASSERT(false);
    }
    return ERROR_NONE;
}


inline error_code Replication::new_top_level_table(const char* name)
{
    size_t length = std::strlen(name);
    error_code err = simple_cmd('N', tuple(length));
    if (err) return err;
    return transact_log_append(name, length);
}


inline error_code Replication::add_column(const Table* table, const Spec* spec,
                                          ColumnType type, const char* name)
{
    error_code err = check_spec(table, spec);
    if (err) return err;
    size_t length = std::strlen(name);
    err = simple_cmd('A', tuple(int(type), length));
    if (err) return err;
    return transact_log_append(name, length);
}


template<class T>
inline error_code Replication::set_value(const Table* t, std::size_t column_ndx,
                                         std::size_t ndx, T value)
{
    error_code err = check_table(t);
    if (err) return err;
    return simple_cmd('s', tuple(column_ndx, ndx, value));
}

inline error_code Replication::set_value(const Table* t, std::size_t column_ndx,
                                         std::size_t ndx, BinaryData value)
{
    error_code err = check_table(t);
    if (err) return err;
    return string_cmd('s', column_ndx, ndx, value.pointer, value.len);
}

inline error_code Replication::set_value(const Table* t, std::size_t column_ndx,
                                         std::size_t ndx, const Mixed& value)
{
    error_code err = check_table(t);
    if (err) return err;
    return mixed_cmd('s', column_ndx, ndx, value);
}

inline error_code Replication::set_value(const Table* t, std::size_t column_ndx,
                                         std::size_t ndx, subtable_tag)
{
    error_code err = check_table(t);
    if (err) return err;
    return simple_cmd('s', tuple(column_ndx, ndx));
}


template<class T>
inline error_code Replication::insert_value(const Table* t, std::size_t column_ndx,
                                            std::size_t ndx, T value)
{
    error_code err = check_table(t);
    if (err) return err;
    return simple_cmd('i', tuple(column_ndx, ndx, value));
}

inline error_code Replication::insert_value(const Table* t, std::size_t column_ndx,
                                            std::size_t ndx, BinaryData value)
{
    error_code err = check_table(t);
    if (err) return err;
    return string_cmd('i', column_ndx, ndx, value.pointer, value.len);
}

inline error_code Replication::insert_value(const Table* t, std::size_t column_ndx,
                                            std::size_t ndx, const Mixed& value)
{
    error_code err = check_table(t);
    if (err) return err;
    return mixed_cmd('i', column_ndx, ndx, value);
}

inline error_code Replication::insert_value(const Table* t, std::size_t column_ndx,
                                            std::size_t ndx, subtable_tag)
{
    error_code err = check_table(t);
    if (err) return err;
    return simple_cmd('i', tuple(column_ndx, ndx));
}


inline error_code Replication::row_insert_complete(const Table* t)
{
    error_code err = check_table(t);
    if (err) return err;
    return simple_cmd('c', tuple());
}


inline error_code Replication::insert_empty_rows(const Table* t, std::size_t row_ndx,
                                                 std::size_t num_rows)
{
    error_code err = check_table(t);
    if (err) return err;
    return simple_cmd('I', tuple(row_ndx, num_rows));
}


inline error_code Replication::remove_row(const Table* t, std::size_t row_ndx)
{
    error_code err = check_table(t);
    if (err) return err;
    return simple_cmd('R', tuple(row_ndx));
}


inline error_code Replication::add_int_to_column(const Table* t, std::size_t column_ndx,
                                                 int64_t value)
{
    error_code err = check_table(t);
    if (err) return err;
    return simple_cmd('a', tuple(column_ndx, value));
}


inline error_code Replication::add_index_to_column(const Table* t, std::size_t column_ndx)
{
    error_code err = check_table(t);
    if (err) return err;
    return simple_cmd('x', tuple(column_ndx));
}


inline error_code Replication::clear_table(const Table* t)
{
    error_code err = check_table(t);
    if (err) return err;
    return simple_cmd('C', tuple());
}


inline error_code Replication::optimize_table(const Table* t)
{
    error_code err = check_table(t);
    if (err) return err;
    return simple_cmd('Z', tuple());
}


template<class T> bool Replication::Buffer<T>::set_size(std::size_t new_size)
{
    T* const new_data = new (std::nothrow) T[new_size];
    if (!new_data) return false;
    delete[] m_data;
    m_data = new_data;
    m_size = new_size;
    return true;
}


} // namespace tightdb

#endif // TIGHTDB_REPLICATION_HPP
