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
#include <algorithm>
#include <limits>

#include <tightdb/meta.hpp>
#include <tightdb/tuple.hpp>
#include <tightdb/error.hpp>
#include <tightdb/terminate.hpp>
#include <tightdb/mixed.hpp>

namespace tightdb {


class Spec;
class Table;


// FIXME: Be careful about the possibility of one modification functions being called by another where both do transaction logging.

// FIXME: How to handle unordered tables. Will it introduce nondeterminism in actual order such that indices cannot be considered equal across replication instances?

// FIXME: The current table/subtable selection scheme assumes that a TableRef of a subtable is not accessed after any modification of one of its ancestor tables.

// FIXME: None of the methods here must return with ERROR_INTERRUPTED due to a system call being interrupted by a handled signal. The must only return with ERROR_INTERRUPTED after a call to shutdown().

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
///
struct Replication {
    Replication();
    ~Replication();

    static const char* get_path_to_database_file() { return "/var/lib/tightdb/replication.db"; }

    error_code init();

    /// Abort any blocking call to acquire_write_access() or
    /// wait_for_write_request(). This method may be called
    /// asyncronously from any thread, but it may not be called from a
    /// system signal handler.
    ///
    /// FIXME: All the methods that submit data to the transaction log
    /// may also block, and therefore they may also be aborted by a
    /// call to this function, however, since they do not currently
    /// have any way of reporting such an interruption, or an error,
    /// they will, for now, abort the program.
    void shutdown();

    /// Acquire permision to start a new 'write' transaction. This
    /// method must be called by a client before it requests a 'write'
    /// transaction. This ensures that the local shared database is
    /// up-to-date. During the transaction, all modifications must be
    /// posted to this Replication instance as calls to set() and
    /// friends. After the completion of the transaction, the client
    /// must call release_write_access().
    ///
    /// This method returns ERROR_INTERRUPTED if it was interrupted by
    /// an asynchronous call to shutdown().
    error_code acquire_write_access();

    void release_write_access(bool rollback);

    /// Called by the local coordinator to wait for the next client
    /// that wants to start a new 'write' transaction.
    ///
    /// \return False if the operation was aborted by an asynchronous
    /// call to shutdown().
    bool wait_for_write_request();

    struct TransactLog { size_t offset1, size1, offset2, size2; };

    /// Called by the local coordinator to grant permission for one
    /// client to start a new 'write' transaction. This method also
    /// waits for the client to signal completion of the write
    /// transaction.
    ///
    /// \return False if the operation was aborted by an asynchronous
    /// call to shutdown().
    bool grant_write_access_and_wait_for_completion(TransactLog&);

    /// Called by the local coordinator to release space in the
    /// transaction log buffer corresponding to a previously completed
    /// transaction log that is no longer needed. This function may be
    /// called asynchronously with respect to wait_for_write_request()
    /// and grant_write_access_and_wait_for_completion().
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
    //   I  insert empty row
    //   R  Remove row
    //   a  Add int to column
    //   x  Add index to column
    //   C  Clear table
    //   Z  Optimize table

    struct SubtableTag {};

    void new_top_level_table(const char* name);
    void add_column(const Table*, const Spec*, ColumnType, const char* name);

    template<class T>
    void set_value(const Table*, std::size_t column_ndx, std::size_t ndx, T value);
    void set_value(const Table*, std::size_t column_ndx, std::size_t ndx, BinaryData value);
    void set_value(const Table*, std::size_t column_ndx, std::size_t ndx, const Mixed& value);
    void set_value(const Table*, std::size_t column_ndx, std::size_t ndx, SubtableTag);

    template<class T>
    void insert_value(const Table*, std::size_t column_ndx, std::size_t ndx, T value);
    void insert_value(const Table*, std::size_t column_ndx, std::size_t ndx, BinaryData value);
    void insert_value(const Table*, std::size_t column_ndx, std::size_t ndx, const Mixed& value);
    void insert_value(const Table*, std::size_t column_ndx, std::size_t ndx, SubtableTag);

    void row_insert_complete(const Table*);
    void insert_empty_rows(const Table*, std::size_t row_ndx, std::size_t num_rows);
    void remove_row(const Table*, std::size_t row_ndx);
    void add_int_to_column(const Table*, std::size_t column_ndx, int64_t value);
    void add_index_to_column(const Table*, std::size_t column_ndx);
    void clear_table(const Table*);
    void optimize_table(const Table*);

    // FIXME: Need function for table spec changes and for creation of new tables in Group.

    void on_table_destroyed(const Table* t) { if (m_selected_table == t) m_selected_table = 0; }
    void on_spec_destroyed(const Spec* s)   { if (m_selected_spec  == s) m_selected_spec  = 0; }

private:
    struct SharedState;

public:
    typedef SharedState* Replication::*unspecified_bool_type;
    operator unspecified_bool_type() const
    {
        return m_shared_state ? &Replication::m_shared_state : 0;
    }

private:
    int m_fd; // Memory mapped file descriptor
    SharedState* m_shared_state;
    // Invariant: m_shared_state_size <= std::numeric_limits<std::ptrdiff_t>::max()
    std::size_t m_shared_state_mapped_size;
    bool m_shared_state_shutdown; // Protected by m_shared_state->m_mutex

    // These two delimit a contiguous region of free space in the
    // buffer following the last written data. It may be empty.
    char* m_transact_log_free_begin;
    char* m_transact_log_free_end;

    struct Buffer {
        size_t* m_data;
        std::size_t m_size;
        Buffer(): m_data(0), m_size(0) {}
        ~Buffer() { delete[] m_data; }
        bool set_size(std::size_t);
    };
    Buffer m_subtab_path_buf;

    const Table* m_selected_table;
    const Spec*  m_selected_spec;

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

    void check_table(const Table*);
    error_code select_table(const Table*);

    void check_spec(const Table*, const Spec*);
    error_code select_spec(const Table*, const Spec*);

    void string_cmd(char cmd, std::size_t column_ndx, std::size_t ndx,
                    const char* data,std::size_t size);

    void mixed_cmd(char cmd, std::size_t column_ndx, std::size_t ndx, const Mixed& value);

    template<class L> void simple_cmd(char cmd, const Tuple<L>& integers);

    template<class T> struct EncodeInt { void operator()(T value, char** ptr); };

    template<class T> static char* encode_int(char* ptr, T value);

    // Make sure this is in agreement with the integer encoding scheme
    static const int max_enc_bytes_per_int = (std::numeric_limits<int64_t>::digits+1+6)/7;
};





// Implementation:

inline Replication::Replication():
    m_shared_state(0), m_shared_state_shutdown(false), m_selected_table(0), m_selected_spec(0) {}


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


template<class L> inline void Replication::simple_cmd(char cmd, const Tuple<L>& integers)
{
    char* buf;
    error_code err = transact_log_reserve(&buf, 1 + TypeCount<L>::value*max_enc_bytes_per_int);
    // FIXME: Termination due to lack of resources is unacceptable behaviour here.
    // FIXME: Also consider interruption due to shutdown() being called
    if (err) TIGHTDB_TERMINATE("Failed to expand transaction log");
    *buf++ = cmd;
    for_each<EncodeInt>(integers, &buf);
    transact_log_advance(buf);
}


inline void Replication::check_table(const Table* t)
{
    if (t != m_selected_table) {
        error_code err = select_table(t);
        // FIXME: Termination due to lack of resources is unacceptable behaviour here.
        // FIXME: Also consider interruption due to shutdown() being called
        if (err) TIGHTDB_TERMINATE("Failed to select new table");
    }
}


inline void Replication::check_spec(const Table* t, const Spec* s)
{
    if (s != m_selected_spec) {
        error_code err = select_spec(t,s);
        // FIXME: Termination due to lack of resources is unacceptable behaviour here.
        // FIXME: Also consider interruption due to shutdown() being called
        if (err) TIGHTDB_TERMINATE("Failed to select new table spec");
    }
}


inline void Replication::string_cmd(char cmd, std::size_t column_ndx,
                                    std::size_t ndx, const char* data, std::size_t size)
{
    simple_cmd(cmd, tuple(column_ndx, ndx, size));
    error_code err = transact_log_append(data, size);
    // FIXME: Termination due to lack of resources is unacceptable behaviour here.
    // FIXME: Also consider interruption due to shutdown() being called
    if (err) TIGHTDB_TERMINATE("Failed to expand transaction log");
}


inline void Replication::mixed_cmd(char cmd, std::size_t column_ndx,
                                   std::size_t ndx, const Mixed& value)
{
    char* buf;
    error_code err = transact_log_reserve(&buf, 1 + 4*max_enc_bytes_per_int);
    // FIXME: Termination due to lack of resources is unacceptable behaviour here.
    // FIXME: Also consider interruption due to shutdown() being called
    if (err) TIGHTDB_TERMINATE("Failed to expand transaction log");
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
            if (err) TIGHTDB_TERMINATE("Failed to expand transaction log");
        }
        break;
    case COLUMN_TYPE_BINARY:
        {
            BinaryData data = value.get_binary();
            buf = encode_int(buf, data.len);
            transact_log_advance(buf);
            err = transact_log_append(data.pointer, data.len);
            if (err) TIGHTDB_TERMINATE("Failed to expand transaction log");
        }
        break;
    case COLUMN_TYPE_TABLE:
        transact_log_advance(buf);
        break;
    default:
        assert(false);
    }
}


inline void Replication::new_top_level_table(const char* name)
{
    size_t length = std::strlen(name);
    simple_cmd('N', tuple(length));
    error_code err = transact_log_append(name, length);
    // FIXME: Termination due to lack of resources is unacceptable behaviour here.
    // FIXME: Also consider interruption due to shutdown() being called
    if (err) TIGHTDB_TERMINATE("Failed to expand transaction log");
}


inline void Replication::add_column(const Table* table, const Spec* spec,
                                    ColumnType type, const char* name)
{
    check_spec(table, spec);
    size_t length = std::strlen(name);
    simple_cmd('A', tuple(int(type), length));
    error_code err = transact_log_append(name, length);
    // FIXME: Termination due to lack of resources is unacceptable behaviour here.
    // FIXME: Also consider interruption due to shutdown() being called
    if (err) TIGHTDB_TERMINATE("Failed to expand transaction log");
}


template<class T>
inline void Replication::set_value(const Table* t, std::size_t column_ndx,
                                   std::size_t ndx, T value)
{
    check_table(t);
    simple_cmd('s', tuple(column_ndx, ndx, value));
}

inline void Replication::set_value(const Table* t, std::size_t column_ndx,
                                   std::size_t ndx, BinaryData value)
{
    check_table(t);
    string_cmd('s', column_ndx, ndx, value.pointer, value.len);
}

inline void Replication::set_value(const Table* t, std::size_t column_ndx,
                                   std::size_t ndx, const Mixed& value)
{
    check_table(t);
    mixed_cmd('s', column_ndx, ndx, value);
}

inline void Replication::set_value(const Table* t, std::size_t column_ndx,
                                   std::size_t ndx, SubtableTag)
{
    check_table(t);
    simple_cmd('s', tuple(column_ndx, ndx));
}


template<class T>
inline void Replication::insert_value(const Table* t, std::size_t column_ndx,
                                      std::size_t ndx, T value)
{
    check_table(t);
    simple_cmd('i', tuple(column_ndx, ndx, value));
}

inline void Replication::insert_value(const Table* t, std::size_t column_ndx,
                                      std::size_t ndx, BinaryData value)
{
    check_table(t);
    string_cmd('i', column_ndx, ndx, value.pointer, value.len);
}

inline void Replication::insert_value(const Table* t, std::size_t column_ndx,
                                      std::size_t ndx, const Mixed& value)
{
    check_table(t);
    mixed_cmd('i', column_ndx, ndx, value);
}

inline void Replication::insert_value(const Table* t, std::size_t column_ndx,
                                      std::size_t ndx, SubtableTag)
{
    check_table(t);
    simple_cmd('i', tuple(column_ndx, ndx));
}


inline void Replication::row_insert_complete(const Table* t)
{
    check_table(t);
    simple_cmd('c', tuple());
}


inline void Replication::insert_empty_rows(const Table* t, std::size_t row_ndx,
                                           std::size_t num_rows)
{
    check_table(t);
    simple_cmd('I', tuple(row_ndx, num_rows));
}


inline void Replication::remove_row(const Table* t, std::size_t row_ndx)
{
    check_table(t);
    simple_cmd('R', tuple(row_ndx));
}


inline void Replication::add_int_to_column(const Table* t, std::size_t column_ndx, int64_t value)
{
    check_table(t);
    simple_cmd('a', tuple(column_ndx, value));
}


inline void Replication::add_index_to_column(const Table* t, std::size_t column_ndx)
{
    check_table(t);
    simple_cmd('x', tuple(column_ndx));
}


inline void Replication::clear_table(const Table* t)
{
    check_table(t);
    simple_cmd('C', tuple());
}


inline void Replication::optimize_table(const Table* t)
{
    check_table(t);
    simple_cmd('Z', tuple());
}


} // namespace tightdb

#endif // TIGHTDB_REPLICATION_HPP
