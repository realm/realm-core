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
#include <stdexcept>

#include <tightdb/meta.hpp>
#include <tightdb/tuple.hpp>
#include <tightdb/unique_ptr.hpp>
#include <tightdb/file.hpp>
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
class Replication {
public:
    /// Create a Replication instance in its unattached state. To
    /// attach it to a replication coordination buffer, call open().
    /// You may test whether this instance is currently in its
    /// attached state by calling is_attached(). Calling any other
    /// method (except the destructor) while in the unattached state
    /// has undefined behaviour.
    Replication() TIGHTDB_NOEXCEPT;

    ~Replication() TIGHTDB_NOEXCEPT;

    static std::string get_path_to_database_file() TIGHTDB_NOEXCEPT
    {
        return "/var/lib/tightdb/replication.db";
    }

    /// Attach this instance to the replication coordination buffer
    /// associated with the specified database file.
    ///
    /// This function must be called before using an instance of this
    /// class. It must not be called more than once. It is legal to
    /// destroy an instance of this class without this function having
    /// been called.
    ///
    /// \param file The file system path to the database file. This is
    /// used only to derive a path for a replication specific shared
    /// memory file object. Its path is derived by appending ".repl"
    /// to the specified path.
    ///
    /// \param map_transact_log_buf When true, all of the replication
    /// specific shared memory is mapped. When false, the transaction
    /// log buffer is not mapped. When used in conjunction with an
    /// instance of SharedGroup, this must always be true.
    void open(const std::string& file = "", bool map_transact_log_buf = true);

    bool is_attached() const TIGHTDB_NOEXCEPT;

    /// Interrupt any blocking call to a function in this class. This
    /// function may be called asyncronously from any thread, but it
    /// may not be called from a system signal handler.
    ///
    /// Some of the public function members of this class may block,
    /// but only when it it is explicitely stated in the documention
    /// for those functions.
    ///
    /// FIXME: Currently we do not state blocking behaviour for all
    /// the functions that can block.
    ///
    /// After any function has returned with an interruption
    /// indication, the only functions that may safely be called are
    /// rollback_write_transact() and the destructor. If a client,
    /// after having received an interruption indication, calls
    /// rollback_write_transact() and then clear_interrupt(), it may
    /// then resume normal operation on this object.
    void interrupt() TIGHTDB_NOEXCEPT;

    struct Interrupted: std::runtime_error {
        Interrupted(): std::runtime_error("Interrupted") {}
    };

    /// Acquire permision to start a new 'write' transaction. This
    /// function must be called by a client before it requests a
    /// 'write' transaction. This ensures that the local shared
    /// database is up-to-date. During the transaction, all
    /// modifications must be posted to this Replication instance as
    /// calls to set_value() and friends. After the completion of the
    /// transaction, the client must call either
    /// commit_write_transact() or rollback_write_transact().
    ///
    /// \throw Interrupted If this call was interrupted by an
    /// asynchronous call to interrupt().
    void begin_write_transact();

    /// Called by a client to commit the accumulated transaction
    /// log. The transaction log may not be committed if any of the
    /// functions that submit data to it, have failed or been
    /// interrupted. This operation will block until the local
    /// coordinator reports that the transaction log has been dealt
    /// with in a manner that makes the transaction persistent. This
    /// operation may be interrupted by an asynchronous call to
    /// interrupt().
    ///
    /// \throw Interrupted If this call was interrupted by an
    /// asynchronous call to interrupt().
    ///
    /// FIXME: In general the transaction will be considered complete
    /// even if this operation is interrupted. Is that ok?
    void commit_write_transact();

    /// Called by a client to discard the accumulated transaction
    /// log. This function must be called if a write transaction was
    /// successfully initiated, but one of the functions that submit
    /// data to the transaction log has failed or has been
    /// interrupted. It must also be called after a failed or
    /// interrupted call to commit_write_transact().
    void rollback_write_transact() TIGHTDB_NOEXCEPT;

    /// May be called by a client to reset this replication instance
    /// after an interrupted transaction. It is not an error to call
    /// this function in a situation where no interruption has
    /// occured.
    void clear_interrupt() TIGHTDB_NOEXCEPT;

    /// Called by the local coordinator to wait for the next client
    /// that wants to start a new 'write' transaction.
    ///
    /// \return False if the operation was aborted by an asynchronous
    /// call to interrupt().
    bool wait_for_write_request() TIGHTDB_NOEXCEPT;

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
    /// call to interrupt().
    bool grant_write_access_and_wait_for_completion(TransactLog& transact_log) TIGHTDB_NOEXCEPT;

    /// Called by the local coordinator to gain access to the memory
    /// that corresponds to the specified transaction log. The memory
    /// mapping of the underlying file is expanded as necessary. This
    /// function cannot block, and can therefore not be interrupted.
    void map_transact_log(const TransactLog&, const char** addr1, const char** addr2);

    /// Called by the local coordinator to signal to clients that a
    /// new version of the database has been persisted. Presumably,
    /// one of the clients is blocked in a call to
    /// commit_write_transact() waiting for this signal.
    void update_persisted_db_version(db_version_type) TIGHTDB_NOEXCEPT;

    /// Called by the local coordinator to release space in the
    /// transaction log buffer corresponding to a previously completed
    /// transaction log that is no longer needed. This function may be
    /// called asynchronously with respect to wait_for_write_request()
    /// and grant_write_access_and_wait_for_completion().
    void transact_log_consumed(size_t size) TIGHTDB_NOEXCEPT;


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

    void new_top_level_table(StringData name);
    void add_column(const Table*, const Spec*, DataType, StringData name);

    template<class T>
    void set_value(const Table*, std::size_t column_ndx, std::size_t ndx, T value);
    void set_value(const Table*, std::size_t column_ndx, std::size_t ndx, StringData value);
    void set_value(const Table*, std::size_t column_ndx, std::size_t ndx, BinaryData value);
    void set_value(const Table*, std::size_t column_ndx, std::size_t ndx, const Mixed& value);
    void set_value(const Table*, std::size_t column_ndx, std::size_t ndx, subtable_tag);

    template<class T>
    void insert_value(const Table*, std::size_t column_ndx, std::size_t ndx, T value);
    void insert_value(const Table*, std::size_t column_ndx, std::size_t ndx, StringData value);
    void insert_value(const Table*, std::size_t column_ndx, std::size_t ndx, BinaryData value);
    void insert_value(const Table*, std::size_t column_ndx, std::size_t ndx, const Mixed& value);
    void insert_value(const Table*, std::size_t column_ndx, std::size_t ndx, subtable_tag);

    void row_insert_complete(const Table*);
    void insert_empty_rows(const Table*, std::size_t row_ndx, std::size_t num_rows);
    void remove_row(const Table*, std::size_t row_ndx);
    void add_int_to_column(const Table*, std::size_t column_ndx, int64_t value);
    void add_index_to_column(const Table*, std::size_t column_ndx);
    void clear_table(const Table*);
    void optimize_table(const Table*);

    void on_table_destroyed(const Table*) TIGHTDB_NOEXCEPT;
    void on_spec_destroyed(const Spec*) TIGHTDB_NOEXCEPT;


    struct InputStream {
        /// \return The number of extracted bytes. This will always be
        /// less than or equal to \a size. A value of zero indicates
        /// end-of-input unless \a size was zero.
        virtual std::size_t read(char* buffer, std::size_t size) = 0;

        virtual ~InputStream() {}
    };

    struct BadTransactLog: std::runtime_error {
        BadTransactLog(): std::runtime_error("Bad transaction log") {}
    };

    /// Called by the local coordinator to apply a transaction log
    /// received from another local coordinator.
    ///
    /// \param apply_log If specified, and the library was compiled in
    /// debug mode, then a line describing each individual operation
    /// is writted to the specified stream.
    ///
    /// \throw BadTransactLog If the transaction log could not be
    /// successfully parsed, or ended prematurely.
#ifdef TIGHTDB_DEBUG
    static void apply_transact_log(InputStream& transact_log, Group& target,
                                   std::ostream* apply_log = 0);
#else
    static void apply_transact_log(InputStream& transact_log, Group& target);
#endif

private:
    struct SharedState;
    struct TransactLogApplier;

    File m_file;
    // Invariant: m_fil_map.get_size() <= std::numeric_limits<std::ptrdiff_t>::max()
    File::Map<SharedState> m_file_map;
    bool m_interrupt; // Protected by m_shared_state->m_mutex

    // Set by begin_write_transact() to the new database version
    // created by the initiated 'write' transaction.
    db_version_type m_write_transact_db_version;

    // These two delimit a contiguous region of free space in the
    // buffer following the last written data. It may be empty.
    char* m_transact_log_free_begin;
    char* m_transact_log_free_end;

    template<class T> struct Buffer {
        UniquePtr<T[]> m_data;
        std::size_t m_size;
        T& operator[](std::size_t i) TIGHTDB_NOEXCEPT { return m_data[i]; }
        const T& operator[](std::size_t i) const TIGHTDB_NOEXCEPT { return m_data[i]; }
        Buffer() TIGHTDB_NOEXCEPT: m_data(0), m_size(0) {}
        void set_size(std::size_t);
        friend void swap(Buffer&a, Buffer&b)
        {
            using std::swap;
            swap(a.m_data, b.m_data);
            swap(a.m_size, b.m_size);
        }
    };
    Buffer<std::size_t> m_subtab_path_buf;

    const Table* m_selected_table;
    const Spec*  m_selected_spec;

    /// \param n Must be small (probably not greater than 1024)
    void transact_log_reserve(char** buf, int n);

    void transact_log_advance(char* ptr) TIGHTDB_NOEXCEPT;

    void transact_log_append(const char* data, std::size_t size);

    /// \param n Must be small (probably not greater than 1024)
    void transact_log_reserve_contig(std::size_t n);

    void transact_log_append_overflow(const char* data, std::size_t size);

    /// Must be called only by a client that has 'write' access and
    /// only when there are no transaction logs in the transaction log
    /// buffer beyond the one being created.
    void transact_log_expand(std::size_t free, bool contig);

    void remap_file(std::size_t size);

    void check_table(const Table*);
    void select_table(const Table*);

    void check_spec(const Table*, const Spec*);
    void select_spec(const Table*, const Spec*);

    void string_cmd(char cmd, std::size_t column_ndx, std::size_t ndx,
                    const char* data, std::size_t size);

    void mixed_cmd(char cmd, std::size_t column_ndx, std::size_t ndx, const Mixed& value);

    template<class L> void simple_cmd(char cmd, const Tuple<L>& numbers);

    template<class> struct EncodeNumber;

    template<class T> static char* encode_int(char* ptr, T value);

    static char* encode_float(char* ptr, float value);
    static char* encode_double(char* ptr, double value);

    // Make sure this is in agreement with the actual integer encoding scheme
    static const int max_enc_bytes_per_int = (std::numeric_limits<int64_t>::digits+1+6)/7;
    static const int max_enc_bytes_per_double = sizeof (double);
    static const int max_enc_bytes_per_num = max_enc_bytes_per_int <
        max_enc_bytes_per_double ? max_enc_bytes_per_double : max_enc_bytes_per_int;
};





// Implementation:

inline Replication::Replication() TIGHTDB_NOEXCEPT:
    m_interrupt(false), m_selected_table(0), m_selected_spec(0) {}


inline bool Replication::is_attached() const TIGHTDB_NOEXCEPT
{
    return m_file_map.is_attached();
}


inline void Replication::transact_log_reserve(char** buf, int n)
{
    if (std::size_t(m_transact_log_free_end - m_transact_log_free_begin) < std::size_t(n))
        transact_log_reserve_contig(n); // Throws
    *buf = m_transact_log_free_begin;
}


inline void Replication::transact_log_advance(char* ptr) TIGHTDB_NOEXCEPT
{
    m_transact_log_free_begin = ptr;
}


inline void Replication::transact_log_append(const char* data, std::size_t size)
{
    if (TIGHTDB_UNLIKELY(std::size_t(m_transact_log_free_end -
                                     m_transact_log_free_begin) < size)) {
        transact_log_append_overflow(data, size); // Throws
        return;
    }
    m_transact_log_free_begin = std::copy(data, data+size, m_transact_log_free_begin);
}


template<class T> inline char* Replication::encode_int(char* ptr, T value)
{
    TIGHTDB_STATIC_ASSERT(std::numeric_limits<T>::is_integer, "Integer required");
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


inline char* Replication::encode_float(char* ptr, float value)
{
    TIGHTDB_STATIC_ASSERT(std::numeric_limits<float>::is_iec559 &&
                          sizeof (float) * std::numeric_limits<unsigned char>::digits == 32,
                          "Unsupported 'float' representation");
    const char* val_ptr = reinterpret_cast<char*>(&value);
    return std::copy(val_ptr, val_ptr + sizeof value, ptr);
}


inline char* Replication::encode_double(char* ptr, double value)
{
    TIGHTDB_STATIC_ASSERT(std::numeric_limits<float>::is_iec559 &&
                          sizeof (double) * std::numeric_limits<unsigned char>::digits == 64,
                          "Unsupported 'double' representation");
    const char* val_ptr = reinterpret_cast<char*>(&value);
    return std::copy(val_ptr, val_ptr + sizeof value, ptr);
}


template<class T> struct Replication::EncodeNumber {
    void operator()(T value, char** ptr)
    {
        *ptr = encode_int(*ptr, value);
    }
};
template<> struct Replication::EncodeNumber<float> {
    void operator()(float  value, char** ptr)
    {
        *ptr = encode_float(*ptr, value);
    }
};
template<> struct Replication::EncodeNumber<double> {
    void operator()(double value, char** ptr)
    {
        *ptr = encode_double(*ptr, value);
    }
};


template<class L> inline void Replication::simple_cmd(char cmd, const Tuple<L>& numbers)
{
    char* buf;
    transact_log_reserve(&buf, 1 + TypeCount<L>::value*max_enc_bytes_per_num); // Throws
    *buf++ = cmd;
    for_each<EncodeNumber>(numbers, &buf);
    transact_log_advance(buf);
}


inline void Replication::check_table(const Table* t)
{
    if (t != m_selected_table) select_table(t); // Throws
}


inline void Replication::check_spec(const Table* t, const Spec* s)
{
    if (s != m_selected_spec) select_spec(t,s); // Throws
}


inline void Replication::string_cmd(char cmd, std::size_t column_ndx,
                                    std::size_t ndx, const char* data, std::size_t size)
{
    simple_cmd(cmd, tuple(column_ndx, ndx, size)); // Throws
    transact_log_append(data, size); // Throws
}


inline void Replication::mixed_cmd(char cmd, std::size_t column_ndx,
                                   std::size_t ndx, const Mixed& value)
{
    char* buf;
    transact_log_reserve(&buf, 1 + 4*max_enc_bytes_per_num); // Throws
    *buf++ = cmd;
    buf = encode_int(buf, column_ndx);
    buf = encode_int(buf, ndx);
    buf = encode_int(buf, int(value.get_type()));
    switch (value.get_type()) {
    case type_Int:
        buf = encode_int(buf, value.get_int());
        transact_log_advance(buf);
        break;
    case type_Bool:
        buf = encode_int(buf, int(value.get_bool()));
        transact_log_advance(buf);
        break;
    case type_Float:
        buf = encode_float(buf, value.get_float());
        transact_log_advance(buf);
        break;
    case type_Double:
        buf = encode_double(buf, value.get_double());
        transact_log_advance(buf);
        break;
    case type_Date:
        buf = encode_int(buf, value.get_date().get_date());
        transact_log_advance(buf);
        break;
    case type_String:
        {
            StringData data = value.get_string();
            buf = encode_int(buf, data.size());
            transact_log_advance(buf);
            transact_log_append(data.data(), data.size()); // Throws
        }
        break;
    case type_Binary:
        {
            BinaryData data = value.get_binary();
            buf = encode_int(buf, data.size());
            transact_log_advance(buf);
            transact_log_append(data.data(), data.size()); // Throws
        }
        break;
    case type_Table:
        transact_log_advance(buf);
        break;
    default:
        TIGHTDB_ASSERT(false);
    }
}


inline void Replication::new_top_level_table(StringData name)
{
    simple_cmd('N', tuple(name.size())); // Throws
    transact_log_append(name.data(), name.size()); // Throws
}


inline void Replication::add_column(const Table* table, const Spec* spec,
                                    DataType type, StringData name)
{
    check_spec(table, spec); // Throws
    simple_cmd('A', tuple(int(type), name.size())); // Throws
    transact_log_append(name.data(), name.size()); // Throws
}


template<class T>
inline void Replication::set_value(const Table* t, std::size_t column_ndx,
                                   std::size_t ndx, T value)
{
    check_table(t); // Throws
    simple_cmd('s', tuple(column_ndx, ndx, value)); // Throws
}

inline void Replication::set_value(const Table* t, std::size_t column_ndx,
                                   std::size_t ndx, StringData value)
{
    check_table(t); // Throws
    string_cmd('s', column_ndx, ndx, value.data(), value.size()); // Throws
}

inline void Replication::set_value(const Table* t, std::size_t column_ndx,
                                   std::size_t ndx, BinaryData value)
{
    check_table(t); // Throws
    string_cmd('s', column_ndx, ndx, value.data(), value.size()); // Throws
}

inline void Replication::set_value(const Table* t, std::size_t column_ndx,
                                   std::size_t ndx, const Mixed& value)
{
    check_table(t); // Throws
    mixed_cmd('s', column_ndx, ndx, value); // Throws
}

inline void Replication::set_value(const Table* t, std::size_t column_ndx,
                                   std::size_t ndx, subtable_tag)
{
    check_table(t); // Throws
    simple_cmd('s', tuple(column_ndx, ndx)); // Throws
}


template<class T>
inline void Replication::insert_value(const Table* t, std::size_t column_ndx,
                                      std::size_t ndx, T value)
{
    check_table(t); // Throws
    simple_cmd('i', tuple(column_ndx, ndx, value)); // Throws
}

inline void Replication::insert_value(const Table* t, std::size_t column_ndx,
                                      std::size_t ndx, StringData value)
{
    check_table(t); // Throws
    string_cmd('i', column_ndx, ndx, value.data(), value.size()); // Throws
}

inline void Replication::insert_value(const Table* t, std::size_t column_ndx,
                                      std::size_t ndx, BinaryData value)
{
    check_table(t); // Throws
    string_cmd('i', column_ndx, ndx, value.data(), value.size()); // Throws
}

inline void Replication::insert_value(const Table* t, std::size_t column_ndx,
                                      std::size_t ndx, const Mixed& value)
{
    check_table(t); // Throws
    mixed_cmd('i', column_ndx, ndx, value); // Throws
}

inline void Replication::insert_value(const Table* t, std::size_t column_ndx,
                                      std::size_t ndx, subtable_tag)
{
    check_table(t); // Throws
    simple_cmd('i', tuple(column_ndx, ndx)); // Throws
}


inline void Replication::row_insert_complete(const Table* t)
{
    check_table(t); // Throws
    simple_cmd('c', tuple()); // Throws
}


inline void Replication::insert_empty_rows(const Table* t, std::size_t row_ndx,
                                           std::size_t num_rows)
{
    check_table(t); // Throws
    simple_cmd('I', tuple(row_ndx, num_rows)); // Throws
}


inline void Replication::remove_row(const Table* t, std::size_t row_ndx)
{
    check_table(t); // Throws
    simple_cmd('R', tuple(row_ndx)); // Throws
}


inline void Replication::add_int_to_column(const Table* t, std::size_t column_ndx, int64_t value)
{
    check_table(t); // Throws
    simple_cmd('a', tuple(column_ndx, value)); // Throws
}


inline void Replication::add_index_to_column(const Table* t, std::size_t column_ndx)
{
    check_table(t); // Throws
    simple_cmd('x', tuple(column_ndx)); // Throws
}


inline void Replication::clear_table(const Table* t)
{
    check_table(t); // Throws
    simple_cmd('C', tuple()); // Throws
}


inline void Replication::optimize_table(const Table* t)
{
    check_table(t); // Throws
    simple_cmd('Z', tuple()); // Throws
}


inline void Replication::on_table_destroyed(const Table* t) TIGHTDB_NOEXCEPT
{
    if (m_selected_table == t) m_selected_table = 0;
}


inline void Replication::on_spec_destroyed(const Spec* s) TIGHTDB_NOEXCEPT
{
    if (m_selected_spec == s) m_selected_spec = 0;
}


template<class T> void Replication::Buffer<T>::set_size(std::size_t size)
{
    m_data.reset(new T[size]);
    m_size = size;
}


} // namespace tightdb

#endif // TIGHTDB_REPLICATION_HPP
