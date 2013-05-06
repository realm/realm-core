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

#include <stdint.h> // <cstdint> is not available in C++03

#include <tightdb/meta.hpp>
#include <tightdb/tuple.hpp>
#include <tightdb/unique_ptr.hpp>
#include <tightdb/file.hpp>
#include <tightdb/mixed.hpp>

namespace tightdb {


class Spec;
class Table;
class Group;
class SharedGroup;


// FIXME: Be careful about the possibility of one modification functions being called by another where both do transaction logging.

// FIXME: How to handle unordered tables. Will it introduce nondeterminism in actual order such that indices cannot be considered equal across replication instances?

// FIXME: The current table/subtable selection scheme assumes that a TableRef of a subtable is not accessed after any modification of one of its ancestor tables.

// FIXME: Checking on same Table* requires that ~Table checks and nullifies on match. Another option would be to store m_selected_table as a TableRef. Yet another option would be to assign unique identifiers to each Table instance vial Allocator. Yet another option would be to explicitely invalidate subtables recursively when parent is modified.



/// Replication is enabled by passing an instance of this class to the
/// SharedGroup constructor.
class Replication {
public:
    class Provider {
    public:
        /// Caller receives ownership and must `delete` when done.
        virtual Replication* new_instance() = 0;
    };

    // Be sure to keep this type aligned with what is actually used in
    // SharedGroup.
    typedef uint_fast32_t database_version_type;

    std::string get_database_path();

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
    void begin_write_transact(SharedGroup&);

    /// Commit the accumulated transaction log. The transaction log
    /// may not be committed if any of the functions that submit data
    /// to it, have failed or been interrupted. This operation will
    /// block until the local coordinator reports that the transaction
    /// log has been dealt with in a manner that makes the transaction
    /// persistent. This operation may be interrupted by an
    /// asynchronous call to interrupt().
    ///
    /// \throw Interrupted If this call was interrupted by an
    /// asynchronous call to interrupt().
    ///
    /// FIXME: In general the transaction will be considered complete
    /// even if this operation is interrupted. Is that ok?
    database_version_type commit_write_transact(SharedGroup&);

    /// Called by a client to discard the accumulated transaction
    /// log. This function must be called if a write transaction was
    /// successfully initiated, but one of the functions that submit
    /// data to the transaction log has failed or has been
    /// interrupted. It must also be called after a failed or
    /// interrupted call to commit_write_transact().
    void rollback_write_transact(SharedGroup&) TIGHTDB_NOEXCEPT;

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

    /// May be called by a client to reset this replication instance
    /// after an interrupted transaction. It is not an error to call
    /// this function in a situation where no interruption has
    /// occured.
    void clear_interrupt() TIGHTDB_NOEXCEPT;

    struct Interrupted: std::runtime_error {
        Interrupted(): std::runtime_error("Interrupted") {}
    };

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

protected:
    // These two delimit a contiguous region of free space in a
    // transaction log buffer following the last written data. It may
    // be empty.
    char* m_transact_log_free_begin;
    char* m_transact_log_free_end;

    Replication();

    virtual std::string do_get_database_path() = 0;

    /// As part of the initiation of a write transaction, this method
    /// is supposed to update `m_transact_log_free_begin` and
    /// `m_transact_log_free_end` such that they refer to a (possibly
    /// empty) chunk of free space.
    virtual void do_begin_write_transact(SharedGroup&) = 0;

    virtual database_version_type do_commit_write_transact(SharedGroup&) = 0;

    virtual void do_rollback_write_transact(SharedGroup&) TIGHTDB_NOEXCEPT = 0;

    virtual void do_interrupt() TIGHTDB_NOEXCEPT = 0;

    virtual void do_clear_interrupt() TIGHTDB_NOEXCEPT = 0;

    /// Ensure contiguous free space in the transaction log
    /// buffer. This method must update `m_transact_log_free_begin`
    /// and `m_transact_log_free_end` such that they refer to a chunk
    /// of free space whose size is at least \a n.
    ///
    /// \param n The require amout of contiguous free space. Must be
    /// small (probably not greater than 1024)
    virtual void do_transact_log_reserve(std::size_t n) = 0;

    /// Copy the specified data into the transaction log buffer. This
    /// function should be called only when the specified data does
    /// not fit inside the chunk of free space currently referred to
    /// by `m_transact_log_free_begin` and `m_transact_log_free_end`.
    ///
    /// This method must update `m_transact_log_free_begin` and
    /// `m_transact_log_free_end` such that, upon return, they still
    /// refer to a (possibly empty) chunk of free space.
    virtual void do_transact_log_append(const char* data, std::size_t size) = 0;

    /// Must be called only from do_begin_write_transact(),
    /// do_commit_write_transact(), or do_rollback_write_transact().
    static Group& get_group(SharedGroup&) TIGHTDB_NOEXCEPT;

    /// Must be called only from do_begin_write_transact(),
    /// do_commit_write_transact(), or do_rollback_write_transact().
    static database_version_type get_current_version(SharedGroup&) TIGHTDB_NOEXCEPT;

    /// Must be called only from do_begin_write_transact().
    static void commit_foreign_transact_log(SharedGroup&, database_version_type new_version);

private:
    struct TransactLogApplier;

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

    /// \param ptr Must be in the rangle [m_transact_log_free_begin, m_transact_log_free_end]
    void transact_log_advance(char* ptr) TIGHTDB_NOEXCEPT;

    void transact_log_append(const char* data, std::size_t size);

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

    // Make sure this is in agreement with the actual integer encoding
    // scheme (see encode_int()).
    static const int max_enc_bytes_per_int = 10;
    static const int max_enc_bytes_per_double = sizeof (double);
    static const int max_enc_bytes_per_num = max_enc_bytes_per_int <
        max_enc_bytes_per_double ? max_enc_bytes_per_double : max_enc_bytes_per_int;
};





// Implementation:

inline std::string Replication::get_database_path()
{
    return do_get_database_path();
}


inline void Replication::begin_write_transact(SharedGroup& sg)
{
    do_begin_write_transact(sg);
    m_selected_table = 0;
    m_selected_spec  = 0;
}

inline Replication::database_version_type Replication::commit_write_transact(SharedGroup& sg)
{
    return do_commit_write_transact(sg);
}

inline void Replication::rollback_write_transact(SharedGroup& sg) TIGHTDB_NOEXCEPT
{
    do_rollback_write_transact(sg);
}

inline void Replication::interrupt() TIGHTDB_NOEXCEPT
{
    do_interrupt();
}

inline void Replication::clear_interrupt() TIGHTDB_NOEXCEPT
{
    do_clear_interrupt();
}


inline void Replication::transact_log_reserve(char** buf, int n)
{
    if (std::size_t(m_transact_log_free_end - m_transact_log_free_begin) < std::size_t(n))
        do_transact_log_reserve(n); // Throws
    *buf = m_transact_log_free_begin;
}


inline void Replication::transact_log_advance(char* ptr) TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(m_transact_log_free_begin <= ptr);
    TIGHTDB_ASSERT(ptr <= m_transact_log_free_end);
    m_transact_log_free_begin = ptr;
}


inline void Replication::transact_log_append(const char* data, std::size_t size)
{
    if (TIGHTDB_UNLIKELY(std::size_t(m_transact_log_free_end -
                                     m_transact_log_free_begin) < size)) {
        do_transact_log_append(data, size); // Throws
        return;
    }
    m_transact_log_free_begin = std::copy(data, data+size, m_transact_log_free_begin);
}


// The integer encoding is platform independent. Also, it does not
// depend on the type of the specified integer. Integers of any type
// can be encoded as long as the specified buffer is large enough (see
// below). The decoding does not have to use the same type. Decoding
// will fail if, and only if the encoded value falls outside the range
// of the requested destination type.
//
// The encoding uses one or more bytes. It never uses more than 8 bits
// per byte. The last byte in the sequence is the first one that has
// its 8th bit set to zero.
//
// Consider a particular non-negative value V. Let W be the number of
// bits needed to encode V using the trivial binary encoding of
// integers. The total number of bytes produced is then
// ceil((W+1)/7). The first byte holds the 7 least significant bits of
// V. The last byte holds at most 6 bits of V including the most
// significant one. The value of the first bit of the last byte is
// always 2**((N-1)*7) where N is the total number of bytes.
//
// A negative value W is encoded by setting the sign bit to one and
// then encoding the positive result of -(W+1) as described above. The
// advantage of this representation is that it converts small negative
// values to small positive values which require a small number of
// bytes. This would not have been true for 2's complements
// representation, for example. The sign bit is always stored as the
// 7th bit of the last byte.
//
//               value bits    value + sign    max bytes
//     --------------------------------------------------
//     int8_t         7              8              2
//     uint8_t        8              9              2
//     int16_t       15             16              3
//     uint16_t      16             17              3
//     int32_t       31             32              5
//     uint32_t      32             33              5
//     int64_t       63             64             10
//     uint64_t      64             65             10
//
template<class T> inline char* Replication::encode_int(char* ptr, T value)
{
    TIGHTDB_STATIC_ASSERT(std::numeric_limits<T>::is_integer, "Integer required");
    bool negative = false;
    if (is_negative(value)) {
        negative = true;
        // The following conversion is guaranteed by C++03 to never
        // overflow (contrast this with "-value" which indeed could
        // overflow).
        value = -(value + 1);
    }
    // At this point 'value' is always a positive number. Also, small
    // negative numbers have been converted to small positive numbers.
    TIGHTDB_ASSERT(!is_negative(value));
    // One sign bit plus number of value bits
    const int num_bits = 1 + std::numeric_limits<T>::digits;
    // Only the first 7 bits are available per byte. Had it not been
    // for the fact that maximum guaranteed bit width of a char is 8,
    // this value could have been increased to 15 (one less than the
    // number of value bits in 'unsigned').
    const int bits_per_byte = 7;
    const int max_bytes = (num_bits + (bits_per_byte-1)) / bits_per_byte;
    TIGHTDB_STATIC_ASSERT(max_bytes <= max_enc_bytes_per_int, "Bad max_enc_bytes_per_int");
    // An explicit constant maximum number of iterations is specified
    // in the hope that it will help the optimizer (to do loop
    // unrolling, for example).
    for (int i=0; i<max_bytes; ++i) {
        if (value >> (bits_per_byte-1) == 0) break;
        *reinterpret_cast<unsigned char*>(ptr) =
            (1U<<bits_per_byte) | unsigned(value & ((1U<<bits_per_byte)-1));
        ++ptr;
        value >>= bits_per_byte;
    }
    *reinterpret_cast<unsigned char*>(ptr) =
        negative ? (1U<<(bits_per_byte-1)) | unsigned(value) : value;
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
    TIGHTDB_STATIC_ASSERT(std::numeric_limits<double>::is_iec559 &&
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
