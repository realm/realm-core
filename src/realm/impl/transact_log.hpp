/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#ifndef REALM_IMPL_TRANSACT_LOG_HPP
#define REALM_IMPL_TRANSACT_LOG_HPP

#include <stdexcept>

#include <realm/string_data.hpp>
#include <realm/data_type.hpp>
#include <realm/binary_data.hpp>
#include <realm/util/buffer.hpp>
#include <realm/util/string_buffer.hpp>
#include <realm/impl/input_stream.hpp>

#include <realm/group.hpp>
#include <realm/list.hpp>

namespace realm {
namespace _impl {

/// Transaction log instruction encoding
/// NOTE: Any change to this enum is a file-format breaking change.
enum Instruction {
    instr_InsertGroupLevelTable = 1,
    instr_EraseGroupLevelTable = 2, // Remove columnless table from group
    instr_RenameGroupLevelTable = 3,
    instr_MoveGroupLevelTable = 4, // UNSUPPORTED/UNUSED. FIXME: remove in next breaking change
    instr_SelectTable = 5,
    instr_Set = 6,
    instr_SetUnique = 7,
    instr_SetDefault = 8,
    instr_AddInteger = 9,   // Add value to integer field
    instr_NullifyLink = 10, // Set link to null due to target being erased
    instr_InsertSubstring = 11,
    instr_EraseFromString = 12,
    instr_InsertEmptyRows = 13,
    instr_EraseRows = 14, // Remove (multiple) rows
    instr_SwapRows = 15,
    instr_MoveRow = 16,
    instr_MergeRows = 17,  // Replace links pointing to row A with links to row B
    instr_ClearTable = 18, // Remove all rows in selected table
    instr_EnumerateStringColumn = 19,
    instr_InsertColumn =
        21, // Insert new non-nullable column into to selected descriptor (nullable is instr_InsertNullableColumn)
    instr_InsertLinkColumn = 22,     // do, but for a link-type column
    instr_InsertNullableColumn = 23, // Insert nullable column
    instr_EraseColumn = 24,          // Remove column from selected descriptor
    instr_EraseLinkColumn = 25,      // Remove link-type column from selected descriptor
    instr_RenameColumn = 26,         // Rename column in selected descriptor
    instr_MoveColumn = 27,           // Move column in selected descriptor (UNSUPPORTED/UNUSED) FIXME: remove
    instr_AddSearchIndex = 28,       // Add a search index to a column
    instr_RemoveSearchIndex = 29,    // Remove a search index from a column
    instr_SetLinkType = 30,          // Strong/weak
    instr_SelectList = 31,
    instr_ListSet = 32,         // Assign to list entry
    instr_ListInsertNull = 33,  // Insert null entry into list, only used to revert deletion
    instr_ListMove = 34,        // Move an entry within a link list
    instr_ListSwap = 35,        // Swap two entries within a list
    instr_ListErase = 36,       // Remove an entry from a list
    instr_LinkListNullify = 37, // Remove an entry from a link list due to linked row being erased
    instr_ListClear = 38,       // Remove all entries from a list
    instr_AddRowWithKey = 39,   // Insert a row with a given key
    instr_CreateObject = 40,
    instr_RemoveObject = 41,
    instr_InsertListColumn = 42, // Insert list column
    instr_ListInsert = 43,       // Insert list entry
};

class TransactLogStream {
public:
    /// Ensure contiguous free space in the transaction log
    /// buffer. This method must update `out_free_begin`
    /// and `out_free_end` such that they refer to a chunk
    /// of free space whose size is at least \a n.
    ///
    /// \param n The required amount of contiguous free space. Must be
    /// small (probably not greater than 1024)
    /// \param n Must be small (probably not greater than 1024)
    virtual void transact_log_reserve(size_t size, char** out_free_begin, char** out_free_end) = 0;

    /// Copy the specified data into the transaction log buffer. This
    /// function should be called only when the specified data does
    /// not fit inside the chunk of free space currently referred to
    /// by `out_free_begin` and `out_free_end`.
    ///
    /// This method must update `out_begin` and
    /// `out_end` such that, upon return, they still
    /// refer to a (possibly empty) chunk of free space.
    virtual void transact_log_append(const char* data, size_t size, char** out_free_begin, char** out_free_end) = 0;
};

class TransactLogBufferStream : public TransactLogStream {
public:
    void transact_log_reserve(size_t size, char** out_free_begin, char** out_free_end) override;
    void transact_log_append(const char* data, size_t size, char** out_free_begin, char** out_free_end) override;

    const char* transact_log_data() const;

    util::Buffer<char> m_buffer;
};


// LCOV_EXCL_START (because the NullInstructionObserver is trivial)
class NullInstructionObserver {
public:
    /// The following methods are also those that TransactLogParser expects
    /// to find on the `InstructionHandler`.

    // No selection needed:
    bool select_table(TableKey)
    {
        return true;
    }
    bool select_descriptor(size_t, const size_t*)
    {
        return true;
    }
    bool select_list(ColKey, ObjKey)
    {
        return true;
    }
    bool select_link_list(ColKey, ObjKey)
    {
        return true;
    }
    bool insert_group_level_table(TableKey, size_t, StringData)
    {
        return true;
    }
    bool erase_group_level_table(TableKey, size_t)
    {
        return true;
    }
    bool rename_group_level_table(TableKey, StringData)
    {
        return true;
    }

    // Must have table selected:
    bool create_object(ObjKey)
    {
        return true;
    }
    bool remove_object(ObjKey)
    {
        return true;
    }
    bool clear_table(size_t)
    {
        return true;
    }
    bool set_int(ColKey, ObjKey, int_fast64_t, Instruction, size_t)
    {
        return true;
    }
    bool add_int(ColKey, ObjKey, int_fast64_t)
    {
        return true;
    }
    bool set_bool(ColKey, ObjKey, bool, Instruction)
    {
        return true;
    }
    bool set_float(ColKey, ObjKey, float, Instruction)
    {
        return true;
    }
    bool set_double(ColKey, ObjKey, double, Instruction)
    {
        return true;
    }
    bool set_string(ColKey, ObjKey, StringData, Instruction, size_t)
    {
        return true;
    }
    bool set_binary(ColKey, ObjKey, BinaryData, Instruction)
    {
        return true;
    }
    bool set_timestamp(ColKey, ObjKey, Timestamp, Instruction)
    {
        return true;
    }
    bool set_link(ColKey, ObjKey, ObjKey, TableKey, Instruction)
    {
        return true;
    }
    bool set_null(ColKey, ObjKey, Instruction, size_t)
    {
        return true;
    }
    bool nullify_link(ColKey, ObjKey, TableKey)
    {
        return true;
    }
    bool insert_substring(ColKey, ObjKey, size_t, StringData)
    {
        return true;
    }
    bool erase_substring(ColKey, ObjKey, size_t, size_t)
    {
        return true;
    }
    bool list_set_int(size_t, int64_t)
    {
        return true;
    }
    bool list_set_bool(size_t, bool)
    {
        return true;
    }
    bool list_set_float(size_t, float)
    {
        return true;
    }
    bool list_set_double(size_t, double)
    {
        return true;
    }
    bool list_set_string(size_t, StringData)
    {
        return true;
    }
    bool list_set_binary(size_t, BinaryData)
    {
        return true;
    }
    bool list_set_timestamp(size_t, Timestamp)
    {
        return true;
    }
    bool list_insert_int(size_t, int64_t, size_t)
    {
        return true;
    }
    bool list_insert_bool(size_t, bool, size_t)
    {
        return true;
    }
    bool list_insert_float(size_t, float, size_t)
    {
        return true;
    }
    bool list_insert_double(size_t, double, size_t)
    {
        return true;
    }
    bool list_insert_string(size_t, StringData, size_t)
    {
        return true;
    }
    bool list_insert_binary(size_t, BinaryData, size_t)
    {
        return true;
    }
    bool list_insert_timestamp(size_t, Timestamp, size_t)
    {
        return true;
    }

    bool enumerate_string_column(ColKey)
    {
        return true;
    }

    // Must have descriptor selected:
    bool insert_link_column(ColKey, DataType, StringData, TableKey, ColKey)
    {
        return true;
    }
    bool insert_column(ColKey, DataType, StringData, bool, bool)
    {
        return true;
    }
    bool erase_link_column(ColKey, TableKey, ColKey)
    {
        return true;
    }
    bool erase_column(ColKey)
    {
        return true;
    }
    bool rename_column(ColKey, StringData)
    {
        return true;
    }
    bool add_search_index(ColKey)
    {
        return true;
    }
    bool remove_search_index(ColKey)
    {
        return true;
    }
    bool set_link_type(ColKey, LinkType)
    {
        return true;
    }

    // Must have linklist selected:
    bool list_insert_null(size_t, size_t)
    {
        return true;
    }
    bool list_set_link(size_t, ObjKey)
    {
        return true;
    }
    bool list_insert_link(size_t, ObjKey, size_t)
    {
        return true;
    }
    bool list_move(size_t, size_t)
    {
        return true;
    }
    bool list_swap(size_t, size_t)
    {
        return true;
    }
    bool list_erase(size_t, size_t)
    {
        return true;
    }
    bool link_list_nullify(size_t, size_t)
    {
        return true;
    }
    bool list_clear(size_t)
    {
        return true;
    }

    void parse_complete()
    {
    }
};
// LCOV_EXCL_STOP (NullInstructionObserver)


/// See TransactLogConvenientEncoder for information about the meaning of the
/// arguments of each of the functions in this class.
class TransactLogEncoder {
public:
    /// The following methods are also those that TransactLogParser expects
    /// to find on the `InstructionHandler`.

    // No selection needed:
    bool select_table(TableKey key);
    bool select_descriptor(size_t levels, const size_t* path);
    bool select_list(ColKey col_key, ObjKey key);
    bool select_link_list(ColKey col_key, ObjKey key);
    bool insert_group_level_table(TableKey table_key, size_t num_tables, StringData name);
    bool erase_group_level_table(TableKey table_key, size_t num_tables);
    bool rename_group_level_table(TableKey table_key, StringData new_name);

    /// Must have table selected.
    bool create_object(ObjKey);
    bool remove_object(ObjKey);
    bool clear_table(size_t old_table_size);

    bool set_int(ColKey col_key, ObjKey key, int_fast64_t, Instruction = instr_Set, size_t = 0);
    bool add_int(ColKey col_key, ObjKey key, int_fast64_t);
    bool set_bool(ColKey col_key, ObjKey key, bool, Instruction = instr_Set);
    bool set_float(ColKey col_key, ObjKey key, float, Instruction = instr_Set);
    bool set_double(ColKey col_key, ObjKey key, double, Instruction = instr_Set);
    bool set_string(ColKey col_key, ObjKey key, StringData, Instruction = instr_Set, size_t = 0);
    bool set_binary(ColKey col_key, ObjKey key, BinaryData, Instruction = instr_Set);
    bool set_timestamp(ColKey col_key, ObjKey key, Timestamp, Instruction = instr_Set);
    bool set_link(ColKey col_key, ObjKey key, ObjKey, TableKey target_table_key, Instruction = instr_Set);
    bool set_null(ColKey col_key, ObjKey key, Instruction = instr_Set, size_t = 0);
    bool nullify_link(ColKey col_key, ObjKey key, TableKey target_table_key);
    bool insert_substring(ColKey col_key, ObjKey key, size_t pos, StringData);
    bool erase_substring(ColKey col_key, ObjKey key, size_t pos, size_t size);

    bool list_set_int(size_t list_ndx, int64_t value);
    bool list_set_bool(size_t list_ndx, bool value);
    bool list_set_float(size_t list_ndx, float value);
    bool list_set_double(size_t list_ndx, double value);
    bool list_set_string(size_t list_ndx, StringData value);
    bool list_set_binary(size_t list_ndx, BinaryData value);
    bool list_set_timestamp(size_t list_ndx, Timestamp value);

    bool list_insert_int(size_t list_ndx, int64_t value, size_t prior_size);
    bool list_insert_bool(size_t list_ndx, bool value, size_t prior_size);
    bool list_insert_float(size_t list_ndx, float value, size_t prior_size);
    bool list_insert_double(size_t list_ndx, double value, size_t prior_size);
    bool list_insert_string(size_t list_ndx, StringData value, size_t prior_size);
    bool list_insert_binary(size_t list_ndx, BinaryData value, size_t prior_size);
    bool list_insert_timestamp(size_t list_ndx, Timestamp value, size_t prior_size);

    bool enumerate_string_column(ColKey col_key);

    // Must have descriptor selected:
    bool insert_link_column(ColKey col_key, DataType, StringData name, TableKey link_target_table_key,
                            ColKey backlink_col_key);
    bool insert_column(ColKey col_key, DataType, StringData name, bool nullable = false, bool listtype = false);
    bool erase_link_column(ColKey col_key, TableKey link_target_table_key, ColKey backlink_col_key);
    bool erase_column(ColKey col_key);
    bool rename_column(ColKey col_key, StringData new_name);
    bool add_search_index(ColKey col_key);
    bool remove_search_index(ColKey col_key);
    bool set_link_type(ColKey col_key, LinkType);

    // Must have linklist selected:
    bool list_insert_null(size_t ndx, size_t prior_size);
    bool list_set_link(size_t link_ndx, ObjKey value);
    bool list_insert_link(size_t link_ndx, ObjKey value, size_t prior_size);
    bool list_move(size_t from_link_ndx, size_t to_link_ndx);
    bool list_swap(size_t link1_ndx, size_t link2_ndx);
    bool list_erase(size_t list_ndx, size_t prior_size);
    bool link_list_nullify(size_t link_ndx, size_t prior_size);
    bool list_clear(size_t old_list_size);

    /// End of methods expected by parser.


    TransactLogEncoder(TransactLogStream& out_stream);
    void set_buffer(char* new_free_begin, char* new_free_end);
    char* write_position() const
    {
        return m_transact_log_free_begin;
    }

private:
    // Make sure this is in agreement with the actual integer encoding
    // scheme (see encode_int()).
    static constexpr int max_enc_bytes_per_int = 10;
    static constexpr int max_enc_bytes_per_double = sizeof(double);
    static constexpr int max_enc_bytes_per_num =
        max_enc_bytes_per_int < max_enc_bytes_per_double ? max_enc_bytes_per_double : max_enc_bytes_per_int;
// Space is reserved in chunks to avoid excessive over allocation.
#ifdef REALM_DEBUG
    static constexpr int max_numbers_per_chunk = 2; // Increase the chance of chunking in debug mode
#else
    static constexpr int max_numbers_per_chunk = 8;
#endif

    // This value is used in Set* instructions in place of the 'type' field in
    // the stream to indicate that the value of the Set* instruction is NULL,
    // which doesn't have a type.
    static constexpr int set_null_sentinel()
    {
        return -1;
    }

    TransactLogStream& m_stream;

    // These two delimit a contiguous region of free space in a
    // transaction log buffer following the last written data. It may
    // be empty.
    char* m_transact_log_free_begin = nullptr;
    char* m_transact_log_free_end = nullptr;

    char* reserve(size_t size);
    /// \param ptr Must be in the range [m_transact_log_free_begin, m_transact_log_free_end]
    void advance(char* ptr) noexcept;

    template <class T>
    size_t max_size(T);

    size_t max_size_list()
    {
        return 0;
    }

    template <class T, class... Args>
    size_t max_size_list(T val, Args... args)
    {
        return max_size(val) + max_size_list(args...);
    }

    template <class T>
    char* encode(char* ptr, T value);

    char* encode_list(char* ptr)
    {
        advance(ptr);
        return ptr;
    }

    template <class T, class... Args>
    char* encode_list(char* ptr, T value, Args... args)
    {
        return encode_list(encode(ptr, value), args...);
    }

    template <class... L>
    void append_simple_instr(L... numbers);

    template <class T>
    static char* encode_int(char*, T value);
    friend class TransactLogParser;
};

class TransactLogConvenientEncoder {
public:
    virtual void insert_group_level_table(TableKey table_key, size_t num_tables, StringData name);
    virtual void erase_group_level_table(TableKey table_key, size_t num_tables);
    virtual void rename_group_level_table(TableKey table_key, StringData new_name);
    virtual void insert_column(const Table*, ColKey col_key, DataType type, StringData name, LinkTargetInfo& link,
                               bool nullable = false, bool listtype = false);
    virtual void erase_column(const Table*, ColKey col_key);
    virtual void rename_column(const Table*, ColKey col_key, StringData name);

    virtual void set_int(const Table*, ColKey col_key, ObjKey key, int_fast64_t value,
                         Instruction variant = instr_Set);
    virtual void add_int(const Table*, ColKey col_key, ObjKey key, int_fast64_t value);
    virtual void set_bool(const Table*, ColKey col_key, ObjKey key, bool value, Instruction variant = instr_Set);
    virtual void set_float(const Table*, ColKey col_key, ObjKey key, float value, Instruction variant = instr_Set);
    virtual void set_double(const Table*, ColKey col_key, ObjKey key, double value, Instruction variant = instr_Set);
    virtual void set_string(const Table*, ColKey col_key, ObjKey key, StringData value,
                            Instruction variant = instr_Set);
    virtual void set_binary(const Table*, ColKey col_key, ObjKey key, BinaryData value,
                            Instruction variant = instr_Set);
    virtual void set_timestamp(const Table*, ColKey col_key, ObjKey key, Timestamp value,
                               Instruction variant = instr_Set);
    virtual void set_link(const Table*, ColKey col_key, ObjKey key, ObjKey value, Instruction variant = instr_Set);
    virtual void set_null(const Table*, ColKey col_key, ObjKey key, Instruction variant = instr_Set);
    virtual void insert_substring(const Table*, ColKey col_key, ObjKey key, size_t pos, StringData);
    virtual void erase_substring(const Table*, ColKey col_key, ObjKey key, size_t pos, size_t size);

    virtual void list_set_int(const List<Int>& list, size_t list_ndx, int64_t value);
    virtual void list_set_bool(const List<Bool>& list, size_t list_ndx, bool value);
    virtual void list_set_float(const List<Float>& list, size_t list_ndx, float value);
    virtual void list_set_double(const List<Double>& list, size_t list_ndx, double value);
    virtual void list_set_string(const List<String>& list, size_t list_ndx, StringData value);
    virtual void list_set_binary(const List<Binary>& list, size_t list_ndx, BinaryData value);
    virtual void list_set_timestamp(const List<Timestamp>& list, size_t list_ndx, Timestamp value);

    virtual void list_insert_int(const List<Int>& list, size_t list_ndx, int64_t value);
    virtual void list_insert_bool(const List<Bool>& list, size_t list_ndx, bool value);
    virtual void list_insert_float(const List<Float>& list, size_t list_ndx, float value);
    virtual void list_insert_double(const List<Double>& list, size_t list_ndx, double value);
    virtual void list_insert_string(const List<String>& list, size_t list_ndx, StringData value);
    virtual void list_insert_binary(const List<Binary>& list, size_t list_ndx, BinaryData value);
    virtual void list_insert_timestamp(const List<Timestamp>& list, size_t list_ndx, Timestamp value);

    virtual void create_object(const Table*, ObjKey);
    virtual void remove_object(const Table*, ObjKey);
    /// \param prior_num_rows The number of rows in the table prior to the
    /// modification.
    virtual void add_search_index(const Table*, ColKey col_key);
    virtual void remove_search_index(const Table*, ColKey col_key);
    virtual void set_link_type(const Table*, ColKey col_key, LinkType);
    virtual void clear_table(const Table*, size_t prior_num_rows);
    virtual void enumerate_string_column(const Table*, ColKey col_key);

    virtual void list_insert_null(const ConstListBase&, size_t ndx);
    virtual void list_set_link(const List<ObjKey>&, size_t link_ndx, ObjKey value);
    virtual void list_insert_link(const List<ObjKey>&, size_t link_ndx, ObjKey value);
    virtual void list_move(const ConstListBase&, size_t from_link_ndx, size_t to_link_ndx);
    virtual void list_swap(const ConstListBase&, size_t link_ndx_1, size_t link_ndx_2);
    virtual void list_erase(const ConstListBase&, size_t link_ndx);
    virtual void list_clear(const ConstListBase&);

    //@{

    /// Implicit nullifications due to removal of target row. This is redundant
    /// information from the point of view of replication, as the removal of the
    /// target row will reproduce the implicit nullifications in the target
    /// Realm anyway. The purpose of this instruction is to allow observers
    /// (reactor pattern) to be explicitly notified about the implicit
    /// nullifications.

    virtual void nullify_link(const Table*, ColKey col_key, ObjKey key);
    virtual void link_list_nullify(const LinkList&, size_t link_ndx);

    //@}

    void on_table_destroyed(const Table*) noexcept;
    void on_spec_destroyed(const Spec*) noexcept;

protected:
    TransactLogConvenientEncoder(TransactLogStream& encoder);

    void reset_selection_caches() noexcept;
    void set_buffer(char* new_free_begin, char* new_free_end)
    {
        m_encoder.set_buffer(new_free_begin, new_free_end);
    }
    char* write_position() const
    {
        return m_encoder.write_position();
    }

private:
    struct LinkListId {
        TableKey table_key;
        ObjKey object_key;
        ColKey col_id;

        LinkListId() = default;
        LinkListId(const ConstListBase& list)
            : table_key(list.get_table()->get_key())
            , object_key(list.get_key())
            , col_id(list.get_col_key())
        {
        }
        LinkListId(TableKey t, ObjKey k, ColKey c)
            : table_key(t)
            , object_key(k)
            , col_id(c)
        {
        }
        bool operator!=(const LinkListId& other)
        {
            return object_key != other.object_key || table_key != other.table_key || col_id != other.col_id;
        }
    };
    TransactLogEncoder m_encoder;
    // These are mutable because they are caches.
    mutable util::Buffer<size_t> m_subtab_path_buf;
    mutable const Table* m_selected_table;
    mutable const Spec* m_selected_spec;
    mutable LinkListId m_selected_list;

    void unselect_all() noexcept;
    void select_table(const Table*); // unselects link list
    void select_list(const ConstListBase&);

    void do_select_table(const Table*);
    void do_select_list(const ConstListBase&);

    friend class TransactReverser;
};


class TransactLogParser {
public:
    class BadTransactLog; // Exception

    TransactLogParser();
    ~TransactLogParser() noexcept;

    /// See `TransactLogEncoder` for a list of methods that the `InstructionHandler` must define.
    /// parse() promises that the path passed by reference to
    /// InstructionHandler::select_descriptor() will remain valid
    /// during subsequent calls to all descriptor modifying functions.
    template <class InstructionHandler>
    void parse(InputStream&, InstructionHandler&);

    template <class InstructionHandler>
    void parse(NoCopyInputStream&, InstructionHandler&);

private:
    util::Buffer<char> m_input_buffer;

    // The input stream is assumed to consist of chunks of memory organised such that
    // every instruction resides in a single chunk only.
    NoCopyInputStream* m_input;
    // pointer into transaction log, each instruction is parsed from m_input_begin and onwards.
    // Each instruction are assumed to be contiguous in memory.
    const char* m_input_begin;
    // pointer to one past current instruction log chunk. If m_input_begin reaches m_input_end,
    // a call to next_input_buffer will move m_input_begin and m_input_end to a new chunk of
    // memory. Setting m_input_end to 0 disables this check, and is used if it is already known
    // that all of the instructions are in memory.
    const char* m_input_end;
    util::StringBuffer m_string_buffer;

    REALM_NORETURN void parser_error() const;

    template <class InstructionHandler>
    void parse_one(InstructionHandler&);
    bool has_next() noexcept;

    template <class T>
    T read_int();

    void read_bytes(char* data, size_t size);
    BinaryData read_buffer(util::StringBuffer&, size_t size);

    bool read_bool();
    float read_float();
    double read_double();

    StringData read_string(util::StringBuffer&);
    BinaryData read_binary(util::StringBuffer&);
    Timestamp read_timestamp();

    // Advance m_input_begin and m_input_end to reflect the next block of instructions
    // Returns false if no more input was available
    bool next_input_buffer();

    // return true if input was available
    bool read_char(char&); // throws

    bool is_valid_data_type(int type);
    bool is_valid_link_type(int type);
};


class TransactLogParser::BadTransactLog : public std::exception {
public:
    const char* what() const noexcept override
    {
        return "Bad transaction log";
    }
};


/// Implementation:

inline void TransactLogBufferStream::transact_log_reserve(size_t n, char** inout_new_begin, char** out_new_end)
{
    char* data = m_buffer.data();
    REALM_ASSERT(*inout_new_begin >= data);
    REALM_ASSERT(*inout_new_begin <= (data + m_buffer.size()));
    size_t size = *inout_new_begin - data;
    m_buffer.reserve_extra(size, n);
    data = m_buffer.data(); // May have changed
    *inout_new_begin = data + size;
    *out_new_end = data + m_buffer.size();
}

inline void TransactLogBufferStream::transact_log_append(const char* data, size_t size, char** out_new_begin,
                                                         char** out_new_end)
{
    transact_log_reserve(size, out_new_begin, out_new_end);
    *out_new_begin = realm::safe_copy_n(data, size, *out_new_begin);
}

inline const char* TransactLogBufferStream::transact_log_data() const
{
    return m_buffer.data();
}

inline TransactLogEncoder::TransactLogEncoder(TransactLogStream& stream)
    : m_stream(stream)
{
}

inline void TransactLogEncoder::set_buffer(char* free_begin, char* free_end)
{
    REALM_ASSERT(free_begin <= free_end);
    m_transact_log_free_begin = free_begin;
    m_transact_log_free_end = free_end;
}

inline void TransactLogConvenientEncoder::reset_selection_caches() noexcept
{
    unselect_all();
}

inline char* TransactLogEncoder::reserve(size_t n)
{
    if (size_t(m_transact_log_free_end - m_transact_log_free_begin) < n) {
        m_stream.transact_log_reserve(n, &m_transact_log_free_begin, &m_transact_log_free_end);
    }
    return m_transact_log_free_begin;
}

inline void TransactLogEncoder::advance(char* ptr) noexcept
{
    REALM_ASSERT_DEBUG(m_transact_log_free_begin <= ptr);
    REALM_ASSERT_DEBUG(ptr <= m_transact_log_free_end);
    m_transact_log_free_begin = ptr;
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
template <class T>
char* TransactLogEncoder::encode_int(char* ptr, T value)
{
    static_assert(std::numeric_limits<T>::is_integer, "Integer required");
    bool negative = util::is_negative(value);
    if (negative) {
        // The following conversion is guaranteed by C++11 to never
        // overflow (contrast this with "-value" which indeed could
        // overflow). See C99+TC3 section 6.2.6.2 paragraph 2.
        REALM_DIAG_PUSH();
        REALM_DIAG_IGNORE_UNSIGNED_MINUS();
        value = -(value + 1);
        REALM_DIAG_POP();
    }
    // At this point 'value' is always a positive number. Also, small
    // negative numbers have been converted to small positive numbers.
    REALM_ASSERT(!util::is_negative(value));
    // One sign bit plus number of value bits
    const int num_bits = 1 + std::numeric_limits<T>::digits;
    // Only the first 7 bits are available per byte. Had it not been
    // for the fact that maximum guaranteed bit width of a char is 8,
    // this value could have been increased to 15 (one less than the
    // number of value bits in 'unsigned').
    const int bits_per_byte = 7;
    const int max_bytes = (num_bits + (bits_per_byte - 1)) / bits_per_byte;
    static_assert(max_bytes <= max_enc_bytes_per_int, "Bad max_enc_bytes_per_int");
    // An explicit constant maximum number of iterations is specified
    // in the hope that it will help the optimizer (to do loop
    // unrolling, for example).
    typedef unsigned char uchar;
    for (int i = 0; i < max_bytes; ++i) {
        if (value >> (bits_per_byte - 1) == 0)
            break;
        *reinterpret_cast<uchar*>(ptr) = uchar((1U << bits_per_byte) | unsigned(value & ((1U << bits_per_byte) - 1)));
        ++ptr;
        value >>= bits_per_byte;
    }
    *reinterpret_cast<uchar*>(ptr) = uchar(negative ? (1U << (bits_per_byte - 1)) | unsigned(value) : value);
    return ++ptr;
}

template <class T>
char* TransactLogEncoder::encode(char* ptr, T value)
{
    auto value_2 = value + 0; // Perform integral promotion
    return encode_int(ptr, value_2);
}

template <>
inline char* TransactLogEncoder::encode<char>(char* ptr, char value)
{
    // Write the char as-is without encoding.
    *ptr++ = value;
    return ptr;
}

template <>
inline char* TransactLogEncoder::encode<TableKey>(char* ptr, TableKey key)
{
    return encode<int64_t>(ptr, key.value);
}

template <>
inline char* TransactLogEncoder::encode<ColKey>(char* ptr, ColKey key)
{
    return encode<int64_t>(ptr, key.value);
}

template <>
inline char* TransactLogEncoder::encode<Instruction>(char* ptr, Instruction inst)
{
    return encode<char>(ptr, inst);
}

template <>
inline char* TransactLogEncoder::encode<bool>(char* ptr, bool value)
{
    return encode<char>(ptr, value);
}

template <>
inline char* TransactLogEncoder::encode<float>(char* ptr, float value)
{
    static_assert(std::numeric_limits<float>::is_iec559 &&
                      sizeof(float) * std::numeric_limits<unsigned char>::digits == 32,
                  "Unsupported 'float' representation");
    const char* val_ptr = reinterpret_cast<char*>(&value);
    return realm::safe_copy_n(val_ptr, sizeof value, ptr);
}

template <>
inline char* TransactLogEncoder::encode<double>(char* ptr, double value)
{
    static_assert(std::numeric_limits<double>::is_iec559 &&
                      sizeof(double) * std::numeric_limits<unsigned char>::digits == 64,
                  "Unsupported 'double' representation");
    const char* val_ptr = reinterpret_cast<char*>(&value);
    return realm::safe_copy_n(val_ptr, sizeof value, ptr);
}

template <>
inline char* TransactLogEncoder::encode<DataType>(char* ptr, DataType type)
{
    return encode<char>(ptr, type);
}

template <>
inline char* TransactLogEncoder::encode<StringData>(char* ptr, StringData s)
{
    ptr = encode_int(ptr, s.size());
    return std::copy_n(s.data(), s.size(), ptr);
}

template <class T>
size_t TransactLogEncoder::max_size(T)
{
    return max_enc_bytes_per_num;
}

template <>
inline size_t TransactLogEncoder::max_size(char)
{
    return 1;
}

template <>
inline size_t TransactLogEncoder::max_size(bool)
{
    return 1;
}

template <>
inline size_t TransactLogEncoder::max_size(Instruction)
{
    return 1;
}

template <>
inline size_t TransactLogEncoder::max_size(DataType)
{
    return 1;
}

template <>
inline size_t TransactLogEncoder::max_size(StringData s)
{
    return max_enc_bytes_per_num + s.size();
}

template <class... L>
void TransactLogEncoder::append_simple_instr(L... numbers)
{
    size_t max_required_bytes = max_size_list(numbers...);
    char* ptr = reserve(max_required_bytes); // Throws
    encode_list(ptr, numbers...);
}

inline void TransactLogConvenientEncoder::unselect_all() noexcept
{
    m_selected_table = nullptr;
    m_selected_spec = nullptr;
    m_selected_list = LinkListId();
}

inline void TransactLogConvenientEncoder::select_table(const Table* table)
{
    if (table != m_selected_table)
        do_select_table(table); // Throws
    m_selected_spec = nullptr;
    m_selected_list = LinkListId();
}

inline void TransactLogConvenientEncoder::select_list(const ConstListBase& list)
{
    if (LinkListId(list) != m_selected_list) {
        do_select_list(list); // Throws
    }
    m_selected_spec = nullptr;
}

inline bool TransactLogEncoder::insert_group_level_table(TableKey table_key, size_t prior_num_tables, StringData name)
{
    append_simple_instr(instr_InsertGroupLevelTable, table_key, prior_num_tables, name); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::insert_group_level_table(TableKey table_key, size_t prior_num_tables,
                                                                   StringData name)
{
    unselect_all();
    m_encoder.insert_group_level_table(table_key, prior_num_tables, name); // Throws
}

inline bool TransactLogEncoder::erase_group_level_table(TableKey table_key, size_t prior_num_tables)
{
    append_simple_instr(instr_EraseGroupLevelTable, table_key, prior_num_tables); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::erase_group_level_table(TableKey table_key, size_t prior_num_tables)
{
    unselect_all();
    m_encoder.erase_group_level_table(table_key, prior_num_tables); // Throws
}

inline bool TransactLogEncoder::rename_group_level_table(TableKey table_key, StringData new_name)
{
    append_simple_instr(instr_RenameGroupLevelTable, table_key, new_name); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::rename_group_level_table(TableKey table_key, StringData new_name)
{
    unselect_all();
    m_encoder.rename_group_level_table(table_key, new_name); // Throws
}

inline bool TransactLogEncoder::insert_column(ColKey col_key, DataType type, StringData name, bool nullable,
                                              bool listtype)
{
    Instruction instr =
        listtype ? instr_InsertListColumn : (nullable ? instr_InsertNullableColumn : instr_InsertColumn);
    append_simple_instr(instr, col_key, type, name); // Throws
    return true;
}

inline bool TransactLogEncoder::insert_link_column(ColKey col_key, DataType type, StringData name,
                                                   TableKey link_target_table_key, ColKey backlink_col_key)
{
    REALM_ASSERT(_impl::TableFriend::is_link_type(ColumnType(type)));
    append_simple_instr(instr_InsertLinkColumn, col_key, type, link_target_table_key, backlink_col_key,
                        name); // Throws
    return true;
}


inline void TransactLogConvenientEncoder::insert_column(const Table* t, ColKey col_key, DataType type,
                                                        StringData name, LinkTargetInfo& link, bool nullable,
                                                        bool listtype)
{
    select_table(t); // Throws

    if (link.is_valid()) {
        typedef _impl::TableFriend tf;
        TableKey target_table_key = link.m_target_table->get_key();

        const Table& origin_table = *t;
        REALM_ASSERT(origin_table.is_group_level());
        // const Spec& target_spec = tf::get_spec(*(link.m_target_table));
        auto origin_table_key = origin_table.get_key();
        ColKey backlink_col_key = tf::find_backlink_column(*link.m_target_table, origin_table_key, col_key);
        REALM_ASSERT(backlink_col_key == link.m_backlink_col_key);
        m_encoder.insert_link_column(col_key, type, name, target_table_key, backlink_col_key); // Throws
    }
    else {
        m_encoder.insert_column(col_key, type, name, nullable, listtype); // Throws
    }
}

inline bool TransactLogEncoder::erase_column(ColKey col_key)
{
    append_simple_instr(instr_EraseColumn, col_key); // Throws
    return true;
}

inline bool TransactLogEncoder::erase_link_column(ColKey col_key, TableKey link_target_table_key,
                                                  ColKey backlink_col_key)
{
    append_simple_instr(instr_EraseLinkColumn, col_key, link_target_table_key, backlink_col_key); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::erase_column(const Table* t, ColKey col_key)
{
    select_table(t); // Throws

    DataType type = t->get_column_type(col_key);
    typedef _impl::TableFriend tf;
    if (!tf::is_link_type(ColumnType(type))) {
        m_encoder.erase_column(col_key); // Throws
    }
    else {
        /*
          FIXME: Unimplemented. Need to be done differently based on keys.

                // it's a link column:
                const Table& target_table = *tf::get_link_target_table_accessor(*t, col_key);
                size_t target_table_ndx = target_table.get_index_in_group();
                const Spec& target_spec = tf::get_spec(target_table);
                auto origin_table_key = t->get_key();
                size_t backlink_col_key = target_spec.find_backlink_column(origin_table_key, col_key);
                m_encoder.erase_link_column(col_key, target_table_ndx, backlink_col_key); // Throws
        */
    }
}

inline bool TransactLogEncoder::rename_column(ColKey col_key, StringData new_name)
{
    append_simple_instr(instr_RenameColumn, col_key, new_name); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::rename_column(const Table* t, ColKey col_key, StringData name)
{
    select_table(t);                        // Throws
    m_encoder.rename_column(col_key, name); // Throws
}


inline bool TransactLogEncoder::set_int(ColKey col_key, ObjKey key, int_fast64_t value, Instruction variant,
                                        size_t prior_num_rows)
{
    REALM_ASSERT_EX(variant == instr_Set || variant == instr_SetDefault || variant == instr_SetUnique, variant);
    if (REALM_UNLIKELY(variant == instr_SetUnique))
        append_simple_instr(variant, type_Int, col_key, key.value, prior_num_rows, value); // Throws
    else
        append_simple_instr(variant, type_Int, col_key, key.value, value); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::set_int(const Table* t, ColKey col_key, ObjKey key, int_fast64_t value,
                                                  Instruction variant)
{
    select_table(t); // Throws
    size_t prior_num_rows = (variant == instr_SetUnique ? t->size() : 0);
    m_encoder.set_int(col_key, key, value, variant, prior_num_rows); // Throws
}


inline bool TransactLogEncoder::add_int(ColKey col_key, ObjKey key, int_fast64_t value)
{
    append_simple_instr(instr_AddInteger, col_key, key.value, value); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::add_int(const Table* t, ColKey col_key, ObjKey key, int_fast64_t value)
{
    select_table(t); // Throws
    m_encoder.add_int(col_key, key, value);
}

inline bool TransactLogEncoder::set_bool(ColKey col_key, ObjKey key, bool value, Instruction variant)
{
    REALM_ASSERT_EX(variant == instr_Set || variant == instr_SetDefault, variant);
    append_simple_instr(variant, type_Bool, col_key, key.value, value); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::set_bool(const Table* t, ColKey col_key, ObjKey key, bool value,
                                                   Instruction variant)
{
    select_table(t);                                  // Throws
    m_encoder.set_bool(col_key, key, value, variant); // Throws
}

inline bool TransactLogEncoder::set_float(ColKey col_key, ObjKey key, float value, Instruction variant)
{
    REALM_ASSERT_EX(variant == instr_Set || variant == instr_SetDefault, variant);
    append_simple_instr(variant, type_Float, col_key, key.value, value); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::set_float(const Table* t, ColKey col_key, ObjKey key, float value,
                                                    Instruction variant)
{
    select_table(t);                                   // Throws
    m_encoder.set_float(col_key, key, value, variant); // Throws
}

inline bool TransactLogEncoder::set_double(ColKey col_key, ObjKey key, double value, Instruction variant)
{
    REALM_ASSERT_EX(variant == instr_Set || variant == instr_SetDefault, variant);
    append_simple_instr(instr_Set, type_Double, col_key, key.value, value); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::set_double(const Table* t, ColKey col_key, ObjKey key, double value,
                                                     Instruction variant)
{
    select_table(t);                                    // Throws
    m_encoder.set_double(col_key, key, value, variant); // Throws
}

inline bool TransactLogEncoder::set_string(ColKey col_key, ObjKey key, StringData value, Instruction variant,
                                           size_t prior_num_rows)
{
    REALM_ASSERT_EX(variant == instr_Set || variant == instr_SetDefault || variant == instr_SetUnique, variant);
    if (value.is_null()) {
        set_null(col_key, key, variant, prior_num_rows); // Throws
    }
    else {
        if (REALM_UNLIKELY(variant == instr_SetUnique))
            append_simple_instr(variant, type_String, col_key, key.value, prior_num_rows, value); // Throws
        else
            append_simple_instr(variant, type_String, col_key, key.value, value); // Throws
    }
    return true;
}

inline void TransactLogConvenientEncoder::set_string(const Table* t, ColKey col_key, ObjKey key, StringData value,
                                                     Instruction variant)
{
    select_table(t); // Throws
    size_t prior_num_rows = (variant == instr_SetUnique ? t->size() : 0);
    m_encoder.set_string(col_key, key, value, variant, prior_num_rows); // Throws
}

inline bool TransactLogEncoder::set_binary(ColKey col_key, ObjKey key, BinaryData value, Instruction variant)
{
    REALM_ASSERT_EX(variant == instr_Set || variant == instr_SetDefault, variant);
    if (value.is_null()) {
        set_null(col_key, key, variant); // Throws
    }
    else {
        StringData value_2(value.data(), value.size());
        append_simple_instr(variant, type_Binary, col_key, key.value, value_2); // Throws
    }
    return true;
}

inline void TransactLogConvenientEncoder::set_binary(const Table* t, ColKey col_key, ObjKey key, BinaryData value,
                                                     Instruction variant)
{
    select_table(t);                                    // Throws
    m_encoder.set_binary(col_key, key, value, variant); // Throws
}

inline bool TransactLogEncoder::set_timestamp(ColKey col_key, ObjKey key, Timestamp value, Instruction variant)
{
    REALM_ASSERT_EX(variant == instr_Set || variant == instr_SetDefault, variant);
    append_simple_instr(variant, type_Timestamp, col_key, key.value, value.get_seconds(),
                        value.get_nanoseconds()); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::set_timestamp(const Table* t, ColKey col_key, ObjKey key, Timestamp value,
                                                        Instruction variant)
{
    select_table(t);                                       // Throws
    m_encoder.set_timestamp(col_key, key, value, variant); // Throws
}

inline bool TransactLogEncoder::set_link(ColKey col_key, ObjKey key, ObjKey target_key, TableKey target_table_key,
                                         Instruction variant)
{
    REALM_ASSERT_EX(variant == instr_Set || variant == instr_SetDefault, variant);
    // Map `realm::npos` to zero, and `n` to `n+1`, where `n` is a target row
    // index.
    int64_t value_2 = target_key.value + 1;
    append_simple_instr(variant, type_Link, col_key, key.value, value_2, target_table_key); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::set_link(const Table* t, ColKey col_key, ObjKey key, ObjKey target_key,
                                                   Instruction variant)
{
    select_table(t); // Throws
    typedef _impl::TableFriend tf;
    auto target_table_key = tf::get_opposite_link_table_key(*t, col_key);
    m_encoder.set_link(col_key, key, target_key, target_table_key, variant); // Throws
}

inline bool TransactLogEncoder::set_null(ColKey col_key, ObjKey key, Instruction variant, size_t prior_num_rows)
{
    REALM_ASSERT_EX(variant == instr_Set || variant == instr_SetDefault || variant == instr_SetUnique, variant);
    if (REALM_UNLIKELY(variant == instr_SetUnique)) {
        append_simple_instr(variant, set_null_sentinel(), col_key, key.value, prior_num_rows); // Throws
    }
    else {
        append_simple_instr(variant, set_null_sentinel(), col_key, key.value); // Throws
    }
    return true;
}

inline void TransactLogConvenientEncoder::set_null(const Table* t, ColKey col_key, ObjKey key, Instruction variant)
{
    select_table(t); // Throws
    size_t prior_num_rows = (variant == instr_SetUnique ? t->size() : 0);
    m_encoder.set_null(col_key, key, variant, prior_num_rows); // Throws
}

inline bool TransactLogEncoder::nullify_link(ColKey col_key, ObjKey key, TableKey target_table_key)
{
    append_simple_instr(instr_NullifyLink, col_key, key.value, target_table_key); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::nullify_link(const Table* t, ColKey col_key, ObjKey key)
{
    select_table(t); // Throws
    typedef _impl::TableFriend tf;
    auto target_table_key = tf::get_opposite_link_table_key(*t, col_key);
    m_encoder.nullify_link(col_key, key, target_table_key); // Throws
}

inline bool TransactLogEncoder::insert_substring(ColKey col_key, ObjKey key, size_t pos, StringData value)
{
    append_simple_instr(instr_InsertSubstring, col_key, key.value, pos, value); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::insert_substring(const Table* t, ColKey col_key, ObjKey key, size_t pos,
                                                           StringData value)
{
    if (value.size() > 0) {
        select_table(t);                                          // Throws
        m_encoder.insert_substring(col_key, key, pos, value);     // Throws
    }
}

inline bool TransactLogEncoder::erase_substring(ColKey col_key, ObjKey key, size_t pos, size_t size)
{
    append_simple_instr(instr_EraseFromString, col_key, key.value, pos, size); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::erase_substring(const Table* t, ColKey col_key, ObjKey key, size_t pos,
                                                          size_t size)
{
    if (size > 0) {
        select_table(t);                                        // Throws
        m_encoder.erase_substring(col_key, key, pos, size);     // Throws
    }
}

inline bool TransactLogEncoder::list_set_int(size_t list_ndx, int64_t value)
{
    append_simple_instr(instr_ListSet, type_Int, list_ndx, value); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::list_set_int(const List<Int>& list, size_t list_ndx, int64_t value)
{
    select_list(list);                       // Throws
    m_encoder.list_set_int(list_ndx, value); // Throws
}

inline bool TransactLogEncoder::list_set_bool(size_t list_ndx, bool value)
{
    append_simple_instr(instr_ListSet, type_Bool, list_ndx, value); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::list_set_bool(const List<Bool>& list, size_t list_ndx, bool value)
{
    select_list(list);                        // Throws
    m_encoder.list_set_bool(list_ndx, value); // Throws
}

inline bool TransactLogEncoder::list_set_float(size_t list_ndx, float value)
{
    append_simple_instr(instr_ListSet, type_Float, list_ndx, value); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::list_set_float(const List<Float>& list, size_t list_ndx, float value)
{
    select_list(list);                         // Throws
    m_encoder.list_set_float(list_ndx, value); // Throws
}

inline bool TransactLogEncoder::list_set_double(size_t list_ndx, double value)
{
    append_simple_instr(instr_ListSet, type_Double, list_ndx, value); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::list_set_double(const List<Double>& list, size_t list_ndx, double value)
{
    select_list(list);                          // Throws
    m_encoder.list_set_double(list_ndx, value); // Throws
}

inline bool TransactLogEncoder::list_set_string(size_t list_ndx, StringData value)
{
    append_simple_instr(instr_ListSet, type_String, list_ndx, value); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::list_set_string(const List<String>& list, size_t list_ndx, StringData value)
{
    select_list(list);                          // Throws
    m_encoder.list_set_string(list_ndx, value); // Throws
}

inline bool TransactLogEncoder::list_set_binary(size_t list_ndx, BinaryData value)
{
    StringData value_2(value.data(), value.size());
    append_simple_instr(instr_ListSet, type_Binary, list_ndx, value_2); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::list_set_binary(const List<Binary>& list, size_t list_ndx, BinaryData value)
{
    select_list(list);                          // Throws
    m_encoder.list_set_binary(list_ndx, value); // Throws
}

inline bool TransactLogEncoder::list_set_timestamp(size_t list_ndx, Timestamp value)
{
    append_simple_instr(instr_ListSet, type_Timestamp, list_ndx, value.get_seconds(),
                        value.get_nanoseconds()); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::list_set_timestamp(const List<Timestamp>& list, size_t list_ndx,
                                                             Timestamp value)
{
    select_list(list);                             // Throws
    m_encoder.list_set_timestamp(list_ndx, value); // Throws
}

inline bool TransactLogEncoder::list_insert_int(size_t list_ndx, int64_t value, size_t prior_size)
{
    append_simple_instr(instr_ListInsert, type_Int, list_ndx, prior_size, value); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::list_insert_int(const List<Int>& list, size_t list_ndx, int64_t value)
{
    select_list(list);                                       // Throws
    m_encoder.list_insert_int(list_ndx, value, list.size()); // Throws
}

inline bool TransactLogEncoder::list_insert_bool(size_t list_ndx, bool value, size_t prior_size)
{
    append_simple_instr(instr_ListInsert, type_Bool, list_ndx, prior_size, value); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::list_insert_bool(const List<Bool>& list, size_t list_ndx, bool value)
{
    select_list(list);                                        // Throws
    m_encoder.list_insert_bool(list_ndx, value, list.size()); // Throws
}

inline bool TransactLogEncoder::list_insert_float(size_t list_ndx, float value, size_t prior_size)
{
    append_simple_instr(instr_ListInsert, type_Float, list_ndx, prior_size, value); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::list_insert_float(const List<Float>& list, size_t list_ndx, float value)
{
    select_list(list);                                         // Throws
    m_encoder.list_insert_float(list_ndx, value, list.size()); // Throws
}

inline bool TransactLogEncoder::list_insert_double(size_t list_ndx, double value, size_t prior_size)
{
    append_simple_instr(instr_ListInsert, type_Double, list_ndx, prior_size, value); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::list_insert_double(const List<Double>& list, size_t list_ndx, double value)
{
    select_list(list);                                          // Throws
    m_encoder.list_insert_double(list_ndx, value, list.size()); // Throws
}

inline bool TransactLogEncoder::list_insert_string(size_t list_ndx, StringData value, size_t prior_size)
{
    append_simple_instr(instr_ListInsert, type_String, list_ndx, prior_size, value); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::list_insert_string(const List<String>& list, size_t list_ndx,
                                                             StringData value)
{
    select_list(list);                                          // Throws
    m_encoder.list_insert_string(list_ndx, value, list.size()); // Throws
}

inline bool TransactLogEncoder::list_insert_binary(size_t list_ndx, BinaryData value, size_t prior_size)
{
    StringData value_2(value.data(), value.size());
    append_simple_instr(instr_ListInsert, type_Binary, list_ndx, prior_size, value_2); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::list_insert_binary(const List<Binary>& list, size_t list_ndx,
                                                             BinaryData value)
{
    select_list(list);                                          // Throws
    m_encoder.list_insert_binary(list_ndx, value, list.size()); // Throws
}

inline bool TransactLogEncoder::list_insert_timestamp(size_t list_ndx, Timestamp value, size_t prior_size)
{
    append_simple_instr(instr_ListInsert, type_Timestamp, list_ndx, prior_size, value.get_seconds(),
                        value.get_nanoseconds()); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::list_insert_timestamp(const List<Timestamp>& list, size_t list_ndx,
                                                                Timestamp value)
{
    select_list(list);                                             // Throws
    m_encoder.list_insert_timestamp(list_ndx, value, list.size()); // Throws
}

inline bool TransactLogEncoder::create_object(ObjKey key)
{
    append_simple_instr(instr_CreateObject, key.value); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::create_object(const Table* t, ObjKey key)
{
    select_table(t);                    // Throws
    m_encoder.create_object(key);       // Throws
}

inline bool TransactLogEncoder::remove_object(ObjKey key)
{
    append_simple_instr(instr_RemoveObject, key.value); // Throws
    return true;
}


inline void TransactLogConvenientEncoder::remove_object(const Table* t, ObjKey key)
{
    select_table(t);                    // Throws
    m_encoder.remove_object(key);       // Throws
}

inline bool TransactLogEncoder::add_search_index(ColKey col_key)
{
    append_simple_instr(instr_AddSearchIndex, col_key); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::add_search_index(const Table* t, ColKey col_key)
{
    select_table(t);                     // Throws
    m_encoder.add_search_index(col_key); // Throws
}


inline bool TransactLogEncoder::remove_search_index(ColKey col_key)
{
    append_simple_instr(instr_RemoveSearchIndex, col_key); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::remove_search_index(const Table* t, ColKey col_key)
{
    select_table(t);                        // Throws
    m_encoder.remove_search_index(col_key); // Throws
}

inline bool TransactLogEncoder::set_link_type(ColKey col_key, LinkType link_type)
{
    append_simple_instr(instr_SetLinkType, col_key, int(link_type)); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::set_link_type(const Table* t, ColKey col_key, LinkType link_type)
{
    select_table(t);                             // Throws
    m_encoder.set_link_type(col_key, link_type); // Throws
}


inline bool TransactLogEncoder::clear_table(size_t old_size)
{
    append_simple_instr(instr_ClearTable, old_size); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::clear_table(const Table* t, size_t prior_num_rows)
{
    select_table(t);         // Throws
    m_encoder.clear_table(prior_num_rows); // Throws
}

inline bool TransactLogEncoder::enumerate_string_column(ColKey col_key)
{
    append_simple_instr(instr_EnumerateStringColumn, col_key); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::enumerate_string_column(const Table* t, ColKey col_key)
{
    select_table(t);            // Throws
    m_encoder.enumerate_string_column(col_key); // Throws
}

inline bool TransactLogEncoder::list_insert_null(size_t list_ndx, size_t prior_size)
{
    append_simple_instr(instr_ListInsertNull, list_ndx, prior_size); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::list_insert_null(const ConstListBase& list, size_t list_ndx)
{
    select_list(list);                                 // Throws
    m_encoder.list_insert_null(list_ndx, list.size()); // Throws
}

inline bool TransactLogEncoder::list_set_link(size_t link_ndx, ObjKey key)
{
    append_simple_instr(instr_ListSet, type_Link, link_ndx, key.value); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::list_set_link(const List<ObjKey>& list, size_t link_ndx, ObjKey key)
{
    select_list(list);                      // Throws
    m_encoder.list_set_link(link_ndx, key); // Throws
}

inline bool TransactLogEncoder::list_insert_link(size_t link_ndx, ObjKey key, size_t prior_size)
{
    append_simple_instr(instr_ListInsert, type_Link, link_ndx, prior_size, key.value); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::list_insert_link(const List<ObjKey>& list, size_t link_ndx, ObjKey key)
{
    select_list(list);                                      // Throws
    m_encoder.list_insert_link(link_ndx, key, list.size()); // Throws
}

inline bool TransactLogEncoder::link_list_nullify(size_t link_ndx, size_t prior_size)
{
    append_simple_instr(instr_LinkListNullify, link_ndx, prior_size); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::link_list_nullify(const LinkList& list, size_t link_ndx)
{
    select_list(list);                                 // Throws
    size_t prior_size = list.size();                   // Instruction is emitted before the fact.
    m_encoder.link_list_nullify(link_ndx, prior_size); // Throws
}

inline bool TransactLogEncoder::list_move(size_t from_link_ndx, size_t to_link_ndx)
{
    REALM_ASSERT(from_link_ndx != to_link_ndx);
    append_simple_instr(instr_ListMove, from_link_ndx, to_link_ndx); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::list_move(const ConstListBase& list, size_t from_link_ndx,
                                                    size_t to_link_ndx)
{
    select_list(list);                               // Throws
    m_encoder.list_move(from_link_ndx, to_link_ndx); // Throws
}

inline bool TransactLogEncoder::list_swap(size_t link1_ndx, size_t link2_ndx)
{
    append_simple_instr(instr_ListSwap, link1_ndx, link2_ndx); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::list_swap(const ConstListBase& list, size_t link1_ndx, size_t link2_ndx)
{
    select_list(list);                         // Throws
    m_encoder.list_swap(link1_ndx, link2_ndx); // Throws
}

inline bool TransactLogEncoder::list_erase(size_t list_ndx, size_t prior_size)
{
    append_simple_instr(instr_ListErase, list_ndx, prior_size); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::list_erase(const ConstListBase& list, size_t link_ndx)
{
    select_list(list);                               // Throws
    size_t prior_size = list.size();                 // The instruction is emitted before the fact.
    m_encoder.list_erase(link_ndx, prior_size);      // Throws
}

inline bool TransactLogEncoder::list_clear(size_t old_list_size)
{
    append_simple_instr(instr_ListClear, old_list_size); // Throws
    return true;
}

inline void TransactLogConvenientEncoder::on_table_destroyed(const Table* t) noexcept
{
    if (m_selected_table == t)
        m_selected_table = nullptr;
}

inline void TransactLogConvenientEncoder::on_spec_destroyed(const Spec* s) noexcept
{
    if (m_selected_spec == s)
        m_selected_spec = nullptr;
}


inline TransactLogParser::TransactLogParser()
    : m_input_buffer(1024) // Throws
{
}


inline TransactLogParser::~TransactLogParser() noexcept
{
}


template <class InstructionHandler>
void TransactLogParser::parse(NoCopyInputStream& in, InstructionHandler& handler)
{
    m_input = &in;
    m_input_begin = m_input_end = nullptr;

    while (has_next())
        parse_one(handler); // Throws
}

template <class InstructionHandler>
void TransactLogParser::parse(InputStream& in, InstructionHandler& handler)
{
    NoCopyInputStreamAdaptor in_2(in, m_input_buffer.data(), m_input_buffer.size());
    parse(in_2, handler); // Throws
}

inline bool TransactLogParser::has_next() noexcept
{
    return m_input_begin != m_input_end || next_input_buffer();
}

template <class InstructionHandler>
void TransactLogParser::parse_one(InstructionHandler& handler)
{
    char instr_ch = 0; // silence a warning
    if (!read_char(instr_ch))
        parser_error(); // Throws
    //    std::cerr << "parsing " << util::promote(instr) << " @ " << std::hex << long(m_input_begin) << std::dec <<
    //    "\n";
    Instruction instr = Instruction(instr_ch);
    switch (instr) {
        case instr_SetDefault:
        case instr_SetUnique:
        case instr_Set: {
            int type = read_int<int>();          // Throws
            ColKey col_key = ColKey(read_int<size_t>()); // Throws
            ObjKey key(read_int<int64_t>());             // Throws
            size_t prior_num_rows = 0;
            if (REALM_UNLIKELY(instr == instr_SetUnique))
                prior_num_rows = read_int<size_t>(); // Throws

            if (type == TransactLogEncoder::set_null_sentinel()) {
                // Special case for set_null
                if (!handler.set_null(col_key, key, instr, prior_num_rows)) // Throws
                    parser_error();
                return;
            }

            switch (DataType(type)) {
                case type_Int: {
                    int_fast64_t value = read_int<int64_t>();                             // Throws
                    if (!handler.set_int(col_key, key, value, instr, prior_num_rows))     // Throws
                        parser_error();
                    return;
                }
                case type_Bool: {
                    bool value = read_bool();                              // Throws
                    if (!handler.set_bool(col_key, key, value, instr))     // Throws
                        parser_error();
                    return;
                }
                case type_Float: {
                    float value = read_float();                             // Throws
                    if (!handler.set_float(col_key, key, value, instr))     // Throws
                        parser_error();
                    return;
                }
                case type_Double: {
                    double value = read_double();                            // Throws
                    if (!handler.set_double(col_key, key, value, instr))     // Throws
                        parser_error();
                    return;
                }
                case type_String: {
                    StringData value = read_string(m_string_buffer);                         // Throws
                    if (!handler.set_string(col_key, key, value, instr, prior_num_rows))     // Throws
                        parser_error();
                    return;
                }
                case type_Binary: {
                    BinaryData value = read_binary(m_string_buffer);         // Throws
                    if (!handler.set_binary(col_key, key, value, instr))     // Throws
                        parser_error();
                    return;
                }
                case type_OldDateTime:
                    parser_error();
                    return;
                case type_Timestamp: {
                    int64_t seconds = read_int<int64_t>();     // Throws
                    int32_t nanoseconds = read_int<int32_t>(); // Throws
                    Timestamp value = Timestamp(seconds, nanoseconds);
                    if (!handler.set_timestamp(col_key, key, value, instr)) // Throws
                        parser_error();
                    return;
                }
                case type_OldTable:
                    parser_error();
                    return;
                case type_OldMixed:
                    parser_error();
                    return;
                case type_Link: {
                    int64_t value = read_int<int64_t>(); // Throws
                    // Map zero to realm::npos, and `n+1` to `n`, where `n` is a target row index.
                    ObjKey target_key = ObjKey(value - 1);
                    TableKey target_table_key = TableKey(read_int<int64_t>());                        // Throws
                    if (!handler.set_link(col_key, key, target_key, target_table_key, instr))         // Throws
                        parser_error();
                    return;
                }
                case type_LinkList: {
                    // Unsupported column type for Set.
                    parser_error();
                    return;
                }
            }
            parser_error();
            return;
        }
        case instr_ListSet: {
            int type = read_int<int>(); // Throws
            size_t list_ndx = read_int<size_t>();
            switch (DataType(type)) {
                case type_Int: {
                    int_fast64_t value = read_int<int64_t>();   // Throws
                    if (!handler.list_set_int(list_ndx, value)) // Throws
                        parser_error();
                    return;
                }
                case type_Bool: {
                    bool value = read_bool();                    // Throws
                    if (!handler.list_set_bool(list_ndx, value)) // Throws
                        parser_error();
                    return;
                }
                case type_Float: {
                    float value = read_float();                   // Throws
                    if (!handler.list_set_float(list_ndx, value)) // Throws
                        parser_error();
                    return;
                }
                case type_Double: {
                    double value = read_double();                  // Throws
                    if (!handler.list_set_double(list_ndx, value)) // Throws
                        parser_error();
                    return;
                }
                case type_String: {
                    StringData value = read_string(m_string_buffer); // Throws
                    if (!handler.list_set_string(list_ndx, value))   // Throws
                        parser_error();
                    return;
                }
                case type_Binary: {
                    BinaryData value = read_binary(m_string_buffer); // Throws
                    if (!handler.list_set_binary(list_ndx, value))   // Throws
                        parser_error();
                    return;
                }
                case type_Timestamp: {
                    int64_t seconds = read_int<int64_t>();     // Throws
                    int32_t nanoseconds = read_int<int32_t>(); // Throws
                    Timestamp value = Timestamp(seconds, nanoseconds);
                    if (!handler.list_set_timestamp(list_ndx, value)) // Throws
                        parser_error();
                    return;
                }
                case type_Link: {
                    ObjKey key = ObjKey(read_int<int64_t>());  // Throws
                    if (!handler.list_set_link(list_ndx, key)) // Throws
                        parser_error();
                    return;
                }
                default:
                    parser_error();
                    break;
            }
            return;
        }
        case instr_AddInteger: {
            ColKey col_key = ColKey(read_int<size_t>());   // Throws
            ObjKey key(read_int<int64_t>());               // Throws
            int_fast64_t value = read_int<int64_t>();      // Throws
            if (!handler.add_int(col_key, key, value))     // Throws
                parser_error();
            return;
        }
        case instr_NullifyLink: {
            ColKey col_key = ColKey(read_int<size_t>());                   // Throws
            ObjKey key(read_int<int64_t>());                               // Throws
            TableKey target_table_key = TableKey(read_int<size_t>());      // Throws
            if (!handler.nullify_link(col_key, key, target_table_key))     // Throws
                parser_error();
            return;
        }
        case instr_InsertSubstring: {
            ColKey col_key = ColKey(read_int<size_t>());                 // Throws
            ObjKey key(read_int<int64_t>());                             // Throws
            size_t pos = read_int<size_t>();                             // Throws
            StringData value = read_string(m_string_buffer);             // Throws
            if (!handler.insert_substring(col_key, key, pos, value))     // Throws
                parser_error();
            return;
        }
        case instr_EraseFromString: {
            ColKey col_key = ColKey(read_int<size_t>());               // Throws
            ObjKey key(read_int<int64_t>());                           // Throws
            size_t pos = read_int<size_t>();                           // Throws
            size_t size = read_int<size_t>();                          // Throws
            if (!handler.erase_substring(col_key, key, pos, size))     // Throws
                parser_error();
            return;
        }
        case instr_CreateObject: {
            ObjKey key(read_int<int64_t>()); // Throws
            if (!handler.create_object(key)) // Throws
                parser_error();
            return;
        }
        case instr_RemoveObject: {
            ObjKey key(read_int<int64_t>()); // Throws
            if (!handler.remove_object(key)) // Throws
                parser_error();
            return;
        }
        case instr_InsertEmptyRows:
        case instr_AddRowWithKey:
        case instr_EraseRows:
        case instr_SwapRows:
        case instr_MoveRow:
        case instr_MergeRows:
            parser_error();
            return;
        case instr_SelectTable: {
            int levels = read_int<int>(); // Throws
            REALM_ASSERT(levels == 0);
            TableKey key = TableKey(read_int<int64_t>());
            if (!handler.select_table(key)) // Throws
                parser_error();
            return;
        }
        case instr_ClearTable: {
            size_t old_size = read_int<size_t>();   // Throws
            if (!handler.clear_table(old_size)) // Throws
                parser_error();
            return;
        }
        case instr_ListInsertNull: {
            size_t list_ndx = read_int<size_t>();                    // Throws
            size_t prior_size = read_int<size_t>();                  // Throws
            if (!handler.list_insert_null(list_ndx, prior_size))     // Throws
                parser_error();
            return;
        }
        case instr_ListInsert: {
            int type = read_int<int>(); // Throws
            size_t list_ndx = read_int<size_t>();
            size_t prior_size = read_int<size_t>(); // Throws
            switch (DataType(type)) {
                case type_Int: {
                    int_fast64_t value = read_int<int64_t>();                  // Throws
                    if (!handler.list_insert_int(list_ndx, value, prior_size)) // Throws
                        parser_error();
                    return;
                }
                case type_Bool: {
                    bool value = read_bool();                                   // Throws
                    if (!handler.list_insert_bool(list_ndx, value, prior_size)) // Throws
                        parser_error();
                    return;
                }
                case type_Float: {
                    float value = read_float();                                  // Throws
                    if (!handler.list_insert_float(list_ndx, value, prior_size)) // Throws
                        parser_error();
                    return;
                }
                case type_Double: {
                    double value = read_double();                                 // Throws
                    if (!handler.list_insert_double(list_ndx, value, prior_size)) // Throws
                        parser_error();
                    return;
                }
                case type_String: {
                    StringData value = read_string(m_string_buffer);              // Throws
                    if (!handler.list_insert_string(list_ndx, value, prior_size)) // Throws
                        parser_error();
                    return;
                }
                case type_Binary: {
                    BinaryData value = read_binary(m_string_buffer);              // Throws
                    if (!handler.list_insert_binary(list_ndx, value, prior_size)) // Throws
                        parser_error();
                    return;
                }
                case type_Timestamp: {
                    int64_t seconds = read_int<int64_t>();     // Throws
                    int32_t nanoseconds = read_int<int32_t>(); // Throws
                    Timestamp value = Timestamp(seconds, nanoseconds);
                    if (!handler.list_insert_timestamp(list_ndx, value, prior_size)) // Throws
                        parser_error();
                    return;
                }
                case type_Link: {
                    ObjKey key = ObjKey(read_int<int64_t>());                 // Throws
                    if (!handler.list_insert_link(list_ndx, key, prior_size)) // Throws
                        parser_error();
                    return;
                }
                default:
                    parser_error();
                    break;
            }
            return;
        }
        case instr_ListMove: {
            size_t from_link_ndx = read_int<size_t>();               // Throws
            size_t to_link_ndx = read_int<size_t>();                 // Throws
            if (!handler.list_move(from_link_ndx, to_link_ndx))      // Throws
                parser_error();
            return;
        }
        case instr_ListSwap: {
            size_t link1_ndx = read_int<size_t>();             // Throws
            size_t link2_ndx = read_int<size_t>();             // Throws
            if (!handler.list_swap(link1_ndx, link2_ndx))      // Throws
                parser_error();
            return;
        }
        case instr_ListErase: {
            size_t link_ndx = read_int<size_t>();               // Throws
            size_t prior_size = read_int<size_t>();             // Throws
            if (!handler.list_erase(link_ndx, prior_size))      // Throws
                parser_error();
            return;
        }
        case instr_LinkListNullify: {
            size_t link_ndx = read_int<size_t>();                 // Throws
            size_t prior_size = read_int<size_t>();               // Throws
            if (!handler.link_list_nullify(link_ndx, prior_size)) // Throws
                parser_error();
            return;
        }
        case instr_ListClear: {
            size_t old_list_size = read_int<size_t>();   // Throws
            if (!handler.list_clear(old_list_size))      // Throws
                parser_error();
            return;
        }
        case instr_SelectList: {
            ColKey col_key = ColKey(read_int<size_t>());                             // Throws
            ObjKey key = ObjKey(read_int<int64_t>());                                // Throws
            if (!handler.select_list(col_key, key))                                  // Throws
                parser_error();
            return;
        }
        case instr_MoveColumn: {
            // FIXME: remove this in the next breaking change.
            // This instruction is no longer supported and not used by either
            // bindings or sync, so if we see it here, there was a problem parsing.
            parser_error();
            return;
        }
        case instr_AddSearchIndex: {
            ColKey col_key = ColKey(read_int<size_t>()); // Throws
            if (!handler.add_search_index(col_key))      // Throws
                parser_error();
            return;
        }
        case instr_RemoveSearchIndex: {
            ColKey col_key = ColKey(read_int<size_t>()); // Throws
            if (!handler.remove_search_index(col_key))   // Throws
                parser_error();
            return;
        }
        case instr_SetLinkType: {
            ColKey col_key = ColKey(read_int<size_t>()); // Throws
            int link_type = read_int<int>();     // Throws
            if (!is_valid_link_type(link_type))
                parser_error();
            if (!handler.set_link_type(col_key, LinkType(link_type))) // Throws
                parser_error();
            return;
        }
        case instr_InsertColumn:
        case instr_InsertNullableColumn: {
            ColKey col_key = ColKey(read_int<size_t>()); // Throws
            int type = read_int<int>();          // Throws
            if (!is_valid_data_type(type))
                parser_error();
            if (REALM_UNLIKELY(type == type_Link || type == type_LinkList))
                parser_error();
            StringData name = read_string(m_string_buffer); // Throws
            bool nullable = (Instruction(instr) == instr_InsertNullableColumn);
            if (!handler.insert_column(col_key, DataType(type), name, nullable, false)) // Throws
                parser_error();
            return;
        }
        case instr_InsertListColumn: {
            ColKey col_key = ColKey(read_int<size_t>()); // Throws
            int type = read_int<int>();          // Throws
            if (!is_valid_data_type(type))
                parser_error();
            if (REALM_UNLIKELY(type == type_Link || type == type_LinkList))
                parser_error();
            StringData name = read_string(m_string_buffer);                         // Throws
            if (!handler.insert_column(col_key, DataType(type), name, false, true)) // Throws
                parser_error();
            return;
        }
        case instr_InsertLinkColumn: {
            ColKey col_key = ColKey(read_int<size_t>()); // Throws
            int type = read_int<int>();          // Throws
            if (!is_valid_data_type(type))
                parser_error();
            if (REALM_UNLIKELY(type != type_Link && type != type_LinkList))
                parser_error();
            TableKey link_target_table_key = TableKey(read_int<size_t>()); // Throws
            ColKey backlink_col_key = ColKey(read_int<size_t>());          // Throws
            StringData name = read_string(m_string_buffer);    // Throws
            if (!handler.insert_link_column(col_key, DataType(type), name, link_target_table_key,
                                            backlink_col_key)) // Throws
                parser_error();
            return;
        }
        case instr_EraseColumn: {
            ColKey col_key = ColKey(read_int<size_t>()); // Throws
            if (!handler.erase_column(col_key))          // Throws
                parser_error();
            return;
        }
        case instr_EraseLinkColumn: {
            ColKey col_key = ColKey(read_int<size_t>());                                      // Throws
            TableKey link_target_table_key = TableKey(read_int<size_t>());                    // Throws
            ColKey backlink_col_key = ColKey(read_int<size_t>());                             // Throws
            if (!handler.erase_link_column(col_key, link_target_table_key, backlink_col_key)) // Throws
                parser_error();
            return;
        }
        case instr_RenameColumn: {
            ColKey col_key = ColKey(read_int<size_t>());    // Throws
            StringData name = read_string(m_string_buffer); // Throws
            if (!handler.rename_column(col_key, name))      // Throws
                parser_error();
            return;
        }
        case instr_InsertGroupLevelTable: {
            TableKey table_key = TableKey(read_int<size_t>());                  // Throws
            size_t num_tables = read_int<size_t>();                             // Throws
            StringData name = read_string(m_string_buffer);                     // Throws
            if (!handler.insert_group_level_table(table_key, num_tables, name)) // Throws
                parser_error();
            return;
        }
        case instr_EraseGroupLevelTable: {
            TableKey table_key = TableKey(read_int<size_t>());                 // Throws
            size_t prior_num_tables = read_int<size_t>();                      // Throws
            if (!handler.erase_group_level_table(table_key, prior_num_tables)) // Throws
                parser_error();
            return;
        }
        case instr_RenameGroupLevelTable: {
            TableKey table_key = TableKey(read_int<size_t>());          // Throws
            StringData new_name = read_string(m_string_buffer);         // Throws
            if (!handler.rename_group_level_table(table_key, new_name)) // Throws
                parser_error();
            return;
        }
        case instr_MoveGroupLevelTable: {
            // This instruction is no longer supported and not used by either
            // bindings or sync, so if we see it here, there was a problem parsing.
            // FIXME: remove this in the next breaking change.
            parser_error();
            return;
        }
        case instr_EnumerateStringColumn: {
            ColKey col_key = ColKey(read_int<size_t>());   // Throws
            if (!handler.enumerate_string_column(col_key)) // Throws
                parser_error();
            return;
        }
    }

    throw BadTransactLog();
}


template <class T>
T TransactLogParser::read_int()
{
    T value = 0;
    int part = 0;
    const int max_bytes = (std::numeric_limits<T>::digits + 1 + 6) / 7;
    for (int i = 0; i != max_bytes; ++i) {
        char c;
        if (!read_char(c))
            goto bad_transact_log;
        part = static_cast<unsigned char>(c);
        if (0xFF < part)
            goto bad_transact_log; // Only the first 8 bits may be used in each byte
        if ((part & 0x80) == 0) {
            T p = part & 0x3F;
            if (util::int_shift_left_with_overflow_detect(p, i * 7))
                goto bad_transact_log;
            value |= p;
            break;
        }
        if (i == max_bytes - 1)
            goto bad_transact_log; // Too many bytes
        value |= T(part & 0x7F) << (i * 7);
    }
    if (part & 0x40) {
        // The real value is negative. Because 'value' is positive at
        // this point, the following negation is guaranteed by C++11
        // to never overflow. See C99+TC3 section 6.2.6.2 paragraph 2.
        REALM_DIAG_PUSH();
        REALM_DIAG_IGNORE_UNSIGNED_MINUS();
        value = -value;
        REALM_DIAG_POP();
        if (util::int_subtract_with_overflow_detect(value, 1))
            goto bad_transact_log;
    }
    return value;

bad_transact_log:
    throw BadTransactLog();
}


inline void TransactLogParser::read_bytes(char* data, size_t size)
{
    for (;;) {
        const size_t avail = m_input_end - m_input_begin;
        if (size <= avail)
            break;
        realm::safe_copy_n(m_input_begin, avail, data);
        if (!next_input_buffer())
            throw BadTransactLog();
        data += avail;
        size -= avail;
    }
    const char* to = m_input_begin + size;
    realm::safe_copy_n(m_input_begin, size, data);
    m_input_begin = to;
}


inline BinaryData TransactLogParser::read_buffer(util::StringBuffer& buf, size_t size)
{
    const size_t avail = m_input_end - m_input_begin;
    if (avail >= size) {
        m_input_begin += size;
        return BinaryData(m_input_begin - size, size);
    }

    buf.clear();
    buf.resize(size); // Throws
    read_bytes(buf.data(), size);
    return BinaryData(buf.data(), size);
}


inline bool TransactLogParser::read_bool()
{
    return read_int<char>();
}


inline float TransactLogParser::read_float()
{
    static_assert(std::numeric_limits<float>::is_iec559 &&
                      sizeof(float) * std::numeric_limits<unsigned char>::digits == 32,
                  "Unsupported 'float' representation");
    float value;
    read_bytes(reinterpret_cast<char*>(&value), sizeof value); // Throws
    return value;
}


inline double TransactLogParser::read_double()
{
    static_assert(std::numeric_limits<double>::is_iec559 &&
                      sizeof(double) * std::numeric_limits<unsigned char>::digits == 64,
                  "Unsupported 'double' representation");
    double value;
    read_bytes(reinterpret_cast<char*>(&value), sizeof value); // Throws
    return value;
}


inline StringData TransactLogParser::read_string(util::StringBuffer& buf)
{
    size_t size = read_int<size_t>(); // Throws

    if (size > Table::max_string_size)
        parser_error();

    BinaryData buffer = read_buffer(buf, size);
    return StringData{buffer.data(), size};
}

inline Timestamp TransactLogParser::read_timestamp()
{
    int64_t seconds = read_int<int64_t>();     // Throws
    int32_t nanoseconds = read_int<int32_t>(); // Throws
    return Timestamp(seconds, nanoseconds);
}


inline BinaryData TransactLogParser::read_binary(util::StringBuffer& buf)
{
    size_t size = read_int<size_t>(); // Throws

    return read_buffer(buf, size);
}


inline bool TransactLogParser::next_input_buffer()
{
    return m_input->next_block(m_input_begin, m_input_end);
}


inline bool TransactLogParser::read_char(char& c)
{
    if (m_input_begin == m_input_end && !next_input_buffer())
        return false;
    c = *m_input_begin++;
    return true;
}


inline bool TransactLogParser::is_valid_data_type(int type)
{
    switch (DataType(type)) {
        case type_Int:
        case type_Bool:
        case type_Float:
        case type_Double:
        case type_String:
        case type_Binary:
        case type_OldDateTime:
        case type_Timestamp:
        case type_Link:
        case type_LinkList:
            return true;
        case type_OldTable:
        case type_OldMixed:
            return false;
    }
    return false;
}


inline bool TransactLogParser::is_valid_link_type(int type)
{
    switch (LinkType(type)) {
        case link_Strong:
        case link_Weak:
            return true;
    }
    return false;
}


class TransactReverser {
public:
    bool select_table(TableKey key)
    {
        sync_table();
        m_encoder.select_table(key);
        m_pending_ts_instr = get_inst();
        return true;
    }

    bool select_descriptor(size_t levels, const size_t* path)
    {
        sync_descriptor();
        m_encoder.select_descriptor(levels, path);
        m_pending_ds_instr = get_inst();
        return true;
    }

    bool insert_group_level_table(TableKey table_key, size_t num_tables, StringData)
    {
        sync_table();
        m_encoder.erase_group_level_table(table_key, num_tables + 1);
        append_instruction();
        return true;
    }

    bool erase_group_level_table(TableKey table_key, size_t num_tables)
    {
        sync_table();
        m_encoder.insert_group_level_table(table_key, num_tables - 1, "");
        append_instruction();
        return true;
    }

    bool rename_group_level_table(TableKey, StringData)
    {
        sync_table();
        return true;
    }

    bool enumerate_string_column(ColKey)
    {
        return true; // No-op
    }

    bool create_object(ObjKey key)
    {
        m_encoder.remove_object(key); // Throws
        append_instruction();
        return true;
    }

    bool remove_object(ObjKey key)
    {
        m_encoder.create_object(key); // Throws
        append_instruction();
        return true;
    }

    bool set_int(ColKey col_key, ObjKey key, int_fast64_t value, Instruction variant, size_t prior_num_rows)
    {
        m_encoder.set_int(col_key, key, value, variant, prior_num_rows);
        append_instruction();
        return true;
    }

    bool add_int(ColKey col_key, ObjKey key, int_fast64_t value)
    {
        m_encoder.add_int(col_key, key, -value);
        append_instruction();
        return true;
    }

    bool set_bool(ColKey col_key, ObjKey key, bool value, Instruction variant)
    {
        m_encoder.set_bool(col_key, key, value, variant);
        append_instruction();
        return true;
    }

    bool set_float(ColKey col_key, ObjKey key, float value, Instruction variant)
    {
        m_encoder.set_float(col_key, key, value, variant);
        append_instruction();
        return true;
    }

    bool set_double(ColKey col_key, ObjKey key, double value, Instruction variant)
    {
        m_encoder.set_double(col_key, key, value, variant);
        append_instruction();
        return true;
    }

    bool set_string(ColKey col_key, ObjKey key, StringData value, Instruction variant, size_t prior_num_rows)
    {
        m_encoder.set_string(col_key, key, value, variant, prior_num_rows);
        append_instruction();
        return true;
    }

    bool set_binary(ColKey col_key, ObjKey key, BinaryData value, Instruction variant)
    {
        m_encoder.set_binary(col_key, key, value, variant);
        append_instruction();
        return true;
    }

    bool set_timestamp(ColKey col_key, ObjKey key, Timestamp value, Instruction variant)
    {
        m_encoder.set_timestamp(col_key, key, value, variant);
        append_instruction();
        return true;
    }

    bool set_null(ColKey col_key, ObjKey key, Instruction variant, size_t prior_num_rows)
    {
        m_encoder.set_null(col_key, key, variant, prior_num_rows);
        append_instruction();
        return true;
    }

    bool set_link(ColKey col_key, ObjKey key, ObjKey target_key_value, TableKey target_table_key, Instruction variant)
    {
        m_encoder.set_link(col_key, key, target_key_value, target_table_key, variant);
        append_instruction();
        return true;
    }

    bool insert_substring(ColKey, ObjKey, size_t, StringData)
    {
        return true; // No-op
    }

    bool erase_substring(ColKey, ObjKey, size_t, size_t)
    {
        return true; // No-op
    }

    bool list_set_int(size_t ndx, int64_t value)
    {
        m_encoder.list_set_int(ndx, value);
        append_instruction();
        return true;
    }

    bool list_set_bool(size_t ndx, bool value)
    {
        m_encoder.list_set_bool(ndx, value);
        append_instruction();
        return true;
    }

    bool list_set_float(size_t ndx, float value)
    {
        m_encoder.list_set_float(ndx, value);
        append_instruction();
        return true;
    }

    bool list_set_double(size_t ndx, double value)
    {
        m_encoder.list_set_double(ndx, value);
        append_instruction();
        return true;
    }

    bool list_set_string(size_t ndx, StringData value)
    {
        m_encoder.list_set_string(ndx, value);
        append_instruction();
        return true;
    }

    bool list_set_binary(size_t ndx, BinaryData value)
    {
        m_encoder.list_set_binary(ndx, value);
        append_instruction();
        return true;
    }

    bool list_set_timestamp(size_t ndx, Timestamp value)
    {
        m_encoder.list_set_timestamp(ndx, value);
        append_instruction();
        return true;
    }

    bool list_insert_int(size_t ndx, int64_t, size_t prior_size)
    {
        m_encoder.list_erase(ndx, prior_size);
        append_instruction();
        return true;
    }

    bool list_insert_bool(size_t ndx, bool, size_t prior_size)
    {
        m_encoder.list_erase(ndx, prior_size);
        append_instruction();
        return true;
    }

    bool list_insert_float(size_t ndx, float, size_t prior_size)
    {
        m_encoder.list_erase(ndx, prior_size);
        append_instruction();
        return true;
    }

    bool list_insert_double(size_t ndx, double, size_t prior_size)
    {
        m_encoder.list_erase(ndx, prior_size);
        append_instruction();
        return true;
    }

    bool list_insert_string(size_t ndx, StringData, size_t prior_size)
    {
        m_encoder.list_erase(ndx, prior_size);
        append_instruction();
        return true;
    }

    bool list_insert_binary(size_t ndx, BinaryData, size_t prior_size)
    {
        m_encoder.list_erase(ndx, prior_size);
        append_instruction();
        return true;
    }

    bool list_insert_timestamp(size_t ndx, Timestamp, size_t prior_size)
    {
        m_encoder.list_erase(ndx, prior_size);
        append_instruction();
        return true;
    }

    bool clear_table(size_t old_size)
    {
        while (old_size--) {
            m_encoder.create_object(null_key);
            append_instruction();
        }
        return true;
    }

    bool add_search_index(ColKey)
    {
        return true; // No-op
    }

    bool remove_search_index(ColKey)
    {
        return true; // No-op
    }

    bool set_link_type(ColKey, LinkType)
    {
        return true; // No-op
    }

    bool insert_link_column(ColKey col_key, DataType, StringData, TableKey target_table_key, ColKey backlink_col_key)
    {
        m_encoder.erase_link_column(col_key, target_table_key, backlink_col_key);
        append_instruction();
        return true;
    }

    bool erase_link_column(ColKey col_key, TableKey target_table_key, ColKey backlink_col_key)
    {
        DataType type = type_Link; // The real type of the column doesn't matter here,
        // but the encoder asserts that it's actually a link type.
        m_encoder.insert_link_column(col_key, type, "", target_table_key, backlink_col_key);
        append_instruction();
        return true;
    }

    bool insert_column(ColKey col_key, DataType, StringData, bool, bool)
    {
        m_encoder.erase_column(col_key);
        append_instruction();
        return true;
    }

    bool erase_column(ColKey col_key)
    {
        m_encoder.insert_column(col_key, DataType(), "");
        append_instruction();
        return true;
    }

    bool rename_column(ColKey, StringData)
    {
        return true; // No-op
    }

    bool select_list(ColKey col_key, ObjKey key)
    {
        m_encoder.select_list(col_key, key);
        append_instruction();
        return true;
    }

    bool list_set_link(size_t list_ndx, ObjKey key)
    {
        m_encoder.list_set_link(list_ndx, key);
        append_instruction();
        return true;
    }

    bool list_insert_link(size_t list_ndx, ObjKey, size_t prior_size)
    {
        m_encoder.list_erase(list_ndx, prior_size);
        append_instruction();
        return true;
    }

    bool list_insert_null(size_t ndx, size_t prior_size)
    {
        m_encoder.list_erase(ndx, prior_size + 1);
        append_instruction();
        return true;
    }

    bool list_move(size_t from_link_ndx, size_t to_link_ndx)
    {
        m_encoder.list_move(from_link_ndx, to_link_ndx);
        append_instruction();
        return true;
    }

    bool list_swap(size_t link1_ndx, size_t link2_ndx)
    {
        m_encoder.list_swap(link1_ndx, link2_ndx);
        append_instruction();
        return true;
    }

    bool list_erase(size_t list_ndx, size_t prior_size)
    {
        m_encoder.list_insert_null(list_ndx, prior_size - 1);
        append_instruction();
        return true;
    }

    bool list_clear(size_t old_list_size)
    {
        // Append in reverse order because the reversed log is itself applied
        // in reverse, and this way it generates all back-insertions rather than
        // all front-insertions
        for (size_t i = old_list_size; i > 0; --i) {
            m_encoder.list_insert_null(i - 1, old_list_size - i);
            append_instruction();
        }
        return true;
    }

    bool nullify_link(ColKey col_key, ObjKey key, TableKey target_table_key)
    {
        // FIXME: Is zero this right value to pass here, or should
        // TransactReverser::nullify_link() also have taken a
        // `target_group_level_ndx` argument.
        m_encoder.set_link(col_key, key, null_key, target_table_key);
        append_instruction();
        return true;
    }

    bool link_list_nullify(size_t link_ndx, size_t prior_size)
    {
        m_encoder.list_insert_null(link_ndx, prior_size - 1);
        append_instruction();
        return true;
    }

private:
    _impl::TransactLogBufferStream m_buffer;
    _impl::TransactLogEncoder m_encoder{m_buffer};
    struct Instr {
        size_t begin;
        size_t end;
    };
    std::vector<Instr> m_instructions;
    size_t current_instr_start = 0;
    Instr m_pending_ts_instr{0, 0};
    Instr m_pending_ds_instr{0, 0};
    Instr m_pending_lv_instr{0, 0};

    Instr get_inst()
    {
        Instr instr;
        instr.begin = current_instr_start;
        current_instr_start = transact_log_size();
        instr.end = current_instr_start;
        return instr;
    }

    size_t transact_log_size() const
    {
        REALM_ASSERT_3(m_encoder.write_position(), >=, m_buffer.transact_log_data());
        return m_encoder.write_position() - m_buffer.transact_log_data();
    }

    void append_instruction()
    {
        m_instructions.push_back(get_inst());
    }

    void append_instruction(Instr instr)
    {
        m_instructions.push_back(instr);
    }

    void sync_select(Instr& pending_instr)
    {
        if (pending_instr.begin != pending_instr.end) {
            append_instruction(pending_instr);
            pending_instr = {0, 0};
        }
    }

    void sync_linkview()
    {
        sync_select(m_pending_lv_instr);
    }

    void sync_descriptor()
    {
        sync_linkview();
        sync_select(m_pending_ds_instr);
    }

    void sync_table()
    {
        sync_descriptor();
        sync_select(m_pending_ts_instr);
    }

    friend class ReversedNoCopyInputStream;
};


class ReversedNoCopyInputStream : public NoCopyInputStream {
public:
    ReversedNoCopyInputStream(TransactReverser& reverser)
        : m_instr_order(reverser.m_instructions)
    {
        // push any pending select_table or select_descriptor into the buffer
        reverser.sync_table();

        m_buffer = reverser.m_buffer.transact_log_data();
        m_current = m_instr_order.size();
    }

    bool next_block(const char*& begin, const char*& end) override
    {
        if (m_current != 0) {
            m_current--;
            begin = m_buffer + m_instr_order[m_current].begin;
            end = m_buffer + m_instr_order[m_current].end;
            return (end > begin);
        }
        return false;
    }

private:
    const char* m_buffer;
    std::vector<TransactReverser::Instr>& m_instr_order;
    size_t m_current;
};

} // namespace _impl
} // namespace realm

#endif // REALM_IMPL_TRANSACT_LOG_HPP
