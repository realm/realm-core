#include <stdexcept>
#include <utility>
#include <ostream>
#include <iomanip>

#include <tightdb/util/string_buffer.hpp>
#include <tightdb/util/unique_ptr.hpp>
#include <tightdb/table.hpp>
#include <tightdb/descriptor.hpp>
#include <tightdb/group_shared.hpp>
#include <tightdb/replication.hpp>

using namespace std;
using namespace tightdb;
using namespace tightdb::util;

namespace {

const size_t init_subtab_path_buf_levels = 2; // 2 table levels (soft limit)
const size_t init_subtab_path_buf_size = 2*init_subtab_path_buf_levels - 1;

} // anonymous namespace


Replication::Replication(): m_selected_table(0), m_selected_spec(0)
{
    m_subtab_path_buf.set_size(init_subtab_path_buf_size); // Throws
}


Group& Replication::get_group(SharedGroup& sg) TIGHTDB_NOEXCEPT
{
    return sg.m_group;
}

void Replication::set_replication(Group& group, Replication* repl) TIGHTDB_NOEXCEPT
{
    group.set_replication(repl);
}


Replication::version_type Replication::get_current_version(SharedGroup& sg)
{
    return sg.get_current_version();
}


void Replication::select_table(const Table* table)
{
    size_t* begin;
    size_t* end;
    for (;;) {
        begin = m_subtab_path_buf.data();
        end   = begin + m_subtab_path_buf.size();
        end = _impl::TableFriend::record_subtable_path(*table, begin, end);
        if (end)
            break;
        size_t new_size = m_subtab_path_buf.size();
        if (int_multiply_with_overflow_detect(new_size, 2))
            throw runtime_error("Too many subtable nesting levels");
        m_subtab_path_buf.set_size(new_size); // Throws
    }
    char* buf;
    const int max_elems_per_chunk = 8; // FIXME: Use smaller number when compiling in debug mode
    transact_log_reserve(&buf, 1 + (1+max_elems_per_chunk)*max_enc_bytes_per_int); // Throws
    *buf++ = 'T';
    TIGHTDB_ASSERT(1 <= end - begin);
    const size_t level = (end - begin) / 2;
    buf = encode_int(buf, level);
    for (;;) {
        for (int i = 0; i < max_elems_per_chunk; ++i) {
            buf = encode_int(buf, *--end);
            if (begin == end)
                goto good;
        }
        transact_log_advance(buf);
        transact_log_reserve(&buf, max_elems_per_chunk*max_enc_bytes_per_int); // Throws
    }

good:
    transact_log_advance(buf);
    m_selected_spec  = 0;
    m_selected_table = table;
}


void Replication::select_desc(const Descriptor& desc)
{
    check_table(&*desc.m_root_table);
    size_t* begin;
    size_t* end;
    for (;;) {
        begin = m_subtab_path_buf.data();
        end   = begin + m_subtab_path_buf.size();
        begin = desc.record_subdesc_path(begin, end);
        if (begin)
            break;
        size_t new_size = m_subtab_path_buf.size();
        if (int_multiply_with_overflow_detect(new_size, 2))
            throw runtime_error("Too many table type descriptor nesting levels");
        m_subtab_path_buf.set_size(new_size); // Throws
    }
    char* buf;
    int max_elems_per_chunk = 8; // FIXME: Use smaller number when compiling in debug mode
    transact_log_reserve(&buf, 1 + (1+max_elems_per_chunk)*max_enc_bytes_per_int); // Throws
    *buf++ = 'S';
    size_t level = end - begin;
    buf = encode_int(buf, level);
    if (begin == end)
        goto good;
    for (;;) {
        for (int i = 0; i < max_elems_per_chunk; ++i) {
            buf = encode_int(buf, *begin);
            if (++begin == end)
                goto good;
        }
        transact_log_advance(buf);
        transact_log_reserve(&buf, max_elems_per_chunk*max_enc_bytes_per_int); // Throws
    }

good:
    transact_log_advance(buf);
    m_selected_spec = desc.m_spec;
}


struct Replication::TransactLogApplier {
    TransactLogApplier(InputStream& transact_log, Group& group):
        m_input(transact_log), m_group(group), m_input_buffer(0)
    {
    }

    ~TransactLogApplier()
    {
        delete[] m_input_buffer;
    }

    void set_apply_log(ostream* log)
    {
        m_log = log;
        if (m_log)
            *m_log << boolalpha;
    }

    void apply();

private:
    InputStream& m_input;
    Group& m_group;
    static const size_t m_input_buffer_size = 4096; // FIXME: Use smaller number when compiling in debug mode
    char* m_input_buffer;
    const char* m_input_begin;
    const char* m_input_end;
    TableRef m_table;
    StringBuffer m_string_buffer;
    ostream* m_log;

    // Returns false if no more input was available
    bool fill_input_buffer()
    {
        const size_t n = m_input.read(m_input_buffer, m_input_buffer_size);
        if (n == 0)
            return false;
        m_input_begin = m_input_buffer;
        m_input_end   = m_input_buffer + n;
        return true;
    }

    // Returns false if no input was available
    bool read_char(char& c)
    {
        if (m_input_begin == m_input_end && !fill_input_buffer())
            return false;
        c = *m_input_begin++;
        return true;
    }

    template<class T> T read_int();

    void read_bytes(char* data, size_t size);

    float read_float();
    double read_double();

    void read_string(StringBuffer&);

    template<bool insert> void set_or_insert(size_t column_ndx, size_t ndx);

    bool is_valid_column_type(int type)
    {
        switch (type) {
            case type_Int:
            case type_Bool:
            case type_Float:
            case type_Double:
            case type_String:
            case type_Binary:
            case type_DateTime:
            case type_Table:
            case type_Mixed:
                return true;
        }
        return false;
    }
};


template<class T> T Replication::TransactLogApplier::read_int()
{
    T value = 0;
    int part;
    const int max_bytes = (numeric_limits<T>::digits+1+6)/7;
    for (int i = 0; i != max_bytes; ++i) {
        char c;
        if (!read_char(c))
            goto bad_transact_log;
        part = static_cast<unsigned char>(c);
        if (0xFF < part)
            goto bad_transact_log; // Only the first 8 bits may be used in each byte
        if ((part & 0x80) == 0) {
            T p = part & 0x3F;
            if (int_shift_left_with_overflow_detect(p, i*7))
                goto bad_transact_log;
            value |= p;
            break;
        }
        if (i == max_bytes-1)
            goto bad_transact_log; // Too many bytes
        value |= T(part & 0x7F) << (i*7);
    }
    if (part & 0x40) {
        // The real value is negative. Because 'value' is positive at
        // this point, the following negation is guaranteed by C++11
        // to never overflow. See C99+TC3 section 6.2.6.2 paragraph 2.
        value = -value;
        if (int_subtract_with_overflow_detect(value, 1))
            goto bad_transact_log;
    }
    return value;

  bad_transact_log:
    throw BadTransactLog();
}


inline void Replication::TransactLogApplier::read_bytes(char* data, size_t size)
{
    for (;;) {
        const size_t avail = m_input_end - m_input_begin;
        if (size <= avail)
            break;
        const char* to = m_input_begin + avail;
        copy(m_input_begin, to, data);
        if (!fill_input_buffer())
            throw BadTransactLog();
        data += avail;
        size -= avail;
    }
    const char* to = m_input_begin + size;
    copy(m_input_begin, to, data);
    m_input_begin = to;
}


float Replication::TransactLogApplier::read_float()
{
    TIGHTDB_STATIC_ASSERT(numeric_limits<float>::is_iec559 &&
                          sizeof (float) * numeric_limits<unsigned char>::digits == 32,
                          "Unsupported 'float' representation");
    float value;
    read_bytes(reinterpret_cast<char*>(&value), sizeof value); // Throws
    return value;
}


double Replication::TransactLogApplier::read_double()
{
    TIGHTDB_STATIC_ASSERT(numeric_limits<double>::is_iec559 &&
                          sizeof (double) * numeric_limits<unsigned char>::digits == 64,
                          "Unsupported 'double' representation");
    double value;
    read_bytes(reinterpret_cast<char*>(&value), sizeof value); // Throws
    return value;
}


void Replication::TransactLogApplier::read_string(StringBuffer& buf)
{
    buf.clear();
    size_t size = read_int<size_t>(); // Throws
    buf.resize(size); // Throws
    read_bytes(buf.data(), size);
}


template<bool insert>
void Replication::TransactLogApplier::set_or_insert(size_t column_ndx, size_t ndx)
{
    switch (m_table->get_column_type(column_ndx)) {
        case type_Int: {
            int64_t value = read_int<int64_t>(); // Throws
#ifdef TIGHTDB_DEBUG
            if (m_log) {
                if (insert) {
                    *m_log << "table->insert_int("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
                }
                else {
                    *m_log << "table->set_int("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
                }
            }
#endif
            if (insert) {
                m_table->insert_int(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
            }
            else {
                m_table->set_int(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
            }
            return;
        }
        case type_Bool: {
            bool value = read_int<bool>(); // Throws
#ifdef TIGHTDB_DEBUG
            if (m_log) {
                if (insert) {
                    *m_log << "table->insert_bool("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
                }
                else {
                    *m_log << "table->set_bool("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
                }
            }
#endif
            if (insert) {
                m_table->insert_bool(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
            }
            else {
                m_table->set_bool(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
            }
            return;
        }
        case type_Float: {
            float value = read_float(); // Throws
#ifdef TIGHTDB_DEBUG
            if (m_log) {
                if (insert) {
                    *m_log << "table->insert_float("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
                }
                else {
                    *m_log << "table->set_float("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
                }
            }
#endif
            if (insert) {
                m_table->insert_float(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
            }
            else {
                m_table->set_float(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
            }
            return;
        }
        case type_Double: {
            double value = read_double(); // Throws
#ifdef TIGHTDB_DEBUG
            if (m_log) {
                if (insert) {
                    *m_log << "table->insert_double("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
                }
                else {
                    *m_log << "table->set_double("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
                }
            }
#endif
            if (insert) {
                m_table->insert_double(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
            }
            else {
                m_table->set_double(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
            }
            return;
        }
        case type_String: {
            read_string(m_string_buffer); // Throws
            StringData value(m_string_buffer.data(), m_string_buffer.size());
#ifdef TIGHTDB_DEBUG
            if (m_log) {
                if (insert) {
                    *m_log << "table->insert_string("<<column_ndx<<", "<<ndx<<", \""<<value<<"\")\n";
                }
                else {
                    *m_log << "table->set_string("<<column_ndx<<", "<<ndx<<", \""<<value<<"\")\n";
                }
            }
#endif
            if (insert) {
                m_table->insert_string(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
            }
            else {
                m_table->set_string(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
            }
            return;
        }
        case type_Binary: {
            read_string(m_string_buffer); // Throws
            BinaryData value(m_string_buffer.data(), m_string_buffer.size());
#ifdef TIGHTDB_DEBUG
            if (m_log) {
                if (insert) {
                    *m_log << "table->insert_binary("<<column_ndx<<", "<<ndx<<", ...)\n";
                }
                else {
                    *m_log << "table->set_binary("<<column_ndx<<", "<<ndx<<", ...)\n";
                }
            }
#endif
            if (insert) {
                m_table->insert_binary(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
            }
            else {
                m_table->set_binary(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
            }
            return;
        }
        case type_DateTime: {
            time_t value = read_int<time_t>(); // Throws
#ifdef TIGHTDB_DEBUG
            if (m_log) {
                if (insert) {
                    *m_log << "table->insert_datetime("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
                }
                else {
                    *m_log << "table->set_datetime("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
                }
            }
#endif
            if (insert) {
                m_table->insert_datetime(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
            }
            else {
                m_table->set_datetime(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
            }
            return;
        }
        case type_Table: {
#ifdef TIGHTDB_DEBUG
            if (m_log) {
                if (insert) {
                    *m_log << "table->insert_subtable("<<column_ndx<<", "<<ndx<<")\n";
                }
                else {
                    *m_log << "table->clear_subtable("<<column_ndx<<", "<<ndx<<")\n";
                }
            }
#endif
            if (insert) {
                m_table->insert_subtable(column_ndx, ndx); // FIXME: Memory allocation failure!!!
            }
            else {
                m_table->clear_subtable(column_ndx, ndx); // FIXME: Memory allocation failure!!!
            }
            return;
        }
        case type_Mixed: {
            DataType type = DataType(read_int<int>()); // Throws
            Mixed mixed;
            switch (type) {
                case type_Int: {
                    int64_t value = read_int<int64_t>(); // Throws
                    mixed = value;
                    goto mixed;
                }
                case type_Bool: {
                    bool value = read_int<bool>(); // Throws
                    mixed = value;
                    goto mixed;
                }
                case type_Float: {
                    float value = read_float(); // Throws
                    mixed = value;
                    goto mixed;
                }
                case type_Double: {
                    double value = read_double(); // Throws
                    mixed = value;
                    goto mixed;
                }
                case type_DateTime: {
                    time_t value = read_int<time_t>(); // Throws
                    mixed = DateTime(value);
                    goto mixed;
                }
                case type_String: {
                    read_string(m_string_buffer); // Throws
                    StringData value(m_string_buffer.data(), m_string_buffer.size());
                    mixed = value;
                    goto mixed;
                }
                case type_Binary: {
                    read_string(m_string_buffer); // Throws
                    BinaryData value(m_string_buffer.data(), m_string_buffer.size());
                    mixed = value;
                    goto mixed;
                }
                case type_Table: {
                    mixed = Mixed::subtable_tag();
                    goto mixed;
                }
                case type_Mixed:
                    break;
            }
            break;

          mixed:
#ifdef TIGHTDB_DEBUG
            if (m_log) {
                if (insert) {
                    *m_log << "table->insert_mixed("<<column_ndx<<", "<<ndx<<", \""<<mixed<<"\")\n";
                }
                else {
                    *m_log << "table->set_mixed("<<column_ndx<<", "<<ndx<<", \""<<mixed<<"\")\n";
                }
            }
#endif
            if (insert) {
                m_table->insert_mixed(column_ndx, ndx, mixed); // FIXME: Memory allocation failure!!!
            }
            else {
                m_table->set_mixed(column_ndx, ndx, mixed); // FIXME: Memory allocation failure!!!
            }
            return;
        }
    }
    throw BadTransactLog();
}


void Replication::TransactLogApplier::apply()
{
    if (!m_input_buffer)
        m_input_buffer = new char[m_input_buffer_size]; // Throws
    m_input_begin = m_input_end = m_input_buffer;

    // FIXME: Problem: The modifying methods of group, table, and descriptor generally throw.
    DescriptorRef desc;
    for (;;) {
        char instr;
        if (!read_char(instr))
            break;
//cerr << "["<<instr<<"]";
        switch (instr) {
            case 's': { // Set value
                size_t column_ndx = read_int<size_t>(); // Throws
                size_t ndx = read_int<size_t>(); // Throws
                if (!m_table)
                    goto bad_transact_log;
                if (column_ndx >= m_table->get_column_count())
                    goto bad_transact_log;
                if (m_table->size() <= ndx)
                    goto bad_transact_log;
                const bool insert = false;
                set_or_insert<insert>(column_ndx, ndx); // Throws
                break;
            }

            case 'i': { // Insert value
                size_t column_ndx = read_int<size_t>(); // Throws
                size_t ndx = read_int<size_t>(); // Throws
                if (!m_table)
                    goto bad_transact_log;
                if (column_ndx >= m_table->get_column_count())
                    goto bad_transact_log;
                if (m_table->size() < ndx)
                    goto bad_transact_log;
                const bool insert = true;
                set_or_insert<insert>(column_ndx, ndx); // Throws
                break;
            }

            case 'c': { // Row insert complete
                if (!m_table)
                    goto bad_transact_log;
#ifdef TIGHTDB_DEBUG
                if (m_log)
                    *m_log << "table->insert_done()\n";
#endif
                m_table->insert_done(); // FIXME: May fail
                break;
            }

            case 'I': { // Insert empty rows
                size_t ndx = read_int<size_t>(); // Throws
                size_t num_rows = read_int<size_t>(); // Throws
                if (!m_table || m_table->size() < ndx)
                    goto bad_transact_log;
#ifdef TIGHTDB_DEBUG
                if (m_log)
                    *m_log << "table->insert_empty_row("<<ndx<<", "<<num_rows<<")\n";
#endif
                m_table->insert_empty_row(ndx, num_rows); // FIXME: May fail
                break;
            }

            case 'R': { // Remove row
                size_t ndx = read_int<size_t>(); // Throws
                if (!m_table || m_table->size() < ndx)
                    goto bad_transact_log;
#ifdef TIGHTDB_DEBUG
                if (m_log)
                    *m_log << "table->remove("<<ndx<<")\n";
#endif
                m_table->remove(ndx); // FIXME: May fail
                break;
            }

            case 'a': { // Add int to column
                size_t column_ndx = read_int<size_t>(); // Throws
                if (!m_table)
                    goto bad_transact_log;
                if (column_ndx >= m_table->get_column_count())
                    goto bad_transact_log;
                int64_t value = read_int<int64_t>(); // Throws
#ifdef TIGHTDB_DEBUG
                if (m_log)
                    *m_log << "table->add_int("<<column_ndx<<", "<<value<<")\n";
#endif
                m_table->add_int(column_ndx, value); // FIXME: Memory allocation failure!!!
                break;
            }

            case 'T': { // Select table
                int levels = read_int<int>(); // Throws
                size_t ndx = read_int<size_t>(); // Throws
                if (m_group.size() <= ndx)
                    goto bad_transact_log;
#ifdef TIGHTDB_DEBUG
                if (m_log)
                    *m_log << "table = group->get_table("<<ndx<<")\n";
#endif
                m_table = m_group.get_table(ndx);
                desc.reset();
                for (int i = 0; i < levels; ++i) {
                    size_t column_ndx = read_int<size_t>(); // Throws
                    ndx = read_int<size_t>(); // Throws
                    if (column_ndx >= m_table->get_column_count())
                        goto bad_transact_log;
                    if (m_table->size() <= ndx)
                        goto bad_transact_log;
#ifdef TIGHTDB_DEBUG
                    if (m_log)
                        *m_log << "table = table->get_subtable("<<column_ndx<<", "<<ndx<<")\n";
#endif
                    switch (m_table->get_column_type(column_ndx)) {
                        case type_Table:
                            m_table = m_table->get_subtable(column_ndx, ndx);
                            break;
                        case type_Mixed:
                            m_table = m_table->get_subtable(column_ndx, ndx);
                            if (!m_table)
                                goto bad_transact_log;
                            break;
                        default:
                            goto bad_transact_log;
                    }
                }
                break;
            }

            case 'C': { // Clear table
                if (!m_table)
                    goto bad_transact_log;
#ifdef TIGHTDB_DEBUG
                if (m_log)
                    *m_log << "table->clear()\n";
#endif
                m_table->clear(); // FIXME: Can probably fail!
                break;
            }

            case 'x': { // Add index to column
                size_t column_ndx = read_int<size_t>(); // Throws
                if (!m_table)
                    goto bad_transact_log;
                if (m_table->has_shared_type())
                    goto bad_transact_log;
                if (column_ndx >= m_table->get_column_count())
                    goto bad_transact_log;
#ifdef TIGHTDB_DEBUG
                if (m_log)
                    *m_log << "table->set_index("<<column_ndx<<")\n";
#endif
                m_table->set_index(column_ndx); // FIXME: Memory allocation failure!!!
                break;
            }

            case 'O': { // Insert column into selected descriptor
                size_t column_ndx = read_int<size_t>(); // Throws
                int type = read_int<int>(); // Throws
                read_string(m_string_buffer); // Throws
                StringData name(m_string_buffer.data(), m_string_buffer.size());
                if (!desc)
                    goto bad_transact_log;
                if (column_ndx > desc->get_column_count())
                    goto bad_transact_log;
                if (!is_valid_column_type(type))
                    goto bad_transact_log;
#ifdef TIGHTDB_DEBUG
                if (m_log)
                    *m_log << "desc->insert_column("<<column_ndx<<", "<<type<<", \""<<name<<"\")\n";
#endif
                _impl::TableFriend::insert_column(*desc, column_ndx, DataType(type), name);
                break;
            }

            case 'P': { // Remove column from selected descriptor
                size_t column_ndx = read_int<size_t>(); // Throws
                if (!desc)
                    goto bad_transact_log;
                if (column_ndx >= desc->get_column_count())
                    goto bad_transact_log;
#ifdef TIGHTDB_DEBUG
                if (m_log)
                    *m_log << "desc->remove_column("<<column_ndx<<")\n";
#endif
                _impl::TableFriend::remove_column(*desc, column_ndx);
                break;
            }

            case 'Q': { // Rename column in selected descriptor
                size_t column_ndx = read_int<size_t>(); // Throws
                read_string(m_string_buffer); // Throws
                StringData name(m_string_buffer.data(), m_string_buffer.size());
                if (!desc)
                    goto bad_transact_log;
                if (column_ndx >= desc->get_column_count())
                    goto bad_transact_log;
#ifdef TIGHTDB_DEBUG
                if (m_log)
                    *m_log << "desc->rename_column("<<column_ndx<<", \""<<name<<"\")\n";
#endif
                _impl::TableFriend::rename_column(*desc, column_ndx, name);
                break;
            }

            case 'S': { // Select descriptor from currently selected root table
                if (!m_table)
                    goto bad_transact_log;
                if (m_table->has_shared_type())
                    goto bad_transact_log;
#ifdef TIGHTDB_DEBUG
                if (m_log)
                    *m_log << "desc = table->get_descriptor()\n";
#endif
                desc = m_table->get_descriptor();
                int levels = read_int<int>(); // Throws
                for (int i = 0; i < levels; ++i) {
                    size_t column_ndx = read_int<size_t>(); // Throws
                    if (column_ndx >= desc->get_column_count())
                        goto bad_transact_log;
#ifdef TIGHTDB_DEBUG
                    if (m_log)
                        *m_log << "desc = desc->get_subdescriptor("<<column_ndx<<")\n";
#endif
                    desc = desc->get_subdescriptor(column_ndx);
                }
                break;
            }

            case 'N': { // New top level table
                read_string(m_string_buffer); // Throws
                StringData name(m_string_buffer.data(), m_string_buffer.size());
                if (m_group.has_table(name))
                    goto bad_transact_log;
#ifdef TIGHTDB_DEBUG
                if (m_log)
                    *m_log << "group->create_new_table(\""<<name<<"\")\n";
#endif
                m_group.create_new_table(name); // Throws
                break;
            }

            case 'Z': { // Optimize table
                if (!m_table)
                    goto bad_transact_log;
                if (m_table->has_shared_type())
                    goto bad_transact_log;
#ifdef TIGHTDB_DEBUG
                if (m_log)
                    *m_log << "table->optimize()\n";
#endif
                m_table->optimize(); // FIXME: May fail
                break;
            }

        default:
            goto bad_transact_log;
        }
    }

    return;

  bad_transact_log:
    throw BadTransactLog();
}


void Replication::apply_transact_log(InputStream& transact_log, Group& group, ostream* log)
{
    TransactLogApplier applier(transact_log, group);
    applier.set_apply_log(log);
    applier.apply(); // Throws
}


namespace {

class InputStreamImpl: public Replication::InputStream {
public:
    InputStreamImpl(const char* data, size_t size) TIGHTDB_NOEXCEPT:
        m_begin(data), m_end(data+size) {}

    ~InputStreamImpl() TIGHTDB_NOEXCEPT {}

    size_t read(char* buffer, size_t size)
    {
        size_t n = min<size_t>(size, m_end-m_begin);
        const char* end = m_begin + n;
        copy(m_begin, end, buffer);
        m_begin = end;
        return n;
    }
    const char* m_begin;
    const char* const m_end;
};

} // anonymous namespace

void TrivialReplication::apply_transact_log(const char* data, size_t size, SharedGroup& target,
                                            ostream* log)
{
    InputStreamImpl in(data, size);
    WriteTransaction wt(target); // Throws
    Replication::apply_transact_log(in, wt.get_group(), log); // Throws
    wt.commit(); // Throws
}

string TrivialReplication::do_get_database_path()
{
    return m_database_file;
}

void TrivialReplication::do_begin_write_transact(SharedGroup&)
{
    char* data = m_transact_log_buffer.data();
    size_t size = m_transact_log_buffer.size();
    m_transact_log_free_begin = data;
    m_transact_log_free_end = data + size;
}

Replication::version_type
TrivialReplication::do_commit_write_transact(SharedGroup&, version_type orig_version)
{
    char* data = m_transact_log_buffer.data();
    size_t size = m_transact_log_free_begin - data;
    handle_transact_log(data, size); // Throws
    return orig_version + 1;
}

void TrivialReplication::do_rollback_write_transact(SharedGroup&) TIGHTDB_NOEXCEPT
{
}

void TrivialReplication::do_interrupt() TIGHTDB_NOEXCEPT
{
}

void TrivialReplication::do_clear_interrupt() TIGHTDB_NOEXCEPT
{
}

void TrivialReplication::do_transact_log_reserve(size_t n)
{
    transact_log_reserve(n);
}

void TrivialReplication::do_transact_log_append(const char* data, size_t size)
{
    transact_log_reserve(size);
    m_transact_log_free_begin = copy(data, data+size, m_transact_log_free_begin);
}
