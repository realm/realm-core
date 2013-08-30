#include <utility>
#include <ostream>
#include <iomanip>

#include <tightdb/terminate.hpp>
#include <tightdb/safe_int_ops.hpp>
#include <tightdb/string_buffer.hpp>
#include <tightdb/unique_ptr.hpp>
#include <tightdb/table.hpp>
#include <tightdb/group_shared.hpp>
#include <tightdb/replication.hpp>

using namespace std;
using namespace tightdb;

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


Replication::database_version_type
Replication::get_current_version(SharedGroup& sg) TIGHTDB_NOEXCEPT
{
    return sg.get_current_version();
}


void Replication::commit_foreign_transact_log(SharedGroup& sg, database_version_type new_version)
{
    sg.low_level_commit(new_version);
}


void Replication::select_table(const Table* table)
{
    size_t* begin;
    size_t* end;
    for (;;) {
        begin = m_subtab_path_buf.data();
        end = table->record_subtable_path(begin, begin+m_subtab_path_buf.size());
        if (end)
            break;
        size_t new_size = m_subtab_path_buf.size();
        if (int_multiply_with_overflow_detect(new_size, 2))
            throw runtime_error("To many subtable nesting levels");
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
        for (int i=0; i<max_elems_per_chunk; ++i) {
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


void Replication::select_spec(const Table* table, const Spec* spec)
{
    check_table(table);
    size_t* begin;
    size_t* end;
    for (;;) {
        begin = m_subtab_path_buf.data();
        end = table->record_subspec_path(spec, begin, begin+m_subtab_path_buf.size());
        if (end)
            break;
        size_t new_size = m_subtab_path_buf.size();
        if (int_multiply_with_overflow_detect(new_size, 2))
            throw runtime_error("To many subspec nesting levels");
        m_subtab_path_buf.set_size(new_size); // Throws
    }
    char* buf;
    const int max_elems_per_chunk = 8; // FIXME: Use smaller number when compiling in debug mode
    transact_log_reserve(&buf, 1 + (1+max_elems_per_chunk)*max_enc_bytes_per_int); // Throws
    *buf++ = 'S';
    const size_t level = end - begin;
    buf = encode_int(buf, level);
    if (begin == end)
        goto good;
    for (;;) {
        for (int i=0; i<max_elems_per_chunk; ++i) {
            buf = encode_int(buf, *--end);
            if (begin == end)
                goto good;
        }
        transact_log_advance(buf);
        transact_log_reserve(&buf, max_elems_per_chunk*max_enc_bytes_per_int); // Throws
    }

good:
    transact_log_advance(buf);
    m_selected_spec = spec;
}


struct Replication::TransactLogApplier {
    TransactLogApplier(InputStream& transact_log, Group& group):
        m_input(transact_log), m_group(group), m_input_buffer(0),
        m_num_subspecs(0), m_dirty_spec(false) {}

    ~TransactLogApplier()
    {
        delete[] m_input_buffer;
        delete_subspecs();
    }

#ifdef TIGHTDB_DEBUG
    void set_apply_log(ostream* log)
    {
        m_log = log;
        if (m_log)
            *m_log << boolalpha;
    }
#endif

    void apply();

private:
    InputStream& m_input;
    Group& m_group;
    static const size_t m_input_buffer_size = 4096; // FIXME: Use smaller number when compiling in debug mode
    char* m_input_buffer;
    const char* m_input_begin;
    const char* m_input_end;
    TableRef m_table;
    util::Buffer<Spec*> m_subspecs;
    size_t m_num_subspecs;
    bool m_dirty_spec;
    StringBuffer m_string_buffer;
#ifdef TIGHTDB_DEBUG
    ostream* m_log;
#endif

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

    void add_subspec(Spec*);

    template<bool insert> void set_or_insert(int column_ndx, size_t ndx);

    void delete_subspecs()
    {
        const size_t n = m_num_subspecs;
        for (size_t i=0; i<n; ++i)
            delete m_subspecs[i];
        m_num_subspecs = 0;
    }

    void finalize_spec() // FIXME: May fail
    {
        TIGHTDB_ASSERT(m_table);
        m_table->update_from_spec();
#ifdef TIGHTDB_DEBUG
        if (m_log)
            *m_log << "table->update_from_spec()\n";
#endif
        m_dirty_spec = false;
    }

    bool is_valid_column_type(int type)
    {
        switch (type) {
            case type_Int:
            case type_Bool:
            case type_Date:
            case type_String:
            case type_Binary:
            case type_Table:
            case type_Mixed: return true;
            default:         break;
        }
        return false;
    }
};


template<class T> T Replication::TransactLogApplier::read_int()
{
    T value = 0;
    int part;
    const int max_bytes = (numeric_limits<T>::digits+1+6)/7;
    for (int i=0; i<max_bytes; ++i) {
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
            goto bad_transact_log; // Two many bytes
        value |= T(part & 0x7F) << (i*7);
    }
    if (part & 0x40) {
        // The real value is negative. Because 'value' is positive at
        // this point, the following negation is guaranteed by the
        // standard to never overflow.
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
                          sizeof (float) * std::numeric_limits<unsigned char>::digits == 32,
                          "Unsupported 'float' representation");
    float value;
    read_bytes(reinterpret_cast<char*>(&value), sizeof value); // Throws
    return value;
}


double Replication::TransactLogApplier::read_double()
{
    TIGHTDB_STATIC_ASSERT(numeric_limits<double>::is_iec559 &&
                          sizeof (double) * std::numeric_limits<unsigned char>::digits == 64,
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


void Replication::TransactLogApplier::add_subspec(Spec* spec)
{
    if (m_num_subspecs == m_subspecs.size()) {
        util::Buffer<Spec*> new_subspecs;
        size_t new_size = m_subspecs.size();
        if (new_size == 0) {
            new_size = 16; // FIXME: Use a small value (1) when compiling in debug mode
        }
        else {
            if (int_multiply_with_overflow_detect(new_size, 2))
                throw runtime_error("To many subspec nesting levels");
        }
        new_subspecs.set_size(new_size); // Throws
        copy(m_subspecs.data(), m_subspecs.data()+m_num_subspecs,
             new_subspecs.data());
        swap(m_subspecs, new_subspecs);
    }
    m_subspecs[m_num_subspecs++] = spec;
}


template<bool insert>
void Replication::TransactLogApplier::set_or_insert(int column_ndx, size_t ndx)
{
    switch (m_table->get_column_type(column_ndx)) {
        case type_Int: {
            int64_t value = read_int<int64_t>(); // Throws
            if (insert)
                m_table->insert_int(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
            else
                m_table->set_int(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
            if (m_log) {
                if (insert)
                    *m_log << "table->insert_int("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
                else
                    *m_log << "table->set_int("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
            }
#endif
            return;
        }
        case type_Bool: {
            bool value = read_int<bool>(); // Throws
            if (insert)
                m_table->insert_bool(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
            else
                m_table->set_bool(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
            if (m_log) {
                if (insert)
                    *m_log << "table->insert_bool("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
                else
                    *m_log << "table->set_bool("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
            }
#endif
            return;
        }
        case type_Float: {
            float value = read_float(); // Throws
            if (insert)
                m_table->insert_float(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
            else
                m_table->set_float(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
            if (m_log) {
                if (insert)
                    *m_log << "table->insert_float("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
                else
                    *m_log << "table->set_float("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
            }
#endif
            return;
        }
        case type_Double: {
            double value = read_double(); // Throws
            if (insert)
                m_table->insert_double(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
            else
                m_table->set_double(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
            if (m_log) {
                if (insert)
                    *m_log << "table->insert_double("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
                else
                    *m_log << "table->set_double("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
            }
#endif
            return;
        }
        case type_Date: {
            time_t value = read_int<time_t>(); // Throws
            if (insert)
                m_table->insert_date(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
            else
                m_table->set_date(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
            if (m_log) {
                if (insert)
                    *m_log << "table->insert_date("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
                else
                    *m_log << "table->set_date("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
            }
#endif
            return;
        }
        case type_String: {
            read_string(m_string_buffer); // Throws
            StringData value(m_string_buffer.data(), m_string_buffer.size());
            if (insert)
                m_table->insert_string(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
            else
                m_table->set_string(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
            if (m_log) {
                if (insert)
                    *m_log << "table->insert_string("<<column_ndx<<", "<<ndx<<", \""<<value<<"\")\n";
                else
                    *m_log << "table->set_string("<<column_ndx<<", "<<ndx<<", \""<<value<<"\")\n";
            }
#endif
            return;
        }
        case type_Binary: {
            read_string(m_string_buffer); // Throws
            BinaryData value(m_string_buffer.data(), m_string_buffer.size());
            if (insert)
                m_table->insert_binary(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
            else
                m_table->set_binary(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
            if (m_log) {
                if (insert)
                    *m_log << "table->insert_binary("<<column_ndx<<", "<<ndx<<", ...)\n";
                else
                    *m_log << "table->set_binary("<<column_ndx<<", "<<ndx<<", ...)\n";
            }
#endif
            return;
        }
        case type_Table: {
            if (insert)
                m_table->insert_subtable(column_ndx, ndx); // FIXME: Memory allocation failure!!!
            else
                m_table->clear_subtable(column_ndx, ndx); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
            if (m_log) {
                if (insert)
                    *m_log << "table->insert_subtable("<<column_ndx<<", "<<ndx<<")\n";
                else
                    *m_log << "table->clear_subtable("<<column_ndx<<", "<<ndx<<")\n";
            }
#endif
            return;
        }
        case type_Mixed: {
            int type = read_int<int>(); // Throws
            switch (type) {
                case type_Int: {
                    int64_t value = read_int<int64_t>(); // Throws
                    if (insert)
                        m_table->insert_mixed(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
                    else
                        m_table->set_mixed(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
                    if (m_log) {
                        if (insert)
                            *m_log << "table->insert_mixed("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
                        else
                            *m_log << "table->set_mixed("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
                    }
#endif
                    return;
                }
                case type_Bool: {
                    bool value = read_int<bool>(); // Throws
                    if (insert)
                        m_table->insert_mixed(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
                    else
                        m_table->set_mixed(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
                    if (m_log) {
                        if (insert)
                            *m_log << "table->insert_mixed("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
                        else
                            *m_log << "table->set_mixed("<<column_ndx<<", "<<ndx<<", "<<value<<")\n";
                    }
#endif
                    return;
                }
                case type_Date: {
                    time_t value = read_int<time_t>(); // Throws
                    if (insert)
                        m_table->insert_mixed(column_ndx, ndx, Date(value)); // FIXME: Memory allocation failure!!!
                    else
                        m_table->set_mixed(column_ndx, ndx, Date(value)); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
                    if (m_log) {
                        if (insert)
                            *m_log << "table->insert_mixed("<<column_ndx<<", "<<ndx<<", Date("<<value<<"))\n";
                        else
                            *m_log << "table->set_mixed("<<column_ndx<<", "<<ndx<<", Date("<<value<<"))\n";
                    }
#endif
                    return;
                }
                case type_String: {
                    read_string(m_string_buffer); // Throws
                    StringData value(m_string_buffer.data(), m_string_buffer.size());
                    if (insert)
                        m_table->insert_mixed(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
                    else
                        m_table->set_mixed(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
                    if (m_log) {
                        if (insert)
                            *m_log << "table->insert_mixed("<<column_ndx<<", "<<ndx<<", \""<<value<<"\")\n";
                        else
                            *m_log << "table->set_mixed("<<column_ndx<<", "<<ndx<<", \""<<value<<"\")\n";
                    }
#endif
                    return;
                }
                case type_Binary: {
                    read_string(m_string_buffer); // Throws
                    BinaryData value(m_string_buffer.data(), m_string_buffer.size());
                    if (insert)
                        m_table->insert_mixed(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
                    else
                        m_table->set_mixed(column_ndx, ndx, value); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
                    if (m_log) {
                        if (insert)
                            *m_log << "table->insert_mixed("<<column_ndx<<", "<<ndx<<", BinaryData(...))\n";
                        else
                            *m_log << "table->set_mixed("<<column_ndx<<", "<<ndx<<", BinaryData(...))\n";
                    }
#endif
                    return;
                }
                case type_Table: {
                    if (insert)
                        m_table->insert_mixed(column_ndx, ndx, Mixed::subtable_tag()); // FIXME: Memory allocation failure!!!
                    else
                        m_table->set_mixed(column_ndx, ndx, Mixed::subtable_tag()); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
                    if (m_log) {
                        if (insert)
                            *m_log << "table->insert_mixed("<<column_ndx<<", "<<ndx<<", Mixed::subtable_tag())\n";
                        else
                            *m_log << "table->set_mixed("<<column_ndx<<", "<<ndx<<", Mixed::subtable_tag())\n";
                    }
#endif
                    return;
                }
            }
            break;
        }
    }
    throw BadTransactLog();
}


void Replication::TransactLogApplier::apply()
{
    if (!m_input_buffer)
        m_input_buffer = new char[m_input_buffer_size]; // Throws
    m_input_begin = m_input_end = m_input_buffer;

    // FIXME: Problem: The modifying methods of group, table, and spec generally throw.
    Spec* spec = 0;
    for (;;) {
        char instr;
        if (!read_char(instr))
            break;
// cerr << "["<<instr<<"]";
        switch (instr) {
            case 's': { // Set value
                if (m_dirty_spec)
                    finalize_spec();
                int column_ndx = read_int<int>(); // Throws
                size_t ndx = read_int<size_t>(); // Throws
                if (!m_table)
                    goto bad_transact_log;
                if (column_ndx < 0 || int(m_table->get_column_count()) <= column_ndx)
                    goto bad_transact_log;
                if (m_table->size() <= ndx)
                    goto bad_transact_log;
                const bool insert = false;
                set_or_insert<insert>(column_ndx, ndx); // Throws
                break;
            }

            case 'i': { // Insert value
                if (m_dirty_spec)
                    finalize_spec();
                int column_ndx = read_int<int>(); // Throws
                size_t ndx = read_int<size_t>(); // Throws
                if (!m_table)
                    goto bad_transact_log;
                if (column_ndx < 0 || int(m_table->get_column_count()) <= column_ndx)
                    goto bad_transact_log;
                if (m_table->size() < ndx)
                    goto bad_transact_log;
                const bool insert = true;
                set_or_insert<insert>(column_ndx, ndx); // Throws
                break;
            }

            case 'c': { // Row insert complete
                if (m_dirty_spec)
                    finalize_spec();
                if (!m_table)
                    goto bad_transact_log;
                m_table->insert_done(); // FIXME: May fail
#ifdef TIGHTDB_DEBUG
                if (m_log)
                    *m_log << "table->insert_done()\n";
#endif
                break;
            }

            case 'I': { // Insert empty rows
                if (m_dirty_spec)
                    finalize_spec();
                size_t ndx = read_int<size_t>(); // Throws
                size_t num_rows = read_int<size_t>(); // Throws
                if (!m_table || m_table->size() < ndx)
                    goto bad_transact_log;
                m_table->insert_empty_row(ndx, num_rows); // FIXME: May fail
#ifdef TIGHTDB_DEBUG
                if (m_log)
                    *m_log << "table->insert_empty_row("<<ndx<<", "<<num_rows<<")\n";
#endif
                break;
            }

            case 'R': { // Remove row
                if (m_dirty_spec)
                    finalize_spec();
                size_t ndx = read_int<size_t>(); // Throws
                if (!m_table || m_table->size() < ndx)
                    goto bad_transact_log;
                m_table->remove(ndx); // FIXME: May fail
#ifdef TIGHTDB_DEBUG
                if (m_log)
                    *m_log << "table->remove("<<ndx<<")\n";
#endif
                break;
            }

            case 'a': { // Add int to column
                if (m_dirty_spec)
                    finalize_spec();
                int column_ndx = read_int<int>(); // Throws
                if (!m_table)
                    goto bad_transact_log;
                if (column_ndx < 0 || int(m_table->get_column_count()) <= column_ndx)
                    goto bad_transact_log;
                int64_t value = read_int<int64_t>(); // Throws
                m_table->add_int(column_ndx, value); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
                if (m_log)
                    *m_log << "table->add_int("<<column_ndx<<", "<<value<<")\n";
#endif
                break;
            }

            case 'T': { // Select table
                if (m_dirty_spec)
                    finalize_spec();
                int levels = read_int<int>(); // Throws
                size_t ndx = read_int<size_t>(); // Throws
                if (m_group.size() <= ndx)
                    goto bad_transact_log;
                m_table = m_group.get_table_by_ndx(ndx)->get_table_ref();
#ifdef TIGHTDB_DEBUG
                if (m_log)
                    *m_log << "table = group->get_table_by_ndx("<<ndx<<")\n";
#endif
                spec = 0;
                for (int i=0; i<levels; ++i) {
                    int column_ndx = read_int<int>(); // Throws
                    ndx = read_int<size_t>(); // Throws
                    if (column_ndx < 0 || int(m_table->get_column_count()) <= column_ndx)
                        goto bad_transact_log;
                    if (m_table->size() <= ndx)
                        goto bad_transact_log;
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
#ifdef TIGHTDB_DEBUG
                    if (m_log)
                        *m_log << "table = table->get_subtable("<<column_ndx<<", "<<ndx<<")\n";
#endif
                }
                break;
            }

            case 'C': { // Clear table
                if (m_dirty_spec)
                    finalize_spec();
                if (!m_table)
                    goto bad_transact_log;
                m_table->clear(); // FIXME: Can probably fail!
#ifdef TIGHTDB_DEBUG
                if (m_log)
                    *m_log << "table->clear()\n";
#endif
                break;
            }

            case 'x': { // Add index to column
                if (m_dirty_spec)
                    finalize_spec();
                int column_ndx = read_int<int>(); // Throws
                if (!m_table)
                    goto bad_transact_log;
                if (column_ndx < 0 || int(m_table->get_column_count()) <= column_ndx)
                    goto bad_transact_log;
                m_table->set_index(column_ndx); // FIXME: Memory allocation failure!!!
#ifdef TIGHTDB_DEBUG
                if (m_log)
                    *m_log << "table->set_index("<<column_ndx<<")\n";
#endif
                break;
            }

            case 'A': { // Add column to selected spec
                int type = read_int<int>(); // Throws
                if (!is_valid_column_type(type))
                    goto bad_transact_log;
                read_string(m_string_buffer); // Throws
                StringData name(m_string_buffer.data(), m_string_buffer.size());
                if (!spec)
                    goto bad_transact_log;
                // FIXME: Is it legal to have multiple columns with the same name?
                if (spec->get_column_index(name) != size_t(-1))
                    goto bad_transact_log;
                spec->add_column(DataType(type), name);
#ifdef TIGHTDB_DEBUG
                if (m_log)
                    *m_log << "spec->add_column("<<type<<", \""<<name<<"\")\n";
#endif
                m_dirty_spec = true;
                break;
            }

            case 'S': { // Select spec for currently selected table
                delete_subspecs();
                if (!m_table)
                    goto bad_transact_log;
                spec = &m_table->get_spec();
#ifdef TIGHTDB_DEBUG
                if (m_log)
                    *m_log << "spec = table->get_spec()\n";
#endif
                int levels = read_int<int>(); // Throws
                for (int i=0; i<levels; ++i) {
                    int subspec_ndx = read_int<int>(); // Throws
                    if (subspec_ndx < 0 || int(spec->get_num_subspecs()) <= subspec_ndx)
                        goto bad_transact_log;
                    UniquePtr<Spec> spec2(new Spec(spec->get_subspec_by_ndx(subspec_ndx)));
                    add_subspec(spec2.get());
                    spec = spec2.release();
#ifdef TIGHTDB_DEBUG
                    if (m_log)
                        *m_log << "spec = spec->get_subspec_by_ndx("<<subspec_ndx<<")\n";
#endif
                }
                break;
            }

            case 'N': { // New top level table
                read_string(m_string_buffer); // Throws
                StringData name(m_string_buffer.data(), m_string_buffer.size());
                if (m_group.has_table(name))
                    goto bad_transact_log;
                m_group.create_new_table(name); // Throws
#ifdef TIGHTDB_DEBUG
                if (m_log)
                    *m_log << "group->create_new_table(\""<<name<<"\")\n";
#endif
                break;
            }

            case 'Z': { // Optimize table
                if (m_dirty_spec)
                    finalize_spec();
                if (!m_table)
                    goto bad_transact_log;
                m_table->optimize(); // FIXME: May fail
#ifdef TIGHTDB_DEBUG
                if (m_log)
                    *m_log << "table->optimize()\n";
#endif
                break;
            }

        default:
            goto bad_transact_log;
        }
    }

    if (m_dirty_spec)
        finalize_spec(); // FIXME: Why is this necessary?
    return;

  bad_transact_log:
    throw BadTransactLog();
}


#ifdef TIGHTDB_DEBUG
void Replication::apply_transact_log(InputStream& transact_log, Group& group, ostream* log)
{
    TransactLogApplier applier(transact_log, group);
    applier.set_apply_log(log);
    applier.apply(); // Throws
}
#else
void Replication::apply_transact_log(InputStream& transact_log, Group& group)
{
    TransactLogApplier applier(transact_log, group);
    applier.apply(); // Throws
}
#endif
