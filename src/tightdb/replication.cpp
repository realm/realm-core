#include <stdexcept>
#include <utility>
#include <ostream>
#include <iomanip>

#include <tightdb/table.hpp>
#include <tightdb/descriptor.hpp>
#include <tightdb/link_view.hpp>
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
    typedef _impl::GroupFriend gf;
    gf::set_replication(group, repl);
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
    *buf++ = char(instr_SelectTable);
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
    m_selected_spec      = 0;
    m_selected_link_list = 0;
    m_selected_table     = table;
}


void Replication::select_desc(const Descriptor& desc)
{
    typedef _impl::DescriptorFriend df;
    check_table(&df::get_root_table(desc));
    size_t* begin;
    size_t* end;
    for (;;) {
        begin = m_subtab_path_buf.data();
        end   = begin + m_subtab_path_buf.size();
        begin = df::record_subdesc_path(desc, begin, end);
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
    *buf++ = char(instr_SelectDescriptor);
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
    m_selected_spec = &df::get_spec(desc);
}


void Replication::select_link_list(const LinkView& list)
{
    check_table(list.m_origin_table.get());
    size_t col_ndx = list.m_origin_column.m_column_ndx;
    size_t row_ndx = list.get_origin_row_index();
    simple_cmd(instr_SelectLinkList, util::tuple(col_ndx, row_ndx)); // Throws
    m_selected_link_list = &list;
}


class Replication::TransactLogApplier {
public:
    TransactLogApplier(Group& group):
        m_group(group)
    {
    }

    void set_apply_log(ostream* log) TIGHTDB_NOEXCEPT
    {
        m_log = log;
        if (m_log)
            *m_log << boolalpha;
    }

    bool set_int(size_t col_ndx, size_t row_ndx, int_fast64_t value)
    {
        if (TIGHTDB_LIKELY(check_set_cell(col_ndx, row_ndx))) {
#ifdef TIGHTDB_DEBUG
            if (m_log)
                *m_log << "table->set_int("<<col_ndx<<", "<<row_ndx<<", "<<value<<")\n";
#endif
            m_table->set_int(col_ndx, row_ndx, value); // Throws
            return true;
        }
        return false;
    }

    bool set_bool(size_t col_ndx, size_t row_ndx, bool value)
    {
        if (TIGHTDB_LIKELY(check_set_cell(col_ndx, row_ndx))) {
#ifdef TIGHTDB_DEBUG
            if (m_log)
                *m_log << "table->set_bool("<<col_ndx<<", "<<row_ndx<<", "<<value<<")\n";
#endif
            m_table->set_bool(col_ndx, row_ndx, value); // Throws
            return true;
        }
        return false;
    }

    bool set_float(size_t col_ndx, size_t row_ndx, float value)
    {
        if (TIGHTDB_LIKELY(check_set_cell(col_ndx, row_ndx))) {
#ifdef TIGHTDB_DEBUG
            if (m_log)
                *m_log << "table->set_float("<<col_ndx<<", "<<row_ndx<<", "<<value<<")\n";
#endif
            m_table->set_float(col_ndx, row_ndx, value); // Throws
            return true;
        }
        return false;
    }

    bool set_double(size_t col_ndx, size_t row_ndx, double value)
    {
        if (TIGHTDB_LIKELY(check_set_cell(col_ndx, row_ndx))) {
#ifdef TIGHTDB_DEBUG
            if (m_log)
                *m_log << "table->set_double("<<col_ndx<<", "<<row_ndx<<", "<<value<<")\n";
#endif
            m_table->set_double(col_ndx, row_ndx, value); // Throws
            return true;
        }
        return false;
    }

    bool set_string(size_t col_ndx, size_t row_ndx, StringData value)
    {
        if (TIGHTDB_LIKELY(check_set_cell(col_ndx, row_ndx))) {
#ifdef TIGHTDB_DEBUG
            if (m_log)
                *m_log << "table->set_string("<<col_ndx<<", "<<row_ndx<<", "<<value<<")\n";
#endif
            m_table->set_string(col_ndx, row_ndx, value); // Throws
            return true;
        }
        return false;
    }

    bool set_binary(size_t col_ndx, size_t row_ndx, BinaryData value)
    {
        if (TIGHTDB_LIKELY(check_set_cell(col_ndx, row_ndx))) {
#ifdef TIGHTDB_DEBUG
            if (m_log)
                *m_log << "table->set_binary("<<col_ndx<<", "<<row_ndx<<", "<<value<<")\n";
#endif
            m_table->set_binary(col_ndx, row_ndx, value); // Throws
            return true;
        }
        return false;
    }

    bool set_date_time(size_t col_ndx, size_t row_ndx, DateTime value)
    {
        if (TIGHTDB_LIKELY(check_set_cell(col_ndx, row_ndx))) {
#ifdef TIGHTDB_DEBUG
            if (m_log)
                *m_log << "table->set_datetime("<<col_ndx<<", "<<row_ndx<<", "<<value<<")\n";
#endif
            m_table->set_datetime(col_ndx, row_ndx, value); // Throws
            return true;
        }
        return false;
    }

    bool set_table(size_t col_ndx, size_t row_ndx)
    {
        if (TIGHTDB_LIKELY(check_set_cell(col_ndx, row_ndx))) {
#ifdef TIGHTDB_DEBUG
            if (m_log)
                *m_log << "table->clear_subtable("<<col_ndx<<", "<<row_ndx<<")\n";
#endif
            m_table->clear_subtable(col_ndx, row_ndx); // Throws
            return true;
        }
        return false;
    }

    bool set_mixed(size_t col_ndx, size_t row_ndx, const Mixed& value)
    {
        if (TIGHTDB_LIKELY(check_set_cell(col_ndx, row_ndx))) {
#ifdef TIGHTDB_DEBUG
            if (m_log)
                *m_log << "table->set_mixed("<<col_ndx<<", "<<row_ndx<<", "<<value<<")\n";
#endif
            m_table->set_mixed(col_ndx, row_ndx, value); // Throws
            return true;
        }
        return false;
    }

    bool set_link(size_t col_ndx, size_t row_ndx, std::size_t value)
    {
        if (TIGHTDB_LIKELY(check_set_cell(col_ndx, row_ndx))) {
#ifdef TIGHTDB_DEBUG
            if (m_log) {
                if (value == 0) {
                    *m_log << "table->nullify_link("<<col_ndx<<", "<<row_ndx<<")\n";
                }
                else {
                    *m_log << "table->set_link("<<col_ndx<<", "<<row_ndx<<", "<<(value-1)<<")\n";
                }
            }
#endif
            if (value == 0) {
                m_table->nullify_link(col_ndx, row_ndx); // Throws
            }
            else {
                m_table->set_link(col_ndx, row_ndx, value-1); // Throws
            }
            return true;
        }
        return false;
    }

    bool insert_int(size_t col_ndx, size_t row_ndx, std::size_t, int_fast64_t value)
    {
        if (TIGHTDB_LIKELY(check_insert_cell(col_ndx, row_ndx))) {
#ifdef TIGHTDB_DEBUG
            if (m_log)
                *m_log << "table->insert_int("<<col_ndx<<", "<<row_ndx<<", "<<value<<")\n";
#endif
            m_table->insert_int(col_ndx, row_ndx, value); // Throws
            return true;
        }
        return false;
    }

    bool insert_bool(size_t col_ndx, size_t row_ndx, std::size_t, bool value)
    {
        if (TIGHTDB_LIKELY(check_insert_cell(col_ndx, row_ndx))) {
#ifdef TIGHTDB_DEBUG
            if (m_log)
                *m_log << "table->insert_bool("<<col_ndx<<", "<<row_ndx<<", "<<value<<")\n";
#endif
            m_table->insert_bool(col_ndx, row_ndx, value); // Throws
            return true;
        }
        return false;
    }

    bool insert_float(size_t col_ndx, size_t row_ndx, std::size_t, float value)
    {
        if (TIGHTDB_LIKELY(check_insert_cell(col_ndx, row_ndx))) {
#ifdef TIGHTDB_DEBUG
            if (m_log)
                *m_log << "table->insert_float("<<col_ndx<<", "<<row_ndx<<", "<<value<<")\n";
#endif
            m_table->insert_float(col_ndx, row_ndx, value); // Throws
            return true;
        }
        return false;
    }

    bool insert_double(size_t col_ndx, size_t row_ndx, std::size_t, double value)
    {
        if (TIGHTDB_LIKELY(check_insert_cell(col_ndx, row_ndx))) {
#ifdef TIGHTDB_DEBUG
            if (m_log)
                *m_log << "table->insert_double("<<col_ndx<<", "<<row_ndx<<", "<<value<<")\n";
#endif
            m_table->insert_double(col_ndx, row_ndx, value); // Throws
            return true;
        }
        return false;
    }

    bool insert_string(size_t col_ndx, size_t row_ndx, std::size_t, StringData value)
    {
        if (TIGHTDB_LIKELY(check_insert_cell(col_ndx, row_ndx))) {
#ifdef TIGHTDB_DEBUG
            if (m_log)
                *m_log << "table->insert_string("<<col_ndx<<", "<<row_ndx<<", "<<value<<")\n";
#endif
            m_table->insert_string(col_ndx, row_ndx, value); // Throws
            return true;
        }
        return false;
    }

    bool insert_binary(size_t col_ndx, size_t row_ndx, std::size_t, BinaryData value)
    {
        if (TIGHTDB_LIKELY(check_insert_cell(col_ndx, row_ndx))) {
#ifdef TIGHTDB_DEBUG
            if (m_log)
                *m_log << "table->insert_binary("<<col_ndx<<", "<<row_ndx<<", "<<value<<")\n";
#endif
            m_table->insert_binary(col_ndx, row_ndx, value); // Throws
            return true;
        }
        return false;
    }

    bool insert_date_time(size_t col_ndx, size_t row_ndx, std::size_t, DateTime value)
    {
        if (TIGHTDB_LIKELY(check_insert_cell(col_ndx, row_ndx))) {
#ifdef TIGHTDB_DEBUG
            if (m_log)
                *m_log << "table->insert_datetime("<<col_ndx<<", "<<row_ndx<<", "<<value<<")\n";
#endif
            m_table->insert_datetime(col_ndx, row_ndx, value); // Throws
            return true;
        }
        return false;
    }

    bool insert_table(size_t col_ndx, size_t row_ndx, std::size_t)
    {
        if (TIGHTDB_LIKELY(check_insert_cell(col_ndx, row_ndx))) {
#ifdef TIGHTDB_DEBUG
            if (m_log)
                *m_log << "table->insert_subtable("<<col_ndx<<", "<<row_ndx<<")\n";
#endif
            m_table->insert_subtable(col_ndx, row_ndx); // Throws
            return true;
        }
        return false;
    }

    bool insert_mixed(size_t col_ndx, size_t row_ndx, std::size_t, const Mixed& value)
    {
        if (TIGHTDB_LIKELY(check_insert_cell(col_ndx, row_ndx))) {
#ifdef TIGHTDB_DEBUG
            if (m_log)
                *m_log << "table->insert_mixed("<<col_ndx<<", "<<row_ndx<<", "<<value<<")\n";
#endif
            m_table->insert_mixed(col_ndx, row_ndx, value); // Throws
            return true;
        }
        return false;
    }

    bool insert_link(size_t col_ndx, size_t row_ndx, std::size_t, std::size_t value)
    {
        TIGHTDB_ASSERT(value > 0); // Not yet any support for inserting null links
        if (TIGHTDB_LIKELY(check_insert_cell(col_ndx, row_ndx))) {
#ifdef TIGHTDB_DEBUG
            if (m_log)
                *m_log << "table->insert_link("<<col_ndx<<", "<<row_ndx<<", "<<(value-1)<<")\n";
#endif
            m_table->insert_link(col_ndx, row_ndx, value-1); // Throws
            return true;
        }
        return false;
    }

    bool insert_link_list(size_t col_ndx, size_t row_ndx, std::size_t)
    {
        if (TIGHTDB_LIKELY(check_insert_cell(col_ndx, row_ndx))) {
#ifdef TIGHTDB_DEBUG
            if (m_log)
                *m_log << "table->insert_link_list("<<col_ndx<<", "<<row_ndx<<")\n";
#endif
            m_table->insert_linklist(col_ndx, row_ndx); // Throws
            return true;
        }
        return false;
    }

    bool row_insert_complete()
    {
        if (TIGHTDB_LIKELY(m_table)) {
#ifdef TIGHTDB_DEBUG
            if (m_log)
                *m_log << "table->insert_done()\n";
#endif
            m_table->insert_done(); // Throws
            return true;
        }
        return false;
    }

    bool insert_empty_rows(size_t row_ndx, size_t num_rows, std::size_t, bool)
    {
        if (TIGHTDB_LIKELY(m_table)) {
            if (TIGHTDB_LIKELY(row_ndx <= m_table->size())) {
#ifdef TIGHTDB_DEBUG
                if (m_log)
                    *m_log << "table->insert_empty_row("<<row_ndx<<", "<<num_rows<<")\n";
#endif
                m_table->insert_empty_row(row_ndx, num_rows); // Throws
                return true;
            }
        }
        return false;
    }

    bool erase_row(size_t row_ndx, std::size_t last_row_ndx, bool unordered)
    {
        if (TIGHTDB_LIKELY(m_table)) {
            if (unordered) {
                // It might make sense to change this into an assert, as it should always hold
                if (TIGHTDB_LIKELY(row_ndx < last_row_ndx && last_row_ndx+1 == m_table->size())) {
#ifdef TIGHTDB_DEBUG
                    if (m_log)
                        *m_log << "table->move_last_over("<<row_ndx<<")\n";
#endif
                    m_table->move_last_over(row_ndx); // Throws
                    return true;
                }
            }
            else {
                if (TIGHTDB_LIKELY(row_ndx < m_table->size())) {
#ifdef TIGHTDB_DEBUG
                    if (m_log)
                        *m_log << "table->remove("<<row_ndx<<")\n";
#endif
                    m_table->remove(row_ndx); // Throws
                    return true;
                }
            }
        }
        return false;
    }

    bool add_int_to_column(size_t col_ndx, int_fast64_t value)
    {
        if (TIGHTDB_LIKELY(m_table)) {
            if (TIGHTDB_LIKELY(col_ndx < m_table->get_column_count())) {
                // FIXME: Don't depend on the existence of int64_t,
                // but don't allow values to use more than 64 bits
                // either.
#ifdef TIGHTDB_DEBUG
                if (m_log)
                    *m_log << "table->add_int("<<col_ndx<<", "<<value<<")\n";
#endif
                m_table->add_int(col_ndx, value); // Throws
                return true;
            }
        }
        return false;
    }

    bool select_table(size_t group_level_ndx, int levels, const size_t* path)
    {
        if (TIGHTDB_UNLIKELY(group_level_ndx >= m_group.size()))
            return false;
#ifdef TIGHTDB_DEBUG
        if (m_log)
            *m_log << "table = group->get_table("<<group_level_ndx<<")\n";
#endif
        m_desc.reset();
        m_table = m_group.get_table(group_level_ndx); // Throws
        for (int i = 0; i < levels; ++i) {
            size_t col_ndx = path[2*i + 0];
            size_t row_ndx = path[2*i + 1];
            if (TIGHTDB_UNLIKELY(col_ndx >= m_table->get_column_count()))
                return false;
            if (TIGHTDB_UNLIKELY(row_ndx >= m_table->size()))
                return false;
#ifdef TIGHTDB_DEBUG
            if (m_log)
                *m_log << "table = table->get_subtable("<<col_ndx<<", "<<row_ndx<<")\n";
#endif
            DataType type = m_table->get_column_type(col_ndx);
            switch (type) {
                case type_Table:
                    m_table = m_table->get_subtable(col_ndx, row_ndx); // Throws
                    break;
                case type_Mixed:
                    m_table = m_table->get_subtable(col_ndx, row_ndx); // Throws
                    if (TIGHTDB_UNLIKELY(!m_table))
                        return false;
                    break;
                default:
                    return false;
            }
        }
        return true;
    }

    bool clear_table()
    {
        if (TIGHTDB_LIKELY(m_table)) {
#ifdef TIGHTDB_DEBUG
            if (m_log)
                *m_log << "table->clear()\n";
#endif
            m_table->clear(); // Throws
            return true;
        }
        return false;
    }

    bool add_search_index(size_t col_ndx)
    {
        if (TIGHTDB_LIKELY(m_table)) {
            if (TIGHTDB_LIKELY(!m_table->has_shared_type())) {
                if (TIGHTDB_LIKELY(col_ndx < m_table->get_column_count())) {
#ifdef TIGHTDB_DEBUG
                    if (m_log)
                        *m_log << "table->set_index("<<col_ndx<<")\n";
#endif
                    m_table->set_index(col_ndx); // Throws
                    return true;
                }
            }
        }
        return false;
    }

    bool insert_column(size_t col_ndx, DataType type, StringData name,
                       size_t link_target_table_ndx, size_t)
    {
        if (TIGHTDB_LIKELY(m_desc)) {
            if (TIGHTDB_LIKELY(col_ndx <= m_desc->get_column_count())) {
                typedef _impl::TableFriend tf;
#ifdef TIGHTDB_DEBUG
                if (m_log) {
                    if (link_target_table_ndx != tightdb::npos) {
                        *m_log << "desc->insert_column_link("<<col_ndx<<", "
                            ""<<type_to_str(type)<<", \""<<name<<"\", "
                            "group->get_table("<<link_target_table_ndx<<"))\n";
                    }
                    else {
                        *m_log << "desc->insert_column("<<col_ndx<<", "<<type_to_str(type)<<", "
                            "\""<<name<<"\")\n";
                    }
                }
#endif
                Table* link_target_table = 0;
                if (link_target_table_ndx != tightdb::npos) {
                    typedef _impl::GroupFriend gf;
                    link_target_table = &gf::get_table(m_group, link_target_table_ndx); // Throws
                }
                tf::insert_column(*m_desc, col_ndx, type, name, link_target_table); // Throws
                return true;
            }
        }
        return false;
    }

    bool erase_column(size_t col_ndx, size_t, size_t)
    {
        if (TIGHTDB_LIKELY(m_desc)) {
            if (TIGHTDB_LIKELY(col_ndx < m_desc->get_column_count())) {
#ifdef TIGHTDB_DEBUG
                if (m_log)
                    *m_log << "desc->remove_column("<<col_ndx<<")\n";
#endif
                _impl::TableFriend::erase_column(*m_desc, col_ndx); // Throws
                return true;
            }
        }
        return false;
    }

    bool rename_column(size_t col_ndx, StringData name)
    {
        if (TIGHTDB_LIKELY(m_desc)) {
            if (TIGHTDB_LIKELY(col_ndx < m_desc->get_column_count())) {
#ifdef TIGHTDB_DEBUG
                if (m_log)
                    *m_log << "desc->rename_column("<<col_ndx<<", \""<<name<<"\")\n";
#endif
                _impl::TableFriend::rename_column(*m_desc, col_ndx, name); // Throws
                return true;
            }
        }
        return true;
    }

    bool select_descriptor(int levels, const size_t* path)
    {
        if (TIGHTDB_UNLIKELY(!m_table))
            return false;
        if (TIGHTDB_UNLIKELY(m_table->has_shared_type()))
            return false;
#ifdef TIGHTDB_DEBUG
        if (m_log)
            *m_log << "desc = table->get_descriptor()\n";
#endif
        m_desc = m_table->get_descriptor(); // Throws
        for (int i = 0; i < levels; ++i) {
            size_t col_ndx = path[i];
            if (TIGHTDB_UNLIKELY(col_ndx >= m_desc->get_column_count()))
                return false;
#ifdef TIGHTDB_DEBUG
            if (m_log)
                *m_log << "desc = desc->get_subdescriptor("<<col_ndx<<")\n";
#endif
            m_desc = m_desc->get_subdescriptor(col_ndx);
        }
        return true;
    }

    bool insert_group_level_table(size_t table_ndx, size_t num_tables, StringData name)
    {
        if (TIGHTDB_UNLIKELY(table_ndx != num_tables))
            return false;
        if (TIGHTDB_UNLIKELY(num_tables != m_group.size()))
            return false;
#ifdef TIGHTDB_DEBUG
        if (m_log)
            *m_log << "group->add_table(\""<<name<<"\", false)\n";
#endif
        typedef _impl::GroupFriend gf;
        bool require_unique_name = false;
        gf::add_table(m_group, name, require_unique_name); // Throws
        return true;
    }

    bool erase_group_level_table(std::size_t table_ndx, size_t num_tables) TIGHTDB_NOEXCEPT
    {
        if (TIGHTDB_UNLIKELY(num_tables != m_group.size()))
            return false;
#ifdef TIGHTDB_DEBUG
        if (m_log)
            *m_log << "group->remove_table("<<table_ndx<<")\n";
#endif
        m_group.remove_table(table_ndx);
        return true;
    }

    bool rename_group_level_table(std::size_t table_ndx, StringData new_name) TIGHTDB_NOEXCEPT
    {
#ifdef TIGHTDB_DEBUG
        if (m_log)
            *m_log << "group->rename_table("<<table_ndx<<", \""<<new_name<<"\")\n";
#endif
        m_group.rename_table(table_ndx, new_name);
        return true;
    }

    bool optimize_table()
    {
        if (TIGHTDB_LIKELY(m_table)) {
            if (TIGHTDB_LIKELY(!m_table->has_shared_type())) {
#ifdef TIGHTDB_DEBUG
                if (m_log)
                    *m_log << "table->optimize()\n";
#endif
                m_table->optimize(); // Throws
                return true;
            }
        }
        return true;
    }

    bool select_link_list(size_t col_ndx, size_t row_ndx)
    {
        if (TIGHTDB_UNLIKELY(!m_table))
            return false;
        if (TIGHTDB_UNLIKELY(col_ndx >= m_table->get_column_count()))
            return false;
        DataType type = m_table->get_column_type(col_ndx);
        if (TIGHTDB_UNLIKELY(type != type_LinkList))
            return false;
#ifdef TIGHTDB_DEBUG
        if (m_log)
            *m_log << "link_list = table->get_link_list("<<col_ndx<<", "<<row_ndx<<")\n";
#endif
        m_link_list = m_table->get_linklist(col_ndx, row_ndx); // Throws
        return true;
    }

    bool link_list_set(size_t link_ndx, size_t value)
    {
        if (TIGHTDB_UNLIKELY(!m_link_list))
            return false;
        if (TIGHTDB_UNLIKELY(link_ndx >= m_link_list->size()))
            return false;
#ifdef TIGHTDB_DEBUG
        if (m_log)
            *m_log << "link_list->set("<<link_ndx<<", "<<value<<")\n";
#endif
        m_link_list->set(link_ndx, value); // Throws
        return true;
    }

    bool link_list_insert(size_t link_ndx, size_t value)
    {
        if (TIGHTDB_UNLIKELY(!m_link_list))
            return false;
        if (TIGHTDB_UNLIKELY(link_ndx > m_link_list->size()))
            return false;
#ifdef TIGHTDB_DEBUG
        if (m_log)
            *m_log << "link_list->insert("<<link_ndx<<", "<<value<<")\n";
#endif
        m_link_list->insert(link_ndx, value); // Throws
        return true;
    }

    bool link_list_move(size_t old_link_ndx, size_t new_link_ndx)
    {
        if (TIGHTDB_UNLIKELY(!m_link_list))
            return false;
        size_t num_links = m_link_list->size();
        if (TIGHTDB_UNLIKELY(old_link_ndx >= num_links))
            return false;
        if (TIGHTDB_UNLIKELY(new_link_ndx > num_links))
            return false;
#ifdef TIGHTDB_DEBUG
        if (m_log)
            *m_log << "link_list->move("<<old_link_ndx<<", "<<new_link_ndx<<")\n";
#endif
        m_link_list->move(old_link_ndx, new_link_ndx); // Throws
        return true;
    }

    bool link_list_erase(size_t link_ndx)
    {
        if (TIGHTDB_UNLIKELY(!m_link_list))
            return false;
        if (TIGHTDB_UNLIKELY(link_ndx >= m_link_list->size()))
            return false;
#ifdef TIGHTDB_DEBUG
        if (m_log)
            *m_log << "link_list->remove("<<link_ndx<<")\n";
#endif
        m_link_list->remove(link_ndx); // Throws
        return true;
    }

    bool link_list_clear()
    {
        if (TIGHTDB_UNLIKELY(!m_link_list))
            return false;
#ifdef TIGHTDB_DEBUG
        if (m_log)
            *m_log << "link_list->clear()\n";
#endif
        m_link_list->clear(); // Throws
        return true;
    }

private:
    Group& m_group;
    TableRef m_table;
    DescriptorRef m_desc;
    LinkViewRef m_link_list;
    ostream* m_log;

    bool check_set_cell(size_t col_ndx, size_t row_ndx) TIGHTDB_NOEXCEPT
    {
        if (TIGHTDB_LIKELY(m_table)) {
            if (TIGHTDB_LIKELY(col_ndx < m_table->get_column_count())) {
                if (TIGHTDB_LIKELY(row_ndx < m_table->size()))
                    return true;
            }
        }
        return false;
    }

    bool check_insert_cell(size_t col_ndx, size_t row_ndx) TIGHTDB_NOEXCEPT
    {
        if (TIGHTDB_LIKELY(m_table)) {
            if (TIGHTDB_LIKELY(col_ndx < m_table->get_column_count())) {
                if (TIGHTDB_LIKELY(row_ndx <= m_table->size()))
                    return true;
            }
        }
        return false;
    }

    const char* type_to_str(DataType type)
    {
        switch (type) {
            case type_Int:
                return "type_Int";
            case type_Bool:
                return "type_Bool";
            case type_Float:
                return "type_Float";
            case type_Double:
                return "type_Double";
            case type_String:
                return "type_String";
            case type_Binary:
                return "type_Binary";
            case type_DateTime:
                return "type_DataTime";
            case type_Table:
                return "type_Table";
            case type_Mixed:
                return "type_Mixed";
            case type_Link:
                return "type_Link";
            case type_LinkList:
                return "type_LinkList";
        }
        TIGHTDB_ASSERT(false);
        return 0;
    }
};


void Replication::apply_transact_log(InputStream& transact_log, Group& group, ostream* log)
{
    TransactLogParser parser(transact_log);
    TransactLogApplier applier(group);
    applier.set_apply_log(log);
    parser.parse(applier); // Throws
}


namespace {

class InputStreamImpl: public Replication::InputStream {
public:
    InputStreamImpl(const char* data, size_t size) TIGHTDB_NOEXCEPT:
        m_begin(data), m_end(data+size) {}

    ~InputStreamImpl() TIGHTDB_NOEXCEPT {}

    size_t read(char* buffer, size_t size) TIGHTDB_OVERRIDE
    {
        size_t n = min<size_t>(size, m_end-m_begin);
        const char* end = m_begin + n;
        copy(m_begin, end, buffer);
        m_begin = end;
        return n;
    }

    size_t next_block(const char*& begin, const char*& end) TIGHTDB_OVERRIDE
    {
        if (m_begin != 0) {
            begin = m_begin;
            end = m_end;
            m_begin = 0;
            return end - begin;
        }
        return 0;
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
    version_type new_version = orig_version + 1;
    handle_transact_log(data, size, new_version); // Throws
    return new_version;
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
