#include <stdexcept>
#include <utility>
#include <ostream>
#include <iomanip>


#include <realm/group.hpp>
#include <realm/table.hpp>
#include <realm/descriptor.hpp>
#include <realm/link_view.hpp>
#include <realm/group_shared.hpp>
#include <realm/replication.hpp>

using namespace realm;
using namespace realm::util;


void Replication::set_replication(Group& group, Replication* repl) REALM_NOEXCEPT
{
    typedef _impl::GroupFriend gf;
    gf::set_replication(group, repl);
}

Replication::version_type Replication::get_current_version(SharedGroup& sg)
{
    return sg.get_current_version();
}


class Replication::TransactLogApplier {
public:
    TransactLogApplier(Group& group):
        m_group(group)
    {
    }

    ~TransactLogApplier() REALM_NOEXCEPT
    {
    }

    void set_apply_log(std::ostream* log) REALM_NOEXCEPT
    {
        m_log = log;
        if (m_log)
            *m_log << std::boolalpha;
    }

    bool set_int(size_t col_ndx, size_t row_ndx, int_fast64_t value)
    {
        if (REALM_LIKELY(check_set_cell(col_ndx, row_ndx))) {
#ifdef REALM_DEBUG
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
        if (REALM_LIKELY(check_set_cell(col_ndx, row_ndx))) {
#ifdef REALM_DEBUG
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
        if (REALM_LIKELY(check_set_cell(col_ndx, row_ndx))) {
#ifdef REALM_DEBUG
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
        if (REALM_LIKELY(check_set_cell(col_ndx, row_ndx))) {
#ifdef REALM_DEBUG
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
        if (REALM_LIKELY(check_set_cell(col_ndx, row_ndx))) {
#ifdef REALM_DEBUG
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
        if (REALM_LIKELY(check_set_cell(col_ndx, row_ndx))) {
#ifdef REALM_DEBUG
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
        if (REALM_LIKELY(check_set_cell(col_ndx, row_ndx))) {
#ifdef REALM_DEBUG
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
        if (REALM_LIKELY(check_set_cell(col_ndx, row_ndx))) {
#ifdef REALM_DEBUG
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
        if (REALM_LIKELY(check_set_cell(col_ndx, row_ndx))) {
#ifdef REALM_DEBUG
            if (m_log)
                *m_log << "table->set_mixed("<<col_ndx<<", "<<row_ndx<<", "<<value<<")\n";
#endif
            m_table->set_mixed(col_ndx, row_ndx, value); // Throws
            return true;
        }
        return false;
    }

    bool set_null(size_t col_ndx, size_t row_ndx)
    {
        if (REALM_LIKELY(check_set_cell(col_ndx, row_ndx))) {
#ifdef REALM_DEBUG
            if (m_log)
                *m_log << "table->set_null("<<col_ndx<<", "<<row_ndx<<")\n";
#endif
            m_table->set_null(col_ndx, row_ndx); // Throws
            return true;
        }
        return false;
    }

    bool set_link(size_t col_ndx, size_t row_ndx, std::size_t value)
    {
        if (REALM_LIKELY(check_set_cell(col_ndx, row_ndx))) {
#ifdef REALM_DEBUG
            if (m_log) {
                if (value == 0) {
                    *m_log << "table->nullify_link("<<col_ndx<<", "<<row_ndx<<")\n";
                }
                else {
                    *m_log << "table->set_link("<<col_ndx<<", "<<row_ndx<<", "<<(value-1)<<")\n";
                }
            }
#endif
            typedef _impl::TableFriend tf;
            // Map zero to realm::npos, and `n+1` to `n`, where `n` is a target row index.
            size_t target_row_ndx = value - size_t(1);
            tf::do_set_link(*m_table, col_ndx, row_ndx, target_row_ndx); // Throws
            return true;
        }
        return false;
    }

    bool insert_int(size_t col_ndx, size_t row_ndx, std::size_t, int_fast64_t value)
    {
        if (REALM_LIKELY(check_insert_cell(col_ndx, row_ndx))) {
#ifdef REALM_DEBUG
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
        if (REALM_LIKELY(check_insert_cell(col_ndx, row_ndx))) {
#ifdef REALM_DEBUG
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
        if (REALM_LIKELY(check_insert_cell(col_ndx, row_ndx))) {
#ifdef REALM_DEBUG
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
        if (REALM_LIKELY(check_insert_cell(col_ndx, row_ndx))) {
#ifdef REALM_DEBUG
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
        if (REALM_LIKELY(check_insert_cell(col_ndx, row_ndx))) {
#ifdef REALM_DEBUG
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
        if (REALM_LIKELY(check_insert_cell(col_ndx, row_ndx))) {
#ifdef REALM_DEBUG
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
        if (REALM_LIKELY(check_insert_cell(col_ndx, row_ndx))) {
#ifdef REALM_DEBUG
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
        if (REALM_LIKELY(check_insert_cell(col_ndx, row_ndx))) {
#ifdef REALM_DEBUG
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
        if (REALM_LIKELY(check_insert_cell(col_ndx, row_ndx))) {
#ifdef REALM_DEBUG
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
        REALM_ASSERT_3(value, >, 0); // Not yet any support for inserting null links
        REALM_ASSERT_3(value, >, 0); // Not yet any support for inserting null links
        if (REALM_LIKELY(check_insert_cell(col_ndx, row_ndx))) {
#ifdef REALM_DEBUG
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
        if (REALM_LIKELY(check_insert_cell(col_ndx, row_ndx))) {
#ifdef REALM_DEBUG
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
        if (REALM_LIKELY(m_table)) {
#ifdef REALM_DEBUG
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
        if (REALM_LIKELY(m_table)) {
            if (REALM_LIKELY(row_ndx <= m_table->size())) {
#ifdef REALM_DEBUG
                if (m_log)
                    *m_log << "table->insert_empty_row("<<row_ndx<<", "<<num_rows<<")\n";
#endif
                m_table->insert_empty_row(row_ndx, num_rows); // Throws
                return true;
            }
        }
        return false;
    }

    bool erase_rows(size_t row_ndx, size_t num_rows, std::size_t last_row_ndx, bool unordered)
    {
        if (REALM_UNLIKELY(!m_table))
            return false;
        if (REALM_UNLIKELY(row_ndx > last_row_ndx || last_row_ndx+1 != m_table->size()))
            return false;
        if (REALM_UNLIKELY(num_rows != 1))
            return false;
        typedef _impl::TableFriend tf;
        if (unordered) {
#ifdef REALM_DEBUG
            if (m_log)
                *m_log << "table->move_last_over("<<row_ndx<<")\n";
#endif
            tf::do_move_last_over(*m_table, row_ndx); // Throws
        }
        else {
#ifdef REALM_DEBUG
            if (m_log)
                *m_log << "table->remove("<<row_ndx<<")\n";
#endif
            tf::do_remove(*m_table, row_ndx); // Throws
        }
        return true;
    }

    bool add_int_to_column(size_t col_ndx, int_fast64_t value)
    {
        if (REALM_LIKELY(m_table)) {
            if (REALM_LIKELY(col_ndx < m_table->get_column_count())) {
                // FIXME: Don't depend on the existence of int64_t,
                // but don't allow values to use more than 64 bits
                // either.
#ifdef REALM_DEBUG
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
        if (REALM_UNLIKELY(group_level_ndx >= m_group.size()))
            return false;
#ifdef REALM_DEBUG
        if (m_log)
            *m_log << "table = group->get_table("<<group_level_ndx<<")\n";
#endif
        m_desc.reset();
        m_table = m_group.get_table(group_level_ndx); // Throws
        for (int i = 0; i < levels; ++i) {
            size_t col_ndx = path[2*i + 0];
            size_t row_ndx = path[2*i + 1];
            if (REALM_UNLIKELY(col_ndx >= m_table->get_column_count()))
                return false;
            if (REALM_UNLIKELY(row_ndx >= m_table->size()))
                return false;
#ifdef REALM_DEBUG
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
                    if (REALM_UNLIKELY(!m_table))
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
        if (REALM_LIKELY(m_table)) {
#ifdef REALM_DEBUG
            if (m_log)
                *m_log << "table->clear()\n";
#endif
            typedef _impl::TableFriend tf;
            tf::do_clear(*m_table); // Throws
            return true;
        }
        return false;
    }

    bool add_search_index(size_t col_ndx)
    {
        if (REALM_LIKELY(m_table)) {
            if (REALM_LIKELY(!m_table->has_shared_type())) {
                if (REALM_LIKELY(col_ndx < m_table->get_column_count())) {
#ifdef REALM_DEBUG
                    if (m_log)
                        *m_log << "table->add_search_index("<<col_ndx<<")\n";
#endif
                    m_table->add_search_index(col_ndx); // Throws
                    return true;
                }
            }
        }
        return false;
    }

    bool remove_search_index(size_t col_ndx)
    {
        if (REALM_LIKELY(m_table)) {
            if (REALM_LIKELY(!m_table->has_shared_type())) {
                if (REALM_LIKELY(col_ndx < m_table->get_column_count())) {
#ifdef REALM_DEBUG
                    if (m_log)
                        *m_log << "table->remove_search_index("<<col_ndx<<")\n";
#endif
                    m_table->remove_search_index(col_ndx); // Throws
                    return true;
                }
            }
        }
        return false;
    }

    bool add_primary_key(size_t col_ndx)
    {
        if (REALM_LIKELY(m_table)) {
            if (REALM_LIKELY(!m_table->has_shared_type())) {
                if (REALM_LIKELY(col_ndx < m_table->get_column_count())) {
#ifdef REALM_DEBUG
                    if (m_log)
                        *m_log << "table->add_primary_key("<<col_ndx<<")\n";
#endif
                    // Fails if there are duplicate values, but given valid
                    // transaction logs, there never will be.
                    bool success = m_table->try_add_primary_key(col_ndx); // Throws
                    return success;
                }
            }
        }
        return false;
    }

    bool remove_primary_key()
    {
        if (REALM_LIKELY(m_table)) {
            if (REALM_LIKELY(!m_table->has_shared_type())) {
#ifdef REALM_DEBUG
                if (m_log)
                    *m_log << "table->remove_primary_key()\n";
#endif
                m_table->remove_primary_key(); // Throws
                return true;
            }
        }
        return false;
    }

    bool set_link_type(size_t col_ndx, LinkType link_type)
    {
        if (REALM_LIKELY(m_table)) {
            if (REALM_LIKELY(col_ndx < m_desc->get_column_count())) {
#ifdef REALM_DEBUG
                if (m_log)
                    *m_log << "table->set_link_type("<<col_ndx<<", "
                        "\""<<link_type_to_str(link_type)<<"\")\n";
#endif
                typedef _impl::TableFriend tf;
                tf::set_link_type(*m_table, col_ndx, link_type); // Throws
                return true;
            }
        }
        return false;
    }

    bool insert_column(size_t col_ndx, DataType type, StringData name, bool nullable)
    {
        if (REALM_LIKELY(m_desc)) {
            if (REALM_LIKELY(col_ndx <= m_desc->get_column_count())) {
                typedef _impl::TableFriend tf;
#ifdef REALM_DEBUG
                if (m_log) {
                    *m_log << "desc->insert_column("<<col_ndx<<", "<<data_type_to_str(type)<<", "
                        "\""<<name<< "\", "<<nullable<<")\n";
                }
#endif
                Table* link_target_table = nullptr;
                tf::insert_column(*m_desc, col_ndx, type, name, link_target_table, nullable); // Throws
                return true;
            }
        }
        return false;
    }

    bool insert_link_column(size_t col_ndx, DataType type, StringData name,
                       size_t link_target_table_ndx, size_t)
    {
        if (REALM_LIKELY(m_desc)) {
            if (REALM_LIKELY(col_ndx <= m_desc->get_column_count())) {
                typedef _impl::TableFriend tf;
#ifdef REALM_DEBUG
                if (m_log) {
                    *m_log << "desc->insert_column_link("<<col_ndx<<", "
                        ""<<data_type_to_str(type)<<", \""<<name<<"\", "
                        "group->get_table("<<link_target_table_ndx<<"))\n";
                }
#endif
                typedef _impl::GroupFriend gf;
                Table* link_target_table = &gf::get_table(m_group, link_target_table_ndx); // Throws
                tf::insert_column(*m_desc, col_ndx, type, name, link_target_table); // Throws
                return true;
            }
        }
        return false;
    }

    bool erase_column(size_t col_ndx)
    {
        if (REALM_LIKELY(m_desc)) {
            if (REALM_LIKELY(col_ndx < m_desc->get_column_count())) {
#ifdef REALM_DEBUG
                if (m_log)
                    *m_log << "desc->remove_column("<<col_ndx<<")\n";
#endif
                typedef _impl::TableFriend tf;
                tf::erase_column(*m_desc, col_ndx); // Throws
                return true;
            }
        }
        return false;
    }

    bool erase_link_column(size_t col_ndx, size_t, size_t)
    {
        if (REALM_LIKELY(m_desc)) {
            if (REALM_LIKELY(col_ndx < m_desc->get_column_count())) {
#ifdef REALM_DEBUG
                if (m_log)
                    *m_log << "desc->remove_column("<<col_ndx<<")\n";
#endif
                typedef _impl::TableFriend tf;
                tf::erase_column(*m_desc, col_ndx); // Throws
                return true;
            }
        }
        return false;
    }

    bool rename_column(size_t col_ndx, StringData name)
    {
        if (REALM_LIKELY(m_desc)) {
            if (REALM_LIKELY(col_ndx < m_desc->get_column_count())) {
#ifdef REALM_DEBUG
                if (m_log)
                    *m_log << "desc->rename_column("<<col_ndx<<", \""<<name<<"\")\n";
#endif
                typedef _impl::TableFriend tf;
                tf::rename_column(*m_desc, col_ndx, name); // Throws
                return true;
            }
        }
        return false;
    }

    bool select_descriptor(int levels, const size_t* path)
    {
        if (REALM_UNLIKELY(!m_table))
            return false;
        if (REALM_UNLIKELY(m_table->has_shared_type()))
            return false;
#ifdef REALM_DEBUG
        if (m_log)
            *m_log << "desc = table->get_descriptor()\n";
#endif
        m_desc = m_table->get_descriptor(); // Throws
        for (int i = 0; i < levels; ++i) {
            size_t col_ndx = path[i];
            if (REALM_UNLIKELY(col_ndx >= m_desc->get_column_count()))
                return false;
#ifdef REALM_DEBUG
            if (m_log)
                *m_log << "desc = desc->get_subdescriptor("<<col_ndx<<")\n";
#endif
            m_desc = m_desc->get_subdescriptor(col_ndx);
        }
        return true;
    }

    bool insert_group_level_table(size_t table_ndx, size_t num_tables, StringData name)
    {
        if (REALM_UNLIKELY(table_ndx != num_tables))
            return false;
        if (REALM_UNLIKELY(num_tables != m_group.size()))
            return false;
#ifdef REALM_DEBUG
        if (m_log)
            *m_log << "group->add_table(\""<<name<<"\", false)\n";
#endif
        typedef _impl::GroupFriend gf;
        bool require_unique_name = false;
        gf::add_table(m_group, name, require_unique_name); // Throws
        return true;
    }

    bool erase_group_level_table(std::size_t table_ndx, size_t num_tables) REALM_NOEXCEPT
    {
        if (REALM_UNLIKELY(num_tables != m_group.size()))
            return false;
#ifdef REALM_DEBUG
        if (m_log)
            *m_log << "group->remove_table("<<table_ndx<<")\n";
#endif
        m_group.remove_table(table_ndx);
        return true;
    }

    bool rename_group_level_table(std::size_t table_ndx, StringData new_name) REALM_NOEXCEPT
    {
#ifdef REALM_DEBUG
        if (m_log)
            *m_log << "group->rename_table("<<table_ndx<<", \""<<new_name<<"\")\n";
#endif
        m_group.rename_table(table_ndx, new_name);
        return true;
    }

    bool optimize_table()
    {
        if (REALM_LIKELY(m_table)) {
            if (REALM_LIKELY(!m_table->has_shared_type())) {
#ifdef REALM_DEBUG
                if (m_log)
                    *m_log << "table->optimize()\n";
#endif
                m_table->optimize(); // Throws
                return true;
            }
        }
        return false;
    }

    bool select_link_list(size_t col_ndx, size_t row_ndx)
    {
        if (REALM_UNLIKELY(!m_table))
            return false;
        if (REALM_UNLIKELY(col_ndx >= m_table->get_column_count()))
            return false;
        DataType type = m_table->get_column_type(col_ndx);
        if (REALM_UNLIKELY(type != type_LinkList))
            return false;
#ifdef REALM_DEBUG
        if (m_log)
            *m_log << "link_list = table->get_link_list("<<col_ndx<<", "<<row_ndx<<")\n";
#endif
        m_link_list = m_table->get_linklist(col_ndx, row_ndx); // Throws
        return true;
    }

    bool link_list_set(size_t link_ndx, size_t value)
    {
        if (REALM_UNLIKELY(!m_link_list))
            return false;
        if (REALM_UNLIKELY(link_ndx >= m_link_list->size()))
            return false;
#ifdef REALM_DEBUG
        if (m_log)
            *m_log << "link_list->set("<<link_ndx<<", "<<value<<")\n";
#endif
        typedef _impl::LinkListFriend llf;
        llf::do_set(*m_link_list, link_ndx, value); // Throws
        return true;
    }

    bool link_list_insert(size_t link_ndx, size_t value)
    {
        if (REALM_UNLIKELY(!m_link_list))
            return false;
        if (REALM_UNLIKELY(link_ndx > m_link_list->size()))
            return false;
#ifdef REALM_DEBUG
        if (m_log)
            *m_log << "link_list->insert("<<link_ndx<<", "<<value<<")\n";
#endif
        m_link_list->insert(link_ndx, value); // Throws
        return true;
    }

    bool link_list_move(size_t old_link_ndx, size_t new_link_ndx)
    {
        if (REALM_UNLIKELY(!m_link_list))
            return false;
        size_t num_links = m_link_list->size();
        if (REALM_UNLIKELY(old_link_ndx >= num_links))
            return false;
        if (REALM_UNLIKELY(new_link_ndx > num_links))
            return false;
#ifdef REALM_DEBUG
        if (m_log)
            *m_log << "link_list->move("<<old_link_ndx<<", "<<new_link_ndx<<")\n";
#endif
        m_link_list->move(old_link_ndx, new_link_ndx); // Throws
        return true;
    }

    bool link_list_swap(size_t link1_ndx, size_t link2_ndx)
    {
        if (REALM_UNLIKELY(!m_link_list))
            return false;
        size_t num_links = m_link_list->size();
        if (REALM_UNLIKELY(link1_ndx >= num_links))
            return false;
        if (REALM_UNLIKELY(link2_ndx >= num_links))
            return false;
#ifdef REALM_DEBUG
        if (m_log)
            *m_log << "link_list->swap("<<link1_ndx<<", "<<link2_ndx<<")\n";
#endif
        m_link_list->swap(link1_ndx, link2_ndx); // Throws
        return true;
    }

    bool link_list_erase(size_t link_ndx)
    {
        if (REALM_UNLIKELY(!m_link_list))
            return false;
        if (REALM_UNLIKELY(link_ndx >= m_link_list->size()))
            return false;
#ifdef REALM_DEBUG
        if (m_log)
            *m_log << "link_list->remove("<<link_ndx<<")\n";
#endif
        typedef _impl::LinkListFriend llf;
        llf::do_remove(*m_link_list, link_ndx); // Throws
        return true;
    }

    bool link_list_clear()
    {
        if (REALM_UNLIKELY(!m_link_list))
            return false;
#ifdef REALM_DEBUG
        if (m_log)
            *m_log << "link_list->clear()\n";
#endif
        typedef _impl::LinkListFriend llf;
        llf::do_clear(*m_link_list); // Throws
        return true;
    }

    bool nullify_link(size_t, size_t, size_t)
    {
        return true; // No-op
    }

    bool link_list_nullify(size_t, size_t)
    {
        return true; // No-op
    }

private:
    Group& m_group;
    TableRef m_table;
    DescriptorRef m_desc;
    LinkViewRef m_link_list;
    std::ostream* m_log;

    bool check_set_cell(size_t col_ndx, size_t row_ndx) REALM_NOEXCEPT
    {
        if (REALM_LIKELY(m_table)) {
            if (REALM_LIKELY(col_ndx < m_table->get_column_count())) {
                if (REALM_LIKELY(row_ndx < m_table->size()))
                    return true;
            }
        }
        return false;
    }

    bool check_insert_cell(size_t col_ndx, size_t row_ndx) REALM_NOEXCEPT
    {
        if (REALM_LIKELY(m_table)) {
            if (REALM_LIKELY(col_ndx < m_table->get_column_count())) {
                if (REALM_LIKELY(row_ndx <= m_table->size()))
                    return true;
            }
        }
        return false;
    }

    const char* data_type_to_str(DataType type)
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
        REALM_ASSERT(false);
        return 0;
    }

    const char* link_type_to_str(LinkType type)
    {
        switch (type) {
            case link_Strong:
                return "link_Strong";
            case link_Weak:
                return "link_Weak";
        }
        REALM_ASSERT(false);
        return 0;
    }
};


void Replication::apply_changeset(InputStream& in, Group& group, std::ostream* log)
{
    _impl::TransactLogParser parser; // Throws
    TransactLogApplier applier(group);
    applier.set_apply_log(log);
    parser.parse(in, applier); // Throws
}


namespace {

class InputStreamImpl: public _impl::NoCopyInputStream {
public:
    InputStreamImpl(const char* data, size_t size) REALM_NOEXCEPT:
        m_begin(data), m_end(data+size) {}

    ~InputStreamImpl() REALM_NOEXCEPT {}

    size_t next_block(const char*& begin, const char*& end) override
    {
        if (m_begin != 0) {
            begin = m_begin;
            end = m_end;
            m_begin = nullptr;
            return end - begin;
        }
        return 0;
    }
    const char* m_begin;
    const char* const m_end;
};

} // anonymous namespace

void TrivialReplication::apply_changeset(const char* data, size_t size, SharedGroup& target,
                                         std::ostream* log)
{
    InputStreamImpl in(data, size);
    WriteTransaction wt(target); // Throws
    Replication::apply_changeset(in, wt.get_group(), log); // Throws
    wt.commit(); // Throws
}

std::string TrivialReplication::do_get_database_path()
{
    return m_database_file;
}

void TrivialReplication::do_initiate_transact(SharedGroup&, version_type)
{
    char* data = m_transact_log_buffer.data();
    std::size_t size = m_transact_log_buffer.size();
    set_buffer(data, data + size);
}

Replication::version_type
TrivialReplication::do_prepare_commit(SharedGroup&, version_type orig_version)
{
    char* data = m_transact_log_buffer.data();
    std::size_t size = write_position() - data;
    version_type new_version = orig_version + 1;
    prepare_changeset(data, size, new_version); // Throws
    return new_version;
}

void TrivialReplication::do_finalize_commit(SharedGroup&) REALM_NOEXCEPT
{
    finalize_changeset();
}

void TrivialReplication::do_abort_transact(SharedGroup&) REALM_NOEXCEPT
{
}

void TrivialReplication::do_interrupt() REALM_NOEXCEPT
{
}

void TrivialReplication::do_clear_interrupt() REALM_NOEXCEPT
{
}

void TrivialReplication::transact_log_append(const char* data, std::size_t size, char** new_begin, char** new_end)
{
    internal_transact_log_reserve(size, new_begin, new_end);
    *new_begin = std::copy(data, data + size, *new_begin);
}
