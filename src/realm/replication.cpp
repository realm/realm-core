#include <stdexcept>
#include <utility>
#include <iomanip>


#include <realm/group.hpp>
#include <realm/table.hpp>
#include <realm/descriptor.hpp>
#include <realm/link_view.hpp>
#include <realm/group_shared.hpp>
#include <realm/replication.hpp>

using namespace realm;
using namespace realm::util;


void Replication::set_replication(Group& group, Replication* repl) noexcept
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

    ~TransactLogApplier() noexcept
    {
    }

    void set_logger(util::Logger* logger) noexcept
    {
        m_logger = logger;
    }

    bool set_int(size_t col_ndx, size_t row_ndx, int_fast64_t value)
    {
        if (REALM_LIKELY(check_set_cell(col_ndx, row_ndx))) {
            log("table->set_int(%1, %2, %3);", col_ndx, row_ndx, value); // Throws
            m_table->set_int(col_ndx, row_ndx, value); // Throws
            return true;
        }
        return false;
    }

    bool set_int_unique(size_t col_ndx, size_t row_ndx, size_t prior_num_rows, int_fast64_t value)
    {
        if (REALM_LIKELY(check_set_cell(col_ndx, row_ndx))) {
            if (REALM_UNLIKELY(prior_num_rows != m_table->size())) {
                return false;
            }
            log("table->set_int_unique(%1, %2, %3);", col_ndx, row_ndx, value); // Throws
            m_table->set_int_unique(col_ndx, row_ndx, value); // Throws
            return true;
        }
        return false;
    }

    bool set_bool(size_t col_ndx, size_t row_ndx, bool value)
    {
        if (REALM_LIKELY(check_set_cell(col_ndx, row_ndx))) {
            log("table->set_bool(%1, %2, %3);", col_ndx, row_ndx, value); // Throws
            m_table->set_bool(col_ndx, row_ndx, value); // Throws
            return true;
        }
        return false;
    }

    bool set_float(size_t col_ndx, size_t row_ndx, float value)
    {
        if (REALM_LIKELY(check_set_cell(col_ndx, row_ndx))) {
            log("table->set_float(%1, %2, %3);", col_ndx, row_ndx, value); // Throws
            m_table->set_float(col_ndx, row_ndx, value); // Throws
            return true;
        }
        return false;
    }

    bool set_double(size_t col_ndx, size_t row_ndx, double value)
    {
        if (REALM_LIKELY(check_set_cell(col_ndx, row_ndx))) {
            log("table->set_double(%1, %2, %3);", col_ndx, row_ndx, value); // Throws
            m_table->set_double(col_ndx, row_ndx, value); // Throws
            return true;
        }
        return false;
    }

    bool set_string(size_t col_ndx, size_t row_ndx, StringData value)
    {
        if (REALM_LIKELY(check_set_cell(col_ndx, row_ndx))) {
            log("table->set_string(%1, %2, \"%3\");", col_ndx, row_ndx, value); // Throws
            m_table->set_string(col_ndx, row_ndx, value); // Throws
            return true;
        }
        return false;
    }

    bool set_string_unique(size_t col_ndx, size_t row_ndx, size_t prior_num_rows, StringData value)
    {
        if (REALM_LIKELY(check_set_cell(col_ndx, row_ndx))) {
            if (REALM_UNLIKELY(prior_num_rows != m_table->size())) {
                return false;
            }
            log("table->set_string_unique(%1, %2, \"%3\");", col_ndx, row_ndx, value); // Throws
            m_table->set_string_unique(col_ndx, row_ndx, value); // Throws
            return true;
        }
        return false;
    }

    bool set_binary(size_t col_ndx, size_t row_ndx, BinaryData value)
    {
        if (REALM_LIKELY(check_set_cell(col_ndx, row_ndx))) {
            log("table->set_binary(%1, %2, %3);", col_ndx, row_ndx, value); // Throws
            m_table->set_binary(col_ndx, row_ndx, value); // Throws
            return true;
        }
        return false;
    }

    bool set_date_time(size_t col_ndx, size_t row_ndx, DateTime value)
    {
        if (REALM_LIKELY(check_set_cell(col_ndx, row_ndx))) {
            log("table->set_datetime(%1, %2, %3);", col_ndx, row_ndx, value); // Throws
            m_table->set_datetime(col_ndx, row_ndx, value); // Throws
            return true;
        }
        return false;
    }

    bool set_table(size_t col_ndx, size_t row_ndx)
    {
        if (REALM_LIKELY(check_set_cell(col_ndx, row_ndx))) {
            log("table->clear_subtable(%1, %2);", col_ndx, row_ndx); // Throws
            m_table->clear_subtable(col_ndx, row_ndx); // Throws
            return true;
        }
        return false;
    }

    bool set_mixed(size_t col_ndx, size_t row_ndx, const Mixed& value)
    {
        if (REALM_LIKELY(check_set_cell(col_ndx, row_ndx))) {
            log("table->set_mixed(%1, %2, %3);", col_ndx, row_ndx, value); // Throws
            m_table->set_mixed(col_ndx, row_ndx, value); // Throws
            return true;
        }
        return false;
    }

    bool set_null(size_t col_ndx, size_t row_ndx)
    {
        if (REALM_LIKELY(check_set_cell(col_ndx, row_ndx))) {
            log("table->set_null(%1, %2);", col_ndx, row_ndx); // Throws
            m_table->set_null(col_ndx, row_ndx); // Throws
            return true;
        }
        return false;
    }

    bool set_link(size_t col_ndx, size_t row_ndx, size_t target_row_ndx, size_t)
    {
        if (REALM_LIKELY(check_set_cell(col_ndx, row_ndx))) {
            if (target_row_ndx == realm::npos) {
                log("table->nullify_link(%1, %2);", col_ndx, row_ndx); // Throws
            }
            else {
                log("table->set_link(%1, %2, %3);", col_ndx, row_ndx, target_row_ndx); // Throws
            }
            typedef _impl::TableFriend tf;
            tf::do_set_link(*m_table, col_ndx, row_ndx, target_row_ndx); // Throws
            return true;
        }
        return false;
    }

    bool insert_substring(size_t col_ndx, size_t row_ndx, size_t pos, StringData value)
    {
        if (REALM_UNLIKELY(!m_table))
            return false;
        log("table->insert_substring(%1, %2, %3, %4);", col_ndx, row_ndx, pos, value); // Throws
        try {
            m_table->insert_substring(col_ndx, row_ndx, pos, value); // Throws
            return true;
        }
        catch (LogicError&) {
            return false;
        }
    }

    bool erase_substring(size_t col_ndx, size_t row_ndx, size_t pos, size_t size)
    {
        if (REALM_UNLIKELY(!m_table))
            return false;
        log("table->remove_substring(%1, %2, %3, %4);", col_ndx, row_ndx, pos, size); // Throws
        try {
            m_table->remove_substring(col_ndx, row_ndx, pos, size); // Throws
            return true;
        }
        catch (LogicError&) {
            return false;
        }
    }

    bool insert_empty_rows(size_t row_ndx, size_t num_rows_to_insert, size_t prior_num_rows,
                           bool unordered)
    {
        if (REALM_UNLIKELY(!m_table))
            return false;
        if (REALM_UNLIKELY(row_ndx > prior_num_rows))
            return false;
        if (REALM_UNLIKELY(prior_num_rows != m_table->size()))
            return false;
        if (REALM_UNLIKELY(unordered && row_ndx != prior_num_rows))
            return false;
        log("table->insert_empty_row(%1, %2);", row_ndx, num_rows_to_insert); // Throws
        m_table->insert_empty_row(row_ndx, num_rows_to_insert); // Throws
        return true;
    }

    bool erase_rows(size_t row_ndx, size_t num_rows_to_erase, size_t prior_num_rows,
                    bool unordered)
    {
        if (REALM_UNLIKELY(!m_table))
            return false;
        if (REALM_UNLIKELY(row_ndx >= prior_num_rows))
            return false;
        if (REALM_UNLIKELY(num_rows_to_erase != 1))
            return false;
        if (REALM_UNLIKELY(prior_num_rows != m_table->size()))
            return false;
        typedef _impl::TableFriend tf;
        if (unordered) {
            log("table->move_last_over(%1);", row_ndx); // Throws
            tf::do_move_last_over(*m_table, row_ndx); // Throws
        }
        else {
            log("table->remove(%1);", row_ndx); // Throws
            tf::do_remove(*m_table, row_ndx); // Throws
        }
        return true;
    }

    bool swap_rows(size_t row_ndx_1, size_t row_ndx_2)
    {
        if (REALM_UNLIKELY(!m_table))
            return false;
        if (REALM_UNLIKELY(row_ndx_1 >= m_table->size() || row_ndx_2 >= m_table->size()))
            return false;
        log("table->swap_rows(%1, %2);", row_ndx_1, row_ndx_2); // Throws
        using tf = _impl::TableFriend;
        tf::do_swap_rows(*m_table, row_ndx_1, row_ndx_2); // Throws
        return true;
    }

    bool subsume_identity(size_t row_ndx, size_t subsumed_by_row_ndx)
    {
        if (REALM_UNLIKELY(!m_table))
            return false;
        if (REALM_UNLIKELY(row_ndx >= m_table->size() || subsumed_by_row_ndx >= m_table->size()))
            return false;
        log("table->subsume_identity(%1, %2);", row_ndx, subsumed_by_row_ndx); // Throws
        using tf = _impl::TableFriend;
        tf::do_subsume_identity(*m_table, row_ndx, subsumed_by_row_ndx); // Throws
        return true;
    }

    bool select_table(size_t group_level_ndx, int levels, const size_t* path)
    {
        if (REALM_UNLIKELY(group_level_ndx >= m_group.size()))
            return false;
        log("table = group->get_table(%1);", group_level_ndx); // Throws
        m_desc.reset();
        m_table = m_group.get_table(group_level_ndx); // Throws
        for (int i = 0; i < levels; ++i) {
            size_t col_ndx = path[2*i + 0];
            size_t row_ndx = path[2*i + 1];
            if (REALM_UNLIKELY(col_ndx >= m_table->get_column_count()))
                return false;
            if (REALM_UNLIKELY(row_ndx >= m_table->size()))
                return false;
            log("table = table->get_subtable(%1, %2);", col_ndx, row_ndx); // Throws
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
        if (REALM_LIKELY(m_table && m_table->is_attached())) {
            log("table->clear();"); // Throws
            typedef _impl::TableFriend tf;
            tf::do_clear(*m_table); // Throws
            return true;
        }
        return false;
    }

    bool add_search_index(size_t col_ndx)
    {
        if (REALM_LIKELY(m_table && m_table->is_attached())) {
            if (REALM_LIKELY(!m_table->has_shared_type())) {
                if (REALM_LIKELY(col_ndx < m_table->get_column_count())) {
                    log("table->add_search_index(%1);", col_ndx); // Throws
                    m_table->add_search_index(col_ndx); // Throws
                    return true;
                }
            }
        }
        return false;
    }

    bool remove_search_index(size_t col_ndx)
    {
        if (REALM_LIKELY(m_table && m_table->is_attached())) {
            if (REALM_LIKELY(!m_table->has_shared_type())) {
                if (REALM_LIKELY(col_ndx < m_table->get_column_count())) {
                    log("table->remove_search_index(%1);", col_ndx); // Throws
                    m_table->remove_search_index(col_ndx); // Throws
                    return true;
                }
            }
        }
        return false;
    }

    bool set_link_type(size_t col_ndx, LinkType link_type)
    {
        if (REALM_LIKELY(m_table && m_desc)) {
            if (REALM_LIKELY(col_ndx < m_desc->get_column_count())) {
                using tf = _impl::TableFriend;
                DataType type = m_table->get_column_type(col_ndx);
                if (REALM_UNLIKELY(!tf::is_link_type(ColumnType(type))))
                    return false;
                log("table->set_link_type(%1, %2);", col_ndx,
                    link_type_to_str(link_type)); // Throws
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
                log("desc->insert_column(%1, %2, \"%3\", %4);", col_ndx, data_type_to_str(type),
                    name, nullable); // Throws
                Table* link_target_table = nullptr;
                using tf = _impl::TableFriend;
                tf::insert_column_unless_exists(*m_desc, col_ndx, type, name, link_target_table, nullable); // Throws
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
                log("desc->insert_column_link(%1, %2, \"%3\", group->get_table(%4));", col_ndx,
                    data_type_to_str(type), name, link_target_table_ndx); // Throws
                using gf = _impl::GroupFriend;
                using tf = _impl::TableFriend;
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
                log("desc->remove_column(%1);", col_ndx); // Throws
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
                log("desc->remove_column(%1);", col_ndx); // Throws
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
                log("desc->rename_column(%1, \"%2\");", col_ndx, name); // Throws
                typedef _impl::TableFriend tf;
                tf::rename_column(*m_desc, col_ndx, name); // Throws
                return true;
            }
        }
        return false;
    }

    bool move_column(size_t col_ndx_1, size_t col_ndx_2)
    {
        if (REALM_LIKELY(m_desc)) {
            size_t column_count = m_desc->get_column_count();
            if (REALM_LIKELY(col_ndx_1 < column_count && col_ndx_2 < column_count)) {
                log("desc->move_column(%1, %2);", col_ndx_1, col_ndx_2); // Throws
                typedef _impl::TableFriend tf;
                tf::move_column(*m_desc, col_ndx_1, col_ndx_2); // Throws
                return true;
            }
        }
        return false;
    }

    bool select_descriptor(int levels, const size_t* path)
    {
        if (REALM_UNLIKELY(!m_table))
            return false;
        if (REALM_UNLIKELY(!m_table->is_attached()))
            return false;
        if (REALM_UNLIKELY(m_table->has_shared_type()))
            return false;
        log("desc = table->get_descriptor();"); // Throws
        m_desc = m_table->get_descriptor(); // Throws
        for (int i = 0; i < levels; ++i) {
            size_t col_ndx = path[i];
            if (REALM_UNLIKELY(col_ndx >= m_desc->get_column_count()))
                return false;
            if (REALM_UNLIKELY(m_desc->get_column_type(col_ndx) != type_Table))
                return false;
            log("desc = desc->get_subdescriptor(%1);", col_ndx); // Throws
            m_desc = m_desc->get_subdescriptor(col_ndx);
        }
        return true;
    }

    bool insert_group_level_table(size_t table_ndx, size_t prior_num_tables, StringData name)
    {
        if (REALM_UNLIKELY(prior_num_tables != m_group.size()))
            return false;
        if (REALM_UNLIKELY(table_ndx > m_group.size()))
            return false;
        if (REALM_UNLIKELY(name.size() >= ArrayString::max_width))
            return false;
        log("group->insert_table(%1, \"%2\", false);", table_ndx, name); // Throws
        typedef _impl::GroupFriend gf;
        bool was_inserted;
        gf::get_or_insert_table(m_group, table_ndx, name, &was_inserted); // Throws
        return true;
    }

    bool erase_group_level_table(size_t table_ndx, size_t num_tables) noexcept
    {
        if (REALM_UNLIKELY(num_tables != m_group.size()))
            return false;
        if (REALM_UNLIKELY(table_ndx >= m_group.size()))
            return false;
        log("group->remove_table(%1);", table_ndx); // Throws
        m_group.remove_table(table_ndx);
        return true;
    }

    bool rename_group_level_table(size_t table_ndx, StringData new_name) noexcept
    {
        if (REALM_UNLIKELY(table_ndx >= m_group.size()))
            return false;
        if (REALM_UNLIKELY(m_group.has_table(new_name)))
            return false;
        if (REALM_UNLIKELY(new_name.size() >= ArrayString::max_width))
            return false;
        log("group->rename_table(%1, \"%2\");", table_ndx, new_name); // Throws
        m_group.rename_table(table_ndx, new_name);
        return true;
    }

    bool move_group_level_table(size_t table_ndx_1, size_t table_ndx_2) noexcept
    {
        if (REALM_UNLIKELY(table_ndx_1 == table_ndx_2))
            return false;
        if (REALM_UNLIKELY(table_ndx_1 >= m_group.size()))
            return false;
        if (REALM_UNLIKELY(table_ndx_2 >= m_group.size()))
            return false;
        log("group->move_table(%1, %2);", table_ndx_1, table_ndx_2); // Throws
        m_group.move_table(table_ndx_1, table_ndx_2);
        return true;
    }

    bool optimize_table()
    {
        if (REALM_LIKELY(m_table && m_table->is_attached())) {
            if (REALM_LIKELY(!m_table->has_shared_type())) {
                log("table->optimize();"); // Throws
                m_table->optimize(); // Throws
                return true;
            }
        }
        return false;
    }

    bool select_link_list(size_t col_ndx, size_t row_ndx, size_t)
    {
        if (REALM_UNLIKELY(!m_table))
            return false;
        if (REALM_UNLIKELY(!m_table->is_attached()))
            return false;
        if (REALM_UNLIKELY(col_ndx >= m_table->get_column_count()))
            return false;
        DataType type = m_table->get_column_type(col_ndx);
        if (REALM_UNLIKELY(type != type_LinkList))
            return false;
        log("link_list = table->get_link_list(%1, %2);", col_ndx, row_ndx); // Throws
        m_link_list = m_table->get_linklist(col_ndx, row_ndx); // Throws
        return true;
    }

    bool link_list_set(size_t link_ndx, size_t value)
    {
        if (REALM_UNLIKELY(!m_link_list))
            return false;
        if (REALM_UNLIKELY(link_ndx >= m_link_list->size()))
            return false;
        log("link_list->set(%1, %2);", link_ndx, value); // Throws
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
        log("link_list->insert(%1, %2);", link_ndx, value); // Throws
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
        if (REALM_UNLIKELY(new_link_ndx >= num_links))
            return false;
        log("link_list->move(%1, %2);", old_link_ndx, new_link_ndx); // Throws
        m_link_list->move(old_link_ndx, new_link_ndx); // Throws
        return true;
    }

    bool link_list_swap(size_t link_ndx_1, size_t link_ndx_2)
    {
        if (REALM_UNLIKELY(!m_link_list))
            return false;
        size_t num_links = m_link_list->size();
        if (REALM_UNLIKELY(link_ndx_1 >= num_links))
            return false;
        if (REALM_UNLIKELY(link_ndx_2 >= num_links))
            return false;
        log("link_list->swap(%1, %2);", link_ndx_1, link_ndx_2); // Throws
        m_link_list->swap(link_ndx_1, link_ndx_2); // Throws
        return true;
    }

    bool link_list_erase(size_t link_ndx)
    {
        if (REALM_UNLIKELY(!m_link_list))
            return false;
        if (REALM_UNLIKELY(link_ndx >= m_link_list->size()))
            return false;
        log("link_list->remove(%1);", link_ndx); // Throws
        typedef _impl::LinkListFriend llf;
        llf::do_remove(*m_link_list, link_ndx); // Throws
        return true;
    }

    bool link_list_clear(size_t)
    {
        if (REALM_UNLIKELY(!m_link_list))
            return false;
        log("link_list->clear();"); // Throws
        typedef _impl::LinkListFriend llf;
        llf::do_clear(*m_link_list); // Throws
        return true;
    }

    bool nullify_link(size_t col_ndx, size_t row_ndx, size_t target_group_level_ndx)
    {
        return set_link(col_ndx, row_ndx, realm::npos, target_group_level_ndx);
    }

    bool link_list_nullify(size_t link_ndx)
    {
        return link_list_erase(link_ndx);
    }

private:
    Group& m_group;
    TableRef m_table;
    DescriptorRef m_desc;
    LinkViewRef m_link_list;
    util::Logger* m_logger = nullptr;

    bool check_set_cell(size_t col_ndx, size_t row_ndx) noexcept
    {
        if (REALM_LIKELY(m_table && m_table->is_attached())) {
            if (REALM_LIKELY(col_ndx < m_table->get_column_count())) {
                if (REALM_LIKELY(row_ndx < m_table->size()))
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

#ifdef REALM_DEBUG
    template<class... Params> void log(const char* message, Params... params)
    {
        if (m_logger)
            m_logger->log(message, params...); // Throws
    }
#else
    template<class... Params> void log(const char*, Params...)
    {
    }
#endif
};


void Replication::apply_changeset(InputStream& in, Group& group, util::Logger* logger)
{
    _impl::TransactLogParser parser; // Throws
    TransactLogApplier applier(group);
    applier.set_logger(logger);
    parser.parse(in, applier); // Throws
}


namespace {

class InputStreamImpl: public _impl::NoCopyInputStream {
public:
    InputStreamImpl(const char* data, size_t size) noexcept:
        m_begin(data), m_end(data+size) {}

    ~InputStreamImpl() noexcept {}

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
                                         util::Logger* logger)
{
    InputStreamImpl in(data, size);
    WriteTransaction wt(target); // Throws
    Replication::apply_changeset(in, wt.get_group(), logger); // Throws
    wt.commit(); // Throws
}

std::string TrivialReplication::do_get_database_path()
{
    return m_database_file;
}

void TrivialReplication::do_initiate_transact(SharedGroup&, version_type)
{
    char* data = m_transact_log_buffer.data();
    size_t size = m_transact_log_buffer.size();
    set_buffer(data, data + size);
}

Replication::version_type
TrivialReplication::do_prepare_commit(SharedGroup&, version_type orig_version)
{
    char* data = m_transact_log_buffer.data();
    size_t size = write_position() - data;
    version_type new_version = orig_version + 1;
    prepare_changeset(data, size, new_version); // Throws
    return new_version;
}

void TrivialReplication::do_finalize_commit(SharedGroup&) noexcept
{
    finalize_changeset();
}

void TrivialReplication::do_abort_transact(SharedGroup&) noexcept
{
}

void TrivialReplication::do_interrupt() noexcept
{
}

void TrivialReplication::do_clear_interrupt() noexcept
{
}

void TrivialReplication::transact_log_append(const char* data, size_t size, char** new_begin, char** new_end)
{
    internal_transact_log_reserve(size, new_begin, new_end);
    *new_begin = std::copy(data, data + size, *new_begin);
}
