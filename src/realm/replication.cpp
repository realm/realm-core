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

#include <stdexcept>
#include <utility>
#include <iomanip>

#include <realm/group.hpp>
#include <realm/table.hpp>
#include <realm/link_view.hpp>
#include <realm/group_shared.hpp>
#include <realm/replication.hpp>
#include <realm/util/logger.hpp>

using namespace realm;
using namespace realm::util;


class Replication::TransactLogApplier {
public:
    TransactLogApplier(Group& group)
        : m_group(group)
    {
    }

    ~TransactLogApplier() noexcept
    {
    }

    void set_logger(util::Logger* logger) noexcept
    {
        m_logger = logger;
    }

    bool set_int(size_t col_ndx, Key key, int_fast64_t value, _impl::Instruction variant, size_t prior_num_rows)
    {
        static_cast<void>(prior_num_rows);
        static_cast<void>(variant);
        if (REALM_LIKELY(REALM_COVER_ALWAYS(check_set_cell(col_ndx, key)))) {
            if (REALM_UNLIKELY(REALM_COVER_NEVER(variant == _impl::instr_SetUnique))) {
                if (REALM_UNLIKELY(prior_num_rows != m_table->size())) {
                    return false;
                }
            }
            log("table(%1)->set_int(%2, %3);", key.value, col_ndx, value); // Throws
            // Set and SetDefault are identical in this context.
            // For SetUnique, it is acceptable to call the regular version of
            // set_int(), because we presume that the side-effects of
            // set_int_unique() are already documented as other instructions
            // preceding this. Calling the set_int_unique() here would be a
            // waste of time, because all possible side-effects have already
            // been carried out.
            try {
                m_table->get_object(key).set(col_ndx, value); // Throws
                return true;
            }
            catch (...) {
            }
        }
        return false;
    }

    bool add_int(size_t col_ndx, Key key, int_fast64_t value)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(check_set_cell(col_ndx, key)))) {
            log("table(%1)->add_int(%2, %3);", key.value, col_ndx, value); // Throws
            try {
                m_table->get_object(key).add_int(col_ndx, value); // Throws
                return true;
            }
            catch (...) {
            }
        }
        return false;
    }

    bool set_bool(size_t col_ndx, Key key, bool value, _impl::Instruction)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(check_set_cell(col_ndx, key)))) {
            log("table(%1)->set_bool(%2, %3);", key.value, col_ndx, value); // Throws
            try {
                m_table->get_object(key).set(col_ndx, value); // Throws
                return true;
            }
            catch (...) {
            }
        }
        return false;
    }

    bool set_float(size_t col_ndx, Key key, float value, _impl::Instruction)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(check_set_cell(col_ndx, key)))) {
            log("table(%1)->set_float(%2, %3);", key.value, col_ndx, value); // Throws
            try {
                m_table->get_object(key).set(col_ndx, value); // Throws
                return true;
            }
            catch (...) {
            }
        }
        return false;
    }

    bool set_double(size_t col_ndx, Key key, double value, _impl::Instruction)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(check_set_cell(col_ndx, key)))) {
            log("table(%1)->set_double(%2, %3);", key.value, col_ndx, value); // Throws
            try {
                m_table->get_object(key).set(col_ndx, value); // Throws
                return true;
            }
            catch (...) {
            }
        }
        return false;
    }

    bool set_string(size_t col_ndx, Key key, StringData value, _impl::Instruction variant, size_t prior_num_rows)
    {
        static_cast<void>(prior_num_rows);
        static_cast<void>(variant);
        if (REALM_LIKELY(REALM_COVER_ALWAYS(check_set_cell(col_ndx, key)))) {
            if (REALM_UNLIKELY(REALM_COVER_NEVER(variant == _impl::instr_SetUnique))) {
                if (REALM_UNLIKELY(prior_num_rows != m_table->size())) {
                    return false;
                }
            }
            log("table(%1)->set_string(%2, \"%3\");", key.value, col_ndx, value); // Throws
            // Set and SetDefault are identical in this context.
            // For SetUnique, it is acceptable to call the regular version of
            // set_int(), because we presume that the side-effects of
            // set_int_unique() are already documented as other instructions
            // preceding this. Calling the set_int_unique() here would be a
            // waste of time, because all possible side-effects have already
            // been carried out.
            try {
                m_table->get_object(key).set(col_ndx, value); // Throws
                return true;
            }
            catch (...) {
            }
        }
        return false;
    }

    bool set_binary(size_t col_ndx, Key key, BinaryData value, _impl::Instruction)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(check_set_cell(col_ndx, key)))) {
            log("table(%1)->set_binary(%2, %3);", key.value, col_ndx, value); // Throws
            if (value.size() <= ArrayBlob::max_binary_size) {
                try {
                    m_table->get_object(key).set(col_ndx, value); // Throws
                    return true;
                }
                catch (...) {
                }
            }
        }
        return false;
    }

    bool set_timestamp(size_t col_ndx, Key key, Timestamp value, _impl::Instruction)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(check_set_cell(col_ndx, key)))) {
            log("table(%1)->set_timestamp(%2, %3);", key.value, col_ndx, value); // Throws
            try {
                m_table->get_object(key).set(col_ndx, value); // Throws
                return true;
            }
            catch (...) {
            }
        }
        return false;
    }

    bool set_null(size_t col_ndx, Key key, _impl::Instruction variant, size_t prior_num_rows)
    {
        static_cast<void>(prior_num_rows);
        static_cast<void>(variant);
        if (REALM_LIKELY(REALM_COVER_ALWAYS(check_set_cell(col_ndx, key)))) {
            if (REALM_UNLIKELY(REALM_COVER_NEVER(variant == _impl::instr_SetUnique))) {
                if (REALM_UNLIKELY(prior_num_rows != m_table->size())) {
                    return false;
                }
            }
            log("table(%1)->set_null(%2);", key.value, col_ndx); // Throws
            // Set and SetDefault are identical in this context.
            // For SetUnique, it is acceptable to call the regular version of
            // set_null(), because we presume that the side-effects of
            // set_null_unique() are already documented as other instructions
            // preceding this. Calling the set_null_unique() here would be a
            // waste of time, because all possible side-effects have already
            // been carried out.
            try {
                m_table->get_object(key).set_null(col_ndx); // Throws
                return true;
            }
            catch (...) {
            }
        }
        return false;
    }

    bool set_link(size_t col_ndx, Key key, Key target_obj_key, TableKey, _impl::Instruction)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(check_set_cell(col_ndx, key)))) {
            if (target_obj_key == null_key) {
                log("table(%1)->nullify_link(%2);", key.value, col_ndx); // Throws
            }
            else {
                log("table(%1)->set_link(%1, %2, %3);", key.value, col_ndx, target_obj_key.value); // Throws
            }
            try {
                m_table->get_object(key).set(col_ndx, target_obj_key); // Throws
                return true;
            }
            catch (...) {
            }
        }
        return false;
    }

    bool insert_substring(size_t col_ndx, Key key, size_t pos, StringData value)
    {
        if (REALM_UNLIKELY(REALM_COVER_NEVER(!m_table)))
            return false;
        log("table(%1)->insert_substring(%2, %3, %4);", key.value, col_ndx, pos, value); // Throws
        try {
            // m_table->get_object(key).insert_substring(col_ndx, pos, value); TODO: implement
            return true;
        }
        catch (LogicError&) { // LCOV_EXCL_START
            return false;
        } // LCOV_EXCL_STOP
    }

    bool erase_substring(size_t col_ndx, Key key, size_t pos, size_t size)
    {
        if (REALM_UNLIKELY(REALM_COVER_NEVER(!m_table)))
            return false;
        log("table(%1)->remove_substring(%2, %3, %4);", key.value, col_ndx, pos, size); // Throws
        try {
            // m_table->get_object(key).remove_substring(col_ndx, pos, size); TODO: implement
            return true;
        }
        catch (LogicError&) { // LCOV_EXCL_START
            return false;
        } // LCOV_EXCL_STOP
    }

    bool create_object(Key key)
    {
        if (REALM_UNLIKELY(REALM_COVER_NEVER(!m_table)))
            return false;
        log("table->create_object(%1);", key.value); // Throws
        m_table->create_object(key);                 // Throws
        return true;
    }

    bool remove_object(Key key)
    {
        if (REALM_UNLIKELY(REALM_COVER_NEVER(!m_table)))
            return false;
        typedef _impl::TableFriend tf;
        log("table->remove_object(%1);", key.value); // Throws
        try {
            tf::do_remove_object(*m_table, key); // Throws
            return true;
        }
        catch (...) {
            return false;
        }
    }

    bool select_table(size_t group_level_ndx, int levels, const size_t*)
    {
        log("table = group->get_table(%1);", group_level_ndx); // Throws
        m_link_list.reset();
        try {
            m_table = m_group.get_table(TableKey(group_level_ndx)); // Throws
        }
        catch (...) {
            return false;
        }
        REALM_ASSERT(levels == 0);
        /*
        for (int i = 0; i < levels; ++i) {
            size_t col_ndx = path[2 * i + 0];
            size_t row_ndx = path[2 * i + 1];
            if (REALM_UNLIKELY(REALM_COVER_NEVER(col_ndx >= m_table->get_column_count())))
                return false;
            if (REALM_UNLIKELY(REALM_COVER_NEVER(row_ndx >= m_table->size())))
                return false;
            log("table = table->get_subtable(%1, %2);", col_ndx, row_ndx); // Throws
            DataType type = m_table->get_column_type(col_ndx);
            switch (type) {
                case type_Table:
                    m_table = m_table->get_subtable(col_ndx, row_ndx); // Throws
                    break;
                case type_Mixed:
                    m_table = m_table->get_subtable(col_ndx, row_ndx); // Throws
                    if (REALM_UNLIKELY(REALM_COVER_NEVER(!m_table)))
                        return false;
                    break;
                default:
                    return false;
            }
        }
        */
        return true;
    }

    bool clear_table(size_t)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(m_table && m_table->is_attached()))) {
            log("table->clear();"); // Throws
            typedef _impl::TableFriend tf;
            tf::do_clear(*m_table); // Throws
            return true;
        }
        return false;
    }

    bool add_search_index(size_t col_ndx)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(m_table && m_table->is_attached()))) {
            if (REALM_LIKELY(REALM_COVER_ALWAYS(col_ndx < m_table->get_column_count()))) {
                log("table->add_search_index(%1);", col_ndx); // Throws
                m_table->add_search_index(col_ndx);           // Throws
                return true;
            }
        }
        return false;
    }

    bool remove_search_index(size_t col_ndx)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(m_table && m_table->is_attached()))) {
            if (REALM_LIKELY(REALM_COVER_ALWAYS(col_ndx < m_table->get_column_count()))) {
                log("desc->remove_search_index(%1);", col_ndx); // Throws
                m_table->remove_search_index(col_ndx);          // Throws
                return true;
            }
        }
        return false;
    }

    bool set_link_type(size_t col_ndx, LinkType link_type)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(m_table && m_table->is_attached()))) {
            if (REALM_LIKELY(REALM_COVER_ALWAYS(col_ndx < m_table->get_column_count()))) {
                using tf = _impl::TableFriend;
                DataType type = m_table->get_column_type(col_ndx);
                static_cast<void>(type);
                if (REALM_UNLIKELY(REALM_COVER_NEVER(!tf::is_link_type(ColumnType(type)))))
                    return false;
                log("table->set_link_type(%1, %2);", col_ndx, link_type_to_str(link_type)); // Throws
                tf::set_link_type(*m_table, col_ndx, link_type);                            // Throws
                return true;
            }
        }
        return false;
    }

    bool insert_column(size_t col_ndx, DataType type, StringData name, bool nullable, bool listtype)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(m_table && m_table->is_attached()))) {
            if (REALM_LIKELY(REALM_COVER_ALWAYS(col_ndx <= m_table->get_column_count()))) {
                log("desc->insert_column(%1, %2, \"%3\", %4);", col_ndx, data_type_to_str(type), name,
                    nullable); // Throws
                LinkTargetInfo invalid_link;
                using tf = _impl::TableFriend;
                tf::insert_column_unless_exists(*m_table, col_ndx, type, name, invalid_link, nullable,
                                                listtype); // Throws
                return true;
            }
        }
        return false;
    }

    bool insert_link_column(size_t col_ndx, DataType type, StringData name, size_t link_target_table_ndx,
                            size_t backlink_col_ndx)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(m_table && m_table->is_attached()))) {
            if (REALM_LIKELY(REALM_COVER_ALWAYS(col_ndx <= m_table->get_column_count()))) {
                log("desc->insert_column_link(%1, %2, \"%3\", LinkTargetInfo(group->get_table(%4), %5));", col_ndx,
                    data_type_to_str(type), name, link_target_table_ndx, backlink_col_ndx); // Throws
                using gf = _impl::GroupFriend;
                using tf = _impl::TableFriend;
                Table* link_target_table = &gf::get_table(m_group, TableKey(link_target_table_ndx)); // Throws
                LinkTargetInfo link(link_target_table, backlink_col_ndx);
                tf::insert_column_unless_exists(*m_table, col_ndx, type, name, link); // Throws
                return true;
            }
        }
        return false;
    }

    bool erase_column(size_t col_ndx)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(m_table && m_table->is_attached()))) {
            if (REALM_LIKELY(REALM_COVER_ALWAYS(col_ndx < m_table->get_column_count()))) {
                log("desc->remove_column(%1);", col_ndx); // Throws
                typedef _impl::TableFriend tf;
                tf::erase_column(*m_table, col_ndx); // Throws
                return true;
            }
        }
        return false;
    }

    bool erase_link_column(size_t col_ndx, size_t, size_t)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(m_table && m_table->is_attached()))) {
            if (REALM_LIKELY(REALM_COVER_ALWAYS(col_ndx < m_table->get_column_count()))) {
                log("desc->remove_column(%1);", col_ndx); // Throws
                typedef _impl::TableFriend tf;
                tf::erase_column(*m_table, col_ndx); // Throws
                return true;
            }
        }
        return false;
    }

    bool rename_column(size_t col_ndx, StringData name)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(m_table && m_table->is_attached()))) {
            if (REALM_LIKELY(REALM_COVER_ALWAYS(col_ndx < m_table->get_column_count()))) {
                log("desc->rename_column(%1, \"%2\");", col_ndx, name); // Throws
                typedef _impl::TableFriend tf;
                tf::rename_column(*m_table, col_ndx, name); // Throws
                return true;
            }
        }
        return false;
    }

    bool move_column(size_t col_ndx_1, size_t col_ndx_2)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(m_table && m_table->is_attached()))) {
            size_t column_count = m_table->get_column_count();
            static_cast<void>(column_count);
            if (REALM_LIKELY(REALM_COVER_ALWAYS(col_ndx_1 < column_count && col_ndx_2 < column_count))) {
                log("desc->move_column(%1, %2);", col_ndx_1, col_ndx_2); // Throws
                typedef _impl::TableFriend tf;
                tf::move_column(*m_table, col_ndx_1, col_ndx_2); // Throws
                return true;
            }
        }
        return false;
    }

    bool insert_group_level_table(TableKey table_key, size_t prior_num_tables, StringData name)
    {

        if (REALM_UNLIKELY(REALM_COVER_NEVER(prior_num_tables != m_group.size())))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(name.size() >= ArrayStringShort::max_width)))
            return false;

        log("group->insert_table(%1, \"%2\", false);", table_key.value, name); // Throws
        typedef _impl::GroupFriend gf;
        bool was_inserted;
        // try to add table with correct key and name - but if it fails, just back out silently
        try {
            gf::get_or_add_table(m_group, table_key, name, &was_inserted); // Throws
            return true;
        }
        catch (...) {
        }
        return false;
    }

    bool erase_group_level_table(TableKey table_key, size_t num_tables) noexcept
    {
        static_cast<void>(num_tables);
        log("group->remove_table(%1);", table_key.value); // Throws
        m_group.remove_table(table_key);
        return true;
    }

    bool rename_group_level_table(TableKey table_key, StringData new_name) noexcept
    {
        if (REALM_UNLIKELY(REALM_COVER_NEVER(m_group.has_table(new_name))))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(new_name.size() >= ArrayStringShort::max_width)))
            return false;
        log("group->rename_table(%1, \"%2\");", table_key, new_name); // Throws
        m_group.rename_table(table_key, new_name);
        return true;
    }

    bool move_group_level_table(size_t from_table_ndx, size_t to_table_ndx) noexcept
    {
        REALM_ASSERT(false); // unsupported
        if (REALM_UNLIKELY(REALM_COVER_NEVER(from_table_ndx == to_table_ndx)))
            return false;
        log("group->move_table(%1, %2);", from_table_ndx, to_table_ndx); // Throws
        // m_group.move_table(from_table_ndx, to_table_ndx);
        return true;
    }

    bool optimize_table()
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(m_table && m_table->is_attached()))) {
            log("table->optimize();"); // Throws
            m_table->optimize();       // Throws
            return true;
        }
        return false;
    }

    bool select_link_list(size_t col_ndx, Key key, size_t)
    {
        if (REALM_UNLIKELY(REALM_COVER_NEVER(!m_table)))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(!m_table->is_attached())))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(col_ndx >= m_table->get_column_count())))
            return false;
        DataType type = m_table->get_column_type(col_ndx);
        static_cast<void>(type);
        if (REALM_UNLIKELY(REALM_COVER_NEVER(type != type_LinkList)))
            return false;
        log("link_list = table->get_link_list(%1, %2);", col_ndx, key.value); // Throws
        m_link_list = m_table->get_object(key).get_linklist_ptr(col_ndx);     // Throws
        return true;
    }

    bool link_list_set(size_t link_ndx, Key value, size_t prior_size)
    {
        if (REALM_UNLIKELY(REALM_COVER_NEVER(!m_link_list)))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(link_ndx >= m_link_list->size())))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(prior_size != m_link_list->size())))
            return false;
        static_cast<void>(prior_size);
        log("link_list->set(%1, %2);", link_ndx, value); // Throws
        m_link_list->set(link_ndx, value);
        return true;
    }

    bool link_list_insert(size_t link_ndx, Key key, size_t prior_size)
    {
        if (REALM_UNLIKELY(REALM_COVER_NEVER(!m_link_list)))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(link_ndx > m_link_list->size())))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(prior_size != m_link_list->size())))
            return false;
        static_cast<void>(prior_size);
        log("link_list->insert(%1, %2);", link_ndx, key.value); // Throws
        m_link_list->insert(link_ndx, key);                     // Throws
        return true;
    }

    bool link_list_move(size_t from_link_ndx, size_t to_link_ndx)
    {
        if (REALM_UNLIKELY(REALM_COVER_NEVER(!m_link_list)))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(from_link_ndx == to_link_ndx)))
            return false;
        size_t num_links = m_link_list->size();
        static_cast<void>(num_links);
        if (REALM_UNLIKELY(REALM_COVER_NEVER(from_link_ndx >= num_links)))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(to_link_ndx >= num_links)))
            return false;
        log("link_list->move(%1, %2);", from_link_ndx, to_link_ndx); // Throws
        m_link_list->move(from_link_ndx, to_link_ndx);               // Throws
        return true;
    }

    bool link_list_swap(size_t link_ndx_1, size_t link_ndx_2)
    {
        if (REALM_UNLIKELY(REALM_COVER_NEVER(!m_link_list)))
            return false;
        size_t num_links = m_link_list->size();
        static_cast<void>(num_links);
        if (REALM_UNLIKELY(REALM_COVER_NEVER(link_ndx_1 >= num_links)))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(link_ndx_2 >= num_links)))
            return false;
        log("link_list->swap(%1, %2);", link_ndx_1, link_ndx_2); // Throws
        m_link_list->swap(link_ndx_1, link_ndx_2);               // Throws
        return true;
    }

    bool link_list_erase(size_t link_ndx, size_t prior_size)
    {
        if (REALM_UNLIKELY(REALM_COVER_NEVER(!m_link_list)))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(link_ndx >= m_link_list->size())))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(prior_size != m_link_list->size())))
            return false;
        static_cast<void>(prior_size);
        log("link_list->remove(%1);", link_ndx); // Throws
        m_link_list->remove(link_ndx);
        return true;
    }

    bool link_list_clear(size_t)
    {
        if (REALM_UNLIKELY(REALM_COVER_NEVER(!m_link_list)))
            return false;
        log("link_list->clear();"); // Throws
        m_link_list->clear();
        return true;
    }

    bool nullify_link(size_t col_ndx, Key key, TableKey target_table_key)
    {
        return set_link(col_ndx, key, realm::null_key, target_table_key, _impl::instr_Set);
    }

    bool link_list_nullify(size_t link_ndx, size_t prior_size)
    {
        return link_list_erase(link_ndx, prior_size);
    }

private:
    Group& m_group;
    TableRef m_table;
    LinkListPtr m_link_list;
    util::Logger* m_logger = nullptr;

    bool check_set_cell(size_t col_ndx, Key key) noexcept
    {
        static_cast<void>(col_ndx);
        static_cast<void>(key);
        if (REALM_LIKELY(REALM_COVER_ALWAYS(m_table && m_table->is_attached()))) {
            if (REALM_LIKELY(REALM_COVER_ALWAYS(col_ndx < m_table->get_column_count()))) {
                if (REALM_LIKELY(REALM_COVER_ALWAYS(m_table->is_valid(key)))) {
                    return true;
                }
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
            case type_OldDateTime:
                return "type_DateTime";
            case type_Timestamp:
                return "type_Timestamp";
            case type_Link:
                return "type_Link";
            case type_LinkList:
                return "type_LinkList";
            default:
                break;
        }

        return "type_Unknown"; // LCOV_EXCL_LINE
    }

    const char* link_type_to_str(LinkType type)
    {
        switch (type) {
            case link_Strong:
                return "link_Strong";
            case link_Weak:
                return "link_Weak";
        }

        return "link_Unknown"; // LCOV_EXCL_LINE
    }

#ifdef REALM_DEBUG
    template <class... Params>
    void log(const char* message, Params... params)
    {
        if (m_logger)
            m_logger->trace(message, params...); // Throws
    }
#else
    template <class... Params>
    void log(const char*, Params...)
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

class InputStreamImpl : public _impl::NoCopyInputStream {
public:
    InputStreamImpl(const char* data, size_t size) noexcept
        : m_begin(data)
        , m_end(data + size)
    {
    }

    ~InputStreamImpl() noexcept
    {
    }

    bool next_block(const char*& begin, const char*& end) override
    {
        if (m_begin != 0) {
            begin = m_begin;
            end = m_end;
            m_begin = nullptr;
            return (end > begin);
        }
        return false;
    }
    const char* m_begin;
    const char* const m_end;
};

} // anonymous namespace

void TrivialReplication::apply_changeset(const char* data, size_t size, SharedGroup& target, util::Logger* logger)
{
    InputStreamImpl in(data, size);
    WriteTransaction wt(target);                              // Throws
    Replication::apply_changeset(in, wt.get_group(), logger); // Throws
    wt.commit();                                              // Throws
}

std::string TrivialReplication::get_database_path()
{
    return m_database_file;
}

void TrivialReplication::initialize(SharedGroup&)
{
    // Nothing needs to be done here
}

void TrivialReplication::do_initiate_transact(version_type, bool history_updated)
{
    char* data = m_transact_log_buffer.data();
    size_t size = m_transact_log_buffer.size();
    set_buffer(data, data + size);
    m_history_updated = history_updated;
}

Replication::version_type TrivialReplication::do_prepare_commit(version_type orig_version)
{
    char* data = m_transact_log_buffer.data();
    size_t size = write_position() - data;
    version_type new_version = prepare_changeset(data, size, orig_version); // Throws
    return new_version;
}

void TrivialReplication::do_finalize_commit() noexcept
{
    finalize_changeset();
}

void TrivialReplication::do_abort_transact() noexcept
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
    *new_begin = realm::safe_copy_n(data, size, *new_begin);
}
