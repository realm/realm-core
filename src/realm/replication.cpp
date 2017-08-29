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
#include <realm/descriptor.hpp>
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

    bool set_int(size_t col_ndx, size_t row_ndx, int_fast64_t value, _impl::Instruction variant,
                 size_t prior_num_rows)
    {
        static_cast<void>(prior_num_rows);
        static_cast<void>(variant);
        if (REALM_LIKELY(REALM_COVER_ALWAYS(check_set_cell(col_ndx, row_ndx)))) {
            if (REALM_UNLIKELY(REALM_COVER_NEVER(variant == _impl::instr_SetUnique))) {
                if (REALM_UNLIKELY(prior_num_rows != m_table->size())) {
                    return false;
                }
            }
            log("table->set_int(%1, %2, %3);", col_ndx, row_ndx, value); // Throws
            // Set and SetDefault are identical in this context.
            // For SetUnique, it is acceptable to call the regular version of
            // set_int(), because we presume that the side-effects of
            // set_int_unique() are already documented as other instructions
            // preceding this. Calling the set_int_unique() here would be a
            // waste of time, because all possible side-effects have already
            // been carried out.
            m_table->set_int(col_ndx, row_ndx, value); // Throws
            return true;
        }
        return false;
    }

    bool add_int(size_t col_ndx, size_t row_ndx, int_fast64_t value)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(check_set_cell(col_ndx, row_ndx)))) {
            log("table->add_int(%1, %2, %3);", col_ndx, row_ndx, value); // Throws
            m_table->add_int(col_ndx, row_ndx, value);                   // Throws
            return true;
        }
        return false;
    }

    bool set_bool(size_t col_ndx, size_t row_ndx, bool value, _impl::Instruction)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(check_set_cell(col_ndx, row_ndx)))) {
            log("table->set_bool(%1, %2, %3);", col_ndx, row_ndx, value); // Throws
            m_table->set_bool(col_ndx, row_ndx, value);                   // Throws
            return true;
        }
        return false;
    }

    bool set_float(size_t col_ndx, size_t row_ndx, float value, _impl::Instruction)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(check_set_cell(col_ndx, row_ndx)))) {
            log("table->set_float(%1, %2, %3);", col_ndx, row_ndx, value); // Throws
            m_table->set_float(col_ndx, row_ndx, value);                   // Throws
            return true;
        }
        return false;
    }

    bool set_double(size_t col_ndx, size_t row_ndx, double value, _impl::Instruction)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(check_set_cell(col_ndx, row_ndx)))) {
            log("table->set_double(%1, %2, %3);", col_ndx, row_ndx, value); // Throws
            m_table->set_double(col_ndx, row_ndx, value);                   // Throws
            return true;
        }
        return false;
    }

    bool set_string(size_t col_ndx, size_t row_ndx, StringData value, _impl::Instruction variant,
                    size_t prior_num_rows)
    {
        static_cast<void>(prior_num_rows);
        static_cast<void>(variant);
        if (REALM_LIKELY(REALM_COVER_ALWAYS(check_set_cell(col_ndx, row_ndx)))) {
            if (REALM_UNLIKELY(REALM_COVER_NEVER(variant == _impl::instr_SetUnique))) {
                if (REALM_UNLIKELY(prior_num_rows != m_table->size())) {
                    return false;
                }
            }
            log("table->set_string(%1, %2, \"%3\");", col_ndx, row_ndx, value); // Throws
            // Set and SetDefault are identical in this context.
            // For SetUnique, it is acceptable to call the regular version of
            // set_int(), because we presume that the side-effects of
            // set_int_unique() are already documented as other instructions
            // preceding this. Calling the set_int_unique() here would be a
            // waste of time, because all possible side-effects have already
            // been carried out.
            m_table->set_string(col_ndx, row_ndx, value); // Throws
            return true;
        }
        return false;
    }

    bool set_binary(size_t col_ndx, size_t row_ndx, BinaryData value, _impl::Instruction)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(check_set_cell(col_ndx, row_ndx)))) {
            log("table->set_binary(%1, %2, %3);", col_ndx, row_ndx, value); // Throws
            if (value.size() <= ArrayBlob::max_binary_size) {
                m_table->set_binary(col_ndx, row_ndx, value); // Throws
                return true;
            }
        }
        return false;
    }

    bool set_olddatetime(size_t col_ndx, size_t row_ndx, OldDateTime value, _impl::Instruction)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(check_set_cell(col_ndx, row_ndx)))) {
            log("table->set_olddatetime(%1, %2, %3);", col_ndx, row_ndx, value); // Throws
            m_table->set_olddatetime(col_ndx, row_ndx, value);                   // Throws
            return true;
        }
        return false;
    }

    bool set_timestamp(size_t col_ndx, size_t row_ndx, Timestamp value, _impl::Instruction)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(check_set_cell(col_ndx, row_ndx)))) {
            log("table->set_timestamp(%1, %2, %3);", col_ndx, row_ndx, value); // Throws
            m_table->set_timestamp(col_ndx, row_ndx, value);                   // Throws
            return true;
        }
        return false;
    }

    bool set_table(size_t col_ndx, size_t row_ndx, _impl::Instruction)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(check_set_cell(col_ndx, row_ndx)))) {
            log("table->clear_subtable(%1, %2);", col_ndx, row_ndx); // Throws
            m_table->clear_subtable(col_ndx, row_ndx);               // Throws
            return true;
        }
        return false;
    }

    bool set_mixed(size_t col_ndx, size_t row_ndx, const Mixed& value, _impl::Instruction)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(check_set_cell(col_ndx, row_ndx)))) {
            log("table->set_mixed(%1, %2, %3);", col_ndx, row_ndx, value); // Throws
            m_table->set_mixed(col_ndx, row_ndx, value);                   // Throws
            return true;
        }
        return false;
    }

    bool set_null(size_t col_ndx, size_t row_ndx, _impl::Instruction variant, size_t prior_num_rows)
    {
        static_cast<void>(prior_num_rows);
        static_cast<void>(variant);
        if (REALM_LIKELY(REALM_COVER_ALWAYS(check_set_cell(col_ndx, row_ndx)))) {
            if (REALM_UNLIKELY(REALM_COVER_NEVER(variant == _impl::instr_SetUnique))) {
                if (REALM_UNLIKELY(prior_num_rows != m_table->size())) {
                    return false;
                }
            }
            log("table->set_null(%1, %2);", col_ndx, row_ndx); // Throws
            // Set and SetDefault are identical in this context.
            // For SetUnique, it is acceptable to call the regular version of
            // set_null(), because we presume that the side-effects of
            // set_null_unique() are already documented as other instructions
            // preceding this. Calling the set_null_unique() here would be a
            // waste of time, because all possible side-effects have already
            // been carried out.
            m_table->set_null(col_ndx, row_ndx); // Throws
            return true;
        }
        return false;
    }

    bool set_link(size_t col_ndx, size_t row_ndx, size_t target_row_ndx, size_t, _impl::Instruction)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(check_set_cell(col_ndx, row_ndx)))) {
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
        if (REALM_UNLIKELY(REALM_COVER_NEVER(!m_table)))
            return false;
        log("table->insert_substring(%1, %2, %3, %4);", col_ndx, row_ndx, pos, value); // Throws
        try {
            m_table->insert_substring(col_ndx, row_ndx, pos, value); // Throws
            return true;
        }
        catch (LogicError&) { // LCOV_EXCL_START
            return false;
        } // LCOV_EXCL_STOP
    }

    bool erase_substring(size_t col_ndx, size_t row_ndx, size_t pos, size_t size)
    {
        if (REALM_UNLIKELY(REALM_COVER_NEVER(!m_table)))
            return false;
        log("table->remove_substring(%1, %2, %3, %4);", col_ndx, row_ndx, pos, size); // Throws
        try {
            m_table->remove_substring(col_ndx, row_ndx, pos, size); // Throws
            return true;
        }
        catch (LogicError&) { // LCOV_EXCL_START
            return false;
        } // LCOV_EXCL_STOP
    }

    bool insert_empty_rows(size_t row_ndx, size_t num_rows_to_insert, size_t prior_num_rows, bool unordered)
    {
        static_cast<void>(prior_num_rows);
        static_cast<void>(unordered);
        if (REALM_UNLIKELY(REALM_COVER_NEVER(!m_table)))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(row_ndx > prior_num_rows)))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(prior_num_rows != m_table->size())))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(unordered && row_ndx != prior_num_rows)))
            return false;
        log("table->insert_empty_row(%1, %2);", row_ndx, num_rows_to_insert); // Throws
        m_table->insert_empty_row(row_ndx, num_rows_to_insert);               // Throws
        return true;
    }

    bool add_row_with_key(size_t, size_t, size_t key_col_ndx, int64_t key)
    {
        if (REALM_UNLIKELY(REALM_COVER_NEVER(!m_table)))
            return false;
        log("table->add_row_with_key(%1, %2);", key_col_ndx, key); // Throws
        m_table->add_row_with_key(key_col_ndx, key);               // Throws
        return true;
    }

    bool erase_rows(size_t row_ndx, size_t num_rows_to_erase, size_t prior_num_rows, bool unordered)
    {
        static_cast<void>(num_rows_to_erase);
        static_cast<void>(prior_num_rows);
        if (REALM_UNLIKELY(REALM_COVER_NEVER(!m_table)))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(row_ndx >= prior_num_rows)))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(num_rows_to_erase != 1)))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(prior_num_rows != m_table->size())))
            return false;
        typedef _impl::TableFriend tf;
        if (unordered) {
            log("table->move_last_over(%1);", row_ndx); // Throws
            tf::do_move_last_over(*m_table, row_ndx);   // Throws
        }
        else {
            log("table->remove(%1);", row_ndx); // Throws
            tf::do_remove(*m_table, row_ndx);   // Throws
        }
        return true;
    }

    bool swap_rows(size_t row_ndx_1, size_t row_ndx_2)
    {
        if (REALM_UNLIKELY(REALM_COVER_NEVER(!m_table)))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(row_ndx_1 >= m_table->size() || row_ndx_2 >= m_table->size())))
            return false;
        log("table->swap_rows(%1, %2);", row_ndx_1, row_ndx_2); // Throws
        using tf = _impl::TableFriend;
        tf::do_swap_rows(*m_table, row_ndx_1, row_ndx_2); // Throws
        return true;
    }

    bool merge_rows(size_t row_ndx, size_t new_row_ndx)
    {
        if (REALM_UNLIKELY(REALM_COVER_NEVER(!m_table)))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(row_ndx >= m_table->size() || new_row_ndx >= m_table->size())))
            return false;
        log("table->merge_rows(%1, %2);", row_ndx, new_row_ndx); // Throws
        using tf = _impl::TableFriend;
        tf::do_merge_rows(*m_table, row_ndx, new_row_ndx); // Throws
        return true;
    }

    bool select_table(size_t group_level_ndx, int levels, const size_t* path)
    {
        if (REALM_UNLIKELY(REALM_COVER_NEVER(group_level_ndx >= m_group.size())))
            return false;
        log("table = group->get_table(%1);", group_level_ndx); // Throws
        m_desc.reset();
        m_link_list.reset();
        m_table = m_group.get_table(group_level_ndx); // Throws
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
        if (REALM_LIKELY(REALM_COVER_ALWAYS(m_desc))) {
            if (REALM_LIKELY(REALM_COVER_ALWAYS(col_ndx < m_desc->get_column_count()))) {
                log("desc->add_search_index(%1);", col_ndx); // Throws
                using tf = _impl::TableFriend;
                tf::add_search_index(*m_desc, col_ndx); // Throws
                return true;
            }
        }
        return false;
    }

    bool remove_search_index(size_t col_ndx)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(m_desc))) {
            if (REALM_LIKELY(REALM_COVER_ALWAYS(col_ndx < m_desc->get_column_count()))) {
                log("desc->remove_search_index(%1);", col_ndx); // Throws
                using tf = _impl::TableFriend;
                tf::remove_search_index(*m_desc, col_ndx); // Throws
                return true;
            }
        }
        return false;
    }

    bool set_link_type(size_t col_ndx, LinkType link_type)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(m_table && m_desc))) {
            if (REALM_LIKELY(REALM_COVER_ALWAYS(col_ndx < m_desc->get_column_count()))) {
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

    bool insert_column(size_t col_ndx, DataType type, StringData name, bool nullable)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(m_desc))) {
            if (REALM_LIKELY(REALM_COVER_ALWAYS(col_ndx <= m_desc->get_column_count()))) {
                log("desc->insert_column(%1, %2, \"%3\", %4);", col_ndx, data_type_to_str(type), name,
                    nullable); // Throws
                LinkTargetInfo invalid_link;
                using tf = _impl::TableFriend;
                tf::insert_column_unless_exists(*m_desc, col_ndx, type, name, invalid_link, nullable); // Throws
                return true;
            }
        }
        return false;
    }

    bool insert_link_column(size_t col_ndx, DataType type, StringData name, size_t link_target_table_ndx,
                            size_t backlink_col_ndx)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(m_desc))) {
            if (REALM_LIKELY(REALM_COVER_ALWAYS(col_ndx <= m_desc->get_column_count()))) {
                log("desc->insert_column_link(%1, %2, \"%3\", LinkTargetInfo(group->get_table(%4), %5));", col_ndx,
                    data_type_to_str(type), name, link_target_table_ndx, backlink_col_ndx); // Throws
                using gf = _impl::GroupFriend;
                using tf = _impl::TableFriend;
                Table* link_target_table = &gf::get_table(m_group, link_target_table_ndx); // Throws
                LinkTargetInfo link(link_target_table, backlink_col_ndx);
                tf::insert_column_unless_exists(*m_desc, col_ndx, type, name, link); // Throws
                return true;
            }
        }
        return false;
    }

    bool erase_column(size_t col_ndx)
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(m_desc))) {
            if (REALM_LIKELY(REALM_COVER_ALWAYS(col_ndx < m_desc->get_column_count()))) {
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
        if (REALM_LIKELY(REALM_COVER_ALWAYS(m_desc))) {
            if (REALM_LIKELY(REALM_COVER_ALWAYS(col_ndx < m_desc->get_column_count()))) {
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
        if (REALM_LIKELY(REALM_COVER_ALWAYS(m_desc))) {
            if (REALM_LIKELY(REALM_COVER_ALWAYS(col_ndx < m_desc->get_column_count()))) {
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
        if (REALM_LIKELY(REALM_COVER_ALWAYS(m_desc))) {
            size_t column_count = m_desc->get_column_count();
            static_cast<void>(column_count);
            if (REALM_LIKELY(REALM_COVER_ALWAYS(col_ndx_1 < column_count && col_ndx_2 < column_count))) {
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
        if (REALM_UNLIKELY(REALM_COVER_NEVER(!m_table)))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(!m_table->is_attached())))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(m_table->has_shared_type())))
            return false;
        log("desc = table->get_descriptor();"); // Throws
        m_link_list.reset();
        m_desc = m_table->get_descriptor(); // Throws
        for (int i = 0; i < levels; ++i) {
            size_t col_ndx = path[i];
            if (REALM_UNLIKELY(REALM_COVER_NEVER(col_ndx >= m_desc->get_column_count())))
                return false;
            if (REALM_UNLIKELY(REALM_COVER_NEVER(m_desc->get_column_type(col_ndx) != type_Table)))
                return false;
            log("desc = desc->get_subdescriptor(%1);", col_ndx); // Throws
            m_desc = m_desc->get_subdescriptor(col_ndx);
        }
        return true;
    }

    bool insert_group_level_table(size_t table_ndx, size_t prior_num_tables, StringData name)
    {
        static_cast<void>(prior_num_tables);
        if (REALM_UNLIKELY(REALM_COVER_NEVER(prior_num_tables != m_group.size())))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(table_ndx > m_group.size())))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(name.size() >= ArrayString::max_width)))
            return false;
        log("group->insert_table(%1, \"%2\", false);", table_ndx, name); // Throws
        typedef _impl::GroupFriend gf;
        bool was_inserted;
        gf::get_or_insert_table(m_group, table_ndx, name, &was_inserted); // Throws
        return true;
    }

    bool erase_group_level_table(size_t table_ndx, size_t num_tables) noexcept
    {
        static_cast<void>(num_tables);
        if (REALM_UNLIKELY(REALM_COVER_NEVER(num_tables != m_group.size())))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(table_ndx >= m_group.size())))
            return false;
        log("group->remove_table(%1);", table_ndx); // Throws
        m_group.remove_table(table_ndx);
        return true;
    }

    bool rename_group_level_table(size_t table_ndx, StringData new_name) noexcept
    {
        if (REALM_UNLIKELY(REALM_COVER_NEVER(table_ndx >= m_group.size())))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(m_group.has_table(new_name))))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(new_name.size() >= ArrayString::max_width)))
            return false;
        log("group->rename_table(%1, \"%2\");", table_ndx, new_name); // Throws
        m_group.rename_table(table_ndx, new_name);
        return true;
    }

    bool move_group_level_table(size_t from_table_ndx, size_t to_table_ndx) noexcept
    {
        if (REALM_UNLIKELY(REALM_COVER_NEVER(from_table_ndx == to_table_ndx)))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(from_table_ndx >= m_group.size())))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(to_table_ndx >= m_group.size())))
            return false;
        log("group->move_table(%1, %2);", from_table_ndx, to_table_ndx); // Throws
        m_group.move_table(from_table_ndx, to_table_ndx);
        return true;
    }

    bool optimize_table()
    {
        if (REALM_LIKELY(REALM_COVER_ALWAYS(m_table && m_table->is_attached()))) {
            if (REALM_LIKELY(REALM_COVER_ALWAYS(!m_table->has_shared_type()))) {
                log("table->optimize();"); // Throws
                m_table->optimize();       // Throws
                return true;
            }
        }
        return false;
    }

    bool select_link_list(size_t col_ndx, size_t row_ndx, size_t)
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
        log("link_list = table->get_link_list(%1, %2);", col_ndx, row_ndx); // Throws
        m_desc.reset();
        m_link_list = m_table->get_linklist(col_ndx, row_ndx); // Throws
        return true;
    }

    bool link_list_set(size_t link_ndx, size_t value, size_t prior_size)
    {
        if (REALM_UNLIKELY(REALM_COVER_NEVER(!m_link_list)))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(link_ndx >= m_link_list->size())))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(prior_size != m_link_list->size())))
            return false;
        static_cast<void>(prior_size);
        log("link_list->set(%1, %2);", link_ndx, value); // Throws
        typedef _impl::LinkListFriend llf;
        llf::do_set(*m_link_list, link_ndx, value); // Throws
        return true;
    }

    bool link_list_insert(size_t link_ndx, size_t value, size_t prior_size)
    {
        if (REALM_UNLIKELY(REALM_COVER_NEVER(!m_link_list)))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(link_ndx > m_link_list->size())))
            return false;
        if (REALM_UNLIKELY(REALM_COVER_NEVER(prior_size != m_link_list->size())))
            return false;
        static_cast<void>(prior_size);
        log("link_list->insert(%1, %2);", link_ndx, value); // Throws
        m_link_list->insert(link_ndx, value);               // Throws
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
        typedef _impl::LinkListFriend llf;
        llf::do_remove(*m_link_list, link_ndx); // Throws
        return true;
    }

    bool link_list_clear(size_t)
    {
        if (REALM_UNLIKELY(REALM_COVER_NEVER(!m_link_list)))
            return false;
        log("link_list->clear();"); // Throws
        typedef _impl::LinkListFriend llf;
        llf::do_clear(*m_link_list); // Throws
        return true;
    }

    bool nullify_link(size_t col_ndx, size_t row_ndx, size_t target_group_level_ndx)
    {
        return set_link(col_ndx, row_ndx, realm::npos, target_group_level_ndx, _impl::instr_Set);
    }

    bool link_list_nullify(size_t link_ndx, size_t prior_size)
    {
        return link_list_erase(link_ndx, prior_size);
    }

private:
    Group& m_group;
    TableRef m_table;
    DescriptorRef m_desc;
    LinkViewRef m_link_list;
    util::Logger* m_logger = nullptr;

    bool check_set_cell(size_t col_ndx, size_t row_ndx) noexcept
    {
        static_cast<void>(col_ndx);
        static_cast<void>(row_ndx);
        if (REALM_LIKELY(REALM_COVER_ALWAYS(m_table && m_table->is_attached()))) {
            if (REALM_LIKELY(REALM_COVER_ALWAYS(col_ndx < m_table->get_column_count()))) {
                if (REALM_LIKELY(REALM_COVER_ALWAYS(row_ndx < m_table->size())))
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
            case type_OldDateTime:
                return "type_DateTime";
            case type_Timestamp:
                return "type_Timestamp";
            case type_Table:
                return "type_Table";
            case type_Mixed:
                return "type_Mixed";
            case type_Link:
                return "type_Link";
            case type_LinkList:
                return "type_LinkList";
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
