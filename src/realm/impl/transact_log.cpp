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

#include <realm/impl/transact_log.hpp>
#include <realm/link_view.hpp>

namespace realm {
namespace _impl {

namespace {
const size_t init_subtab_path_buf_levels = 2; // 2 table levels (soft limit)
const size_t init_subtab_path_buf_size = 2 * init_subtab_path_buf_levels - 1;
} // anonymous namespace

TransactLogConvenientEncoder::TransactLogConvenientEncoder(TransactLogStream& stream)
    : m_encoder(stream)
    , m_selected_table(nullptr)
    , m_selected_spec(nullptr)
{
    m_subtab_path_buf.set_size(init_subtab_path_buf_size); // Throws
}

bool TransactLogEncoder::select_table(size_t group_level_ndx, size_t levels, const size_t* path)
{
    const size_t* path_end = path + (levels * 2);
    append_simple_instr(instr_SelectTable, levels, group_level_ndx, std::make_tuple(path, path_end)); // Throws
    return true;
}

void TransactLogConvenientEncoder::record_subtable_path(const Table& table, size_t*& begin, size_t*& end)
{
    for (;;) {
        begin = m_subtab_path_buf.data();
        end = begin + m_subtab_path_buf.size();
        typedef _impl::TableFriend tf;
        end = tf::record_subtable_path(table, begin, end);
        if (end)
            break;
        size_t new_size = m_subtab_path_buf.size();
        if (util::int_multiply_with_overflow_detect(new_size, 2))
            throw std::runtime_error("Too many subtable nesting levels");
        m_subtab_path_buf.set_size(new_size); // Throws
    }
    std::reverse(begin, end);
}

void TransactLogConvenientEncoder::do_select_table(const Table* table)
{
    size_t* begin;
    size_t* end;
    record_subtable_path(*table, begin, end);

    size_t levels = (end - begin) / 2;
    m_encoder.select_table(*begin, levels, begin + 1); // Throws
    m_selected_table = table;
}

bool TransactLogEncoder::select_list(size_t col_ndx, Key key)
{
    append_simple_instr(instr_SelectList, col_ndx, key.value); // Throws
    return true;
}


void TransactLogConvenientEncoder::do_select_list(const ConstListBase& list)
{
    select_table(list.get_table());
    size_t col_ndx = list.get_col_ndx();
    Key key = list.ConstListBase::get_key();

    m_encoder.select_list(col_ndx, key); // Throws
    m_selected_list = LinkListId(list.get_table()->get_key(), key, col_ndx);
}

void TransactLogConvenientEncoder::list_clear(const ConstListBase& list)
{
    select_list(list);                 // Throws
    m_encoder.list_clear(list.size()); // Throws
}

REALM_NORETURN
void TransactLogParser::parser_error() const
{
    throw BadTransactLog();
}

} // namespace _impl
} // namespace realm
