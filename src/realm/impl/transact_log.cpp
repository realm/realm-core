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
    , m_selected_link_list(nullptr)
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
            throw util::overflow_error("Too many subtable nesting levels");
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

bool TransactLogEncoder::select_descriptor(size_t levels, const size_t* path)
{
    const size_t* end = path + levels;
    int max_elems_per_chunk = 8; // FIXME: Use smaller number when compiling in debug mode
    char* buf = reserve(1 + (1 + max_elems_per_chunk) * max_enc_bytes_per_int); // Throws
    *buf++ = char(instr_SelectDescriptor);
    size_t level = end - path;
    buf = encode_int(buf, level);
    if (path == end)
        goto good;
    for (;;) {
        for (int i = 0; i < max_elems_per_chunk; ++i) {
            buf = encode_int(buf, *path);
            if (++path == end)
                goto good;
        }
        buf = reserve(max_elems_per_chunk * max_enc_bytes_per_int); // Throws
    }
good:
    advance(buf);
    return true;
}

void TransactLogConvenientEncoder::do_select_desc(const Descriptor& desc)
{
    typedef _impl::DescriptorFriend df;
    size_t* begin;
    size_t* end;
    select_table(&df::get_root_table(desc));
    for (;;) {
        begin = m_subtab_path_buf.data();
        end = begin + m_subtab_path_buf.size();
        begin = df::record_subdesc_path(desc, begin, end);
        if (begin)
            break;
        size_t new_size = m_subtab_path_buf.size();
        if (util::int_multiply_with_overflow_detect(new_size, 2))
            throw util::overflow_error("Too many table type descriptor nesting levels");
        m_subtab_path_buf.set_size(new_size); // Throws
    }

    m_encoder.select_descriptor(end - begin, begin); // Throws
    m_selected_spec = &df::get_spec(desc);
}

bool TransactLogEncoder::select_link_list(size_t col_ndx, size_t row_ndx, size_t link_target_group_level_ndx)
{
    append_simple_instr(instr_SelectLinkList, col_ndx, row_ndx, link_target_group_level_ndx); // Throws
    return true;
}


void TransactLogConvenientEncoder::do_select_link_list(const LinkView& list)
{
    select_table(list.m_origin_table.get());
    size_t col_ndx = list.m_origin_column->get_column_index();
    size_t row_ndx = list.get_origin_row_index();

    size_t* link_target_path_begin;
    size_t* link_target_path_end;
    record_subtable_path(list.m_origin_column->get_target_table(), link_target_path_begin, link_target_path_end);
    size_t link_target_levels = (link_target_path_end - link_target_path_begin) / 2;
    static_cast<void>(link_target_levels);
    REALM_ASSERT_3(link_target_levels, ==, 0);

    m_encoder.select_link_list(col_ndx, row_ndx, link_target_path_begin[0]); // Throws
    m_selected_link_list = &list;
}

void TransactLogConvenientEncoder::link_list_clear(const LinkView& list)
{
    select_link_list(list);                 // Throws
    m_encoder.link_list_clear(list.size()); // Throws
}

REALM_NORETURN
void TransactLogParser::parser_error() const
{
    throw BadTransactLog();
}

} // namespace _impl
} // namespace realm
