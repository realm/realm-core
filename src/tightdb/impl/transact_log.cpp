#include <tightdb/impl/transact_log.hpp>
#include <tightdb/link_view.hpp>

namespace tightdb {
namespace _impl {

namespace {
const size_t init_subtab_path_buf_levels = 2; // 2 table levels (soft limit)
const size_t init_subtab_path_buf_size = 2*init_subtab_path_buf_levels - 1;
} // anonymous namespace

TransactLogEncoderBase::TransactLogEncoderBase():
    m_selected_table(null_ptr),
    m_selected_spec(null_ptr),
    m_selected_link_list(null_ptr)
{
    m_subtab_path_buf.set_size(init_subtab_path_buf_size); // Throws
}

void TransactLogEncoderBase::do_select_table(const Table* table)
{
    size_t* begin;
    size_t* end;
    for (;;) {
        begin = m_subtab_path_buf.data();
        end   = begin + m_subtab_path_buf.size();
        typedef _impl::TableFriend tf;
        end = tf::record_subtable_path(*table, begin, end);
        if (end)
            break;
        size_t new_size = m_subtab_path_buf.size();
        if (util::int_multiply_with_overflow_detect(new_size, 2))
            throw std::runtime_error("Too many subtable nesting levels");
        m_subtab_path_buf.set_size(new_size); // Throws
    }

    const int max_elems_per_chunk = 8; // FIXME: Use smaller number when compiling in debug mode
    char* buf = reserve(1 + (1+max_elems_per_chunk)*max_enc_bytes_per_int); // Throws
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
        buf = reserve(max_elems_per_chunk*max_enc_bytes_per_int); // Throws
    }
good:
    advance(buf);
    m_selected_spec = null_ptr;
    m_selected_link_list = null_ptr;
    m_selected_table = table;
}

void TransactLogEncoderBase::do_select_desc(const Descriptor& desc)
{
    typedef _impl::DescriptorFriend df;
    size_t* begin;
    size_t* end;
    select_table(&df::get_root_table(desc));
    for (;;) {
        begin = m_subtab_path_buf.data();
        end   = begin + m_subtab_path_buf.size();
        begin = df::record_subdesc_path(desc, begin, end);
        if (begin)
            break;
        size_t new_size = m_subtab_path_buf.size();
        if (util::int_multiply_with_overflow_detect(new_size, 2))
            throw std::runtime_error("Too many table type descriptor nesting levels");
        m_subtab_path_buf.set_size(new_size); // Throws
    }

    int max_elems_per_chunk = 8; // FIXME: Use smaller number when compiling in debug mode
    char* buf = reserve(1 + (1+max_elems_per_chunk)*max_enc_bytes_per_int); // Throws
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
        buf = reserve(max_elems_per_chunk*max_enc_bytes_per_int); // Throws
    }
good:
    advance(buf);
    m_selected_spec = &df::get_spec(desc);
}


void TransactLogEncoderBase::do_select_link_list(const LinkView& list)
{
    select_table(list.m_origin_table.get());
    size_t col_ndx = list.m_origin_column.m_column_ndx;
    size_t row_ndx = list.get_origin_row_index();
    simple_cmd(instr_SelectLinkList, util::tuple(col_ndx, row_ndx)); // Throws
    m_selected_link_list = &list;
}

TIGHTDB_NORETURN
void TransactLogParser::parser_error() const
{
    throw BadTransactLog();
}

}
}

