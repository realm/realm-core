#include <realm/impl/transact_log.hpp>
#include <realm/link_view.hpp>

namespace realm {
namespace _impl {

namespace {
const size_t init_subtab_path_buf_levels = 2; // 2 table levels (soft limit)
const size_t init_subtab_path_buf_size = 2*init_subtab_path_buf_levels - 1;
} // anonymous namespace

TransactLogConvenientEncoder::TransactLogConvenientEncoder(TransactLogStream& stream):
    m_encoder(stream),
    m_selected_table(nullptr),
    m_selected_spec(nullptr),
    m_selected_link_list(nullptr)
{
    m_subtab_path_buf.set_size(init_subtab_path_buf_size); // Throws
}

bool TransactLogEncoder::select_table(size_t group_level_ndx, size_t levels, const size_t* path)
{
    const size_t* p = path;
    const size_t* end = path + (levels * 2);

    // The point with "chunking" here is to avoid reserving
    // very large chunks in the case of very long paths.
    const int max_elems_per_chunk = 8; // FIXME: Use smaller number when compiling in debug mode
    char* buf = reserve(1 + (1 + max_elems_per_chunk) * max_enc_bytes_per_int); // Throws

    *buf++ = char(instr_SelectTable);
    buf = encode_int(buf, levels);
    buf = encode_int(buf, group_level_ndx);

    if (p == end)
        goto good;

    for (;;) {
        for (int i = 0; i < max_elems_per_chunk; ++i) {
            buf = encode_int(buf, *p++);
            if (p == end)
                goto good;
        }
        buf = reserve(max_elems_per_chunk * max_enc_bytes_per_int); // Throws
    }
good:
    advance(buf);
    return true;
}

void TransactLogConvenientEncoder::do_select_table(const Table* table)
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
    std::reverse(begin, end);

    int levels = (end - begin) / 2;
    m_encoder.select_table(*begin, levels, begin + 1); // Throws
    m_selected_spec = nullptr;
    m_selected_link_list = nullptr;
    m_selected_table = table;
}

bool TransactLogEncoder::select_descriptor(size_t levels, const size_t* path)
{
    const size_t* end = path + levels;
    int max_elems_per_chunk = 8; // FIXME: Use smaller number when compiling in debug mode
    char* buf = reserve(1 + (1+max_elems_per_chunk)*max_enc_bytes_per_int); // Throws
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
        end   = begin + m_subtab_path_buf.size();
        begin = df::record_subdesc_path(desc, begin, end);
        if (begin)
            break;
        size_t new_size = m_subtab_path_buf.size();
        if (util::int_multiply_with_overflow_detect(new_size, 2))
            throw std::runtime_error("Too many table type descriptor nesting levels");
        m_subtab_path_buf.set_size(new_size); // Throws
    }

    m_encoder.select_descriptor(end - begin, begin); // Throws
    m_selected_spec = &df::get_spec(desc);
}

bool TransactLogEncoder::select_link_list(size_t col_ndx, size_t row_ndx)
{
    simple_cmd(instr_SelectLinkList, util::tuple(col_ndx, row_ndx)); // Throws
    return true;
}


void TransactLogConvenientEncoder::do_select_link_list(const LinkView& list)
{
    select_table(list.m_origin_table.get());
    size_t col_ndx = list.m_origin_column.m_column_ndx;
    size_t row_ndx = list.get_origin_row_index();
    m_encoder.select_link_list(col_ndx, row_ndx); // Throws
    m_selected_link_list = &list;
}

REALM_NORETURN
void TransactLogParser::parser_error() const
{
    throw BadTransactLog();
}

}
}

