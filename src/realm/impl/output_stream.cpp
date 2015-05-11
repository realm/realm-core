#include <limits>
#include <stdexcept>

#include <realm/util/safe_int_ops.hpp>
#include <realm/impl/output_stream.hpp>

using namespace realm;
using namespace realm::util;
using namespace realm::_impl;


void OutputStream::write(const char* data, size_t size)
{
    size_t size_0 = size;

    const char* data_1 = data;
    size_t size_1 = size_0;

    // Handle the case where 'size_t' has a larger range than 'std::streamsize'
    std::streamsize max_streamsize = std::numeric_limits<std::streamsize>::max();
    size_t max_put = std::numeric_limits<size_t>::max();
    if (int_less_than(max_streamsize, max_put))
        max_put = size_t(max_streamsize);
    while (max_put < size_1) {
        m_out.write(data_1, max_put);
        data_1 += max_put;
        size_1 -= max_put;
    }

    m_out.write(data_1, size_1);

    if (int_add_with_overflow_detect(m_pos, size_0))
        throw std::runtime_error("File size overflow");
}


size_t OutputStream::write_array(const char* data, size_t size, uint_fast32_t checksum)
{
    const char* data_1 = data;
    size_t size_1 = size;
    size_t pos = m_pos;

#ifdef REALM_DEBUG
    const char* cksum_bytes = reinterpret_cast<const char*>(&checksum);
    m_out.write(cksum_bytes, 4);
    data_1 += 4;
    size_1 -= 4;
    if (int_add_with_overflow_detect(m_pos, 4))
        throw std::runtime_error("File size overflow");
#else
    static_cast<void>(checksum);
#endif

    write(data_1, size_1);
    return pos;
}
