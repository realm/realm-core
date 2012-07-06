#include <new>
#include <limits>
#include <algorithm>

#include <tightdb/overflow.hpp>
#include <tightdb/string_buffer.hpp>

using namespace std;

namespace tightdb {


char StringBuffer::m_zero = 0;


error_code StringBuffer::append(const char* data, size_t size)
{
    size_t new_size = m_size;
    if (add_with_overflow_detect(new_size, size)) {
        return ERROR_NO_RESOURCE;
    }
    error_code err = reserve(new_size);
    if (err) return err;
    copy(data, data+size, m_data+m_size);
    m_size = new_size;
    m_data[new_size] = 0; // Add zero termination
    return ERROR_NONE;
}


error_code StringBuffer::realloc(std::size_t capacity)
{
    size_t min_allocated = capacity;
    // Make space of zero termination
    if (add_with_overflow_detect(min_allocated, size_t(1))) {
        return ERROR_NO_RESOURCE;
    }
    size_t new_allocated = m_allocated;
    if (mul_with_overflow_detect(new_allocated, size_t(2)))
        new_allocated = numeric_limits<size_t>::max();
    if (new_allocated < min_allocated) new_allocated = min_allocated;
    char* new_data = new (nothrow) char[new_allocated];
    if (!new_data) return ERROR_OUT_OF_MEMORY;
    copy(m_data, m_data + m_size, new_data);
    delete[] m_data;
    m_data      = new_data;
    m_allocated = new_allocated;
    return ERROR_NONE;
}


} // namespace tightdb
