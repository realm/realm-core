#include <new>
#include <limits>
#include <algorithm>
#include <stdexcept>

#include <tightdb/safe_int_ops.hpp>
#include <tightdb/string_buffer.hpp>

using namespace std;

namespace tightdb {


char StringBuffer::m_zero = 0;


void StringBuffer::append(const char* data, size_t size)
{
    size_t new_size = m_size;
    if (int_add_with_overflow_detect(new_size, size))
        throw runtime_error("Overflow in StringBuffer size");
    reserve(new_size);
    copy(data, data+size, m_data+m_size);
    m_size = new_size;
    m_data[new_size] = 0; // Add zero termination
}


void StringBuffer::reallocate(size_t capacity)
{
    size_t min_allocated = capacity;
    // Make space for zero termination
    if (int_add_with_overflow_detect(min_allocated, 1))
        throw runtime_error("Overflow in StringBuffer size");
    size_t new_allocated = m_allocated;
    if (int_multiply_with_overflow_detect(new_allocated, 2))
        new_allocated = numeric_limits<size_t>::max();
    if (new_allocated < min_allocated) new_allocated = min_allocated;
    char* new_data = new char[new_allocated];
    copy(m_data, m_data + m_size, new_data);
    delete[] m_data;
    m_data      = new_data;
    m_allocated = new_allocated;
}


} // namespace tightdb
