#include <new>
#include <limits>
#include <algorithm>
#include <stdexcept>

#include <tightdb/util/safe_int_ops.hpp>
#include <tightdb/util/string_buffer.hpp>

using namespace std;
using namespace tightdb;
using namespace tightdb::util;


char StringBuffer::m_zero = 0;


void StringBuffer::append(const char* data, size_t size)
{
    size_t new_size = m_size;
    if (int_add_with_overflow_detect(new_size, size))
        throw util::BufferSizeOverflow();
    reserve(new_size); // Throws
    copy(data, data+size, m_buffer.data()+m_size);
    m_size = new_size;
    m_buffer[new_size] = 0; // Add zero termination
}


void StringBuffer::reallocate(size_t min_capacity)
{
    size_t min_capacity_2 = min_capacity;
    // Make space for zero termination
    if (int_add_with_overflow_detect(min_capacity_2, 1))
        throw util::BufferSizeOverflow();
    size_t new_capacity = m_buffer.size();
    if (int_multiply_with_overflow_detect(new_capacity, 2))
        new_capacity = numeric_limits<size_t>::max();
    if (new_capacity < min_capacity_2)
        new_capacity = min_capacity_2;
    m_buffer.resize(new_capacity, 0, m_size, 0); // Throws
}
