#include <cstdlib>
#include <cstring>
#include <cstdio> // debug
#include <iostream>

#include <tightdb/utilities.hpp>
#include <tightdb/column.hpp>
#include <tightdb/array_float.hpp>

using namespace std;

namespace tightdb {

float ArrayFloat::Get(size_t ndx) const
{
    float* dataPtr = (float *)m_data + ndx;
    return *dataPtr;
}

void ArrayFloat::Set(size_t ndx, float value)
{
    TIGHTDB_ASSERT(ndx < m_len);

    // Check if we need to copy before modifying
    if (!CopyOnWrite()) 
        return;

    // Set the value
    float* data = (float *)m_data + ndx;
    *data = value;
}

void ArrayFloat::Insert(size_t ndx, float value)
{
    TIGHTDB_ASSERT(ndx <= m_len);

    // Check if we need to copy before modifying
    (void)CopyOnWrite();

    // Make room for the new value
    if (!Alloc(m_len+1, m_width)) 
        return;

    // Move values below insertion
    if (ndx != m_len) {
        unsigned char* src = m_data + (ndx * m_width);
        unsigned char* dst = src + m_width;
        const size_t count = (m_len - ndx) * m_width;
        memmove(dst, src, count); // FIXME: Use std::copy() or std::copy_backward() instead.
    }

    // Set the value
    float* data = (float *)m_data + ndx;
    *data = value;

     ++m_len;
}

void ArrayFloat::Delete(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < m_len);

    // Check if we need to copy before modifying
    CopyOnWrite();

    --m_len;

    // move data under deletion up
    if (ndx < m_len) {
        unsigned char* src = m_data + ((ndx+1) * m_width);
        unsigned char* dst = m_data + (ndx * m_width);
        const size_t len = (m_len - ndx) * m_width;
        memmove(dst, src, len); // FIXME: Use std::copy() or std::copy_backward() instead.
    }

    // Update length in header
    set_header_len(m_len);
}


size_t ArrayFloat::CalcByteLen(size_t count, size_t width) const
{
    // FIXME: This arithemtic could overflow. Consider using <tightdb/overflow.hpp>
    return 8 + (count * sizeof(float));
}

size_t ArrayFloat::CalcItemCount(size_t bytes, size_t width) const
{
    // ??? what about width = 0? return -1?

    const size_t bytes_without_header = bytes - 8;
    return bytes_without_header / sizeof(float);
}

}
