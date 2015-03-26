/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2015] Realm Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Realm Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Realm Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Realm Incorporated.
 *
 **************************************************************************/

#ifndef REALM_IMPL_INPUT_STREAM_HPP
#define REALM_IMPL_INPUT_STREAM_HPP

#include <algorithm>

namespace realm {
namespace _impl {

class InputStream {
public:
    /// \return the number of accessible bytes.
    /// A value of zero indicates end-of-input.
    /// For non-zero return value, \a begin and \a end are
    /// updated to reflect the start and limit of a
    /// contiguous memory chunk.
    virtual size_t next_block(const char*& begin, const char*& end) = 0;

    virtual ~InputStream() {}
};

class MultiLogInputStream: public InputStream {
public:
    MultiLogInputStream(const BinaryData* logs_begin, const BinaryData* logs_end):
        m_logs_begin(logs_begin), m_logs_end(logs_end)
    {
        if (m_logs_begin != m_logs_end)
            m_curr_buf_remaining_size = m_logs_begin->size();
    }

    ~MultiLogInputStream() override
    {
    }

    size_t read(char* buffer, size_t size)
    {
        if (m_logs_begin == m_logs_end)
            return 0;
        for (;;) {
            if (m_curr_buf_remaining_size > 0) {
                size_t offset = m_logs_begin->size() - m_curr_buf_remaining_size;
                const char* data = m_logs_begin->data() + offset;
                size_t size_2 = std::min(m_curr_buf_remaining_size, size);
                m_curr_buf_remaining_size -= size_2;
                // FIXME: Eliminate the need for copying by changing the API of
                // Replication::InputStream such that blocks can be handed over
                // without copying. This is a straight forward change, but the
                // result is going to be more complicated and less conventional.
                std::copy(data, data + size_2, buffer);
                return size_2;
            }

            ++m_logs_begin;
            if (m_logs_begin == m_logs_end)
                return 0;
            m_curr_buf_remaining_size = m_logs_begin->size();
        }
    }

    size_t next_block(const char*& begin, const char*& end) override
    {
        while (m_logs_begin < m_logs_end) {
            size_t result = m_logs_begin->size();
            const char* data = m_logs_begin->data();
            m_logs_begin++;
            if (result == 0)
                continue; // skip empty blocks
            begin = data;
            end = data + result;
            return result;
        }
        return 0;
    }

private:
    const BinaryData* m_logs_begin;
    const BinaryData* m_logs_end;
    size_t m_curr_buf_remaining_size;
};



}
}

#endif // REALM_IMPL_INPUT_STREAM_HPP
