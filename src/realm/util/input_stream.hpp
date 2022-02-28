/*************************************************************************
 *
 * Copyright 2022 Realm Inc.
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

#ifndef REALM_UTIL_INPUT_STREAM_HPP
#define REALM_UTIL_INPUT_STREAM_HPP

#include <realm/utilities.hpp>

namespace realm::util {
class InputStream {
public:
    /// Read bytes from this input stream and place them in the specified
    /// buffer. The returned value is the actual number of bytes that were read,
    /// and this is some number `n` such that `n <= min(size, m)` where `m` is
    /// the number of bytes that could have been read from this stream before
    /// reaching its end. Also, `n` cannot be zero unless `m` or `size` is
    /// zero. The intention is that `size` should be non-zero, a the return
    /// value used as the end-of-input indicator.
    ///
    /// Implementations are only allowed to block (put the calling thread to
    /// sleep) up until the point in time where the first byte can be made
    /// availble.
    virtual size_t read(char* buffer, size_t size) = 0;

    virtual ~InputStream() noexcept = default;
};

class NoCopyInputStream {
public:
    /// \return if any bytes was read.
    /// A value of false indicates end-of-input.
    /// If return value is true, \a begin and \a end are
    /// updated to reflect the start and limit of a
    /// contiguous memory chunk.
    virtual bool next_block(const char*& begin, const char*& end) = 0;

    virtual ~NoCopyInputStream() noexcept = default;
};


class SimpleInputStream : public InputStream {
public:
    SimpleInputStream(const char* data, size_t size) noexcept
        : m_ptr(data)
        , m_end(data + size)
    {
    }
    size_t read(char* buffer, size_t size) override
    {
        size_t n = std::min(size, size_t(m_end - m_ptr));
        const char* begin = m_ptr;
        m_ptr += n;
        realm::safe_copy_n(begin, n, buffer);
        return n;
    }

private:
    const char* m_ptr;
    const char* const m_end;
};

class NoCopyInputStreamAdaptor : public NoCopyInputStream {
public:
    NoCopyInputStreamAdaptor(InputStream& in, char* buffer, size_t buffer_size) noexcept
        : m_in(in)
        , m_buffer(buffer)
        , m_buffer_size(buffer_size)
    {
    }
    bool next_block(const char*& begin, const char*& end) override
    {
        size_t n = m_in.read(m_buffer, m_buffer_size);
        begin = m_buffer;
        end = m_buffer + n;
        return n;
    }

private:
    util::InputStream& m_in;
    char* m_buffer;
    size_t m_buffer_size;
};

class SimpleNoCopyInputStream : public NoCopyInputStream {
public:
    SimpleNoCopyInputStream(const char* data, size_t size)
        : m_data(data)
        , m_size(size)
    {
    }

    bool next_block(const char*& begin, const char*& end) override
    {
        if (m_size == 0)
            return 0;
        size_t size = m_size;
        begin = m_data;
        end = m_data + size;
        m_size = 0;
        return size;
    }

private:
    const char* m_data;
    size_t m_size;
};

} // namespace realm::util

#endif // REALM_UTIL_INPUT_STREAM_HPP
