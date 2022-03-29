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
#include <realm/util/span.hpp>

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
    virtual size_t read(Span<char> buffer) = 0;

    virtual ~InputStream() noexcept = default;
};

class NoCopyInputStream {
public:
    /// Returns a span containing the next block.
    /// A zero-length span indicates end-of-input.
    virtual Span<const char> next_block() = 0;

    virtual ~NoCopyInputStream() noexcept = default;
};

class SimpleInputStream final : public InputStream {
public:
    SimpleInputStream(Span<const char> data) noexcept
        : m_data(data)
    {
    }
    size_t read(Span<char> buffer) override
    {
        size_t n = std::min(buffer.size(), m_data.size());
        realm::safe_copy_n(m_data.begin(), n, buffer.begin());
        m_data = m_data.sub_span(n);
        return n;
    }

private:
    Span<const char> m_data;
};

class NoCopyInputStreamAdaptor final : public NoCopyInputStream {
public:
    NoCopyInputStreamAdaptor(InputStream& in, Span<char> buffer) noexcept
        : m_in(in)
        , m_buffer(buffer)
    {
    }
    Span<const char> next_block() override
    {
        size_t n = m_in.read(m_buffer);
        return m_buffer.first(n);
    }

private:
    util::InputStream& m_in;
    Span<char> m_buffer;
};

class SimpleNoCopyInputStream final : public NoCopyInputStream {
public:
    SimpleNoCopyInputStream(Span<const char> data)
        : m_data(data)
    {
    }

    Span<const char> next_block() override
    {
        auto ret = m_data;
        m_data = m_data.last(0);
        return ret;
    }

private:
    Span<const char> m_data;
};

} // namespace realm::util

#endif // REALM_UTIL_INPUT_STREAM_HPP
