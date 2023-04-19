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

#include <realm/util/compression.hpp>
#include <realm/util/safe_int_ops.hpp>
#include <realm/util/scope_exit.hpp>

#include <cstring>
#include <limits>
#include <map>
#include <zlib.h>
#include <zconf.h> // for zlib

#if REALM_USE_LIBCOMPRESSION
#include <compression.h>
#include <os/availability.h>
#endif

using namespace realm;
using namespace util;

namespace {

enum class Algorithm {
    None = 0,
    Deflate = 1,
    Lzfse = 2,
};

using stream_avail_size_t = std::conditional_t<sizeof(uInt) < sizeof(size_t), uInt, size_t>;
constexpr stream_avail_size_t g_max_stream_avail = std::numeric_limits<stream_avail_size_t>::max();

stream_avail_size_t bounded_avail(size_t s)
{
    return s > g_max_stream_avail ? g_max_stream_avail : stream_avail_size_t(s);
}

Bytef* to_bytef(const char* str)
{
    return reinterpret_cast<Bytef*>(const_cast<char*>(str));
}

class ErrorCategoryImpl : public std::error_category {
public:
    const char* name() const noexcept override final
    {
        return "realm::util::compression::error";
    }
    std::string message(int err) const override final
    {
        using error = realm::util::compression::error;
        error e = error(err);
        switch (e) {
            case error::out_of_memory:
                return "Out of memory";
            case error::compress_buffer_too_small:
                return "Compression buffer too small";
            case error::compress_error:
                return "Compression error";
            case error::compress_input_too_long:
                return "Compression input too long";
            case error::corrupt_input:
                return "Corrupt input data";
            case error::incorrect_decompressed_size:
                return "Decompressed data size not equal to expected size";
            case error::decompress_error:
                return "Decompression error";
            case error::decompress_unsupported:
                return "Decompression failed due to unsupported input compression";
        }
        REALM_UNREACHABLE();
    }
};

ErrorCategoryImpl g_error_category;

void* custom_alloc(void* opaque, unsigned int cnt, unsigned int size)
{
    using Alloc = realm::util::compression::Alloc;
    Alloc& alloc = *static_cast<Alloc*>(opaque);
    std::size_t accum_size = cnt * std::size_t(size);
    return alloc.alloc(accum_size);
}

void custom_free(void* opaque, void* addr)
{
    using Alloc = realm::util::compression::Alloc;
    Alloc& alloc = *static_cast<Alloc*>(opaque);
    return alloc.free(addr);
}

void init_arena(compression::CompressMemoryArena& compress_memory_arena)
{
    if (compress_memory_arena.size() == 0) {
        // Zlib documentation says that with default settings deflate requires
        // at most 268 KB. We round up slightly.
        compress_memory_arena.resize(270 * 1024); // Throws
    }
    else {
        compress_memory_arena.reset();
    }
}

void grow_arena(compression::CompressMemoryArena& compress_memory_arena)
{
    std::size_t n = compress_memory_arena.size();
    REALM_ASSERT(n != 0);
    REALM_ASSERT(n != std::numeric_limits<std::size_t>::max());
    if (util::int_multiply_with_overflow_detect(n, 2))
        n = std::numeric_limits<std::size_t>::max();
    compress_memory_arena.resize(n); // Throws
}

uint8_t read_byte(NoCopyInputStream& is, Span<const char>& buf)
{
    if (!buf.size())
        buf = is.next_block();
    if (buf.size()) {
        char c = buf.front();
        buf = buf.sub_span(1);
        return c;
    }
    return 0;
}

struct Header {
    Algorithm algorithm;
    size_t size;
};

Header read_header(NoCopyInputStream& is, Span<const char>& buf)
{
    Header ret = {};
    auto first_byte = read_byte(is, buf);
    ret.algorithm = Algorithm(first_byte >> 4);
    size_t size_width = first_byte & 0b1111;
    if (size_width > sizeof(size_t))
        ret.size = -1;
    else {
        for (size_t i = 0; i < size_width; ++i) {
            ret.size += size_t(read_byte(is, buf)) << (i * 8);
        }
    }
    return ret;
}

uint8_t header_width(size_t size)
{
    uint8_t width = 0;
    while (size) {
        ++width;
        size >>= 8;
    }
    return width + 1;
}

size_t write_header(Header h, Span<char> target)
{
    uint8_t width = 0;
    target[0] = uint8_t(h.algorithm) << 4;
    for (size_t sz = h.size; sz; sz >>= 8, ++width) {
        target[width + 1] = uint8_t(sz & 0xFF);
        ++target[0];
    }
    return width + 1;
}

// Feed in a zlib header to inflate() for the places we don't store it
void inflate_zlib_header(z_stream& strm)
{
    Bytef out;
    strm.avail_in = 2;
    strm.next_in = to_bytef("\x78\x5e");
    strm.avail_out = sizeof(out);
    strm.next_out = &out;

    int rc = inflate(&strm, Z_SYNC_FLUSH);
    REALM_ASSERT(rc == Z_OK);
    REALM_ASSERT(strm.avail_in == 0);
}

struct DecompressInputStreamNone final : public NoCopyInputStream {
    DecompressInputStreamNone(NoCopyInputStream& s, Span<const char> b)
        : source(s)
        , current_block(b)
    {
        if (current_block.empty())
            current_block = source.next_block();
    }
    NoCopyInputStream& source;
    Span<const char> current_block;

    Span<const char> next_block() override
    {
        auto ret = current_block;
        if (ret.size())
            current_block = source.next_block();
        return ret;
    }
};

class DecompressInputStreamZlib final : public NoCopyInputStream {
public:
    DecompressInputStreamZlib(NoCopyInputStream& s, Span<const char> b, size_t total_size)
        : m_source(s)
    {
        // Arbitrary upper limit to reduce peak memory usage
        constexpr const size_t max_out_buffer_size = 1024 * 1024;
        m_buffer.reserve(std::min(total_size, max_out_buffer_size));

        int rc = inflateInit(&m_strm);
        if (rc != Z_OK)
            throw std::system_error(make_error_code(compression::error::decompress_error), m_strm.msg);
        inflate_zlib_header(m_strm);

        m_strm.avail_in = bounded_avail(b.size());
        m_strm.next_in = to_bytef(b.data());
        m_current_block = b.sub_span(m_strm.avail_in);
    }

    ~DecompressInputStreamZlib()
    {
        inflateEnd(&m_strm);
    }

    Span<const char> next_block() override
    {
        m_buffer.resize(m_buffer.capacity());
        m_strm.avail_out = bounded_avail(m_buffer.size());
        m_strm.next_out = to_bytef(m_buffer.data());

        while (true) {
            // We may have some leftover input buffer from a previous call if the
            // inflated result didn't fit in the output buffer. If not, we need to
            // fetch the next block.
            if (m_strm.avail_in == 0) {
                m_current_block = m_source.next_block();
                if (m_current_block.size()) {
                    m_strm.next_in = to_bytef(m_current_block.data());
                    m_strm.avail_in = bounded_avail(m_current_block.size());
                }
            }

            m_strm.total_out = 0;
            auto rc = inflate(&m_strm, m_strm.avail_in ? Z_SYNC_FLUSH : Z_FINISH);
            REALM_ASSERT(rc == Z_OK || rc == Z_STREAM_END || rc == Z_BUF_ERROR);

            if (m_strm.total_out) {
                // We got some output, so return that. We might also have reached
                // the end of the stream, which'll be reported on the next call
                // if so.
                REALM_ASSERT(m_strm.total_out <= m_buffer.capacity());
                m_buffer.resize(m_strm.total_out);
                return m_buffer;
            }

            if (rc != Z_OK) {
                // We reached the end of the stream without producing more data, so
                // we're done.
                return {nullptr, nullptr};
            }

            // Otherwise we produced no output but also didn't reach the end of the
            // stream, so we need to feed more data in.
        }
    }

private:
    NoCopyInputStream& m_source;
    Span<const char> m_current_block;
    z_stream m_strm = {};
    AppendBuffer<char> m_buffer;
};

#if REALM_USE_LIBCOMPRESSION

compression_algorithm algorithm_to_compression_algorithm(Algorithm a)
{
    switch (Algorithm(a)) {
        case Algorithm::Deflate:
            return COMPRESSION_ZLIB;
        case Algorithm::Lzfse:
            return COMPRESSION_LZFSE;
        default:
            return (compression_algorithm)0;
    }
}

API_AVAILABLE_BEGIN(macos(10.11))

class DecompressInputStreamLibCompression final : public NoCopyInputStream {
public:
    DecompressInputStreamLibCompression(NoCopyInputStream& s, Span<const char> b, Header h)
        : m_source(s)
    {
        // Arbitrary upper limit to reduce peak memory usage
        constexpr const size_t max_out_buffer_size = 1024 * 1024;
        m_buffer.reserve(std::min(h.size, max_out_buffer_size));
        auto rc = compression_stream_init(&m_strm, COMPRESSION_STREAM_DECODE,
                                          algorithm_to_compression_algorithm(h.algorithm));
        if (rc != COMPRESSION_STATUS_OK)
            throw std::system_error(compression::error::decompress_error);
        m_strm.src_size = b.size();
        m_strm.src_ptr = to_bytef(b.data());
    }

    ~DecompressInputStreamLibCompression()
    {
        compression_stream_destroy(&m_strm);
    }

    Span<const char> next_block() override
    {
        m_buffer.resize(m_buffer.capacity());
        m_strm.dst_size = m_buffer.size();
        m_strm.dst_ptr = to_bytef(m_buffer.data());

        while (true) {
            // We may have some leftover input buffer from a previous call if the
            // inflated result didn't fit in the output buffer. If not, we need to
            // fetch the next block.
            bool end = false;
            if (m_strm.src_size == 0) {
                if (auto block = m_source.next_block(); block.size()) {
                    m_strm.src_ptr = to_bytef(block.data());
                    m_strm.src_size = block.size();
                }
                else {
                    end = true;
                }
            }

            auto rc = compression_stream_process(&m_strm, end ? COMPRESSION_STREAM_FINALIZE : 0);
            if (rc == COMPRESSION_STATUS_ERROR)
                throw std::system_error(compression::error::corrupt_input);
            auto bytes_written = m_buffer.size() - m_strm.dst_size;
            if (bytes_written) {
                // We got some output, so return that. We might also have reached
                // the end of the stream, which'll be reported on the next call
                // if so.
                m_buffer.resize(bytes_written);
                return m_buffer;
            }
            if (rc == COMPRESSION_STATUS_END) {
                // We reached the end of the stream and are done
                return {nullptr, nullptr};
            }

            if (end) {
                // We ran out of input data but didn't get COMPRESSION_STATUS_END,
                // so the input is truncated
                throw std::system_error(compression::error::corrupt_input);
            }

            // Otherwise we produced no output but also didn't reach the end of the
            // stream, so we need to feed more data in.
        }
    }

private:
    NoCopyInputStream& m_source;
    compression_stream m_strm = {};
    AppendBuffer<char> m_buffer;
};

API_AVAILABLE_END
#endif

std::error_code decompress_none(NoCopyInputStream& compressed, Span<const char> compressed_buf,
                                Span<char> decompressed_buf)
{
    do {
        auto count = std::min(decompressed_buf.size(), compressed_buf.size());
        std::memcpy(decompressed_buf.data(), compressed_buf.data(), count);
        decompressed_buf = decompressed_buf.sub_span(count);
        compressed_buf = compressed.next_block();
    } while (compressed_buf.size() && decompressed_buf.size());

    if (compressed_buf.size() || decompressed_buf.size()) {
        return compression::error::incorrect_decompressed_size;
    }
    return std::error_code{};
}

std::error_code decompress_zlib(NoCopyInputStream& compressed, Span<const char> compressed_buf,
                                Span<char> decompressed_buf, bool has_header)
{
    using namespace compression;

    z_stream strm = {};
    int rc = inflateInit(&strm);
    if (rc != Z_OK)
        return error::decompress_error;
    util::ScopeExit cleanup([&]() noexcept {
        // inflateEnd() only fails if we modified the fields of strm in an invalid way
        int rc = inflateEnd(&strm);
        REALM_ASSERT(rc == Z_OK);
        static_cast<void>(rc);
    });

    if (!has_header)
        inflate_zlib_header(strm);

    do {
        size_t in_offset = 0;

        // This loop will typically run exactly once. If size_t is larger than
        // uInt (as it is on most 64-bit platforms), input or output larger than
        // uInt's upper bound will require multiple iterations of passing data
        // to zlib.
        while (in_offset < compressed_buf.size()) {
            strm.avail_in = bounded_avail(compressed_buf.size() - in_offset);
            strm.next_in = to_bytef(compressed_buf.data() + in_offset);
            strm.next_out = to_bytef(decompressed_buf.data());
            strm.avail_out = bounded_avail(decompressed_buf.size());
            strm.total_in = 0;
            strm.total_out = 0;

            int rc = inflate(&strm, Z_SYNC_FLUSH);
            REALM_ASSERT(rc != Z_STREAM_ERROR && rc != Z_MEM_ERROR);
            in_offset += strm.total_in;
            decompressed_buf = decompressed_buf.sub_span(strm.total_out);

            if (rc == Z_OK) {
                // We made forward progress but did not reach the end
                continue;
            }
            if (rc == Z_STREAM_END) {
                // If we got Z_STREAM_END and there's leftover input then the
                // data is invalid
                if (strm.avail_in || in_offset < compressed_buf.size() || compressed.next_block().size())
                    return error::corrupt_input;
                if (decompressed_buf.size() != 0)
                    return error::incorrect_decompressed_size;
                return std::error_code{};
            }
            if (rc == Z_NEED_DICT) {
                // We don't support custom dictionaries
                return error::decompress_unsupported;
            }
            if (rc == Z_DATA_ERROR) {
                return error::corrupt_input;
            }
            if (rc == Z_BUF_ERROR) {
                if (strm.avail_out == 0) {
                    if (decompressed_buf.size() > 0) {
                        // We need to pass in the next range of the decompress buffer
                        continue;
                    }
                    // We should never run out of output buffer space unless the
                    // decompressed size was wrong.
                    return error::incorrect_decompressed_size;
                }
                // If there's space left in the output buffer then that means
                // we ran out of input without getting Z_STREAM_END
                return error::corrupt_input;
            }

            // Unknown error code
            REALM_UNREACHABLE();
        }
    } while ((compressed_buf = compressed.next_block()), compressed_buf.size());

    if (strm.avail_in && !strm.avail_out) {
        // Ran out of output buffer with remaining input
        return error::incorrect_decompressed_size;
    }

    // We ran out of input without getting Z_STREAM_END
    return error::corrupt_input;
}

#if REALM_USE_LIBCOMPRESSION
API_AVAILABLE_BEGIN(macos(10.11))
std::error_code decompress_libcompression(NoCopyInputStream& compressed, Span<const char> compressed_buf,
                                          Span<char> decompressed_buf, Algorithm algorithm, bool has_header)
{
    using namespace compression;

    // If we're given a buffer with a zlib header we have to parse it outselves,
    // as libcompression doesn't handle it.
    if (has_header) {
        // The first nibble is compression algorithm (where 8 is DEFLATE), and second
        // nibble is window size. RFC 1950 only allows window size 7, so the first
        // byte must be 0x78.
        if (read_byte(compressed, compressed_buf) != 0x78)
            return error::corrupt_input;
        // The second byte has flags. Bit 5 is the only interesting one, which
        // indicates if a custom dictionary was used. We don't support that.
        uint8_t flags = read_byte(compressed, compressed_buf);
        if (flags & 0b100000)
            return error::decompress_unsupported;
        algorithm = Algorithm::Deflate;
    }

    auto compression_algorithm = algorithm_to_compression_algorithm(algorithm);
    if (!compression_algorithm)
        return error::decompress_unsupported;

    compression_stream strm;
    auto rc = compression_stream_init(&strm, COMPRESSION_STREAM_DECODE, compression_algorithm);
    if (rc != COMPRESSION_STATUS_OK)
        return error::decompress_error;

    // Using ScopeExit here warns about missing availability checking, but also
    // complains about redundant availability checking if it's added.
    struct Cleanup {
        compression_stream* strm;
        ~Cleanup()
        {
            compression_stream_destroy(strm);
        }
    } cleanup{&strm};

    strm.dst_size = decompressed_buf.size();
    strm.dst_ptr = to_bytef(decompressed_buf.data());

    uint32_t expected_checksum = 0;
    uLong actual_checksum = 1;
    do {
        strm.src_size = compressed_buf.size();
        strm.src_ptr = to_bytef(compressed_buf.data());

        // compression_stream_process() only writes 64 KB at a time, and you
        // have to call it in a loop until it stops giving more output before
        // feeding in more input
        while (rc != COMPRESSION_STATUS_END) {
            auto dst_ptr_start = strm.dst_ptr;
            rc = compression_stream_process(&strm, 0);
            if (rc == COMPRESSION_STATUS_ERROR)
                return error::corrupt_input;
            if (strm.dst_ptr == dst_ptr_start)
                break;

            // libcompression doesn't check the checksum, so do it manually.
            // This loop will never actually run multiple times as in practice
            // libcompression doesn't actually write more bytes than fit in uLong
            // in a single call to compression_stream_process()
            while (dst_ptr_start < strm.dst_ptr) {
                auto size = bounded_avail(strm.dst_ptr - dst_ptr_start);
                actual_checksum = adler32(actual_checksum, dst_ptr_start, size);
                dst_ptr_start += size;
            }
        }

        // The checksum at the end can potentially be straddling a block boundary
        // and we can't rewind, so maintain a rolling window of the last four
        // bytes seen.
        for (uint8_t byte : compressed_buf.last(std::min<size_t>(4u, compressed_buf.size()))) {
            expected_checksum <<= 8;
            expected_checksum += byte;
        }
    } while ((compressed_buf = compressed.next_block()), compressed_buf.size());
    rc = compression_stream_process(&strm, COMPRESSION_STREAM_FINALIZE);
    if (rc != COMPRESSION_STATUS_END)
        return error::corrupt_input;
    if (strm.dst_size != 0)
        return error::incorrect_decompressed_size;
    if (expected_checksum != actual_checksum)
        return error::corrupt_input;
    // Check for remaining extra input
    if (strm.src_size || compressed.next_block().size())
        return error::corrupt_input;
    return std::error_code{};
}
API_AVAILABLE_END
#endif

std::error_code decompress(NoCopyInputStream& compressed, Span<const char> compressed_buf,
                           Span<char> decompressed_buf, Algorithm algorithm, bool has_header)
{
    using namespace compression;

    if (decompressed_buf.size() == 0) {
        return std::error_code{};
    }
    if (!compressed_buf.size()) {
        return error::incorrect_decompressed_size;
    }

#if REALM_USE_LIBCOMPRESSION
    // All of our non-macOS deployment targets are high enough to have libcompression,
    // but we support some older macOS versions
    if (__builtin_available(macOS 10.11, *)) {
        if (algorithm != Algorithm::None)
            return decompress_libcompression(compressed, compressed_buf, decompressed_buf, algorithm, has_header);
    }
#endif

    switch (algorithm) {
        case Algorithm::None:
            return decompress_none(compressed, compressed_buf, decompressed_buf);
        case Algorithm::Deflate:
            return decompress_zlib(compressed, compressed_buf, decompressed_buf, has_header);
        default:
            return error::decompress_unsupported;
    }
}

#if 0
struct CompressionStats {
    std::mutex mutex;
    std::map<size_t, std::pair<size_t, size_t>> stats;
    ~CompressionStats()
    {
        std::lock_guard lock(mutex);
        size_t total_uncompressed = 0;
        size_t total_compressed = 0;
        for (auto& [size, results] : stats) {
            fprintf(stderr, "%zu: %zu %g\n", size, results.first, static_cast<double>(results.second) / results.first / size * 100);
            total_uncompressed += size * results.first;
            total_compressed += results.second;
        }
        fprintf(stderr, "total: %zu -> %zu (%g%%)\n", total_uncompressed, total_compressed, (double)total_compressed / total_uncompressed * 100.0);
    }
} s_compression_stats;

void record_compression_result(size_t uncompressed, size_t compressed)
{
    std::lock_guard lock(s_compression_stats.mutex);
    auto& arr = s_compression_stats.stats[uncompressed];
    arr.first++;
    arr.second += compressed;
}
#else
void record_compression_result(size_t, size_t) {}
#endif

#if REALM_USE_LIBCOMPRESSION
API_AVAILABLE_BEGIN(macos(10.11))
std::error_code compress_lzfse(Span<const char> uncompressed_buf, Span<char> compressed_buf,
                               std::size_t& compressed_size, compression::Alloc* custom_allocator)
{
    using namespace compression;
    if (compressed_buf.size() < 4)
        return error::compress_buffer_too_small;
    // compression_encode_buffer() takes a size_t, but crashes if the value is
    // larger than 2^31. Using the stream API works, but it's slower for
    // normal-sized input, and we can just fall back to zlib for this edge case.
    if (uncompressed_buf.size() > std::numeric_limits<int32_t>::max())
        return error::compress_input_too_long;

    auto uncompressed_ptr = to_bytef(uncompressed_buf.data());
    auto uncompressed_size = uncompressed_buf.size();
    auto compressed_ptr = to_bytef(compressed_buf.data());
    auto compressed_buf_size = compressed_buf.size() - 4;

    void* scratch_buffer = nullptr;
    if (custom_allocator) {
        scratch_buffer = custom_allocator->alloc(compression_encode_scratch_buffer_size(COMPRESSION_LZFSE));
        if (!scratch_buffer)
            return error::out_of_memory;
    }

    size_t bytes = compression_encode_buffer(compressed_ptr, compressed_buf_size, uncompressed_ptr, uncompressed_size,
                                             scratch_buffer, COMPRESSION_LZFSE);
    if (bytes == 0)
        return error::compress_buffer_too_small;

    // Calculate the checksum and append it to the end of the stream
    uLong checksum = htonl(adler32(1, uncompressed_ptr, static_cast<uInt>(uncompressed_size)));
    for (int i = 0; i < 4; ++i) {
        compressed_buf[bytes + i] = checksum & 0xFF;
        checksum >>= 8;
    }
    compressed_size = bytes + 4;
    return std::error_code{};
}
API_AVAILABLE_END
#endif

std::error_code compress_lzfse_or_zlib(Span<const char> uncompressed_buf, Span<char> compressed_buf,
                                       std::size_t& compressed_size, int compression_level,
                                       compression::Alloc* custom_allocator)
{
    using namespace compression;
#if REALM_USE_LIBCOMPRESSION
    if (__builtin_available(macOS 10.11, *)) {
        size_t len = write_header({Algorithm::Lzfse, uncompressed_buf.size()}, compressed_buf);
        auto ec = compress_lzfse(uncompressed_buf, compressed_buf.sub_span(len), compressed_size, custom_allocator);
        if (ec != error::compress_input_too_long)
            return ec;
    }
#endif
    size_t len = header_width(uncompressed_buf.size());
    REALM_ASSERT(len >= 2);
    auto ec = compress(uncompressed_buf, compressed_buf.sub_span(len - 2), compressed_size, compression_level,
                       custom_allocator);
    if (!ec) {
        // Note: overwrites zlib header
        write_header({Algorithm::Deflate, uncompressed_buf.size()}, compressed_buf);
        compressed_size -= 2;
    }
    return ec;
}
} // unnamed namespace


const std::error_category& compression::error_category() noexcept
{
    return g_error_category;
}

std::error_code compression::make_error_code(error error_code) noexcept
{
    return std::error_code(int(error_code), g_error_category);
}


// zlib compression level: 1-9, 1 fastest.

// zlib deflateBound()
std::size_t compression::compress_bound(std::size_t size) noexcept
{
    // DEFLATE's worst-case size is a 6 byte zlib header, plus the uncompressed
    // data, plus a 5 byte header for every 16383 byte block.
    size_t overhead = 6 + 5 * (size / 16383 + 1);
    if (std::numeric_limits<size_t>::max() - overhead < size)
        return 0;
    return size + overhead;
}


// zlib deflate()
std::error_code compression::compress(Span<const char> uncompressed_buf, Span<char> compressed_buf,
                                      std::size_t& compressed_size, int compression_level, Alloc* custom_allocator)
{
    auto uncompressed_ptr = to_bytef(uncompressed_buf.data());
    auto uncompressed_size = uncompressed_buf.size();
    auto compressed_ptr = to_bytef(compressed_buf.data());
    auto compressed_buf_size = compressed_buf.size();

    z_stream strm = {};
    if (custom_allocator) {
        strm.opaque = custom_allocator;
        strm.zalloc = &custom_alloc;
        strm.zfree = &custom_free;
    }

    int rc = deflateInit(&strm, compression_level);
    if (rc == Z_MEM_ERROR)
        return error::out_of_memory;

    if (rc != Z_OK)
        return error::compress_error;

    strm.next_in = uncompressed_ptr;
    strm.avail_in = 0;
    strm.next_out = compressed_ptr;
    strm.avail_out = 0;

    std::size_t next_in_ndx = 0;
    std::size_t next_out_ndx = 0;
    REALM_ASSERT(rc == Z_OK);
    while (rc == Z_OK || rc == Z_BUF_ERROR) {
        REALM_ASSERT(strm.next_in + strm.avail_in == uncompressed_ptr + next_in_ndx);
        REALM_ASSERT(strm.next_out + strm.avail_out == compressed_ptr + next_out_ndx);

        bool stream_updated = false;

        if (strm.avail_in == 0 && next_in_ndx < uncompressed_size) {
            auto in_size = bounded_avail(uncompressed_size - next_in_ndx);
            next_in_ndx += in_size;
            strm.avail_in = uInt(in_size);
            stream_updated = true;
        }

        if (strm.avail_out == 0 && next_out_ndx < compressed_buf_size) {
            auto out_size = bounded_avail(compressed_buf_size - next_out_ndx);
            next_out_ndx += out_size;
            strm.avail_out = uInt(out_size);
            stream_updated = true;
        }

        if (rc == Z_BUF_ERROR && !stream_updated) {
            deflateEnd(&strm);
            return error::compress_buffer_too_small;
        }

        int flush = (next_in_ndx == uncompressed_size) ? Z_FINISH : Z_NO_FLUSH;

        rc = deflate(&strm, flush);
        REALM_ASSERT(rc != Z_STREAM_END || flush == Z_FINISH);
    }

    if (rc != Z_STREAM_END) {
        deflateEnd(&strm);
        return error::compress_error;
    }

    compressed_size = next_out_ndx - strm.avail_out;

    rc = deflateEnd(&strm);
    if (rc != Z_OK)
        return error::compress_error;

    return std::error_code{};
}

std::error_code compression::decompress(NoCopyInputStream& compressed, Span<char> decompressed_buf)
{
    return ::decompress(compressed, compressed.next_block(), decompressed_buf, Algorithm::Deflate, true);
}

std::error_code compression::decompress(Span<const char> compressed_buf, Span<char> decompressed_buf)
{
    SimpleNoCopyInputStream adapter(compressed_buf);
    return ::decompress(adapter, adapter.next_block(), decompressed_buf, Algorithm::Deflate, true);
}

std::error_code compression::decompress_nonportable(NoCopyInputStream& compressed, AppendBuffer<char>& decompressed)
{
    auto compressed_buf = compressed.next_block();
    auto header = read_header(compressed, compressed_buf);
    if (header.size == std::numeric_limits<size_t>::max())
        return error::out_of_memory;
    decompressed.resize(header.size);
    if (header.size == 0)
        return std::error_code{};
    return ::decompress(compressed, compressed_buf, decompressed, header.algorithm, false);
}

std::error_code compression::allocate_and_compress(CompressMemoryArena& compress_memory_arena,
                                                   Span<const char> uncompressed_buf,
                                                   std::vector<char>& compressed_buf)
{
    const int compression_level = 1;
    std::size_t compressed_size = 0;

    if (compressed_buf.size() < 256)
        compressed_buf.resize(256); // Throws

    for (;;) {
        init_arena(compress_memory_arena);
        std::error_code ec = compression::compress(uncompressed_buf, compressed_buf, compressed_size,
                                                   compression_level, &compress_memory_arena);

        if (REALM_UNLIKELY(ec)) {
            if (ec == compression::error::compress_buffer_too_small) {
                std::size_t n = compressed_buf.size();
                REALM_ASSERT(n != std::numeric_limits<std::size_t>::max());
                if (util::int_multiply_with_overflow_detect(n, 2))
                    n = std::numeric_limits<std::size_t>::max();
                compressed_buf.resize(n); // Throws
                continue;
            }
            if (ec == compression::error::out_of_memory) {
                grow_arena(compress_memory_arena); // Throws
                continue;
            }
            return ec;
        }
        break;
    }
    compressed_buf.resize(compressed_size);
    return std::error_code{};
}

void compression::allocate_and_compress_nonportable(CompressMemoryArena& arena, Span<const char> uncompressed,
                                                    util::AppendBuffer<char>& compressed)
{
    if (uncompressed.size() == 0) {
        compressed.resize(0);
        return;
    }

    size_t header_size = header_width(uncompressed.size());
    compressed.resize(uncompressed.size() + header_size);
    size_t compressed_size = 0;
    // zlib is ineffective for very small sizes. Measured results indicate that
    // it only manages to compress at all past 100 bytes and the compression
    // ratio becomes interesting around 200 bytes.
    while (uncompressed.size() > 256) {
        init_arena(arena);
        const int compression_level = 1;
        auto ec = compress_lzfse_or_zlib(uncompressed, compressed, compressed_size, compression_level, &arena);
        if (ec == error::compress_buffer_too_small) {
            // Compressed result was larger than uncompressed, so just store the
            // uncompressed
            compressed_size = 0;
            break;
        }
        if (ec == compression::error::out_of_memory) {
            grow_arena(arena); // Throws
            continue;
        }
        if (ec) {
            throw std::system_error(ec);
        }
        REALM_ASSERT(compressed_size);
        compressed_size += header_size;
        record_compression_result(uncompressed.size(), compressed_size);
        compressed.resize(compressed_size);
        return;
    }

    // If compression made it grow or it was too small to compress then copy
    // the source over uncompressed
    if (!compressed_size) {
        record_compression_result(uncompressed.size(), uncompressed.size() + header_size);
        write_header({Algorithm::None, uncompressed.size()}, compressed);
        std::memcpy(compressed.data() + header_size, uncompressed.data(), uncompressed.size());
    }
}

util::AppendBuffer<char> compression::allocate_and_compress_nonportable(Span<const char> uncompressed_buf)
{
    util::compression::CompressMemoryArena arena;
    util::AppendBuffer<char> compressed;
    allocate_and_compress_nonportable(arena, uncompressed_buf, compressed);
    return compressed;
}

std::unique_ptr<NoCopyInputStream> compression::decompress_nonportable_input_stream(NoCopyInputStream& source,
                                                                                    size_t& total_size)
{
    auto first_block = source.next_block();
    auto header = read_header(source, first_block);
    if (header.size == std::numeric_limits<size_t>::max())
        return nullptr;
    total_size = header.size;

    if (header.algorithm == Algorithm::None)
        return std::make_unique<DecompressInputStreamNone>(source, first_block);
#if REALM_USE_LIBCOMPRESSION
    if (__builtin_available(macOS 10.11, *)) {
        if (header.algorithm == Algorithm::Deflate || header.algorithm == Algorithm::Lzfse)
            return std::make_unique<DecompressInputStreamLibCompression>(source, first_block, header);
    }
#endif
    if (header.algorithm == Algorithm::Deflate)
        return std::make_unique<DecompressInputStreamZlib>(source, first_block, total_size);
    return nullptr;
}

size_t compression::get_uncompressed_size_from_header(NoCopyInputStream& source)
{
    auto first_block = source.next_block();
    return read_header(source, first_block).size;
}
