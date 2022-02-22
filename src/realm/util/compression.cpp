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

#include <limits>
#include <zlib.h>
#include <zconf.h> // for zlib

#if REALM_USE_LIBCOMPRESSION
#include <compression.h>
#endif

namespace {

constexpr std::size_t g_max_stream_avail =
    (sizeof(uInt) < sizeof(size_t)) ? std::numeric_limits<uInt>::max() : std::numeric_limits<std::size_t>::max();

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
            case error::corrupt_input:
                return "Corrupt input data";
            case error::incorrect_decompressed_size:
                return "Decompressed data size not equal to expected size";
            case error::decompress_error:
                return "Decompression error";
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

} // unnamed namespace


using namespace realm;
using namespace util;

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
    auto uncompressed_ptr = reinterpret_cast<unsigned char*>(const_cast<char*>(uncompressed_buf.data()));
    auto uncompressed_size = uncompressed_buf.size();
    auto compressed_ptr = reinterpret_cast<unsigned char*>(compressed_buf.data());
    auto compressed_buf_size = compressed_buf.size();

    z_stream strm;
    strm.opaque = Z_NULL;
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;

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
            std::size_t in_size = std::min(uncompressed_size - next_in_ndx, g_max_stream_avail);
            next_in_ndx += in_size;
            strm.avail_in = uInt(in_size);
            stream_updated = true;
        }

        if (strm.avail_out == 0 && next_out_ndx < compressed_buf_size) {
            std::size_t out_size = std::min(compressed_buf_size - next_out_ndx, g_max_stream_avail);
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


// zlib inflate
std::error_code compression::decompress(Span<const char> compressed_buf, Span<char> decompressed_buf)
{
    auto compressed_ptr = reinterpret_cast<unsigned char*>(const_cast<char*>(compressed_buf.data()));
    auto compressed_size = compressed_buf.size();
    auto decompressed_ptr = reinterpret_cast<unsigned char*>(decompressed_buf.data());
    auto decompressed_buf_size = decompressed_buf.size();

#if REALM_USE_LIBCOMPRESSION
    // All of our non-macOS deployment targets are high enough to have libcompression,
    // but we support some older macOS versions
    if (__builtin_available(macOS 10.11, *)) {
        // libcompression doesn't handle the zlib header, so we have to do it ourselves.
        // The first byte is the compression algorithm and the second is the
        // window size (which zlib uses as an optimization to allocate the correct
        // buffer size, but is optional).
        if (compressed_size < 2 || *compressed_ptr != 0x78)
            return error::corrupt_input;
        compressed_size -= 2;
        compressed_ptr += 2;
        size_t bytes_written = compression_decode_buffer(
            reinterpret_cast<uint8_t*>(decompressed_ptr), decompressed_buf_size,
            reinterpret_cast<const uint8_t*>(compressed_ptr), compressed_size, nullptr, COMPRESSION_ZLIB);
        if (bytes_written == decompressed_buf_size)
            return std::error_code{};
        if (bytes_written == 0)
            return error::corrupt_input;
        return error::incorrect_decompressed_size;
    }
#endif

    z_stream strm;

    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;

    std::size_t next_in_ndx = 0;
    std::size_t next_out_ndx = 0;

    strm.next_in = compressed_ptr;
    next_in_ndx = std::min(compressed_size - next_in_ndx, g_max_stream_avail);
    strm.avail_in = uInt(next_in_ndx);
    strm.next_out = decompressed_ptr;
    strm.avail_out = 0;

    int rc = inflateInit(&strm);
    if (rc != Z_OK)
        return error::decompress_error;


    REALM_ASSERT(rc == Z_OK);
    while (rc == Z_OK || rc == Z_BUF_ERROR) {

        REALM_ASSERT(strm.next_in + strm.avail_in == compressed_ptr + next_in_ndx);
        REALM_ASSERT(strm.next_out + strm.avail_out == decompressed_ptr + next_out_ndx);

        bool stream_updated = false;

        if (strm.avail_in == 0 && next_in_ndx < compressed_size) {
            REALM_ASSERT(strm.next_in == compressed_ptr + next_in_ndx);
            std::size_t in_size = std::min(compressed_size - next_in_ndx, g_max_stream_avail);
            next_in_ndx += in_size;
            strm.avail_in = uInt(in_size);
            stream_updated = true;
        }

        if (strm.avail_out == 0 && next_out_ndx < decompressed_buf_size) {
            REALM_ASSERT(strm.next_out == decompressed_ptr + next_out_ndx);
            std::size_t out_size = std::min(decompressed_buf_size - next_out_ndx, g_max_stream_avail);
            next_out_ndx += out_size;
            strm.avail_out = uInt(out_size);
            stream_updated = true;
        }

        if (rc == Z_BUF_ERROR && !stream_updated) {
            inflateEnd(&strm);
            return error::incorrect_decompressed_size;
        }

        int flush = (next_in_ndx == compressed_size) ? Z_FINISH : Z_NO_FLUSH;

        rc = inflate(&strm, flush);
        REALM_ASSERT(rc != Z_STREAM_END || flush == Z_FINISH);
    }

    if (rc != Z_STREAM_END) {
        inflateEnd(&strm);
        return error::corrupt_input;
    }

    rc = inflateEnd(&strm);
    if (rc != Z_OK)
        return error::decompress_error;

    return std::error_code{};
}


void compression::allocate_and_compress(CompressMemoryArena& compress_memory_arena, Span<const char> uncompressed_buf,
                                        std::vector<char>& compressed_buf)
{
    const int compression_level = 1;
    std::size_t compressed_size = 0;

    if (compressed_buf.size() < 256)
        compressed_buf.resize(256); // Throws

    for (;;) {
        compress_memory_arena.reset();
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
                std::size_t n = compress_memory_arena.size();
                if (n == 0) {
                    // About 256KiB according to ZLIB documentation (about
                    // 1MiB in reality, strangely)
                    n = 256 * 1024;
                }
                else {
                    REALM_ASSERT(n != std::numeric_limits<std::size_t>::max());
                    if (util::int_multiply_with_overflow_detect(n, 2))
                        n = std::numeric_limits<std::size_t>::max();
                }
                compress_memory_arena.resize(n); // Throws
                continue;
            }
            throw std::system_error(ec);
        }
        break;
    }
    compressed_buf.resize(compressed_size);
}
