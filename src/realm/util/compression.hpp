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

#ifndef REALM_UTIL_COMPRESSION_HPP
#define REALM_UTIL_COMPRESSION_HPP

#include <realm/util/buffer.hpp>
#include <realm/util/features.h>
#include <realm/util/input_stream.hpp>
#include <realm/util/span.hpp>

#include <array>
#include <memory>
#include <system_error>
#include <stdint.h>
#include <stddef.h>
#include <string>
#include <vector>

// Use libcompression by default on Apple platforms, but it can be disabled to
// test the zlib codepaths
#ifndef REALM_USE_LIBCOMPRESSION
#define REALM_USE_LIBCOMPRESSION REALM_PLATFORM_APPLE
#endif

namespace realm::util::compression {

enum class error {
    out_of_memory = 1,
    compress_buffer_too_small = 2,
    compress_error = 3,
    compress_input_too_long = 4,
    corrupt_input = 5,
    incorrect_decompressed_size = 6,
    decompress_error = 7,
};

const std::error_category& error_category() noexcept;

std::error_code make_error_code(error) noexcept;

} // namespace realm::util::compression

namespace std {

template <>
struct is_error_code_enum<realm::util::compression::error> {
    static const bool value = true;
};

} // namespace std

namespace realm::util::compression {

enum class Algorithm {
    None = 0,
    Deflate = 0x78,
    LZFSE = 0x09,
};

class Alloc {
public:
    // Returns null on "out of memory"
    virtual void* alloc(size_t size) noexcept = 0;
    virtual void free(void* addr) noexcept = 0;
    virtual ~Alloc() {}
};

class CompressMemoryArena : public Alloc {
public:
    void* alloc(size_t size) noexcept override final
    {
        size_t offset = m_offset;
        size_t misalignment = offset % alignof(std::max_align_t);
        size_t padding = (misalignment == 0) ? 0 : (alignof(std::max_align_t) - misalignment);
        if (padding > m_size - offset)
            return nullptr;
        offset += padding;
        REALM_ASSERT(offset % alignof(std::max_align_t) == 0);
        void* addr = m_buffer.get() + offset;
        if (size > m_size - offset)
            return nullptr;
        m_offset = offset + size;
        return addr;
    }

    void free(void*) noexcept override final
    {
        // No-op
    }

    void reset() noexcept
    {
        m_offset = 0;
    }

    size_t size() const noexcept
    {
        return m_size;
    }

    void resize(size_t size)
    {
        m_buffer = std::make_unique<char[]>(size); // Throws
        m_size = size;
        m_offset = 0;
    }

private:
    size_t m_size = 0, m_offset = 0;
    std::unique_ptr<char[]> m_buffer;
};


/// compress_bound() calculates an upper bound on the size of the compressed
/// data. The caller can use this function to allocate memory buffer calling
/// compress(). Returns 0 if the bound would overflow size_t.
size_t compress_bound(size_t uncompressed_size) noexcept;

/// compress() compresses the data in the \a uncompressed_buf and stores it in
/// \a compressed_buf. If compression is successful, the compressed size is
/// stored in \a compressed_size. \a compression_level is [1-9] with 1 the
/// fastest for the current zlib implementation. The returned error code is of
/// category compression::error_category. If \a Alloc is non-null, it is used
/// for all memory allocations inside compress() and compress() will not throw
/// any exceptions.
std::error_code compress(Span<const char> uncompressed_buf, Span<char> compressed_buf, size_t& compressed_size,
                         int compression_level = 1, Alloc* custom_allocator = nullptr);

/// decompress() decompresses the data in \a compressed_buf into \a decompressed_buf.
/// decompress may throw std::bad_alloc, but all other errors (including the
/// target buffer being too small) are reported by returning an error code of
/// category compression::error_code.
std::error_code decompress(Span<const char> compressed_buf, Span<char> decompressed_buf);

std::error_code decompress(NoCopyInputStream& compressed, Span<char> decompressed_buf);
std::error_code decompress_with_header(NoCopyInputStream& compressed, AppendBuffer<char>& decompressed);

std::error_code allocate_and_compress(CompressMemoryArena& compress_memory_arena, Span<const char> uncompressed_buf,
                                      std::vector<char>& compressed_buf);
void allocate_and_compress_with_header(CompressMemoryArena& compress_memory_arena, Span<const char> uncompressed_buf,
                                       util::AppendBuffer<char>& compressed_buf);
util::AppendBuffer<char> allocate_and_compress_with_header(Span<const char> uncompressed_buf);

std::unique_ptr<NoCopyInputStream> decompress_input_stream(NoCopyInputStream& source, size_t& total_size);

size_t get_uncompressed_size_from_header(NoCopyInputStream& source);

} // namespace realm::util::compression

#endif // REALM_UTIL_COMPRESSION_HPP
