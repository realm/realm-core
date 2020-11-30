#include <cstddef>
#include <limits>

#include <zlib.h>
#include <zconf.h> // for zlib

#include <realm/sync/noinst/compression.hpp>
#include <realm/util/assert.hpp>
#include <realm/util/aes_cryptor.hpp>

namespace {

constexpr std::size_t g_max_stream_avail =
    (sizeof(uInt) < sizeof(size_t)) ? std::numeric_limits<uInt>::max() : std::numeric_limits<std::size_t>::max();

class ErrorCategoryImpl : public std::error_category {
public:
    const char* name() const noexcept override final
    {
        return "realm::_impl::compression::error";
    }
    std::string message(int err) const override final
    {
        using error = realm::_impl::compression::error;
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
            case error::source_file_is_not_readable:
                return "Source file is not readable";
            case error::destination_path_is_not_writable:
                return "Destination path is not writable";
            case error::invalid_input:
                return "Invalid input";
            case error::decryption_error:
                return "Decryption error";
            case error::missing_block_header:
                return "Missing block header";
            case error::invalid_block_size:
                return "Invalid block size";
        }
        REALM_UNREACHABLE();
    }
};

ErrorCategoryImpl g_error_category;

void* custom_alloc(void* opaque, unsigned int cnt, unsigned int size)
{
    using Alloc = realm::_impl::compression::Alloc;
    Alloc& alloc = *static_cast<Alloc*>(opaque);
    std::size_t accum_size = cnt * std::size_t(size);
    return alloc.alloc(accum_size);
}

void custom_free(void* opaque, void* addr)
{
    using Alloc = realm::_impl::compression::Alloc;
    Alloc& alloc = *static_cast<Alloc*>(opaque);
    return alloc.free(addr);
}

} // unnamed namespace


using namespace realm;
using namespace _impl;

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
std::error_code compression::compress_bound(const char* uncompressed_buf, std::size_t uncompressed_size,
                                            std::size_t& bound, int compression_level)
{
    z_stream strm;

    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.next_in = reinterpret_cast<unsigned char*>(const_cast<char*>(uncompressed_buf));
    strm.avail_in = uInt(uncompressed_size);

    int rc = deflateInit(&strm, compression_level);
    if (rc == Z_MEM_ERROR)
        return error::out_of_memory;

    if (rc != Z_OK)
        return error::compress_error;

    unsigned long zlib_bound = deflateBound(&strm, uLong(uncompressed_size));

    rc = deflateEnd(&strm);
    if (rc != Z_OK)
        return error::compress_error;

    bound = zlib_bound;

    return std::error_code{};
}


// zlib deflate()
std::error_code compression::compress(const char* uncompressed_buf, std::size_t uncompressed_size,
                                      char* compressed_buf, std::size_t compressed_buf_size,
                                      std::size_t& compressed_size, int compression_level, Alloc* custom_allocator)
{
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

    strm.next_in = reinterpret_cast<unsigned char*>(const_cast<char*>(uncompressed_buf));
    strm.avail_in = 0;
    strm.next_out = reinterpret_cast<unsigned char*>(compressed_buf);
    strm.avail_out = 0;

    std::size_t next_in_ndx = 0;
    std::size_t next_out_ndx = 0;
    REALM_ASSERT(rc == Z_OK);
    while (rc == Z_OK || rc == Z_BUF_ERROR) {

        REALM_ASSERT(const_cast<const char*>(reinterpret_cast<char*>(strm.next_in + strm.avail_in)) ==
                     uncompressed_buf + next_in_ndx);
        REALM_ASSERT(const_cast<const char*>(reinterpret_cast<char*>(strm.next_out + strm.avail_out)) ==
                     compressed_buf + next_out_ndx);

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
std::error_code compression::decompress(const char* compressed_buf, std::size_t compressed_size,
                                        char* decompressed_buf, std::size_t decompressed_size)
{
    z_stream strm;

    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;

    std::size_t next_in_ndx = 0;
    std::size_t next_out_ndx = 0;

    strm.next_in = reinterpret_cast<unsigned char*>(const_cast<char*>(compressed_buf));
    next_in_ndx = std::min(compressed_size - next_in_ndx, g_max_stream_avail);
    strm.avail_in = uInt(next_in_ndx);
    strm.next_out = reinterpret_cast<unsigned char*>(const_cast<char*>(decompressed_buf));
    strm.avail_out = 0;

    int rc = inflateInit(&strm);
    if (rc != Z_OK)
        return error::decompress_error;


    REALM_ASSERT(rc == Z_OK);
    while (rc == Z_OK || rc == Z_BUF_ERROR) {

        REALM_ASSERT(const_cast<const char*>(reinterpret_cast<char*>(strm.next_in + strm.avail_in)) ==
                     compressed_buf + next_in_ndx);
        REALM_ASSERT(const_cast<const char*>(reinterpret_cast<char*>(strm.next_out + strm.avail_out)) ==
                     decompressed_buf + next_out_ndx);

        bool stream_updated = false;

        if (strm.avail_in == 0 && next_in_ndx < compressed_size) {
            REALM_ASSERT(const_cast<const char*>(reinterpret_cast<char*>(strm.next_in)) ==
                         compressed_buf + next_in_ndx);
            std::size_t in_size = std::min(compressed_size - next_in_ndx, g_max_stream_avail);
            next_in_ndx += in_size;
            strm.avail_in = uInt(in_size);
            stream_updated = true;
        }

        if (strm.avail_out == 0 && next_out_ndx < decompressed_size) {
            REALM_ASSERT(const_cast<const char*>(reinterpret_cast<char*>(strm.next_out)) ==
                         decompressed_buf + next_out_ndx);
            std::size_t out_size = std::min(decompressed_size - next_out_ndx, g_max_stream_avail);
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


std::size_t compression::allocate_and_compress(CompressMemoryArena& compress_memory_arena,
                                               BinaryData uncompressed_buf, std::vector<char>& compressed_buf)
{
    const int compression_level = 1;
    std::size_t compressed_size = 0;

    compress_memory_arena.reset();

    if (compressed_buf.size() < 256)
        compressed_buf.resize(256); // Throws

    for (;;) {
        std::error_code ec =
            compression::compress(uncompressed_buf.data(), uncompressed_buf.size(), compressed_buf.data(),
                                  compressed_buf.size(), compressed_size, compression_level, &compress_memory_arena);

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

    return compressed_size;
}

namespace {

std::error_code do_compress_file(const std::string& src_path, const std::string& dst_path,
                                 util::File::SizeType& src_size, util::File::SizeType& dst_size,
                                 std::size_t memory_usage)
{
    util::File src_file;
    try {
        src_file.open(src_path);
    }
    catch (util::File::AccessError&) {
        return compression::error::source_file_is_not_readable;
    }
    src_size = src_file.get_size();

    util::File dst_file;
    try {
        dst_file.open(dst_path, util::File::mode_Write);
    }
    catch (util::File::AccessError&) {
        return compression::error::destination_path_is_not_writable;
    }

    z_stream strm;
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    strm.data_type = Z_BINARY;

    int compression_level = Z_DEFAULT_COMPRESSION;
    int rc = deflateInit(&strm, compression_level);
    if (rc == Z_MEM_ERROR)
        return compression::error::out_of_memory;
    if (rc != Z_OK)
        return compression::error::compress_error;

    const std::size_t buf_size = memory_usage;
    std::unique_ptr<char[]> in_buf = std::make_unique<char[]>(buf_size);
    std::unique_ptr<char[]> out_buf = std::make_unique<char[]>(buf_size);

    bool eof = false;

    strm.avail_in = 0;
    strm.next_out = reinterpret_cast<Bytef*>(out_buf.get());
    strm.avail_out = uInt(buf_size);

    while (rc != Z_STREAM_END) {
        if (strm.avail_in == 0 && !eof) {
            std::size_t nread = src_file.read(in_buf.get(), buf_size);
            if (nread < buf_size)
                eof = true;
            strm.next_in = reinterpret_cast<Bytef*>(in_buf.get());
            strm.avail_in = uInt(nread);
        }

        int flush = eof ? Z_FINISH : Z_NO_FLUSH;
        rc = deflate(&strm, flush);
        if (rc != Z_OK && rc != Z_BUF_ERROR && rc != Z_STREAM_END) {
            deflateEnd(&strm);
            if (rc == Z_MEM_ERROR)
                return compression::error::out_of_memory;
            return compression::error::compress_error;
        }
        REALM_ASSERT(rc != Z_STREAM_END || flush == Z_FINISH);

        if (strm.avail_out == 0 || rc == Z_STREAM_END) {
            std::size_t nwrite = buf_size - strm.avail_out;
            dst_file.write(out_buf.get(), nwrite);
            strm.next_out = reinterpret_cast<Bytef*>(out_buf.get());
            strm.avail_out = uInt(buf_size);
        }
    }
    REALM_ASSERT(eof && strm.avail_in == 0 && strm.avail_out == buf_size);

    rc = deflateEnd(&strm);
    if (rc != Z_OK)
        return compression::error::compress_error;

    dst_size = dst_file.get_size();

    return std::error_code{};
}

std::error_code do_decompress_file(const std::string& src_path, const std::string& dst_path,
                                   util::File::SizeType& src_size, util::File::SizeType& dst_size,
                                   std::size_t memory_usage)
{
    util::File src_file;
    try {
        src_file.open(src_path);
    }
    catch (util::File::AccessError&) {
        return compression::error::source_file_is_not_readable;
    }
    src_size = src_file.get_size();

    util::File dst_file;
    try {
        dst_file.open(dst_path, util::File::mode_Write);
    }
    catch (util::File::AccessError&) {
        return compression::error::destination_path_is_not_writable;
    }

    z_stream strm;
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    strm.data_type = Z_BINARY;

    int rc = inflateInit(&strm);
    if (rc == Z_MEM_ERROR)
        return compression::error::out_of_memory;
    if (rc != Z_OK)
        return compression::error::decompress_error;

    std::size_t buf_size = memory_usage;
    std::unique_ptr<char[]> in_buf = std::make_unique<char[]>(buf_size);
    std::unique_ptr<char[]> out_buf = std::make_unique<char[]>(buf_size);

    bool eof = false;

    strm.avail_in = 0;
    strm.next_out = reinterpret_cast<Bytef*>(out_buf.get());
    strm.avail_out = uInt(buf_size);

    while (rc != Z_STREAM_END) {
        if (strm.avail_in == 0 && !eof) {
            std::size_t nread = src_file.read(in_buf.get(), buf_size);
            if (nread < buf_size)
                eof = true;
            strm.next_in = reinterpret_cast<Bytef*>(in_buf.get());
            strm.avail_in = uInt(nread);
        }

        int flush = eof ? Z_FINISH : Z_NO_FLUSH;

        rc = inflate(&strm, flush);
        if (rc != Z_OK && rc != Z_BUF_ERROR && rc != Z_STREAM_END) {
            inflateEnd(&strm);
            if (rc == Z_MEM_ERROR)
                return compression::error::out_of_memory;
            return compression::error::corrupt_input;
        }
        REALM_ASSERT(rc != Z_STREAM_END || flush == Z_FINISH);

        if (strm.avail_out == 0 || rc == Z_STREAM_END) {
            std::size_t nwrite = buf_size - strm.avail_out;
            dst_file.write(out_buf.get(), nwrite);
            strm.next_out = reinterpret_cast<Bytef*>(out_buf.get());
            strm.avail_out = uInt(buf_size);
        }
    }
    REALM_ASSERT(eof && strm.avail_in == 0 && strm.avail_out == buf_size);

    rc = inflateEnd(&strm);
    if (rc != Z_OK)
        return compression::error::decompress_error;

    dst_size = dst_file.get_size();

    return std::error_code{};
}

} // namespace

std::error_code compression::compress_file(const std::string& src_path, const std::string& dst_path,
                                           util::File::SizeType& src_size, util::File::SizeType& dst_size)
{
    std::size_t memory_usage = 1 << 20;
    std::error_code ec = do_compress_file(src_path, dst_path, src_size, dst_size, memory_usage);
    return ec;
}

std::error_code compression::decompress_file(const std::string& src_path, const std::string& dst_path,
                                             util::File::SizeType& src_size, util::File::SizeType& dst_size)
{
    std::size_t memory_usage = 1 << 20;
    std::error_code ec = do_decompress_file(src_path, dst_path, src_size, dst_size, memory_usage);
    return ec;
}

std::error_code compression::compress_block_with_header(const char* uncompressed_buf, std::size_t uncompressed_size,
                                                        char* compressed_buf, std::size_t compressed_buf_size,
                                                        std::size_t& compressed_size)
{
    _impl::compression::CompressMemoryArena allocator;

    // The allocator supplied to zlib allocates at most 8 MB. Observations show
    // that zlib asks for less than 300 KB. There is no reason that 8 MB should
    // not be enough to compress 256 KB. We return with an out of memory error
    // if this is not enough, instead of allocating more memory.
    const std::size_t memory_usage = 1 << 23;
    allocator.resize(memory_usage);

    z_stream strm;
    strm.zalloc = &custom_alloc;
    strm.zfree = &custom_free;
    strm.opaque = &allocator;
    strm.data_type = Z_BINARY;
    int compression_level = Z_DEFAULT_COMPRESSION;
    int rc = deflateInit(&strm, compression_level);
    if (rc == Z_MEM_ERROR)
        return compression::error::out_of_memory;
    if (rc != Z_OK)
        return compression::error::compress_error;

    strm.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(uncompressed_buf));
    strm.avail_in = uInt(uncompressed_size);

    // Make space for the 4 byte prefix.
    strm.next_out = reinterpret_cast<Bytef*>(compressed_buf + 4);
    REALM_ASSERT(compressed_buf_size > 4);
    strm.avail_out = uInt(compressed_buf_size - 4);

    int flush = Z_FINISH;
    rc = deflate(&strm, flush);
    if (rc != Z_STREAM_END) {
        deflateEnd(&strm);
        if (rc == Z_MEM_ERROR)
            return compression::error::out_of_memory;
        return compression::error::compress_error;
    }
    std::size_t compressed_size_without_header = compressed_buf_size - 4 - strm.avail_out;

    deflateEnd(&strm);

    // Make prefix
    std::size_t prefix = compressed_size_without_header;
    for (int i = 3; i >= 0; --i) {
        compressed_buf[i] = static_cast<unsigned char>(prefix & 255);
        prefix >>= 8;
    }

    // compressed_size includes the 4 byte header.
    compressed_size = compressed_size_without_header + 4;

    return std::error_code{};
}

std::error_code compression::integrate_compressed_blocks_in_realm_file(
    const char* blocks, std::size_t blocks_size, const std::string& dst_path,
    const util::Optional<std::array<char, 64>>& encryption_key, std::uint_fast64_t& dst_size)
{
#if REALM_ENABLE_ENCRYPTION
    std::unique_ptr<util::AESCryptor> aes_cryptor;
    if (encryption_key)
        aes_cryptor.reset(
            new util::AESCryptor(reinterpret_cast<unsigned char*>(const_cast<char*>(encryption_key->data()))));
#else
    REALM_ASSERT(!encryption_key);
#endif

    // A decompressed block is guaranteed to have size below 256 KB.
    const std::size_t buf_size = 1 << 18;
    std::unique_ptr<char[]> buf{new char[buf_size]};

    util::File file;
    {
        bool was_created;
        file.open(dst_path, was_created);
        file.seek(file.get_size());
    }

#if REALM_ENABLE_ENCRYPTION
    const std::size_t encryption_block_size = 4096;
    const std::size_t blocks_per_metadata_block = 64;
    std::uint_fast64_t decrypted_file_size = 0;
    if (encryption_key) {
        std::uint_fast64_t file_size = file.get_size();
        REALM_ASSERT(file_size % encryption_block_size == 0);
        std::uint_fast64_t number_of_metadata_blocks =
            (file_size / encryption_block_size + blocks_per_metadata_block) / (blocks_per_metadata_block + 1);
        decrypted_file_size = file_size - number_of_metadata_blocks * encryption_block_size;
    }
#endif


    z_stream strm;
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.data_type = Z_BINARY;

    int rc = inflateInit(&strm);
    if (rc == Z_MEM_ERROR)
        return compression::error::out_of_memory;
    if (rc != Z_OK)
        return compression::error::decompress_error;

    std::size_t ndx = 0;
    while (ndx < blocks_size) {
        if (ndx + 4 > blocks_size) {
            inflateEnd(&strm);
            return error::missing_block_header;
        }

        uInt block_size = 0;
        for (int i = 0; i < 4; ++i) {
            block_size <<= 8;
            block_size += std::uint8_t(blocks[ndx]);
            ndx++;
        }
        if (ndx + block_size > blocks_size) {
            inflateEnd(&strm);
            return error::invalid_block_size;
        }

        inflateReset(&strm);
        strm.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(blocks + ndx));
        strm.avail_in = uInt(block_size);

        strm.next_out = reinterpret_cast<Bytef*>(buf.get());
        strm.avail_out = buf_size;

        int flush = Z_FINISH;
        rc = inflate(&strm, flush);

        if (rc != Z_STREAM_END) {
            inflateEnd(&strm);
            if (rc == Z_MEM_ERROR)
                return compression::error::out_of_memory;
            return compression::error::corrupt_input;
        }

        std::size_t decompressed_size = buf_size - strm.avail_out;

#if REALM_ENABLE_ENCRYPTION
        if (encryption_key) {
            REALM_ASSERT(aes_cryptor);
            std::size_t next_decrypted_file_size = std::size_t(decrypted_file_size + decompressed_size);
            aes_cryptor->set_file_size(off_t(next_decrypted_file_size));
            aes_cryptor->write(file.get_descriptor(), off_t(decrypted_file_size), buf.get(), decompressed_size);
            decrypted_file_size = next_decrypted_file_size;
        }
#endif

        if (!encryption_key) {
            file.write(buf.get(), decompressed_size);
        }
        ndx += block_size;
    }

    inflateEnd(&strm);
    dst_size = file.get_size();

    return std::error_code{};
}

std::error_code compression::compress_file_in_blocks(const char* src_path, const char* dst_path, size_t& src_size,
                                                     size_t& dst_size)
{
    util::File src_file;
    try {
        src_file.open(src_path);
    }
    catch (util::File::AccessError&) {
        return compression::error::source_file_is_not_readable;
    }
    src_size = size_t(src_file.get_size());

    util::File dst_file;
    try {
        dst_file.open(dst_path, util::File::mode_Write);
    }
    catch (util::File::AccessError&) {
        return compression::error::destination_path_is_not_writable;
    }

    _impl::compression::CompressMemoryArena allocator;

    // The allocator supplied to zlib allocates at most 8 MB. Observations show
    // that zlib asks for less than 300 KB. There is no reason that 8 MB should
    // not be enough to compress 256 KB. We return with an out of memory error
    // if this is not enough, instead of allocating more memory.
    const std::size_t memory_usage = 1 << 23;
    allocator.resize(memory_usage);

    const std::size_t in_buf_size = 1 << 18; // 256 KB
    std::unique_ptr<char[]> in_buf = std::make_unique<char[]>(in_buf_size);

    const std::size_t out_buf_size = 1 << 20; // 1 MB
    std::unique_ptr<char[]> out_buf = std::make_unique<char[]>(out_buf_size);

    z_stream strm;
    strm.zalloc = &custom_alloc;
    strm.zfree = &custom_free;
    strm.opaque = &allocator;
    strm.data_type = Z_BINARY;

    int compression_level = Z_DEFAULT_COMPRESSION;
    int rc = deflateInit(&strm, compression_level);
    if (rc == Z_MEM_ERROR)
        return compression::error::out_of_memory;
    if (rc != Z_OK)
        return compression::error::compress_error;

    bool eof = false;
    while (!eof) {
        rc = deflateReset(&strm);
        if (rc != Z_OK)
            return compression::error::compress_error;

        std::size_t nread = src_file.read(in_buf.get(), in_buf_size);
        if (nread < in_buf_size)
            eof = true;
        if (nread == 0) {
            rc = deflateEnd(&strm);
            if (rc != Z_OK)
                return compression::error::compress_error;
            break;
        }
        strm.next_in = reinterpret_cast<Bytef*>(in_buf.get());
        strm.avail_in = uInt(nread);

        // Make space for the 4 byte prefix.
        strm.next_out = reinterpret_cast<Bytef*>(out_buf.get() + 4);
        strm.avail_out = out_buf_size - 4;

        int flush = Z_FINISH;
        rc = deflate(&strm, flush);
        if (rc != Z_STREAM_END) {
            deflateEnd(&strm);
            if (rc == Z_MEM_ERROR)
                return compression::error::out_of_memory;
            return compression::error::compress_error;
        }
        std::size_t compressed_size = out_buf_size - 4 - strm.avail_out;

        // Make prefix
        std::size_t prefix = compressed_size;
        for (int i = 3; i >= 0; --i) {
            out_buf[i] = static_cast<unsigned char>(prefix & 255);
            prefix >>= 8;
        }

        dst_file.write(out_buf.get(), compressed_size + 4);
    }
    REALM_ASSERT(eof);

    deflateEnd(&strm);

    dst_size = size_t(dst_file.get_size());

    return std::error_code{};
}

std::error_code compression::decompress_file_from_blocks(const char* src_path, const char* dst_path,
                                                         util::File::SizeType& src_size,
                                                         util::File::SizeType& dst_size)
{
    util::File src_file;
    try {
        src_file.open(src_path);
    }
    catch (util::File::AccessError&) {
        return compression::error::source_file_is_not_readable;
    }
    src_size = src_file.get_size();

    util::File dst_file;
    try {
        dst_file.open(dst_path, util::File::mode_Write);
    }
    catch (util::File::AccessError&) {
        return compression::error::destination_path_is_not_writable;
    }

    _impl::compression::CompressMemoryArena allocator;

    // The allocator supplied to zlib allocates at most 8 MB. Observations show
    // that zlib asks for less than 300 KB. There is no reason that 8 MB should
    // not be enough to compress 256 KB. We return with an out of memory error
    // if this is not enough, instead of allocating more memory.
    const std::size_t memory_usage = 1 << 23;
    allocator.resize(memory_usage);

    unsigned char prefix[4];

    const std::size_t in_buf_size = 1 << 20;
    std::unique_ptr<char[]> in_buf = std::make_unique<char[]>(in_buf_size);

    const std::size_t out_buf_size = 1 << 18;
    std::unique_ptr<char[]> out_buf = std::make_unique<char[]>(out_buf_size);

    z_stream strm;
    strm.zalloc = &custom_alloc;
    strm.zfree = &custom_free;
    strm.opaque = &allocator;
    strm.data_type = Z_BINARY;

    int rc = inflateInit(&strm);
    if (rc == Z_MEM_ERROR)
        return compression::error::out_of_memory;
    if (rc != Z_OK)
        return compression::error::decompress_error;

    while (true) {

        inflateReset(&strm);

        std::size_t nread = src_file.read(reinterpret_cast<char*>(prefix), 4);
        if (nread == 0) {
            inflateEnd(&strm);
            break;
        }
        if (nread < 4) {
            inflateEnd(&strm);
            return compression::error::corrupt_input;
        }
        uInt block_size = 0;
        for (int i = 0; i < 4; ++i) {
            block_size <<= 8;
            block_size += std::uint8_t(prefix[i]);
        }
        if (block_size > in_buf_size) {
            inflateEnd(&strm);
            return compression::error::corrupt_input;
        }
        nread = src_file.read(in_buf.get(), block_size);
        if (nread < block_size) {
            inflateEnd(&strm);
            return compression::error::corrupt_input;
        }

        strm.next_in = reinterpret_cast<Bytef*>(in_buf.get());
        strm.avail_in = uInt(nread);

        // Make space for the 4 byte prefix.
        strm.next_out = reinterpret_cast<Bytef*>(out_buf.get());
        strm.avail_out = out_buf_size;

        int flush = Z_FINISH;
        rc = inflate(&strm, flush);
        if (rc != Z_STREAM_END) {
            inflateEnd(&strm);
            if (rc == Z_MEM_ERROR)
                return compression::error::out_of_memory;
            return compression::error::corrupt_input;
        }

        std::size_t decompressed_size = out_buf_size - strm.avail_out;
        dst_file.write(out_buf.get(), decompressed_size);
    }

    inflateEnd(&strm);

    dst_size = dst_file.get_size();

    return std::error_code{};
}

std::error_code compression::decompress_block(const char* compressed_buf, std::size_t compressed_size,
                                              char* decompressed_buf, std::size_t& decompressed_size)
{
    z_stream strm;
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.data_type = Z_BINARY;

    int rc = inflateInit(&strm);
    if (rc == Z_MEM_ERROR)
        return compression::error::out_of_memory;
    if (rc != Z_OK)
        return compression::error::decompress_error;

    REALM_ASSERT_RELEASE(compressed_size <= std::numeric_limits<uInt>::max());

    strm.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(compressed_buf));
    strm.avail_in = uInt(compressed_size);

    // Guaranteed by caller.
    const std::size_t out_buf_size = 1 << 18;

    strm.next_out = reinterpret_cast<Bytef*>(decompressed_buf);
    strm.avail_out = out_buf_size;

    int flush = Z_FINISH;
    rc = inflate(&strm, flush);
    inflateEnd(&strm);

    if (rc != Z_STREAM_END) {
        if (rc == Z_MEM_ERROR)
            return compression::error::out_of_memory;
        return compression::error::corrupt_input;
    }

    decompressed_size = out_buf_size - strm.avail_out;

    return std::error_code{};
}

std::error_code compression::extract_blocks_from_file(const std::string& path,
                                                      const util::Optional<std::array<char, 64>>& encryption_key,
                                                      std::uint_fast64_t current_offset,
                                                      std::uint_fast64_t& next_offset, std::uint_fast64_t& max_offset,
                                                      char* buf, std::size_t buf_size, std::size_t& blocks_size)
{
    if (encryption_key) {
#if REALM_ENABLE_ENCRYPTION
        return extract_blocks_from_encrypted_realm(path, *encryption_key, current_offset, next_offset, max_offset,
                                                   buf, buf_size, blocks_size);
#endif
    }
    else {
        return extract_blocks_from_unencrypted_block_file(path, current_offset, next_offset, max_offset, buf,
                                                          buf_size, blocks_size);
    }
}

std::error_code compression::extract_blocks_from_unencrypted_block_file(
    const std::string& path, std::uint_fast64_t current_offset, std::uint_fast64_t& next_offset,
    std::uint_fast64_t& max_offset, char* buf, std::size_t buf_size, std::size_t& blocks_size)
{
    util::File file;
    try {
        file.open(path);
    }
    catch (util::File::AccessError&) {
        return compression::error::source_file_is_not_readable;
    }

    std::uint_fast64_t file_size = std::uint_fast64_t(file.get_size());
    max_offset = file_size;
    if (current_offset > max_offset)
        return error::invalid_input;
    if (current_offset == max_offset) {
        next_offset = max_offset;
        blocks_size = 0;
        return std::error_code{};
    }

    file.seek(current_offset);

    std::size_t blocks_size_2 = 0;
    while (current_offset + blocks_size_2 + 4 <= max_offset) {
        unsigned char prefix[4];
        std::size_t nread = file.read(reinterpret_cast<char*>(prefix), 4);
        REALM_ASSERT(nread == 4);
        std::size_t block_size = 0;
        for (int i = 0; i < 4; ++i) {
            block_size <<= 8;
            block_size += std::size_t(prefix[i]);
        }
        if (current_offset + blocks_size_2 + 4 + block_size > max_offset)
            return error::corrupt_input;

        if (blocks_size_2 + 4 + block_size > buf_size)
            break;

        std::memcpy(buf + blocks_size_2, prefix, 4);
        nread = file.read(buf + blocks_size_2 + 4, block_size);
        REALM_ASSERT(nread == block_size);

        blocks_size_2 += 4 + block_size;
    }

    blocks_size = blocks_size_2;
    next_offset = current_offset + blocks_size_2;

    return std::error_code{};
}

#if REALM_ENABLE_ENCRYPTION

std::error_code compression::extract_blocks_from_encrypted_realm(const std::string& path,
                                                                 const std::array<char, 64>& encryption_key,
                                                                 std::uint_fast64_t current_offset,
                                                                 std::uint_fast64_t& next_offset,
                                                                 std::uint_fast64_t& max_offset, char* buf,
                                                                 std::size_t buf_size, std::size_t& blocks_size)
{
    // More blocks will only be compressed as long as the buffer has more space
    // than threshold_buf_size left.
    const std::size_t threshold_buf_size = (1 << 19);
    REALM_ASSERT(buf_size >= threshold_buf_size);

    util::File file;
    try {
        file.open(path);
    }
    catch (util::File::AccessError&) {
        return compression::error::source_file_is_not_readable;
    }

    std::uint_fast64_t file_size = std::uint_fast64_t(file.get_size());

    // Constants from the encryption format.
    const std::size_t encryption_block_size = 4096;
    const std::size_t blocks_per_metadata_block = 64;
    REALM_ASSERT(file_size % encryption_block_size == 0);

    bool file_ends_with_metadata_block = (file_size / encryption_block_size) % (blocks_per_metadata_block + 1) == 1;

    // Ignore a final useless metadata block.
    std::uint_fast64_t effective_file_size = file_size - (file_ends_with_metadata_block ? encryption_block_size : 0);

    const std::uint_fast64_t number_of_metadata_blocks =
        (effective_file_size / encryption_block_size + blocks_per_metadata_block) / (blocks_per_metadata_block + 1);
    REALM_ASSERT(number_of_metadata_blocks > 0);

    // The offset is a position in the encrypted Realm. The offset is always placed at the
    // beginning of a metadata block, except for max_offset. max_offset is the effective file
    // size.
    max_offset = effective_file_size;

    if (current_offset > max_offset)
        return error::invalid_input;
    if (current_offset == max_offset) {
        next_offset = max_offset;
        blocks_size = 0;
        return std::error_code{};
    }

    if (current_offset % (encryption_block_size * (blocks_per_metadata_block + 1)) != 0)
        return compression::error::invalid_input;

    const std::uint_fast64_t decrypted_src_size =
        effective_file_size - number_of_metadata_blocks * encryption_block_size;
    util::AESCryptor aes_cryptor{reinterpret_cast<const unsigned char*>(encryption_key.data())};
    aes_cryptor.set_file_size(off_t(decrypted_src_size));

    const std::size_t unencrypted_buf_size = blocks_per_metadata_block * encryption_block_size;
    std::unique_ptr<char[]> unencrypted_buf{new char[unencrypted_buf_size]};

    std::size_t buf_pos = 0;
    std::uint_fast64_t offset = current_offset;

    while (offset < max_offset && (buf_size - buf_pos) >= threshold_buf_size) {
        std::uint_fast64_t decrypted_offset = (offset / (blocks_per_metadata_block + 1)) * blocks_per_metadata_block;
        REALM_ASSERT(decrypted_offset < decrypted_src_size);
        std::size_t size_to_read =
            std::size_t(std::min(std::uint_fast64_t(blocks_per_metadata_block * encryption_block_size),
                                 decrypted_src_size - decrypted_offset));
        REALM_ASSERT(size_to_read % encryption_block_size == 0);
        REALM_ASSERT(decrypted_src_size % encryption_block_size == 0);
        REALM_ASSERT(decrypted_offset % encryption_block_size == 0);
        // We loop over all individual encryption blocks because aes_cryptor.read() returns false
        // for uninitialized blocks. Those blocks are not used by any Realm data structures but they
        // must be included in the file as well.
        for (std::size_t pos = 0; pos < size_to_read; pos += encryption_block_size) {
            bool success = aes_cryptor.read(file.get_descriptor(), off_t(decrypted_offset + pos),
                                            unencrypted_buf.get() + pos, encryption_block_size);
            if (!success) {
                // zero out the content.
                std::memset(unencrypted_buf.get() + pos, 0, encryption_block_size);
            }

            // The logic here is strange, because we capture uninitialized blocks, but blocks that
            // fail authentication are accepted. We rely on the server's files not being tampered with.
            // The consequence of an unauthenticated Realm is that the client will end up with
            // zero blocks. This is acceptable, but not ideal. The situation could be remedied by
            // introducing a slightly different read function than aes_cryptor.read.
        }

        std::size_t compressed_size_3;
        std::error_code ec = compress_block_with_header(unencrypted_buf.get(), size_to_read, buf + buf_pos,
                                                        buf_size - buf_pos, compressed_size_3);
        if (ec)
            return ec;

        REALM_ASSERT(buf_pos + compressed_size_3 <= buf_size);
        buf_pos += compressed_size_3;
        offset += size_to_read + encryption_block_size;
    }

    blocks_size = buf_pos;
    next_offset = offset;
    REALM_ASSERT(next_offset > current_offset);
    REALM_ASSERT(next_offset <= max_offset);

    return std::error_code{};
}

#endif
