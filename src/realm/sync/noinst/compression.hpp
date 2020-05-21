
#ifndef REALM_NOINST_COMPRESSION_HPP
#define REALM_NOINST_COMPRESSION_HPP

#include <system_error>
#include <vector>
#include <string>
#include <stdint.h>
#include <stddef.h>
#include <memory>
#include <array>

#include <realm/binary_data.hpp>
#include <realm/util/file.hpp>
#include <realm/util/optional.hpp>

namespace realm {
namespace _impl {
namespace compression {

enum class error {
    out_of_memory = 1,
    compress_buffer_too_small = 2,
    compress_error = 3,
    corrupt_input = 4,
    incorrect_decompressed_size = 5,
    decompress_error = 6,
    source_file_is_not_readable = 7,
    destination_path_is_not_writable = 8,
    invalid_input = 9,
    decryption_error = 10,
    missing_block_header = 11,
    invalid_block_size = 12,
};

const std::error_category& error_category() noexcept;

std::error_code make_error_code(error) noexcept;

} // namespace compression
} // namespace _impl
} // namespace realm

namespace std {

template <>
struct is_error_code_enum<realm::_impl::compression::error> {
    static const bool value = true;
};

} // namespace std

namespace realm {
namespace _impl {
namespace compression {

class Alloc {
public:
    // Returns null on "out of memory"
    virtual void* alloc(size_t size) = 0;
    virtual void free(void* addr) noexcept = 0;
    virtual ~Alloc() {}
};

class CompressMemoryArena : public Alloc {
public:
    void* alloc(size_t size) override final
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
/// compress(). \a uncompressed_buf is the buffer with uncompressed data. The
/// size of the uncompressed data is \a uncompressed_size. \a compression_level
/// is described under compress(). \a bound is set to the upper bound at
/// return. The returned error code is of category compression::error_category.
std::error_code compress_bound(const char* uncompressed_buf, size_t uncompressed_size, size_t& bound,
                               int compression_level = 1);

/// compress() compresses the data in the \a uncompressed_buf of size \a
/// uncompressed_size into \a compressed_buf. compress() resizes \a
/// compressed_buf. At return, \a compressed_buf has the size of the compressed
/// data. \a compression_level is [1-9] with 1 the fastest for the current zlib
/// implementation. The returned error code is of category
/// compression::error_category.
std::error_code compress(const char* uncompressed_buf, size_t uncompressed_size, char* compressed_buf,
                         size_t compressed_buf_size, size_t& compressed_size, int compression_level = 1,
                         Alloc* custom_allocator = nullptr);

/// decompress() decompresses the data in \param compressed_buf of size \a
/// compresed_size into \a decompressed_buf. \a decompressed_size is the
/// expected size of the decompressed data. \a decompressed_buf must have size
/// at least \a decompressed_size. decompress() throws on errors, including the
/// error where the size of the decompressed data is unequal to
/// decompressed_size.  The returned error code is of category
/// compression::error_category.
std::error_code decompress(const char* compressed_buf, size_t compressed_size, char* decompressed_buf,
                           size_t decompressed_size);


size_t allocate_and_compress(CompressMemoryArena& compress_memory_arena, BinaryData uncompressed_buf,
                             std::vector<char>& compressed_buf);

/// compress_file() compresses the file at path \a src_path into \a dst_path.
/// The function returns {} on success and returns an error if the source file
/// is not readable, if the destination file is not writable,
/// if memory is low, or the compression library has internal errors. The out
/// variables src_size and dst_size return the file sizes.
std::error_code compress_file(const std::string& src_path, const std::string& dst_path,
                              util::File::SizeType& src_size, util::File::SizeType& dst_size);

/// decompress_file() has the same semantics as compress_file() except that it
/// decompresses.
std::error_code decompress_file(const std::string& src_path, const std::string& dst_path,
                                util::File::SizeType& src_size, util::File::SizeType& dst_size);

// compress_block_with_header() takes the input in 'uncompressed_buf' of size
// 'uncompressed_size' and compresses it into 'compressed_buf' and prepends a
// 4-byte prefix representing the size of the compressed data excluding the
// 4-byte header. The size of 'compressed_buf' is supplied in the argument
// 'compressed_buf_size' and it must be large enough to hold the result. The
// output argument 'compressed_size' denotes the size of the entire compressed
// block including the 4-byte header.
std::error_code compress_block_with_header(const char* uncompressed_buf, size_t uncompressed_size,
                                           char* compressed_buf, size_t compressed_buf_size, size_t& compressed_size);

// integrate_compressed_blocks_in_realm_file() takes the input in the buffer
// 'blocks' of size 'blocks_size', parses the block headers, decompresses the
// blocks, and integrates the result in the destination file with path
// 'dst_path'.
//
// The purpose of the function is to build a Realm file piece by piece from
// downloaded blocks. When this function has been called for all downloaded
// blocks, a full Realm file should have been created.
//
// If 'encryption_key' is set, the Realm file will be encrypted.
//
// The size of the destination file is placed in 'dst_size'.
//
// On failure, the function returns a compression error code.
std::error_code integrate_compressed_blocks_in_realm_file(const char* blocks, size_t blocks_size,
                                                          const std::string& dst_path,
                                                          const util::Optional<std::array<char, 64>>& encryption_key,
                                                          uint_fast64_t& dst_size);

/// compress_file_in_blocks() reads the source file in blocks of 256 KB,
/// compresses each block independently, and writes the compressed block into
/// the destination file with a size prefix. The prefix is 4 bytes and
/// represents the size of the compressed block in network byte order.
///
/// The purpose of splitting the compressed file into blocks is to be able to
/// download it in chunks and have the receiver decompress it on the fly. The size of
/// 256 KB is chosen to make it simple to encrypt the decompressed blocks on the
/// receiving side.
///
/// The arguments and return value have the same meaning as in compress_file().
std::error_code compress_file_in_blocks(const char* src_path, const char* dst_path, size_t& src_size,
                                        size_t& dst_size);

// decompress_file_from_blocks performs the inverse operation of compress_file_in_blocks().
// The arguments have the same meaning as in compress_file_in_blocks().
std::error_code decompress_file_from_blocks(const char* src_path, const char* dst_path,
                                            util::File::SizeType& src_size, util::File::SizeType& dst_size);

// decompress_block() decompresses a single compressed block. The compressed
// data is in compressed_buf of size compressed_size. The decompressed data is
// at most the block size, i.e. 256 KB. decompressed_buf should have size at
// least 256 KB.  The size of the decompressed data is stored in the output
// variable decompressed_size.
// The return value is {} if the decompression succeeds and an appropriate
// error code, of class compression::error, in case of errors.
std::error_code decompress_block(const char* compressed_buf, size_t compressed_size, char* decompressed_buf,
                                 size_t& decompressed_size);


// extract_blocks_from_unencrypted_realm() and
// extract_blocks_from_encrypted_realm() compute a number of
// compressed blocks from the file in 'path' and places the result in
// 'buf' and its size in 'blocks_size'. The size of the input buffer 'buf' is
// supplied in 'buf_size'.
//
// The argument 'current_offset' indicates the position in the file where the
// fetched blocks starts. The function output argument 'next_offset' must be
// used in the next call of the function to fetch the next blocks. 'max_offset'
// is set to the maximal offset. When 'next_offset' is equal to 'max_offset',
// no more data is available. The function will attempt to place more blocks
// in the output buffer as long as it knows that there is enough space. However,
// to avoid unnecessary computations, the function will not compress data
// unless it is certain that the compressed data will fit into the buffer.
//
// It is recommended to set 'buf_size' to at least 512 KB in which case it is
// guaranteed that some data will be returned.
//
// The purpose of this function is to fetch compressed unencrypted data on
// demand without ever persisting any unencrypted data. The server can request
// more unencrypted, compressed block data and send it in a STATE message. The
// format of the block compressed data is identical to the format used for an
// unencrypted Realm.
//
// For the unencrypted case, the file in 'path' has been obtained as the output of
// the function compress_file_in_blocks(). The blocks are precompressed in the
// unencrypted case.
//
// For the encrypted case, the file in 'path' is an encrypted Realm whose encryption key
// must be supplied in 'encryption_key'. The blocks are compressed on the fly in the
// encrypted case.

std::error_code extract_blocks_from_file(const std::string& path,
                                         const util::Optional<std::array<char, 64>>& encryption_key,
                                         uint_fast64_t current_offset, uint_fast64_t& next_offset,
                                         uint_fast64_t& max_offset, char* buf, size_t buf_size, size_t& blocks_size);


std::error_code extract_blocks_from_unencrypted_block_file(const std::string& path, uint_fast64_t current_offset,
                                                           uint_fast64_t& next_offset, uint_fast64_t& max_offset,
                                                           char* buf, size_t buf_size, size_t& blocks_size);
#if REALM_ENABLE_ENCRYPTION
std::error_code extract_blocks_from_encrypted_realm(const std::string& path,
                                                    const std::array<char, 64>& encryption_key,
                                                    uint_fast64_t current_offset, uint_fast64_t& next_offset,
                                                    uint_fast64_t& max_offset, char* buf, size_t buf_size,
                                                    size_t& blocks_size);
#endif

} // namespace compression
} // namespace _impl
} // namespace realm

#endif // REALM_NOINST_COMPRESSION_HPP
