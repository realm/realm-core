#include "test.hpp"
#include "util/random.hpp"

#include <realm/util/buffer.hpp>
#include <realm/util/compression.hpp>

#include <algorithm>
#include <cstring>

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;

namespace {

// Generate data that is highly compressible.
Buffer<char> generate_compressible_data(size_t size)
{
    const char atom[] = "Some unimportant text that can be concatenated multiple times.\n";
    size_t atom_size = sizeof(atom); // Including the terminal '\0'.

    Buffer<char> content(size);
    size_t position = 0;
    while (position < size) {
        size_t copy_size = std::min(atom_size, size - position);
        std::memcpy(content.data() + position, atom, copy_size);
        position += copy_size;
    }
    return content;
}

// Generate data that is not compressible.
Buffer<char> generate_non_compressible_data(size_t size)
{
    // Generating random data using the RNG's native size is dramatically faster
    // than generating individual bytes.
    using uint = std::mt19937::result_type;
    auto rounded_size = (size + sizeof(uint) - 1) / sizeof(uint);
    Buffer<uint> content(rounded_size);
    test_util::Random random(test_util::produce_nondeterministic_random_seed());
    random.draw_ints<uint>(content.data(), rounded_size);

    Buffer<char> result(size);
    memcpy(result.data(), content.data(), size);
    return result;
}

// Compress, decompress and verify equality.
void compress_decompress_compare(test_util::unit_test::TestContext& test_context, Span<const char> uncompressed_buf)
{
    size_t compressed_buf_size = compression::compress_bound(uncompressed_buf.size());
    CHECK(compressed_buf_size);
    if (!compressed_buf_size)
        return;

    Buffer<char> compressed_buf(compressed_buf_size);
    size_t compressed_size;
    int compression_level = 5;

    auto ec = compression::compress(uncompressed_buf, compressed_buf, compressed_size, compression_level);
    CHECK_NOT(ec);
    if (ec)
        return;

    Buffer<char> decompressed_buf(uncompressed_buf.size());
    ec = compression::decompress(Span(compressed_buf).first(compressed_size), decompressed_buf);
    CHECK_NOT(ec);
    if (ec)
        return;

    CHECK(std::equal(uncompressed_buf.begin(), uncompressed_buf.end(), decompressed_buf.data(),
                     decompressed_buf.data() + decompressed_buf.size()));
}

void allocate_and_compress_decompress_compare(test_util::unit_test::TestContext& test_context,
                                              Span<const char> uncompressed_buf)
{
    std::vector<char> compressed_buf;

    compression::CompressMemoryArena compress_memory_arena;

    compression::allocate_and_compress(compress_memory_arena, uncompressed_buf, compressed_buf);

    Buffer<char> decompressed_buf(uncompressed_buf.size());
    std::error_code ec = compression::decompress(compressed_buf, decompressed_buf);
    CHECK_NOT(ec);

    CHECK(std::equal(uncompressed_buf.begin(), uncompressed_buf.end(), decompressed_buf.data(),
                     decompressed_buf.data() + decompressed_buf.size()));
}

} // anonymous namespace


TEST(Compression_Compress_Buffer_Too_Small)
{
    size_t uncompressed_size = 10000;
    auto uncompressed_buf = generate_non_compressible_data(uncompressed_size);

    size_t compressed_buf_size = 1000;
    Buffer<char> compressed_buf(compressed_buf_size);

    size_t compressed_size;
    int compression_level = 1;

    std::error_code ec = compression::compress(uncompressed_buf, compressed_buf, compressed_size, compression_level);
    CHECK_EQUAL(ec, compression::error::compress_buffer_too_small);
}

TEST(Compression_Decompress_Incorrect_Size)
{
    size_t uncompressed_size = 10000;
    auto uncompressed_buf = generate_compressible_data(uncompressed_size);

    size_t compressed_buf_size = 10000;
    Buffer<char> compressed_buf(compressed_buf_size);

    size_t compressed_size;
    int compression_level = 5;

    std::error_code ec = compression::compress(uncompressed_buf, compressed_buf, compressed_size, compression_level);
    CHECK_NOT(ec);

    // Libcompression always says it used the entire output buffer
#if !REALM_USE_LIBCOMPRESSION
    size_t decompressed_size = 5000; // incorrect
    Buffer<char> decompressed_buf(decompressed_buf_size);

    ec = compression::decompress(compressed_buf, decompressed_buf);
    CHECK_EQUAL(ec, compression::error::incorrect_decompressed_size);
#endif
}

// This unit test compresses and decompresses data that is highly compressible.
// Multiple sizes of the uncompressed data are tested.
TEST(Compression_Compressible_Data_Small)
{
    size_t uncompressed_sizes[] = {
        0, 1, 2, 256, 1 << 10, 1 << 20,
    };
    for (size_t uncompressed_size : uncompressed_sizes) {
        compress_decompress_compare(test_context, generate_compressible_data(size_t(uncompressed_size)));
    }
}

// This unit test compresses and decompresses data that is highly compressible. Multiple large sizes of the
// uncompressed data are tested including sizes above 4GB.
TEST_IF(Compression_Compressible_Data_Large, false)
{
    uint64_t uncompressed_sizes[] = {(uint64_t(1) << 32) - 1, (uint64_t(1) << 32) + 500, uint64_t(1) << 33};
    for (size_t uncompressed_size : uncompressed_sizes) {
        compress_decompress_compare(test_context, generate_compressible_data(size_t(uncompressed_size)));
    }
}

// This unit test compresses and decompresses data that is hard to compress.
// Multiple small sizes of the uncompressed data are tested.
TEST(Compression_Non_Compressible_Data_Small)
{
    size_t uncompressed_sizes[] = {0, 1, 1 << 10, 1 << 20};
    for (size_t uncompressed_size : uncompressed_sizes) {
        compress_decompress_compare(test_context, generate_non_compressible_data(uncompressed_size));
    }
}

// This unit test compresses and decompresses data that is hard to compress.
// Multiple large sizes of the uncompressed data are tested including sizes
// above 4GB.
TEST_IF(Compression_Non_Compressible_Data_Large, false)
{
    uint64_t uncompressed_sizes[] = {(uint64_t(1) << 32) - 1, (uint64_t(1) << 32) + 100};
    for (size_t uncompressed_size : uncompressed_sizes) {
        compress_decompress_compare(test_context, generate_non_compressible_data(size_t(uncompressed_size)));
    }
}

// This test checks the allocate_and_compress wrapper around the compression function for a data set of size
// way below the 4GB limit.
TEST(Compression_Allocate_And_Compress_Small)
{
    size_t uncompressed_size = size_t(1) << 20;
    allocate_and_compress_decompress_compare(test_context, generate_compressible_data(uncompressed_size));
}

// This test checks the allocate_and_compress wrapper around the compression
// function for data of size larger than 4GB.
TEST_IF(Compression_Allocate_And_Compress_Large, false)
{
    uint64_t uncompressed_size = (uint64_t(1) << 32) + 100;
    allocate_and_compress_decompress_compare(test_context, generate_compressible_data(size_t(uncompressed_size)));
}
