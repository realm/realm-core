#include "test.hpp"
#include "util/random.hpp"
#include "util/compare_groups.hpp"
#include "util/crypt_key.hpp"

#include <realm/sync/noinst/compression.hpp>
#include <realm/sync/history.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>

#include <algorithm>
#include <cstring>

using namespace realm;
using namespace realm::util;
using namespace realm::sync;
using namespace realm::_impl;
using namespace realm::test_util;

namespace {

// Generate data that is highly compressible.
std::unique_ptr<char[]> generate_compressible_data(size_t size)
{
    const char atom[] = "Some unimportant text that can be concatenated multiple times.\n";
    size_t atom_size = sizeof(atom); // Including the terminal '\0'.

    auto content = std::make_unique<char[]>(size);
    size_t position = 0;
    while (position < size) {
        size_t copy_size = std::min(atom_size, size - position);
        std::memcpy(content.get() + position, atom, copy_size);
        position += copy_size;
    }
    return content;
}

// Generate data that is not compressible.
std::unique_ptr<char[]> generate_non_compressible_data(size_t size)
{
    auto content = std::make_unique<char[]>(size);
    test_util::Random random(test_util::produce_nondeterministic_random_seed());
    random.draw_ints<char>(content.get(), size);
    return content;
}

// Compress, decompress and verify equality.
void compress_decompress_compare(test_util::unit_test::TestContext& test_context, size_t uncompressed_size,
                                 const char* uncompressed_buf)
{
    size_t bound;
    std::error_code ec = compression::compress_bound(uncompressed_buf, uncompressed_size, bound);
    CHECK_NOT(ec);

    size_t compressed_buf_size = bound;
    auto compressed_buf_unique_ptr = std::make_unique<char[]>(compressed_buf_size);
    char* compressed_buf = compressed_buf_unique_ptr.get();
    size_t compressed_size;
    int compression_level = 1;

    ec = compression::compress(uncompressed_buf, uncompressed_size, compressed_buf, compressed_buf_size,
                               compressed_size, compression_level);
    CHECK_NOT(ec);

    auto decompressed_buf_unique_ptr = std::make_unique<char[]>(uncompressed_size);
    char* decompressed_buf = decompressed_buf_unique_ptr.get();
    size_t decompressed_size = uncompressed_size;

    ec = compression::decompress(compressed_buf, compressed_size, decompressed_buf, decompressed_size);
    CHECK_NOT(ec);

    int compare = std::memcmp(uncompressed_buf, decompressed_buf, uncompressed_size);
    CHECK_EQUAL(compare, 0);
}

void allocate_and_compress_decompress_compare(test_util::unit_test::TestContext& test_context,
                                              size_t uncompressed_size, const char* uncompressed_buf)
{
    BinaryData uncompressed_bd{uncompressed_buf, uncompressed_size};
    std::vector<char> compressed_buf;

    compression::CompressMemoryArena compress_memory_arena;

    size_t compressed_size =
        compression::allocate_and_compress(compress_memory_arena, uncompressed_bd, compressed_buf);

    auto decompressed_buf_unique_ptr = std::make_unique<char[]>(uncompressed_size);
    char* decompressed_buf = decompressed_buf_unique_ptr.get();
    size_t decompressed_size = uncompressed_size;

    std::error_code ec =
        compression::decompress(compressed_buf.data(), compressed_size, decompressed_buf, decompressed_size);
    CHECK_NOT(ec);

    int compare = std::memcmp(uncompressed_buf, decompressed_buf, uncompressed_size);
    CHECK_EQUAL(compare, 0);
}

} // anonymous namespace


TEST(Compression_Compress_Buffer_Too_Small)
{
    size_t uncompressed_size = 10000;
    const std::unique_ptr<char[]> content = generate_non_compressible_data(uncompressed_size);
    const char* uncompressed_buf = content.get();

    size_t compressed_buf_size = 1000;
    auto compressed_buf_unique_ptr = std::make_unique<char[]>(compressed_buf_size);
    char* compressed_buf = compressed_buf_unique_ptr.get();

    size_t compressed_size;
    int compression_level = 1;

    std::error_code ec = compression::compress(uncompressed_buf, uncompressed_size, compressed_buf,
                                               compressed_buf_size, compressed_size, compression_level);
    CHECK_EQUAL(ec, compression::error::compress_buffer_too_small);
}

TEST(Compression_Decompress_Incorrect_Size)
{
    size_t uncompressed_size = 10000;
    const std::unique_ptr<char[]> content = generate_compressible_data(uncompressed_size);
    const char* uncompressed_buf = content.get();

    size_t compressed_buf_size = 10000;
    auto compressed_buf_unique_ptr = std::make_unique<char[]>(compressed_buf_size);
    char* compressed_buf = compressed_buf_unique_ptr.get();

    size_t compressed_size;
    int compression_level = 5;

    std::error_code ec = compression::compress(uncompressed_buf, uncompressed_size, compressed_buf,
                                               compressed_buf_size, compressed_size, compression_level);
    CHECK_NOT(ec);

    size_t decompressed_size = 5000; // incorrect
    auto decompressed_buf_unique_ptr = std::make_unique<char[]>(decompressed_size);
    char* decompressed_buf = decompressed_buf_unique_ptr.get();

    ec = compression::decompress(compressed_buf, compressed_size, decompressed_buf, decompressed_size);
    CHECK_EQUAL(ec, compression::error::incorrect_decompressed_size);
}

// This unit test compresses and decompresses data that is highly compressible.
// Multiple sizes of the uncompressed data are tested.
TEST(Compression_Compressible_Data_Small)
{
    size_t uncompressed_sizes[] = {
        0, 1, 2, 256, 1 << 10, 1 << 20,
    };
    size_t num_sizes = sizeof(uncompressed_sizes) / sizeof(uncompressed_sizes[0]);

    for (size_t i = 0; i < num_sizes; ++i) {
        size_t uncompressed_size = uncompressed_sizes[i];

        const std::unique_ptr<char[]> content = generate_compressible_data(uncompressed_size);

        compress_decompress_compare(test_context, uncompressed_size, content.get());
    }
}

// This unit test compresses and decompresses data that is highly compressible. Multiple large sizes of the
// uncompressed data are tested including sizes above 4GB.
TEST_IF(Compression_Compressible_Data_Large, false)
{
    uint64_t uncompressed_sizes[] = {(uint64_t(1) << 32) - 1, (uint64_t(1) << 32) + 500, uint64_t(1) << 33};
    size_t num_sizes = sizeof(uncompressed_sizes) / sizeof(uncompressed_sizes[0]);

    for (size_t i = 0; i < num_sizes; ++i) {
        uint64_t uncompressed_size = uncompressed_sizes[i];

        const std::unique_ptr<char[]> content = generate_compressible_data(size_t(uncompressed_size));

        compress_decompress_compare(test_context, size_t(uncompressed_size), content.get());
    }
}

// This unit test compresses and decompresses data that is hard to compress.
// Multiple small sizes of the uncompressed data are tested.
TEST(Compression_Non_Compressible_Data_Small)
{
    size_t uncompressed_sizes[] = {0, 1, 1 << 10, 1 << 20};
    size_t num_sizes = sizeof(uncompressed_sizes) / sizeof(uncompressed_sizes[0]);

    for (size_t i = 0; i < num_sizes; ++i) {
        size_t uncompressed_size = uncompressed_sizes[i];

        const std::unique_ptr<char[]> content = generate_non_compressible_data(uncompressed_size);

        compress_decompress_compare(test_context, uncompressed_size, content.get());
    }
}

// This unit test compresses and decompresses data that is hard to compress.
// Multiple large sizes of the uncompressed data are tested including sizes
// above 4GB.
TEST_IF(Compression_Non_Compressible_Data_Large, false)
{
    uint64_t uncompressed_sizes[] = {(uint64_t(1) << 32) - 1, (uint64_t(1) << 32) + 100};
    size_t num_sizes = sizeof(uncompressed_sizes) / sizeof(uncompressed_sizes[0]);

    for (size_t i = 0; i < num_sizes; ++i) {
        uint64_t uncompressed_size = uncompressed_sizes[i];

        const std::unique_ptr<char[]> content = generate_non_compressible_data(size_t(uncompressed_size));

        compress_decompress_compare(test_context, size_t(uncompressed_size), content.get());
    }
}

// This test checks the allocate_and_compress wrapper around the compression function for a data set of size
// way below the 4GB limit.
TEST(Compression_Allocate_And_Compress_Small)
{
    size_t uncompressed_size = size_t(1) << 20;

    const std::unique_ptr<char[]> content = generate_compressible_data(uncompressed_size);

    allocate_and_compress_decompress_compare(test_context, uncompressed_size, content.get());
}

// This test checks the allocate_and_compress wrapper around the compression
// function for data of size larger than 4GB.
TEST_IF(Compression_Allocate_And_Compress_Large, false)
{
    uint64_t uncompressed_size = (uint64_t(1) << 32) + 100;

    const std::unique_ptr<char[]> content = generate_compressible_data(size_t(uncompressed_size));

    allocate_and_compress_decompress_compare(test_context, size_t(uncompressed_size), content.get());
}
