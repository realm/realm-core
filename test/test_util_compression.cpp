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
    Buffer<char> result(size);
#if REALM_PLATFORM_APPLE
    // arc4random is orders of magnitude faster than Random, so use that when we can
    arc4random_buf(result.data(), result.size());
#else
    // Generating random data using the RNG's native size is dramatically faster
    // than generating individual bytes.
    using uint = std::mt19937::result_type;
    auto rounded_size = (size + sizeof(uint) - 1) / sizeof(uint);
    Buffer<uint> content(rounded_size);
    test_util::Random random(test_util::produce_nondeterministic_random_seed());
    random.draw_ints<uint>(content.data(), rounded_size);
    memcpy(result.data(), content.data(), size);
#endif
    return result;
}

AppendBuffer<char> compress_buffer(test_util::unit_test::TestContext& test_context, Span<const char> uncompressed_buf)
{
    AppendBuffer<char> compressed_buf;
    size_t compressed_buf_size = compression::compress_bound(uncompressed_buf.size());
    CHECK(compressed_buf_size);
    if (!compressed_buf_size)
        return compressed_buf;
    compressed_buf.resize(compressed_buf_size);

    size_t compressed_size;
    int compression_level = 5;
    auto ec = compression::compress(uncompressed_buf, compressed_buf, compressed_size, compression_level);
    CHECK_NOT(ec);
    if (ec)
        return AppendBuffer<char>();
    compressed_buf.resize(compressed_size);
    return compressed_buf;
}

void compare(test_util::unit_test::TestContext& test_context, Span<const char> uncompressed,
             Span<const char> decompressed)
{
    CHECK(std::equal(uncompressed.begin(), uncompressed.end(), decompressed.begin(), decompressed.end()));
}

// Compress, decompress and verify equality.
void compress_decompress_compare(test_util::unit_test::TestContext& test_context, Span<const char> uncompressed_buf)
{
    auto compressed_buf = compress_buffer(test_context, uncompressed_buf);
    if (!compressed_buf.size())
        return;

    Buffer<char> decompressed_buf(uncompressed_buf.size());
    auto ec = compression::decompress(compressed_buf, decompressed_buf);
    CHECK_NOT(ec);
    if (!ec)
        compare(test_context, uncompressed_buf, decompressed_buf);
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
    if (!ec)
        compare(test_context, uncompressed_buf, decompressed_buf);
}


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

TEST(Compression_Decompress_Too_Small_Buffer)
{
    size_t uncompressed_size = 10000;
    auto uncompressed_buf = generate_compressible_data(uncompressed_size);
    auto compressed_buf = compress_buffer(test_context, uncompressed_buf);

    size_t decompressed_size = uncompressed_size / 2; // incorrect
    Buffer<char> decompressed_buf(decompressed_size);

    auto ec = compression::decompress(compressed_buf, decompressed_buf);
#if REALM_USE_LIBCOMPRESSION
    // There doesn't appear to be a good way to distinguish this with libcompression
    CHECK_EQUAL(ec, compression::error::corrupt_input);
#else
    CHECK_EQUAL(ec, compression::error::incorrect_decompressed_size);
#endif
}

TEST(Compression_Decompress_Too_Large_Buffer)
{
    size_t uncompressed_size = 10000;
    auto uncompressed_buf = generate_compressible_data(uncompressed_size);
    auto compressed_buf = compress_buffer(test_context, uncompressed_buf);

    size_t decompressed_size = uncompressed_size * 2; // incorrect
    Buffer<char> decompressed_buf(decompressed_size);

    auto ec = compression::decompress(compressed_buf, decompressed_buf);
    CHECK_EQUAL(ec, compression::error::incorrect_decompressed_size);
}

TEST(Compression_Decompress_Truncated_Input)
{
    size_t uncompressed_size = 10000;
    auto uncompressed_buf = generate_compressible_data(uncompressed_size);
    auto compressed_buf = compress_buffer(test_context, uncompressed_buf);

    Buffer<char> decompressed_buf(uncompressed_size);
    auto ec = compression::decompress(Span(compressed_buf.data(), compressed_buf.size() - 10), decompressed_buf);
    CHECK_EQUAL(ec, compression::error::corrupt_input);
}

TEST(Compression_Decompress_Too_Long_Input)
{
    size_t uncompressed_size = 10000;
    auto uncompressed_buf = generate_compressible_data(uncompressed_size);
    auto compressed_buf = compress_buffer(test_context, uncompressed_buf);
    compressed_buf.resize(compressed_buf.size() + 100);

    Buffer<char> decompressed_buf(uncompressed_size);
    auto ec = compression::decompress(compressed_buf, decompressed_buf);
    CHECK_EQUAL(ec, compression::error::corrupt_input);
}

TEST(Compression_Decompress_Corrupt_Input)
{
    size_t uncompressed_size = 10000;
    auto uncompressed_buf = generate_compressible_data(uncompressed_size);
    auto compressed_buf = compress_buffer(test_context, uncompressed_buf);

    // Flip a bit in the compressed data so that decompression fails
    compressed_buf.data()[compressed_buf.size() / 2] ^= 1;

    Buffer<char> decompressed_buf(uncompressed_size);
    auto ec = compression::decompress(compressed_buf, decompressed_buf);
    CHECK_EQUAL(ec, compression::error::corrupt_input);
}

// This unit test compresses and decompresses data that is highly compressible.
// Multiple sizes of the uncompressed data are tested.
TEST(Compression_Compressible_Data_Small)
{
    size_t uncompressed_sizes[] = {
        0, 1, 2, 256, 1 << 10, 1 << 20,
    };
    for (size_t uncompressed_size : uncompressed_sizes) {
        compress_decompress_compare(test_context, generate_compressible_data(uncompressed_size));
    }
}

// This unit test compresses and decompresses data that is highly compressible. Multiple large sizes of the
// uncompressed data are tested including sizes above 4GB.
TEST_IF(Compression_Compressible_Data_Large, false)
{
    uint64_t uncompressed_sizes[] = {(uint64_t(1) << 32) - 1, (uint64_t(1) << 32) + 500, uint64_t(1) << 33};
    for (uint64_t uncompressed_size : uncompressed_sizes) {
        compress_decompress_compare(test_context, generate_compressible_data(to_size_t(uncompressed_size)));
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
    for (uint64_t uncompressed_size : uncompressed_sizes) {
        compress_decompress_compare(test_context, generate_non_compressible_data(to_size_t(uncompressed_size)));
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
    allocate_and_compress_decompress_compare(test_context, generate_compressible_data(to_size_t(uncompressed_size)));
}

namespace {
struct ChunkingStream : NoCopyInputStream {
    Span<const char> input;
    size_t block_size;
    Span<const char> next_block() override
    {
        size_t n = std::min(block_size, input.size());
        auto ret = input.sub_span(0, n);
        input = input.sub_span(n);
        return ret;
    }
};

// Check a fibonacci sequence of block sizes to validate everything works
// with weirdly sized blocks. Note that the loop conditional is misleading
// and it actually does one call with block_size > uncompressed_size.
void for_each_fib_block_size(size_t size, Span<const char> input, FunctionRef<void(NoCopyInputStream&)> fn)
{
    ChunkingStream stream;
    size_t f1 = 0, f2 = 1;
    stream.block_size = 0;
    while (stream.block_size < size) {
        stream.input = input;
        stream.block_size = f1 + f2;
        f1 = f2;
        f2 = stream.block_size;
        fn(stream);
    }
}
} // anonymous namespace

TEST(Compression_Decompress_Stream_SmallBlocks)
{
    size_t uncompressed_size = 10000;
    auto uncompressed_buf = generate_compressible_data(uncompressed_size);
    auto compressed_buf = compress_buffer(test_context, uncompressed_buf);
    Buffer<char> decompressed_buf(uncompressed_size);

    for_each_fib_block_size(uncompressed_size, compressed_buf, [&](NoCopyInputStream& stream) {
        auto ec = compression::decompress(stream, decompressed_buf);
        CHECK_NOT(ec);
        compare(test_context, uncompressed_buf, decompressed_buf);
    });
}

// Verify that things work with > 4 GB blocks
TEST_IF(Compression_Decompress_Stream_LargeBlocks, false)
{
    uint64_t uncompressed_size = (uint64_t(1) << 33) + (uint64_t(1) << 32); // 12 GB
    auto uncompressed_buf = generate_non_compressible_data(to_size_t(uncompressed_size));
    auto compressed_buf = compress_buffer(test_context, uncompressed_buf);
    Buffer<char> decompressed_buf{to_size_t(uncompressed_size)};

    ChunkingStream stream;

    // Everything in one > 4 GB block
    stream.block_size = to_size_t(uint64_t(1) << 34);
    stream.input = compressed_buf;
    auto ec = compression::decompress(stream, decompressed_buf);
    CHECK_NOT(ec);
    compare(test_context, uncompressed_buf, decompressed_buf);

    // Multiple > 4 GB blocks
    stream.block_size = to_size_t((uint64_t(1) << 32) + 100);
    stream.input = compressed_buf;
    ec = compression::decompress(stream, decompressed_buf);
    CHECK_NOT(ec);
    compare(test_context, uncompressed_buf, decompressed_buf);
}

TEST(Compression_AllocateAndCompressWithHeader_Compressible)
{
    util::AppendBuffer<char> decompressed;

    {
        // Zero byte input should stay zero bytes
        auto compressed = compression::allocate_and_compress_nonportable(std::array<char, 0>());
        CHECK_EQUAL(compressed.size(), 0);

        util::SimpleNoCopyInputStream compressed_stream(compressed);
        auto ec = compression::decompress_nonportable(compressed_stream, decompressed);
        CHECK_NOT(ec);
        CHECK_EQUAL(decompressed.size(), 0);
    }

    {
        // Short data should be stored uncompressed even if it is compressible
        size_t uncompressed_size = 255;
        auto uncompressed = generate_compressible_data(uncompressed_size);
        auto compressed = compression::allocate_and_compress_nonportable(uncompressed);
        CHECK_EQUAL(compressed.size(), uncompressed.size() + 2);
        compare(test_context, uncompressed, Span(compressed).sub_span(2));

        util::SimpleNoCopyInputStream compressed_stream(compressed);
        auto ec = compression::decompress_nonportable(compressed_stream, decompressed);
        CHECK_NOT(ec);
        compare(test_context, uncompressed, decompressed);
    }

    // Longer data should actually be compressed
    size_t uncompressed_sizes[] = {(1 << 8) + 10, (1 << 16) + 10, (1 << 24) + 10};
    for (size_t uncompressed_size : uncompressed_sizes) {
        auto uncompressed = generate_compressible_data(uncompressed_size);
        auto compressed = compression::allocate_and_compress_nonportable(uncompressed);
        CHECK_LESS(compressed.size(), uncompressed.size());

        util::SimpleNoCopyInputStream compressed_stream(compressed);
        auto ec = compression::decompress_nonportable(compressed_stream, decompressed);
        CHECK_NOT(ec);
        compare(test_context, uncompressed, decompressed);
    }
}

TEST(Compression_AllocateAndCompressWithHeader_Noncompressible)
{
    util::AppendBuffer<char> decompressed;
    size_t expected_header_width = 2;
    size_t uncompressed_sizes[] = {(1 << 0) + 10, (1 << 8) + 10, (1 << 16) + 10, (1 << 24) + 10};
    for (size_t uncompressed_size : uncompressed_sizes) {
        auto uncompressed = generate_non_compressible_data(uncompressed_size);
        auto compressed = compression::allocate_and_compress_nonportable(uncompressed);

        // Should have stored uncompressed with a header added
        CHECK_EQUAL(compressed.size(), uncompressed.size() + expected_header_width);
        compare(test_context, uncompressed, Span(compressed).sub_span(expected_header_width));

        util::SimpleNoCopyInputStream compressed_stream(compressed);
        auto ec = compression::decompress_nonportable(compressed_stream, decompressed);
        CHECK_NOT(ec);
        compare(test_context, uncompressed, decompressed);

        ++expected_header_width;
    }
}

static void set_invalid_compression_algorithm(Span<char> buffer)
{
    // Set the algorithm part of the header to 255
    buffer[0] |= 0b11110000;
}

static void set_invalid_size_width(Span<char> buffer)
{
    // Set the size width to 255 bytes
    buffer[0] |= 0b1111;
}

TEST(Compression_AllocateAndCompressWithHeader_Invalid)
{
    size_t uncompressed_size = 10000;
    auto uncompressed = generate_compressible_data(uncompressed_size);
    util::AppendBuffer<char> decompressed;

    {
        auto compressed = compression::allocate_and_compress_nonportable(uncompressed);
        set_invalid_compression_algorithm(compressed);
        util::SimpleNoCopyInputStream compressed_stream(compressed);
        auto ec = compression::decompress_nonportable(compressed_stream, decompressed);
        CHECK_EQUAL(ec, compression::error::decompress_unsupported);
    }

    {
        auto compressed = compression::allocate_and_compress_nonportable(uncompressed);
        set_invalid_size_width(compressed);
        util::SimpleNoCopyInputStream compressed_stream(compressed);
        auto ec = compression::decompress_nonportable(compressed_stream, decompressed);
        CHECK_EQUAL(ec, compression::error::out_of_memory);
    }
}

static void copy_stream(Span<char> dest, NoCopyInputStream& stream)
{
    Span out = dest;
    Span<const char> block;
    while ((block = stream.next_block()), block.size()) {
        std::memcpy(out.data(), block.data(), block.size());
        out = out.sub_span(block.size());
    }
}

static void test_decompress_stream(test_util::unit_test::TestContext& test_context, Span<const char> uncompressed,
                                   Span<const char> compressed)
{
    Buffer<char> decompressed(uncompressed.size());

    for_each_fib_block_size(uncompressed.size(), compressed, [&](NoCopyInputStream& stream) {
        size_t total_size = 0;
        auto decompress_stream = compression::decompress_nonportable_input_stream(stream, total_size);
        CHECK_EQUAL(total_size, uncompressed.size());
        if (CHECK(decompress_stream)) {
            copy_stream(decompressed, *decompress_stream);
            compare(test_context, uncompressed, decompressed);
        }
    });
}

static void test_failed_compress_stream(test_util::unit_test::TestContext& test_context, Span<const char> compressed)
{
    size_t total_size;
    SimpleNoCopyInputStream stream(compressed);
    auto decompress_stream = compression::decompress_nonportable_input_stream(stream, total_size);
    CHECK_NOT(decompress_stream);
}

TEST(Compression_DecompressInputStream_UnsupportedAlgorithm)
{
    size_t uncompressed_size = 10000;
    auto uncompressed = generate_compressible_data(uncompressed_size);
    auto compressed = compression::allocate_and_compress_nonportable(uncompressed);
    set_invalid_compression_algorithm(compressed);
    test_failed_compress_stream(test_context, compressed);
}

TEST(Compression_DecompressInputStream_InvalidSize)
{
    size_t uncompressed_size = 10000;
    auto uncompressed = generate_compressible_data(uncompressed_size);
    auto compressed = compression::allocate_and_compress_nonportable(uncompressed);
    set_invalid_size_width(compressed);
    test_failed_compress_stream(test_context, compressed);
}

TEST(Compression_DecompressInputStream_Compressible_Small)
{
    size_t uncompressed_size = 10000;
    auto uncompressed = generate_compressible_data(uncompressed_size);
    auto compressed = compression::allocate_and_compress_nonportable(uncompressed);
    test_decompress_stream(test_context, uncompressed, compressed);
}

TEST_IF(Compression_DecompressInputStream_Compressible_Large, false)
{
    uint64_t uncompressed_size = (uint64_t(1) << 32) + 100;
    auto uncompressed = generate_compressible_data(to_size_t(uncompressed_size));
    auto compressed = compression::allocate_and_compress_nonportable(uncompressed);
    test_decompress_stream(test_context, uncompressed, compressed);
}

TEST(Compression_DecompressInputStream_NonCompressible_Small)
{
    size_t uncompressed_size = 10000;
    auto uncompressed = generate_non_compressible_data(uncompressed_size);
    auto compressed = compression::allocate_and_compress_nonportable(uncompressed);
    test_decompress_stream(test_context, uncompressed, compressed);
}

TEST_IF(Compression_DecompressInputStream_NonCompressible_Large, false)
{
    uint64_t uncompressed_size = (uint64_t(1) << 32) + 100;
    auto uncompressed = generate_non_compressible_data(to_size_t(uncompressed_size));
    auto compressed = compression::allocate_and_compress_nonportable(uncompressed);
    test_decompress_stream(test_context, uncompressed, compressed);
}

} // anonymous namespace
