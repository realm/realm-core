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

util::Optional<std::array<char, 64>> make_encryption_key()
{
    bool always_encrypt = true;
    std::string str = crypt_key_2(always_encrypt);
    if (str == "")
        return none;
    std::array<char, 64> key;
    std::copy(begin(str), end(str), begin(key));
    return key;
}

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

bool files_compare_equal(const std::string& path_0, const std::string& path_1)
{
    util::File file_0{path_0};
    util::File file_1{path_1};
    if (file_0.get_size() != file_1.get_size())
        return false;

    const size_t buf_size = 1 << 20;
    std::unique_ptr<char[]> buf_0 = std::make_unique<char[]>(buf_size);
    std::unique_ptr<char[]> buf_1 = std::make_unique<char[]>(buf_size);
    while (true) {
        size_t nread_0 = file_0.read(buf_0.get(), buf_size);
        size_t nread_1 = file_1.read(buf_1.get(), buf_size);
        REALM_ASSERT(nread_0 == nread_1);
        if (std::memcmp(buf_0.get(), buf_1.get(), nread_0) != 0)
            return false;
        if (nread_0 < buf_size)
            return true;
    }
}

size_t generate_repetitive_file(const std::string& path)
{
    // The file contains a number of repetitions of "0123456789" and is highly
    // compressible.
    const size_t num_repetitions = 1000000;
    const char* block = "0123456789";
    const size_t size = 10 * num_repetitions;

    {
        std::unique_ptr<char[]> buf = std::make_unique<char[]>(size);
        for (size_t i = 0; i < num_repetitions; ++i)
            memcpy(buf.get() + 10 * i, block, 10);
        util::File file{path, util::File::mode_Write};
        file.write(buf.get(), size);
        REALM_ASSERT(file.get_size() == size);
    }
    return size;
}

size_t generate_random_file(const std::string& path)
{
    // The file contains random data. The file is written in blocks to limit
    // peak memory consumption.
    const size_t num_blocks = 1 << 2;
    const size_t block_size = 1 << 20;
    const size_t size = num_blocks * block_size;

    {
        util::File file{path, util::File::mode_Write};
        test_util::Random random(test_util::produce_nondeterministic_random_seed());
        std::unique_ptr<char[]> buf = std::make_unique<char[]>(block_size);
        for (size_t i = 0; i < num_blocks; ++i) {
            random.draw_ints<char>(buf.get(), block_size);
            file.write(buf.get(), block_size);
        }
        REALM_ASSERT(file.get_size() == size);
    }
    return size;
}

// The return value is the size of the compressed file.
size_t compress_and_decompress_file(test_util::unit_test::TestContext& test_context, const std::string& path)
{
    std::string path_1 = path + ".1";
    std::string path_2 = path + ".2";

    util::File::SizeType size_0;
    util::File::SizeType size_1;
    std::error_code ec = compression::compress_file(path.c_str(), path_1.c_str(), size_0, size_1);

    CHECK_NOT(ec);
    CHECK(!files_compare_equal(path, path_1));

    util::File::SizeType size_2;
    util::File::SizeType size_3;
    ec = compression::decompress_file(path_1.c_str(), path_2.c_str(), size_2, size_3);
    CHECK_NOT(ec);
    CHECK_EQUAL(size_0, size_3);
    CHECK_EQUAL(size_1, size_2);
    CHECK(files_compare_equal(path, path_2));

    return size_t(size_1);
}

size_t compress_and_decompress_file_in_blocks(test_util::unit_test::TestContext& test_context,
                                              const std::string& path)
{
    std::string path_1 = path + ".1";
    std::string path_2 = path + ".2";

    size_t size_0;
    size_t size_1;
    std::error_code ec = compression::compress_file_in_blocks(path.c_str(), path_1.c_str(), size_0, size_1);
    CHECK_NOT(ec);
    CHECK(!files_compare_equal(path, path_1));

    util::File::SizeType size_2;
    util::File::SizeType size_3;
    ec = compression::decompress_file_from_blocks(path_1.c_str(), path_2.c_str(), size_2, size_3);
    CHECK_NOT(ec);
    CHECK_EQUAL(size_0, size_3);
    CHECK_EQUAL(size_1, size_2);
    CHECK(files_compare_equal(path, path_2));

    return size_1;
}

void make_data_in_realm(const std::string& realm_path, size_t data_size,
                        util::Optional<std::array<char, 64>> encryption_key = none)
{
    DBOptions options{encryption_key ? encryption_key->data() : nullptr};
    DBRef sg = DB::create(make_client_replication(), realm_path, options);

    WriteTransaction wt{sg};
    TableRef tr = wt.add_table("class_table");
    tr->add_column(type_Binary, "binary column");
    std::unique_ptr<char[]> data = generate_non_compressible_data(data_size);
    BinaryData bd(data.get(), data_size);
    tr->create_object().set_all(bd);
    wt.commit();
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

TEST(Compression_File_1)
{
    TEST_DIR(dir);
    std::string path = util::File::resolve("file", std::string(dir));

    size_t size = generate_repetitive_file(path);

    size_t compressed_size = compress_and_decompress_file(test_context, path);
    CHECK_LESS(compressed_size, size / 10);
}

TEST(Compression_File_2)
{
    TEST_DIR(dir);
    std::string path = util::File::resolve("file", std::string(dir));

    size_t size = generate_random_file(path);

    size_t compressed_size = compress_and_decompress_file(test_context, path);
    CHECK_GREATER(compressed_size, size / 10);
}

TEST(Compression_File_Block_1)
{
    TEST_DIR(dir);
    std::string path = util::File::resolve("file", std::string(dir));

    size_t size = generate_repetitive_file(path);

    size_t compressed_size = compress_and_decompress_file_in_blocks(test_context, path);
    CHECK_LESS(compressed_size, size / 10);
}

TEST(Compression_File_Block_2)
{
    TEST_DIR(dir);
    std::string path = util::File::resolve("file", std::string(dir));

    size_t size = generate_random_file(path);

    size_t compressed_size = compress_and_decompress_file_in_blocks(test_context, path);
    CHECK_GREATER(compressed_size, size / 10);
}

TEST(Compression_RealmBlocksSmall)
{
    SHARED_GROUP_TEST_PATH(src_path);
    SHARED_GROUP_TEST_PATH(blocks_path);
    SHARED_GROUP_TEST_PATH(unencrypted_path);
    SHARED_GROUP_TEST_PATH(encrypted_path);

    util::Optional<std::array<char, 64>> encryption_key_none = none;
    util::Optional<std::array<char, 64>> encryption_key = make_encryption_key();

    size_t data_size = 1;
    make_data_in_realm(src_path, data_size);

    size_t src_size;
    size_t blocks_size;

    std::error_code ec;

    ec = compression::compress_file_in_blocks(std::string(src_path).c_str(), std::string(blocks_path).c_str(),
                                              src_size, blocks_size);
    CHECK_NOT(ec);

    util::File blocks_file{std::string(blocks_path)};
    CHECK_EQUAL(blocks_size, blocks_file.get_size());

    std::unique_ptr<char[]> blocks{new char[blocks_size]};
    size_t nread = blocks_file.read(blocks.get(), blocks_size);
    CHECK_EQUAL(nread, blocks_size);

    uint_fast64_t dst_size;
    ec = compression::integrate_compressed_blocks_in_realm_file(
        blocks.get(), blocks_size, std::string(unencrypted_path), encryption_key_none, dst_size);
    CHECK_NOT(ec);

    ec = compression::integrate_compressed_blocks_in_realm_file(
        blocks.get(), blocks_size, std::string(encrypted_path), encryption_key, dst_size);
    CHECK_NOT(ec);

    CHECK(files_compare_equal(src_path, unencrypted_path));
    CHECK(!files_compare_equal(src_path, encrypted_path));
    {
        std::unique_ptr<ClientReplication> history_src = make_client_replication();
        DBRef sg_src = DB::create(*history_src, src_path);
        std::unique_ptr<ClientReplication> history_unencrypted = make_client_replication();
        DBRef sg_unencrypted = DB::create(*history_unencrypted, unencrypted_path);
        std::unique_ptr<ClientReplication> history_encrypted = make_client_replication();
        DBOptions options{encryption_key ? encryption_key->data() : nullptr};
        DBRef sg_encrypted = DB::create(*history_encrypted, encrypted_path, options);
        ReadTransaction rt_src{sg_src};
        ReadTransaction rt_unencrypted{sg_unencrypted};
        ReadTransaction rt_encrypted{sg_encrypted};
        CHECK(compare_groups(rt_src, rt_unencrypted));
        CHECK(compare_groups(rt_src, rt_encrypted));
    }
}

TEST(Compression_RealmBlocksLarge)
{
    SHARED_GROUP_TEST_PATH(src_path);
    SHARED_GROUP_TEST_PATH(blocks_path);
    SHARED_GROUP_TEST_PATH(unencrypted_path);
    SHARED_GROUP_TEST_PATH(encrypted_path);

    util::Optional<std::array<char, 64>> encryption_key_none = none;
    util::Optional<std::array<char, 64>> encryption_key = make_encryption_key();

    size_t data_size = 1 << 20;
    make_data_in_realm(src_path, data_size);

    size_t src_size;
    size_t blocks_size;

    std::error_code ec;

    ec = compression::compress_file_in_blocks(std::string(src_path).c_str(), std::string(blocks_path).c_str(),
                                              src_size, blocks_size);
    CHECK_NOT(ec);

    util::File blocks_file{std::string(blocks_path)};
    CHECK_EQUAL(blocks_size, blocks_file.get_size());

    std::unique_ptr<char[]> blocks{new char[blocks_size]};
    size_t nread = blocks_file.read(blocks.get(), blocks_size);
    CHECK_EQUAL(nread, blocks_size);

    uint_fast64_t dst_size;
    ec = compression::integrate_compressed_blocks_in_realm_file(
        blocks.get(), blocks_size, std::string(unencrypted_path), encryption_key_none, dst_size);
    CHECK_NOT(ec);

    ec = compression::integrate_compressed_blocks_in_realm_file(
        blocks.get(), blocks_size, std::string(encrypted_path), encryption_key, dst_size);
    CHECK_NOT(ec);

    CHECK(files_compare_equal(src_path, unencrypted_path));
    CHECK(!files_compare_equal(src_path, encrypted_path));
    {
        DBRef sg_src = DB::create(make_client_replication(), src_path);
        DBRef sg_unencrypted = DB::create(make_client_replication(), unencrypted_path);
        DBOptions options{encryption_key ? encryption_key->data() : nullptr};
        DBRef sg_encrypted = DB::create(make_client_replication(), encrypted_path, options);
        ReadTransaction rt_src{sg_src};
        ReadTransaction rt_unencrypted{sg_unencrypted};
        ReadTransaction rt_encrypted{sg_encrypted};
        CHECK(compare_groups(rt_src, rt_unencrypted));
        CHECK(compare_groups(rt_src, rt_encrypted));
    }
}

TEST(Compression_RealmBlocksUnencryptedSplit)
{
    SHARED_GROUP_TEST_PATH(src_path);
    SHARED_GROUP_TEST_PATH(blocks_path);
    SHARED_GROUP_TEST_PATH(unencrypted_path);
    SHARED_GROUP_TEST_PATH(encrypted_path);

    util::Optional<std::array<char, 64>> encryption_key_none = none;
    util::Optional<std::array<char, 64>> encryption_key = make_encryption_key();

    size_t data_size = 1 << 16;
    make_data_in_realm(src_path, data_size);

    size_t src_size;
    size_t blocks_size;

    std::error_code ec = compression::compress_file_in_blocks(
        std::string(src_path).c_str(), std::string(blocks_path).c_str(), src_size, blocks_size);

    CHECK_NOT(ec);

    util::File blocks_file{std::string(blocks_path)};
    CHECK_EQUAL(blocks_size, blocks_file.get_size());

    std::unique_ptr<char[]> blocks{new char[blocks_size]};
    size_t nread = blocks_file.read(blocks.get(), blocks_size);
    CHECK_EQUAL(nread, blocks_size);

    // Parse the headers and feed individual blocks to
    // integrate_compressed_blocks_in_realm_file().
    size_t ndx = 0;
    while (ndx < size_t(blocks_size)) {
        size_t block_size = 0;
        for (int i = 0; i < 4; ++i) {
            block_size <<= 8;
            block_size += static_cast<unsigned char>(blocks[ndx]);
            ndx++;
        }
        uint_fast64_t dst_size;
        ec = compression::integrate_compressed_blocks_in_realm_file(
            blocks.get() + ndx - 4, 4 + block_size, std::string(unencrypted_path), encryption_key_none, dst_size);
        CHECK_NOT(ec);

        ec = compression::integrate_compressed_blocks_in_realm_file(
            blocks.get() + ndx - 4, 4 + block_size, std::string(encrypted_path), encryption_key, dst_size);
        CHECK_NOT(ec);

        ndx += block_size;
    }

    CHECK(files_compare_equal(src_path, unencrypted_path));
    CHECK(!files_compare_equal(src_path, encrypted_path));
    {
        DBRef sg_src = DB::create(make_client_replication(), src_path);
        DBRef sg_unencrypted = DB::create(make_client_replication(), unencrypted_path);
        DBOptions options{encryption_key ? encryption_key->data() : nullptr};
        DBRef sg_encrypted = DB::create(make_client_replication(), encrypted_path, options);
        ReadTransaction rt_src{sg_src};
        ReadTransaction rt_unencrypted{sg_unencrypted};
        ReadTransaction rt_encrypted{sg_encrypted};
        CHECK(compare_groups(rt_src, rt_unencrypted));
        CHECK(compare_groups(rt_src, rt_encrypted));
    }
}

TEST(Compression_ExtractBlocksUnencrypted)
{
    SHARED_GROUP_TEST_PATH(src_path);
    SHARED_GROUP_TEST_PATH(blocks_path);
    SHARED_GROUP_TEST_PATH(unencrypted_path);

    util::Optional<std::array<char, 64>> encryption_key_none = none;

    size_t data_size = 1 << 20;
    make_data_in_realm(src_path, data_size);

    size_t src_size;
    size_t blocks_size;

    std::error_code ec = compression::compress_file_in_blocks(
        std::string(src_path).c_str(), std::string(blocks_path).c_str(), src_size, blocks_size);

    CHECK_NOT(ec);

    size_t buf_size = 1 << 19;
    auto buf = std::make_unique<char[]>(buf_size);
    uint_fast64_t current_offset = 0;
    while (true) {
        uint_fast64_t next_offset;
        uint_fast64_t max_offset;
        size_t blocks_size;

        ec = compression::extract_blocks_from_file(std::string(blocks_path).c_str(), none, current_offset,
                                                   next_offset, max_offset, buf.get(), buf_size, blocks_size);

        CHECK_NOT(ec);
        CHECK_GREATER(next_offset, current_offset);
        CHECK_GREATER(blocks_size, 0);

        uint_fast64_t dst_size;
        ec = compression::integrate_compressed_blocks_in_realm_file(
            buf.get(), blocks_size, std::string(unencrypted_path), encryption_key_none, dst_size);
        CHECK_NOT(ec);

        current_offset = next_offset;
        if (current_offset == max_offset)
            break;
    }

    CHECK(files_compare_equal(src_path, unencrypted_path));
}

#if REALM_ENABLE_ENCRYPTION
TEST(Compression_ExtractBlocksEncrypted)
{
    // path_1 is an encrypted Realm that is created directly.
    // path_2 is an unencrypted Realm that is created by extracting
    // compressed blocks from path_1 and integrating them in path_2.
    // path_3 is an encrypted Realm that is created by extracting
    // compressed blocks from path_1 and integrating them in path_3.

    SHARED_GROUP_TEST_PATH(path_1);
    SHARED_GROUP_TEST_PATH(path_2);
    SHARED_GROUP_TEST_PATH(path_3);

    std::array<char, 64> encryption_key_1;
    encryption_key_1.fill(1);
    util::Optional<std::array<char, 64>> encryption_key_2 = none;
    std::array<char, 64> encryption_key_3;
    encryption_key_3.fill(3);

    size_t data_size = 1 << 19;
    make_data_in_realm(path_1, data_size, encryption_key_1);

    std::error_code ec;
    size_t buf_size = 1 << 19;
    auto buf = std::make_unique<char[]>(buf_size);
    uint_fast64_t current_offset = 0;
    while (true) {
        uint_fast64_t next_offset;
        uint_fast64_t max_offset;
        size_t blocks_size;

        ec = compression::extract_blocks_from_file(path_1.c_str(), encryption_key_1, current_offset, next_offset,
                                                   max_offset, buf.get(), buf_size, blocks_size);

        CHECK_NOT(ec);
        CHECK_GREATER(next_offset, current_offset);
        CHECK_GREATER(blocks_size, 0);
        CHECK_LESS_EQUAL(next_offset, max_offset);

        uint_fast64_t size_dummy;
        ec = compression::integrate_compressed_blocks_in_realm_file(buf.get(), blocks_size, std::string(path_2),
                                                                    encryption_key_2, size_dummy);

        CHECK_NOT(ec);

        ec = compression::integrate_compressed_blocks_in_realm_file(buf.get(), blocks_size, std::string(path_3),
                                                                    encryption_key_3, size_dummy);
        CHECK_NOT(ec);

        current_offset = next_offset;
        if (current_offset == max_offset)
            break;
    }

    CHECK(!files_compare_equal(path_1, path_2));
    CHECK(!files_compare_equal(path_1, path_3));
    CHECK(!files_compare_equal(path_2, path_3));
    {
        DBOptions options_1{encryption_key_1.data()};
        DBRef sg_1 = DB::create(make_client_replication(), path_1, options_1);
        DBRef sg_2 = DB::create(make_client_replication(), path_2);
        DBOptions options_3{encryption_key_3.data()};
        DBRef sg_3 = DB::create(make_client_replication(), path_3, options_3);

        ReadTransaction rt_1{sg_1};
        ReadTransaction rt_2{sg_2};
        ReadTransaction rt_3{sg_3};
        CHECK(compare_groups(rt_1, rt_2));
        CHECK(compare_groups(rt_1, rt_3));
    }
}
#endif
