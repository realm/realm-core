#include "test.hpp"

#include <realm/chunked_binary.hpp>

using namespace realm;
using namespace realm::_impl;

TEST(ChunkedBinaryData_From_BinaryData)
{
    {
        std::string str = "Hello, world";
        BinaryData bd(str.data(), str.size());

        ChunkedBinaryData cb(bd);

        CHECK_EQUAL(cb.size(), bd.size());
        CHECK(!cb.is_null());
        CHECK_EQUAL(*cb.get_first_chunk().data(), 'H');
    }

    {
        size_t size = size_t(2e7);
        std::string str(size, 'a');
        BinaryData bd(str.data(), size);

        ChunkedBinaryData cb(bd);

        CHECK_EQUAL(cb.size(), size);
        CHECK(!cb.is_null());
    }
}

TEST(ChunkedBinaryData_From_BinaryColumn)
{
    BinaryColumn bc(Allocator::get_default());
    bc.create();

    std::string str_1 = "Hello, world";
    size_t large_size = size_t(2e7);
    std::string str_2(large_size, 'b');
    str_2[large_size - 2] = 'a';

    {
        BinaryData bd(str_1.data(), str_1.size());
        bc.add(bd);
    }

    {
        BinaryData bd(str_2.data(), str_2.size());
        bc.add(bd);
    }

    {
        BinaryData bd("", 0);
        bc.add(bd);
    }

    CHECK_EQUAL(bc.size(), 3);

    {
        ChunkedBinaryData cb(bc, 0);
        CHECK_EQUAL(cb.size(), str_1.size());
        CHECK(!cb.is_null());
        CHECK_EQUAL(cb[0], str_1[0]);
        CHECK_EQUAL(cb[7], str_1[7]);
        CHECK_EQUAL(cb.hex_dump(), "48 65 6C 6C 6F 2C 20 77 6F 72 6C 64");
    }

    {
        ChunkedBinaryData cb;
        CHECK_EQUAL(cb.size(), 0);
        CHECK(cb.is_null());
        cb = ChunkedBinaryData(bc, 1);
        CHECK_EQUAL(cb.size(), str_2.size());
        CHECK(!cb.is_null());
        CHECK_EQUAL(cb[0], 'b');
        CHECK_EQUAL(cb[large_size - 2], 'a');
        CHECK_EQUAL(cb[large_size - 1], 'b');
    }

    {
        ChunkedBinaryData cb(bc, 2);
        CHECK_EQUAL(cb.size(), 0);
        CHECK(!cb.is_null());
    }

    bc.destroy();
}

TEST(ChunkedBinaryData_From_NullableBinaryColumn)
{
    BinaryColumn bc(Allocator::get_default());
    bc.create();

    std::string str_1 = "Hello, world";
    size_t large_size = size_t(2e7);
    std::string str_2(large_size, 'b');

    {
        BinaryData bd(str_1.data(), str_1.size());
        bc.add(bd);
    }

    {
        BinaryData bd(str_2.data(), str_2.size());
        bc.add(bd);
    }

    {
        BinaryData bd("", 0);
        bc.add(bd);
    }

    {
        BinaryData bd(nullptr, 0);
        bc.add(bd);
    }

    CHECK_EQUAL(bc.size(), 4);

    {
        ChunkedBinaryData cb(bc, 0);
        CHECK_EQUAL(cb.size(), str_1.size());
        CHECK(!cb.is_null());
    }

    {
        ChunkedBinaryData cb(bc, 1);
        CHECK_EQUAL(cb.size(), str_2.size());
        CHECK(!cb.is_null());
    }

    {
        ChunkedBinaryData cb(bc, 2);
        CHECK_EQUAL(cb.size(), 0);
        CHECK(!cb.is_null());
    }

    {
        ChunkedBinaryData cb(bc, 3);
        CHECK(cb.is_null());
    }

    bc.destroy();
}
