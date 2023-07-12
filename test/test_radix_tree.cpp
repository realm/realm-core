/*************************************************************************
 *
 * Copyright 2023 Realm Inc.
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

#include "testsettings.hpp"
#ifdef TEST_RADIX_TREE

#include <realm.hpp>
#include <realm/index_string.hpp>
#include <realm/query_expression.hpp>
#include <realm/tokenizer.hpp>
#include <realm/util/to_string.hpp>
#include <set>
#include <type_traits>
#include "test.hpp"
#include "util/misc.hpp"
#include "util/random.hpp"


// We are interested in testing various sizes of the tree
// but we don't want the core shared library to pay the size
// cost of storing these symbols when none of the SDKs will use
// them. To get around this using explicit template instantiation,
// we include the radix_tree_templates.cpp file here which _only_
// has templated code and instantiate it below.
#include <realm/radix_tree_templates.cpp>
namespace realm {
template class RadixTree<7>;
template class RadixTree<8>;
template class RadixTree<9>;
template class RadixTree<10>;
template class RadixTree<16>;
} // namespace realm


using namespace realm;
using namespace util;
using namespace realm;
using namespace realm::util;
using namespace realm::test_util;
using unit_test::TestContext;


template <size_t Chunk>
using ChunkOf = typename std::integral_constant<size_t, Chunk>;

TEST_TYPES(IndexKey_Get, ChunkOf<4>, ChunkOf<5>, ChunkOf<6>, ChunkOf<7>, ChunkOf<8>, ChunkOf<9>, ChunkOf<10>)
{
    constexpr size_t ChunkWidth = TEST_TYPE::value;

    CHECK(!IndexKey<ChunkWidth>(Mixed{}).get());

    size_t max = uint64_t(1) << ChunkWidth;
    for (size_t i = 0; i < max; ++i) {
        uint64_t shifted_value = uint64_t(i) << (64 - ChunkWidth);
        IndexKey<ChunkWidth> key{int64_t(shifted_value)};
        auto val = key.get();
        CHECK(val);
        CHECK_EQUAL(i, *val);

        const size_t num_chunks_per_int = size_t(std::ceil(double(64) / double(ChunkWidth)));

        size_t chunk_count = 1;
        while (key.get_next()) {
            val = key.get();
            CHECK(val);
            CHECK_EQUAL(*val, 0);
            ++chunk_count;
        }
        CHECK_EQUAL(chunk_count, num_chunks_per_int);
    }
}

TEST(RadixTree_BuildIndexInt)
{
    std::vector<Mixed> values = {0, 1, 2, 3, 4, 4, 5, 5, 5, Mixed{}, -1};
    Table table;
    ColKey col_pk = table.add_column(type_ObjectId, "pk");
    table.set_primary_key_column(col_pk);
    const bool nullable = true;
    ColKey col_key = table.add_column(type_Int, "values", nullable);

    for (auto val : values) {
        table.create_object_with_primary_key(ObjectId::gen()).set_any(col_key, val);
    }

    // Create a new index on column
    table.add_search_index(col_key);
    IntegerIndex* int_index = table.get_int_index(col_key);
    CHECK(int_index);

    for (auto val : values) {
        const ObjKey key = int_index->find_first(val);
        CHECK(key);
    }
    CHECK_EQUAL(int_index->count(4), 2);
    CHECK_EQUAL(int_index->count(5), 3);
    while (table.size()) {
        table.remove_object(table.begin());
    }
}

TEST_TYPES(RadixTree_BuildIndexString, ChunkOf<8>)
{
    constexpr size_t ChunkWidth = TEST_TYPE::value;

    Table::IndexMaker hook = [&](ColKey, const ClusterColumn& cluster, Allocator& alloc, ref_type ref, Array* parent,
                                 size_t col_ndx) -> std::unique_ptr<SearchIndex> {
        if (parent) {
            return std::make_unique<RadixTree<ChunkWidth>>(ref, parent, col_ndx, cluster, alloc);
        }
        return std::make_unique<RadixTree<ChunkWidth>>(cluster, alloc); // Throws
    };
    Table table;
    table.set_index_maker(std::move(hook));
    const bool nullable = true;
    ColKey col_key = table.add_column(type_String, "values", nullable);
    SearchIndex* index;

    auto verify_removal = [&]() {
        while (table.size()) {
            table.remove_object(table.begin());
        }
        table.remove_search_index(col_key);
        table.add_search_index(col_key);
        table.verify();
    };

    auto verify_values = [&](std::vector<StringData>&& values) {
        verify_removal();
        index = table.get_search_index(col_key);
        CHECK(index);
        for (auto val : values) {
            table.create_object().set_any(col_key, val);
        }

        for (auto val : values) {
            const ObjKey key = index->find_first(val);
            CHECK(key);
        }
    };

    verify_values({StringData(), "", "", "prefix", "prefix one", "prefix two", "prefix three"});
    CHECK_EQUAL(index->count(""), 2);
    CHECK_EQUAL(index->count(StringData()), 1);
    CHECK_EQUAL(index->count(StringData("prefix")), 1);
    CHECK(index->has_duplicate_values());

    // these values split an interior node with a prefix more than once
    verify_values({"prefixfirst", "prefixsecond", "prefixsecondmore", "prefixsec", "prefixsx"});
    verify_values({"", "", "", StringData(), StringData(), "", StringData(), ""});
    CHECK_EQUAL(index->count(""), 5);
    CHECK_EQUAL(index->count(StringData()), 3);
    CHECK(index->has_duplicate_values());
    verify_values(
        {"0", "0", "1", "1", "2", "2", "3", "3", "4", "4", "5", "5", "6", "6", "7", "7", "8", "8", "9", "9"});
    CHECK_EQUAL(index->count("10"), 0);
    CHECK_EQUAL(index->count(""), 0);
    CHECK_EQUAL(index->count(StringData()), 0);
    CHECK(index->has_duplicate_values());

    verify_removal();
}

template <typename Type>
void do_test_type(Table& table, TestContext& test_context)
{
    int64_t dup_positive = 8;
    int64_t dup_negative = -77;
    std::vector<int64_t> values = {
        0,
        1,
        2,
        3,
        4,
        5,
        99,
        100,
        999,
        1000,
        -1,
        -2,
        -3,
        -4,
        -5,
        dup_positive,
        dup_positive,
        dup_positive,
        dup_positive,
        dup_negative,
        dup_negative,
        dup_negative,
        dup_negative,
        0xF00000000000000,
        0xFF0000000000000,
        0xFFF000000000000,
        0xFFFFF0000000000,
        0xFFFFFFFFFFFFFFF,
        0xEEE000000000000,
        0xEFF000000000000,
    };
    table.clear();
    std::vector<std::string> string_storage;
    auto convert_value = [&](int64_t val) {
        if constexpr (std::is_same_v<Type, Timestamp>) {
            int32_t ns = uint64_t(val) >> 32;
            if (val < 0 && ns > 0) {
                ns *= -1;
            }
            return Timestamp{val, ns};
        }
        else if constexpr (std::is_same_v<Type, StringData>) {
            // length must not be mod a number in the values list or we'll have unexpected duplicates
            size_t length = abs(val) % 251;
            char c = ' ' + val % 93; // printable ascii range of ' ' to '}'
            string_storage.push_back(std::string(length, c));
            return StringData{string_storage.back()};
        }
        else {
            return val;
        }
    };

    constexpr bool nullable = true;
    ColKey col =
        table.add_column(ColumnTypeTraits<Type>::id, util::format("column %1", table.get_column_count()), nullable);
    table.add_search_index(col);
    SearchIndex* ndx = table.get_search_index(col);
    CHECK(ndx);
    std::vector<ObjKey> keys;

    for (auto val : values) {
        auto obj = table.create_object().set(col, convert_value(val));
        keys.push_back(obj.get_key());
    }
    Obj null_val_obj = table.create_object().set_null(col);
    keys.push_back(null_val_obj.get_key());

    for (size_t i = 0; i < values.size(); ++i) {
        size_t expected_count = 1;
        int64_t val_int = values[i];
        auto val = convert_value(val_int);
        if (val_int == dup_positive || val_int == dup_negative) {
            expected_count = 4;
        }
        CHECK_EQUAL(expected_count, ndx->count(val));
        if (expected_count == 1) {
            ObjKey expected_key = keys[i];
            CHECK_EQUAL(expected_key, ndx->find_first(val));
        }
    }
    CHECK_EQUAL(null_val_obj.get_key(), ndx->find_first(Mixed{}));
    CHECK_EQUAL(ndx->count(Mixed{}), 1);
    CHECK(ndx->has_duplicate_values());
    CHECK_NOT(ndx->is_empty());

    for (auto key : keys) {
        table.remove_object(key);
    }
    CHECK_EQUAL(ndx->count(Mixed{}), 0);
    CHECK_NOT(ndx->has_duplicate_values());
    CHECK(ndx->is_empty());
    CHECK_EQUAL(ndx->find_first(Mixed{}), ObjKey{});

    CHECK(table.is_empty());
}

TEST_TYPES(IndexNode, ChunkOf<4>, ChunkOf<5>, ChunkOf<6>, ChunkOf<7>, ChunkOf<8>, ChunkOf<9>, ChunkOf<10>)
{
    constexpr size_t ChunkWidth = TEST_TYPE::value;

    Table::IndexMaker hook = [&](ColKey col_key, const ClusterColumn& cluster, Allocator& alloc, ref_type ref,
                                 Array* parent, size_t col_ndx) -> std::unique_ptr<SearchIndex> {
        if (parent) {
            if (col_key.get_type() == col_type_Int || col_key.get_type() == col_type_Timestamp ||
                col_key.get_type() == col_type_String) {
                return std::make_unique<RadixTree<ChunkWidth>>(ref, parent, col_ndx, cluster, alloc);
            }
            return std::make_unique<StringIndex>(ref, parent, col_ndx, cluster, alloc);
        }
        if (col_key.get_type() == col_type_Int || col_key.get_type() == col_type_Timestamp ||
            col_key.get_type() == col_type_String) {
            return std::make_unique<RadixTree<ChunkWidth>>(cluster, alloc); // Throws
        }
        return std::make_unique<StringIndex>(cluster, alloc); // Throws
    };
    Table table;
    table.set_index_maker(std::move(hook));

    do_test_type<int64_t>(table, test_context);
    do_test_type<Timestamp>(table, test_context);
    //    do_test_type<StringData>(table, test_context);
}

#endif // TEST_RADIX_TREE
