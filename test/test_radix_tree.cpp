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

constexpr size_t c_compact_threshold = 100;

TEST_TYPES(IndexKey_Get, ChunkOf<4>, ChunkOf<5>, ChunkOf<6>, ChunkOf<7>, ChunkOf<8>, ChunkOf<9>, ChunkOf<10>)
{
    constexpr size_t ChunkWidth = TEST_TYPE::value;

    CHECK(!IndexKey<ChunkWidth>(Mixed{}).get());

    const uint64_t max = uint64_t(1) << ChunkWidth;
    const uint64_t sign_bit_flip = uint64_t(1) << (ChunkWidth - 1);
    for (uint64_t i = 0; i < max; ++i) {
        uint64_t shifted_value = i << (64 - ChunkWidth);
        IndexKey<ChunkWidth> key{int64_t(shifted_value)};
        auto val = key.get();
        CHECK(val);
        CHECK_EQUAL(i ^ sign_bit_flip, *val);

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
    std::vector<ObjKey> obj_keys;
    for (auto val : values) {
        obj_keys.push_back(table.create_object_with_primary_key(ObjectId::gen()).set_any(col_key, val).get_key());
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
    CHECK(int_index->has_duplicate_values());
    std::vector<ObjKey> results;
    int_index->find_all(results, Mixed{5});
    CHECK_EQUAL(results.size(), 3);
    CHECK_EQUAL(results[0], obj_keys[6]);
    CHECK_EQUAL(results[1], obj_keys[7]);
    CHECK_EQUAL(results[2], obj_keys[8]);
    InternalFindResult res;
    FindRes res_type = int_index->find_all_no_copy(Mixed{4}, res);
    CHECK_EQUAL(res_type, FindRes_column);
    IntegerColumn col(table.get_alloc(), ref_type(res.payload));
    CHECK(col.size() >= res.end_ndx);
    CHECK_EQUAL(res.end_ndx - res.start_ndx, 2);
    CHECK_EQUAL(col.get(res.start_ndx), obj_keys[4].value);
    CHECK_EQUAL(col.get(res.start_ndx + 1), obj_keys[5].value);

    int_index->find_all_greater_equal(Mixed{4}, results);
    std::vector<ObjKey> expected;
    expected.insert(expected.begin(), obj_keys.begin() + 4, obj_keys.begin() + 9);
    CHECK_EQUAL(results, expected);

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
            return std::make_unique<RadixTree<ChunkWidth>>(ref, parent, col_ndx, cluster, alloc, c_compact_threshold);
        }
        return std::make_unique<RadixTree<ChunkWidth>>(cluster, alloc, c_compact_threshold); // Throws
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
        table.verify();
    };

    std::vector<ObjKey> keys_inserted;
    auto verify_values = [&](std::vector<StringData>&& values) {
        verify_removal();
        index = table.get_search_index(col_key);
        CHECK(!index);
        for (auto val : values) {
            table.create_object().set_any(col_key, val);
        }
        // bulk insertion
        table.add_search_index(col_key);
        index = table.get_search_index(col_key);
        CHECK(index);
        // verify find
        for (auto val : values) {
            const ObjKey key = index->find_first(val);
            CHECK(key);
        }
        // remove in reverse to exercise erase()
        while (table.size()) {
            table.remove_object(table.begin() += table.size() - 1);
        }
        keys_inserted.clear();
        // exercise insertion
        for (auto val : values) {
            keys_inserted.push_back(table.create_object().set_any(col_key, val).get_key());
        }
    };

    auto remove_nth_inserted_item = [&keys_inserted, &table](size_t n) {
        table.remove_object(keys_inserted[n]);
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
    RadixTree<8>* ndx = dynamic_cast<RadixTree<8>*>(index);
    CHECK(ndx);
    std::vector<ObjKey> result_keys;
    ndx->find_all_between_inclusive(Mixed{"0"}, Mixed{"9"}, result_keys);
    CHECK_EQUAL(result_keys, keys_inserted);
    ndx->find_all_less_equal(Mixed{"3"}, result_keys);
    std::vector<ObjKey> expected;
    expected.insert(expected.begin(), keys_inserted.begin(), keys_inserted.begin() + 8);
    CHECK_EQUAL(result_keys, expected);
    ndx->find_all_greater_equal(Mixed{"3"}, result_keys);
    expected.clear();
    expected.insert(expected.begin(), keys_inserted.begin() + 6, keys_inserted.end());
    CHECK_EQUAL(result_keys, expected);

    verify_values({StringData(), "aabc", "aab", "aabcd", "aa"});
    verify_values({"aa", "aab", "aa", "aa"});

    // check node collapse on erase of specific item
    // increase the prefix size to check the boundary across inline/lookup prefix modes being combined
    for (size_t i = 0; i < 10; ++i) {
        std::string prefix_two = "prefix two";
        std::string prefix_three = "prefix three";
        std::string shared_prefix = std::string(i, 'x');
        prefix_two.insert(8, shared_prefix);
        prefix_three.insert(8, shared_prefix);
        verify_values({"prefix", "prefix one", prefix_two, prefix_three});
        remove_nth_inserted_item(2);
    }

    std::string storage;
    storage.reserve(513);
    std::vector<StringData> all_first_level_strings;
    std::vector<StringData> twice_all_first_level_strings;
    for (size_t i = 0; i < 255; ++i) {
        storage.append(std::string(1, char(i)));
        StringData sd(storage.c_str() + i, 1);
        all_first_level_strings.push_back(sd);
        twice_all_first_level_strings.push_back(sd);
        twice_all_first_level_strings.push_back(sd);
    }
    verify_values(std::move(all_first_level_strings));
    verify_values(std::move(twice_all_first_level_strings));

    verify_removal();
}

template <typename Type, size_t ChunkWidth>
void do_test_type(Table& table, TestContext& test_context)
{
    int64_t dup_positive = 8;
    int64_t dup_negative = -77;
    // clang-format off
    std::vector<int64_t> values = {
        0, 1, 2, 3, 4, 5, 99, 100, 999, 1000, 1001,
        -1, -2, -3, -4, -5, -99, -100, -999, -1000, -1001,
        dup_positive, dup_positive, dup_positive, dup_positive,
        dup_negative, dup_negative, dup_negative, dup_negative,
        0xF00000000000000,
        0xFF0000000000000,
        0xFFF000000000000,
        0xFFFFF0000000000,
        0xFFFFFFFFFFFFFFF,
        0xFFEEEEEEEEEEEEE,
        0xFFEFEEFEFEFEFEF,
        0xDEADBEEFDEADBEE,
        0xEEE000000000000,
        0xEFF000000000000,
        0x00FF00000000000,
        0x00FFEE000000000,
        0x00FFEEEEEEEEEEE,
        0x00FEFEFEFEFEFEF,
        int64_t(0xFFFFFFFFFFFFFFFF),
        int64_t(0xFEEEEEEEEEEEEEEE),
        int64_t(0xDEADBEEFDEADBEEF),
        int64_t(0xFFEEFFEEFFEEFFEE),
        int64_t(0xFFADBEEFDEADBEEF),
        int64_t(0xFFEEBEEFDEADBEEF),
        int64_t(0xFF00000000000000),
        int64_t(0xFF0000000000000F),
        int64_t(0xFF00000000000001),
        int64_t(0xFFEEEEEEEEEEEEE1),
        int64_t(0xFF11111111111111),
        int64_t(0xFF1111111111111F),
    };
    // clang-format on
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
        if (val_int == -1) {
            expected_count = 2;
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

    if constexpr (std::is_same_v<Type, int64_t>) {
        auto get_result_values = [&](std::vector<ObjKey>& keys, bool expect_null) -> std::vector<int64_t> {
            std::vector<int64_t> result_values;
            auto it_of_null = std::find(keys.begin(), keys.end(), null_val_obj.get_key());
            CHECK(expect_null ? (it_of_null != keys.end()) : (it_of_null == keys.end()));
            if (expect_null) {
                keys.erase(it_of_null);
            }
            std::for_each(keys.begin(), keys.end(), [&](ObjKey key) {
                result_values.push_back(table.get_object(key).get<int64_t>(col));
            });
            return result_values;
        };
        std::sort(values.begin(), values.end());
        std::vector<ObjKey> results;
        constexpr bool should_contain_null = true;
        constexpr bool should_not_contain_null = false;
        RadixTree<ChunkWidth>* int_index = dynamic_cast<RadixTree<ChunkWidth>*>(ndx);
        CHECK(int_index);
        if (int_index) {
            int_index->find_all_less_equal(values.back(), results);
            std::vector<int64_t> result_values = get_result_values(results, should_contain_null);
            CHECK_EQUAL(values, result_values);
            int_index->find_all_greater_equal(values.front(), results);
            result_values = get_result_values(results, should_not_contain_null);
            CHECK_EQUAL(values, result_values);
            int_index->find_all_between_inclusive(values.front(), values.back(), results);
            result_values = get_result_values(results, should_not_contain_null);
            CHECK_EQUAL(values, result_values);
        }
    }

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
    std::vector<size_t> compact_thresholds = {10}; // {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100, 1000, 2000};
    for (size_t threshold : compact_thresholds) {
        Table::IndexMaker hook = [&](ColKey col_key, const ClusterColumn& cluster, Allocator& alloc, ref_type ref,
                                     Array* parent, size_t col_ndx) -> std::unique_ptr<SearchIndex> {
            if (parent) {
                if (col_key.get_type() == col_type_Int || col_key.get_type() == col_type_Timestamp ||
                    col_key.get_type() == col_type_String) {
                    return std::make_unique<RadixTree<ChunkWidth>>(ref, parent, col_ndx, cluster, alloc, threshold);
                }
                return std::make_unique<StringIndex>(ref, parent, col_ndx, cluster, alloc);
            }
            if (col_key.get_type() == col_type_Int || col_key.get_type() == col_type_Timestamp ||
                col_key.get_type() == col_type_String) {
                return std::make_unique<RadixTree<ChunkWidth>>(cluster, alloc, threshold); // Throws
            }
            return std::make_unique<StringIndex>(cluster, alloc); // Throws
        };
        Table table;
        table.set_index_maker(std::move(hook));

        do_test_type<int64_t, ChunkWidth>(table, test_context);
        do_test_type<Timestamp, ChunkWidth>(table, test_context);
        if constexpr (ChunkWidth == 8) {
            do_test_type<StringData, ChunkWidth>(table, test_context);
        }
    }
}

#endif // TEST_RADIX_TREE
