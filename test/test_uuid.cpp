/*************************************************************************
 *
 * Copyright 2020 Realm Inc.
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

#include <realm.hpp>
#include <realm/uuid.hpp>
#include <chrono>
#include <realm/array_fixed_bytes.hpp>

#include "test.hpp"

using namespace realm;

struct WithIndex {
    constexpr static bool do_add_index = true;
};

struct WithoutIndex {
    constexpr static bool do_add_index = false;
};

UUID generate_random_uuid()
{
    std::string str;
    str.resize(36);
    std::generate<std::string::iterator, char (*)()>(str.begin(), str.end(), []() -> char {
        char c = test_util::random_int<char>(0, 15);
        return c >= 10 ? (c - 10 + 'a') : (c + '0');
    });
    str.at(8) = '-';
    str.at(13) = '-';
    str.at(18) = '-';
    str.at(23) = '-';
    return UUID(str.c_str());
}

util::Optional<UUID> generate_random_nullable_uuid()
{
    if (test_util::random_int<size_t>(0, 1) == 0) {
        return util::none;
    }
    return util::Optional<UUID>(generate_random_uuid());
}

TEST(UUID_Basics)
{
    std::string init_str0("3b241101-e2bb-4255-8caf-4136c566a962");
    UUID id0(init_str0.c_str());
    CHECK_EQUAL(id0.to_string(), init_str0);

    std::string init_str1("3b241101-e2bb-4255-8caf-4136c566a962");
    UUID id1(init_str1.c_str());
    CHECK_EQUAL(id1.to_string(), init_str1);

    UUID id_zeros;
    CHECK(id_zeros == UUID("00000000-0000-0000-0000-000000000000"));

    std::string init_str_max("ffffffff-ffff-ffff-ffff-ffffffffffff");
    UUID id_max(init_str_max);
    CHECK(id_max.to_string() == init_str_max);

    std::string exception_message;
    try {
        UUID should_fail("hello world");
    }
    catch (const InvalidUUIDString& e) {
        exception_message = e.what();
    }
    CHECK_EQUAL(exception_message, "Invalid string format encountered when constructing a UUID: 'hello world'.");

    UUID::UUIDBytes raw_zeros = {};
    CHECK_EQUAL(UUID(raw_zeros).to_string(), "00000000-0000-0000-0000-000000000000");
    UUID::UUIDBytes raw_one = {255, 124, 32, 16, 8, 4, 2, 1, 15};
    CHECK_EQUAL(UUID(raw_one).to_string(), "ff7c2010-0804-0201-0f00-000000000000");
}

// with credit to https://github.com/mongodb/mongo/blob/master/src/mongo/base/uuid_test.cpp
TEST(UUID_isValid)
{
    // Several valid strings
    CHECK(UUID::is_valid_string("00000000-0000-4000-8000-000000000000"));
    CHECK(UUID::is_valid_string("01234567-9abc-4def-9012-3456789abcde"));
    CHECK(UUID::is_valid_string("dddddddd-eeee-4fff-aaaa-bbbbbbbbbbbb"));
    CHECK(UUID::is_valid_string("A9A9A9A9-BEDF-4DD9-B001-222345716283"));

    // No version or variant set
    CHECK(UUID::is_valid_string("00000000-0000-0000-0000-000000000000"));

    // Mixed casing is weird, but technically legal
    CHECK(UUID::is_valid_string("abcdefAB-CDEF-4000-AaAa-FDFfdffd9991"));

    // Wrong number of Hyphens
    CHECK(!UUID::is_valid_string("00000000-0000-4000-8000-0000000000-00"));
    CHECK(!UUID::is_valid_string("000000000000-4000-8000-000000000000"));
    CHECK(!UUID::is_valid_string("00000000000040008000000000000000"));

    // Hyphens in the wrong places
    CHECK(!UUID::is_valid_string("dddddd-ddeeee-4fff-aaaa-bbbbbbbbbbbb"));
    CHECK(!UUID::is_valid_string("ddddddd-deeee-4fff-aaaa-bbbbbbbbbbbb"));
    CHECK(!UUID::is_valid_string("d-d-d-dddddeeee4fffaaaa-bbbbbbbbbbbb"));

    // Illegal characters
    CHECK(!UUID::is_valid_string("samsamsa-sams-4sam-8sam-samsamsamsam"));

    // Too short
    CHECK(!UUID::is_valid_string("A9A9A9A9-BEDF-4DD9-B001"));
    CHECK(!UUID::is_valid_string("dddddddd-eeee-4fff-aaaa-bbbbbbbbbbb"));

    // Too long
    CHECK(!UUID::is_valid_string("01234567-9abc-4def-9012-3456789abcdef"));
    CHECK(!UUID::is_valid_string("0123004567-9abc-4def-9012-3456789abcdef0000"));
}

TEST(UUID_toAndFromString)
{
    // String -> UUID -> string
    auto s1 = "00000000-0000-4000-8000-000000000000";
    CHECK(UUID::is_valid_string(s1));
    UUID uuid1Res(s1);
    auto uuid1 = uuid1Res.to_string();
    CHECK(UUID::is_valid_string(uuid1));
    CHECK_EQUAL(uuid1, s1);

    // UUID -> string -> UUID
    auto uuid2 = generate_random_uuid();
    auto s2 = uuid2.to_string();
    CHECK(UUID::is_valid_string(s2));

    UUID uuid2FromString(s2);
    CHECK_EQUAL(uuid2FromString, uuid2);
    CHECK_EQUAL(uuid2FromString.to_string(), s2);

    // Two UUIDs constructed from the same string are equal
    auto s3 = "01234567-9abc-4def-9012-3456789abcde";
    CHECK(UUID::is_valid_string(s3));
    UUID uuid3Res(s3);
    UUID uuid3AgainRes(s3);
    CHECK_EQUAL(uuid3Res, uuid3AgainRes);
    CHECK_EQUAL(uuid3Res.to_string(), uuid3AgainRes.to_string());

    // Two UUIDs constructed from differently cased string are equal
    auto sLower = "00000000-aaaa-4000-8000-000000000000";
    auto sUpper = "00000000-AAAA-4000-8000-000000000000";
    CHECK(UUID::is_valid_string(sLower));
    CHECK(UUID::is_valid_string(sUpper));
    UUID uuidLower(sLower);
    UUID uuidUpper(sUpper);
    CHECK_EQUAL(uuidLower, uuidUpper);
    // Casing is not preserved on round trip, both become lowercase
    CHECK_EQUAL(uuidLower.to_string(), uuidUpper.to_string());
    CHECK_EQUAL(uuidLower.to_string(), sLower);
    CHECK_EQUAL(uuidUpper.to_string(), sLower);
    CHECK_NOT_EQUAL(uuidUpper.to_string(), sUpper);

    // UUIDs constructed from different strings are not equal
    auto s4 = "01234567-9abc-4def-9012-3456789abcde";
    auto s5 = "01234567-0000-4def-9012-3456789abcde";
    CHECK(UUID::is_valid_string(s4));
    CHECK(UUID::is_valid_string(s5));
    CHECK_NOT_EQUAL(UUID(s4), UUID(s5));
}

TEST(UUID_Array)
{
    const char str0[] = "3b241101-e2bb-4255-8caf-4136c566a960";
    const char str1[] = "3b241101-e2bb-4255-8caf-4136c566a961";
    const char str2[] = "3b241101-e2bb-4255-8caf-4136c566a962";

    ArrayUUID arr(Allocator::get_default());
    arr.create();

    CHECK_EQUAL(arr.size(), 0);
    arr.add(UUID{str0});
    CHECK_EQUAL(arr.size(), 1);
    CHECK_EQUAL(arr.get(0), UUID(str0));
    arr.add(UUID{str1});
    arr.insert(1, UUID{str2});
    CHECK_EQUAL(arr.size(), 3);

    UUID id2(str2);
    CHECK_EQUAL(arr.get(0), UUID(str0));
    CHECK_EQUAL(arr.get(1), id2);
    CHECK_EQUAL(arr.get(2), UUID(str1));
    CHECK_EQUAL(arr.find_first(id2), 1);

    arr.erase(1);
    CHECK_EQUAL(arr.get(1), UUID(str1));
    CHECK_EQUAL(arr.size(), 2);

    ArrayUUID arr1(Allocator::get_default());
    arr1.create();
    arr.move(arr1, 1);

    CHECK_EQUAL(arr.size(), 1);
    CHECK_EQUAL(arr1.size(), 1);
    CHECK_EQUAL(arr1.get(0), UUID(str1));

    arr.destroy();
    arr1.destroy();
}


TEST(UUID_ArrayNull)
{
    const char str0[] = "3b241101-e2bb-4255-8caf-4136c566a960";
    const char str1[] = "3b241101-e2bb-4255-8caf-4136c566a961";
    const char str2[] = "3b241101-e2bb-4255-8caf-4136c566a962";

    ArrayUUIDNull arr(Allocator::get_default());
    arr.create();

    arr.add(UUID{str0});
    arr.add(UUID{str1});
    arr.insert(1, UUID{str2});
    UUID id2(str2);
    CHECK(!arr.is_null(0));
    CHECK_EQUAL(arr.get(0), UUID(str0));
    CHECK(!arr.is_null(1));
    CHECK_EQUAL(arr.get(1), id2);
    CHECK(!arr.is_null(2));
    CHECK_EQUAL(arr.get(2), UUID(str1));
    CHECK_EQUAL(arr.find_first(id2), 1);
    CHECK_EQUAL(arr.find_first_null(), npos);

    arr.add(util::none);
    CHECK_EQUAL(arr.find_first_null(0), 3);
    CHECK_EQUAL(arr.find_first_null(1), 3);
    CHECK_EQUAL(arr.find_first_null(2), 3);
    CHECK_EQUAL(arr.find_first_null(3), 3);
    CHECK_EQUAL(arr.find_first_null(0, 3), npos);
    CHECK_EQUAL(arr.find_first_null(3, 3), npos);
    CHECK_EQUAL(arr.find_first_null(4), npos);

    arr.erase(1);
    CHECK_EQUAL(arr.get(1), UUID(str1));
    ArrayUUIDNull arr1(Allocator::get_default());
    arr1.create();
    arr.move(arr1, 1);

    CHECK_EQUAL(arr.size(), 1);
    CHECK_EQUAL(arr1.size(), 2);
    CHECK_EQUAL(arr1.get(0), UUID(str1));
    CHECK(!arr1.is_null(0));
    CHECK(arr1.is_null(1));
    CHECK_EQUAL(arr1.find_first_null(0), 1);

    arr.destroy();
    arr1.destroy();
}

TEST_TYPES(UUID_Table, WithIndex, WithoutIndex)
{
    const char str0[] = "3b241101-e2bb-4255-8caf-4136c566a960";
    const char str1[] = "3b241101-e2bb-4255-8caf-4136c566a961";

    Table t;
    auto col_id = t.add_column(type_UUID, "id");
    auto col_id_null = t.add_column(type_UUID, "id_null", true);
    auto obj0 = t.create_object().set(col_id, UUID(str0)).set(col_id_null, UUID(str0));
    auto obj1 = t.create_object().set(col_id, UUID(str1)).set(col_id_null, UUID(str1));
    auto obj2 = t.create_object();

    if constexpr (TEST_TYPE::do_add_index) {
        t.add_search_index(col_id);
        t.add_search_index(col_id_null);
    }

    CHECK_EQUAL(obj0.get<UUID>(col_id), UUID(str0));
    CHECK_EQUAL(obj1.get<UUID>(col_id), UUID(str1));
    CHECK_NOT(obj2.is_null(col_id));
    CHECK_EQUAL(obj0.get<util::Optional<UUID>>(col_id_null), UUID(str0));
    CHECK_EQUAL(obj1.get<util::Optional<UUID>>(col_id_null), UUID(str1));
    CHECK(obj2.is_null(col_id_null));
    CHECK_NOT(obj2.get<util::Optional<UUID>>(col_id_null));

    auto key = t.find_first(col_id, UUID(str0));
    CHECK_EQUAL(key, obj0.get_key());
    key = t.find_first(col_id, UUID(str1));
    CHECK_EQUAL(key, obj1.get_key());
    key = t.find_first_null(col_id);
    CHECK_NOT(key);
    key = t.find_first(col_id_null, util::Optional<UUID>(UUID(str0)));
    CHECK_EQUAL(key, obj0.get_key());
    key = t.find_first(col_id_null, util::Optional<UUID>(UUID(str1)));
    CHECK_EQUAL(key, obj1.get_key());
    key = t.find_first_null(col_id_null);
    CHECK_EQUAL(key, obj2.get_key());
    key = t.find_first(col_id_null, util::Optional<UUID>());
    CHECK_EQUAL(key, obj2.get_key());
}


TEST(UUID_PrimaryKey)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef db = DB::create(path);
    UUID id{"3b241101-e2bb-4255-8caf-4136c566a960"};
    ObjKey key;
    {
        auto wt = db->start_write();
        auto table = wt->add_table_with_primary_key("Foo", type_UUID, "id");
        table->create_object_with_primary_key(UUID("3b241101-e2bb-4255-8caf-4136c566a961"));
        key = table->create_object_with_primary_key(id).get_key();
        wt->commit();
    }
    {
        auto rt = db->start_read();
        auto table = rt->get_table("Foo");
        CHECK_EQUAL(table->size(), 2);
        CHECK_EQUAL(table->find_first_uuid(table->get_primary_key_column(), id), key);
    }
}

TEST(UUID_PrimaryKeyNullable)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef db = DB::create(path);
    UUID id{"3b241101-e2bb-4255-8caf-4136c566a960"};
    ObjKey key0;
    ObjKey key1;
    ObjKey key2;
    {
        auto wt = db->start_write();
        auto table = wt->add_table_with_primary_key("Foo", type_UUID, "id", true);
        key0 = table->create_object_with_primary_key(UUID()).get_key();
        key1 = table->create_object_with_primary_key(id).get_key();
        key2 = table->create_object_with_primary_key(util::Optional<UUID>{}).get_key();
        wt->commit();
    }
    {
        auto rt = db->start_read();
        auto table = rt->get_table("Foo");
        CHECK_EQUAL(table->size(), 3);
        CHECK_EQUAL(table->find_first_uuid(table->get_primary_key_column(), UUID()), key0);
        CHECK_EQUAL(table->find_first_uuid(table->get_primary_key_column(), id), key1);
        CHECK_EQUAL(table->find_first_null(table->get_primary_key_column()), key2);
    }
}


TEST(UUID_Commit)
{
    // Tend to discover errors in the size calculation logic
    SHARED_GROUP_TEST_PATH(path);
    DBRef db = DB::create(path);
    UUID id("3b241101-e2bb-4255-8caf-4136c566a960");
    ColKey col;
    {
        auto wt = db->start_write();
        auto table = wt->add_table("Foo");
        col = table->add_column(type_UUID, "id");
        wt->commit();
    }
    {
        auto wt = db->start_write();
        auto table = wt->get_table("Foo");
        col = table->get_column_key("id");
        table->create_object().set(col, id);
        wt->commit();
    }
    {
        auto rt = db->start_read();
        auto table = rt->get_table("Foo");
        CHECK_EQUAL(table->size(), 1);
        CHECK_EQUAL(table->begin()->get<UUID>(col), id);
    }
}

// This test has a higher chance of finding node merge issues
// when using REALM_MAX_BPNODE_SIZE = 4
TEST_TYPES(UUID_GrowAndShrink, UUID, util::Optional<UUID>)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef db = DB::create(path);
    ColKey col;
    using underlying_type = typename util::RemoveOptional<TEST_TYPE>::type;
    constexpr bool is_optional = !std::is_same<underlying_type, TEST_TYPE>::value;
    {
        auto wt = db->start_write();
        auto table = wt->add_table("Foo");
        col = table->add_column(type_UUID, "id", is_optional);
        wt->commit();
    }
    constexpr size_t num_insertions = 2000;
    using KV = std::pair<ObjKey, TEST_TYPE>;
    std::vector<KV> copy;
    auto verify_values = [&](TableRef table) {
        CHECK_EQUAL(copy.size(), table->size());
        for (auto it : copy) {
            auto actual = table->get_object(it.first).template get<TEST_TYPE>(col);
            auto expected = it.second;
            CHECK_EQUAL(actual, expected);
        }
    };
    copy.reserve(num_insertions);
    {
        auto wt = db->start_write();
        auto table = wt->get_table("Foo");
        col = table->get_column_key("id");
        for (size_t i = 0; i < num_insertions; ++i) {
            TEST_TYPE id;
            if constexpr (is_optional) {
                id = generate_random_nullable_uuid();
            }
            else {
                id = generate_random_uuid();
            }
            auto obj = table->create_object();
            obj.set(col, id);
            copy.push_back({obj.get_key(), id});
        }
        wt->commit();
    }
    {
        auto rt = db->start_read();
        auto table = rt->get_table("Foo");
        CHECK_EQUAL(table->size(), num_insertions);
        verify_values(table);
    }
    {
        auto wt = db->start_write();
        auto table = wt->get_table("Foo");
        col = table->get_column_key("id");
        for (size_t i = 0; i < num_insertions; ++i) {
            auto ndx_to_remove = test_util::random_int<size_t>(0, table->size() - 1);
            auto key_to_erase = copy[ndx_to_remove].first;
            copy.erase(copy.begin() + ndx_to_remove);
            table->remove_object(key_to_erase);
            if (i % 8 == 0) {
                verify_values(table);
            }
        }
        wt->commit();
    }
    {
        auto rt = db->start_read();
        auto table = rt->get_table("Foo");
        CHECK_EQUAL(table->size(), 0);
        CHECK_EQUAL(copy.size(), 0);
    }
}

// This should exhaustively test all cases of ArrayUUIDNull::find_first_null.
TEST(UUID_ArrayNull_FindFirstNull_StressTest)
{
    // Test is O(2^N * N^2) in terms of this, so don't go too high...
    // 17 should be enough to cover all cases, including a middle block that is neither first nor last.
    const auto MaxSize = 17;

    for (int size = 0; size <= MaxSize; size++) {
        ArrayUUIDNull arr(Allocator::get_default());
        arr.create();
        for (int i = 0; i < size; i++) {
            arr.add(util::none);
        }

        for (unsigned mask = 0; mask < (1u << size); mask++) {
            // Set nulls to match mask.
            for (int i = 0; i < size; i++) {
                if (mask & (1 << i)) {
                    arr.set(i, util::none);
                }
                else {
                    arr.set(i, UUID());
                }
            }

            for (int begin = 0; begin <= size; begin++) {
                for (int end = begin; end <= size; end++) {
                    auto adjusted_mask = (mask & ~(unsigned(-1) << end)) >> begin;
                    const size_t expected = adjusted_mask ? begin + ctz(adjusted_mask) : npos;
                    CHECK_EQUAL(arr.find_first_null(begin, end), expected);
                }
            }
        }

        arr.destroy();
    }
}

TEST_TYPES(UUID_Query, WithIndex, WithoutIndex)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef db = DB::create(path);
    UUID uuid1("00000000-0000-0000-0000-000000000001");
    UUID uuid2("00000000-0000-0000-0000-000000000002");
    UUID uuid3("ffffffff-ffff-ffff-ffff-ffffffffffff");
    ColKey col_id;
    ColKey col_int;
    ColKey col_owns;
    ColKey col_has;

    {
        auto wt = db->start_write();

        auto target = wt->add_table("Target");
        auto origin = wt->add_table("Origin");
        auto table = wt->add_table_with_primary_key("Foo", type_UUID, "id");

        col_id = table->add_column(type_UUID, "alternative_id", true);
        col_int = table->add_column(type_Int, "int");
        col_has = table->add_column(*target, "Has");
        col_owns = origin->add_column(*table, "Owns");

        if constexpr (TEST_TYPE::do_add_index) {
            table->add_search_index(col_id);
        }

        ObjKeys target_keys;
        target->create_objects(16, target_keys);

        for (int i = 0; i < 1000; i++) {
            UUID id = generate_random_uuid();
            if (i == 0)
                id = uuid1;
            if (i == 25)
                id = uuid2;
            auto obj = table->create_object_with_primary_key(id).set(col_int, i);
            if (i % 30 == 0)
                obj.set(col_id, uuid3);
            origin->create_object().set(col_owns, obj.get_key());
            obj.set(col_has, target_keys[i % target_keys.size()]);
        }
        wt->commit();
    }
    {
        auto rt = db->start_read();
        auto table = rt->get_table("Foo");
        auto origin = rt->get_table("Origin");
        auto target = rt->get_table("Target");
        auto col = table->get_primary_key_column();
        constexpr size_t num_expected_nulls = 1000 - 34;

        Query q = table->column<UUID>(col) != uuid1;
        CHECK_EQUAL(q.count(), 999);
        q = table->column<UUID>(col) == uuid1;
        CHECK_EQUAL(q.count(), 1);
        q = table->column<UUID>(col) != uuid2;
        CHECK_EQUAL(q.count(), 999);
        q = table->column<UUID>(col) == uuid2;
        CHECK_EQUAL(q.count(), 1);
        q = table->column<UUID>(col) == UUID();
        CHECK_EQUAL(q.count(), 0);
        q = table->column<UUID>(col) > UUID();
        CHECK_EQUAL(q.count(), 1000);
        q = table->column<UUID>(col) < UUID();
        CHECK_EQUAL(q.count(), 0);
        q = table->column<UUID>(col) >= uuid1;
        CHECK_EQUAL(q.count(), 1000);
        q = table->column<UUID>(col) <= uuid1;
        CHECK_EQUAL(q.count(), 1);

        q = table->column<UUID>(col_id) >= uuid3;
        CHECK_EQUAL(q.count(), 34);
        q = table->column<UUID>(col_id) <= uuid3;
        CHECK_EQUAL(q.count(), 34);
        q = table->column<UUID>(col_id) != uuid3;
        CHECK_EQUAL(q.count(), num_expected_nulls);
        q = table->column<UUID>(col_id) == uuid3;
        CHECK_EQUAL(q.count(), 34);

        auto tv = table->get_sorted_view({{{col}}, {true}});
        auto tv2 = table->get_sorted_view({{{col}}, {false}});
        CHECK_EQUAL(tv.size(), tv2.size());
        // check that sorting ascending vs descending is stable
        for (size_t i = 0; i < tv.size(); i++) {
            CHECK_EQUAL(tv.get(i).get<UUID>(col), tv2.get(tv2.size() - i - 1).get<UUID>(col));
        }

        Query q2 = table->column<UUID>(col_id) == uuid3;
        CHECK_EQUAL(q2.count(), 34);
        q2 = table->column<UUID>(col_id) == realm::null();
        CHECK_EQUAL(q2.count(), num_expected_nulls);
        q2 = table->where().equal(col_id, realm::null());
        CHECK_EQUAL(q2.count(), num_expected_nulls);

        // Test query over links
        Query q3 = origin->link(col_owns).column<UUID>(col_id) == uuid3;
        CHECK_EQUAL(q3.count(), 34);
        q3 = origin->link(col_owns).column<UUID>(col_id) == realm::null();
        CHECK_EQUAL(q3.count(), num_expected_nulls);

        // Test query over backlink (link list)
        Query q4 = target->backlink(*table, col_has).column<UUID>(col_id) == uuid3;
        CHECK_EQUAL(q4.count(), 8);

        // just check that it does not crash
        std::ostringstream ostr;
        tv = q4.find_all();
        tv.to_json(ostr);
    }
}

TEST(UUID_Distinct)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef db = DB::create(path);

    {
        std::vector<UUID> ids{generate_random_uuid(), generate_random_uuid(), generate_random_uuid()};
        auto wt = db->start_write();
        auto table = wt->add_table("Foo");
        auto col_id = table->add_column(type_UUID, "id", true);
        for (int i = 1; i < 10; i++) {
            auto obj = table->create_object().set(col_id, ids[i % ids.size()]);
        }

        wt->commit();
    }
    {
        auto rt = db->start_read();
        auto table = rt->get_table("Foo");
        ColKey col = table->get_column_key("id");
        DescriptorOrdering order;
        order.append_distinct(DistinctDescriptor({{col}}));
        auto tv = table->where().find_all(order);
        CHECK_EQUAL(tv.size(), 3);
    }
}
