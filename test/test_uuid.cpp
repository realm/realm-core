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

#include "test.hpp"


using namespace realm;

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
    CHECK(id_zeros.is_null());
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

    //    // UUID -> string -> UUID
    //    auto uuid2 = UUID::gen();
    //    auto s2 = uuid2.toString();
    //    ASSERT(UUID::isUUIDString(s2));

    //    auto uuid2FromStringRes = UUID::parse(s2);
    //    ASSERT_OK(uuid2FromStringRes);
    //    auto uuid2FromString = uuid2FromStringRes.getValue();
    //    ASSERT_EQUALS(uuid2FromString, uuid2);
    //    ASSERT_EQUALS(uuid2FromString.toString(), s2);

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

/*
TEST(ObjectId_Array)
{
    const char str0[] = "0000012300000000009218a4";
    const char str1[] = "000004560000000000170232";
    const char str2[] = "0000078900000000002999f3";

    ArrayObjectId arr(Allocator::get_default());
    arr.create();

    arr.add({str0});
    arr.add({str1});
    arr.insert(1, {str2});

    ObjectId id2(str2);
    CHECK_EQUAL(arr.get(0), ObjectId(str0));
    CHECK_EQUAL(arr.get(1), id2);
    CHECK_EQUAL(arr.get(2), ObjectId(str1));
    CHECK_EQUAL(arr.find_first(id2), 1);

    arr.erase(1);
    CHECK_EQUAL(arr.get(1), ObjectId(str1));

    ArrayObjectId arr1(Allocator::get_default());
    arr1.create();
    arr.move(arr1, 1);

    CHECK_EQUAL(arr.size(), 1);
    CHECK_EQUAL(arr1.size(), 1);
    CHECK_EQUAL(arr1.get(0), ObjectId(str1));

    arr.destroy();
    arr1.destroy();
}

TEST(ObjectId_ArrayNull)
{
    const char str0[] = "0000012300000000009218a4";
    const char str1[] = "DEADDEADDEADDEADDEADDEAD";
    const char str2[] = "0000078900000000002999f3";

    ArrayObjectIdNull arr(Allocator::get_default());
    arr.create();

    arr.add({str0});
    arr.add({str1});
    arr.insert(1, {str2});
    ObjectId id2(str2);
    CHECK(!arr.is_null(0));
    CHECK_EQUAL(arr.get(0), ObjectId(str0));
    CHECK(!arr.is_null(1));
    CHECK_EQUAL(arr.get(1), id2);
    CHECK(!arr.is_null(2));
    CHECK_EQUAL(arr.get(2), ObjectId(str1));
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
    CHECK_EQUAL(arr.get(1), ObjectId(str1));
    ArrayObjectIdNull arr1(Allocator::get_default());
    arr1.create();
    arr.move(arr1, 1);

    CHECK_EQUAL(arr.size(), 1);
    CHECK_EQUAL(arr1.size(), 2);
    CHECK_EQUAL(arr1.get(0), ObjectId(str1));
    CHECK(!arr1.is_null(0));
    CHECK(arr1.is_null(1));
    CHECK_EQUAL(arr1.find_first_null(0), 1);

    arr.destroy();
    arr1.destroy();
}

// This should exhaustively test all cases of ArrayObjectIdNull::find_first_null.
TEST(ObjectId_ArrayNull_FindFirstNull_StressTest)
{
    // Test is O(2^N * N^2) in terms of this, so don't go too high...
    // 17 should be enough to cover all cases, including a middle block that is neither first nor last.
    const auto MaxSize = 17;

    for (int size = 0; size <= MaxSize; size++) {
        ArrayObjectIdNull arr(Allocator::get_default());
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
                    arr.set(i, ObjectId());
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

TEST(ObjectId_Table)
{
    const char str0[] = "0000012300000000009218a4";
    const char str1[] = "000004560000000000170232";

    Table t;
    auto col_id = t.add_column(type_ObjectId, "id");
    auto col_id_null = t.add_column(type_ObjectId, "id_null", true);
    auto obj0 = t.create_object().set(col_id, ObjectId(str0));
    auto obj1 = t.create_object().set(col_id, ObjectId(str1)).set(col_id_null, ObjectId(str1));
    auto obj2 = t.create_object();
    CHECK_EQUAL(obj0.get<ObjectId>(col_id), ObjectId(str0));
    CHECK_EQUAL(obj1.get<ObjectId>(col_id), ObjectId(str1));
    CHECK_NOT(obj2.is_null(col_id));
    CHECK(obj2.is_null(col_id_null));
    auto id = obj1.get<util::Optional<ObjectId>>(col_id_null);
    CHECK(id);
    id = obj2.get<util::Optional<ObjectId>>(col_id_null);
    CHECK_NOT(id);
    auto key = t.find_first(col_id, ObjectId(str1));
    CHECK_EQUAL(key, obj1.get_key());
    t.add_search_index(col_id);
    key = t.find_first(col_id, ObjectId(str1));
    CHECK_EQUAL(key, obj1.get_key());
}

TEST(ObjectId_PrimaryKey)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef db = DB::create(path);
    Timestamp now{std::chrono::steady_clock::now()};
    ObjectId id{now};
    ObjKey key;
    {
        auto wt = db->start_write();
        auto table = wt->add_table_with_primary_key("Foo", type_ObjectId, "id");
        table->create_object_with_primary_key(ObjectId(now));
        key = table->create_object_with_primary_key(id).get_key();
        wt->commit();
    }
    {
        auto rt = db->start_read();
        auto table = rt->get_table("Foo");
        CHECK_EQUAL(table->size(), 2);
        CHECK_EQUAL(table->find_first_object_id(table->get_primary_key_column(), id), key);
    }
}

TEST(ObjectId_Commit)
{
    // Tend to discover errors in the size calculation logic
    SHARED_GROUP_TEST_PATH(path);
    DBRef db = DB::create(path);
    ObjectId id("0000002a9a7969d24bea4cf2");
    ColKey col;
    {
        auto wt = db->start_write();
        auto table = wt->add_table("Foo");
        col = table->add_column(type_ObjectId, "id");
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
        CHECK_EQUAL(table->begin()->get<ObjectId>(col), id);
    }
}

TEST(ObjectId_Query)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef db = DB::create(path);
    auto now = std::chrono::steady_clock::now();
    ObjectId t0;
    ObjectId t25;
    ObjectId alternative_id("000004560000000000170232");
    ColKey col_id;
    ColKey col_int;
    ColKey col_owns;
    ColKey col_has;

    {
        auto wt = db->start_write();

        auto target = wt->add_table("Target");
        auto origin = wt->add_table("Origin");
        auto table = wt->add_table_with_primary_key("Foo", type_ObjectId, "id");

        col_id = table->add_column(type_ObjectId, "alternative_id", true);
        col_int = table->add_column(type_Int, "int");
        col_has = table->add_column(*target, "Has");
        col_owns = origin->add_column(*table, "Owns");

        ObjKeys target_keys;
        target->create_objects(16, target_keys);

        for (int i = 0; i < 1000; i++) {
            ObjectId id{now + std::chrono::seconds(i / 20)};
            if (i == 0)
                t0 = id;
            if (i == 25)
                t25 = id;
            auto obj = table->create_object_with_primary_key(id).set(col_int, i);
            if (i % 30 == 0)
                obj.set(col_id, alternative_id);
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

        Query q = table->column<ObjectId>(col) > t0;
        CHECK_EQUAL(q.count(), 999);
        q = table->where().greater(col, t0);
        CHECK_EQUAL(q.count(), 999);
        Query q1 = table->column<ObjectId>(col) < t25;
        CHECK_EQUAL(q1.count(), 25);
        q1 = table->where().less(col, t25);
        CHECK_EQUAL(q1.count(), 25);
        q1 = table->where().less_equal(col, t25);
        CHECK_EQUAL(q1.count(), 26);
        auto tv = q1.find_all();
        tv.sort(col, true);
        for (int i = 0; i < 25; i++) {
            CHECK_EQUAL(tv.get(i).get<int64_t>(col_int), i);
        }
        Query q2 = table->column<ObjectId>(col_id) == alternative_id;
        // std::cout << q2.get_description() << std::endl;
        CHECK_EQUAL(q2.count(), 34);
        q2 = table->column<ObjectId>(col_id) == realm::null();
        // std::cout << q2.get_description() << std::endl;
        CHECK_EQUAL(q2.count(), 1000 - 34);
        q2 = table->where().equal(col_id, realm::null());
        CHECK_EQUAL(q2.count(), 1000 - 34);

        // Test query over links
        Query q3 = origin->link(col_owns).column<ObjectId>(col_id) == alternative_id;
        CHECK_EQUAL(q3.count(), 34);

        // Test query over backlink (link list)
        Query q4 = target->backlink(*table, col_has).column<ObjectId>(col_id) == alternative_id;
        CHECK_EQUAL(q4.count(), 8);

        // just check that it does not crash
        std::ostringstream ostr;
        tv.to_json(ostr);
        Query q5 = table->column<ObjectId>(col) >= t0;
        CHECK_EQUAL(q5.count(), 1000);
        Query q6 = table->column<ObjectId>(col) <= t25;
        CHECK_EQUAL(q6.count(), 26);
    }
}

TEST(ObjectId_Distinct)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef db = DB::create(path);

    {
        std::vector<ObjectId> ids{"000004560000000000170232", "000004560000000000170233", "000004550000000000170232"};
        auto wt = db->start_write();
        auto table = wt->add_table("Foo");
        auto col_id = table->add_column(type_ObjectId, "id", true);
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

TEST(ObjectId_Gen)
{
    auto a = ObjectId::gen();
    auto b = ObjectId::gen();

    if (b < a) {
        // This can only happen if the seq counter rolled over. Since it is 24 bits, this is expected once every 16
        // million runs. Generate new ones which should not involve another rollover.
        // This could also happen if the clock goes backwards, and while it could happen again, hopefully it won't.
        a = ObjectId::gen();
        b = ObjectId::gen();
    }

    CHECK_LESS(a, b);
}
*/
