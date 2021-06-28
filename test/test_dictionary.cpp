/*************************************************************************
 *
 * Copyright 2018 Realm Inc.
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

#include <realm.hpp>
#include <realm/dictionary.hpp>
#include <realm/group.hpp>
#include <realm/history.hpp>

#include "test.hpp"
#include <chrono>
#include <set>
// #include <valgrind/callgrind.h>

using namespace std::chrono;

using namespace realm;
using namespace realm::test_util;

#ifndef CALLGRIND_START_INSTRUMENTATION
#define CALLGRIND_START_INSTRUMENTATION
#endif

#ifndef CALLGRIND_STOP_INSTRUMENTATION
#define CALLGRIND_STOP_INSTRUMENTATION
#endif

// Test independence and thread-safety
// -----------------------------------
//
// All tests must be thread safe and independent of each other. This
// is required because it allows for both shuffling of the execution
// order and for parallelized testing.
//
// In particular, avoid using std::rand() since it is not guaranteed
// to be thread safe. Instead use the API offered in
// `test/util/random.hpp`.
//
// All files created in tests must use the TEST_PATH macro (or one of
// its friends) to obtain a suitable file system path. See
// `test/util/test_path.hpp`.
//
//
// Debugging and the ONLY() macro
// ------------------------------
//
// A simple way of disabling all tests except one called `Foo`, is to
// replace TEST(Foo) with ONLY(Foo) and then recompile and rerun the
// test suite. Note that you can also use filtering by setting the
// environment varible `UNITTEST_FILTER`. See `README.md` for more on
// this.
//
// Another way to debug a particular test, is to copy that test into
// `experiments/testcase.cpp` and then run `sh build.sh
// check-testcase` (or one of its friends) from the command line.

extern unsigned int unit_test_random_seed;

TEST(Dictionary_Basics)
{
    Group g;
    auto cmp = [this](Mixed x, Mixed y) {
        CHECK_EQUAL(x, y);
    };

    Dictionary dummy;
    CHECK_THROW_ANY(dummy.insert("Hello", "world"));

    auto foo = g.add_table("foo");
    auto col_dict = foo->add_column_dictionary(type_Mixed, "dictionaries");

    Obj obj1 = foo->create_object();
    Obj obj2 = foo->create_object();
    StringData foo_key("foo.bar", 3); // The '.' must not be considered part of the key

    {
        Dictionary dict = obj1.get_dictionary(col_dict);

        CHECK_EQUAL(dict.size(), 0);
        CHECK_EQUAL(dict.find_any(9), realm::npos);

        CHECK(dict.insert("Hello", 9).second);
        CHECK_EQUAL(dict.size(), 1);
        CHECK_EQUAL(dict.get("Hello").get_int(), 9);
        CHECK(dict.contains("Hello"));
        CHECK_NOT(dict.insert("Hello", 10).second);
        CHECK_EQUAL(dict.get("Hello").get_int(), 10);
        CHECK_EQUAL(dict.find_any(9), realm::npos);
        CHECK_EQUAL(dict.find_any(10), 0);

        dict.insert("Goodbye", "cruel world");
        CHECK_EQUAL(dict.size(), 2);
        CHECK_EQUAL(dict["Goodbye"].get_string(), "cruel world");
        CHECK_THROW_ANY(dict.get("Baa").get_string()); // Within range
        CHECK_THROW_ANY(dict.get("Foo").get_string()); // Outside range
        CHECK_THROW_ANY(dict.insert("$foo", ""));      // Must not start with '$'
        CHECK_THROW_ANY(dict.insert("foo.bar", ""));   // Must not contain '.'
        CHECK(dict.insert(foo_key, 9).second);         // This should be ok
    }
    {
        Dictionary dict = obj1.get_dictionary(col_dict);
        CHECK_EQUAL(dict.size(), 3);
        cmp(dict.get("Hello"), 10);
        cmp(dict["Goodbye"], "cruel world");
        auto it = dict.find("puha");
        CHECK(it == dict.end());
        it = dict.find("Goodbye");
        cmp((*it).second, "cruel world");
        dict.erase(it);
        CHECK_EQUAL(dict.size(), 2);
        cmp(dict["Goodbye"], Mixed());
        CHECK_EQUAL(dict.size(), 3);
        dict.erase("foo");
        CHECK_EQUAL(dict.size(), 2);
        dict.clear();
        CHECK_EQUAL(dict.size(), 0);
        // Check that you can insert after clear
        CHECK(dict.insert("Hello", 9).second);
        CHECK_EQUAL(dict.size(), 1);
        dict.erase("Hello");
        CHECK_EQUAL(dict.size(), 0);
        CHECK_THROW_ANY(dict.erase("$foo"));    // Must not start with '$'
        CHECK_THROW_ANY(dict.erase("foo.bar")); // Must not contain '.'
    }
    {
        Dictionary dict1 = obj1.get_dictionary(col_dict);
        Dictionary dict2 = obj2.get_dictionary(col_dict);
        CHECK_EQUAL(dict2.size(), 0);
        CHECK_THROW_ANY(dict2.get("Baa").get_string());

        dict2.insert("Hello", "world");
        dict1.insert("Hello", 9);
        obj2.remove();
        CHECK_NOT(dict2.is_attached());
        CHECK_EQUAL(dict1.size(), 1);
        dict1 = dict2;
        CHECK_NOT(dict1.is_attached());
    }
}

TEST(Dictionary_Links)
{
    Group g;
    auto cmp = [this](Mixed x, Mixed y) {
        CHECK_EQUAL(x, y);
    };

    auto dogs = g.add_table_with_primary_key("dog", type_String, "name");
    auto cats = g.add_table_with_primary_key("cat", type_String, "name");
    auto persons = g.add_table_with_primary_key("person", type_String, "name");
    auto col_dict = persons->add_column_dictionary(*dogs, "dictionaries");

    Obj adam = persons->create_object_with_primary_key("adam");
    Obj bernie = persons->create_object_with_primary_key("bernie");
    Obj pluto = dogs->create_object_with_primary_key("pluto");
    Obj lady = dogs->create_object_with_primary_key("lady");
    Obj garfield = cats->create_object_with_primary_key("garfield");

    {
        Dictionary dict = adam.get_dictionary(col_dict);
        CHECK(dict.insert("Pet", pluto).second);
        CHECK_EQUAL(pluto.get_backlink_count(), 1);
        CHECK_NOT(dict.insert("Pet", lady).second);
        CHECK_EQUAL(pluto.get_backlink_count(), 0);
        CHECK_EQUAL(lady.get_backlink_count(*persons, col_dict), 1);
        CHECK_EQUAL(lady.get_backlink(*persons, col_dict, 0), adam.get_key());
        CHECK_EQUAL(lady.get_backlink_count(), 1);
        CHECK_EQUAL(dict.get("Pet").get<ObjKey>(), lady.get_key());
        lady.remove();
        cmp(dict["Pet"], Mixed());
        CHECK_THROW_ANY(dict.insert("Pet", garfield));
        CHECK_THROW_ANY(dict.insert("Pet", Mixed(ObjKey(27))));

        // Reinsert lady
        lady = dogs->create_object_with_primary_key("lady");
        dict.insert("Pet", lady);
        lady.invalidate(); // Make lady a tombstone :-(
        cmp(dict["Pet"], Mixed());
        lady = dogs->create_object_with_primary_key("lady");
        cmp(dict["Pet"], Mixed(lady.get_key()));

        auto invalid_link = pluto.get_link();
        pluto.remove();
        CHECK_THROW(dict.insert("Pet", invalid_link), LogicError);

        dict = bernie.get_dictionary(col_dict);
        dict.insert("Pet", lady);
        CHECK_EQUAL(lady.get_backlink_count(), 2);
        adam.remove();
        CHECK_EQUAL(lady.get_backlink_count(), 1);
        dict.erase("Pet");
        CHECK_EQUAL(lady.get_backlink_count(), 0);

        dict.insert("Pet", dogs->get_objkey_from_primary_key("pongo"));
        cmp(dict["Pet"], Mixed());
        Obj pongo = dogs->create_object_with_primary_key("pongo");
        CHECK_EQUAL(pongo.get_backlink_count(), 1);
        cmp(dict["Pet"], Mixed(pongo.get_key()));
    }
}

TEST(Dictionary_TypedLinks)
{
    Group g;
    auto cmp = [this](Mixed x, Mixed y) {
        CHECK_EQUAL(x, y);
    };

    auto dogs = g.add_table_with_primary_key("dog", type_String, "name");
    auto persons = g.add_table_with_primary_key("person", type_String, "name");
    auto col_dict = persons->add_column_dictionary(type_Mixed, "dictionaries");

    Obj adam = persons->create_object_with_primary_key("adam");
    Obj pluto = dogs->create_object_with_primary_key("pluto");
    Obj lady = dogs->create_object_with_primary_key("lady");

    {
        Dictionary dict = adam.get_dictionary(col_dict);
        CHECK(dict.insert("Pet", pluto).second);
        CHECK_EQUAL(pluto.get_backlink_count(), 1);
        CHECK_NOT(dict.insert("Pet", lady).second);
        CHECK_EQUAL(pluto.get_backlink_count(), 0);
        CHECK_EQUAL(lady.get_backlink_count(*persons, col_dict), 1);
        CHECK_EQUAL(lady.get_backlink(*persons, col_dict, 0), adam.get_key());
        CHECK_EQUAL(lady.get_backlink_count(), 1);
        lady.remove();
        cmp(dict["Pet"], Mixed());

        // Reinsert lady
        lady = dogs->create_object_with_primary_key("lady");
        dict.insert("Pet", lady);
        lady.invalidate(); // Make lady a tombstone :-(
        cmp(dict["Pet"], Mixed());
        lady = dogs->create_object_with_primary_key("lady");
        cmp(dict["Pet"], Mixed(lady.get_link()));

        auto invalid_link = pluto.get_link();
        pluto.remove();
        CHECK_THROW(dict.insert("Pet", invalid_link), LogicError);

        dict.insert("Pet", Mixed(ObjLink(dogs->get_key(), dogs->get_objkey_from_primary_key("pongo"))));
        cmp(dict["Pet"], Mixed());
        Obj pongo = dogs->create_object_with_primary_key("pongo");
        CHECK_EQUAL(pongo.get_backlink_count(), 1);
        cmp(dict["Pet"], Mixed(pongo.get_link()));
    }
}

TEST(Dictionary_Clear)
{
    Group g;
    auto dogs = g.add_table_with_primary_key("dog", type_String, "name");
    auto persons = g.add_table_with_primary_key("person", type_String, "name");
    auto col_dict_typed = persons->add_column_dictionary(type_TypedLink, "typed");
    auto col_dict_implicit = persons->add_column_dictionary(*dogs, "implicit");

    Obj adam = persons->create_object_with_primary_key("adam");
    Obj pluto = dogs->create_object_with_primary_key("pluto");
    Obj lady = dogs->create_object_with_primary_key("lady");

    adam.get_dictionary(col_dict_typed).insert("Dog1", pluto);
    adam.get_dictionary(col_dict_implicit).insert("DOg2", lady.get_key());

    CHECK_EQUAL(lady.get_backlink_count(), 1);
    CHECK_EQUAL(pluto.get_backlink_count(), 1);
    persons->clear();
    CHECK_EQUAL(lady.get_backlink_count(), 0);
    CHECK_EQUAL(pluto.get_backlink_count(), 0);
    g.verify();
}

TEST(Dictionary_Transaction)
{
    SHARED_GROUP_TEST_PATH(path);
    auto hist = make_in_realm_history(path);
    DBRef db = DB::create(*hist);
    ObjKey k0;
    ColKey col_dict;
    auto cmp = [this](Mixed x, Mixed y) {
        CHECK_EQUAL(x, y);
    };

    auto rt = db->start_read();
    {
        WriteTransaction wt(db);
        auto foo = wt.add_table("foo");
        col_dict = foo->add_column_dictionary(type_Mixed, "dictionaries");

        Obj obj1 = foo->create_object();
        Obj obj2 = foo->create_object();
        Dictionary dict = obj1.get_dictionary(col_dict);
        k0 = obj1.get_key();
        dict.insert("Hello", 9);
        dict.insert("Goodbye", "cruel world");

        wt.commit();
    }
    rt->advance_read();
    rt->verify();
    ConstTableRef table = rt->get_table("foo");
    Dictionary dict;
    dict = table->get_object(k0).get_dictionary(col_dict);
    cmp(dict.get("Hello"), 9);
    cmp(dict.get("Goodbye"), "cruel world");

    {
        WriteTransaction wt(db);
        auto foo = wt.get_table("foo");
        Dictionary d = foo->get_object(k0).get_dictionary(col_dict);
        d.insert("Good morning", "sunshine");

        wt.commit();
    }
    rt->advance_read();
    rt->verify();
    // rt->to_json(std::cout);
    cmp(dict.get("Good morning"), "sunshine");

    {
        auto wt = db->start_write();
        auto foo = wt->get_table("foo");
        Dictionary d = foo->get_object(k0).get_dictionary(col_dict);
        d.clear();

        wt->commit_and_continue_as_read();
        wt->promote_to_write();
        wt->verify();
    }
}

TEST(Dictionary_Aggregate)
{
    SHARED_GROUP_TEST_PATH(path);
    auto hist = make_in_realm_history(path);
    DBRef db = DB::create(*hist);
    auto tr = db->start_write();
    auto foo = tr->add_table("foo");
    auto col_dict = foo->add_column_dictionary(type_Int, "dictionaries");

    Obj obj1 = foo->create_object();
    Dictionary dict = obj1.get_dictionary(col_dict);
    std::vector<int64_t> random_idx(100);
    std::iota(random_idx.begin(), random_idx.end(), 0);
    std::shuffle(random_idx.begin(), random_idx.end(), std::mt19937(unit_test_random_seed));

    for (int i = 0; i < 100; i++) {
        dict.insert(util::to_string(i), random_idx[i]);
    }

    std::vector<size_t> indices;
    dict.sort(indices, true);
    int64_t last = -1;
    for (size_t ndc : indices) {
        int64_t val = dict.get_any(ndc).get_int();
        CHECK_GREATER(val, last);
        last = val;
    }
    tr->commit_and_continue_as_read();

    size_t ndx;
    auto max = dict.max(&ndx);
    CHECK(max);
    CHECK_EQUAL(max->get_int(), 99);

    auto min = dict.min(&ndx);
    CHECK(min);
    CHECK_EQUAL(min->get_int(), 0);

    size_t cnt;
    auto sum = dict.sum(&cnt);
    CHECK(sum);
    CHECK_EQUAL(cnt, 100);
    CHECK_EQUAL(sum->get_int(), 50 * 99);

    auto avg = dict.avg(&cnt);
    CHECK(avg);
    CHECK_EQUAL(cnt, 100);
    CHECK_EQUAL(avg->get_double(), double(50 * 99) / 100);
}

TEST(Dictionary_Performance)
{
    size_t nb_reps = 1000;

    Group g;
    auto foo = g.add_table("foo");
    auto col_dict = foo->add_column_dictionary(type_Int, "dictionaries", false, type_Int);

    Obj obj1 = foo->create_object();
    Dictionary dict = obj1.get_dictionary(col_dict);
    std::vector<int64_t> random_idx(nb_reps);
    std::iota(random_idx.begin(), random_idx.end(), 0);
    std::shuffle(random_idx.begin(), random_idx.end(), std::mt19937(unit_test_random_seed));

    auto t1 = steady_clock::now();
    CALLGRIND_START_INSTRUMENTATION;

    for (auto i : random_idx) {
        dict.insert(i, i);
    }

    CALLGRIND_STOP_INSTRUMENTATION;
    auto t2 = steady_clock::now();

    for (auto i : random_idx) {
        CHECK_EQUAL(dict.get(i), Mixed(i));
    }

    auto t3 = steady_clock::now();

    std::cout << nb_reps << " values in dictionary" << std::endl;
    std::cout << "    insertion: " << duration_cast<nanoseconds>(t2 - t1).count() / nb_reps << " ns/val" << std::endl;
    std::cout << "    lookup: " << duration_cast<nanoseconds>(t3 - t2).count() / nb_reps << " ns/val" << std::endl;
}

TEST(Dictionary_Tombstones)
{
    Group g;
    auto foos = g.add_table_with_primary_key("class_Foo", type_Int, "id");
    auto bars = g.add_table_with_primary_key("class_Bar", type_String, "id");
    auto col_int = bars->add_column(type_Int, "value");
    ColKey col_dict = foos->add_column_dictionary(*bars, "dict");

    auto foo = foos->create_object_with_primary_key(123);
    auto a = bars->create_object_with_primary_key("a").set(col_int, 1);
    auto b = bars->create_object_with_primary_key("b").set(col_int, 2);

    auto dict = foo.get_dictionary(col_dict);
    dict.insert("a", a);
    dict.insert("b", b);

    auto q = bars->where(dict).equal(col_int, 1);
    CHECK_EQUAL(q.count(), 1);

    a.invalidate();

    CHECK_EQUAL(dict.size(), 2);
    CHECK((*dict.find("a")).second.is_unresolved_link());

    CHECK(dict.find("b") != dict.end());

    CHECK_EQUAL(q.count(), 0);
}

TEST(Dictionary_UseAfterFree)
{
    Group g;
    auto foos = g.add_table("Foo");
    ColKey col_dict = foos->add_column_dictionary(type_String, "dict");

    auto foo = foos->create_object();
    auto dict = foo.get_dictionary(col_dict);
    dict.insert("a", "monkey");
    dict.insert("b", "lion");
    dict.insert("c", "à");

    Query q;
    {
        auto str = std::make_unique<std::string>("à");
        auto col = foos->column<Dictionary>(col_dict);
        q = col.equal(StringData(*str), true); // A copy of the string must be taken here
    }
    CHECK_EQUAL(q.count(), 1);
}

TEST(Dictionary_HashCollision)
{
    constexpr int64_t nb_entries = 100;
    auto mask = Dictionary::set_hash_mask(0xFF);
    Group g;
    auto foos = g.add_table("Foo");
    ColKey col_dict = foos->add_column_dictionary(type_Int, "dict");

    auto foo = foos->create_object();
    auto dict = foo.get_dictionary(col_dict);
    for (int64_t i = 0; i < nb_entries; i++) {
        std::string key = "key" + util::to_string(i);
        dict.insert(Mixed(key), i);
        dict.erase(key);
        dict.insert(Mixed(key), i);
    }

    // g.to_json(std::cout);

    // Check that values can be read back
    for (int64_t i = 0; i < nb_entries; i++) {
        std::string key = "key" + util::to_string(i);
        CHECK_EQUAL(dict[key].get_int(), i);
    }

    // Check that a query can find matching key and value
    for (int64_t i = 0; i < nb_entries; i++) {
        std::string key = "key" + util::to_string(i);
        Query q = (foos->column<Dictionary>(col_dict).key(key) == Mixed(i));
        CHECK_EQUAL(q.count(), 1);
    }

    // Check that dict.find works
    for (int64_t i = 0; i < nb_entries; i++) {
        std::string key = "key" + util::to_string(i);
        auto it = dict.find(key);
        CHECK_EQUAL((*it).second.get_int(), i);
    }

    auto check_aggregates = [&]() {
        int64_t expected_sum = nb_entries * (nb_entries - 1) / 2;
        size_t count = 0;
        util::Optional<Mixed> actual_sum = dict.sum(&count);
        CHECK(actual_sum);
        CHECK(actual_sum && *actual_sum == Mixed{expected_sum});
        CHECK_EQUAL(count, nb_entries);
        Query q = (foos->column<Dictionary>(col_dict).sum() == Mixed(expected_sum));
        CHECK_EQUAL(q.count(), 1);

        util::Optional<Mixed> actual_min = dict.min();
        CHECK(actual_min && *actual_min == Mixed{0});
        q = (foos->column<Dictionary>(col_dict).min() == Mixed(0));
        CHECK_EQUAL(q.count(), 1);

        util::Optional<Mixed> actual_max = dict.max();
        CHECK(actual_max && *actual_max == Mixed{nb_entries - 1});
        q = (foos->column<Dictionary>(col_dict).max() == Mixed(nb_entries - 1));
        CHECK_EQUAL(q.count(), 1);

        util::Optional<Mixed> actual_avg = dict.avg(&count);
        Mixed expected_avg{Decimal128(expected_sum) / nb_entries};
        CHECK_EQUAL(count, nb_entries);
        CHECK(actual_avg && *actual_avg == expected_avg);
        q = (foos->column<Dictionary>(col_dict).average() == expected_avg);
        CHECK_EQUAL(q.count(), 1);
    };

    check_aggregates();

    // Update with new values
    for (int64_t i = 0; i < nb_entries; i++) {
        std::string key = "key" + util::to_string(i);
        dict.insert(Mixed(key), nb_entries - i - 1);
    }

    check_aggregates();

    // Check that values was updated properly
    for (int64_t i = 0; i < nb_entries; i++) {
        std::string key = "key" + util::to_string(i);
        CHECK_EQUAL(dict[key].get_int(), nb_entries - i - 1);
    }

    // Now erase one entry at a time and check that the rest of the values are ok
    for (int64_t i = 0; i < nb_entries; i++) {
        std::string key = "key" + util::to_string(i);
        dict.erase(key);
        CHECK_EQUAL(dict.size(), nb_entries - i - 1);

        // Check that remaining entries still can be found
        for (int64_t j = i + 1; j < nb_entries; j++) {
            std::string key_j = "key" + util::to_string(j);
            CHECK_EQUAL(dict[key_j].get_int(), nb_entries - j - 1);
        }
    }
    Dictionary::set_hash_mask(mask);
}

TEST(Dictionary_HashCollisionTransaction)
{
    auto mask = Dictionary::set_hash_mask(0xFF);
    SHARED_GROUP_TEST_PATH(path);
    auto hist = make_in_realm_history(path);
    DBRef db = DB::create(*hist);

    {
        auto tr = db->start_write();
        auto foos = tr->add_table("Foo");
        ColKey col_dict = foos->add_column_dictionary(type_Int, "dict");

        auto foo = foos->create_object();
        auto dict = foo.get_dictionary(col_dict);
        for (int64_t i = 0; i < 100; i++) {
            std::string key = "key" + util::to_string(i);
            dict.insert(Mixed(key), i);
        }
        tr->commit();
    }

    {
        auto rt = db->start_read();
        auto foos = rt->get_table("Foo");
        ColKey col_dict = foos->get_column_key("dict");
        auto dict = foos->begin()->get_dictionary(col_dict);
        for (int64_t i = 0; i < 100; i++) {
            std::string key = "key" + util::to_string(i);
            CHECK_EQUAL(dict[key].get_int(), i);
        }
    }

    auto rt = db->start_read();
    for (int64_t i = 0; i < 100; i++) {
        rt->promote_to_write();

        auto foos = rt->get_table("Foo");
        ColKey col_dict = foos->get_column_key("dict");
        auto dict = foos->begin()->get_dictionary(col_dict);

        std::string key = "key" + util::to_string(i);
        dict.erase(key);
        CHECK_EQUAL(dict.size(), 100 - i - 1);

        rt->commit_and_continue_as_read();

        for (int64_t j = i + 1; j < 100; j++) {
            std::string key_j = "key" + util::to_string(j);
            CHECK_EQUAL(dict[key_j].get_int(), j);
        }
    }
    Dictionary::set_hash_mask(mask);
}
