/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
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

#include <limits>
#include <iostream>

#include <realm/array_integer.hpp>
#include <realm/array_ref.hpp>
#include <realm/column_integer.hpp>
#include <realm/array_integer_tpl.hpp>
#include <realm/query_conditions.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::test_util;

// #define ARRAY_PERFORMANCE_TESTING
#if !defined(REALM_DEBUG) && defined(ARRAY_PERFORMANCE_TESTING)
NONCONCURRENT_TEST(perf_array_encode_get_vs_array_get_less_32bit)
{
    using namespace std;
    using namespace std::chrono;
    size_t n_values = 1000;
    size_t n_runs = 100;
    std::cout << "   < 32 bit values " << std::endl;
    std::cout << "   N values = " << n_values << std::endl;
    std::cout << "   N runs = " << n_runs << std::endl;

    std::vector<int64_t> input_array;
    ArrayInteger a(Allocator::get_default());
    ArrayInteger a_encoded(Allocator::get_default());
    a.create();

    for (size_t i = 0; i < n_values; i++)
        input_array.push_back(i);
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(input_array.begin(), input_array.end(), g);
    for (const auto& v : input_array)
        a.add(v);

    auto t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i)
            REALM_ASSERT(a.get(i) == input_array[i]);
    }
    auto t2 = high_resolution_clock::now();

    std::cout << "   Positive values - Array::get(): " << duration_cast<nanoseconds>(t2 - t1).count() << " ns"
              << std::endl;
    std::cout << "   Positive values - Array::get(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    a.try_encode(a_encoded);
    CHECK(a_encoded.is_encoded());
    CHECK(a_encoded.size() == a.size());
    t1 = high_resolution_clock::now();

    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            REALM_ASSERT(a_encoded.get(i) == a.get(i));
        }
    }
    t2 = high_resolution_clock::now();
    std::cout << "   Positive values - ArrayEncode::get(): " << duration_cast<nanoseconds>(t2 - t1).count() << " ns"
              << std::endl;
    std::cout << "   Positive values - ArrayEncode::get(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    a.destroy();
    a_encoded.destroy();
    a.create();
    input_array.clear();
    for (size_t i = 0; i < n_values; i++)
        input_array.push_back(-i);
    std::random_device rd1;
    std::mt19937 g1(rd1());
    std::shuffle(input_array.begin(), input_array.end(), g1);
    for (const auto& v : input_array)
        a.add(v);

    t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i)
            REALM_ASSERT(a.get(i) == input_array[i]);
    }
    t2 = high_resolution_clock::now();

    std::cout << std::endl;

    std::cout << "   Negative values - Array::get(): " << duration_cast<nanoseconds>(t2 - t1).count() << " ns"
              << std::endl;
    std::cout << "   Negative values - Array::get(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    a.try_encode(a_encoded);
    CHECK(a_encoded.is_encoded());
    CHECK(a_encoded.size() == a.size());
    t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            REALM_ASSERT(a_encoded.get(i) == a.get(i));
        }
    }
    t2 = high_resolution_clock::now();
    std::cout << "   Negative values - ArrayEncode::get(): " << duration_cast<nanoseconds>(t2 - t1).count() << " ns"
              << std::endl;
    std::cout << "   Negative values - ArrayEncode::get(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    a.destroy();
    a_encoded.destroy();
}


NONCONCURRENT_TEST(Test_basic_find_EQ_less_32bit)
{
    using namespace std;
    using namespace std::chrono;
    size_t n_values = 1000;
    size_t n_runs = 100;
    std::cout << "   Value with bitwidth < 32 " << std::endl;
    std::cout << "   N values = " << n_values << std::endl;
    std::cout << "   N runs = " << n_runs << std::endl;

    std::vector<int64_t> input_array;
    ArrayInteger a(Allocator::get_default());
    ArrayInteger a_encoded(Allocator::get_default());
    a.create();

    for (size_t i = 0; i < n_values; i++)
        input_array.push_back(i);
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(input_array.begin(), input_array.end(), g);
    for (const auto& v : input_array)
        a.add(v);

    auto t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            auto ndx = a.find_first(input_array[i]);
            REALM_ASSERT(ndx != realm::not_found);
            REALM_ASSERT(a.get(ndx) == input_array[ndx]);
        }
    }
    auto t2 = high_resolution_clock::now();

    std::cout << "   Positive values - Array::find<Equal>(): " << duration_cast<milliseconds>(t2 - t1).count()
              << " ms" << std::endl;
    std::cout << "   Positive values - Array::find<Equal>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    a.try_encode(a_encoded);
    CHECK(a_encoded.is_encoded());
    CHECK(a_encoded.size() == a.size());

    // verify that both find the same thing
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            auto v = a.find_first(input_array[i]);
            auto v1 = a_encoded.find_first(input_array[i]);
            REALM_ASSERT(v == v1);
        }
    }

    t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            auto ndx = a_encoded.find_first(input_array[i]);
            REALM_ASSERT(ndx != realm::not_found);
            REALM_ASSERT(a_encoded.get(ndx) == input_array[ndx]);
        }
    }
    t2 = high_resolution_clock::now();
    std::cout << "   Positive values - ArrayEncode::find<Equal>(): " << duration_cast<milliseconds>(t2 - t1).count()
              << " ms" << std::endl;
    std::cout << "   Positive values - ArrayEncode::find<Equal>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    std::cout << std::endl;

    a.destroy();
    a_encoded.destroy();
    a.create();
    input_array.clear();
    for (size_t i = 0; i < n_values; i++)
        input_array.push_back(-i);
    std::random_device rd1;
    std::mt19937 g1(rd1());
    std::shuffle(input_array.begin(), input_array.end(), g1);
    for (const auto& v : input_array)
        a.add(v);

    a.try_encode(a_encoded);
    CHECK(a_encoded.is_encoded());
    CHECK(a_encoded.size() == a.size());

    // verify that both find the same thing
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            auto v = a.find_first(input_array[i]);
            auto v1 = a_encoded.find_first(input_array[i]);
            REALM_ASSERT(v == v1);
        }
    }

    t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            auto ndx = a.find_first(input_array[i]);
            REALM_ASSERT(ndx != realm::not_found);
            REALM_ASSERT(a.get(ndx) == input_array[ndx]);
        }
    }
    t2 = high_resolution_clock::now();

    std::cout << "   Negative values - Array::find<Equal>(): " << duration_cast<milliseconds>(t2 - t1).count()
              << " ms" << std::endl;
    std::cout << "   Negative values - Array::find<Equal>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            auto ndx = a_encoded.find_first(input_array[i]);
            REALM_ASSERT(ndx != realm::not_found);
            REALM_ASSERT(a_encoded.get(ndx) == a.get(ndx));
        }
    }
    t2 = high_resolution_clock::now();
    std::cout << "   Negative values - ArrayEncode::find<Equal>(): " << duration_cast<milliseconds>(t2 - t1).count()
              << " ms" << std::endl;
    std::cout << "   Negative values - ArrayEncode::find<Equal>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    a.destroy();
    a_encoded.destroy();
}

NONCONCURRENT_TEST(Test_basic_find_NEQ_value_less_32bit)
{
    using namespace std;
    using namespace std::chrono;
    size_t n_values = 1000;
    size_t n_runs = 100;
    std::cout << "   Value with bitwidth < 32 " << std::endl;
    std::cout << "   N values = " << n_values << std::endl;
    std::cout << "   N runs = " << n_runs << std::endl;

    std::vector<int64_t> input_array;
    ArrayInteger a(Allocator::get_default());
    ArrayInteger a_encoded(Allocator::get_default());
    a.create();

    for (size_t i = 0; i < n_values; i++)
        input_array.push_back(i);
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(input_array.begin(), input_array.end(), g);
    for (const auto& v : input_array)
        a.add(v);

    QueryStateFindFirst state1;
    QueryStateFindFirst state2;
    auto t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            a.find<NotEqual>(i, 0, a.size(), &state1);
            REALM_ASSERT(state1.m_state != realm::not_found);
            REALM_ASSERT(a.get(state1.m_state) == input_array[state1.m_state]);
        }
    }
    auto t2 = high_resolution_clock::now();

    std::cout << "   Positive values - Array::find<NotEqual>(): " << duration_cast<milliseconds>(t2 - t1).count()
              << " ms" << std::endl;
    std::cout << "   Positive values - Array::find<NotEqual>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    a.try_encode(a_encoded);
    CHECK(a_encoded.is_encoded());
    CHECK(a_encoded.size() == a.size());

    // verify that both find the same thing
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            a.find<NotEqual>(i, 0, a.size(), &state1);
            a_encoded.find<NotEqual>(i, 0, a_encoded.size(), &state2);
            REALM_ASSERT(state1.m_state == state2.m_state);
        }
    }

    t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            a_encoded.find<NotEqual>(i, 0, a_encoded.size(), &state2);
            REALM_ASSERT(state2.m_state != realm::not_found);
            REALM_ASSERT(a_encoded.get(state2.m_state) == a.get(state2.m_state));
        }
    }
    t2 = high_resolution_clock::now();
    std::cout << "   Positive values - ArrayEncode::find<NotEqual>(): "
              << duration_cast<milliseconds>(t2 - t1).count() << " ms" << std::endl;
    std::cout << "   Positive values - ArrayEncode::find<NotEqual>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    std::cout << std::endl;

    a.destroy();
    a_encoded.destroy();
    a.create();
    input_array.clear();
    for (size_t i = 0; i < n_values; i++)
        input_array.push_back(-i);
    std::random_device rd1;
    std::mt19937 g1(rd1());
    std::shuffle(input_array.begin(), input_array.end(), g1);
    for (const auto& v : input_array)
        a.add(v);

    a.try_encode(a_encoded);
    CHECK(a_encoded.is_encoded());
    CHECK(a_encoded.size() == a.size());

    // NEQ for signed integers is not working. TODO: investigate this.
    // verify that both find the same thing

    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            a.find<NotEqual>(-i, 0, a.size(), &state1);
            a_encoded.find<NotEqual>(-i, 0, a_encoded.size(), &state2);
            REALM_ASSERT(state1.m_state == state2.m_state);
        }
    }

    t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            a.find<NotEqual>(-i, 0, a.size(), &state1);
            REALM_ASSERT(state1.m_state != realm::not_found);
            REALM_ASSERT(a.get(state1.m_state) == input_array[state1.m_state]);
        }
    }
    t2 = high_resolution_clock::now();

    std::cout << "   Negative values - Array::find<NotEqual>(): " << duration_cast<milliseconds>(t2 - t1).count()
              << " ms" << std::endl;
    std::cout << "   Negative values - Array::find<NotEqual>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            a_encoded.find<NotEqual>(-i, 0, a_encoded.size(), &state2);
            REALM_ASSERT(state2.m_state != realm::not_found);
            REALM_ASSERT(a_encoded.get(state2.m_state) == a.get(state2.m_state));
        }
    }
    t2 = high_resolution_clock::now();
    std::cout << "   Negative values - ArrayEncode::find<NotEqual>(): "
              << duration_cast<milliseconds>(t2 - t1).count() << " ms" << std::endl;
    std::cout << "   Negative values - ArrayEncode::find<NotEqual>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    a.destroy();
    a_encoded.destroy();
}

NONCONCURRENT_TEST(Test_basic_find_LT_value_less_32bit)
{
    using namespace std;
    using namespace std::chrono;
    size_t n_values = 1000;
    size_t n_runs = 100;
    std::cout << "   Value with bitwidth < 32 " << std::endl;
    std::cout << "   N values = " << n_values << std::endl;
    std::cout << "   N runs = " << n_runs << std::endl;

    std::vector<int64_t> input_array;
    ArrayInteger a(Allocator::get_default());
    ArrayInteger a_encoded(Allocator::get_default());
    a.create();

    for (size_t i = 0; i < n_values; i++)
        input_array.push_back(i);
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(input_array.begin(), input_array.end(), g);
    for (const auto& v : input_array)
        a.add(v);

    QueryStateFindFirst state1{};
    QueryStateFindFirst state2{};
    auto t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 1; i < n_values; ++i) { // there is nothing less than 0
            a.find<Less>(i, 0, a.size(), &state1);
            REALM_ASSERT(state1.m_state != realm::not_found);
            REALM_ASSERT(a.get(state1.m_state) == input_array[state1.m_state]);
        }
    }
    auto t2 = high_resolution_clock::now();

    std::cout << "   Positive values - Array::find<Less>(): " << duration_cast<milliseconds>(t2 - t1).count() << " ms"
              << std::endl;
    std::cout << "   Positive values - Array::find<Less>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    a.try_encode(a_encoded);
    CHECK(a_encoded.is_encoded());
    CHECK(a_encoded.size() == a.size());

    // verify that both find the same thing
    state1 = {};
    state2 = {};
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            a.find<Less>(i, 0, a.size(), &state1);
            a_encoded.find<Less>(i, 0, a_encoded.size(), &state2);
            REALM_ASSERT(state1.m_state == state2.m_state);
        }
    }

    t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 1; i < n_values; ++i) { // there is nothing less than 0
            a_encoded.find<Less>(i, 0, a_encoded.size(), &state2);
            REALM_ASSERT(state2.m_state != realm::not_found);
            REALM_ASSERT(a_encoded.get(state2.m_state) == a.get(state2.m_state));
        }
    }
    t2 = high_resolution_clock::now();
    std::cout << "   Positive values - ArrayEncode::find<Less>(): " << duration_cast<milliseconds>(t2 - t1).count()
              << " ms" << std::endl;
    std::cout << "   Positive values - ArrayEncode::find<Less>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    std::cout << std::endl;

    a.destroy();
    a_encoded.destroy();
    a.create();
    input_array.clear();
    for (size_t i = 0; i < n_values; i++)
        input_array.push_back(-i);
    std::random_device rd1;
    std::mt19937 g1(rd1());
    std::shuffle(input_array.begin(), input_array.end(), g1);
    for (const auto& v : input_array)
        a.add(v);

    a.try_encode(a_encoded);
    CHECK(a_encoded.is_encoded());
    CHECK(a_encoded.size() == a.size());

    // verify that both find the same thing
    state1 = {};
    state2 = {};
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            a.find<Less>(-i, 0, a.size(), &state1);
            a_encoded.find<Less>(-i, 0, a_encoded.size(), &state2);
            REALM_ASSERT(state1.m_state == state2.m_state);
        }
    }

    t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values - 1; ++i) { // nothing less than the biggest negative number
            a.find<Less>(-i, 0, a.size(), &state1);
            REALM_ASSERT(state1.m_state != realm::not_found);
            REALM_ASSERT(a.get(state1.m_state) == input_array[state1.m_state]);
        }
    }
    t2 = high_resolution_clock::now();

    std::cout << "   Negative values - Array::find<Less>(): " << duration_cast<milliseconds>(t2 - t1).count() << " ms"
              << std::endl;
    std::cout << "   Negative values - Array::find<Less>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values - 1; ++i) { // nothing less than the biggest negative number
            a_encoded.find<Less>(-i, 0, a_encoded.size(), &state2);
            REALM_ASSERT(state2.m_state != realm::not_found);
            REALM_ASSERT(a_encoded.get(state2.m_state) == a.get(state2.m_state));
        }
    }
    t2 = high_resolution_clock::now();
    std::cout << "   Negative values - ArrayEncode::find<Less>(): " << duration_cast<milliseconds>(t2 - t1).count()
              << " ms" << std::endl;
    std::cout << "   Negative values - ArrayEncode::find<Less>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    a.destroy();
    a_encoded.destroy();
}

NONCONCURRENT_TEST(Test_basic_find_GT_value_less_32bit)
{
    // GT subword parallel search is not working... TODO : investigate
    using namespace std;
    using namespace std::chrono;
    size_t n_values = 1000;
    size_t n_runs = 100;
    std::cout << "   Value with bitwidth < 32 " << std::endl;
    std::cout << "   N values = " << n_values << std::endl;
    std::cout << "   N runs = " << n_runs << std::endl;

    std::vector<int64_t> input_array;
    ArrayInteger a(Allocator::get_default());
    ArrayInteger a_encoded(Allocator::get_default());
    a.create();

    for (size_t i = 0; i < n_values; i++)
        input_array.push_back(i);
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(input_array.begin(), input_array.end(), g);
    for (const auto& v : input_array)
        a.add(v);

    QueryStateFindFirst state1;
    QueryStateFindFirst state2;
    auto t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values - 1; ++i) { // nothing greatest than the last number
            a.find<Greater>(i, 0, a.size(), &state1);
            REALM_ASSERT(state1.m_state != realm::not_found);
            REALM_ASSERT(a.get(state1.m_state) == input_array[state1.m_state]);
        }
    }
    auto t2 = high_resolution_clock::now();

    std::cout << "   Positive values - Array::find<Greater>(): " << duration_cast<milliseconds>(t2 - t1).count()
              << " ms" << std::endl;
    std::cout << "   Positive values - Array::find<Greater>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    a.try_encode(a_encoded);
    CHECK(a_encoded.is_encoded());
    CHECK(a_encoded.size() == a.size());

    // verify that both find the same thing
    state1 = {};
    state2 = {};
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            a.find<Greater>(i, 0, a.size(), &state1);
            a_encoded.find<Greater>(i, 0, a_encoded.size(), &state2);
            REALM_ASSERT(state1.m_state == state2.m_state);
        }
    }

    t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values - 1; ++i) { // nothing bigger than the last val
            a_encoded.find<Greater>(i, 0, a_encoded.size(), &state2);
            REALM_ASSERT(state2.m_state != realm::not_found);
            REALM_ASSERT(a_encoded.get(state2.m_state) == a.get(state2.m_state));
        }
    }
    t2 = high_resolution_clock::now();
    std::cout << "   Positive values - ArrayEncode::find<Greater>(): " << duration_cast<milliseconds>(t2 - t1).count()
              << " ms" << std::endl;
    std::cout << "   Positive values - ArrayEncode::find<Greater>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    std::cout << std::endl;

    a.destroy();
    a_encoded.destroy();
    a.create();
    input_array.clear();
    for (size_t i = 0; i < n_values; i++)
        input_array.push_back(-i);
    std::random_device rd1;
    std::mt19937 g1(rd1());
    std::shuffle(input_array.begin(), input_array.end(), g1);
    for (const auto& v : input_array)
        a.add(v);

    a.try_encode(a_encoded);
    CHECK(a_encoded.is_encoded());
    CHECK(a_encoded.size() == a.size());

    // verify that both find the same thing
    state1 = {};
    state2 = {};
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            a.find<Greater>(-i, 0, a.size(), &state1);
            a_encoded.find<Greater>(-i, 0, a_encoded.size(), &state2);
            REALM_ASSERT(state1.m_state == state2.m_state);
        }
    }

    t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 1; i < n_values; ++i) { // nothing bigger than 0
            a.find<Greater>(-i, 0, a.size(), &state1);
            REALM_ASSERT(state1.m_state != realm::not_found);
            REALM_ASSERT(a.get(state1.m_state) == input_array[state1.m_state]);
        }
    }
    t2 = high_resolution_clock::now();

    std::cout << "   Negative values - Array::find<Greater>(): " << duration_cast<milliseconds>(t2 - t1).count()
              << " ms" << std::endl;
    std::cout << "   Negative values - Array::find<Greater>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 1; i < n_values; ++i) { // nothing bigger than 0
            a_encoded.find<Greater>(-i, 0, a_encoded.size(), &state2);
            REALM_ASSERT(state2.m_state != realm::not_found);
            REALM_ASSERT(a_encoded.get(state2.m_state) == a.get(state2.m_state));
        }
    }
    t2 = high_resolution_clock::now();
    std::cout << "   Negative values - ArrayEncode::find<Greater>(): " << duration_cast<milliseconds>(t2 - t1).count()
              << " ms" << std::endl;
    std::cout << "   Negative values - ArrayEncode::find<Greater>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    a.destroy();
    a_encoded.destroy();
}

NONCONCURRENT_TEST(perf_array_encode_get_vs_array_get_greater_32bit)
{
    using namespace std;
    using namespace std::chrono;
    size_t start_value = 0x0000000100000000; // 32 bit val
    size_t n_values = 1000;
    size_t n_runs = 100;
    std::cout << "   >= 32 bit values " << std::endl;
    std::cout << "   N values = " << n_values << std::endl;
    std::cout << "   N runs = " << n_runs << std::endl;

    std::vector<int64_t> input_array;
    ArrayInteger a(Allocator::get_default());
    ArrayInteger a_encoded(Allocator::get_default());
    a.create();

    for (size_t i = 0; i < n_values; i++)
        input_array.push_back(start_value + i);
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(input_array.begin(), input_array.end(), g);
    for (const auto& v : input_array)
        a.add(v);

    auto t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i)
            REALM_ASSERT(a.get(i) == input_array[i]);
    }
    auto t2 = high_resolution_clock::now();

    std::cout << "   Positive values - Array::get(): " << duration_cast<nanoseconds>(t2 - t1).count() << " ns"
              << std::endl;
    std::cout << "   Positive values - Array::get(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    a.try_encode(a_encoded);
    CHECK(a_encoded.is_encoded());
    CHECK(a_encoded.size() == a.size());
    t1 = high_resolution_clock::now();

    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            REALM_ASSERT(a_encoded.get(i) == a.get(i));
        }
    }
    t2 = high_resolution_clock::now();
    std::cout << "   Positive values - ArrayEncode::get(): " << duration_cast<nanoseconds>(t2 - t1).count() << " ns"
              << std::endl;
    std::cout << "   Positive values - ArrayEncode::get(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    a.destroy();
    a_encoded.destroy();
    a.create();
    input_array.clear();
    for (size_t i = 0; i < n_values; i++)
        input_array.push_back(-i);
    std::random_device rd1;
    std::mt19937 g1(rd1());
    std::shuffle(input_array.begin(), input_array.end(), g1);
    for (const auto& v : input_array)
        a.add(v);

    t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i)
            REALM_ASSERT(a.get(i) == input_array[i]);
    }
    t2 = high_resolution_clock::now();

    std::cout << std::endl;

    std::cout << "   Negative values - Array::get(): " << duration_cast<nanoseconds>(t2 - t1).count() << " ns"
              << std::endl;
    std::cout << "   Negative values - Array::get(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    a.try_encode(a_encoded);
    CHECK(a_encoded.is_encoded());
    CHECK(a_encoded.size() == a.size());
    t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            REALM_ASSERT(a_encoded.get(i) == a.get(i));
        }
    }
    t2 = high_resolution_clock::now();
    std::cout << "   Negative values - ArrayEncode::get(): " << duration_cast<nanoseconds>(t2 - t1).count() << " ns"
              << std::endl;
    std::cout << "   Negative values - ArrayEncode::get(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    a.destroy();
    a_encoded.destroy();
}

NONCONCURRENT_TEST(Test_basic_find_EQ_greater_32bit)
{
    using namespace std;
    using namespace std::chrono;
    size_t start_value = 0x000001000000000; // 32 bit val
    size_t n_values = 1000;
    size_t n_runs = 100;
    std::cout << "   Value with bitwidth >= 32 " << std::endl;
    std::cout << "   N values = " << n_values << std::endl;
    std::cout << "   N runs = " << n_runs << std::endl;

    std::vector<int64_t> input_array;
    ArrayInteger a(Allocator::get_default());
    ArrayInteger a_encoded(Allocator::get_default());
    a.create();

    for (size_t i = 0; i < n_values; i++)
        input_array.push_back(start_value + i);
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(input_array.begin(), input_array.end(), g);
    for (const auto& v : input_array)
        a.add(v);

    auto t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            auto ndx = a.find_first(start_value + i);
            REALM_ASSERT(ndx != realm::not_found);
            REALM_ASSERT(a.get(ndx) == input_array[ndx]);
        }
    }
    auto t2 = high_resolution_clock::now();

    std::cout << "   Positive values - Array::find<Equal>(): " << duration_cast<milliseconds>(t2 - t1).count()
              << " ms" << std::endl;
    std::cout << "   Positive values - Array::find<Equal>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    a.try_encode(a_encoded);
    CHECK(a_encoded.is_encoded());
    CHECK(a_encoded.size() == a.size());

    // verify that both find the same thing
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            REALM_ASSERT(a.find_first(start_value + i) == a_encoded.find_first(start_value + i));
        }
    }

    t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            auto ndx = a_encoded.find_first(start_value + i);
            REALM_ASSERT(ndx != realm::not_found);
            REALM_ASSERT(a_encoded.get(ndx) == a.get(ndx));
        }
    }
    t2 = high_resolution_clock::now();
    std::cout << "   Positive values - ArrayEncode::find<Equal>(): " << duration_cast<milliseconds>(t2 - t1).count()
              << " ms" << std::endl;
    std::cout << "   Positive values - ArrayEncode::find<Equal>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    std::cout << std::endl;

    a.destroy();
    a_encoded.destroy();
    a.create();
    input_array.clear();
    for (size_t i = 0; i < n_values; i++)
        input_array.push_back(-(start_value + i));
    std::random_device rd1;
    std::mt19937 g1(rd1());
    std::shuffle(input_array.begin(), input_array.end(), g1);
    for (const auto& v : input_array)
        a.add(v);

    a.try_encode(a_encoded);
    CHECK(a_encoded.is_encoded());
    CHECK(a_encoded.size() == a.size());

    // verify that both find the same thing
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            const auto k = -(start_value + i);
            const auto v1 = a.find_first(k);
            const auto v2 = a_encoded.find_first(k);
            REALM_ASSERT(v1 == v2);
        }
    }

    t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            auto ndx = a.find_first(-(start_value + i));
            REALM_ASSERT(ndx != realm::not_found);
            REALM_ASSERT(a.get(ndx) == input_array[ndx]);
        }
    }
    t2 = high_resolution_clock::now();

    std::cout << "   Negative values - Array::find<Equal>(): " << duration_cast<milliseconds>(t2 - t1).count()
              << " ms" << std::endl;
    std::cout << "   Negative values - Array::find<Equal>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            auto ndx = a_encoded.find_first(-(start_value + i));
            REALM_ASSERT(ndx != realm::not_found);
            REALM_ASSERT(a_encoded.get(ndx) == a.get(ndx));
        }
    }
    t2 = high_resolution_clock::now();
    std::cout << "   Negative values - ArrayEncode::find<Equal>(): " << duration_cast<milliseconds>(t2 - t1).count()
              << " ms" << std::endl;
    std::cout << "   Negative values - ArrayEncode::find<Equal>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    a.destroy();
    a_encoded.destroy();
}

NONCONCURRENT_TEST(Test_basic_find_NEQ_value_greater_32bit)
{
    using namespace std;
    using namespace std::chrono;
    size_t start_value = 0x0000000100000000; // 32 bit val
    size_t n_values = 1000;
    size_t n_runs = 100;
    std::cout << "   Value with bitwidth >= 32 " << std::endl;
    std::cout << "   N values = " << n_values << std::endl;
    std::cout << "   N runs = " << n_runs << std::endl;

    std::vector<int64_t> input_array;
    ArrayInteger a(Allocator::get_default());
    ArrayInteger a_encoded(Allocator::get_default());
    a.create();

    for (size_t i = 0; i < n_values; i++)
        input_array.push_back(start_value + i);
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(input_array.begin(), input_array.end(), g);
    for (const auto& v : input_array)
        a.add(v);

    QueryStateFindFirst state1;
    QueryStateFindFirst state2;
    auto t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            a.find<NotEqual>(start_value + i, 0, a.size(), &state1);
            REALM_ASSERT(state1.m_state != realm::not_found);
            REALM_ASSERT(a.get(state1.m_state) == input_array[state1.m_state]);
        }
    }
    auto t2 = high_resolution_clock::now();

    std::cout << "   Positive values - Array::find<NotEqual>(): " << duration_cast<milliseconds>(t2 - t1).count()
              << " ms" << std::endl;
    std::cout << "   Positive values - Array::find<NotEqual>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    a.try_encode(a_encoded);
    CHECK(a_encoded.is_encoded());
    CHECK(a_encoded.size() == a.size());

    // verify that both find the same thing
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            a.find<NotEqual>(start_value + i, 0, a.size(), &state1);
            a_encoded.find<NotEqual>(start_value + i, 0, a_encoded.size(), &state2);
            REALM_ASSERT(state1.m_state == state2.m_state);
        }
    }

    t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            a_encoded.find<NotEqual>(start_value + i, 0, a_encoded.size(), &state2);
            REALM_ASSERT(state2.m_state != realm::not_found);
            REALM_ASSERT(a_encoded.get(state2.m_state) == a.get(state2.m_state));
        }
    }
    t2 = high_resolution_clock::now();
    std::cout << "   Positive values - ArrayEncode::find<NotEqual>(): "
              << duration_cast<milliseconds>(t2 - t1).count() << " ms" << std::endl;
    std::cout << "   Positive values - ArrayEncode::find<NotEqual>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    std::cout << std::endl;

    a.destroy();
    a_encoded.destroy();
    a.create();
    input_array.clear();
    for (size_t i = 0; i < n_values; i++)
        input_array.push_back(-(start_value + i));
    std::random_device rd1;
    std::mt19937 g1(rd1());
    std::shuffle(input_array.begin(), input_array.end(), g1);
    for (const auto& v : input_array)
        a.add(v);

    a.try_encode(a_encoded);
    CHECK(a_encoded.is_encoded());
    CHECK(a_encoded.size() == a.size());

    // verify that both find the same thing
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            a.find<NotEqual>(-(start_value + i), 0, a.size(), &state1);
            a_encoded.find<NotEqual>(-(start_value + i), 0, a_encoded.size(), &state2);
            REALM_ASSERT(state1.m_state == state2.m_state);
        }
    }

    t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            a.find<NotEqual>(-(start_value + i), 0, a.size(), &state1);
            REALM_ASSERT(state1.m_state != realm::not_found);
            REALM_ASSERT(a.get(state1.m_state) == input_array[state1.m_state]);
        }
    }
    t2 = high_resolution_clock::now();

    std::cout << "   Negative values - Array::find<NotEqual>(): " << duration_cast<milliseconds>(t2 - t1).count()
              << " ms" << std::endl;
    std::cout << "   Negative values - Array::find<NotEqual>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            a_encoded.find<NotEqual>(-(start_value + i), 0, a_encoded.size(), &state2);
            REALM_ASSERT(state2.m_state != realm::not_found);
            REALM_ASSERT(a_encoded.get(state2.m_state) == a.get(state2.m_state));
        }
    }
    t2 = high_resolution_clock::now();
    std::cout << "   Negative values - ArrayEncode::find<NotEqual>(): "
              << duration_cast<milliseconds>(t2 - t1).count() << " ms" << std::endl;
    std::cout << "   Negative values - ArrayEncode::find<NotEqual>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    a.destroy();
    a_encoded.destroy();
}

NONCONCURRENT_TEST(Test_basic_find_LT_value_greater_32bit)
{
    using namespace std;
    using namespace std::chrono;
    size_t start_value = 0x0000000100000000; // 32 bit val
    size_t n_values = 1000;
    size_t n_runs = 100;
    std::cout << "   Value with bitwidth >= 32 " << std::endl;
    std::cout << "   N values = " << n_values << std::endl;
    std::cout << "   N runs = " << n_runs << std::endl;

    std::vector<int64_t> input_array;
    ArrayInteger a(Allocator::get_default());
    ArrayInteger a_encoded(Allocator::get_default());
    a.create();

    for (size_t i = 0; i < n_values; i++)
        input_array.push_back(start_value + i);
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(input_array.begin(), input_array.end(), g);
    for (const auto& v : input_array)
        a.add(v);

    QueryStateFindFirst state1;
    QueryStateFindFirst state2;
    auto t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 1; i < n_values; ++i) {
            a.find<Less>(start_value + i, 0, a.size(), &state1);
            REALM_ASSERT(state1.m_state != realm::not_found);
            REALM_ASSERT(a.get(state1.m_state) == input_array[state1.m_state]);
        }
    }
    auto t2 = high_resolution_clock::now();

    std::cout << "   Positive values - Array::find<Less>(): " << duration_cast<milliseconds>(t2 - t1).count() << " ms"
              << std::endl;
    std::cout << "   Positive values - Array::find<Less>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    a.try_encode(a_encoded);
    CHECK(a_encoded.is_encoded());
    CHECK(a_encoded.size() == a.size());

    // verify that both find the same thing
    state1 = {};
    state2 = {};
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            a.find<Less>(start_value + i, 0, a.size(), &state1);
            a_encoded.find<Less>(start_value + i, 0, a_encoded.size(), &state2);
            REALM_ASSERT(state1.m_state == state2.m_state);
        }
    }

    t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 1; i < n_values; ++i) {
            a_encoded.find<Less>(start_value + i, 0, a_encoded.size(), &state2);
            REALM_ASSERT(state2.m_state != realm::not_found);
            REALM_ASSERT(a_encoded.get(state2.m_state) == a.get(state2.m_state));
        }
    }
    t2 = high_resolution_clock::now();
    std::cout << "   Positive values - ArrayEncode::find<Less>(): " << duration_cast<milliseconds>(t2 - t1).count()
              << " ms" << std::endl;
    std::cout << "   Positive values - ArrayEncode::find<Less>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    std::cout << std::endl;

    a.destroy();
    a_encoded.destroy();
    a.create();
    input_array.clear();
    for (size_t i = 0; i < n_values; i++)
        input_array.push_back(-(start_value + i));
    std::random_device rd1;
    std::mt19937 g1(rd1());
    std::shuffle(input_array.begin(), input_array.end(), g1);
    for (const auto& v : input_array)
        a.add(v);

    a.try_encode(a_encoded);
    CHECK(a_encoded.is_encoded());
    CHECK(a_encoded.size() == a.size());

    // verify that both find the same thing
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            a.find<Less>(-(start_value + i), 0, a.size(), &state1);
            a_encoded.find<Less>(-(start_value + i), 0, a_encoded.size(), &state2);
            REALM_ASSERT(state1.m_state == state2.m_state);
        }
    }

    t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            a.find<Less>(-(start_value + i), 0, a.size(), &state1);
            REALM_ASSERT(state1.m_state != realm::not_found);
            REALM_ASSERT(a.get(state1.m_state) == input_array[state1.m_state]);
        }
    }
    t2 = high_resolution_clock::now();

    std::cout << "   Negative values - Array::find<Less>(): " << duration_cast<milliseconds>(t2 - t1).count() << " ms"
              << std::endl;
    std::cout << "   Negative values - Array::find<Less>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            a_encoded.find<Less>(-(start_value + i), 0, a_encoded.size(), &state2);
            REALM_ASSERT(state2.m_state != realm::not_found);
            REALM_ASSERT(a_encoded.get(state2.m_state) == a.get(state2.m_state));
        }
    }
    t2 = high_resolution_clock::now();
    std::cout << "   Negative values - ArrayEncode::find<Less>(): " << duration_cast<milliseconds>(t2 - t1).count()
              << " ms" << std::endl;
    std::cout << "   Negative values - ArrayEncode::find<Less>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    a.destroy();
    a_encoded.destroy();
}

NONCONCURRENT_TEST(Test_basic_find_GT_value_greater_32bit)
{
    using namespace std;
    using namespace std::chrono;
    size_t start_value = 0x0000100000000; // 32 bit val
    size_t n_values = 1000;
    size_t n_runs = 100;
    std::cout << "   Value with bitwidth >= 32 " << std::endl;
    std::cout << "   N values = " << n_values << std::endl;
    std::cout << "   N runs = " << n_runs << std::endl;

    std::vector<int64_t> input_array;
    ArrayInteger a(Allocator::get_default());
    ArrayInteger a_encoded(Allocator::get_default());
    a.create();

    for (size_t i = 0; i < n_values; i++)
        input_array.push_back(start_value + i);
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(input_array.begin(), input_array.end(), g);
    for (const auto& v : input_array)
        a.add(v);

    QueryStateFindFirst state1;
    QueryStateFindFirst state2;
    auto t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values - 1; ++i) {
            a.find<Greater>(start_value + i, 0, a.size(), &state1);
            REALM_ASSERT(state1.m_state != realm::not_found);
            REALM_ASSERT(a.get(state1.m_state) == input_array[state1.m_state]);
        }
    }
    auto t2 = high_resolution_clock::now();

    std::cout << "   Positive values - Array::find<Greater>(): " << duration_cast<milliseconds>(t2 - t1).count()
              << " ms" << std::endl;
    std::cout << "   Positive values - Array::find<Greater>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    a.try_encode(a_encoded);
    CHECK(a_encoded.is_encoded());
    CHECK(a_encoded.size() == a.size());

    // verify that both find the same thing
    state1 = {};
    state2 = {};
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            const auto k = start_value + i;
            a.find<Greater>(k, 0, a.size(), &state1);
            a_encoded.find<Greater>(k, 0, a_encoded.size(), &state2);
            REALM_ASSERT(state1.m_state == state2.m_state);
        }
    }

    t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values - 1; ++i) {
            a_encoded.find<Greater>(start_value + i, 0, a_encoded.size(), &state2);
            REALM_ASSERT(state2.m_state != realm::not_found);
            REALM_ASSERT(a_encoded.get(state2.m_state) == a.get(state2.m_state));
        }
    }
    t2 = high_resolution_clock::now();
    std::cout << "   Positive values - ArrayEncode::find<Greater>(): " << duration_cast<milliseconds>(t2 - t1).count()
              << " ms" << std::endl;
    std::cout << "   Positive values - ArrayEncode::find<Greater>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    std::cout << std::endl;

    a.destroy();
    a_encoded.destroy();
    a.create();
    input_array.clear();
    for (size_t i = 0; i < n_values; i++)
        input_array.push_back(-(start_value + i));
    std::random_device rd1;
    std::mt19937 g1(rd1());
    std::shuffle(input_array.begin(), input_array.end(), g1);
    for (const auto& v : input_array)
        a.add(v);

    a.try_encode(a_encoded);
    CHECK(a_encoded.is_encoded());
    CHECK(a_encoded.size() == a.size());

    // verify that both find the same thing
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 1; i < n_values; ++i) {
            a.find<Greater>(-(start_value + i), 0, a.size(), &state1);
            a_encoded.find<Greater>(-(start_value + i), 0, a_encoded.size(), &state2);
            REALM_ASSERT(state1.m_state == state2.m_state);
        }
    }

    t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 0; i < n_values; ++i) {
            a.find<Greater>(-(start_value + i), 0, a.size(), &state1);
            REALM_ASSERT(state1.m_state != realm::not_found);
            REALM_ASSERT(a.get(state1.m_state) == input_array[state1.m_state]);
        }
    }
    t2 = high_resolution_clock::now();

    std::cout << "   Negative values - Array::find<Greater>(): " << duration_cast<milliseconds>(t2 - t1).count()
              << " ms" << std::endl;
    std::cout << "   Negative values - Array::find<Greater>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    t1 = high_resolution_clock::now();
    for (size_t j = 0; j < n_runs; ++j) {
        for (size_t i = 1; i < n_values; ++i) {
            a_encoded.find<Greater>(-(start_value + i), 0, a_encoded.size(), &state2);
            REALM_ASSERT(state2.m_state != realm::not_found);
            REALM_ASSERT(a_encoded.get(state2.m_state) == a.get(state2.m_state));
        }
    }
    t2 = high_resolution_clock::now();
    std::cout << "   Negative values - ArrayEncode::find<Greater>(): " << duration_cast<milliseconds>(t2 - t1).count()
              << " ms" << std::endl;
    std::cout << "   Negative values - ArrayEncode::find<Greater>(): "
              << (double)duration_cast<nanoseconds>(t2 - t1).count() / n_values / n_runs << " ns/value" << std::endl;

    a.destroy();
    a_encoded.destroy();
}

#endif

// disable this test if forcing compression to Packed.
#if !REALM_COMPRESS
TEST(Test_ArrayInt_no_encode)
{
    ArrayInteger a(Allocator::get_default());
    ArrayInteger a1(Allocator::get_default());
    a.create();
    a.add(10);
    a.add(11);
    a.add(12);
    // the original array is never encoded. a1 is the array to write down to disk
    // in this case compression is not needed
    CHECK_NOT(a.try_compress(a1));
    CHECK_NOT(a.is_compressed());
    CHECK(a.get(0) == 10);
    CHECK(a.get(1) == 11);
    CHECK(a.get(2) == 12);
    a.destroy();
    a1.destroy();
}

TEST(Test_ArrayInt_encode_decode_needed)
{
    ArrayInteger a(Allocator::get_default());
    ArrayInteger a1(Allocator::get_default());
    a.create();
    a.add(10);
    a.add(5);
    a.add(5);
    // uncompressed requires 3 x 4 bits, compressed takes 2 x 5 bits + 3 x 2 bits
    // with 8 byte alignment this is both 16 bytes.
    CHECK_NOT(a.try_compress(a1));
    CHECK_NOT(a.is_compressed());
    a.add(10);
    a.add(15);
    // uncompressed is 5x4 bits, compressed is 3x5 bits + 5x2 bits
    // with 8 byte alignment this is both 16 bytes
    CHECK_NOT(a.try_compress(a1));
    CHECK_NOT(a.is_compressed());
    a.add(10);
    a.add(15);
    a.add(10);
    a.add(15);
    // uncompressed is 9x4 bits, compressed is 3x5 bits + 9x2 bits
    // with 8 byte alignment this is both 16 bytes
    CHECK_NOT(a.try_compress(a1));
    CHECK_NOT(a.is_compressed());
    a.add(-1);
    // the addition of -1 forces the array from unsigned to signed form
    // changing from 4 bits per element to 8 bits.
    // (1,2,4 bit elements are unsigned, larger elements are signed)
    // uncompressed is 10x8 bits, compressed is 3x5 bits + 10x2 bits
    // with alignment, this is 24 bytes uncompressed and 16 bytes compressed
    CHECK(a.try_compress(a1));
    CHECK_NOT(a.is_compressed());
    CHECK(a.get(0) == 10);
    CHECK(a.get(1) == 5);
    CHECK(a.get(2) == 5);
    CHECK(a.get(3) == 10);
    CHECK(a.get(4) == 15);
    CHECK(a1.is_compressed());
    auto v = a1.get(0);
    CHECK(v == a.get(0));
    CHECK(a1.get(1) == a.get(1));
    CHECK(a1.get(2) == a.get(2));
    CHECK(a1.get(3) == a.get(3));
    CHECK(a1.get(4) == a.get(4));
    a.destroy();
    a1.destroy();
}
#endif

TEST(Test_array_same_size_less_bits)
{
    ArrayInteger a(Allocator::get_default());
    ArrayInteger a1(Allocator::get_default());
    a.create();
    a.add(1000000);
    a.add(1000000);
    a.add(1000000);
    CHECK(a.try_compress(a1));
    CHECK_NOT(a.is_compressed());
    CHECK(a.get_any(0) == 1000000);
    CHECK(a.get_any(1) == 1000000);
    CHECK(a.get_any(2) == 1000000);
    CHECK(a1.is_compressed());
    CHECK(a1.get_any(0) == 1000000);
    CHECK(a1.get_any(1) == 1000000);
    CHECK(a1.get_any(2) == 1000000);
    a.destroy();
    a1.destroy();
}

TEST(Test_ArrayInt_negative_nums)
{
    ArrayInteger a(Allocator::get_default());
    ArrayInteger a1(Allocator::get_default());
    a.create();
    a.add(-1000000);
    a.add(0);
    a.add(1000000);
    CHECK_NOT(a.is_compressed());
    CHECK(a.try_compress(a1));
    a1.destroy();
    CHECK(a.get(0) == -1000000);
    CHECK(a.get(1) == 0);
    CHECK(a.get(2) == 1000000);
    a.add(-1000000);
    a.add(-1000000);
    CHECK(a.try_compress(a1));
    CHECK_NOT(a.is_compressed());
    CHECK(a.get(0) == -1000000);
    CHECK(a.get(1) == 0);
    CHECK(a.get(2) == 1000000);
    CHECK(a.get(3) == -1000000);
    CHECK(a.get(4) == -1000000);
    a.add(0);
    a1.destroy();
    CHECK(a.try_compress(a1));
    CHECK_NOT(a.is_compressed());
    CHECK(a1.is_compressed());

    CHECK(a1.get(0) == a.get(0));
    CHECK(a1.get(1) == a.get(1));
    CHECK(a1.get(2) == a.get(2));
    CHECK(a1.get(3) == a.get(3));
    CHECK(a1.get(4) == a.get(4));
    CHECK(a1.get(5) == a.get(5));

    a.add(1000000);
    a1.destroy(); // this decodes the array
    CHECK(a.try_compress(a1));
    CHECK_NOT(a.is_compressed());
    CHECK(a1.is_compressed());
    CHECK(a1.get(0) == a.get(0));
    CHECK(a1.get(1) == a.get(1));
    CHECK(a1.get(2) == a.get(2));
    CHECK(a1.try_decompress());
    a.add(-1000000);
    a1.destroy();
    CHECK(a.try_compress(a1));
    CHECK_NOT(a.is_compressed());
    CHECK(a1.is_compressed());
    CHECK(a1.get(0) == a.get(0));
    CHECK(a1.get(1) == a.get(1));
    CHECK(a1.get(2) == a.get(2));
    a.add(0);
    a1.destroy();
    CHECK(a.try_compress(a1));
    CHECK_NOT(a.is_compressed());
    CHECK(a1.is_compressed());
    CHECK(a1.get(0) == a.get(0));
    CHECK(a1.get(1) == a.get(1));
    CHECK(a1.get(2) == a.get(2));
    a.add(1000000);
    a1.destroy();
    CHECK(a.try_compress(a1));
    CHECK_NOT(a.is_compressed());
    CHECK(a1.is_compressed());
    CHECK(a.size() == 10);
    CHECK(a.size() == a1.size());
    CHECK(a1.is_compressed());
    CHECK(a1.get(0) == a.get(0));
    CHECK(a1.get(1) == a.get(1));
    CHECK(a1.get(2) == a.get(2));
    CHECK(a1.get(3) == a.get(3));
    CHECK(a1.get(4) == a.get(4));
    CHECK(a1.get(5) == a.get(5));
    CHECK(a1.get(6) == a.get(6));
    CHECK(a1.get(7) == a.get(7));
    CHECK(a1.get(8) == a.get(8));
    a.destroy();
    a1.destroy();
}

TEST(Test_ArrayInt_compress_data)
{
    ArrayInteger a(Allocator::get_default());
    ArrayInteger a1(Allocator::get_default());

    a.create();
    a.add(-4427957085475570907);
    a.add(-4427957085475570907);
    a.add(-4427957085475570907);
    a.add(-4427957085475570907);
    a.add(4);
    a.add(5);
    a.add(6);
    a.add(7);
    a.add(8);
    a.add(4);
    a.try_compress(a1);
    bool ok = a1.is_compressed();
    CHECK(ok);
    CHECK(a1.is_compressed());
    CHECK(a1.is_attached());
    CHECK(a.is_attached());
    for (size_t i = 0; i < a.size(); ++i) {
        auto v0 = a1.get(i);
        auto v1 = a.get(i);
        CHECK(v0 == v1);
    }
    a.destroy();
    a1.destroy();

    a.create();
    a.add(-4427957085475570907);
    a.add(-4427957085475570907);
    a.add(-4427957085475570907);
    a.add(-4427957085475570907);
    a.try_compress(a1);
    for (size_t i = 0; i < a.size(); ++i)
        CHECK(a1.get(i) == a.get(i));
    a.destroy();
    a1.destroy();

    a.create();

    a.add(16388);
    a.add(409);
    a.add(16388);
    a.add(16388);
    a.add(409);
    a.add(16388);
    CHECK(a.size() == 6);
    // Current: [16388:16, 409:16, 16388:16, 16388:16, 409:16, 16388:16], space needed: 6*16 bits = 96 bits +
    // header
    // compress the array is a good option.
    CHECK(a.try_compress(a1));
    CHECK(a1.is_compressed());
    // Compressed: [409:16, 16388:16][1:1,0:1,1:1,1:1,0:1,1:1], space needed: 2*16 bits + 6 * 1 bit = 38 bits +
    // header
    CHECK(a1.size() == a.size());
    CHECK(a1.get(0) == a.get(0));
    CHECK(a1.get(1) == a.get(1));
    CHECK(a1.get(2) == a.get(2));
    CHECK(a1.get(3) == a.get(3));
    CHECK(a1.get(4) == a.get(4));
    CHECK(a1.get(5) == a.get(5));
    // decompress
    CHECK(a1.try_decompress());
    a.add(20);
    // compress again, it should be a viable option
    a1.destroy();
    CHECK(a.try_compress(a1));
    CHECK(a1.is_compressed());
    CHECK(a1.size() == 7);
    CHECK(a1.get(0) == a.get(0));
    CHECK(a1.get(1) == a.get(1));
    CHECK(a1.get(2) == a.get(2));
    CHECK(a1.get(3) == a.get(3));
    CHECK(a1.get(4) == a.get(4));
    CHECK(a1.get(5) == a.get(5));
    CHECK(a1.get(6) == a.get(6));
    CHECK(a1.try_decompress());
    CHECK_NOT(a1.is_compressed());
    CHECK(a1.get(0) == a.get(0));
    CHECK(a1.get(1) == a.get(1));
    CHECK(a1.get(2) == a.get(2));
    CHECK(a1.get(3) == a.get(3));
    CHECK(a1.get(4) == a.get(4));
    CHECK(a1.get(5) == a.get(5));
    CHECK(a1.get(6) == a.get(6));
    a.destroy();
    a1.destroy();
}

TEST(Test_ArrayInt_compress_data_init_from_mem)
{
    ArrayInteger a(Allocator::get_default());
    ArrayInteger a1(Allocator::get_default());
    a.create();
    a.add(16388);
    a.add(409);
    a.add(16388);
    a.add(16388);
    a.add(409);
    a.add(16388);
    const auto sz = a.size();
    CHECK(sz == 6);
    // Current: [16388:16, 409:16, 16388:16, 16388:16, 409:16, 16388:16],
    // space needed: 6*16 bits = 96 bits + header
    // compress the array is a good option (it should already be compressed).
    CHECK(a.try_compress(a1));
    CHECK(a1.is_compressed());
    //  Array should be in compressed form now
    auto mem = a1.get_mem();
    ArrayInteger a2(Allocator::get_default());
    a2.init_from_mem(mem); // initialise a1 with a
    // check a2
    CHECK(a2.is_compressed());
    const auto sz2 = a2.size();
    CHECK(sz2 == 6);
    CHECK(a2.get(0) == 16388);
    CHECK(a2.get(1) == 409);
    CHECK(a2.get(2) == 16388);
    CHECK(a2.get(3) == 16388);
    CHECK(a2.get(4) == 409);
    CHECK(a2.get(5) == 16388);
    // decompress a2 and compresses again
    CHECK(a2.is_compressed());
    CHECK(a2.try_decompress());
    CHECK_NOT(a2.is_compressed());
    a2.add(20);
    CHECK(a2.try_compress(a1));
    CHECK(a1.is_compressed());
    CHECK(a1.size() == 7);
    CHECK(a1.get(0) == 16388);
    CHECK(a1.get(1) == 409);
    CHECK(a1.get(2) == 16388);
    CHECK(a1.get(3) == 16388);
    CHECK(a1.get(4) == 409);
    CHECK(a1.get(5) == 16388);
    CHECK(a1.get(6) == 20);
    CHECK(a1.try_decompress());
    a.destroy();
    a1.destroy();
    a2.destroy();
    CHECK_NOT(a.is_attached());
    CHECK_NOT(a1.is_attached());
    CHECK_NOT(a2.is_attached());
}

TEST(ArrayIntNull_SetNull)
{
    ArrayIntNull a(Allocator::get_default());
    a.create();

    a.add(0);
    CHECK(!a.is_null(0));
    a.set_null(0);
    CHECK(a.is_null(0));

    a.add(128);
    CHECK(a.is_null(0));

    a.add(120000);
    CHECK(a.is_null(0));

    a.destroy();
}

TEST(ArrayIntNull_SetIntegerToPreviousNullValueChoosesNewNull)
{
    ArrayIntNull a(Allocator::get_default());
    a.create();

    a.add(126);
    // NULL value should be 127
    a.add(0);
    a.set_null(1);
    a.set(0, 127);
    // array should be upgraded now
    CHECK(a.is_null(1));

    a.add(1000000000000LL); // upgrade to 64-bit, null should now be a "random" value
    CHECK(a.is_null(1));
    int64_t old_null = a.null_value();
    a.add(old_null);
    CHECK(a.is_null(1));
    CHECK_NOT_EQUAL(a.null_value(), old_null);

    a.destroy();
}

TEST(ArrayIntNull_Boundaries)
{
    ArrayIntNull a(Allocator::get_default());
    a.create();
    a.add(0);
    a.set_null(0);
    a.add(0);
    CHECK(a.is_null(0));
    CHECK(!a.is_null(1));
    CHECK_EQUAL(a.get_width(), 1); // not sure if this should stay. Makes assumtions about implementation details.


    // consider turning this into a array + loop
    a.add(0);
    CHECK_EQUAL(0, a.back());
    CHECK(a.is_null(0));

    a.add(1);
    CHECK_EQUAL(1, a.back());
    CHECK(a.is_null(0));

    a.add(3);
    CHECK_EQUAL(3, a.back());
    CHECK(a.is_null(0));

    a.add(15);
    CHECK_EQUAL(15, a.back());
    CHECK(a.is_null(0));


    a.add(std::numeric_limits<int8_t>::max());
    CHECK_EQUAL(std::numeric_limits<int8_t>::max(), a.back());
    CHECK(a.is_null(0));

    a.add(std::numeric_limits<int8_t>::min());
    CHECK_EQUAL(std::numeric_limits<int8_t>::min(), a.back());
    CHECK(a.is_null(0));

    a.add(std::numeric_limits<uint8_t>::max());
    CHECK_EQUAL(std::numeric_limits<uint8_t>::max(), a.back());
    CHECK(a.is_null(0));


    a.add(std::numeric_limits<int16_t>::max());
    CHECK_EQUAL(std::numeric_limits<int16_t>::max(), a.back());
    CHECK(a.is_null(0));
    a.add(std::numeric_limits<int16_t>::min());
    CHECK_EQUAL(std::numeric_limits<int16_t>::min(), a.back());
    CHECK(a.is_null(0));
    a.add(std::numeric_limits<uint16_t>::max());
    CHECK_EQUAL(std::numeric_limits<uint16_t>::max(), a.back());
    CHECK(a.is_null(0));


    a.add(std::numeric_limits<int32_t>::max());
    CHECK_EQUAL(std::numeric_limits<int32_t>::max(), a.back());
    CHECK(a.is_null(0));
    a.add(std::numeric_limits<int32_t>::min());
    CHECK_EQUAL(std::numeric_limits<int32_t>::min(), a.back());
    CHECK(a.is_null(0));
    a.add(std::numeric_limits<uint32_t>::max());
    CHECK_EQUAL(std::numeric_limits<uint32_t>::max(), a.back());
    CHECK(a.is_null(0));


    a.add(std::numeric_limits<int_fast64_t>::max());
    CHECK_EQUAL(std::numeric_limits<int_fast64_t>::max(), a.back());
    CHECK(a.is_null(0));
    a.add(std::numeric_limits<int_fast64_t>::min());
    CHECK_EQUAL(std::numeric_limits<int_fast64_t>::min(), a.back());
    CHECK(a.is_null(0));

    a.destroy();
}

// Test if allocator relocation preserves null and non-null
TEST(ArrayIntNull_Relocate)
{
    ArrayIntNull a(Allocator::get_default());
    a.create();

    // Enforce 64 bits and hence use magic value
    a.add(0x1000000000000000LL);
    a.add(0);
    a.set_null(1);

    // Add values until relocation has happend multiple times (80 kilobyte payload in total)
    for (size_t t = 0; t < 10000; t++)
        a.add(0);

    CHECK(!a.is_null(0));
    CHECK(a.is_null(1));
    a.destroy();
}

TEST(ArrayIntNull_Find)
{
    ArrayIntNull a(Allocator::get_default());
    a.create();

    a.clear();
    for (size_t i = 0; i < 100; ++i) {
        a.add(0x33);
    }
    a.add(0x100);
    a.set(50, 0x44);
    a.set_null(51);
    a.set(60, 0x44);

    size_t t = a.find_first<NotEqual>(0x33);
    CHECK_EQUAL(50, t);

    t = a.find_first<NotEqual>(0x33, 0, 50);
    CHECK_EQUAL(not_found, t);

    t = a.find_first<NotEqual>(null());
    CHECK_EQUAL(0, t);

    t = a.find_first<NotEqual>(null(), 51);
    CHECK_EQUAL(52, t);

    size_t t2 = a.find_first(0x44);
    CHECK_EQUAL(50, t2);

    t2 = a.find_first(null());
    CHECK_EQUAL(51, t2);

    size_t t3 = a.find_first(0);
    CHECK_EQUAL(not_found, t3);

    size_t t22 = a.find_first<Greater>(0x100);
    CHECK_EQUAL(t22, not_found);

    {
        IntegerColumn col(Allocator::get_default());
        col.create();

        a.find_all(&col, 0x44);

        CHECK_EQUAL(2, col.size());
        CHECK_EQUAL(a.get(static_cast<size_t>(col.get(0))), 0x44);
        CHECK_EQUAL(a.get(static_cast<size_t>(col.get(1))), 0x44);

        col.destroy();
    }
    a.destroy();
}

TEST(ArrayRef_Basic)
{
    ArrayRef a(Allocator::get_default());
    a.create();
    CHECK(a.has_refs());

    ref_type ref = 8;
    a.insert(0, ref);
    CHECK_EQUAL(a.get(0), ref);
    a.insert(0, 16);
    CHECK_EQUAL(a.get(0), 16);
    CHECK_EQUAL(a.get(1), ref);
    a.set(0, 32);
    CHECK_EQUAL(a.get(0), 32);
    CHECK_EQUAL(a.get(1), ref);
    a.add(16);
    CHECK_EQUAL(a.get(0), 32);
    CHECK_EQUAL(a.get(1), ref);
    CHECK_EQUAL(a.get(2), 16);

    a.destroy();
}
