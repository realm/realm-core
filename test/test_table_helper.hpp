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

#ifndef TEST_TEST_TABLE_HELPER_HPP_
#define TEST_TEST_TABLE_HELPER_HPP_

#include <realm/table.hpp>

enum Days { Mon, Tue, Wed, Thu, Fri, Sat, Sun };

class TestTable : public realm::Table {
public:
    using Table::Table;
    template <class T>
    void set(size_t column_ndx, size_t row_ndx, T value, bool is_default = false);
};

template <>
inline void TestTable::set(size_t column_ndx, size_t row_ndx, int value, bool is_default)
{
    set_int(column_ndx, row_ndx, value, is_default);
}

template <>
inline void TestTable::set(size_t column_ndx, size_t row_ndx, unsigned value, bool is_default)
{
    set_int(column_ndx, row_ndx, value, is_default);
}

template <>
inline void TestTable::set(size_t column_ndx, size_t row_ndx, bool value, bool is_default)
{
    set_bool(column_ndx, row_ndx, value, is_default);
}

template <>
inline void TestTable::set(size_t column_ndx, size_t row_ndx, uint64_t value, bool is_default)
{
    set_int(column_ndx, row_ndx, value, is_default);
}

template <>
inline void TestTable::set(size_t column_ndx, size_t row_ndx, int64_t value, bool is_default)
{
    set_int(column_ndx, row_ndx, value, is_default);
}

template <>
inline void TestTable::set(size_t column_ndx, size_t row_ndx, double value, bool is_default)
{
    set_double(column_ndx, row_ndx, value, is_default);
}

template <>
inline void TestTable::set(size_t column_ndx, size_t row_ndx, float value, bool is_default)
{
    set_float(column_ndx, row_ndx, value, is_default);
}

template <>
inline void TestTable::set(size_t column_ndx, size_t row_ndx, const char* value, bool is_default)
{
    set_string(column_ndx, row_ndx, realm::StringData(value), is_default);
}

template <>
inline void TestTable::set(size_t column_ndx, size_t row_ndx, realm::BinaryData value, bool is_default)
{
    set_binary(column_ndx, row_ndx, value, is_default);
}

template <>
inline void TestTable::set(size_t column_ndx, size_t row_ndx, realm::OldDateTime value, bool is_default)
{
    set_olddatetime(column_ndx, row_ndx, value, is_default);
}

template <>
inline void TestTable::set(size_t column_ndx, size_t row_ndx, realm::Timestamp value, bool is_default)
{
    set_timestamp(column_ndx, row_ndx, value, is_default);
}

template <>
inline void TestTable::set(size_t column_ndx, size_t row_ndx, Days value, bool is_default)
{
    set_int(column_ndx, row_ndx, value, is_default);
}

template <>
inline void TestTable::set(size_t, size_t, decltype(nullptr), bool)
{
}

template <>
inline void TestTable::set(size_t column_ndx, size_t row_ndx, realm::Mixed value, bool is_default)
{
    set_mixed(column_ndx, row_ndx, value, is_default);
}

/*****************************************************************************/

template <class Val>
void set2(TestTable& t, size_t col_ndx, size_t row_ndx, Val v)
{
    t.set(col_ndx, row_ndx, v);
}

template <class Head, class... Tail>
void set2(TestTable& t, size_t col_ndx, size_t row_ndx, Head v, Tail... tail)
{
    t.set(col_ndx, row_ndx, v);
    set2(t, col_ndx + 1, row_ndx, tail...);
}

/*****************************************************************************/

template <class Head, class... Tail>
void add(TestTable& t, Head v, Tail... tail)
{
    auto row_ndx = t.add_empty_row();
    set2(t, 0, row_ndx, v, tail...);
}

template <class Head, class... Tail>
void add(realm::TableRef r, Head v, Tail... tail)
{
    TestTable& t = *static_cast<TestTable*>(r.get());
    auto row_ndx = t.add_empty_row();
    set2(t, 0, row_ndx, v, tail...);
}

template <class Head, class... Tail>
void insert(TestTable& t, size_t row_ndx, Head v, Tail... tail)
{
    t.insert_empty_row(row_ndx);
    set2(t, 0, row_ndx, v, tail...);
}

template <class Head, class... Tail>
void insert(realm::TableRef r, size_t row_ndx, Head v, Tail... tail)
{
    TestTable& t = *static_cast<TestTable*>(r.get());
    t.insert_empty_row(row_ndx);
    set2(t, 0, row_ndx, v, tail...);
}

// 'set' is a helper method to make it easier to switch from typed tables to untyped in our unit tests.
template <class Head, class... Tail>
void set(TestTable& t, size_t row_ndx, Head v, Tail... tail)
{
    set2(t, 0, row_ndx, v, tail...);
}

template <class Head, class... Tail>
void set(realm::TableRef r, size_t row_ndx, Head v, Tail... tail)
{
    TestTable& t = *static_cast<TestTable*>(r.get());
    set2(t, 0, row_ndx, v, tail...);
}

#endif /* TEST_TEST_TABLE_HELPER_HPP_ */
