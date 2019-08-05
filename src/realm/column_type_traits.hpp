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

#ifndef REALM_COLUMN_TYPE_TRAITS_HPP
#define REALM_COLUMN_TYPE_TRAITS_HPP

#include <realm/column_fwd.hpp>
#include <realm/column_type.hpp>
#include <realm/data_type.hpp>
#include <realm/array.hpp>

namespace realm {

struct ObjKey;
class Timestamp;
class OldDateTime;
class ArraySmallBlobs;
class ArrayString;
class ArrayStringShort;
class ArrayBinary;
class ArrayTimestamp;
class ArrayInteger;
class ArrayRef;
class ArrayIntNull;
class ArrayBool;
class ArrayBoolNull;
class ArrayKey;
class ArrayKeyNonNullable;
template <class>
class BasicArray;
template <class>
class BasicArrayNull;

template <class T>
struct ColumnTypeTraits;

template <class T, Action action>
struct AggregateResultType {
    using result_type = T;
};

template <class T, Action action>
struct AggregateResultType<util::Optional<T>, action> {
    using result_type = T;
};

template <>
struct AggregateResultType<float, act_Sum> {
    using result_type = double;
};

template <>
struct ColumnTypeTraits<int64_t> {
    using leaf_type = ArrayInteger;
    using cluster_leaf_type = ArrayInteger;
    using sum_type = int64_t;
    using minmax_type = int64_t;
    static const DataType id = type_Int;
    static const ColumnType column_id = col_type_Int;
    static const ColumnType real_column_type = col_type_Int;
};

template <>
struct ColumnTypeTraits<ref_type> {
    using cluster_leaf_type = ArrayRef;
    static const DataType id = type_Int;
    static const ColumnType column_id = col_type_Int;
};

template <>
struct ColumnTypeTraits<util::Optional<int64_t>> {
    using leaf_type = ArrayIntNull;
    using cluster_leaf_type = ArrayIntNull;
    using sum_type = int64_t;
    using minmax_type = int64_t;
    static const DataType id = type_Int;
    static const ColumnType column_id = col_type_Int;
    static const ColumnType real_column_type = col_type_Int;
};

template <>
struct ColumnTypeTraits<bool> {
    using cluster_leaf_type = ArrayBool;
    static const DataType id = type_Bool;
    static const ColumnType column_id = col_type_Bool;
};

template <>
struct ColumnTypeTraits<util::Optional<bool>> {
    using cluster_leaf_type = ArrayBoolNull;
    static const DataType id = type_Bool;
    static const ColumnType column_id = col_type_Bool;
};

template <>
struct ColumnTypeTraits<ObjKey> {
    using cluster_leaf_type = ArrayKey;
    static const DataType id = type_Link;
    static const ColumnType column_id = col_type_Link;
};

template <>
struct ColumnTypeTraits<float> {
    using cluster_leaf_type = BasicArray<float>;
    using sum_type = double;
    using minmax_type = float;
    static const DataType id = type_Float;
    static const ColumnType column_id = col_type_Float;
    static const ColumnType real_column_type = col_type_Float;
};

template <>
struct ColumnTypeTraits<util::Optional<float>> {
    using cluster_leaf_type = BasicArrayNull<float>;
    using sum_type = double;
    using minmax_type = float;
    static const DataType id = type_Float;
    static const ColumnType column_id = col_type_Float;
    static const ColumnType real_column_type = col_type_Float;
};

template <>
struct ColumnTypeTraits<double> {
    using cluster_leaf_type = BasicArray<double>;
    using sum_type = double;
    using minmax_type = double;
    static const DataType id = type_Double;
    static const ColumnType column_id = col_type_Double;
    static const ColumnType real_column_type = col_type_Double;
};

template <>
struct ColumnTypeTraits<util::Optional<double>> {
    using cluster_leaf_type = BasicArrayNull<double>;
    using sum_type = double;
    using minmax_type = double;
    static const DataType id = type_Double;
    static const ColumnType column_id = col_type_Double;
    static const ColumnType real_column_type = col_type_Double;
};

template <>

struct ColumnTypeTraits<Timestamp> {
    using cluster_leaf_type = ArrayTimestamp;
    using minmax_type = Timestamp;
    static const DataType id = type_Timestamp;
    static const ColumnType column_id = col_type_Timestamp;
};

template <>
struct ColumnTypeTraits<StringData> {
    using cluster_leaf_type = ArrayString;
    using no_sum_type = double;
    static const DataType id = type_String;
    static const ColumnType column_id = col_type_String;
};

template <>
struct ColumnTypeTraits<BinaryData> {
    using leaf_type = ArraySmallBlobs;
    using cluster_leaf_type = ArrayBinary;
    static const DataType id = type_Binary;
    static const ColumnType column_id = col_type_Binary;
    static const ColumnType real_column_type = col_type_Binary;
};

template <DataType, bool Nullable>
struct GetLeafType;
template <>
struct GetLeafType<type_Int, false> {
    using type = ArrayInteger;
};
template <>
struct GetLeafType<type_Int, true> {
    using type = ArrayIntNull;
};
template <bool N>
struct GetLeafType<type_Float, N> {
    // FIXME: Null definition
    using type = BasicArray<float>;
};
template <bool N>
struct GetLeafType<type_Double, N> {
    // FIXME: Null definition
    using type = BasicArray<double>;
};
template <bool N>
struct GetLeafType<type_Timestamp, N> {
    // FIXME: Null definition
    using type = ArrayTimestamp;
};
}

#endif // REALM_COLUMN_TYPE_TRAITS_HPP
