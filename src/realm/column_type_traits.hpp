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
#include <realm/keys.hpp>

namespace realm {

struct ObjKey;
class Decimal128;
class ObjectId;
class Mixed;
class Timestamp;
class ArraySmallBlobs;
class ArrayString;
class ArrayStringShort;
class ArrayBinary;
class ArrayMixed;
class ArrayTimestamp;
class ArrayInteger;
class ArrayRef;
class ArrayIntNull;
class ArrayBool;
class ArrayBoolNull;
class ArrayKey;
class ArrayKeyNonNullable;
class ArrayDecimal128;
class ArrayObjectId;
class ArrayObjectIdNull;
template <class>
class BasicArray;
template <class>
class BasicArrayNull;
struct Link;
template <class>
class Lst;
struct SizeOfList;

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
    using average_type = double;
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
    using average_type = double;
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
struct ColumnTypeTraits<Mixed> {
    using cluster_leaf_type = ArrayMixed;
    static const DataType id = type_OldMixed;
    static const ColumnType column_id = col_type_OldMixed;
};

template <>
struct ColumnTypeTraits<Link> {
    static const ColumnType column_id = col_type_Link;
};

template <>
struct ColumnTypeTraits<float> {
    using cluster_leaf_type = BasicArray<float>;
    using sum_type = double;
    using minmax_type = float;
    using average_type = double;
    static const DataType id = type_Float;
    static const ColumnType column_id = col_type_Float;
    static const ColumnType real_column_type = col_type_Float;
};

template <>
struct ColumnTypeTraits<util::Optional<float>> {
    using cluster_leaf_type = BasicArrayNull<float>;
    using sum_type = double;
    using minmax_type = float;
    using average_type = double;
    static const DataType id = type_Float;
    static const ColumnType column_id = col_type_Float;
    static const ColumnType real_column_type = col_type_Float;
};

template <>
struct ColumnTypeTraits<double> {
    using cluster_leaf_type = BasicArray<double>;
    using sum_type = double;
    using minmax_type = double;
    using average_type = double;
    static const DataType id = type_Double;
    static const ColumnType column_id = col_type_Double;
    static const ColumnType real_column_type = col_type_Double;
};

template <>
struct ColumnTypeTraits<util::Optional<double>> {
    using cluster_leaf_type = BasicArrayNull<double>;
    using sum_type = double;
    using minmax_type = double;
    using average_type = double;
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
struct ColumnTypeTraits<ObjectId> {
    using cluster_leaf_type = ArrayObjectId;
    static const DataType id = type_ObjectId;
    static const ColumnType column_id = col_type_ObjectId;
};

template <>
struct ColumnTypeTraits<util::Optional<ObjectId>> {
    using cluster_leaf_type = ArrayObjectIdNull;
    static const DataType id = type_ObjectId;
    static const ColumnType column_id = col_type_ObjectId;
};

template <>
struct ColumnTypeTraits<StringData> {
    using cluster_leaf_type = ArrayString;
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

template <>
struct ColumnTypeTraits<Decimal128> {
    using cluster_leaf_type = ArrayDecimal128;
    using sum_type = Decimal128;
    using minmax_type = Decimal128;
    using average_type = Decimal128;
    static const DataType id = type_Decimal;
    static const ColumnType column_id = col_type_Decimal;
};

template <>
struct ColumnTypeTraits<SizeOfList> {
    static const DataType id = type_Int;
};

template <>
struct ColumnTypeTraits<int> {
    static const DataType id = type_Int;
};

template <>
struct ColumnTypeTraits<null> {
    static const DataType id = DataType(-1);
};

template <typename T>
struct ObjectTypeTraits {
    constexpr static bool self_contained_null =
        realm::is_any_v<T, StringData, BinaryData, Decimal128, Timestamp, Mixed>;
};

template <typename T>
using ColumnClusterLeafType = typename ColumnTypeTraits<T>::cluster_leaf_type;
template <typename T>
using ColumnSumType = typename ColumnTypeTraits<T>::sum_type;
template <typename T>
using ColumnMinMaxType = typename ColumnTypeTraits<T>::minmax_type;
template <typename T>
using ColumnAverageType = typename ColumnTypeTraits<T>::average_type;

template <class T>
struct ColumnTypeTraits<Lst<T>> {
    static const ColumnType column_id = ColumnTypeTraits<T>::column_id;
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
template <bool N>
struct GetLeafType<type_Decimal, N> {
    // FIXME: Null definition
    using type = ArrayDecimal128;
};

template <class T>
inline bool value_is_null(const T& val)
{
    return val.is_null();
}
template <class T>
inline bool value_is_null(const util::Optional<T>& val)
{
    return !val;
}
inline bool value_is_null(const int64_t&)
{
    return false;
}
inline bool value_is_null(const bool&)
{
    return false;
}
inline bool value_is_null(const ObjectId&)
{
    return false;
}
inline bool value_is_null(const float& val)
{
    return null::is_null_float(val);
}
inline bool value_is_null(const double& val)
{
    return null::is_null_float(val);
}
inline bool value_is_null(const ObjKey& val)
{
    return !val;
}

} // namespace realm

#endif // REALM_COLUMN_TYPE_TRAITS_HPP
