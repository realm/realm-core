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


#include "realm/set.hpp"
#include "realm/array_mixed.hpp"
#include "realm/replication.hpp"

namespace realm {

template <class T>
Set<T>::Set(const Obj& obj, ColKey col_key)
    : Collection<T, SetBase>(obj, col_key)
{
    if (m_obj) {
        init_from_parent();
    }
}


template Set<int64_t>::Set(const Obj& obj, ColKey col_key);
template Set<StringData>::Set(const Obj& obj, ColKey col_key);
template Set<Mixed>::Set(const Obj& obj, ColKey col_key);

/***************************** Set<T>::insert_repl *****************************/

template <>
void Set<Int>::insert_repl(Replication* repl, int64_t value, size_t ndx)
{
    repl->set_insert_int(*this, value, ndx);
}

template <>
void Set<StringData>::insert_repl(Replication* repl, StringData value, size_t ndx)
{
    repl->set_insert_string(*this, value, ndx);
}

template <>
void Set<Mixed>::insert_repl(Replication* repl, Mixed value, size_t ndx)
{
    if (value.is_null()) {
        repl->set_insert_null(*this, ndx);
    }
    else {
        switch (value.get_type()) {
            case type_Int: {
                repl->set_insert_int(*this, ndx, value.get_int());
                break;
            }
            case type_Bool: {
                repl->set_insert_bool(*this, ndx, value.get_bool());
                break;
            }
            case type_String: {
                repl->set_insert_string(*this, ndx, value.get_string());
                break;
            }
            case type_Binary: {
                repl->set_insert_binary(*this, ndx, value.get_binary());
                break;
            }
            case type_Timestamp: {
                repl->set_insert_timestamp(*this, ndx, value.get_timestamp());
                break;
            }
            case type_Float: {
                repl->set_insert_float(*this, ndx, value.get_float());
                break;
            }
            case type_Double: {
                repl->set_insert_double(*this, ndx, value.get_double());
                break;
            }
            case type_Decimal: {
                repl->set_insert_decimal(*this, ndx, value.get<Decimal128>());
                break;
            }
            case type_ObjectId: {
                repl->set_insert_object_id(*this, ndx, value.get<ObjectId>());
                break;
            }
            case type_TypedLink: {
                repl->set_insert_typed_link(*this, ndx, value.get<ObjLink>());
                break;
            }

            case type_OldTable:
                [[fallthrough]];
            case type_Mixed:
                [[fallthrough]];
            case type_LinkList:
                [[fallthrough]];
            case type_Link:
                [[fallthrough]];
            case type_OldDateTime:
                REALM_TERMINATE("Invalid Mixed type");
        }
    }
}

template <>
void Set<ObjLink>::insert_repl(Replication* repl, ObjLink value, size_t ndx)
{
    repl->set_insert_typed_link(*this, value, ndx);
}

template <>
void Set<Timestamp>::insert_repl(Replication* repl, Timestamp value, size_t ndx)
{
    repl->set_insert_timestamp(*this, value, ndx);
}

template <>
void Set<BinaryData>::insert_repl(Replication* repl, BinaryData value, size_t ndx)
{
    repl->set_insert_binary(*this, value, ndx);
}

template <>
void Set<float>::insert_repl(Replication* repl, float value, size_t ndx)
{
    repl->set_insert_float(*this, value, ndx);
}

template <>
void Set<double>::insert_repl(Replication* repl, double value, size_t ndx)
{
    repl->set_insert_double(*this, value, ndx);
}

template <>
void Set<Decimal128>::insert_repl(Replication* repl, Decimal128 value, size_t ndx)
{
    repl->set_insert_decimal(*this, value, ndx);
}

template <>
void Set<ObjectId>::insert_repl(Replication* repl, ObjectId value, size_t ndx)
{
    repl->set_insert_object_id(*this, value, ndx);
}


/***************************** Set<T>::erase_repl ******************************/

template <>
void Set<Int>::erase_repl(Replication*, int64_t, size_t)
{
    // repl->set_erase_int(*this, value);
}

template <>
void Set<String>::erase_repl(Replication*, StringData, size_t)
{
    // repl->set_erase_string(*this, value);
}

template <>
void Set<Mixed>::erase_repl(Replication*, Mixed, size_t)
{
    // repl->set_erase_any(*this, value);
}

} // namespace realm
