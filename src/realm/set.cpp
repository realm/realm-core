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
#include "realm/array_basic.hpp"
#include "realm/array_integer.hpp"
#include "realm/array_bool.hpp"
#include "realm/array_string.hpp"
#include "realm/array_binary.hpp"
#include "realm/array_timestamp.hpp"
#include "realm/array_decimal128.hpp"
#include "realm/array_fixed_bytes.hpp"
#include "realm/array_typed_link.hpp"
#include "realm/array_mixed.hpp"
#include "realm/replication.hpp"

namespace realm {

// FIXME: This method belongs in obj.cpp.
SetBasePtr Obj::get_setbase_ptr(ColKey col_key) const
{
    auto attr = get_table()->get_column_attr(col_key);
    REALM_ASSERT(attr.test(col_attr_Set));
    bool nullable = attr.test(col_attr_Nullable);

    switch (get_table()->get_column_type(col_key)) {
        case type_Int: {
            if (nullable)
                return std::make_unique<Set<util::Optional<Int>>>(*this, col_key);
            else
                return std::make_unique<Set<Int>>(*this, col_key);
        }
        case type_Bool: {
            if (nullable)
                return std::make_unique<Set<util::Optional<Bool>>>(*this, col_key);
            else
                return std::make_unique<Set<Bool>>(*this, col_key);
        }
        case type_Float: {
            if (nullable)
                return std::make_unique<Set<util::Optional<Float>>>(*this, col_key);
            else
                return std::make_unique<Set<Float>>(*this, col_key);
        }
        case type_Double: {
            if (nullable)
                return std::make_unique<Set<util::Optional<Double>>>(*this, col_key);
            else
                return std::make_unique<Set<Double>>(*this, col_key);
        }
        case type_String: {
            return std::make_unique<Set<String>>(*this, col_key);
        }
        case type_Binary: {
            return std::make_unique<Set<Binary>>(*this, col_key);
        }
        case type_Timestamp: {
            return std::make_unique<Set<Timestamp>>(*this, col_key);
        }
        case type_Decimal: {
            return std::make_unique<Set<Decimal128>>(*this, col_key);
        }
        case type_ObjectId: {
            if (nullable)
                return std::make_unique<Set<util::Optional<ObjectId>>>(*this, col_key);
            else
                return std::make_unique<Set<ObjectId>>(*this, col_key);
        }
        case type_UUID: {
            if (nullable)
                return std::make_unique<Set<util::Optional<UUID>>>(*this, col_key);
            else
                return std::make_unique<Set<UUID>>(*this, col_key);
        }
        case type_TypedLink: {
            return std::make_unique<Set<ObjLink>>(*this, col_key);
        }
        case type_Mixed: {
            return std::make_unique<Set<Mixed>>(*this, col_key);
        }
        case type_LinkList:
            [[fallthrough]];
        case type_Link:
            [[fallthrough]];
        case type_OldDateTime:
            [[fallthrough]];
        case type_OldTable:
            REALM_ASSERT(false);
            break;
    }
    return {};
}

void SetBase::insert_repl(Replication* repl, size_t index, Mixed value) const
{
    repl->set_insert(*this, index, value);
}

void SetBase::erase_repl(Replication* repl, size_t index, Mixed value) const
{
    repl->set_erase(*this, index, value);
}

void SetBase::clear_repl(Replication* repl) const
{
    repl->set_clear(*this);
}

} // namespace realm
