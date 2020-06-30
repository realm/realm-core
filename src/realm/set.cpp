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
#include "realm/array_object_id.hpp"
#include "realm/array_decimal128.hpp"
#include "realm/replication.hpp"

namespace realm {

SetBasePtr Obj::get_setbase_ptr(ColKey col_key) const
{
    REALM_TERMINATE("Not implemented yet");
    return SetBasePtr{};
}

} // namespace realm
