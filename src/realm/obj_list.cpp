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

#include <realm/obj_list.hpp>
#include <realm/table.hpp>
#include <realm/sort_descriptor.hpp>

using namespace realm;

ObjList::ObjList()
#ifdef REALM_COOKIE_CHECK
    : m_debug_cookie(cookie_expected)
#endif
{
}

ObjList::ObjList(const Table* parent)
    : m_table(parent)
#ifdef REALM_COOKIE_CHECK
    , m_debug_cookie(cookie_expected)
#endif
{
}

void ObjList::assign(const Table* parent)
{
    m_table = ConstTableRef(parent);
}
