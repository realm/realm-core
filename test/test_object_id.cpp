/*************************************************************************
 *
 * Copyright 2019 Realm Inc.
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
#include <realm/object_id.hpp>

#include "test.hpp"


using namespace realm;

TEST(ObjectId_Basics)
{
    std::string init_str("000123450000ffbeef91906c");
    ObjectId id0(init_str.c_str());
    CHECK_EQUAL(id0.to_string(), init_str);
    Timestamp t0(0x12345, 0);
    ObjectId id1(t0, 0xff0000, 0xefbe);
    CHECK_EQUAL(id1.to_string().substr(0, 18), init_str.substr(0, 18));
    CHECK_EQUAL(id1.get_timestamp(), t0);
    ObjectId id2(Timestamp(0x54321, 0));
    CHECK_GREATER(id2, id1);
    CHECK_LESS(id1, id2);

    ObjectId id_null;
    CHECK(id_null.is_null());
}
