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

#include <iostream>
#include <realm.hpp>
#include <realm/group_shared.hpp>
#include <realm/util/file.hpp>

using namespace realm;

namespace {

REALM_TABLE_1(MyTable, text, String)

} // namespace

// Results:
// -rw-r--r--  1 kristian kristian 1092616192 Oct 12 15:32 over_alloc_1.db
// -rw-r--r--  1 kristian kristian    1048576 Oct 12 15:32 over_alloc_2.db

int main()
{
    int n_outer = 3000;
    int n_inner = 42;

    // Many transactions
    {
        File::try_remove("over_alloc_1.realm");
        File::try_remove("over_alloc_1.realm.lock");
        DB db("over_alloc_1.realm");
        if (!db.is_valid())
            throw runtime_error("Failed to open database 1");

        for (int i = 0; i < n_outer; ++i) {
            {
                Group& group = db.begin_write();
                MyTable::Ref table = group.get_table<MyTable>("my_table");
                for (int j = 0; j < n_inner; ++j) {
                    table->add("x");
                }
            }
            db.commit();
        }
    }

    // One transaction
    {
        File::try_remove("over_alloc_2.realm");
        File::try_remove("over_alloc_2.realm.lock");
        DB db("over_alloc_2.realm");
        if (!db.is_valid())
            throw runtime_error("Failed to open database 2");

        {
            Group& group = db.begin_write();
            MyTable::Ref table = group.get_table<MyTable>("my_table");
            for (int i = 0; i < n_outer; ++i) {
                for (int j = 0; j < n_inner; ++j) {
                    table->add("x");
                }
            }
        }
        db.commit();
    }

    return 0;
}
