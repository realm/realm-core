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

#include <realm.hpp>
#include <realm/util/file.hpp>

using namespace realm;

REALM_TABLE_1(TestTable,
              value, Int)

int main()
{
    util::File::try_remove("test.realm");
    util::File::try_remove("test.realm.lock");

    // Testing 'async' mode because it has the special requirement of
    // being able to find `realmd` (typically in
    // /usr/local/libexec/).
    bool no_create = false;
    SharedGroup sg("test.realm", no_create, SharedGroup::durability_Async);
    {
        WriteTransaction wt(sg);
        TestTable::Ref test = wt.get_table<TestTable>("test");
        test->add(3821);
        wt.commit();
    }
    {
        ReadTransaction rt(sg);
        TestTable::ConstRef test = rt.get_table<TestTable>("test");
        if (test[0].value != 3821)
            return 1;
    }

    util::File::remove("test.realm");
    util::File::remove("test.realm.lock");
}
