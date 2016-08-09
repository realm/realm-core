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

using namespace realm;

// defining a table
REALM_TABLE_2(MyTable,
//              columns: types:
              name,    String,
              age,     Int)

int main()
{
    // create an in-memory shared data structure
    SharedGroup sg("persons.realm", false, SharedGroup::durability_MemOnly);

    // a write transaction
    {
        WriteTransaction tr(sg);

        // create a table
        MyTable::Ref table = tr.add_table<MyTable>("persons");

        // add three rows
        table->add("Mary", 40);
        table->add("Mary", 20);
        table->add("Phil", 43);

        // commit changes
        tr.commit();
    }

    // a read transaction
    {
        ReadTransaction tr(sg);

        // get the table
        MyTable::ConstRef table = tr.get_table<MyTable>("persons");

        // calculate number of rows and total age
        std::cout << table->size() << " " << table->column().age.sum() << std::endl;

        // find persons in the forties
        MyTable::View view = table->where().age.between(40, 49).find_all();
        for (size_t i = 0; i < view.size(); ++i) {
            std::cout << view[i].name << std::endl;
        }
    }
}
