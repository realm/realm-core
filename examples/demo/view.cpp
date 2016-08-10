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

using namespace realm;

REALM_TABLE_3(People,
              name, String,
              age,  Int,
              hired, Bool)


int main(int, char* [])
{
    // Create table
    People t;

    // Add rows
    t.add("John", 13, true);
    t.add("Mary", 18, false);
    t.add("Lars", 16, true);
    t.add("Phil", 43, false);
    t.add("Anni", 20, true);

    // Get a view
    People::View v1 = t.where().hired.equal(true).find_all();
    std::cout << "Hired: " << v1.size() << std::endl;

    // Retire seniors
    People::View v2 = t.where().age.greater(65).find_all();
    for (size_t i = 0; i < v2.size(); ++i) {
        v2[i].hired = false;
    }

    // Remove teenagers
    People::View v3 = t.where().age.between(13, 19).find_all();
    v3.clear();

    std::cout << "Rows: " << t.size() << std::endl;
}
