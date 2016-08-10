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

REALM_TABLE_2(Books,
              title, String,
              author, String)

int main()
{
    // Create group
    Group g1;

    // Create table (as reference)
    People::Ref t1 = g1.add_table<People>("people");

    // Add rows
    t1->add("John", 13, true);
    t1->add("Mary", 18, false);
    t1->add("Lars", 16, true);
    t1->add("Phil", 43, false);
    t1->add("Anni", 20, true);

    // And another table
    Books::Ref t2 = g1.get_table<Books>("books");
    t2->add("I, Robot", "Isaac Asimov");
    t2->add("Childhood's End", "Arthur C. Clarke");

    // and save to disk
    g1.write("test.realm");

    // Read a group from disk
    Group g2("test.realm");
    Books::Ref t3 = g2.get_table<Books>("books");
    std::cout << "Table Books" << std::endl;
    for (size_t i = 0; i < t3->size(); ++i) {
        std::cout << "'" << t3[i].title << "' by " << t3[i].author << std::endl;
    }
}
