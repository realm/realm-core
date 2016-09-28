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

#include <realm/group.hpp>

using namespace realm;

int main(int argc, const char* const argv[])
{
    if (argc != 2) {
        std::cerr << "Wrong number of command line arguments\n"
                     "Synopsis: "
                  << argv[0] << "  DATABASE-FILE" << std::endl;
        return 1;
    }
    Group g(argv[1], GROUP_READONLY);
    if (!g.is_valid()) {
        std::cerr << "Failed to open Realm database '" << argv[1] << "'" << std::endl;
        return 1;
    }
    g.to_dot(cout);
    return 0;
}
