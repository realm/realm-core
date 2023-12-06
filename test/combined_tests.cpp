/*************************************************************************
 *
 * Copyright 2023 Realm Inc.
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

#include "test_all.hpp"
#include "util/test_path.hpp"

#include <iostream>

// see test/object-store/test_runner.cpp
extern int run_object_store_tests(int, const char**);

int main(int argc, const char* argv[])
{
    if (!realm::test_util::initialize_test_path(argc, argv))
        return 1;
    int status = test_all();
    if (status) {
        std::cerr << "core and sync tests failed: " << status << std::endl;
        return status;
    }
    std::cout << "core and sync tests passed\n";
    return run_object_store_tests(argc, argv);
}
