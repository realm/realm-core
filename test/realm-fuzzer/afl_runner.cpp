/*************************************************************************
 *
 * Copyright 2022 Realm Inc.
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

#include "fuzz_engine.hpp"

int main(int argc, const char* argv[])
{
    FuzzEngine fuzz_engine;
    bool enable_logging = false;
    std::string path = "realm-afl.txt";
    size_t input_index = 0;
    for (size_t i = 0; i < (size_t)argc; ++i) {
        if (strcmp(argv[i], "--log") == 0) {
            enable_logging = true;
        }
        else {
            input_index = i;
        }
    }
    return fuzz_engine.run_fuzzer(argv[input_index], "realm_afl", enable_logging, path);
}