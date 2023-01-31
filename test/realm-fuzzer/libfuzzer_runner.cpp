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
#include <exception>

// This function is the entry point for libfuzzer, main is auto-generated
extern "C" int LLVMFuzzerTestOneInput(const uint8_t* Data, size_t Size);

int LLVMFuzzerTestOneInput(const uint8_t* Data, size_t Size)
{
    if (Size == 0)
        return 0;
    std::string input{(const char*)Data, Size};
    FuzzEngine fuzz_engine;
    return fuzz_engine.run_fuzzer(input, "realm_libfuzz", false,
                                  "realm-libfuzz.txt"); // run the fuzzer with no logging
}
