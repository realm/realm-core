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

#ifndef REALM_SIMULATION_STABLE_KEY_HPP
#define REALM_SIMULATION_STABLE_KEY_HPP

#include <stdint.h>

namespace realm {
namespace simulation {

class StableKey {
public:
    StableKey();
    ~StableKey() noexcept;

private:
    uint64_t value;
    static uint64_t last_assigned;
};

} // namespace simulation
} // namespace realm

#endif // REALM_SIMULATION_STABLE_KEY_HPP
