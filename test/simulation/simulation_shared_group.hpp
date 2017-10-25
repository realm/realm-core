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

#ifndef REALM_SIMULATION_SHARED_GROUP_HPP
#define REALM_SIMULATION_SHARED_GROUP_HPP

#include <memory>
#include <string>
#include <vector>

#include "simulation_group.hpp"
#include "simulation_table.hpp"

#include <realm/version_id.hpp>

namespace realm {

class Group;

namespace simulation {

struct SimulationGroupSnapshot {
    SimulationGroup group;
    realm::VersionID version;
    size_t num_writers = 0;
};

class SimulationSharedGroup {
public:
    SimulationSharedGroup();
    ~SimulationSharedGroup() noexcept;
    void verify(Group* other) const;
private:
    std::vector<std::shared_ptr<SimulationGroupSnapshot>> groups;
};

} // namespace simulation
} // namespace realm

#endif // REALM_SIMULATION_SHARED_GROUP_HPP
