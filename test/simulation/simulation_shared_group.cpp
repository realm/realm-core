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

#include "simulation_shared_group.hpp"

#include <realm/group.hpp>

using namespace realm;
using namespace realm::simulation;

SimulationSharedGroup::SimulationSharedGroup()
{
}

SimulationSharedGroup::~SimulationSharedGroup() noexcept
{
}

void SimulationSharedGroup::prune_orphaned_groups()
{
    groups.erase(std::remove_if(groups.begin(), groups.end(),
                                    [](std::shared_ptr<SimulationGroup> snapshot) {
                                        return bool(!snapshot);
                                    }
                                ), groups.end());
}

std::shared_ptr<SimulationGroup> SimulationSharedGroup::get_group(VersionID version)
{
    prune_orphaned_groups();
    for (size_t i = 0; i < groups.size(); ++i) {
        std::shared_ptr<SimulationGroup> group = groups.at(i);
        REALM_ASSERT(group);
        if (group->get_version() == version) {
            return group;
        }
    }
    return nullptr;
}

void SimulationSharedGroup::add_reader(std::shared_ptr<SimulationGroup> group)
{
    groups.push_back(group);
}

void SimulationSharedGroup::begin_write_on(VersionID version)
{
    std::shared_ptr<SimulationGroup> group = get_group(version);
    REALM_ASSERT(group);
    group->begin_write();
}

void SimulationSharedGroup::verify(Group* other) const
{
    REALM_ASSERT(other);
}

