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

#include "simulation_group.hpp"

#include <realm/group.hpp>

using namespace realm;
using namespace realm::simulation;

SimulationGroup::SimulationGroup(VersionID version)
: version_id(version)
{
}

SimulationGroup::~SimulationGroup() noexcept
{
}

void SimulationGroup::verify(const Group& other) const
{
    REALM_ASSERT_EX(other.size() == tables.size(), other.size(), tables.size());

    for (size_t i = 0; i < tables.size(); ++i) {
        StringData name1 = other.get_table_name(i);
        std::string name2 = tables[i].get_name();
        REALM_ASSERT(name1 == name2);
    }
}

realm::VersionID SimulationGroup::get_version() const
{
    return version_id;
}

void SimulationGroup::commit_version(realm::VersionID new_version)
{
    version_id = new_version;
}

void SimulationGroup::add_table(std::string name, size_t ndx)
{
    if (ndx > tables.size()) {
        ndx = tables.size();
    }
    tables.insert(tables.begin() + ndx, SimulationTable(name));
}

void SimulationGroup::remove_table(size_t ndx)
{
    tables.erase(tables.begin() + ndx);
}

template <typename T>
void move_range(size_t start, size_t length, size_t dst, std::vector<T> & v)
{
    typename std::vector<T>::iterator first, middle, last;
    if (start < dst)
    {
        first  = v.begin() + start;
        middle = first + length;
        last   = v.begin() + dst + 1;
    }
    else
    {
        first  = v.begin() + dst;
        middle = v.begin() + start;
        last   = middle + length;
    }
    std::rotate(first, middle, last);
}

void SimulationGroup::move_table(size_t from, size_t to)
{
    move_range<SimulationTable>(from, 1, to, tables);
}

void SimulationGroup::rename_table(size_t ndx, std::string name)
{
    tables[ndx].set_name(name);
}

std::string SimulationGroup::get_table_name(size_t ndx)
{
    return tables[ndx].get_name();
}
