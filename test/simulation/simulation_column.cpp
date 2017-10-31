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

#include "simulation_column.hpp"

using namespace realm;
using namespace realm::simulation;

SimulationColumn::SimulationColumn(DataType type, std::string name)
: m_type(type), m_name(name)
{
}

SimulationColumn::SimulationColumn(DataType type, std::string name, StableKey linked_table)
: m_type(type)
, m_name(name)
, m_linked_table(linked_table)
{
}

SimulationColumn::~SimulationColumn() noexcept
{
}

void SimulationColumn::insert_value(size_t ndx, AnyType value)
{
    REALM_ASSERT_EX(ndx <= m_values.size(), ndx, m_values.size());
    m_values.insert(m_values.begin() + ndx, value);
}

AnyType SimulationColumn::get_value(size_t ndx) const
{
    REALM_ASSERT_EX(ndx < m_values.size(), ndx, m_values.size());
    return m_values[ndx];
}

void SimulationColumn::set_name(std::string name)
{
    m_name = name;
}

std::string SimulationColumn::get_name() const
{
    return m_name;
}

StableKey SimulationColumn::get_id() const
{
    return m_key;
}

realm::DataType SimulationColumn::get_type() const
{
    return m_type;
}
