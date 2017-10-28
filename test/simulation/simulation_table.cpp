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

#include "simulation_table.hpp"

using namespace realm;
using namespace realm::simulation;

SimulationTable::SimulationTable(std::string table_name)
: m_name(table_name)
{
}

SimulationTable::~SimulationTable() noexcept
{
}

std::string SimulationTable::get_name() const
{
    return m_name;
}

void SimulationTable::set_name(std::string table_name)
{
    m_name = table_name;
}

void SimulationTable::insert_column(size_t ndx, DataType type, std::string name)
{
    REALM_ASSERT_EX(ndx <= m_columns.size(), ndx, m_columns.size());
    m_columns.insert(m_columns.begin() + ndx, SimulationColumn(type, name));
}

void SimulationTable::remove_column(size_t ndx)
{
    REALM_ASSERT_EX(ndx < m_columns.size(), ndx, m_columns.size());
    m_columns.erase(m_columns.begin() + ndx);
}

void SimulationTable::rename_column(size_t ndx, std::string name)
{
    REALM_ASSERT_EX(ndx < m_columns.size(), ndx, m_columns.size());
    m_columns[ndx].set_name(name);
}

std::string SimulationTable::get_column_name(size_t ndx) const
{
    REALM_ASSERT_EX(ndx < m_columns.size(), ndx, m_columns.size());
    return m_columns[ndx].get_name();
}


