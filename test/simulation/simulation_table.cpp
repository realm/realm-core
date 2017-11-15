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

void SimulationTable::insert_column(size_t ndx, SimulationColumn col)
{
    REALM_ASSERT_EX(ndx <= m_columns.size(), ndx, m_columns.size());
    col.insert_value(0, AnyType::get_default_value(col.get_type()), get_num_rows());
    m_columns.insert(m_columns.begin() + ndx, col);
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

size_t SimulationTable::get_num_columns() const
{
    return m_columns.size();
}

size_t SimulationTable::get_num_rows() const
{
    if (m_columns.size() == 0) {
        return 0;
    }
    return m_columns[0].num_rows();
}

StableKey SimulationTable::get_id() const
{
    return m_key;
}

void SimulationTable::move_column(size_t from, size_t to)
{
    move_range<SimulationColumn>(from, 1, to, m_columns);
}

SimulationColumn& SimulationTable::get_column(size_t ndx)
{
    return m_columns[ndx];
}

StableKey SimulationTable::get_row_id(size_t row) const
{
    return m_ids.at(row);
}

void SimulationTable::add_row(size_t num_rows, std::vector<AnyType> values)
{
    if (m_columns.empty())
        return;
    size_t insert_pos = m_columns[0].num_rows();
    insert_row(insert_pos, num_rows, values);
}

void SimulationTable::insert_row(size_t ndx, size_t num_rows, std::vector<AnyType> values)
{
    if (m_columns.empty())
        return;
    m_ids.insert(m_ids.begin() + ndx, num_rows, StableKey());
    if (values.size() == 0) {
        for (size_t col_ndx = 0; col_ndx < m_columns.size(); ++col_ndx) {
            m_columns[col_ndx].insert_value(ndx, AnyType::get_default_value(m_columns[col_ndx].get_type()), num_rows);
        }
    }
    else {
        REALM_ASSERT(values.size() == m_columns.size());
        for (size_t col_ndx = 0; col_ndx < values.size(); ++col_ndx) {
            REALM_ASSERT(m_columns[col_ndx].get_type() == values[col_ndx].get_type());
            m_columns[col_ndx].insert_value(ndx, values[col_ndx], num_rows);
        }
    }
}

void SimulationTable::remove_row(size_t ndx)
{
    for (size_t col_ndx = 0; col_ndx < m_columns.size(); ++col_ndx) {
        m_columns[col_ndx].remove(ndx);
    }
    m_ids.erase(m_ids.begin() + ndx);
}

void SimulationTable::move_last_over(size_t ndx)
{
    m_ids.erase(m_ids.begin() + ndx);
    for (size_t col_ndx = 0; col_ndx < m_columns.size(); ++col_ndx) {
        m_columns[col_ndx].remove(ndx);
    }
    if (m_ids.size() > 0) {
        size_t last_ndx = m_ids.size() - 1;
        move_range(last_ndx, 1, ndx, m_ids);
        for (size_t col_ndx = 0; col_ndx < m_columns.size(); ++col_ndx) {
            m_columns[col_ndx].move(last_ndx, 1, ndx);
        }
    }
}

void SimulationTable::clear()
{
    m_ids.clear();
    for (size_t col_ndx = 0; col_ndx < m_columns.size(); ++col_ndx) {
        m_columns[col_ndx].clear();
    }
}
