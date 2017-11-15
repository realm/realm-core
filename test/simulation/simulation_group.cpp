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

void SimulationGroup::verify(const Group& other)
{
    REALM_ASSERT_EX(other.size() == tables.size(), other.size(), tables.size());

    for (size_t i = 0; i < tables.size(); ++i) {
        StringData name1 = other.get_table_name(i);
        std::string name2 = tables[i].get_name();
        REALM_ASSERT(name1 == name2);
        size_t num_cols1 = other.get_table(i)->get_column_count();
        size_t num_cols2 = tables.at(i).get_num_columns();
        REALM_ASSERT_EX(num_cols1 == num_cols2, num_cols1, num_cols2);
        for (size_t col = 0; col < num_cols2; ++col) {
            ConstTableRef t1 = other.get_table(i);
            SimulationTable& t2 = tables[i];
            StringData col_name1 = t1->get_column_name(col);
            const std::string& col_name2 = t2.get_column_name(col);
            REALM_ASSERT(col_name1 == StringData(col_name2));
            DataType col_type1 = t1->get_column_type(col);
            DataType col_type2 = t2.get_column(col).get_type();
            REALM_ASSERT(col_type1 == col_type2);
            size_t num_rows1 = t1->size();
            size_t num_rows2 = t2.get_num_rows();
            REALM_ASSERT(num_rows1 == num_rows2);
        }
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

realm::simulation::SimulationTable &SimulationGroup::get_table(size_t ndx)
{
    return tables[ndx];
}

