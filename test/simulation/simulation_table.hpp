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

#ifndef REALM_SIMULATION_TABLE_HPP
#define REALM_SIMULATION_TABLE_HPP

#include <string>
#include <vector>

#include "simulation_column.hpp"

namespace realm {
namespace simulation {

class SimulationTable {
public:
    SimulationTable(std::string table_name);
    ~SimulationTable() noexcept;

    std::string get_name() const;
    void set_name(std::string table_name);

    void insert_column(size_t ndx, DataType type, std::string name);
    void remove_column(size_t ndx);
    void rename_column(size_t ndx, std::string name);
    std::string get_column_name(size_t ndx) const;
    
private:
    std::vector<SimulationColumn> m_columns;
    std::string m_name;
    StableKey m_key;
};

} // namespace simulation
} // namespace realm

#endif // REALM_SIMULATION_TABLE_HPP
