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

#ifndef REALM_SIMULATION_COLUMN_HPP
#define REALM_SIMULATION_COLUMN_HPP

#include <string>
#include <vector>

#include "any_type.hpp"
#include "stable_key.hpp"

namespace realm {
namespace simulation {

class SimulationColumn {
public:
    SimulationColumn(DataType type, std::string name);
    SimulationColumn(DataType type, std::string name, StableKey linked_table);
    ~SimulationColumn() noexcept;
    void insert_value(size_t ndx, AnyType value);
    AnyType get_value(size_t ndx) const;
    void set_name(std::string name);
    std::string get_name() const;
    StableKey get_id() const;
    DataType get_type() const;
private:
    std::vector<AnyType> m_values;
    DataType m_type;
    std::string m_name;
    StableKey m_key;
    StableKey m_linked_table;
};

} // namespace simulation
} // namespace realm

#endif // REALM_SIMULATION_COLUMN_HPP
