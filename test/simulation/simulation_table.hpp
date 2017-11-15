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

    void insert_column(size_t ndx, SimulationColumn col);
    void remove_column(size_t ndx);
    void rename_column(size_t ndx, std::string name);
    void move_column(size_t from, size_t to);
    std::string get_column_name(size_t ndx) const;
    size_t get_num_columns() const;
    size_t get_num_rows() const;
    StableKey get_id() const;
    SimulationColumn& get_column(size_t ndx);
    StableKey get_row_id(size_t row) const;

    void add_row(size_t num_rows, std::vector<AnyType> values = {});
    void insert_row(size_t ndx, size_t num_rows, std::vector<AnyType> values = {});
    void remove_row(size_t ndx);
    void move_last_over(size_t ndx);
    void clear();

private:
    std::vector<SimulationColumn> m_columns;
    std::vector<StableKey> m_ids;
    std::string m_name;
    StableKey m_key;
};

} // namespace simulation
} // namespace realm

#endif // REALM_SIMULATION_TABLE_HPP
