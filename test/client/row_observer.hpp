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

#ifndef REALM_TEST_CLIENT_ROW_OBSERVER_HPP
#define REALM_TEST_CLIENT_ROW_OBSERVER_HPP

#include <algorithm>
#include <vector>

#include <realm/impl/transact_log.hpp>
#include <realm/string_data.hpp>


namespace realm {
namespace test_client {

/// \brief Track table-level indexes of rows, and discover new rows added to a
/// specific group-level table.
///
/// If the table does not exist initially, the first table created with a
/// specified name, if any, will be observed. If the table does exist already,
/// but is removed, then the first table with that specified name to be created
/// thereafter, if any, will be observed.
class RowObserver : public _impl::NullInstructionObserver {
public:
    /// \param table_name The name of the group-level table to observe.
    ///
    /// \param table_ndx The group level index of the table to observe, or
    /// `realm::npos` if the table does not already exist. If set to
    /// `realm::npos`, and a new group-level table with the specified name is
    /// created, \a table_ndx will be set to the group-level index of that
    /// table. If the table is moved the a new group-level index, \a table_ndx
    /// will be adjusted accordingly.
    ///
    /// \param new_rows The table-level indexes of a set of rows to track. The
    /// set is allowed to be empty initially, and can be changed or cleared at
    /// any time. Must be ordered according to increasing row index. New row
    /// indexes will be added to the set as new rows are added to the table.
    RowObserver(StringData table_name, TableKey& table_ndx, std::set<ObjKey>& new_rows)
        : m_table_name{table_name}
        , m_table_ndx{table_ndx}
        , m_new_rows{new_rows}
    {
    }

    bool insert_group_level_table(TableKey table_ndx)
    {
        if (!m_table_ndx) {
            m_table_ndx = table_ndx;
        }
        return true;
    }

    bool erase_group_level_table(TableKey table_ndx)
    {
        if (table_ndx == m_table_ndx) {
            m_new_rows.clear();
            m_table_ndx = TableKey{};
        }
        return true;
    }

    bool select_table(TableKey group_level_ndx)
    {
        m_is_table_selected = (group_level_ndx == m_table_ndx);
        return true;
    }

    bool create_object(ObjKey row_ndx)
    {
        if (m_is_table_selected) {
            // Add indexes of new rows
            m_new_rows.insert(row_ndx);
        }
        return true;
    }

    bool remove_object(ObjKey row_ndx)
    {
        if (m_is_table_selected) {
            // Remove erased rows
            m_new_rows.erase(row_ndx);
        }
        return true;
    }

private:
    StringData m_table_name;
    TableKey& m_table_ndx;
    bool m_is_table_selected = false;
    std::set<ObjKey>& m_new_rows; // Ordered by increasing row index
};

} // namespace test_client
} // namespace realm

#endif // REALM_TEST_CLIENT_ROW_OBSERVER_HPP
