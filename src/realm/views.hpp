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

#ifndef REALM_VIEWS_HPP
#define REALM_VIEWS_HPP

#include <realm/column.hpp>
#include <realm/column_string_enum.hpp>
#include <realm/handover_defs.hpp>
#include <realm/index_string.hpp>

namespace realm {

const int64_t detached_ref = -1;

// This class is for common functionality of ListView and LinkView which inherit from it. Currently it only
// supports sorting.
class RowIndexes
{
public:
    RowIndexes(IntegerColumn::unattached_root_tag urt, realm::Allocator& alloc) :
#ifdef REALM_COOKIE_CHECK
        cookie(cookie_expected),
#endif
        m_row_indexes(urt, alloc)
    {}

    RowIndexes(IntegerColumn&& col) :
#ifdef REALM_COOKIE_CHECK
        cookie(cookie_expected),
#endif
        m_row_indexes(std::move(col))
    {}

    RowIndexes(const RowIndexes& source, ConstSourcePayload mode);
    RowIndexes(RowIndexes& source, MutableSourcePayload mode);

    virtual ~RowIndexes()
    {
#ifdef REALM_COOKIE_CHECK
        cookie = 0x7765697633333333; // 0x77656976 = 'view'; 0x33333333 = '3333' = destructed
#endif
    }

    // Return a column of the table that m_row_indexes are pointing at (which is the target table for LinkList and
    // parent table for TableView)
    virtual const ColumnBase& get_column_base(size_t index) const = 0;

    virtual size_t size() const = 0;

    // These two methods are overridden by TableView and LinkView.
    virtual uint_fast64_t sync_if_needed() const = 0;
    virtual bool is_in_sync() const { return true; }

    void check_cookie() const
    {
#ifdef REALM_COOKIE_CHECK
        REALM_ASSERT_RELEASE(cookie == cookie_expected);
#endif
    }

    // Predicate for std::sort
    struct Sorter
    {
        Sorter(){}
        Sorter(const std::vector<size_t>& columns, const std::vector<bool>& ascending)
            : m_column_indexes(columns), m_ascending(ascending) {}
        bool operator()(size_t i, size_t j) const
        {
            for (size_t t = 0; t < m_columns.size(); t++) {
                int c = m_columns[t]->compare_values(i, j);

                if (c != 0)
                    return m_ascending[t] ? c > 0 : c < 0;
            }
            return false; // row i == row j
        }

        void init(RowIndexes* row_indexes)
        {
            m_columns.clear();
            m_columns.resize(m_column_indexes.size(), 0);

            for (size_t i = 0; i < m_column_indexes.size(); i++) {
                m_columns[i] = &row_indexes->get_column_base(m_column_indexes[i]);
            }
        }

        explicit operator bool() const { return !m_column_indexes.empty(); }

        std::vector<size_t> m_column_indexes;
        std::vector<bool> m_ascending;
        std::vector<const ColumnBase*> m_columns;
    };

    void sort(Sorter& sorting_predicate);

#ifdef REALM_COOKIE_CHECK
    static const uint64_t cookie_expected = 0x7765697677777777ull; // 0x77656976 = 'view'; 0x77777777 = '7777' = alive
    uint64_t cookie;
#endif

    IntegerColumn m_row_indexes;
};

} // namespace realm

#endif // REALM_VIEWS_HPP
