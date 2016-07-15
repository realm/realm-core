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
#include <realm/impl/destroy_guard.hpp>

namespace realm {

const int64_t detached_ref = -1;

class RowIndexes;

struct IndexPair {
    IndexPair(size_t col_ndx, size_t view_ndx)
        : index_in_column(col_ndx), index_in_view(view_ndx) {}
    size_t index_in_column;
    size_t index_in_view;
};

class LinkChain
{
public:
    LinkChain(size_t single_index);
    LinkChain(std::vector<size_t> chain);
    size_t operator[](size_t index) const { return m_column_indices[index]; }
    size_t size() const { return m_column_indices.size(); }
    util::Optional<size_t> translate(size_t index) const;
    const ColumnBase& init(const ColumnBase* cb, IntegerColumn* row_indexes);
    void cleanup() { m_link_translator.reset(); }
private:
    typedef std::vector<util::Optional<size_t>> NullableVector;
    std::vector<size_t> m_column_indices;
    std::shared_ptr<NullableVector> m_link_translator;
};

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
        Sorter() {}

        Sorter(std::vector<LinkChain> columns, std::vector<bool> ascending)
            : m_link_chains(std::move(columns)), m_ascending(std::move(ascending)) {}

        bool operator()(IndexPair i, IndexPair j) const
        {
            for (size_t t = 0; t < m_columns.size(); t++) {
                size_t index_i = i.index_in_column;
                size_t index_j = j.index_in_column;

                if (m_link_chains[t].size() > 1) {
                    util::Optional<size_t> translated_i = m_link_chains[t].translate(i.index_in_view);
                    util::Optional<size_t> translated_j = m_link_chains[t].translate(j.index_in_view);
                    bool valid_i = bool(translated_i);
                    bool valid_j = bool(translated_j);

                    if (!valid_i && !valid_j) {
                        if (t == m_link_chains.size() - 1) {
                            return false; // Two nulls in last sort column, treat as equals.
                        }
                        else {
                            continue;   // Two nulls in non-last column, check next column for order.
                        }
                    }
                    else if (!valid_i || !valid_j) {
                        // Sort null links at the end if m_ascending[t], else at beginning.
                        return m_ascending[t] == valid_i;
                    }

                    index_i = translated_i.value();
                    index_j = translated_j.value();
                }

                int c = m_columns[t]->compare_values(index_i, index_j);
                if (c != 0)
                    return m_ascending[t] ? c > 0 : c < 0;
            }
            return false; // row(index_i) == row(index_j)
        }

        void init(RowIndexes* row_indexes)
        {
            REALM_ASSERT(row_indexes);
            m_columns.clear();
            m_columns.resize(m_link_chains.size(), nullptr);

            for (size_t i = 0; i < m_link_chains.size(); i++) {
                REALM_ASSERT_EX(m_link_chains[i].size() >= 1, m_link_chains[i].size());
                const ColumnBase& cb = row_indexes->get_column_base(m_link_chains[i][0]);
                const ColumnBase& end_of_chain = m_link_chains[i].init(&cb, &(row_indexes->m_row_indexes));
                m_columns[i] = &end_of_chain;
            }
        }

        void cleanup()
        {
            for (auto& link_chain : m_link_chains) {
                link_chain.cleanup();
            }
        }

        explicit operator bool() const { return !m_link_chains.empty(); }

        std::vector<LinkChain> m_link_chains;
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
