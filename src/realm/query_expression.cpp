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

#include <realm/query_expression.hpp>

namespace realm {

void Columns<Link>::evaluate(size_t index, ValueBase& destination)
{
    std::vector<size_t> links = m_link_map.get_links(index);
    Value<RowIndex> v = make_value_for_link<RowIndex>(m_link_map.only_unary_links(), links.size());

    for (size_t t = 0; t < links.size(); t++) {
        v.m_storage.set(t, RowIndex(links[t]));
    }
    destination.import(v);
}

void Columns<SubTable>::evaluate(size_t index, ValueBase& destination)
{
    REALM_ASSERT_DEBUG(dynamic_cast<Value<ConstTableRef>*>(&destination) != nullptr);
    Value<ConstTableRef>* d = static_cast<Value<ConstTableRef>*>(&destination);
    REALM_ASSERT(d);

    if (m_link_map.m_link_columns.size() > 0) {
        std::vector<size_t> links = m_link_map.get_links(index);
        auto sz = links.size();

        if (m_link_map.only_unary_links()) {
            ConstTableRef val;
            if (sz == 1) {
                val = ConstTableRef(m_column->get_subtable_ptr(links[0]));
            }
            d->init(false, 1, val);
        }
        else {
            d->init(true, sz);
            for (size_t t = 0; t < sz; t++) {
                const Table* table = m_column->get_subtable_ptr(links[t]);
                d->m_storage.set(t, ConstTableRef(table));
            }
        }
    }
    else {
        // Adding zero to ValueBase::default_size to avoid taking the address
        size_t rows = std::min(m_column->size() - index, ValueBase::default_size + 0);

        d->init(false, rows);

        for (size_t t = 0; t < rows; t++) {
            const Table* table = m_column->get_subtable_ptr(index + t);
            d->m_storage.set(t, ConstTableRef(table));
        }
    }
}
}
