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

#include <realm/row.hpp>
#include <realm/table.hpp>
#include <realm/group.hpp>

using namespace realm;


void RowBase::attach(Table* table, size_t row_ndx) noexcept
{
    if (table) {
        table->register_row_accessor(this);
        m_table.reset(table);
        m_row_ndx = row_ndx;
    }
}

void RowBase::reattach(Table* table, size_t row_ndx) noexcept
{
    if (m_table.get() != table) {
        if (m_table)
            m_table->unregister_row_accessor(this);
        if (table)
            table->register_row_accessor(this);
        m_table.reset(table);
    }
    m_row_ndx = row_ndx;
}

void RowBase::impl_detach() noexcept
{
    if (m_table) {
        m_table->unregister_row_accessor(this);
        m_table.reset();
    }
}

RowBase::RowBase(const RowBase& source, HandoverPatch& patch)
    : m_table(TableRef())
{
    generate_patch(source, patch);
}

void RowBase::generate_patch(const RowBase& source, HandoverPatch& patch)
{
    Table::generate_patch(source.m_table.get(), patch.m_table);
    patch.row_ndx = source.m_row_ndx;
}

void RowBase::apply_patch(HandoverPatch& patch, Group& group)
{
    m_table = Table::create_from_and_consume_patch(patch.m_table, group);
    if (m_table)
        m_table->register_row_accessor(this);
    m_row_ndx = patch.row_ndx;
}
