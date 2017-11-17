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
#include <realm/handover_defs.hpp>
#include <realm/sort_descriptor.hpp>

namespace realm {

const int64_t detached_ref = -1;

class RowIndexes;


// This class is for common functionality of ListView and LinkView which inherit from it. Currently it only
// supports sorting and distinct.
class RowIndexes {
public:
    RowIndexes(IntegerColumn::unattached_root_tag urt, realm::Allocator& alloc);
    RowIndexes(IntegerColumn&& col);
    RowIndexes(const RowIndexes& source, ConstSourcePayload mode);
    RowIndexes(RowIndexes& source, MutableSourcePayload mode);

    virtual ~RowIndexes()
    {
#ifdef REALM_COOKIE_CHECK
        m_debug_cookie = 0x7765697633333333; // 0x77656976 = 'view'; 0x33333333 = '3333' = destructed
#endif
    }

    // Disable copying, this is not supported.
    RowIndexes& operator=(const RowIndexes&) = delete;
    RowIndexes(const RowIndexes&) = delete;

    virtual size_t size() const = 0;

    // These two methods are overridden by TableView and LinkView.
    virtual uint_fast64_t sync_if_needed() const = 0;
    virtual bool is_in_sync() const
    {
        return true;
    }

    void check_cookie() const
    {
#ifdef REALM_COOKIE_CHECK
        REALM_ASSERT_RELEASE(m_debug_cookie == cookie_expected);
#endif
    }

    IntegerColumn m_row_indexes;

protected:
    void do_sort(const DescriptorOrdering& ordering);

    static const uint64_t cookie_expected = 0x7765697677777777ull; // 0x77656976 = 'view'; 0x77777777 = '7777' = alive
    uint64_t m_debug_cookie;
};

} // namespace realm

#endif // REALM_VIEWS_HPP
