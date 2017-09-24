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

#include <realm/obj_list.hpp>
#include <realm/table.hpp>

using namespace realm;

inline void ObjList::allocate_key_values()
{
    // FIXME: This code is unreasonably complicated because it uses `IntegerColumn` as
    // a free-standing container, and beause `IntegerColumn` does not conform to the
    // RAII idiom (nor should it).
    Allocator& alloc = m_key_values.get_alloc();
    _impl::DeepArrayRefDestroyGuard ref_guard(alloc);
    ref_guard.reset(IntegerColumn::create(alloc)); // Throws
    m_key_values.init_from_ref(alloc, ref_guard.release());
}


ObjList::ObjList()
    : m_key_values(IntegerColumn::unattached_root_tag(), Allocator::get_default())
#ifdef REALM_COOKIE_CHECK
    , m_debug_cookie(cookie_expected)
#endif
{
    ref_type ref = IntegerColumn::create(m_key_values.get_alloc()); // Throws
    m_key_values.get_root_array()->init_from_ref(ref);
}

ObjList::ObjList(const ObjList& source)
    : m_table(source.m_table)
#ifdef REALM_COOKIE_CHECK
    , m_debug_cookie(source.m_debug_cookie)
#endif
{
    Allocator& alloc = m_key_values.get_alloc();
    MemRef mem = source.m_key_values.get_root_array()->clone_deep(alloc); // Throws
    m_key_values.init_from_mem(alloc, mem);
}

ObjList::ObjList(ObjList&& source)
    : m_table(std::move(source.m_table))
    , m_key_values(std::move(source.m_key_values))
#ifdef REALM_COOKIE_CHECK
    , m_debug_cookie(source.m_debug_cookie)
#endif
{
}


ObjList::ObjList(Table* parent)
    : m_table(parent->get_table_ref())
    , m_key_values(IntegerColumn::unattached_root_tag(), Allocator::get_default())
#ifdef REALM_COOKIE_CHECK
    , m_debug_cookie(cookie_expected)
#endif
{
    allocate_key_values();
}

ObjList::ObjList(IntegerColumn&& col)
    : m_key_values(std::move(col))
#ifdef REALM_COOKIE_CHECK
    , m_debug_cookie(cookie_expected)
#endif
{
}

// FIXME: this only works (and is only used) for row indexes with memory
// managed by the default allocator, e.q. for TableViews.
ObjList::ObjList(const ObjList& source, ConstSourcePayload mode)
#ifdef REALM_COOKIE_CHECK
    : m_debug_cookie(source.m_debug_cookie)
#endif
{
    REALM_ASSERT(&source.m_key_values.get_alloc() == &Allocator::get_default());

    if (mode == ConstSourcePayload::Copy && source.m_key_values.is_attached()) {
        MemRef mem = source.m_key_values.clone_deep(Allocator::get_default());
        m_key_values.init_from_mem(Allocator::get_default(), mem);
    }
}

ObjList::ObjList(ObjList& source, MutableSourcePayload)
#ifdef REALM_COOKIE_CHECK
    : m_debug_cookie(source.m_debug_cookie)
#endif
{
    REALM_ASSERT(&source.m_key_values.get_alloc() == &Allocator::get_default());

    // move the data payload, but make sure to leave the source array intact or
    // attempts to reuse it for a query rerun will crash (or assert, if lucky)
    // There really *has* to be a way where we don't need to first create an empty
    // array, and then destroy it
    if (source.m_key_values.is_attached()) {
        m_key_values.detach();
        m_key_values.init_from_mem(Allocator::get_default(), source.m_key_values.get_mem());
        source.m_key_values.init_from_ref(Allocator::get_default(), IntegerColumn::create(Allocator::get_default()));
    }
}

ConstObj ObjList::get(size_t row_ndx) const noexcept
{
    REALM_ASSERT(m_table);
    REALM_ASSERT(row_ndx < m_key_values.size());
    Key key(m_key_values.get(row_ndx));
    REALM_ASSERT(key != realm::null_key);
    return m_table->get_object(key);
}
