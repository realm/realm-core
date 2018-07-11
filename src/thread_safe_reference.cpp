////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include "thread_safe_reference.hpp"

#include "impl/realm_coordinator.hpp"
#include "list.hpp"
#include "object.hpp"
#include "object_schema.hpp"
#include "object_store.hpp"
#include "results.hpp"

#include <realm/util/scope_exit.hpp>
#include <realm/db.hpp>

using namespace realm;

ThreadSafeReferenceBase::~ThreadSafeReferenceBase()
{
}

ThreadSafeReference<List>::ThreadSafeReference(List const& list)
    : ThreadSafeReferenceBase()
    , m_key(list.m_list_base->get_key())
    , m_table_key(list.m_list_base->get_table()->get_key())
    , m_col_key(list.m_list_base->get_col_key())
{
}

ThreadSafeReference<Results>::ThreadSafeReference(Results const&) {}

ThreadSafeReference<Object>::ThreadSafeReference(Object const& object)
    : ThreadSafeReferenceBase()
    , m_key(object.obj().get_key())
    , m_object_schema_name(object.get_object_schema().name)
{
}

List ThreadSafeReference<List>::import_into(std::shared_ptr<Realm>& r)
{
    try {
        Obj obj = r->read_group().get_table(m_table_key)->get_object(m_key);
        return List(r, obj, m_col_key);
    }
    catch (const InvalidKey&) {
    }
    return {};
}

Results ThreadSafeReference<Results>::import_into(std::shared_ptr<Realm>&) { REALM_TERMINATE("not implemented"); }

Object ThreadSafeReference<Object>::import_into(std::shared_ptr<Realm>& r)
{
    return Object(r, m_object_schema_name, m_key);
}

#if 0
ThreadSafeReference<List>::ThreadSafeReference(List const& list)
: ThreadSafeReferenceBase(list.get_realm())
, m_link_view(get_source_shared_group().export_linkview_for_handover(list.m_link_view))
, m_table(get_source_shared_group().export_table_for_handover(list.m_table))
{ }

List ThreadSafeReference<List>::import_into_realm(SharedRealm realm) && {
    return invalidate_after_import<List>(*realm, [&](Transaction& shared_group) {
        if (auto link_view = shared_group.import_linkview_from_handover(std::move(m_link_view)))
            return List(std::move(realm), std::move(link_view));
        return List(std::move(realm), shared_group.import_table_from_handover(std::move(m_table)));
    });
}

Object ThreadSafeReference<Object>::import_into_realm(SharedRealm realm) && {
    return invalidate_after_import<Object>(*realm, [&](Transaction& shared_group) {
        Row row = *shared_group.import_from_handover(std::move(m_row));
        auto object_schema = realm->schema().find(m_object_schema_name);
        REALM_ASSERT_DEBUG(object_schema != realm->schema().end());
        return Object(std::move(realm), *object_schema, row);
    });
}

ThreadSafeReference<Results>::ThreadSafeReference(Results const& results)
: ThreadSafeReferenceBase(results.get_realm())
, m_query(get_source_shared_group().export_for_handover(results.get_query(), ConstSourcePayload::Copy))
, m_ordering_patch([&]() {
    DescriptorOrdering::HandoverPatch ordering_patch;
    DescriptorOrdering::generate_patch(results.get_descriptor_ordering(), ordering_patch);
    return ordering_patch;
}()){ }

Results ThreadSafeReference<Results>::import_into_realm(SharedRealm realm) && {
    return invalidate_after_import<Results>(*realm, [&](Transaction& shared_group) {
        Query query = *shared_group.import_from_handover(std::move(m_query));
        Table& table = *query.get_table();
        DescriptorOrdering descriptors = DescriptorOrdering::create_from_and_consume_patch(m_ordering_patch, table);
        return Results(std::move(realm), std::move(query), std::move(descriptors));
    });
}
#endif
