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

#ifndef REALM_THREAD_SAFE_REFERENCE_HPP
#define REALM_THREAD_SAFE_REFERENCE_HPP

#include "list.hpp"
#include "object_accessor.hpp"
#include "results.hpp"

#include <realm/group_shared.hpp>
#include <realm/link_view.hpp>
#include <realm/query.hpp>
#include <realm/row.hpp>
#include <realm/table_view.hpp>

namespace realm {

// Opaque type representing an object for handover
class ThreadSafeReferenceBase {
public:
    ThreadSafeReferenceBase(const ThreadSafeReferenceBase&) = delete;
    ThreadSafeReferenceBase& operator=(const ThreadSafeReferenceBase&) = delete;
    ThreadSafeReferenceBase(ThreadSafeReferenceBase&&) = default;
    ThreadSafeReferenceBase& operator=(ThreadSafeReferenceBase&&) = default;
    ThreadSafeReferenceBase();
    virtual ~ThreadSafeReferenceBase();

    bool is_invalidated() const { return m_source_realm == nullptr; };

protected:
    // Precondition: The associated Realm is for the current thread and is not in a write transaction;.
    ThreadSafeReferenceBase(SharedRealm source_realm);

    SharedGroup& get_source_shared_group() const;

    template <typename V, typename T>
    V invalidate_after_import(Realm& destination_realm, T construct_with_shared_group);

private:
    friend Realm;

    VersionID m_version_id;
    SharedRealm m_source_realm; // Strong reference keeps alive so version stays pinned! Don't touch!!

    bool has_same_config(Realm& realm) const;
    void invalidate();
};

template <typename T>
class ThreadSafeReference: public ThreadSafeReferenceBase {
private:
    friend Realm;

    // Precondition: The associated Realm is for the current thread and is not in a write transaction;.
    ThreadSafeReference(T value);

    // Precondition: Realm and handover are on same version
    T import_into_realm(SharedRealm realm) &&;
};

template<>
class ThreadSafeReference<List>: public ThreadSafeReferenceBase {
private:
    friend Realm;

    std::unique_ptr<SharedGroup::Handover<LinkView>> m_link_view;

    // Precondition: The associated Realm is for the current thread and is not in a write transaction;.
    ThreadSafeReference(List const& value);

    // Precondition: Realm and handover are on same version.
    List import_into_realm(SharedRealm realm) &&;
};

template<>
class ThreadSafeReference<Object>: public ThreadSafeReferenceBase {
private:
    friend Realm;

    std::unique_ptr<SharedGroup::Handover<Row>> m_row;
    std::string m_object_schema_name;

    // Precondition: The associated Realm is for the current thread and is not in a write transaction;.
    ThreadSafeReference(Object const& value);

    // Precondition: Realm and handover are on same version.
    Object import_into_realm(SharedRealm realm) &&;
};

template<>
class ThreadSafeReference<Results>: public ThreadSafeReferenceBase {
private:
    friend Realm;

    std::unique_ptr<SharedGroup::Handover<Query>> m_query;
    SortDescriptor::HandoverPatch m_sort_order;
    SortDescriptor::HandoverPatch m_distinct_descriptor;

    // Precondition: The associated Realm is for the current thread and is not in a write transaction;.
    ThreadSafeReference(Results const& value);

    // Precondition: Realm and handover are on same version.
    Results import_into_realm(SharedRealm realm) &&;
};
}

#endif /* REALM_THREAD_SAFE_REFERENCE_HPP */
