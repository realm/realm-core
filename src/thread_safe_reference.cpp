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

#include "list.hpp"
#include "object.hpp"
#include "object_schema.hpp"
#include "results.hpp"
#include "shared_realm.hpp"

#include <realm/db.hpp>
#include <realm/keys.hpp>

using namespace realm;

namespace realm {
class ThreadSafeReference::Payload {
public:
    virtual ~Payload() = default;
};

template<>
class ThreadSafeReference::PayloadImpl<List> : public ThreadSafeReference::Payload {
public:
    PayloadImpl(List const& list, Transaction& t)
    : m_key(list.get_parent_object_key())
    , m_table_key(list.get_parent_table_key())
    , m_col_key(list.get_parent_column_key())
    , m_version(t.get_version_of_current_transaction())
    {
    }

    List import_into(std::shared_ptr<Realm>&& r, Transaction& t)
    {
        Obj obj = t.get_table(m_table_key)->get_object(m_key);
        return List(std::move(r), obj, m_col_key);
    }

    VersionID desired_version() const noexcept { return m_version; }

private:
    ObjKey m_key;
    TableKey m_table_key;
    ColKey m_col_key;
    VersionID m_version;
};

template<>
class ThreadSafeReference::PayloadImpl<Object> : public ThreadSafeReference::Payload {
public:
    PayloadImpl(Object const& object, Transaction& t)
    : m_key(object.obj().get_key())
    , m_object_schema_name(object.get_object_schema().name)
    , m_version(t.get_version_of_current_transaction())
    {
    }

    Object import_into(std::shared_ptr<Realm>&& r, Transaction&)
    {
        return Object(std::move(r), m_object_schema_name, m_key);
    }

    VersionID desired_version() const noexcept { return m_version; }

private:
    ObjKey m_key;
    std::string m_object_schema_name;
    VersionID m_version;
};

template<>
class ThreadSafeReference::PayloadImpl<Results> : public ThreadSafeReference::Payload {
public:
    PayloadImpl(Results const& r, Transaction& t)
    : m_transaction(t.duplicate())
    , m_query([&] { Query q(r.get_query()); return m_transaction->import_copy_of(q, PayloadPolicy::Move); }())
    , m_ordering(r.get_descriptor_ordering())
    {
    }

    Results import_into(std::shared_ptr<Realm>&& r, Transaction& t)
    {
        auto realm_version = t.get_version_of_current_transaction();
        if (realm_version > m_transaction->get_version_of_current_transaction())
            m_transaction->advance_read(realm_version);

        auto q = t.import_copy_of(*m_query, PayloadPolicy::Copy);
        return Results(std::move(r), std::move(*q), m_ordering);
    }

    VersionID desired_version() const noexcept { return m_transaction->get_version_of_current_transaction(); }

private:
    TransactionRef m_transaction;
    std::unique_ptr<Query> m_query;
    DescriptorOrdering m_ordering;
};

template<>
class ThreadSafeReference::PayloadImpl<std::shared_ptr<Realm>> : public ThreadSafeReference::Payload {
public:
    PayloadImpl(std::shared_ptr<Realm> const& realm)
    : m_realm(realm)
    {
    }

    std::shared_ptr<Realm> get_realm()
    {
        return std::move(m_realm);
    }

private:
    std::shared_ptr<Realm> m_realm;
};

ThreadSafeReference::ThreadSafeReference() noexcept = default;
ThreadSafeReference::~ThreadSafeReference() = default;
ThreadSafeReference::ThreadSafeReference(ThreadSafeReference&&) noexcept = default;
ThreadSafeReference& ThreadSafeReference::operator=(ThreadSafeReference&&) noexcept = default;

template<typename T>
ThreadSafeReference::ThreadSafeReference(T const& value)
{
    auto realm = value.get_realm();
    realm->verify_thread();
    m_payload.reset(new PayloadImpl<T>(value, Realm::Internal::get_transaction(*realm)));
}

template<>
ThreadSafeReference::ThreadSafeReference(std::shared_ptr<Realm> const& value)
{
    m_payload.reset(new PayloadImpl<std::shared_ptr<Realm>>(value));
}

template ThreadSafeReference::ThreadSafeReference(List const&);
template ThreadSafeReference::ThreadSafeReference(Results const&);
template ThreadSafeReference::ThreadSafeReference(Object const&);

template<typename T>
T ThreadSafeReference::resolve(std::shared_ptr<Realm> realm)
{
    REALM_ASSERT(realm);
    realm->verify_thread();

    REALM_ASSERT(m_payload);
    auto& payload = static_cast<PayloadImpl<T>&>(*m_payload);
    REALM_ASSERT(typeid(payload) == typeid(PayloadImpl<T>));

    try {
        if (!realm->is_in_read_transaction())
            Realm::Internal::begin_read(*realm, payload.desired_version());
        auto& transaction = Realm::Internal::get_transaction(*realm);
        if (transaction.get_version_of_current_transaction() < payload.desired_version())
            realm->refresh();
        return payload.import_into(std::move(realm), transaction);
    }
    catch (const InvalidKey&) {
        return {};
    }
}

template<>
std::shared_ptr<Realm> ThreadSafeReference::resolve<std::shared_ptr<Realm>>(std::shared_ptr<Realm>)
{
    REALM_ASSERT(m_payload);
    auto& payload = static_cast<PayloadImpl<std::shared_ptr<Realm>>&>(*m_payload);
    REALM_ASSERT(typeid(payload) == typeid(PayloadImpl<std::shared_ptr<Realm>>));

    return payload.get_realm();
}

template Results ThreadSafeReference::resolve<Results>(std::shared_ptr<Realm>);
template List ThreadSafeReference::resolve<List>(std::shared_ptr<Realm>);
template Object ThreadSafeReference::resolve<Object>(std::shared_ptr<Realm>);

} // namespace realm
