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

#include <realm/version_id.hpp>
#include <realm/keys.hpp>

#include <memory>
#include <string>

namespace realm {
class List;
class Object;
class Query;
class Realm;
class Results;
class TableView;
class Transaction;
template <class> class Lst;

// Opaque type representing an object for handover
class ThreadSafeReferenceBase {
public:
    ThreadSafeReferenceBase(const ThreadSafeReferenceBase&) = delete;
    ThreadSafeReferenceBase& operator=(const ThreadSafeReferenceBase&) = delete;
    ThreadSafeReferenceBase(ThreadSafeReferenceBase&&) = default;
    ThreadSafeReferenceBase& operator=(ThreadSafeReferenceBase&&) = default;
    ThreadSafeReferenceBase() = default;
    virtual ~ThreadSafeReferenceBase();

private:
    friend Realm;
};

template <typename T>
class ThreadSafeReference;

template<>
class ThreadSafeReference<List>: public ThreadSafeReferenceBase {
    friend class Realm;

    ObjKey m_key;
    std::string m_object_schema_name;
    ColKey m_col_key;

    // Precondition: The associated Realm is for the current thread and is not in a write transaction;.
    ThreadSafeReference(List const& value);

    // Precondition: Realm and handover are on same version.
    List import_into(std::shared_ptr<Realm>& r);
};

template<>
class ThreadSafeReference<Object>: public ThreadSafeReferenceBase {
    friend class Realm;

    ObjKey m_key;
    std::string m_object_schema_name;

    // Precondition: The associated Realm is for the current thread and is not in a write transaction;.
    ThreadSafeReference(Object const& value);

    // Precondition: Realm and handover are on same version.
    Object import_into(std::shared_ptr<Realm>& r);
};

template<>
class ThreadSafeReference<Results>: public ThreadSafeReferenceBase {
    friend class Realm;

//    std::unique_ptr<Transaction::Handover<Query>> m_query;
//    DescriptorOrdering::HandoverPatch m_ordering_patch;

    // Precondition: The associated Realm is for the current thread and is not in a write transaction;.
    ThreadSafeReference(Results const& value);

    // Precondition: Realm and handover are on same version.
    Results import_into(std::shared_ptr<Realm>& r);
};
}

#endif /* REALM_THREAD_SAFE_REFERENCE_HPP */
