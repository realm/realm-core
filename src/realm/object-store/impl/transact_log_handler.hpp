////////////////////////////////////////////////////////////////////////////
//
// Copyright 2015 Realm Inc.
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

#ifndef REALM_TRANSACT_LOG_HANDLER_HPP
#define REALM_TRANSACT_LOG_HANDLER_HPP

#include <realm/object-store/binding_context.hpp>
#include <realm/object-store/impl/collection_notifier.hpp>
#include <realm/object-store/index_set.hpp>

#include <realm/transaction.hpp>

namespace realm::_impl {
class NotifierPackage;

struct UnsupportedSchemaChange : std::logic_error {
    UnsupportedSchemaChange();
};

class RealmTransactionObserver : public Transaction::Observer {
public:
    RealmTransactionObserver(Realm& realm, NotifierPackage* notifiers = nullptr);

    void will_advance(Transaction&, DB::version_type old_version, DB::version_type new_version) override;
    void did_advance(Transaction&, DB::version_type old_version, DB::version_type new_version) override;
    void will_reverse(Transaction&, util::Span<const char>) override;

private:
    Realm& m_realm;
    _impl::TransactionChangeInfo m_info;
    std::vector<BindingContext::ObserverState> m_observers;
    std::vector<void*> m_invalidated;
    _impl::NotifierPackage* m_notifiers;
    BindingContext* m_context;
};

void parse(Transaction& tr, TransactionChangeInfo& info, VersionID::version_type initial_version,
           VersionID::version_type end_version);
} // namespace realm::_impl

#endif /* REALM_TRANSACT_LOG_HANDLER_HPP */
