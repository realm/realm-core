///////////////////////////////////////////////////////////////////////////
//
// Copyright 2022 Realm Inc.
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

#ifndef REALM_NOINST_CLIENT_RESET_RECOVERY_HPP
#define REALM_NOINST_CLIENT_RESET_RECOVERY_HPP

#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/transaction.hpp>
#include <realm/util/logger.hpp>

namespace realm::_impl::client_reset {
struct RecoveredChange {
    util::AppendBuffer<char> encoded_changeset;
    sync::ClientHistory::version_type version;
};

std::vector<RecoveredChange>
process_recovered_changesets(Transaction& dest_tr, Transaction& pre_reset_state, util::Logger& logger,
                             const std::vector<sync::ClientHistory::LocalChange>& changesets);
} // namespace realm::_impl::client_reset

#endif // REALM_NOINST_CLIENT_RESET_RECOVERY_HPP
