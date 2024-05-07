/*************************************************************************
 *
 * Copyright 2023 Realm, Inc.
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

#pragma once

#include <realm/db.hpp>
#include <realm/transaction.hpp>

#include <optional>

namespace realm {
namespace _impl::sync_schema_migration {

std::optional<uint64_t> has_pending_migration(const Transaction& rt);

void track_sync_schema_migration(Transaction& wt, uint64_t previous_schema_version);

void perform_schema_migration(DB& db);

} // namespace _impl::sync_schema_migration
} // namespace realm