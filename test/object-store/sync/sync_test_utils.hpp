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

#ifndef REALM_SYNC_TEST_UTILS_HPP
#define REALM_SYNC_TEST_UTILS_HPP

#include "catch2/catch.hpp"

#include "sync/impl/sync_file.hpp"
#include "sync/impl/sync_metadata.hpp"

namespace realm {

bool results_contains_user(SyncUserMetadataResults& results, const std::string& identity, const std::string& auth_server);
bool results_contains_original_name(SyncFileActionMetadataResults& results, const std::string& original_name);

} // namespace realm

#endif // REALM_SYNC_TEST_UTILS_HPP
