////////////////////////////////////////////////////////////////////////////
//
// Copyright 2021 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or utilied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#pragma once

#include <realm.h>
#include <realm/object-store/sync/sync_manager.hpp>

namespace realm::c_api {

realm::SyncClientConfig::LoggerFactory make_logger_factory(realm_log_func_t logger, void* userdata,
                                                           realm_free_userdata_func_t free_userdata);

} // namespace realm::c_api
