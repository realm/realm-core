////////////////////////////////////////////////////////////////////////////
//
// Copyright 2017 Realm Inc.
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

#ifdef REALM_OS_SYNC_FEATURES_HPP
#define REALM_OS_SYNC_FEATURES_HPP

#if REALM_ENABLE_SYNC

#include <realm/sync/version.hpp>
#define REALM_HAVE_SYNC_STABLE_IDS (REALM_SYNC_VER_MAJOR > 1)

#else

#define REALM_HAVE_SYNC_STABLE_IDS 0

#endif // REALM_ENABLE_SYNC

#endif // REALM_OS_SYNC_FEATURES_HPP
