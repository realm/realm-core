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

#ifndef REALM_OS_NETWORK_REACHABILITY_HPP
#define REALM_OS_NETWORK_REACHABILITY_HPP

#if REALM_PLATFORM_APPLE

#define NETWORK_REACHABILITY_AVAILABLE !TARGET_OS_WATCH

#if NETWORK_REACHABILITY_AVAILABLE
#include "sync/impl/apple/network_reachability_observer.hpp"
#endif

#endif // REALM_PLATFORM_APPLE

#endif // REALM_OS_NETWORK_REACHABILITY_HPP
