////////////////////////////////////////////////////////////////////////////
//
// Copyright 2024 Realm Inc.
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

#include <realm/util/logger.hpp>
#include <realm/util/functional.hpp>
#include <realm/util/future.hpp>
#include <realm/sync/socket_provider.hpp>
#include <realm/sync/client_base.hpp>

namespace realm {
struct MockableProxySocketProviderCallbacks {
    util::UniqueFunction<util::Future<sync::WebSocketEvent>(uint64_t conn_id, sync::WebSocketEvent&&)>
        on_websocket_event;
    util::UniqueFunction<sync::WebSocketEndpoint(uint64_t conn_id, sync::WebSocketEndpoint&& endpoint)>
        on_websocket_create;
    util::UniqueFunction<util::Future<util::Span<const char>>(uint64_t, util::Span<const char>)> on_websocket_send;
    util::UniqueFunction<void(uint64_t)> on_websocket_close;
};


std::shared_ptr<sync::SyncSocketProvider>
create_mockable_proxy_socket_provider(std::shared_ptr<sync::SyncSocketProvider> proxied_provider,
                                      MockableProxySocketProviderCallbacks&& callbacks);

std::shared_ptr<sync::SyncSocketProvider> get_testing_sync_socket_provider();
std::optional<sync::ResumptionDelayInfo> get_testing_resumption_delay_info();

struct DisableNetworkChaosGuard {
    DisableNetworkChaosGuard();
    ~DisableNetworkChaosGuard();

    DisableNetworkChaosGuard(const DisableNetworkChaosGuard&) = delete;
    DisableNetworkChaosGuard& operator=(const DisableNetworkChaosGuard&) = delete;
    DisableNetworkChaosGuard(DisableNetworkChaosGuard&&) = delete;
    DisableNetworkChaosGuard& operator=(DisableNetworkChaosGuard&&) = delete;
};

} // namespace realm
