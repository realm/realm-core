/*************************************************************************
 *
 * Copyright 2022 Realm Inc.
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
#ifndef REALM_LEGACY_WEBSOCKET_HPP
#define REALM_LEGACY_WEBSOCKET_HPP

#include <random>
#include <system_error>
#include <map>

#include <realm/sync/config.hpp>
#include <realm/util/client_websocket.hpp>
#include <realm/util/network.hpp>

namespace realm::util::websocket {

// Legacy socket configuration
struct DefaultSocketFactoryConfig {
    util::Logger& logger;
    std::mt19937_64& random;
    util::network::Service& service;
};

// Legacy Core websocket implementation
class DefaultSocketFactory  : public SocketFactory {
public:
    DefaultSocketFactory(const SocketFactoryConfig& config, const DefaultSocketFactoryConfig& legacy_config)
        : SocketFactory(config)
        , m_legacy_config(legacy_config)
    {
    }

    virtual std::unique_ptr<WebSocket> connect(SocketObserver* observer, Endpoint&& endpoint) override;

    virtual void post(util::UniqueFunction<void()>&& handler) override
    {
        m_legacy_config.service.post(std::move(handler)); // Throws
    }

private:
    const DefaultSocketFactoryConfig m_legacy_config;
};

} // namespace realm::util::websocket

#endif // REALM_CLIENT_WEBSOCKET_HPP
