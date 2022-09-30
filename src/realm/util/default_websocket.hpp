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
#ifndef REALM_DEFAULT_WEBSOCKET_HPP
#define REALM_DEFAULT_WEBSOCKET_HPP

#include <random>
#include <system_error>
#include <map>

#include <realm/sync/config.hpp>
#include <realm/util/client_websocket.hpp>
#include <realm/util/network.hpp>

namespace realm::util::websocket {

// Default Core websocket implementation
class DefaultSocketFactory : public SocketFactory {
public:
    DefaultSocketFactory(const std::string& user_agent, util::Logger& logger)
        : SocketFactory(SocketFactoryConfig{user_agent})
        , logger(logger)
    {
    }

    DefaultSocketFactory(const SocketFactoryConfig& config, util::Logger& logger)
        : SocketFactory(config)
        , logger(logger)
    {
    }

    virtual std::unique_ptr<WebSocket> connect(SocketObserver* observer, Endpoint&& endpoint) override;

private:
    util::Logger& logger;
};

} // namespace realm::util::websocket

#endif // REALM_DEFAULT_WEBSOCKET_HPP
