////////////////////////////////////////////////////////////////////////////
//
// Copyright 2020 Realm Inc.
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

#ifndef REALM_OS_AUTH_REQUEST_CLIENT_HPP
#define REALM_OS_AUTH_REQUEST_CLIENT_HPP

#include <realm/object-store/sync/generic_network_transport.hpp>

namespace realm::app {
class User;
struct Response;

class AuthRequestClient {
public:
    virtual ~AuthRequestClient() = default;

    virtual std::string url_for_path(const std::string& path) const = 0;

    virtual void do_authenticated_request(HttpMethod, std::string&& route, std::string&& body,
                                          const std::shared_ptr<User>& user, RequestTokenType,
                                          util::UniqueFunction<void(const Response&)>&&) = 0;
};

} // namespace realm::app

#endif /* REALM_OS_AUTH_REQUEST_CLIENT_HPP */
