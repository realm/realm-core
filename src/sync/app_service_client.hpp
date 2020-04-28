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


#ifndef APP_SERVICE_CLIENT_HPP
#define APP_SERVICE_CLIENT_HPP

#include "sync/generic_network_transport.hpp"
#include "sync/auth_request_client.hpp"
#include <realm/util/optional.hpp>
#include <string>

namespace realm {
namespace app {

/// A class providing the core functionality necessary to make authenticated function call requests for a particular
/// Stitch service.
class AppServiceClient {
public:
        
    AppServiceClient(const std::string& service_name,
                     const std::string& base_route,
                     const std::string& app_id,
                     const AuthRequestClient& auth_request_client) :
    service_name(service_name),
    m_base_route(base_route),
    m_app_id(app_id),
    m_auth_request_client(auth_request_client) { }
    
    AppServiceClient(const AppServiceClient& other) = default;
    AppServiceClient(AppServiceClient&&) = default;

    /// The name of the Realm Cloud service
    std::string service_name;

    /// Calls the Realm Cloud function with the provided name and arguments.
    /// @param name The name of the Realm Cloud function to be called.
    /// @param args_json The `BSONArray` of arguments to be provided to the function.
    /// @param service_name The name of the service, this is optional.
    /// @param completion_block Returns the result from the intended call, will return an Optional AppError is an error is thrown and a json string if successful
    void call_function(const std::string& name,
                       const std::string& args_json,
                       const util::Optional<std::string>& service_name,
                       std::function<void (util::Optional<AppError>, util::Optional<std::string>)> completion_block) const;
    
    /// Calls the Realm Cloud function with the provided name and arguments.
    /// @param name The name of the Realm Cloud function to be called.
    /// @param args_json The `BSONArray` of arguments to be provided to the function.
    /// @param completion_block Returns the result from the intended call, will return an Optional AppError is an error is thrown and a json string if successful
    void call_function(const std::string& name,
                       const std::string& args_json,
                       std::function<void (util::Optional<AppError>, util::Optional<std::string>)> completion_block) const;
    
private:
    std::string m_base_route;
    std::string m_app_id;
    const AuthRequestClient& m_auth_request_client;
};

} // namespace app
} // namespace realm

#endif /* APP_SERVICE_CLIENT_HPP */
