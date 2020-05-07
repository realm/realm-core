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

#ifndef REMOTE_MONGO_CLIENT_HPP
#define REMOTE_MONGO_CLIENT_HPP

#include "sync/app_service_client.hpp"
#include <string>
#include <map>

namespace realm {
namespace app {

class RemoteMongoDatabase;

/// A client responsible for communication with the Stitch API
class RemoteMongoClient {
public:
    ~RemoteMongoClient() = default;
    RemoteMongoClient(const RemoteMongoClient&) = default;
    RemoteMongoClient(RemoteMongoClient&&) = default;
    RemoteMongoClient& operator=(const RemoteMongoClient&) = default;
    RemoteMongoClient& operator=(RemoteMongoClient&&) = default;

    /// Gets a `RemoteMongoDatabase` instance for the given database name.
    /// @param name the name of the database to retrieve
    RemoteMongoDatabase operator[](const std::string& name);
  
    /// Gets a `RemoteMongoDatabase` instance for the given database name.
    /// @param name the name of the database to retrieve
    RemoteMongoDatabase db(const std::string& name);
    
private:
    friend class App;

    RemoteMongoClient(std::shared_ptr<AppServiceClient> service, std::string service_name)
    : m_service(service)
    , m_service_name(service_name) {}

    std::shared_ptr<AppServiceClient> m_service;
    std::string m_service_name;
};

} // namespace app
} // namespace realm

#endif /* remote_mongo_client_hpp */
