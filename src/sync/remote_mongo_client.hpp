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
#include <realm/util/optional.hpp>
#include <string>
#include <map>

namespace realm {
namespace app {

class RemoteMongoDatabase;

/// A client responsible for communication with the Stitch API
class RemoteMongoClient {
public:

    RemoteMongoClient(std::unique_ptr<AppServiceClient> service) :
    m_service(std::move(service)) { }
    
    /// Gets a `RemoteMongoDatabase` instance for the given database name.
    /// @param name the name of the database to retrieve
    RemoteMongoDatabase operator[](const std::string& name);
  
    /// Gets a `RemoteMongoDatabase` instance for the given database name.
    /// @param name the name of the database to retrieve
    RemoteMongoDatabase db(const std::string& name);
    
private:
    std::unique_ptr<AppServiceClient> m_service;
};

} // namespace app
} // namespace realm

#endif /* remote_mongo_client_hpp */
