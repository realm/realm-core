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

#ifndef REMOTE_MONGO_DATABASE_HPP
#define REMOTE_MONGO_DATABASE_HPP

#include "sync/remote_mongo_collection.hpp"
#include <json.hpp>
#include <string>

namespace realm {
namespace app {

class AppServiceClient;

class RemoteMongoDatabase {
public:
    ~RemoteMongoDatabase() = default;
    RemoteMongoDatabase(const RemoteMongoDatabase&) = default;
    RemoteMongoDatabase(RemoteMongoDatabase&&) = default;
    RemoteMongoDatabase& operator=(const RemoteMongoDatabase&) = default;
    RemoteMongoDatabase& operator=(RemoteMongoDatabase&&) = default;

    /// The name of this database
    const std::string& name() const
    {
        return m_name;
    }
    
    /// Gets a collection.
    /// @param collection_name The name of the collection to return
    /// @returns The collection as json
    RemoteMongoCollection collection(const std::string& collection_name);
    
    /// Gets a collection.
    /// @param collection_name The name of the collection to return
    /// @returns The collection as json
    RemoteMongoCollection operator[](const std::string& collection_name);
    
private:
    RemoteMongoDatabase(std::string name,
                        std::shared_ptr<AppServiceClient> service,
                        std::string service_name)
    : m_name(name)
    , m_service(service)
    , m_service_name(service_name) { };

    friend class RemoteMongoClient;

    std::string m_name;
    std::shared_ptr<AppServiceClient> m_service;
    std::string m_service_name;
};

} // namespace app
} // namespace realm

#endif /* remote_mongo_database_h */
