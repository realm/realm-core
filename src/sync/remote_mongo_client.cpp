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

#include "sync/remote_mongo_client.hpp"
#include "sync/remote_mongo_database.hpp"

namespace realm {
namespace app {

MongoDatabase MongoClient::operator[](const std::string& name)
{
    return MongoDatabase(name, m_user, m_service, m_service_name);
}

MongoDatabase MongoClient::db(const std::string& name)
{
    return MongoDatabase(name, m_user, m_service, m_service_name);
}

} // namespace app
} // namespace realm
