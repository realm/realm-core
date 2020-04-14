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

#ifndef REMOTE_MONGO_READ_OPERATION_HPP
#define REMOTE_MONGO_READ_OPERATION_HPP

#include "remote_mongo_client.hpp"

namespace realm {
namespace app {

class RemoteMongoReadOperation {
    
public:
    RemoteMongoReadOperation(std::string command,
                             std::string args_json,
                             std::unique_ptr<AppServiceClient> service) :
    m_command(command),
    m_args_json(args_json),
    m_service(std::move(service)) { }
    
    void execute_read();
private:
    std::string m_command;
    std::string m_args_json;
    std::unique_ptr<AppServiceClient> m_service;
};

} // namespace app
} // namespace realm

#endif /* remote_mongo_read_operation */
