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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include <realm/object-store/sync/jwt.hpp>

#include <realm/object-store/sync/generic_network_transport.hpp>
#include <realm/util/base64.hpp>

namespace realm {

static std::string_view split_token(std::string_view jwt) noexcept
{
    constexpr static char delimiter = '.';

    auto pos = jwt.find(delimiter);
    if (pos == 0 || pos == jwt.npos) {
        return {};
    }
    jwt = jwt.substr(pos + 1);

    pos = jwt.find(delimiter);
    if (pos == jwt.npos) {
        return {};
    }
    auto payload = jwt.substr(0, pos);
    jwt = jwt.substr(pos + 1);

    // We don't use the signature, but verify one is present
    if (jwt.size() == 0 || std::count(jwt.begin(), jwt.end(), '.') != 0) {
        return {};
    }

    return payload;
}

RealmJWT::RealmJWT(std::string_view token)
{
    auto payload = split_token(token);
    if (!payload.data()) {
        throw app::AppError(ErrorCodes::BadToken, "malformed JWT");
    }

    auto json_str = util::base64_decode_to_vector(payload);
    if (!json_str) {
        throw app::AppError(ErrorCodes::BadToken, "JWT payload could not be base64 decoded");
    }

    auto json = static_cast<bson::BsonDocument>(bson::parse(*json_str));

    this->token = token;
    this->expires_at = static_cast<int64_t>(json["exp"]);
    this->issued_at = static_cast<int64_t>(json["iat"]);

    if (auto user_data = json.find("user_data")) {
        this->user_data = static_cast<bson::BsonDocument>(*user_data);
    }
}

bool RealmJWT::validate(std::string_view token)
{
    auto payload = split_token(token);
    if (!payload.data()) {
        return false;
    }

    auto json_str = util::base64_decode_to_vector(payload);
    if (!json_str) {
        return false;
    }

    return bson::accept(*json_str);
}

} // namespace realm
