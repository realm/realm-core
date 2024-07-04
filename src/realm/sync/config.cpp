////////////////////////////////////////////////////////////////////////////
//
// Copyright 2015 Realm Inc.
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

#include <realm/sync/config.hpp>
#include <realm/sync/client.hpp>
#include <realm/sync/protocol.hpp>
#include <realm/sync/network/network.hpp>
#include <realm/util/bson/bson.hpp>

#include <ostream>

namespace realm {
namespace {

constexpr static std::string_view s_middle_part(" Logs: ");
std::string format_sync_error_message(const Status& status, std::optional<std::string_view> log_url)
{
    if (!log_url) {
        return status.reason();
    }

    return util::format("%1%2%3", status.reason(), s_middle_part, *log_url);
}

} // namespace
// sync defines its own copy of port_type to avoid depending on network.hpp, but they should be the same.
static_assert(std::is_same_v<sync::port_type, sync::network::Endpoint::port_type>);

using ProtocolError = realm::sync::ProtocolError;

SyncError::SyncError(Status orig_status, bool is_fatal, std::optional<std::string_view> server_log,
                     std::vector<sync::CompensatingWriteErrorInfo> compensating_writes)
    : status(orig_status.code(), format_sync_error_message(orig_status, server_log))
    , is_fatal(is_fatal)
    , simple_message(std::string_view(status.reason()).substr(0, orig_status.reason().size()))
    , compensating_writes_info(std::move(compensating_writes))
{
    if (server_log) {
        logURL = std::string_view(status.reason()).substr(simple_message.size() + s_middle_part.size());
    }
}

/// The error indicates a client reset situation.
bool SyncError::is_client_reset_requested() const
{
    if (server_requests_action == sync::ProtocolErrorInfo::Action::ClientReset ||
        server_requests_action == sync::ProtocolErrorInfo::Action::ClientResetNoRecovery) {
        return true;
    }
    if (status == ErrorCodes::AutoClientResetFailed) {
        return true;
    }
    return false;
}

std::ostream& operator<<(std::ostream& os, const ClientResyncMode& mode)
{
    switch (mode) {
        case ClientResyncMode::Manual:
            return os << "Manual";
        case ClientResyncMode::DiscardLocal:
            return os << "DiscardLocal";
        case ClientResyncMode::Recover:
            return os << "Recover";
        case ClientResyncMode::RecoverOrDiscard:
            return os << "RecoverOrDiscard";
    }
    REALM_TERMINATE("Invalid ClientResyncMode value");
}

std::ostream& operator<<(std::ostream& os, const SyncClientHookEvent& event)
{
    switch (event) {
        case SyncClientHookEvent::DownloadMessageReceived:
            return os << "DownloadMessageReceived";
        case SyncClientHookEvent::DownloadMessageIntegrated:
            return os << "DownloadMessageIntegrated";
        case SyncClientHookEvent::BootstrapMessageProcessed:
            return os << "BootstrapMessageProcessed";
        case SyncClientHookEvent::BootstrapProcessed:
            return os << "BootstrapProcessed";
        case SyncClientHookEvent::ErrorMessageReceived:
            return os << "ErrorMessageReceived";
        case SyncClientHookEvent::SessionActivating:
            return os << "SessionActivating";
        case SyncClientHookEvent::SessionSuspended:
            return os << "SessionSuspended";
        case SyncClientHookEvent::SessionConnected:
            return os << "SessionConnected";
        case SyncClientHookEvent::SessionResumed:
            return os << "SessionResumed";
        case SyncClientHookEvent::BindMessageSent:
            return os << "BindMessageSent";
        case SyncClientHookEvent::IdentMessageSent:
            return os << "IdentMessageSent";
        case SyncClientHookEvent::ClientResetMergeComplete:
            return os << "ClientResetMergeComplete";
        case SyncClientHookEvent::BootstrapBatchAboutToProcess:
            return os << "BootstrapBatchAboutToProcess";
        case SyncClientHookEvent::UploadMessageSent:
            return os << "UploadMessageSent";
    }
    REALM_TERMINATE("Invalid SyncClientHookEvent value");
}

std::ostream& operator<<(std::ostream& os, const SyncClientHookAction& action)
{
    switch (action) {
        case SyncClientHookAction::NoAction:
            return os << "NoAction";
        case SyncClientHookAction::EarlyReturn:
            return os << "EarlyReturn";
        case SyncClientHookAction::SuspendWithRetryableError:
            return os << "SuspendWithRetryableError";
        case SyncClientHookAction::TriggerReconnect:
            return os << "TriggerReconnect";
    }
    REALM_TERMINATE("Invalid SyncClientHookAction value");
}

SyncConfig::SyncConfig(std::shared_ptr<SyncUser> user, bson::Bson partition)
    : user(std::move(user))
    , partition_value(partition.to_string())
{
}
SyncConfig::SyncConfig(std::shared_ptr<SyncUser> user, std::string partition)
    : user(std::move(user))
    , partition_value(std::move(partition))
{
}
SyncConfig::SyncConfig(std::shared_ptr<SyncUser> user, const char* partition)
    : user(std::move(user))
    , partition_value(partition)
{
}

SyncConfig::SyncConfig(std::shared_ptr<SyncUser> user, FLXSyncEnabled)
    : user(std::move(user))
    , partition_value()
    , flx_sync_requested(true)
{
}

} // namespace realm
