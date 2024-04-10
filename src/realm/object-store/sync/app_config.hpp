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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or utilied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#ifndef REALM_OS_SYNC_APP_CONFIG_HPP
#define REALM_OS_SYNC_APP_CONFIG_HPP

#include <realm/object-store/sync/generic_network_transport.hpp>
#include <realm/sync/binding_callback_thread_observer.hpp>
#include <realm/sync/config.hpp>
#include <realm/sync/socket_provider.hpp>
#include <realm/util/logger.hpp>

namespace realm {
struct SyncClientTimeouts {
    SyncClientTimeouts();
    // See sync::Client::Config for the meaning of these fields.
    uint64_t connect_timeout;
    uint64_t connection_linger_time;
    uint64_t ping_keepalive_period;
    uint64_t pong_keepalive_timeout;
    uint64_t fast_reconnect_limit;
    // Used for requesting location metadata at startup and reconnecting sync connections.
    // NOTE: delay_jitter_divisor is not configurable
    sync::ResumptionDelayInfo reconnect_backoff_info;
};

struct SyncClientConfig {
    using LoggerFactory = std::function<std::shared_ptr<util::Logger>(util::Logger::Level)>;
    LoggerFactory logger_factory;
    util::Logger::Level log_level = util::Logger::Level::info;
    ReconnectMode reconnect_mode = ReconnectMode::normal; // For internal sync-client testing only!
#if REALM_DISABLE_SYNC_MULTIPLEXING
    bool multiplex_sessions = false;
#else
    bool multiplex_sessions = true;
#endif

    // The SyncSocket instance used by the Sync Client for event synchronization
    // and creating WebSockets. If not provided the default implementation will be used.
    std::shared_ptr<sync::SyncSocketProvider> socket_provider;

    // Optional thread observer for event loop thread events in the default SyncSocketProvider
    // implementation. It is not used for custom SyncSocketProvider implementations.
    std::shared_ptr<BindingCallbackThreadObserver> default_socket_provider_thread_observer;

    // {@
    // Optional information about the binding/application that is sent as part of the User-Agent
    // when establishing a connection to the server. These values are only used by the default
    // SyncSocket implementation. Custom SyncSocket implementations must update the User-Agent
    // directly, if supported by the platform APIs.
    std::string user_agent_binding_info;
    std::string user_agent_application_info;
    // @}

    SyncClientTimeouts timeouts;
};

namespace app {
struct AppConfig {
    // Information about the device where the app is running
    struct DeviceInfo {
        std::string platform_version;  // json: platformVersion
        std::string sdk_version;       // json: sdkVersion
        std::string sdk;               // json: sdk
        std::string device_name;       // json: deviceName
        std::string device_version;    // json: deviceVersion
        std::string framework_name;    // json: frameworkName
        std::string framework_version; // json: frameworkVersion
        std::string bundle_id;         // json: bundleId
    };

    std::string app_id;
    std::shared_ptr<GenericNetworkTransport> transport;
    std::optional<std::string> base_url;
    std::optional<uint64_t> default_request_timeout_ms;
    DeviceInfo device_info;

    std::string base_file_path;
    SyncClientConfig sync_client_config;

    enum class MetadataMode {
        NoEncryption, // Enable metadata, but disable encryption.
        Encryption,   // Enable metadata, and use encryption (automatic if possible).
        InMemory,     // Do not persist metadata
    };
    MetadataMode metadata_mode = MetadataMode::Encryption;
    std::optional<std::vector<char>> custom_encryption_key;
    // If non-empty, mode is Encryption, and no key is explicitly set, the
    // automatically generated key is stored in the keychain using this access
    // group. Must be set when the metadata Realm is stored in an access group
    // and shared between apps. Not applicable on non-Apple platforms.
    std::string security_access_group;
};

} // namespace app
} // namespace realm

#endif // REALM_OS_SYNC_APP_CONFIG_HPP
