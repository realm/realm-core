////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
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

#ifndef REALM_OS_NETWORK_REACHABILITY_OBSERVER_HPP
#define REALM_OS_NETWORK_REACHABILITY_OBSERVER_HPP

#include <functional>
#include <string>

#include <realm/util/cf_ptr.hpp>

#include <SystemConfiguration/SystemConfiguration.h>

namespace realm {

enum NetworkReachabilityStatus {
    NotReachable,
    ReachableViaWiFi,
    ReachableViaWWAN
};

class NetworkReachabilityObserver {
public:
    // An instanse to check if the default route is available.
    NetworkReachabilityObserver();

    // An instanse to check if the specific host is available.
    NetworkReachabilityObserver(const std::string hostname);

    ~NetworkReachabilityObserver();

    NetworkReachabilityStatus reachability_status() const;

    bool set_reachability_change_handler(std::function<void (const NetworkReachabilityStatus)>);

private:
    bool start_observing();
    void stop_observing();
    static void reachability_callback(SCNetworkReachabilityRef, SCNetworkReachabilityFlags, void*);
    void reachability_changed();

    util::CFPtr<SCNetworkReachabilityRef> m_reachability_ref;
    std::function<void (const NetworkReachabilityStatus)> m_reachability_change_handler;
};

}

#endif // REALM_OS_NETWORK_REACHABILITY_OBSERVER_HPP
