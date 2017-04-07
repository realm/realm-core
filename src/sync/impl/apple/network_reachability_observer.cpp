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

#include "sync/impl/apple/network_reachability_observer.hpp"

#if NETWORK_REACHABILITY_AVAILABLE

using namespace realm;

namespace {

static NetworkReachabilityStatus reachability_status_for_flags(SCNetworkReachabilityFlags flags)
{
    if (!(flags & kSCNetworkReachabilityFlagsReachable))
        return NotReachable;

    if (flags & kSCNetworkReachabilityFlagsConnectionRequired) {
        if (!(flags & kSCNetworkReachabilityFlagsConnectionOnTraffic) ||
            (flags & kSCNetworkReachabilityFlagsInterventionRequired))
            return NotReachable;
    }

    NetworkReachabilityStatus status = ReachableViaWiFi;

#if TARGET_OS_IPHONE
    if (flags & kSCNetworkReachabilityFlagsIsWWAN) {
        status = ReachableViaWWAN;
    }
#endif

    return status;
}

static void reachability_callback(SCNetworkReachabilityRef, SCNetworkReachabilityFlags, void* info)
{
    auto callback = reinterpret_cast<std::function<void ()> *>(info);
    (*callback)();
}

} // (anonymous namespace)

NetworkReachabilityObserver::NetworkReachabilityObserver(util::Optional<std::string> hostname,
                                                         std::function<void (const NetworkReachabilityStatus)> handler)
: m_callback_queue(dispatch_queue_create("io.realm.sync.reachability", DISPATCH_QUEUE_SERIAL))
, m_reachability_callback([=]() { reachability_changed(); })
, m_change_handler(std::move(handler))
{
    if (hostname) {
        m_reachability_ref = util::adoptCF(SCNetworkReachabilityCreateWithName(nullptr, hostname->c_str()));
    } else {
        struct sockaddr zeroAddress = {};
        zeroAddress.sa_len = sizeof(zeroAddress);
        zeroAddress.sa_family = AF_INET;

        m_reachability_ref = util::adoptCF(SCNetworkReachabilityCreateWithAddress(nullptr, &zeroAddress));
    }
}

NetworkReachabilityObserver::~NetworkReachabilityObserver()
{
    stop_observing();
    dispatch_release(m_callback_queue);
}

NetworkReachabilityStatus NetworkReachabilityObserver::reachability_status() const
{
    SCNetworkReachabilityFlags flags;

    if (SCNetworkReachabilityGetFlags(m_reachability_ref.get(), &flags)) {
        return reachability_status_for_flags(flags);
    }

    return NotReachable;
}

bool NetworkReachabilityObserver::start_observing()
{
    m_previous_status = reachability_status();

    SCNetworkReachabilityContext context = {0, &m_reachability_callback, nullptr, nullptr, nullptr};

    if (!SCNetworkReachabilitySetCallback(m_reachability_ref.get(), reachability_callback, &context)) {
        return false;
    }

    if (!SCNetworkReachabilitySetDispatchQueue(m_reachability_ref.get(), m_callback_queue)) {
        return false;
    }

    return true;
}

void NetworkReachabilityObserver::stop_observing()
{
    SCNetworkReachabilitySetDispatchQueue(m_reachability_ref.get(), nullptr);
    SCNetworkReachabilitySetCallback(m_reachability_ref.get(), nullptr, nullptr);
    
    // Wait for all previously-enqueued blocks to execute to guarantee that
    // no callback will be called after returning from this method
    dispatch_sync(m_callback_queue, ^{});
}

void NetworkReachabilityObserver::reachability_changed()
{
    auto current_status = reachability_status();

    // When observing reachability of the specific host the callback might be called
    // several times (because of DNS queries) with the same reachability flags while
    // the caller should be notified only when the reachability status is really changed.
    if (current_status != m_previous_status) {
        m_change_handler(current_status);
        m_previous_status = current_status;
    }
}

#endif
