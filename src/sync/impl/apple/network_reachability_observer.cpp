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

using namespace realm;

namespace {

NetworkReachabilityStatus reachability_status_for_flags(SCNetworkReachabilityFlags flags)
{
    // This function uses the same method to detect connection type as Apple's Reachability sample:
    // https://developer.apple.com/library/content/samplecode/Reachability
    if (!(flags & kSCNetworkReachabilityFlagsReachable)) {
        return NotReachable;
    }

    NetworkReachabilityStatus status = NotReachable;

    if (!(flags & kSCNetworkReachabilityFlagsConnectionRequired)) {
        status = ReachableViaWiFi;
    }

    if ((flags & kSCNetworkReachabilityFlagsConnectionOnDemand) ||
        (flags & kSCNetworkReachabilityFlagsConnectionOnTraffic)) {
        if (!(flags & kSCNetworkReachabilityFlagsInterventionRequired)) {
            status = ReachableViaWiFi;
        }
    }

#if TARGET_OS_IPHONE
    if (flags & kSCNetworkReachabilityFlagsIsWWAN) {
        status = ReachableViaWWAN;
    }
#endif

    return status;
}
    
} // (anonymous namespace)

NetworkReachabilityObserver::NetworkReachabilityObserver() {
    struct sockaddr zeroAddress = {};
    zeroAddress.sa_len = sizeof(zeroAddress);
    zeroAddress.sa_family = AF_INET;

    m_reachability_ref = util::adoptCF(SCNetworkReachabilityCreateWithAddress(nullptr, &zeroAddress));
}

NetworkReachabilityObserver::NetworkReachabilityObserver(const std::string hostname)
{
    m_reachability_ref = util::adoptCF(SCNetworkReachabilityCreateWithName(nullptr, hostname.c_str()));
}

NetworkReachabilityObserver::~NetworkReachabilityObserver()
{
    stop_observing();
}

NetworkReachabilityStatus NetworkReachabilityObserver::reachability_status() const
{
    SCNetworkReachabilityFlags flags;

    if (SCNetworkReachabilityGetFlags(m_reachability_ref.get(), &flags)) {
        return reachability_status_for_flags(flags);
    }

    return NotReachable;
}

bool NetworkReachabilityObserver::set_reachability_change_handler(std::function<void (const NetworkReachabilityStatus)> handler)
{
    stop_observing();

    m_reachability_change_handler = std::move(handler);

    return m_reachability_change_handler ? start_observing() : true;
}

bool NetworkReachabilityObserver::start_observing()
{
    SCNetworkReachabilityContext context = {0, this, nullptr, nullptr, nullptr};

    if (!SCNetworkReachabilitySetCallback(m_reachability_ref.get(), reachability_callback, &context)) {
        return false;
    }

    if (!SCNetworkReachabilityScheduleWithRunLoop(m_reachability_ref.get(), CFRunLoopGetCurrent(), kCFRunLoopDefaultMode)) {
        return false;
    }

    return true;
}

void NetworkReachabilityObserver::stop_observing()
{
    SCNetworkReachabilityUnscheduleFromRunLoop(m_reachability_ref.get(), CFRunLoopGetCurrent(), kCFRunLoopDefaultMode);
    SCNetworkReachabilitySetCallback(m_reachability_ref.get(), nullptr, nullptr);
}

void NetworkReachabilityObserver::reachability_callback(SCNetworkReachabilityRef, SCNetworkReachabilityFlags, void* info)
{
    auto helper = reinterpret_cast<realm::NetworkReachabilityObserver *>(info);
    helper->reachability_changed();
}

void NetworkReachabilityObserver::reachability_changed()
{
    m_reachability_change_handler(reachability_status());
}
