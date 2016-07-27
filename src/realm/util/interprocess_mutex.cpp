/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2016] Realm Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Realm Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Realm Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Realm Incorporated.
 *
 **************************************************************************/

#include <realm/util/interprocess_mutex.hpp>

#ifdef REALM_ROBUST_MUTEX_EMULATION

using namespace realm::util;

std::map<std::string, std::weak_ptr<Mutex>> InterprocessMutex::s_mutex_map;
Mutex InterprocessMutex::s_mutex;

#endif

