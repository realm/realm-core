/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <realm/util/interprocess_mutex.hpp>

using namespace realm::util;

#if REALM_ROBUST_MUTEX_EMULATION

std::once_flag InterprocessMutex::s_init_flag;
std::map<File::UniqueID, std::weak_ptr<InterprocessMutex::LockInfo>>* InterprocessMutex::s_info_map;
Mutex* InterprocessMutex::s_mutex;

#endif // REALM_ROBUST_MUTEX_EMULATION

#if REALM_PLATFORM_APPLE
SemaphoreMutex::SemaphoreMutex() noexcept
    : m_semaphore(dispatch_semaphore_create(1))
{
}

SemaphoreMutex::~SemaphoreMutex() noexcept
{
    dispatch_release(m_semaphore);
}

void SemaphoreMutex::lock() noexcept
{
    dispatch_semaphore_wait(m_semaphore, DISPATCH_TIME_FOREVER);
}

bool SemaphoreMutex::try_lock() noexcept
{
    return dispatch_semaphore_wait(m_semaphore, DISPATCH_TIME_NOW) == 0;
}

void SemaphoreMutex::unlock() noexcept
{
    dispatch_semaphore_signal(m_semaphore);
}
#endif // REALM_PLATFORM_APPLE
