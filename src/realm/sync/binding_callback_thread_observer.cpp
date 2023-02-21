////////////////////////////////////////////////////////////////////////////
//
// Copyright 2017 Realm Inc.
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

#include <realm/sync/binding_callback_thread_observer.hpp>

namespace realm {

void BindingCallbackThreadObserver::call_did_create_thread(
    const std::shared_ptr<BindingCallbackThreadObserver>& observer_ptr)
{
    // Call into the observer ptr if not null, otherwise, use the global thread observer
    if (observer_ptr)
        observer_ptr->did_create_thread();
}

void BindingCallbackThreadObserver::call_will_destroy_thread(
    const std::shared_ptr<BindingCallbackThreadObserver>& observer_ptr)
{
    // Call into the observer ptr if not null, otherwise, use the global thread observer
    if (observer_ptr)
        observer_ptr->will_destroy_thread();
}

bool BindingCallbackThreadObserver::call_handle_error(
    const std::exception& e, const std::shared_ptr<BindingCallbackThreadObserver>& observer_ptr)
{
    // Call into the observer ptr if not null, otherwise, use the global thread observer
    if (observer_ptr)
        return observer_ptr->handle_error(e);
    return false;
}

void BindingCallbackThreadObserver::did_create_thread()
{
    if (m_create_thread_callback) {
        (*m_create_thread_callback)();
    }
}

void BindingCallbackThreadObserver::will_destroy_thread()
{
    if (m_destroy_thread_callback) {
        (*m_destroy_thread_callback)();
    }
}

bool BindingCallbackThreadObserver::handle_error(const std::exception& e)
{
    if (!m_handle_error_callback)
        return false;

    return (*m_handle_error_callback)(e);
}
} // namespace realm
