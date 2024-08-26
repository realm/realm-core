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

#ifndef REALM_OS_BINDING_CALLBACK_THREAD_OBSERVER_HPP
#define REALM_OS_BINDING_CALLBACK_THREAD_OBSERVER_HPP

#include <exception>

namespace realm {
// Interface for observing the lifecycle of the worker thread used by
// DefaultSocketProvider. This is required to be able to attach/detach the thread
// to the JVM to be able to perform JNI calls.
struct BindingCallbackThreadObserver {
    virtual ~BindingCallbackThreadObserver() = default;

    // Called on the thread shortly after it is created. This is guaranteed to
    // be called before any other callbacks to the SDK are made.
    virtual void did_create_thread() {}
    // Called on the thread shortly before it is destroyed. No further callbacks
    // to the SDK on the thread will be made after this is called.
    virtual void will_destroy_thread() {}
    // If has_handle_error() returns true, any uncaught exceptions from the
    // event loop are passed to this. If this returns true, the thread exits
    // cleanly, while if it returns false the exception is rethrown.
    virtual bool handle_error(const std::exception&)
    {
        return false;
    }
    virtual bool has_handle_error()
    {
        return false;
    }
};

} // namespace realm

#endif // REALM_OS_BINDING_CALLBACK_THREAD_OBSERVER_HPP
