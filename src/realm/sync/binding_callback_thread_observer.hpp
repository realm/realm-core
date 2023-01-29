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
#include <functional>
#include <memory>
#include <optional>
#include <thread>


namespace realm {
// Interface for bindings interested in registering callbacks before/after the ObjectStore thread runs.
// This is for example helpful to attach/detach the pthread to the JavaVM in order to be able to perform JNI calls.
struct BindingCallbackThreadObserver {
    using NotificationCallback = std::function<void()>;
    using ErrorCallback = std::function<void(const std::exception&)>;

    // Create a BindingCallbackThreadObserver that can be used in SyncClientConfig
    BindingCallbackThreadObserver(std::optional<NotificationCallback>&& did_create_thread,
                                  std::optional<NotificationCallback>&& will_destroy_thread,
                                  std::optional<ErrorCallback>&& error_handler)
        : m_create_thread_callback{std::move(did_create_thread)}
        , m_destroy_thread_callback{std::move(will_destroy_thread)}
        , m_handle_error_callback{std::move(error_handler)}
    {
    }

    virtual ~BindingCallbackThreadObserver() = default;

    // Set the global thread observer with the provided (optional) callback functions
    static void set_global_thread_observer(std::unique_ptr<BindingCallbackThreadObserver>&& observer_ptr)
    {
        if (!observer_ptr) {
            BindingCallbackThreadObserver::reset_global_thread_observer();
            return; // early return
        }

        std::lock_guard<std::mutex> lock{BindingCallbackThreadObserver::m_mutex};
        BindingCallbackThreadObserver::m_instance.reset(observer_ptr.release());
    }

    // Returns true if the global binding callback thread observer is set, otherwise false
    static bool has_global_thread_observer()
    {
        return bool(BindingCallbackThreadObserver::m_instance);
    }

    // Resets the global thread observer so no more callback functions will be called
    static void reset_global_thread_observer()
    {
        std::lock_guard<std::mutex> lock{BindingCallbackThreadObserver::m_mutex};
        BindingCallbackThreadObserver::m_instance.reset();
    }

    ///
    /// Execution Functions - check for a valid instance and if the function was set
    ///

    // BindingCallbackThreadObserver class that will call will_destroy_thread() when destroyed
    struct ThreadGuard {
        ~ThreadGuard()
        {
            BindingCallbackThreadObserver::call_will_destroy_thread(m_observer);
        }
        // Constructor that only works with the global thread observer
        ThreadGuard() = default;

        // Constructor that works with either the local or global thread observer
        ThreadGuard(const std::shared_ptr<BindingCallbackThreadObserver>& observer_ptr)
            : m_observer{observer_ptr}
        {
        }

    private:
        std::shared_ptr<BindingCallbackThreadObserver> m_observer;
    };

    // This method is called just before the thread is started
    // This takes an optional reference to an observer_ptr and decides whether to use the passed in or global observer
    static void call_did_create_thread(const std::shared_ptr<BindingCallbackThreadObserver>& observer_ptr = nullptr);

    // This method is called just before the thread is being destroyed
    // This takes an optional reference to an observer_ptr and decides whether to use the passed in or global observer
    static void
    call_will_destroy_thread(const std::shared_ptr<BindingCallbackThreadObserver>& observer_ptr = nullptr);

    // This method is called with any exception thrown by client.run().
    // This takes an optional reference to an observer_ptr and decides whether to use the passed in or global observer
    // Returns true if the exception was passed along otherwise false.
    static bool call_handle_error(const std::exception& e,
                                  const std::shared_ptr<BindingCallbackThreadObserver>& observer_ptr = nullptr);

protected:
    // Default constructor
    BindingCallbackThreadObserver() = default;

    // Call the stored create thread callback function with the id of this thread
    // Can be overridden to provide a custom implementation
    virtual void did_create_thread();

    // Call the stored destroy thread callback function with the id of this thread
    // Can be overridden to provide a custom implementation
    virtual void will_destroy_thread();

    // Call the stored handle error callback function with the id of this thread
    // Can be overridden to provide a custom implementation
    // Return true if the exception was handled by this function, otherwise false
    virtual bool handle_error(const std::exception& e);

    // Global instances of the BindingCallbackThreadObserver
    static std::unique_ptr<BindingCallbackThreadObserver> m_instance;
    static std::mutex m_mutex;

    std::optional<NotificationCallback> m_create_thread_callback;
    std::optional<NotificationCallback> m_destroy_thread_callback;
    std::optional<ErrorCallback> m_handle_error_callback;
};

} // namespace realm

#endif // REALM_OS_BINDING_CALLBACK_THREAD_OBSERVER_HPP
