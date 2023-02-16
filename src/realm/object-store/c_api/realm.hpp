/*************************************************************************
 *
 * Copyright 2021 Realm Inc.
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

#pragma once

#include <realm/object-store/c_api/util.hpp>
#include <realm/object-store/binding_context.hpp>
#include <realm/object-store/binding_callback_thread_observer.hpp>

namespace realm::c_api {

class CBindingContext : public BindingContext {
public:
    static CBindingContext& get(SharedRealm realm);

    CBindingContext() = default;
    CBindingContext(SharedRealm realm)
        : BindingContext()
    {
        this->realm = realm;
    }

    CallbackRegistry<>& realm_changed_callbacks()
    {
        return m_realm_changed_callbacks;
    }

    CallbackRegistryWithVersion<>& realm_pending_refresh_callbacks()
    {
        return m_realm_pending_refresh_callbacks;
    }

    CallbackRegistry<const Schema&>& schema_changed_callbacks()
    {
        return m_schema_changed_callbacks;
    }

protected:
    void did_change(std::vector<ObserverState> const&, std::vector<void*> const&, bool) final;

    void schema_did_change(const Schema& schema) final
    {
        m_schema_changed_callbacks.invoke(schema);
    }

private:
    CallbackRegistry<> m_realm_changed_callbacks;
    CallbackRegistryWithVersion<> m_realm_pending_refresh_callbacks;
    CallbackRegistry<const Schema&> m_schema_changed_callbacks;
};

class CBindingThreadObserver : public realm::BindingCallbackThreadObserver {
public:
    using ThreadCallback = util::UniqueFunction<void()>;
    using ErrorCallback = util::UniqueFunction<void(const char*)>;

    static CBindingThreadObserver& create()
    {
        static CBindingThreadObserver instance;
        return instance;
    }

    void set(ThreadCallback&& on_create, ThreadCallback&& on_destroy, ErrorCallback&& on_error)
    {
        m_create_callback = std::move(on_create);
        m_destroy_callback = std::move(on_destroy);
        m_error_callback = std::move(on_error);
    }

    void did_create_thread() override
    {
        if (m_create_callback)
            m_create_callback();
    }

    void will_destroy_thread() override
    {
        if (m_destroy_callback)
            m_destroy_callback();
    }

    void handle_error(std::exception const& e) override
    {
        if (m_error_callback)
            m_error_callback(e.what());
    }

private:
    CBindingThreadObserver() = default;
    ThreadCallback m_create_callback;
    ThreadCallback m_destroy_callback;
    ErrorCallback m_error_callback;
};

} // namespace realm::c_api
