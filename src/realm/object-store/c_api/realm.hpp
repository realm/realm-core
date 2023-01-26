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

#if REALM_ENABLE_SYNC
#include <realm/sync/binding_callback_thread_observer.hpp>
#endif // REALM_ENABLE_SYNC

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

#if REALM_ENABLE_SYNC

class CBindingThreadObserver : public realm::BindingCallbackThreadObserver {
public:
    static void set(realm_on_object_store_thread_callback_t on_thread_create,
                    realm_on_object_store_thread_callback_t on_thread_destroy,
                    realm_on_object_store_error_callback_t on_error, realm_userdata_t userdata,
                    realm_free_userdata_func_t free_userdata)
    {
        auto& observer = CBindingThreadObserver::get_instance();
        observer.m_create_callback = on_thread_create;
        observer.m_destroy_callback = on_thread_destroy;
        observer.m_error_callback = on_error;
        observer.m_user_data = UserdataPtr(userdata, free_userdata);
        g_binding_callback_thread_observer = &observer;
    }

    static void reset()
    {
        g_binding_callback_thread_observer = nullptr;
        auto& observer = CBindingThreadObserver::get_instance();
        observer.m_create_callback = nullptr;
        observer.m_destroy_callback = nullptr;
        observer.m_error_callback = nullptr;
        observer.m_user_data.reset();
    }

    void did_create_thread() override
    {
        if (m_create_callback)
            m_create_callback(m_user_data.get());
    }

    void will_destroy_thread() override
    {
        if (m_destroy_callback)
            m_destroy_callback(m_user_data.get());
    }

    void handle_error(std::exception const& e) override
    {
        if (m_error_callback)
            m_error_callback(m_user_data.get(), e.what());
    }

private:
    static CBindingThreadObserver& get_instance()
    {
        static CBindingThreadObserver instance;
        return instance;
    }

    CBindingThreadObserver() = default;
    realm_on_object_store_thread_callback_t m_create_callback = nullptr;
    realm_on_object_store_thread_callback_t m_destroy_callback = nullptr;
    realm_on_object_store_error_callback_t m_error_callback = nullptr;
    UserdataPtr m_user_data;
};

#endif // REALM_ENABLE_SYNC

} // namespace realm::c_api
