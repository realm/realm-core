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

struct CBindingThreadObserver : public realm::BindingCallbackThreadObserver {
public:
    CBindingThreadObserver(realm_on_object_store_thread_callback_t on_thread_create,
                           realm_on_object_store_thread_callback_t on_thread_destroy,
                           realm_on_object_store_error_callback_t on_error, realm_userdata_t userdata,
                           realm_free_userdata_func_t free_userdata)
        : m_create_callback_func{on_thread_create}
        , m_destroy_callback_func{on_thread_destroy}
        , m_error_callback_func{on_error}
        , m_user_data{UserdataPtr(userdata, free_userdata)}
    {
    }

    virtual ~CBindingThreadObserver() = default;

    // For testing: compare two CBindingThreadObserver instances to see if they have
    // the same callback functions and userdata ptr values.
    // This needed to be added since there are no implicit or default equality operators (until C++20)
    bool operator==(const CBindingThreadObserver& other) const
    {
        return m_create_callback_func == other.m_create_callback_func &&
               m_destroy_callback_func == other.m_destroy_callback_func &&
               m_error_callback_func == other.m_error_callback_func && m_user_data == other.m_user_data;
    }

protected:
    CBindingThreadObserver() = default;

    void did_create_thread() override
    {
        if (m_create_callback_func)
            m_create_callback_func(m_user_data.get());
    }

    void will_destroy_thread() override
    {
        if (m_destroy_callback_func)
            m_destroy_callback_func(m_user_data.get());
    }

    bool handle_error(std::exception const& e) override
    {
        if (!m_error_callback_func)
            return false;

        m_error_callback_func(m_user_data.get(), e.what());
        return true;
    }

    realm_on_object_store_thread_callback_t m_create_callback_func = nullptr;
    realm_on_object_store_thread_callback_t m_destroy_callback_func = nullptr;
    realm_on_object_store_error_callback_t m_error_callback_func = nullptr;
    UserdataPtr m_user_data;
};

#endif // REALM_ENABLE_SYNC

} // namespace realm::c_api
