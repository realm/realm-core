#include <realm/object-store/c_api/types.hpp>
#include "realm.hpp"

namespace realm::c_api {

RLM_API bool realm_get_version_id(const realm_t* realm, bool* out_found, realm_version_id_t* out_version)
{
    return wrap_err([&]() {
        util::Optional<VersionID> version = (*realm)->current_transaction_version();
        if (version) {
            if (out_version) {
                *out_version = to_capi(version.value());
            }
            if (out_found) {
                *out_found = true;
            }
        }
        else {
            if (out_version) {
                *out_version = to_capi(VersionID(0, 0));
            }
            if (out_found) {
                *out_found = false;
            }
        }
        return true;
    });
}

RLM_API bool realm_get_num_versions(const realm_t* realm, uint64_t* out_versions_count)
{
    return wrap_err([&]() {
        if (out_versions_count) {
            *out_versions_count = (*realm)->get_number_of_versions();
        }
        return true;
    });
}

RLM_API const char* realm_get_library_version()
{
    return REALM_VERSION_STRING;
}

RLM_API void realm_get_library_version_numbers(int* out_major, int* out_minor, int* out_patch, const char** out_extra)
{
    *out_major = REALM_VERSION_MAJOR;
    *out_minor = REALM_VERSION_MINOR;
    *out_patch = REALM_VERSION_PATCH;
    *out_extra = REALM_VERSION_EXTRA;
}

RLM_API realm_t* realm_open(const realm_config_t* config)
{
    return wrap_err([&]() {
        return new shared_realm{Realm::get_shared_realm(*config)};
    });
}

RLM_API bool realm_delete_files(const char* realm_file_path, bool* did_delete_realm)
{
    return wrap_err([&]() {
        Realm::delete_files(realm_file_path, did_delete_realm);
        return true;
    });
}

RLM_API realm_t* _realm_from_native_ptr(const void* pshared_ptr, size_t n)
{
    REALM_ASSERT_RELEASE(n == sizeof(SharedRealm));
    auto ptr = static_cast<const SharedRealm*>(pshared_ptr);
    return new shared_realm{*ptr};
}

RLM_API void _realm_get_native_ptr(const realm_t* realm, void* pshared_ptr, size_t n)
{
    REALM_ASSERT_RELEASE(n == sizeof(SharedRealm));
    auto& shared_ptr = *static_cast<SharedRealm*>(pshared_ptr);
    shared_ptr = *realm;
}

RLM_API bool realm_is_closed(realm_t* realm)
{
    return (*realm)->is_closed();
}

RLM_API bool realm_is_writable(const realm_t* realm)
{
    return (*realm)->is_in_transaction();
}

RLM_API bool realm_close(realm_t* realm)
{
    return wrap_err([&]() {
        (*realm)->close();
        return true;
    });
}

RLM_API bool realm_begin_read(realm_t* realm)
{
    return wrap_err([&]() {
        (*realm)->read_group();
        return true;
    });
}

RLM_API bool realm_begin_write(realm_t* realm)
{
    return wrap_err([&]() {
        (*realm)->begin_transaction();
        return true;
    });
}

RLM_API bool realm_commit(realm_t* realm)
{
    return wrap_err([&]() {
        (*realm)->commit_transaction();
        return true;
    });
}

RLM_API bool realm_rollback(realm_t* realm)
{
    return wrap_err([&]() {
        (*realm)->cancel_transaction();
        return true;
    });
}

RLM_API uint64_t realm_add_realm_changed_callback(realm_t* realm, realm_on_realm_change_func_t callback,
                                                  void* userdata, realm_free_userdata_func_t free_userdata)
{
    util::UniqueFunction<void()> func = [callback, userdata = UserdataPtr{userdata, free_userdata}]() {
        callback(userdata.get());
    };
    return CBindingContext::get(*realm).realm_changed_callbacks().add(std::move(func));
}

RLM_API void realm_remove_realm_changed_callback(realm_t* realm, uint64_t token)
{
    CBindingContext::get(*realm).realm_changed_callbacks().remove(token);
}

RLM_API bool realm_refresh(realm_t* realm)
{
    return wrap_err([&]() {
        (*realm)->refresh();
        return true;
    });
}

RLM_API realm_t* realm_freeze(const realm_t* live_realm)
{
    return wrap_err([&]() {
        auto& p = **live_realm;
        return new realm_t{p.freeze()};
    });
}

RLM_API bool realm_compact(realm_t* realm, bool* did_compact)
{
    return wrap_err([&]() {
        auto& p = **realm;
        *did_compact = p.compact();
        return true;
    });
}

RLM_API realm_t* realm_from_thread_safe_reference(realm_thread_safe_reference_t* tsr, realm_scheduler_t* scheduler)
{
    return wrap_err([&]() {
        auto rtsr = dynamic_cast<shared_realm::thread_safe_reference*>(tsr);
        if (!rtsr) {
            throw std::logic_error{"Thread safe reference type mismatch"};
        }

        // FIXME: This moves out of the ThreadSafeReference, so it isn't
        // reusable.
        std::shared_ptr<util::Scheduler> sch;
        if (scheduler) {
            sch = *scheduler;
        }
        auto realm = Realm::get_shared_realm(static_cast<ThreadSafeReference&&>(*rtsr), sch);
        return new shared_realm{std::move(realm)};
    });
}

CBindingContext& CBindingContext::get(SharedRealm realm)
{
    if (!realm->m_binding_context) {
        realm->m_binding_context.reset(new CBindingContext());
    }

    CBindingContext* ctx = dynamic_cast<CBindingContext*>(realm->m_binding_context.get());
    REALM_ASSERT(ctx != nullptr);
    return *ctx;
}

} // namespace realm::c_api
