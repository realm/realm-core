#include <realm/object-store/c_api/util.hpp>
#include <realm/object-store/util/scheduler.hpp>

#if defined(REALM_USE_UV) && REALM_USE_UV
#define REALM_HAS_DEFAULT_SCHEDULER 1
#elif defined(REALM_USE_CF) && REALM_USE_CF
#define REALM_HAS_DEFAULT_SCHEDULER 1
#elif defined(REALM_USE_ALOOPER) && REALM_USE_ALOOPER
#define REALM_HAS_DEFAULT_SCHEDULER 1
#else
#define REALM_HAS_DEFAULT_SCHEDULER 0
#endif

using namespace realm::util;

// LCOV_EXCL_START

namespace realm::c_api {
namespace {

// A callback coming from C++ code registered in a scheduler created by
// `realm_scheduler_new()`.
struct NotifyCppCallback {
    std::function<void()> m_callback;
};

void free_notify_cpp_callback(void* ptr)
{
    delete static_cast<NotifyCppCallback*>(ptr);
}

void invoke_notify_cpp_callback(void* ptr)
{
    static_cast<NotifyCppCallback*>(ptr)->m_callback();
}

// A callback coming from C code registered in a scheduler defined in C++ code.
struct NotifyCAPICallback {
    struct Inner {
        void* m_userdata = nullptr;
        realm_free_userdata_func_t m_free = nullptr;
        realm_scheduler_notify_func_t m_notify = nullptr;

        ~Inner()
        {
            if (m_free)
                m_free(m_userdata);
        }
    };

    // Indirection because we are wrapping ourselves in an `std::function`,
    // which must be copyable.
    std::shared_ptr<Inner> m_inner;

    NotifyCAPICallback(void* userdata, realm_free_userdata_func_t free_func, realm_scheduler_notify_func_t notify)
        : m_inner(std::make_shared<Inner>())
    {
        m_inner->m_userdata = userdata;
        m_inner->m_free = free_func;
        m_inner->m_notify = notify;
    }

    void operator()()
    {
        if (m_inner->m_notify)
            m_inner->m_notify(m_inner->m_userdata);
    }
};

struct CAPIScheduler : Scheduler {
    void* m_userdata = nullptr;
    realm_free_userdata_func_t m_free = nullptr;
    realm_scheduler_notify_func_t m_notify = nullptr;
    realm_scheduler_is_on_thread_func_t m_is_on_thread = nullptr;
    realm_scheduler_is_same_as_func_t m_is_same_as = nullptr;
    realm_scheduler_can_deliver_notifications_func_t m_can_deliver_notifications = nullptr;
    realm_scheduler_set_notify_callback_func_t m_set_notify_callback = nullptr;


    CAPIScheduler() = default;
    CAPIScheduler(CAPIScheduler&& other)
        : m_userdata(std::exchange(other.m_userdata, nullptr))
        , m_free(std::exchange(other.m_free, nullptr))
        , m_notify(std::exchange(other.m_notify, nullptr))
        , m_is_on_thread(std::exchange(other.m_is_on_thread, nullptr))
        , m_can_deliver_notifications(std::exchange(other.m_can_deliver_notifications, nullptr))
        , m_set_notify_callback(std::exchange(other.m_set_notify_callback, nullptr))
    {
    }

    ~CAPIScheduler()
    {
        if (m_free)
            m_free(m_userdata);
    }

    void notify() final
    {
        if (m_notify)
            m_notify(m_userdata);
    }

    bool is_on_thread() const noexcept final
    {
        if (m_is_on_thread)
            return m_is_on_thread(m_userdata);
        return false;
    }

    bool is_same_as(const Scheduler* other) const noexcept final
    {
        if (auto rhs = dynamic_cast<const CAPIScheduler*>(other)) {
            bool same_callbacks = m_free == rhs->m_free && m_notify == rhs->m_notify &&
                                  m_is_same_as == rhs->m_is_same_as && m_is_on_thread == rhs->m_is_on_thread &&
                                  m_can_deliver_notifications == rhs->m_can_deliver_notifications &&
                                  m_set_notify_callback == rhs->m_set_notify_callback;
            if (same_callbacks && m_userdata == rhs->m_userdata) {
                return true;
            }
            if (same_callbacks && m_is_same_as) {
                return m_is_same_as(m_userdata, rhs->m_userdata);
            }
        }
        return false;
    }

    bool can_deliver_notifications() const noexcept final
    {
        if (m_can_deliver_notifications)
            return m_can_deliver_notifications(m_userdata);
        return false;
    }

    void set_notify_callback(std::function<void()> callback) final
    {
        if (m_set_notify_callback) {
            auto ptr = new NotifyCppCallback{std::move(callback)};
            m_set_notify_callback(m_userdata, ptr, free_notify_cpp_callback, invoke_notify_cpp_callback);
        }
    }
};

struct DefaultFactory {
    struct Inner {
        void* m_userdata = nullptr;
        realm_free_userdata_func_t m_free_func = nullptr;
        realm_scheduler_default_factory_func_t m_factory_func = nullptr;

        ~Inner()
        {
            if (m_free_func)
                m_free_func(m_userdata);
        }
    };

    // Indirection because we are wrapping ourselves in an `std::function`,
    // which must be copyable.
    std::shared_ptr<Inner> m_inner;

    DefaultFactory(void* userdata, realm_free_userdata_func_t free_func,
                   realm_scheduler_default_factory_func_t factory_func)
        : m_inner(std::make_shared<Inner>())
    {
        m_inner->m_userdata = userdata;
        m_inner->m_free_func = free_func;
        m_inner->m_factory_func = factory_func;
    }

    std::shared_ptr<Scheduler> operator()()
    {
        if (m_inner->m_factory_func) {
            auto ptr = m_inner->m_factory_func(m_inner->m_userdata);
            std::shared_ptr<Scheduler> scheduler = *ptr;
            realm_release(ptr);
            return scheduler;
        }
        return nullptr;
    }
};

} // namespace

RLM_API realm_scheduler_t*
realm_scheduler_new(void* userdata, realm_free_userdata_func_t free_func, realm_scheduler_notify_func_t notify_func,
                    realm_scheduler_is_on_thread_func_t is_on_thread_func,
                    realm_scheduler_is_same_as_func_t is_same_as,
                    realm_scheduler_can_deliver_notifications_func_t can_deliver_notifications_func,
                    realm_scheduler_set_notify_callback_func_t set_notify_callback_func)
{
    return wrap_err([&]() {
        auto capi_scheduler = std::make_shared<CAPIScheduler>();
        capi_scheduler->m_userdata = userdata;
        capi_scheduler->m_free = free_func;
        capi_scheduler->m_notify = notify_func;
        capi_scheduler->m_is_on_thread = is_on_thread_func;
        capi_scheduler->m_is_same_as = is_same_as;
        capi_scheduler->m_can_deliver_notifications = can_deliver_notifications_func;
        capi_scheduler->m_set_notify_callback = set_notify_callback_func;
        return new realm_scheduler_t(std::move(capi_scheduler));
    });
}

RLM_API realm_scheduler_t* realm_scheduler_make_default()
{
    return wrap_err([&]() {
        return new realm_scheduler_t{Scheduler::make_default()};
    });
}

RLM_API const realm_scheduler_t* realm_scheduler_get_frozen()
{
    return wrap_err([&]() {
        // FIXME: Provide a `realm_version_id_t`.
        return static_cast<realm_scheduler_t*>(nullptr);
        // static const realm_scheduler_t* frozen = new realm_scheduler_t{Scheduler::get_frozen()};
        // return frozen;
    });
}

// FIXME: Move this into `GenericScheduler` (i.e. make `Scheduler::set_default_factory()` thread-safe).
static std::mutex s_default_factory_mutex;
static bool s_default_factory_set = false;

RLM_API bool realm_scheduler_has_default_factory()
{
#if REALM_HAS_DEFAULT_SCHEDULER
    return true;
#else
    return s_default_factory_set;
#endif
}

RLM_API bool realm_scheduler_set_default_factory(void* userdata, realm_free_userdata_func_t free_func,
                                                 realm_scheduler_default_factory_func_t factory_func)
{
    return wrap_err([&]() {
#if REALM_HAS_DEFAULT_SCHEDULER
        static_cast<void>(userdata);
        static_cast<void>(free_func);
        static_cast<void>(factory_func);
        static_cast<void>(s_default_factory_mutex);
        static_cast<void>(s_default_factory_set);
        throw std::logic_error{"This platform already has a default scheduler implementation"};
        return true;
#else
        std::unique_lock<std::mutex> lock{s_default_factory_mutex};
        if (s_default_factory_set) {
            throw std::logic_error{"A default scheduler factory has already been registered"};
        }
        DefaultFactory factory{userdata, free_func, factory_func};
        Scheduler::set_default_factory(std::move(factory));
        s_default_factory_set = true;
        return true;
#endif
    });
}

RLM_API void realm_scheduler_notify(realm_scheduler_t* scheduler)
{
    (*scheduler)->notify();
}

RLM_API bool realm_scheduler_is_on_thread(const realm_scheduler_t* scheduler)
{
    return (*scheduler)->is_on_thread();
}

RLM_API bool realm_scheduler_can_deliver_notifications(const realm_scheduler_t* scheduler)
{
    return (*scheduler)->can_deliver_notifications();
}

RLM_API bool realm_scheduler_set_notify_callback(realm_scheduler_t* scheduler, void* userdata,
                                                 realm_free_userdata_func_t free_func,
                                                 realm_scheduler_notify_func_t notify_func)
{
    return wrap_err([&]() {
        auto capi_scheduler = dynamic_cast<CAPIScheduler*>(scheduler->get());
        if (capi_scheduler) {
            // Avoid needless roundtrips through the std::function wrappers.
            if (capi_scheduler->m_set_notify_callback) {
                capi_scheduler->m_set_notify_callback(capi_scheduler->m_userdata, userdata, free_func, notify_func);
            }
        }
        else {
            NotifyCAPICallback callback{userdata, free_func, notify_func};
            (*scheduler)->set_notify_callback(std::move(callback));
        }
        return true;
    });
}

} // namespace realm::c_api

// LCOV_EXCL_STOP
