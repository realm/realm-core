#include <realm/object-store/c_api/util.hpp>
#include <realm/object-store/c_api/types.hpp>

namespace realm::c_api {

template <class T>
inline T* cast_ptr(void* ptr)
{
    auto rptr = static_cast<WrapC*>(ptr);
    REALM_ASSERT(dynamic_cast<T*>(rptr) != nullptr);
    return static_cast<T*>(rptr);
}

template <class T>
inline const T* cast_ptr(const void* ptr)
{
    auto rptr = static_cast<const WrapC*>(ptr);
    REALM_ASSERT(dynamic_cast<const T*>(rptr) != nullptr);
    return static_cast<const T*>(rptr);
}

RLM_API void realm_free(void* buffer)
{
    if (!buffer)
        return;
    free(buffer);
}

RLM_API void realm_release(void* ptr)
{
    if (!ptr)
        return;
    delete cast_ptr<WrapC>(ptr);
}

RLM_API void* realm_clone(const void* ptr)
{
    return wrap_err([=]() {
        return cast_ptr<WrapC>(ptr)->clone();
    });
}

RLM_API bool realm_is_frozen(const void* ptr)
{
    return cast_ptr<WrapC>(ptr)->is_frozen();
}

RLM_API bool realm_equals(const void* a, const void* b)
{
    if (a == b)
        return true;
    if (!a || !b)
        return false;

    auto lhs = static_cast<const WrapC*>(a);
    auto rhs = static_cast<const WrapC*>(b);

    return lhs->equals(*rhs);
}

RLM_API realm_thread_safe_reference_t* realm_create_thread_safe_reference(const void* ptr)
{
    return wrap_err([=]() {
        auto cptr = static_cast<const WrapC*>(ptr);
        return cptr->get_thread_safe_reference();
    });
}

} // namespace realm::c_api
