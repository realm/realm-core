#include <realm/object-store/c_api/util.hpp>
#include <realm/object-store/c_api/types.hpp>

RLM_API void realm_release(const void* ptr)
{
    if (!ptr)
        return;
    delete cast_ptr<WrapC>(ptr);
}

RLM_API void* realm_clone(const void* ptr)
{
    return cast_ptr<WrapC>(ptr)->clone();
}

RLM_API bool realm_is_frozen(const void* ptr)
{
    return cast_ptr<WrapC>(ptr)->is_frozen();
}

RLM_API bool realm_equals(const void* a, const void* b)
{
    if (a == b)
        return true;
    if (bool(a) != bool(b))
        return false;

    auto lhs = static_cast<const WrapC*>(a);
    auto rhs = static_cast<const WrapC*>(b);

    return lhs->equals(*rhs);
}