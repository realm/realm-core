#include <realm/object-store/c_api/error.hpp>
#include <realm/object-store/c_api/util.hpp>
#include <realm/parser/query_parser.hpp>

#if REALM_PLATFORM_APPLE && !defined(RLM_NO_THREAD_LOCAL)
#define RLM_NO_THREAD_LOCAL
#endif

#if defined(RLM_NO_THREAD_LOCAL)
#include <pthread.h>
#endif

namespace realm::c_api {

ErrorStorage::ErrorStorage(std::exception_ptr ptr) noexcept
    : m_err(none)
    , m_message_buf()
{
    assign(std::move(ptr));
}

ErrorStorage::ErrorStorage(const ErrorStorage& other)
    : m_err(other.m_err)
    , m_message_buf(other.m_message_buf)
{
    if (m_err) {
        m_err->message = m_message_buf.c_str();
    }
}

ErrorStorage& ErrorStorage::operator=(const ErrorStorage& other)
{
    m_err = other.m_err;
    m_message_buf = other.m_message_buf;
    if (m_err) {
        m_err->message = m_message_buf.c_str();
    }
    return *this;
}

ErrorStorage::ErrorStorage(ErrorStorage&& other)
    : m_err(std::move(other.m_err))
    , m_message_buf(std::move(other.m_message_buf))
{
    if (m_err) {
        m_err->message = m_message_buf.c_str();
    }
    other.m_err.reset();
}

ErrorStorage& ErrorStorage::operator=(ErrorStorage&& other)
{
    m_err = std::move(other.m_err);
    m_message_buf = std::move(other.m_message_buf);
    if (m_err) {
        m_err->message = m_message_buf.c_str();
    }
    other.m_err.reset();
    return *this;
}

bool ErrorStorage::operator==(const ErrorStorage& other) const noexcept
{
    if (bool(m_err) != bool(other.m_err)) {
        return false;
    }
    else if (!m_err && !other.m_err) {
        return true;
    }
    return m_err->error == other.m_err->error && m_message_buf == other.m_message_buf;
}

void ErrorStorage::assign(std::exception_ptr eptr) noexcept
{
    if (!eptr) {
        clear();
        return;
    }

    m_err.emplace();
    m_err->usercode_error = nullptr;
    m_err->path = nullptr;
    auto populate_error = [&](const std::exception& ex, ErrorCodes::Error error_code) {
        m_err->error = realm_errno_e(error_code);
        m_err->categories = ErrorCodes::error_categories(error_code).value();
        try {
            m_message_buf = ex.what();
            m_err->message = m_message_buf.c_str();
        }
        catch (const std::bad_alloc&) {
            // If we are unable to build the new error because we ran out of memory we should propagate the OOM
            // condition and leaf the m_message_buf as it was.
            m_err->error = RLM_ERR_OUT_OF_MEMORY;
            m_err->message = "Out of memory while creating realm_error_t";
        }
    };

    try {
        std::rethrow_exception(eptr);
    }

    // Core exceptions:
    catch (const Exception& ex) {
        populate_error(ex, ex.code());
        if (ex.code() == ErrorCodes::CallbackFailed) {
            m_err->usercode_error = static_cast<const CallbackFailed&>(ex).usercode_error;
        }
        if (ErrorCodes::error_categories(ex.code()).test(ErrorCategory::file_access)) {
            auto& file_access_error = static_cast<const FileAccessError&>(ex);
            m_path_buf = file_access_error.get_path();
            m_err->path = m_path_buf.c_str();
        }
    }

    // Generic exceptions:
    catch (const std::invalid_argument& ex) {
        populate_error(ex, ErrorCodes::InvalidArgument);
    }
    catch (const std::out_of_range& ex) {
        populate_error(ex, ErrorCodes::OutOfBounds);
    }
    catch (const std::logic_error& ex) {
        populate_error(ex, ErrorCodes::LogicError);
    }
    catch (const std::runtime_error& ex) {
        populate_error(ex, ErrorCodes::RuntimeError);
    }
    catch (const std::bad_alloc& ex) {
        populate_error(ex, ErrorCodes::OutOfMemory);
    }
    catch (const std::exception& ex) {
        populate_error(ex, ErrorCodes::UnknownError);
    }
    // FIXME: Handle more exception types.
    catch (...) {
        m_err->error = RLM_ERR_UNKNOWN;
        m_message_buf = "Unknown error";
        m_err->message = m_message_buf.c_str();
    }
}

bool ErrorStorage::has_error() const noexcept
{
    return static_cast<bool>(m_err);
}

bool ErrorStorage::get_as_realm_error_t(realm_error_t* out) const noexcept
{
    if (!m_err) {
        return false;
    }

    if (out) {
        *out = *m_err;
    }
    return true;
}

bool ErrorStorage::clear() noexcept
{
    auto ret = static_cast<bool>(m_err);
    m_err.reset();
    return ret;
}

void ErrorStorage::set_usercode_error(void* usercode_error)
{
    m_usercode_error = usercode_error;
}

void* ErrorStorage::get_and_clear_usercode_error()
{
    auto ret = m_usercode_error;
    m_usercode_error = nullptr;
    return ret;
}

ErrorStorage* ErrorStorage::get_thread_local()
{
#if !defined(RLM_NO_THREAD_LOCAL)
    static thread_local ErrorStorage g_error_storage;
    return &g_error_storage;
#else
    static pthread_key_t g_last_exception_key;
    static pthread_once_t g_last_exception_key_init_once = PTHREAD_ONCE_INIT;

    pthread_once(&g_last_exception_key_init_once, [] {
        pthread_key_create(&g_last_exception_key, [](void* ptr) {
            delete reinterpret_cast<ErrorStorage*>(ptr);
        });
    });

    if (auto ptr = reinterpret_cast<ErrorStorage*>(pthread_getspecific(g_last_exception_key)); ptr != nullptr) {
        return ptr;
    }

    auto ptr = new ErrorStorage{};
    pthread_setspecific(g_last_exception_key, ptr);
    return ptr;
#endif
}

void set_last_exception(std::exception_ptr eptr)
{
    ErrorStorage::get_thread_local()->assign(std::move(eptr));
}

RLM_API bool realm_get_last_error(realm_error_t* err)
{
    return ErrorStorage::get_thread_local()->get_as_realm_error_t(err);
}

RLM_API bool realm_clear_last_error()
{
    return ErrorStorage::get_thread_local()->clear();
}

RLM_API realm_async_error_t* realm_get_last_error_as_async_error(void)
{
    if (!ErrorStorage::get_thread_local()->has_error()) {
        return nullptr;
    }

    return new realm_async_error_t{*ErrorStorage::get_thread_local()};
}

RLM_API bool realm_get_async_error(const realm_async_error_t* async_err, realm_error_t* out_err)
{
    if (!async_err)
        return false;

    return async_err->error_storage.get_as_realm_error_t(out_err);
}

} // namespace realm::c_api

RLM_EXPORT bool realm_wrap_exceptions(void (*func)()) noexcept
{
    return realm::c_api::wrap_err([=]() {
        (func)();
        return true;
    });
}

RLM_API void realm_register_user_code_callback_error(void* usercode_error) noexcept
{
    realm::c_api::ErrorStorage::get_thread_local()->set_usercode_error(usercode_error);
}
