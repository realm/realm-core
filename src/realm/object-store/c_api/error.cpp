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
    m_err->kind.code = 0;
    auto populate_error = [&](const std::exception& ex, realm_errno_e error_number) {
        m_err->error = error_number;
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

    // C API exceptions:
    catch (const NotClonableException& ex) {
        populate_error(ex, RLM_ERR_NOT_CLONABLE);
    }
    catch (const InvalidatedObjectException& ex) {
        populate_error(ex, RLM_ERR_INVALIDATED_OBJECT);
    }
    catch (const UnexpectedPrimaryKeyException& ex) {
        populate_error(ex, RLM_ERR_UNEXPECTED_PRIMARY_KEY);
    }
    catch (const DuplicatePrimaryKeyException& ex) {
        populate_error(ex, RLM_ERR_DUPLICATE_PRIMARY_KEY_VALUE);
    }
    catch (const InvalidPropertyKeyException& ex) {
        populate_error(ex, RLM_ERR_INVALID_PROPERTY);
    }
    catch (const CallbackFailed& ex) {
        populate_error(ex, RLM_ERR_CALLBACK);
    }

    // Core exceptions:
    catch (const NoSuchTable& ex) {
        populate_error(ex, RLM_ERR_NO_SUCH_TABLE);
    }
    catch (const KeyNotFound& ex) {
        populate_error(ex, RLM_ERR_NO_SUCH_OBJECT);
    }
    catch (const LogicError& ex) {
        using Kind = LogicError::ErrorKind;
        switch (ex.kind()) {
            case Kind::column_does_not_exist:
            case Kind::column_index_out_of_range:
                populate_error(ex, RLM_ERR_INVALID_PROPERTY);
                break;
            case Kind::wrong_transact_state:
                populate_error(ex, RLM_ERR_NOT_IN_A_TRANSACTION);
                break;
            default:
                populate_error(ex, RLM_ERR_LOGIC);
        }
    }

    // File exceptions:
    catch (const util::File::PermissionDenied& ex) {
        populate_error(ex, RLM_ERR_FILE_PERMISSION_DENIED);
    }
    catch (const util::File::AccessError& ex) {
        populate_error(ex, RLM_ERR_FILE_ACCESS_ERROR);
    }

    // Object Store exceptions:
    catch (const InvalidTransactionException& ex) {
        populate_error(ex, RLM_ERR_NOT_IN_A_TRANSACTION);
    }
    catch (const IncorrectThreadException& ex) {
        populate_error(ex, RLM_ERR_WRONG_THREAD);
    }
    catch (const DeleteOnOpenRealmException& ex) {
        populate_error(ex, RLM_ERR_DELETE_OPENED_REALM);
    }
    catch (const List::InvalidatedException& ex) {
        populate_error(ex, RLM_ERR_INVALIDATED_OBJECT);
    }
    catch (const MissingPrimaryKeyException& ex) {
        populate_error(ex, RLM_ERR_MISSING_PRIMARY_KEY);
    }
    catch (const WrongPrimaryKeyTypeException& ex) {
        populate_error(ex, RLM_ERR_WRONG_PRIMARY_KEY_TYPE);
    }
    catch (const PropertyTypeMismatch& ex) {
        populate_error(ex, RLM_ERR_PROPERTY_TYPE_MISMATCH);
    }
    catch (const NotNullableException& ex) {
        populate_error(ex, RLM_ERR_PROPERTY_NOT_NULLABLE);
    }
    catch (const object_store::Collection::OutOfBoundsIndexException& ex) {
        populate_error(ex, RLM_ERR_INDEX_OUT_OF_BOUNDS);
    }
    catch (const Results::OutOfBoundsIndexException& ex) {
        populate_error(ex, RLM_ERR_INDEX_OUT_OF_BOUNDS);
    }
    catch (const query_parser::InvalidQueryError& ex) {
        populate_error(ex, RLM_ERR_INVALID_QUERY);
    }
    catch (const query_parser::SyntaxError& ex) {
        populate_error(ex, RLM_ERR_INVALID_QUERY_STRING);
    }

    // Generic exceptions:
    catch (const std::invalid_argument& ex) {
        populate_error(ex, RLM_ERR_INVALID_ARGUMENT);
    }
    catch (const std::out_of_range& ex) {
        populate_error(ex, RLM_ERR_INDEX_OUT_OF_BOUNDS);
    }
    catch (const std::logic_error& ex) {
        populate_error(ex, RLM_ERR_LOGIC);
    }
    catch (const std::bad_alloc& ex) {
        populate_error(ex, RLM_ERR_OUT_OF_MEMORY);
    }
    catch (const std::exception& ex) {
        populate_error(ex, RLM_ERR_OTHER_EXCEPTION);
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

    *out = *m_err;
    return true;
}

bool ErrorStorage::clear() noexcept
{
    auto ret = static_cast<bool>(m_err);
    m_err.reset();
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

RLM_API void realm_get_async_error(const realm_async_error_t* async_err, realm_error_t* out_err)
{
    async_err->error_storage.get_as_realm_error_t(out_err);
}

} // namespace realm::c_api

RLM_EXPORT bool realm_wrap_exceptions(void (*func)()) noexcept
{
    return realm::c_api::wrap_err([=]() {
        (func)();
        return true;
    });
}
