#include <realm/object-store/c_api/util.hpp>
#include <realm/parser/query_parser.hpp>

#if REALM_PLATFORM_APPLE && !defined(RLM_NO_THREAD_LOCAL)
#define RLM_NO_THREAD_LOCAL
#endif

#if defined(RLM_NO_THREAD_LOCAL)
#include <pthread.h>
#endif

namespace realm::c_api {

#if !defined(RLM_NO_THREAD_LOCAL)

thread_local std::exception_ptr g_last_exception;

void set_last_exception(std::exception_ptr eptr)
{
    g_last_exception = eptr;
}

static std::exception_ptr* get_last_exception()
{
    return &g_last_exception;
}

#else // RLM_NO_THREAD_LOCAL

static pthread_key_t g_last_exception_key;
static pthread_once_t g_last_exception_key_init_once = PTHREAD_ONCE_INIT;

static void destroy_last_exception(void* ptr)
{
    auto p = static_cast<std::exception_ptr*>(ptr);
    delete p;
}

static void init_last_exception_key()
{
    pthread_key_create(&g_last_exception_key, destroy_last_exception);
}

void set_last_exception(std::exception_ptr eptr)
{
    pthread_once(&g_last_exception_key_init_once, init_last_exception_key);
    void* ptr = pthread_getspecific(g_last_exception_key);
    std::exception_ptr* p;
    if (!ptr) {
        p = new std::exception_ptr;
        pthread_setspecific(g_last_exception_key, p);
    }
    else {
        p = static_cast<std::exception_ptr*>(ptr);
    }
    *p = eptr;
}

static std::exception_ptr* get_last_exception()
{
    pthread_once(&g_last_exception_key_init_once, init_last_exception_key);
    void* ptr = pthread_getspecific(g_last_exception_key);
    return static_cast<std::exception_ptr*>(ptr);
}

#endif // RLM_NO_THREAD_LOCAL

static bool convert_error(std::exception_ptr* ptr, realm_error_t* err)
{
    if (ptr && *ptr) {
        err->kind.code = 0;

        auto populate_error = [&](const std::exception& ex, realm_errno_e error_number) {
            if (err) {
                err->error = error_number;
                err->message = ex.what();
            }
            *ptr = std::current_exception();
        };

        try {
            std::rethrow_exception(*ptr);
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

        // Object Store exceptions:
        catch (const InvalidTransactionException& ex) {
            populate_error(ex, RLM_ERR_NOT_IN_A_TRANSACTION);
        }
        catch (const IncorrectThreadException& ex) {
            populate_error(ex, RLM_ERR_WRONG_THREAD);
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
        catch (const List::OutOfBoundsIndexException& ex) {
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
            err->error = RLM_ERR_UNKNOWN;
            err->message = "Unknown error";
            *ptr = std::current_exception();
        }
        return true;
    }
    return false;
}

RLM_API bool realm_get_last_error(realm_error_t* err)
{
    std::exception_ptr* ptr = get_last_exception();
    if (ptr) {
        return convert_error(ptr, err);
    }
    return false;
}

RLM_API bool realm_clear_last_error()
{
    std::exception_ptr* ptr = get_last_exception();
    if (ptr && *ptr) {
        *ptr = std::exception_ptr{};
        return true;
    }
    return false;
}

RLM_API realm_async_error_t* realm_get_last_error_as_async_error(void)
{
    auto p = get_last_exception();
    if (p && *p) {
        return new realm_async_error_t{*p};
    }
    return nullptr;
}

RLM_API void realm_get_async_error(const realm_async_error_t* async_err, realm_error_t* out_err)
{
    convert_error(&const_cast<realm_async_error_t*>(async_err)->ep, out_err);
}

} // namespace realm::c_api

RLM_EXPORT void realm_rethrow_last_error()
{
    std::exception_ptr* ptr = realm::c_api::get_last_exception();
    if (ptr && *ptr) {
        std::rethrow_exception(*ptr);
    }
}

RLM_EXPORT bool realm_wrap_exceptions(void (*func)()) noexcept
{
    return realm::c_api::wrap_err([=]() {
        (func)();
        return true;
    });
}
