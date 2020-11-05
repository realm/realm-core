#include <cstring>
#include <mutex>

#include <realm/string_data.hpp>
#include <realm/util/cf_str.hpp>
#include <realm/util/features.h>
#include <realm/util/network_ssl.hpp>

#if REALM_HAVE_OPENSSL
#ifdef _WIN32
#include <Windows.h>
#else
#include <pthread.h>
#endif
#include <openssl/conf.h>
#include <openssl/x509v3.h>
#elif REALM_HAVE_SECURE_TRANSPORT
#include <fstream>
#include <vector>
#endif

using namespace realm;
using namespace realm::util;
using namespace realm::util::network;
using namespace realm::util::network::ssl;


namespace {

#if REALM_INCLUDE_CERTS

const char* root_certs[] = {
#include <realm/sync/noinst/root_certs.hpp>
};

bool verify_certificate_from_root_cert(const char* root_cert, X509* server_cert)
{
    bool verified = false;
    BIO* bio;
    X509* x509;
    EVP_PKEY* pkey;

    bio = BIO_new_mem_buf(const_cast<char*>(root_cert), -1);
    if (!bio)
        goto out;

    x509 = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr);
    if (!x509)
        goto free_bio;

    pkey = X509_get_pubkey(x509);
    if (!pkey)
        goto free_x509;

    verified = (X509_verify(server_cert, pkey) == 1);

    EVP_PKEY_free(pkey);
free_x509:
    X509_free(x509);
free_bio:
    BIO_free(bio);
out:
    return verified;
}

bool verify_certificate_from_root_certs(X509* server_cert, util::Logger* logger)
{
    std::size_t num_certs = sizeof(root_certs) / sizeof(root_certs[0]);

    if (logger)
        logger->info("Verifying server SSL certificate using %1 root certificates", num_certs);

    for (std::size_t i = 0; i < num_certs; ++i) {
        const char* root_cert = root_certs[i];
        bool verified = verify_certificate_from_root_cert(root_cert, server_cert);
        if (verified) {
            if (logger)
                logger->debug("Server SSL certificate verified using root certificate(%1):\n%2", i, root_cert);
            return true;
        }
    }

    if (logger)
        logger->error("The server certificate was not signed by any root certificate");
    return false;
}

#endif // REALM_INCLUDE_CERTS


#if REALM_HAVE_OPENSSL && (OPENSSL_VERSION_NUMBER < 0x10100000L || defined(LIBRESSL_VERSION_NUMBER))

// These must be made to execute before main() is called, i.e., before there is
// any chance of threads being spawned.
struct OpensslInit {
    std::unique_ptr<std::mutex[]> mutexes;
    OpensslInit();
    ~OpensslInit();
};

OpensslInit g_openssl_init;


void openssl_locking_func(int mode, int i, const char*, int)
{
    if (mode & CRYPTO_LOCK) {
        g_openssl_init.mutexes[i].lock();
    }
    else {
        g_openssl_init.mutexes[i].unlock();
    }
}


OpensslInit::OpensslInit()
{
    SSL_library_init();
    SSL_load_error_strings();
    OpenSSL_add_all_algorithms();
    std::size_t n = CRYPTO_num_locks();
    mutexes.reset(new std::mutex[n]); // Throws
    CRYPTO_set_locking_callback(&openssl_locking_func);
    /*
    #if !defined(SSL_OP_NO_COMPRESSION) && (OPENSSL_VERSION_NUMBER >= 0x00908000L)
        null_compression_methods_ = sk_SSL_COMP_new_null();
    #endif
    */
}


OpensslInit::~OpensslInit()
{
    /*
    #if !defined(SSL_OP_NO_COMPRESSION) && (OPENSSL_VERSION_NUMBER >= 0x00908000L)
        sk_SSL_COMP_free(null_compression_methods_);
    #endif
    */
    CRYPTO_set_locking_callback(0);
    ERR_free_strings();
#if OPENSSL_VERSION_NUMBER < 0x10000000L
    ERR_remove_state(0);
#else
    ERR_remove_thread_state(0);
#endif
    EVP_cleanup();
    CRYPTO_cleanup_all_ex_data();
    CONF_modules_unload(1);
}

#endif // REALM_HAVE_OPENSSL && (OPENSSL_VERSION_NUMBER < 0x10100000L || defined(LIBRESSL_VERSION_NUMBER))

} // unnamed namespace


namespace realm {
namespace util {
namespace network {
namespace ssl {

ErrorCategory error_category;


const char* ErrorCategory::name() const noexcept
{
    return "realm.util.network.ssl";
}


std::string ErrorCategory::message(int value) const
{
    switch (Errors(value)) {
        case Errors::certificate_rejected:
            return "SSL certificate rejected"; // Throws
    }
    REALM_ASSERT(false);
    return {};
}


bool ErrorCategory::equivalent(const std::error_code& ec, int condition) const noexcept
{
    switch (Errors(condition)) {
        case Errors::certificate_rejected:
#if REALM_HAVE_OPENSSL
            if (ec.category() == openssl_error_category) {
                // FIXME: Why use string comparison here? Seems like it would
                // suffice to compare the underlying numerical error codes.
                std::string message = ec.message();
                return ((message == "certificate verify failed" || message == "sslv3 alert bad certificate" ||
                         message == "sslv3 alert certificate expired" ||
                         message == "sslv3 alert certificate revoked"));
            }
#elif REALM_HAVE_SECURE_TRANSPORT
            if (ec.category() == secure_transport_error_category) {
                switch (ec.value()) {
                    case errSSLXCertChainInvalid:
                        return true;
                    default:
                        break;
                }
            }
#endif
            return false;
    }
    return false;
}

} // namespace ssl


OpensslErrorCategory openssl_error_category;


const char* OpensslErrorCategory::name() const noexcept
{
    return "openssl";
}


std::string OpensslErrorCategory::message(int value) const
{
#if REALM_HAVE_OPENSSL
    if (const char* s = ERR_reason_error_string(value))
        return std::string(s); // Throws
#endif
    return "Unknown OpenSSL error (" + util::to_string(value) + ")"; // Throws
}


SecureTransportErrorCategory secure_transport_error_category;


const char* SecureTransportErrorCategory::name() const noexcept
{
    return "securetransport";
}


std::string SecureTransportErrorCategory::message(int value) const
{
#if REALM_HAVE_SECURE_TRANSPORT
#if __has_builtin(__builtin_available)
    if (__builtin_available(iOS 11.3, macOS 10.3, tvOS 11.3, watchOS 4.3, *)) {
        auto status = OSStatus(value);
        void* reserved = nullptr;
        if (auto message = adoptCF(SecCopyErrorMessageString(status, reserved)))
            return cfstring_to_std_string(message.get());
    }
#endif // __has_builtin(__builtin_available)
#endif // REALM_HAVE_SECURE_TRANSPORT

    return std::string("Unknown SecureTransport error (") + util::to_string(value) + ")"; // Throws
}


namespace ssl {

const char* ProtocolNotSupported::what() const noexcept
{
    return "SSL/TLS protocol not supported";
}


std::error_code Stream::handshake(std::error_code& ec)
{
    REALM_ASSERT(!m_tcp_socket.m_read_oper || !m_tcp_socket.m_read_oper->in_use());
    REALM_ASSERT(!m_tcp_socket.m_write_oper || !m_tcp_socket.m_write_oper->in_use());
    m_tcp_socket.m_desc.ensure_blocking_mode(); // Throws
    Want want = Want::nothing;
    ssl_handshake(ec, want);
    REALM_ASSERT(want == Want::nothing);
    return ec;
}


std::error_code Stream::shutdown(std::error_code& ec)
{
    REALM_ASSERT(!m_tcp_socket.m_write_oper || !m_tcp_socket.m_write_oper->in_use());
    m_tcp_socket.m_desc.ensure_blocking_mode(); // Throws
    Want want = Want::nothing;
    ssl_shutdown(ec, want);
    REALM_ASSERT(want == Want::nothing);
    return ec;
}


#if REALM_HAVE_OPENSSL

void Context::ssl_init()
{
    ERR_clear_error();

    // Despite the name, SSLv23_method isn't specific to SSLv2 and SSLv3.
    // It negotiates with the peer to pick the newest enabled protocol version.
    const SSL_METHOD* method = SSLv23_method();

    SSL_CTX* ssl_ctx = SSL_CTX_new(method);
    if (REALM_UNLIKELY(!ssl_ctx)) {
        std::error_code ec(int(ERR_get_error()), openssl_error_category);
        throw std::system_error(ec);
    }

    // Disable use of older protocol versions (SSLv2 and SSLv3).
    // Disable SSL compression by default, as compression is unavailable
    // with Apple's Secure Transport API.
    long options = 0;
    options |= SSL_OP_NO_SSLv2;
    options |= SSL_OP_NO_SSLv3;
    options |= SSL_OP_NO_COMPRESSION;
    SSL_CTX_set_options(ssl_ctx, options);

    m_ssl_ctx = ssl_ctx;
}


void Context::ssl_destroy() noexcept
{
    /*
        if (handle_->default_passwd_callback_userdata) {
            detail::password_callback_base* callback =
       static_cast<detail::password_callback_base*>(handle_->default_passwd_callback_userdata); delete callback;
            handle_->default_passwd_callback_userdata = nullptr;
        }

        if (SSL_CTX_get_app_data(handle_)) {
            detail::verify_callback_base* callback =
       static_cast<detail::verify_callback_base*>(SSL_CTX_get_app_data(handle_)); delete callback;
            SSL_CTX_set_app_data(handle_, nullptr);
        }
    */
    SSL_CTX_free(m_ssl_ctx);
}


void Context::ssl_use_certificate_chain_file(const std::string& path, std::error_code& ec)
{
    ERR_clear_error();
    int ret = SSL_CTX_use_certificate_chain_file(m_ssl_ctx, path.c_str());
    if (REALM_UNLIKELY(ret != 1)) {
        ec = std::error_code(int(ERR_get_error()), openssl_error_category);
        return;
    }
    ec = std::error_code();
}


void Context::ssl_use_private_key_file(const std::string& path, std::error_code& ec)
{
    ERR_clear_error();
    int type = SSL_FILETYPE_PEM;
    int ret = SSL_CTX_use_PrivateKey_file(m_ssl_ctx, path.c_str(), type);
    if (REALM_UNLIKELY(ret != 1)) {
        ec = std::error_code(int(ERR_get_error()), openssl_error_category);
        return;
    }
    ec = std::error_code();
}


void Context::ssl_use_default_verify(std::error_code& ec)
{
    ERR_clear_error();
    int ret = SSL_CTX_set_default_verify_paths(m_ssl_ctx);
    if (ret != 1) {
        ec = std::error_code(int(ERR_get_error()), openssl_error_category);
        return;
    }
    ec = std::error_code();
}


void Context::ssl_use_verify_file(const std::string& path, std::error_code& ec)
{
    ERR_clear_error();
    int ret = SSL_CTX_load_verify_locations(m_ssl_ctx, path.c_str(), nullptr);
    if (ret != 1) {
        ec = std::error_code(int(ERR_get_error()), openssl_error_category);
        return;
    }

    ec = std::error_code();
}

#if OPENSSL_VERSION_NUMBER >= 0x10100000L && !defined(LIBRESSL_VERSION_NUMBER)
class Stream::BioMethod {
public:
    BIO_METHOD* bio_method;

    BioMethod()
    {
        const char* name = "realm::util::Stream::BioMethod";
        bio_method = BIO_meth_new(BIO_get_new_index(), name);
        if (!bio_method)
            throw util::bad_alloc();

        BIO_meth_set_write(bio_method, &Stream::bio_write);
        BIO_meth_set_read(bio_method, &Stream::bio_read);
        BIO_meth_set_puts(bio_method, &Stream::bio_puts);
        BIO_meth_set_gets(bio_method, nullptr);
        BIO_meth_set_ctrl(bio_method, &Stream::bio_ctrl);
        BIO_meth_set_create(bio_method, &Stream::bio_create);
        BIO_meth_set_destroy(bio_method, &Stream::bio_destroy);
        BIO_meth_set_callback_ctrl(bio_method, nullptr);
    }

    ~BioMethod()
    {
        BIO_meth_free(bio_method);
    }
};
#else
class Stream::BioMethod {
public:
    BIO_METHOD* bio_method;

    BioMethod()
    {
        bio_method = new BIO_METHOD{
            BIO_TYPE_SOCKET,      // int type
            nullptr,              // const char* name
            &Stream::bio_write,   // int (*bwrite)(BIO*, const char*, int)
            &Stream::bio_read,    // int (*bread)(BIO*, char*, int)
            &Stream::bio_puts,    // int (*bputs)(BIO*, const char*)
            nullptr,              // int (*bgets)(BIO*, char*, int)
            &Stream::bio_ctrl,    // long (*ctrl)(BIO*, int, long, void*)
            &Stream::bio_create,  // int (*create)(BIO*)
            &Stream::bio_destroy, // int (*destroy)(BIO*)
            nullptr               // long (*callback_ctrl)(BIO*, int, bio_info_cb*)
        };
    }

    ~BioMethod()
    {
        delete bio_method;
    }
};
#endif


Stream::BioMethod Stream::s_bio_method;


#if OPENSSL_VERSION_NUMBER < 0x10002000L || defined(LIBRESSL_VERSION_NUMBER)

namespace {

// check_common_name() checks that \param  server_cert constains host_name
// as Common Name. The function is used by verify_callback() for
// OpenSSL versions before 1.0.2.
bool check_common_name(X509* server_cert, const std::string& host_name)
{
    // Find the position of the Common Name field in the Subject field of the certificate
    int common_name_loc = -1;
    common_name_loc = X509_NAME_get_index_by_NID(X509_get_subject_name(server_cert), NID_commonName, -1);
    if (common_name_loc < 0)
        return false;

    // Extract the Common Name field
    X509_NAME_ENTRY* common_name_entry;
    common_name_entry = X509_NAME_get_entry(X509_get_subject_name(server_cert), common_name_loc);
    if (!common_name_entry)
        return false;

    // Convert the Common Namefield to a C string
    ASN1_STRING* common_name_asn1;
    common_name_asn1 = X509_NAME_ENTRY_get_data(common_name_entry);
    if (!common_name_asn1)
        return false;

    char* common_name_str = reinterpret_cast<char*>(ASN1_STRING_data(common_name_asn1));

    // Make sure there isn't an embedded NUL character in the Common Name
    if (static_cast<std::size_t>(ASN1_STRING_length(common_name_asn1)) != std::strlen(common_name_str))
        return false;

    bool names_equal = (host_name == common_name_str);
    return names_equal;
}

// check_common_name() checks that \param  server_cert constains host_name
// in the Subject Alternative Name DNS section. The function is used by verify_callback()
// for OpenSSL versions before 1.0.2.
bool check_san(X509* server_cert, const std::string& host_name)
{
    STACK_OF(GENERAL_NAME) * san_names;

    // Try to extract the names within the SAN extension from the certificate
    san_names =
        static_cast<STACK_OF(GENERAL_NAME)*>(X509_get_ext_d2i(server_cert, NID_subject_alt_name, nullptr, nullptr));
    if (!san_names)
        return false;

    int san_names_nb = sk_GENERAL_NAME_num(san_names);

    bool found = false;

    // Check each name within the extension
    for (int i = 0; i < san_names_nb; ++i) {
        const GENERAL_NAME* current_name = sk_GENERAL_NAME_value(san_names, i);

        if (current_name->type == GEN_DNS) {
            // Current name is a DNS name
            char* dns_name = static_cast<char*>(ASN1_STRING_data(current_name->d.dNSName));

            // Make sure there isn't an embedded NUL character in the DNS name
            if (static_cast<std::size_t>(ASN1_STRING_length(current_name->d.dNSName)) != std::strlen(dns_name))
                break;

            if (host_name == dns_name) {
                found = true;
                break;
            }
        }
    }

    sk_GENERAL_NAME_pop_free(san_names, GENERAL_NAME_free);

    return found;
}

} // namespace

int Stream::verify_callback_using_hostname(int preverify_ok, X509_STORE_CTX* ctx) noexcept
{
    if (preverify_ok != 1)
        return preverify_ok;

    X509* server_cert = X509_STORE_CTX_get_current_cert(ctx);

    int err = X509_STORE_CTX_get_error(ctx);
    if (err != X509_V_OK)
        return 0;

    int depth = X509_STORE_CTX_get_error_depth(ctx);

    // We only inspect the certificate at depth = 0.
    if (depth > 0)
        return preverify_ok;

    // Retrieve the pointer to the SSL object for this connection.
    SSL* ssl = static_cast<SSL*>(X509_STORE_CTX_get_ex_data(ctx, SSL_get_ex_data_X509_STORE_CTX_idx()));

    // The stream object is stored as data in the SSL object.
    Stream* stream = static_cast<Stream*>(SSL_get_ex_data(ssl, 0));

    const std::string& host_name = stream->m_host_name;

    if (check_common_name(server_cert, host_name))
        return 1;

    if (check_san(server_cert, host_name))
        return 1;

    return 0;
}

#endif


void Stream::ssl_set_verify_mode(VerifyMode mode, std::error_code& ec)
{
    int mode_2 = 0;
    switch (mode) {
        case VerifyMode::none:
            break;
        case VerifyMode::peer:
            mode_2 = SSL_VERIFY_PEER;
            break;
    }

    int rc = SSL_set_ex_data(m_ssl, 0, this);
    if (rc == 0) {
        ec = std::error_code(int(ERR_get_error()), openssl_error_category);
        return;
    }

#if OPENSSL_VERSION_NUMBER < 0x10002000L || defined(LIBRESSL_VERSION_NUMBER)
    SSL_set_verify(m_ssl, mode_2, &Stream::verify_callback_using_hostname);
#else
    // verify_callback is nullptr.
    SSL_set_verify(m_ssl, mode_2, nullptr);
#endif
    ec = std::error_code();
}


void Stream::ssl_set_host_name(const std::string& host_name, std::error_code& ec)
{
    // Enable Server Name Indication (SNI) extension
#if OPENSSL_VERSION_NUMBER >= 0x10101000L
    {
#ifndef _WIN32
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
#endif
        auto ret = SSL_set_tlsext_host_name(m_ssl, host_name.c_str());
#ifndef _WIN32
#pragma GCC diagnostic pop
#endif
        if (ret == 0) {
            ec = std::error_code(int(ERR_get_error()), openssl_error_category);
            return;
        }
    }
#else
    static_cast<void>(host_name);
    static_cast<void>(ec);
#endif

    // Enable host name check during certificate validation
#if OPENSSL_VERSION_NUMBER >= 0x10002000L && !defined(LIBRESSL_VERSION_NUMBER)
    {
        X509_VERIFY_PARAM* param = SSL_get0_param(m_ssl);
        X509_VERIFY_PARAM_set_hostflags(param, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
        auto ret = X509_VERIFY_PARAM_set1_host(param, host_name.c_str(), 0);
        if (ret == 0) {
            ec = std::error_code(int(ERR_get_error()), openssl_error_category);
            return;
        }
    }
#else
    static_cast<void>(host_name);
    static_cast<void>(ec);
#endif
}

void Stream::ssl_use_verify_callback(const std::function<SSLVerifyCallback>& callback, std::error_code&)
{
    m_ssl_verify_callback = &callback;

    SSL_set_verify(m_ssl, SSL_VERIFY_PEER, &Stream::verify_callback_using_delegate);
}

#ifndef _WIN32
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
#endif

#if REALM_INCLUDE_CERTS
void Stream::ssl_use_included_certificates(std::error_code&)
{
    REALM_ASSERT(!m_ssl_verify_callback);

    SSL_set_verify(m_ssl, SSL_VERIFY_PEER, &Stream::verify_callback_using_root_certs);
}

int Stream::verify_callback_using_root_certs(int preverify_ok, X509_STORE_CTX* ctx)
{
    if (preverify_ok)
        return 1;

    X509* server_cert = X509_STORE_CTX_get_current_cert(ctx);

    SSL* ssl = static_cast<SSL*>(X509_STORE_CTX_get_ex_data(ctx, SSL_get_ex_data_X509_STORE_CTX_idx()));
    Stream* stream = static_cast<Stream*>(SSL_get_ex_data(ssl, 0));
    REALM_ASSERT(stream);

    util::Logger* logger = stream->logger;

    const std::string& host_name = stream->m_host_name;
    port_type server_port = stream->m_server_port;

    if (logger && logger->would_log(util::Logger::Level::debug)) {
        BIO* bio = BIO_new(BIO_s_mem());
        if (bio) {
            int ret = PEM_write_bio_X509(bio, server_cert);
            if (ret) {
                BUF_MEM* buffer;
                BIO_get_mem_ptr(bio, &buffer);

                const char* pem_data = buffer->data;
                std::size_t pem_size = buffer->length;

                logger->debug("Verifying server SSL certificate using root certificates, "
                              "host name = %1, server port = %2, certificate =\n%3",
                              host_name, server_port, StringData{pem_data, pem_size});
            }
            BIO_free(bio);
        }
    }

    bool valid = verify_certificate_from_root_certs(server_cert, logger);
    if (!valid && logger) {
        logger->error("server SSL certificate rejected using root certificates, "
                      "host name = %1, server port = %2",
                      host_name, server_port);
    }

    return int(valid);
}
#endif

int Stream::verify_callback_using_delegate(int preverify_ok, X509_STORE_CTX* ctx) noexcept
{
    X509* server_cert = X509_STORE_CTX_get_current_cert(ctx);

    int depth = X509_STORE_CTX_get_error_depth(ctx);

    BIO* bio = BIO_new(BIO_s_mem());
    if (!bio) {
        // certificate rejected if a memory error occurs.
        return 0;
    }

    int ret = PEM_write_bio_X509(bio, server_cert);
    if (!ret) {
        BIO_free(bio);
        return 0;
    }

    BUF_MEM* buffer;
    BIO_get_mem_ptr(bio, &buffer);

    const char* pem_data = buffer->data;
    std::size_t pem_size = buffer->length;

    SSL* ssl = static_cast<SSL*>(X509_STORE_CTX_get_ex_data(ctx, SSL_get_ex_data_X509_STORE_CTX_idx()));
    Stream* stream = static_cast<Stream*>(SSL_get_ex_data(ssl, 0));
    const std::string& host_name = stream->m_host_name;
    port_type server_port = stream->m_server_port;

    REALM_ASSERT(stream->m_ssl_verify_callback);
    const std::function<SSLVerifyCallback>& callback = *stream->m_ssl_verify_callback;

    // FIXME: Oops, the callback may throw, but verify_callback_using_delegate()
    // is not allowed to throw. It does not seem to be reasonable to deny the
    // callback the opportunity of throwing. The right solution seems to be to
    // carry an exception across the OpenSSL C-layer using the exception object
    // transportation mechanism offered by C++.
    bool valid = callback(host_name, server_port, pem_data, pem_size, preverify_ok, depth); // Throws

    BIO_free(bio);
    return int(valid);
}

#ifndef _WIN32
#pragma GCC diagnostic pop
#endif

void Stream::ssl_init()
{
    SSL_CTX* ssl_ctx = m_ssl_context.m_ssl_ctx;
    SSL* ssl = SSL_new(ssl_ctx);
    if (REALM_UNLIKELY(!ssl)) {
        std::error_code ec(int(ERR_get_error()), openssl_error_category);
        throw std::system_error(ec);
    }
    SSL_set_mode(ssl, SSL_MODE_ENABLE_PARTIAL_WRITE);
#if defined(SSL_MODE_RELEASE_BUFFERS)
    SSL_set_mode(ssl, SSL_MODE_RELEASE_BUFFERS);
#endif

    BIO* bio = BIO_new(s_bio_method.bio_method);

    if (REALM_UNLIKELY(!bio)) {
        SSL_free(ssl);
        std::error_code ec(int(ERR_get_error()), openssl_error_category);
        throw std::system_error(ec);
    }

#if OPENSSL_VERSION_NUMBER >= 0x10100000L && !defined(LIBRESSL_VERSION_NUMBER)
    BIO_set_data(bio, this);
#else
    bio->ptr = this;
#endif

    SSL_set_bio(ssl, bio, bio);
    m_ssl = ssl;
}


void Stream::ssl_destroy() noexcept
{
    SSL_free(m_ssl);
}


int Stream::bio_write(BIO* bio, const char* data, int size) noexcept
{
#if OPENSSL_VERSION_NUMBER >= 0x10100000L && !defined(LIBRESSL_VERSION_NUMBER)
    Stream& stream = *static_cast<Stream*>(BIO_get_data(bio));
#else
    Stream& stream = *static_cast<Stream*>(bio->ptr);
#endif
    Service::Descriptor& desc = stream.m_tcp_socket.m_desc;
    std::error_code ec;
    std::size_t n = desc.write_some(data, std::size_t(size), ec);
    BIO_clear_retry_flags(bio);
    if (ec) {
        if (REALM_UNLIKELY(ec != error::resource_unavailable_try_again)) {
            stream.m_bio_error_code = ec;
            return -1;
        }
        BIO_set_retry_write(bio);
        return -1;
    }
    return int(n);
}


int Stream::bio_read(BIO* bio, char* buffer, int size) noexcept
{
#if OPENSSL_VERSION_NUMBER >= 0x10100000L && !defined(LIBRESSL_VERSION_NUMBER)
    Stream& stream = *static_cast<Stream*>(BIO_get_data(bio));
#else
    Stream& stream = *static_cast<Stream*>(bio->ptr);
#endif
    Service::Descriptor& desc = stream.m_tcp_socket.m_desc;
    std::error_code ec;
    std::size_t n = desc.read_some(buffer, std::size_t(size), ec);
    BIO_clear_retry_flags(bio);
    if (ec) {
        if (REALM_UNLIKELY(ec == MiscExtErrors::end_of_input)) {
            // This behaviour agrees with `crypto/bio/bss_sock.c` of OpenSSL.
            return 0;
        }
        if (REALM_UNLIKELY(ec != error::resource_unavailable_try_again)) {
            stream.m_bio_error_code = ec;
            return -1;
        }
        BIO_set_retry_read(bio);
        return -1;
    }
    return int(n);
}


int Stream::bio_puts(BIO* bio, const char* c_str) noexcept
{
    std::size_t size = std::strlen(c_str);
    return bio_write(bio, c_str, int(size));
}


long Stream::bio_ctrl(BIO*, int cmd, long, void*) noexcept
{
    switch (cmd) {
        case BIO_CTRL_PUSH:
        case BIO_CTRL_POP:
            // Ignoring in alignment with `crypto/bio/bss_sock.c` of OpenSSL.
            return 0;
        case BIO_CTRL_FLUSH:
            // Ignoring in alignment with `crypto/bio/bss_sock.c` of OpenSSL.
            return 1;
    }
    REALM_ASSERT(false);
    return 0;
}


int Stream::bio_create(BIO* bio) noexcept
{
#if OPENSSL_VERSION_NUMBER >= 0x10100000L && !defined(LIBRESSL_VERSION_NUMBER)
    BIO_set_init(bio, 1);
    BIO_set_data(bio, nullptr);
    BIO_clear_flags(bio, 0);
    BIO_set_shutdown(bio, 0);
#else
    // In alignment with `crypto/bio/bss_sock.c` of OpenSSL.
    bio->init = 1;
    bio->num = 0;
    bio->ptr = nullptr;
    bio->flags = 0;
#endif
    return 1;
}


int Stream::bio_destroy(BIO*) noexcept
{
    return 1;
}


#elif REALM_HAVE_SECURE_TRANSPORT

#pragma GCC diagnostic ignored "-Wdeprecated-declarations" // FIXME: Should this be removed at some point?

void Context::ssl_init() {}

void Context::ssl_destroy() noexcept
{
#if REALM_HAVE_KEYCHAIN_APIS
    if (m_keychain) {
        m_keychain.reset();
        unlink(m_keychain_path.data());
        m_keychain_path = {};
    }
#endif
}

// Load certificates and/or keys from the specified PEM file. If keychain is non-null, the items will be
// imported into that keychain.
util::CFPtr<CFArrayRef> Context::load_pem_file(const std::string& path, SecKeychainRef keychain, std::error_code& ec)
{
    using util::adoptCF;
    using util::CFPtr;

    std::ifstream file(path);
    if (!file) {
        // Rely on the open attempt having set errno to a sensible value as ifstream's
        // own error reporting gives terribly generic error messages.
        ec = make_basic_system_error_code(errno);
        return util::CFPtr<CFArrayRef>();
    }
    std::vector<char> contents{std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};

    auto contentsCF = adoptCF(CFDataCreateWithBytesNoCopy(nullptr, reinterpret_cast<const UInt8*>(contents.data()),
                                                          contents.size(), kCFAllocatorNull));

    // If we don't need to import it into a keychain, try to interpret the data
    // as a certificate directly. This only works for DER files, so we fall back
    // to SecItemImport() on platforms which support that if this fails.
    if (keychain == nullptr) {
        if (auto certificate = adoptCF(SecCertificateCreateWithData(NULL, contentsCF.get()))) {
            auto ref = certificate.get();
            return adoptCF(CFArrayCreate(nullptr, const_cast<const void**>(reinterpret_cast<void**>(&ref)), 1,
                                         &kCFTypeArrayCallBacks));
        }

        // SecCertificateCreateWithData doesn't tell us why it failed, so just
        // report the error code that SecItemImport uses when given something
        // that's not a certificate
        ec = std::error_code(errSecUnknownFormat, secure_transport_error_category);
    }

    CFArrayRef items = nullptr;

#if REALM_HAVE_KEYCHAIN_APIS
    SecItemImportExportKeyParameters params{};
    params.version = SEC_KEY_IMPORT_EXPORT_PARAMS_VERSION;

    CFPtr<CFStringRef> pathCF = adoptCF(CFStringCreateWithBytes(nullptr, reinterpret_cast<const UInt8*>(path.data()),
                                                                path.size(), kCFStringEncodingUTF8, false));

    SecExternalFormat format = kSecFormatUnknown;
    SecExternalItemType itemType = kSecItemTypeUnknown;
    if (OSStatus status =
            SecItemImport(contentsCF.get(), pathCF.get(), &format, &itemType, 0, &params, keychain, &items)) {
        ec = std::error_code(status, secure_transport_error_category);
        return util::CFPtr<CFArrayRef>();
    }
    ec = {};
#endif

    return adoptCF(items);
}

#if REALM_HAVE_KEYCHAIN_APIS

static std::string temporary_directory()
{
    auto ensure_trailing_slash = [](auto str) {
        return str.back() == '/' ? str : str + '/';
    };

    std::string path;
    path.resize(PATH_MAX);
    std::size_t result = confstr(_CS_DARWIN_USER_TEMP_DIR, &path[0], path.size());
    if (result && result <= path.size()) {
        path.resize(result - 1);
        return ensure_trailing_slash(std::move(path));
    }

    // We failed to retrieve temporary directory from confstr. Fall back to the TMPDIR
    // environment variable if we're not running with elevated privileges, and then to /tmp.
    if (!issetugid()) {
        path = getenv("TMPDIR");
        if (path.size()) {
            return ensure_trailing_slash(std::move(path));
        }
    }
    return "/tmp/";
}


std::error_code Context::open_temporary_keychain_if_needed()
{
    if (m_keychain) {
        return std::error_code();
    }

    std::string path = temporary_directory() + "realm-sync-ssl-XXXXXXXX.keychain";
    int fd = mkstemps(&path[0], std::strlen(".keychain"));
    if (fd < 0) {
        return make_basic_system_error_code(errno);
    }

    // Close and remove the file so that we can create a keychain in its place.
    close(fd);
    unlink(path.data());

    SecKeychainRef keychain = nullptr;
    std::string password = "";
    if (OSStatus status =
            SecKeychainCreate(path.data(), UInt32(password.size()), password.data(), false, nullptr, &keychain))
        return std::error_code(status, secure_transport_error_category);

    m_keychain = adoptCF(keychain);
    m_keychain_path = std::move(path);

    return std::error_code();
}


// Creates an identity from the certificate and private key. The private key must exist in m_keychain.
std::error_code Context::update_identity_if_needed()
{
    // If we've not yet loaded both the certificate and private key there's nothing to do.
    if (!m_certificate || !m_private_key) {
        return std::error_code();
    }

    SecIdentityRef identity = nullptr;
    if (OSStatus status = SecIdentityCreateWithCertificate(m_keychain.get(), m_certificate.get(), &identity)) {
        return std::error_code(status, secure_transport_error_category);
    }

    m_identity = util::adoptCF(identity);
    return std::error_code();
}

#endif // REALM_HAVE_KEYCHAIN_APIS

void Context::ssl_use_certificate_chain_file(const std::string& path, std::error_code& ec)
{
#if !REALM_HAVE_KEYCHAIN_APIS
    static_cast<void>(path);
    ec = make_basic_system_error_code(ENOTSUP);
#else
    auto items = load_pem_file(path, nullptr, ec);
    if (!items) {
        REALM_ASSERT(ec);
        return;
    }

    if (CFArrayGetCount(items.get()) < 1) {
        ec = std::error_code(errSecItemNotFound, secure_transport_error_category);
        return;
    }

    CFTypeRef item = CFArrayGetValueAtIndex(items.get(), 0);
    if (CFGetTypeID(item) != SecCertificateGetTypeID()) {
        ec = std::error_code(errSecItemNotFound, secure_transport_error_category);
        return;
    }

    m_certificate = util::retainCF(reinterpret_cast<SecCertificateRef>(const_cast<void*>(item)));

    // The returned array contains the server certificate followed by the remainder of the certificates in the chain.
    // Remove the server certificate to leave us with an array containing only the remainder of the certificate chain.
    auto certificate_chain = util::adoptCF(CFArrayCreateMutableCopy(nullptr, 0, items.get()));
    CFArrayRemoveValueAtIndex(certificate_chain.get(), 0);
    m_certificate_chain = util::adoptCF(reinterpret_cast<CFArrayRef>(certificate_chain.release()));

    ec = update_identity_if_needed();
#endif
}


void Context::ssl_use_private_key_file(const std::string& path, std::error_code& ec)
{
#if !REALM_HAVE_KEYCHAIN_APIS
    static_cast<void>(path);
    ec = make_basic_system_error_code(ENOTSUP);
#else
    ec = open_temporary_keychain_if_needed();
    if (ec) {
        return;
    }

    auto items = load_pem_file(path, m_keychain.get(), ec);
    if (!items) {
        return;
    }

    if (CFArrayGetCount(items.get()) != 1) {
        ec = std::error_code(errSecItemNotFound, secure_transport_error_category);
        return;
    }

    CFTypeRef item = CFArrayGetValueAtIndex(items.get(), 0);
    if (CFGetTypeID(item) != SecKeyGetTypeID()) {
        ec = std::error_code(errSecItemNotFound, secure_transport_error_category);
        return;
    }

    m_private_key = util::retainCF(reinterpret_cast<SecKeyRef>(const_cast<void*>(item)));
    ec = update_identity_if_needed();
#endif
}

void Context::ssl_use_default_verify(std::error_code&) {}

void Context::ssl_use_verify_file(const std::string& path, std::error_code& ec)
{
#if REALM_HAVE_KEYCHAIN_APIS
    m_trust_anchors = load_pem_file(path, m_keychain.get(), ec);
#else
    m_trust_anchors = load_pem_file(path, nullptr, ec);
#endif

    if (m_trust_anchors && CFArrayGetCount(m_trust_anchors.get())) {
        const void* leaf_certificate = CFArrayGetValueAtIndex(m_trust_anchors.get(), 0);
        m_pinned_certificate =
            adoptCF(SecCertificateCopyData(static_cast<SecCertificateRef>(const_cast<void*>(leaf_certificate))));
    }
    else {
        m_pinned_certificate.reset();
    }
}

void Stream::ssl_init()
{
    SSLProtocolSide side = m_handshake_type == HandshakeType::client ? kSSLClientSide : kSSLServerSide;
    m_ssl = util::adoptCF(SSLCreateContext(nullptr, side, kSSLStreamType));
    if (OSStatus status = SSLSetIOFuncs(m_ssl.get(), Stream::tcp_read, Stream::tcp_write)) {
        std::error_code ec(status, secure_transport_error_category);
        throw std::system_error(ec);
    }
    if (OSStatus status = SSLSetConnection(m_ssl.get(), this)) {
        std::error_code ec(status, secure_transport_error_category);
        throw std::system_error(ec);
    }

    // Require TLSv1 or greater.
    if (OSStatus status = SSLSetProtocolVersionMin(m_ssl.get(), kTLSProtocol1)) {
        std::error_code ec(status, secure_transport_error_category);
        throw std::system_error(ec);
    }

    // Break after certificate exchange to allow for customizing the verification process.
    SSLSessionOption option = m_handshake_type == HandshakeType::client ? kSSLSessionOptionBreakOnServerAuth
                                                                        : kSSLSessionOptionBreakOnClientAuth;
    if (OSStatus status = SSLSetSessionOption(m_ssl.get(), option, true)) {
        std::error_code ec(status, secure_transport_error_category);
        throw std::system_error(ec);
    }

#if REALM_HAVE_KEYCHAIN_APIS
    if (m_ssl_context.m_identity && m_ssl_context.m_certificate_chain) {
        // SSLSetCertificate expects an array containing the identity followed by the identity's certificate chain.
        auto certificates = util::adoptCF(CFArrayCreateMutable(nullptr, 0, &kCFTypeArrayCallBacks));
        CFArrayInsertValueAtIndex(certificates.get(), 0, m_ssl_context.m_identity.get());

        CFArrayRef certificate_chain = m_ssl_context.m_certificate_chain.get();
        CFArrayAppendArray(certificates.get(), certificate_chain, CFRangeMake(0, CFArrayGetCount(certificate_chain)));

        if (OSStatus status = SSLSetCertificate(m_ssl.get(), certificates.get())) {
            std::error_code ec(status, secure_transport_error_category);
            throw std::system_error(ec);
        }
    }
#endif
}


void Stream::ssl_destroy() noexcept
{
    m_ssl.reset();
}


void Stream::ssl_set_verify_mode(VerifyMode verify_mode, std::error_code& ec)
{
    m_verify_mode = verify_mode;
    ec = std::error_code();
}


void Stream::ssl_set_host_name(const std::string& host_name, std::error_code& ec)
{
    if (OSStatus status = SSLSetPeerDomainName(m_ssl.get(), host_name.data(), host_name.size()))
        ec = std::error_code(status, secure_transport_error_category);
}

void Stream::ssl_use_verify_callback(const std::function<SSLVerifyCallback>&, std::error_code&) {}

void Stream::ssl_handshake(std::error_code& ec, Want& want) noexcept
{
    auto perform = [this]() noexcept {
        return do_ssl_handshake();
    };
    ssl_perform(std::move(perform), ec, want);
}

std::pair<OSStatus, std::size_t> Stream::do_ssl_handshake() noexcept
{
    OSStatus result = SSLHandshake(m_ssl.get());
    if (result != errSSLPeerAuthCompleted) {
        return {result, 0};
    }

    if (OSStatus status = verify_peer()) {
        // When performing peer verification internally, verification failure results in SecureTransport
        // sending a fatal alert to the peer, closing the connection. Sadly SecureTransport has no way
        // to explicitly send a fatal alert when trust evaluation is handled externally. The best we can
        // do is close the connection gracefully.
        SSLClose(m_ssl.get());
        return {status, 0};
    }

    // Verification suceeded. Resume the handshake.
    return do_ssl_handshake();
}


OSStatus Stream::verify_peer() noexcept
{
    switch (m_verify_mode) {
        case VerifyMode::none:
            // Peer verification is disabled.
            return noErr;

        case VerifyMode::peer: {
            SecTrustRef peerTrustRef = nullptr;
            if (OSStatus status = SSLCopyPeerTrust(m_ssl.get(), &peerTrustRef)) {
                return status;
            }

            auto peerTrust = util::adoptCF(peerTrustRef);

            if (m_ssl_context.m_trust_anchors) {
                if (OSStatus status =
                        SecTrustSetAnchorCertificates(peerTrust.get(), m_ssl_context.m_trust_anchors.get())) {
                    return status;
                }
                if (OSStatus status = SecTrustSetAnchorCertificatesOnly(peerTrust.get(), true)) {
                    return status;
                }
            }

            // FIXME: SecTrustEvaluate can block if evaluation needs to fetch missing intermediate
            // certificates or to check revocation using OCSP. Consider disabling these network
            // fetches or doing async trust evaluation instead.
#if __has_builtin(__builtin_available)
            if (__builtin_available(iOS 12.0, macOS 10.14, tvOS 12.0, watchOS 5.0, *)) {
                CFErrorRef cfErrorRef;
                if (!SecTrustEvaluateWithError(peerTrust.get(), &cfErrorRef)) {
                    auto cfError = util::adoptCF(cfErrorRef);
                    if (logger) {
                        auto errorStr = util::adoptCF(CFErrorCopyDescription(cfErrorRef));
                        logger->debug("SSL peer verification failed: %1", cfstring_to_std_string(errorStr.get()));
                    }
                    return errSSLXCertChainInvalid;
                }
            }
            else
#endif
            {
                SecTrustResultType trustResult;
                if (OSStatus status = SecTrustEvaluate(peerTrust.get(), &trustResult)) {
                    return status;
                }

                // A "proceed" result means the cert is explicitly trusted, e.g. "Always Trust" was selected.
                // "Unspecified" means the cert has no explicit trust settings, but is implicitly OK since it
                // chains back to a trusted root. Any other result means the cert is not trusted.
                if (trustResult == kSecTrustResultRecoverableTrustFailure) {
                    // Not trusted.
                    return errSSLXCertChainInvalid;
                }
                if (trustResult != kSecTrustResultProceed && trustResult != kSecTrustResultUnspecified) {
                    return errSSLBadCert;
                }
            }

            if (!m_ssl_context.m_pinned_certificate) {
                // Certificate is trusted!
                return noErr;
            }

            // Verify that the certificate is one of our pinned certificates
            // Loop backwards as the pinned certificate will normally be the last one
            for (CFIndex i = SecTrustGetCertificateCount(peerTrust.get()); i > 0; --i) {
                SecCertificateRef certificate = SecTrustGetCertificateAtIndex(peerTrust.get(), i - 1);
                auto certificate_data = adoptCF(SecCertificateCopyData(certificate));
                if (CFEqual(certificate_data.get(), m_ssl_context.m_pinned_certificate.get())) {
                    return noErr;
                }
            }

            // Although the cerificate is valid, it's not the one we've pinned so reject it.
            return errSSLXCertChainInvalid;
        }
    }
}


std::size_t Stream::ssl_read(char* buffer, std::size_t size, std::error_code& ec, Want& want) noexcept
{
    auto perform = [this, buffer, size]() noexcept {
        return do_ssl_read(buffer, size);
    };
    std::size_t n = ssl_perform(std::move(perform), ec, want);
    if (want == Want::nothing && n == 0 && !ec) {
        // End of input on TCP socket
        SSLSessionState state;
        if (SSLGetSessionState(m_ssl.get(), &state) == noErr && state == kSSLClosed) {
            ec = MiscExtErrors::end_of_input;
        }
        else {
            ec = MiscExtErrors::premature_end_of_input;
        }
    }
    return n;
}

std::pair<OSStatus, std::size_t> Stream::do_ssl_read(char* buffer, std::size_t size) noexcept
{
    std::size_t processed = 0;
    OSStatus result = SSLRead(m_ssl.get(), buffer, size, &processed);
    return {result, processed};
}


std::size_t Stream::ssl_write(const char* data, std::size_t size, std::error_code& ec, Want& want) noexcept
{
    auto perform = [this, data, size]() noexcept {
        return do_ssl_write(data, size);
    };
    std::size_t n = ssl_perform(std::move(perform), ec, want);
    if (want == Want::nothing && n == 0 && !ec) {
        // End of input on TCP socket
        ec = MiscExtErrors::premature_end_of_input;
    }
    return n;
}

std::pair<OSStatus, std::size_t> Stream::do_ssl_write(const char* data, std::size_t size) noexcept
{
    m_last_error = {};

    REALM_ASSERT(size >= m_num_partially_written_bytes);
    data += m_num_partially_written_bytes;
    size -= m_num_partially_written_bytes;

    std::size_t processed = 0;
    OSStatus result = SSLWrite(m_ssl.get(), data, size, &processed);

    if (result != noErr) {
        // Map errors that indicate the connection is closed to broken_pipe, for
        // consistency with OpenSSL.
        if (REALM_LIKELY(result == errSSLWouldBlock)) {
            m_num_partially_written_bytes += processed;
        }
        else if (result == errSSLClosedGraceful || result == errSSLClosedAbort || result == errSSLClosedNoNotify) {
            result = errSecIO;
            m_last_error = error::broken_pipe;
        }
        processed = 0;
    }
    else {
        processed += m_num_partially_written_bytes;
        m_num_partially_written_bytes = 0;
    }

    return {result, processed};
}


bool Stream::ssl_shutdown(std::error_code& ec, Want& want) noexcept
{
    auto perform = [this]() noexcept {
        return do_ssl_shutdown();
    };
    std::size_t n = ssl_perform(std::move(perform), ec, want);
    REALM_ASSERT(n == 0 || n == 1);
    return (n > 0);
}

std::pair<OSStatus, std::size_t> Stream::do_ssl_shutdown() noexcept
{
    SSLSessionState previousState;
    if (OSStatus result = SSLGetSessionState(m_ssl.get(), &previousState)) {
        return {result, false};
    }
    if (OSStatus result = SSLClose(m_ssl.get())) {
        return {result, false};
    }

    // SSLClose returns noErr if it encountered an I/O error. We can still
    // detect such errors if they originated from our underlying tcp_read /
    // tcp_write functions as we'll have set m_last_error in such cases. This
    // allows us to reconstruct the I/O error and communicate it to our caller.
    if (m_last_error) {
        return {errSecIO, false};
    }
    return {noErr, previousState == kSSLClosed};
}


OSStatus Stream::tcp_read(SSLConnectionRef connection, void* data, std::size_t* length) noexcept
{
    return static_cast<Stream*>(const_cast<void*>(connection))->tcp_read(data, length);
}

OSStatus Stream::tcp_read(void* data, std::size_t* length) noexcept
{
    Service::Descriptor& desc = m_tcp_socket.m_desc;
    std::error_code ec;
    std::size_t bytes_read = desc.read_some(reinterpret_cast<char*>(data), *length, ec);

    // A successful but short read should be treated the same as EAGAIN.
    if (!ec && bytes_read < *length) {
        ec = error::resource_unavailable_try_again;
    }

    *length = bytes_read;
    m_last_error = ec;

    if (ec) {
        if (REALM_UNLIKELY(ec == MiscExtErrors::end_of_input)) {
            return noErr;
        }
        if (ec == error::resource_unavailable_try_again) {
            m_last_operation = BlockingOperation::read;
            return errSSLWouldBlock;
        }
        return errSecIO;
    }
    return noErr;
}

OSStatus Stream::tcp_write(SSLConnectionRef connection, const void* data, std::size_t* length) noexcept
{
    return static_cast<Stream*>(const_cast<void*>(connection))->tcp_write(data, length);
}

OSStatus Stream::tcp_write(const void* data, std::size_t* length) noexcept
{
    Service::Descriptor& desc = m_tcp_socket.m_desc;
    std::error_code ec;
    std::size_t bytes_written = desc.write_some(reinterpret_cast<const char*>(data), *length, ec);

    // A successful but short write should be treated the same as EAGAIN.
    if (!ec && bytes_written < *length) {
        ec = error::resource_unavailable_try_again;
    }

    *length = bytes_written;
    m_last_error = ec;

    if (ec) {
        if (ec == error::resource_unavailable_try_again) {
            m_last_operation = BlockingOperation::write;
            return errSSLWouldBlock;
        }
        return errSecIO;
    }
    return noErr;
}


#else // !REALM_HAVE_OPENSSL && !REALM_HAVE_SECURE_TRANSPORT


void Context::ssl_init()
{
    throw ProtocolNotSupported();
}


void Context::ssl_destroy() noexcept {}


void Stream::ssl_init() {}


void Stream::ssl_destroy() noexcept {}


void Context::ssl_use_certificate_chain_file(const std::string&, std::error_code&) {}


void Context::ssl_use_private_key_file(const std::string&, std::error_code&) {}


void Context::ssl_use_default_verify(std::error_code&) {}


void Context::ssl_use_verify_file(const std::string&, std::error_code&) {}


void Stream::ssl_set_verify_mode(VerifyMode, std::error_code&) {}


void Stream::ssl_set_host_name(const std::string&, std::error_code&) {}


void Stream::ssl_use_verify_callback(const std::function<SSLVerifyCallback>&, std::error_code&) {}


void Stream::ssl_handshake(std::error_code&, Want&) noexcept {}


std::size_t Stream::ssl_read(char*, std::size_t, std::error_code&, Want&) noexcept
{
    return 0;
}


std::size_t Stream::ssl_write(const char*, std::size_t, std::error_code&, Want&) noexcept
{
    return 0;
}


bool Stream::ssl_shutdown(std::error_code&, Want&) noexcept
{
    return false;
}


bool is_server_cert_rejected_error(std::error_code&)
{
    return false;
}

#endif // ! REALM_HAVE_OPENSSL


} // namespace ssl
} // namespace network
} // namespace util
} // namespace realm
