#include <realm/sync/noinst/server/crypto_server.hpp>

#include <openssl/bio.h>
#include <openssl/pem.h>
#include <openssl/evp.h>
#include <openssl/rsa.h>

using namespace realm;
using namespace realm::sync;

namespace {

template <class T, class D>
std::unique_ptr<T, D> as_unique_ptr(T* ptr, D&& deleter)
{
    return std::unique_ptr<T, D>{ptr, std::forward<D>(deleter)};
}

} // namespace

using key_type = std::unique_ptr<EVP_PKEY, void (*)(EVP_PKEY*)>;

struct PKey::Impl {
    key_type key;
    bool both_parts; // true if both public and private key are loaded

    Impl()
        : key(nullptr, nullptr)
    {
    }
};

PKey::PKey()
    : m_impl(new Impl)
{
    m_impl->key = nullptr;
}

PKey::PKey(PKey&&) = default;
PKey& PKey::operator=(PKey&&) = default;

PKey::~PKey() {}

static key_type load_public_from_bio(BIO* bio)
{
    pem_password_cb* password_cb = nullptr; // OpenSSL will display a prompt if necessary
    void* password_cb_userdata = nullptr;

    void (*rsa_free)(RSA*) = RSA_free; // silences a warning on VS2017
    auto rsa = as_unique_ptr(PEM_read_bio_RSA_PUBKEY(bio, nullptr, password_cb, password_cb_userdata), rsa_free);
    if (rsa == nullptr)
        throw CryptoError{"Not a valid RSA public key."};

    void (*evp_pkey_free)(EVP_PKEY*) = EVP_PKEY_free; // silences a warning on VS2017
    key_type key = as_unique_ptr(EVP_PKEY_new(), evp_pkey_free);
    if (EVP_PKEY_assign_RSA(key.get(), rsa.get()) == 0)
        throw CryptoError{"Error assigning RSA key."};
    rsa.release();

    return key;
}

PKey PKey::load_public(const std::string& pemfile)
{
    int (*bio_free)(BIO*) = BIO_free; // silences warning on VS2017
    auto bio = as_unique_ptr(BIO_new_file(pemfile.c_str(), "r"), bio_free);
    if (bio == nullptr)
        throw CryptoError{std::string("Could not read PEM file: ") + pemfile};

    PKey result;
    result.m_impl->key = load_public_from_bio(bio.get());
    result.m_impl->both_parts = false;

    return result;
}

PKey PKey::load_public(BinaryData pem_buffer)
{
    std::size_t size = pem_buffer.size();
    int (*bio_free)(BIO*) = BIO_free; // silences a warning on VS2017
    REALM_ASSERT_RELEASE(int(size) <= std::numeric_limits<int>::max());
    auto bio = as_unique_ptr(BIO_new_mem_buf(const_cast<char*>(pem_buffer.data()), int(size)), bio_free);

    PKey result;
    result.m_impl->key = load_public_from_bio(bio.get());
    result.m_impl->both_parts = false;

    return result;
}

bool PKey::can_sign() const noexcept
{
    return m_impl->both_parts;
}

bool PKey::can_verify() const noexcept
{
    return true;
}

bool PKey::verify(BinaryData message, BinaryData signature) const
{
    if (!can_verify()) {
        throw CryptoError{"Cannot verify (no public key)."};
    }
    const EVP_MD* digest = EVP_sha256();

    EVP_MD_CTX* ctx = EVP_MD_CTX_create();

    EVP_VerifyInit(ctx, digest);
    EVP_VerifyUpdate(ctx, message.data(), message.size());

    const unsigned char* sig = reinterpret_cast<const unsigned char*>(signature.data());
    std::size_t size = signature.size();
    REALM_ASSERT_RELEASE(int(size) <= std::numeric_limits<int>::max());
    int ret = EVP_VerifyFinal(ctx, sig, int(size), m_impl->key.get());

    EVP_MD_CTX_destroy(ctx);

    if (ret < 0)
        throw CryptoError{"Error verifying message."};
    return ret == 1;
}
