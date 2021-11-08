#include <realm/sync/noinst/server/crypto_server.hpp>

#include <realm/util/cf_ptr.hpp>
#include <realm/util/scope_exit.hpp>

#define __ASSERT_MACROS_DEFINE_VERSIONS_WITHOUT_UNDERSCORES 0

#include <CommonCrypto/CommonDigest.h>
#include <Foundation/Foundation.h>
#include <Security/Security.h>
#include <TargetConditionals.h>

using namespace realm;
using namespace realm::sync;

using util::CFPtr;
using util::adoptCF;

struct PKey::Impl {
  CFPtr<SecKeyRef> public_key;
  CFPtr<SecKeyRef> private_key;
};

PKey::PKey() : m_impl(std::make_unique<Impl>()) {}

PKey::PKey(PKey &&) = default;
PKey &PKey::operator=(PKey &&) = default;

PKey::~PKey() = default;

static CFPtr<SecKeyRef> load_public_from_data(CFDataRef pem_data) {
  CFArrayRef itemsCF = nullptr;
  auto scope_exit = util::make_scope_exit([&]() noexcept {
    if (itemsCF)
      CFRelease(itemsCF);
  });

  SecExternalFormat format = kSecFormatPEMSequence;
  SecExternalItemType itemType = kSecItemTypePublicKey;
  OSStatus status = SecItemImport(pem_data, CFSTR(".pem"), &format, &itemType,
                                  0, nullptr, nullptr, &itemsCF);
  if (status != errSecSuccess) {
    NSError *error = [NSError errorWithDomain:NSOSStatusErrorDomain
                                         code:status
                                     userInfo:nil];
    throw CryptoError(std::string("Could not import PEM data: ") +
                      error.localizedDescription.UTF8String);
  }
  if (CFArrayGetCount(itemsCF) != 1) {
    throw CryptoError(
        std::string("Loading PEM file produced unexpected number of keys."));
  }
  SecKeyRef key = static_cast<SecKeyRef>(
      const_cast<void *>(CFArrayGetValueAtIndex(itemsCF, 0)));
  if (CFGetTypeID(key) != SecKeyGetTypeID()) {
    throw CryptoError(
        std::string("Loading PEM file produced a key of unexpected type."));
  }

  return util::retainCF(key);
}

PKey PKey::load_public(const std::string &pemfile) {
  NSData *pem_data = [NSData dataWithContentsOfFile:@(pemfile.c_str())];
  if (!pem_data) {
    throw CryptoError(std::string("Could not load PEM file: " + pemfile));
  }

  PKey pkey;
  pkey.m_impl->public_key = load_public_from_data((__bridge CFDataRef)pem_data);
  return pkey;
}

PKey PKey::load_public(BinaryData pem_buffer) {
  CFPtr<CFDataRef> pem_data = adoptCF(CFDataCreateWithBytesNoCopy(
      kCFAllocatorDefault, reinterpret_cast<const UInt8 *>(pem_buffer.data()),
      pem_buffer.size(), kCFAllocatorNull));

  PKey pkey;
  pkey.m_impl->public_key =
      load_public_from_data(static_cast<CFDataRef>(pem_data.get()));
  return pkey;
}

bool PKey::can_sign() const noexcept {
  // Signing is not yet implemented.
  return false;
}

bool PKey::can_verify() const noexcept { return bool(m_impl->public_key); }

bool PKey::verify(BinaryData message, BinaryData signature) const {
  if (!can_verify()) {
    throw CryptoError{"Cannot verify (no public key)."};
  }

#if TARGET_OS_MAC

  CFPtr<CFDataRef> signatureCF = adoptCF(CFDataCreateWithBytesNoCopy(
      kCFAllocatorDefault, reinterpret_cast<const UInt8 *>(signature.data()),
      signature.size(), kCFAllocatorNull));
  CFPtr<CFDataRef> messageCF = adoptCF(CFDataCreateWithBytesNoCopy(
      kCFAllocatorDefault, reinterpret_cast<const UInt8 *>(message.data()),
      message.size(), kCFAllocatorNull));
  if (!signatureCF || !messageCF) {
    throw util::bad_alloc();
  }

  auto translateError = [](CFErrorRef error) -> CryptoError {
    auto errorCF = adoptCF(error);
    return CryptoError(std::string("Error verifying message: ") +
                       [(__bridge NSError *)error description].UTF8String);
  };

  CFErrorRef error = nullptr;
  CFPtr<SecTransformRef> verifier = adoptCF(SecVerifyTransformCreate(
      m_impl->public_key.get(), signatureCF.get(), &error));
  if (!verifier) {
    throw translateError(error);
  }

  if (!SecTransformSetAttribute(verifier.get(), kSecTransformInputAttributeName,
                                messageCF.get(), &error) ||
      !SecTransformSetAttribute(verifier.get(), kSecDigestTypeAttribute,
                                kSecDigestSHA2, &error) ||
      !SecTransformSetAttribute(verifier.get(), kSecDigestLengthAttribute,
                                (__bridge CFTypeRef) @256, &error)) {
    throw translateError(error);
  }

  CFPtr<CFTypeRef> result =
      adoptCF(SecTransformExecute(verifier.get(), &error));
  if (!result) {
    throw translateError(error);
  }
  return result.get() == kCFBooleanTrue ? true : false;

#else

  OSStatus status = SecKeyRawVerify(
      m_impl->public_key.get(), (SecPadding)kSecPaddingPKCS1SHA256,
      reinterpret_cast<const uint8_t *>(message.data()), message.size(),
      reinterpret_cast<const uint8_t *>(signature.data()), signature.size());
  return status == errSecSuccess;

#endif
}
