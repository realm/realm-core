// SHA-256. Adapted from LibTomCrypt. This code is Public Domain
#ifndef REALM_SHA256_HPP
#define REALM_SHA256_HPP

#include <cstdint>

struct sha256_state
{
    std::uint64_t length;
    std::uint32_t state[8];
    std::uint32_t curlen;
    unsigned char buf[64];
};

void sha_init(sha256_state& md);
void sha_process(sha256_state& md, const void* in, std::uint32_t inlen);
void sha_done(sha256_state& md, void* out);

#endif
