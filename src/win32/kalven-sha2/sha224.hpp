// SHA-224. Adapted from LibTomCrypt. This code is Public Domain
#ifndef REALM_SHA224_HPP
#define REALM_SHA224_HPP

// SHA-224 is a truncated SHA-256 with different input vector.
#include "sha256.hpp"

struct sha224_state
{
    sha256_state md;
};

void sha_init(sha224_state& md);
void sha_process(sha224_state& md, const void* in, std::uint32_t inlen);
void sha_done(sha224_state& md, void* out);

#endif