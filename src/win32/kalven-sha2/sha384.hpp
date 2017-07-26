// SHA-384. Adapted from LibTomCrypt. This code is Public Domain
#pragma once

// SHA-384 is a truncated SHA-512 with different input vector.
#include "sha512.hpp"

struct sha384_state
{
    sha512_state md;
};

void sha_init(sha384_state& md);
void sha_process(sha384_state& md, const void* in, std::uint32_t inlen);
void sha_done(sha384_state& md, void* out);
