// SHA-384. Adapted from LibTomCrypt. This code is Public Domain
#include "sha384.hpp"

#include <cstring>

void sha_init(sha384_state& md)
{
    md.md.curlen = 0;
    md.md.length = 0;
    md.md.state[0] = 0xcbbb9d5dc1059ed8ULL;
    md.md.state[1] = 0x629a292a367cd507ULL;
    md.md.state[2] = 0x9159015a3070dd17ULL;
    md.md.state[3] = 0x152fecd8f70e5939ULL;
    md.md.state[4] = 0x67332667ffc00b31ULL;
    md.md.state[5] = 0x8eb44a8768581511ULL;
    md.md.state[6] = 0xdb0c2e0d64f98fa7ULL;
    md.md.state[7] = 0x47b5481dbefa4fa4ULL;
}

void sha_process(sha384_state& md, const void* in, std::uint32_t inlen)
{
    sha_process(md.md, in, inlen);
}

void sha_done(sha384_state& md, void* out)
{
    unsigned char res[64];
    sha_done(md.md, res);
    std::memcpy(out, res, 48);
}
