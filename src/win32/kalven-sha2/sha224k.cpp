// SHA-224. Adapted from LibTomCrypt. This code is Public Domain
#include "sha224.hpp"

#include <cstring>

void sha_init(sha224_state& md)
{
    md.md.curlen = 0;
    md.md.length = 0;
    md.md.state[0] = 0xc1059ed8UL;
    md.md.state[1] = 0x367cd507UL;
    md.md.state[2] = 0x3070dd17UL;
    md.md.state[3] = 0xf70e5939UL;
    md.md.state[4] = 0xffc00b31UL;
    md.md.state[5] = 0x68581511UL;
    md.md.state[6] = 0x64f98fa7UL;
    md.md.state[7] = 0xbefa4fa4UL;
}

void sha_process(sha224_state& md, const void* in, std::uint32_t inlen)
{
    sha_process(md.md, in, inlen);
}

void sha_done(sha224_state& md, void* out)
{
    unsigned char res[32];
    sha_done(md.md, res);
    std::memcpy(out, res, 28);
}
