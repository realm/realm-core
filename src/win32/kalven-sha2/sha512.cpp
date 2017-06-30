// SHA-512. Adapted from LibTomCrypt. This code is Public Domain
#include "sha512.hpp"

#include <cstring>

typedef std::uint32_t u32;
typedef std::uint64_t u64;

static const u64 K[80] =
{
    0x428a2f98d728ae22ULL, 0x7137449123ef65cdULL, 0xb5c0fbcfec4d3b2fULL, 0xe9b5dba58189dbbcULL, 0x3956c25bf348b538ULL,
    0x59f111f1b605d019ULL, 0x923f82a4af194f9bULL, 0xab1c5ed5da6d8118ULL, 0xd807aa98a3030242ULL, 0x12835b0145706fbeULL,
    0x243185be4ee4b28cULL, 0x550c7dc3d5ffb4e2ULL, 0x72be5d74f27b896fULL, 0x80deb1fe3b1696b1ULL, 0x9bdc06a725c71235ULL,
    0xc19bf174cf692694ULL, 0xe49b69c19ef14ad2ULL, 0xefbe4786384f25e3ULL, 0x0fc19dc68b8cd5b5ULL, 0x240ca1cc77ac9c65ULL,
    0x2de92c6f592b0275ULL, 0x4a7484aa6ea6e483ULL, 0x5cb0a9dcbd41fbd4ULL, 0x76f988da831153b5ULL, 0x983e5152ee66dfabULL,
    0xa831c66d2db43210ULL, 0xb00327c898fb213fULL, 0xbf597fc7beef0ee4ULL, 0xc6e00bf33da88fc2ULL, 0xd5a79147930aa725ULL,
    0x06ca6351e003826fULL, 0x142929670a0e6e70ULL, 0x27b70a8546d22ffcULL, 0x2e1b21385c26c926ULL, 0x4d2c6dfc5ac42aedULL,
    0x53380d139d95b3dfULL, 0x650a73548baf63deULL, 0x766a0abb3c77b2a8ULL, 0x81c2c92e47edaee6ULL, 0x92722c851482353bULL,
    0xa2bfe8a14cf10364ULL, 0xa81a664bbc423001ULL, 0xc24b8b70d0f89791ULL, 0xc76c51a30654be30ULL, 0xd192e819d6ef5218ULL,
    0xd69906245565a910ULL, 0xf40e35855771202aULL, 0x106aa07032bbd1b8ULL, 0x19a4c116b8d2d0c8ULL, 0x1e376c085141ab53ULL,
    0x2748774cdf8eeb99ULL, 0x34b0bcb5e19b48a8ULL, 0x391c0cb3c5c95a63ULL, 0x4ed8aa4ae3418acbULL, 0x5b9cca4f7763e373ULL,
    0x682e6ff3d6b2b8a3ULL, 0x748f82ee5defb2fcULL, 0x78a5636f43172f60ULL, 0x84c87814a1f0ab72ULL, 0x8cc702081a6439ecULL,
    0x90befffa23631e28ULL, 0xa4506cebde82bde9ULL, 0xbef9a3f7b2c67915ULL, 0xc67178f2e372532bULL, 0xca273eceea26619cULL,
    0xd186b8c721c0c207ULL, 0xeada7dd6cde0eb1eULL, 0xf57d4f7fee6ed178ULL, 0x06f067aa72176fbaULL, 0x0a637dc5a2c898a6ULL,
    0x113f9804bef90daeULL, 0x1b710b35131c471bULL, 0x28db77f523047d84ULL, 0x32caab7b40c72493ULL, 0x3c9ebe0a15c9bebcULL,
    0x431d67c49c100d4cULL, 0x4cc5d4becb3e42b6ULL, 0x597f299cfc657e2aULL, 0x5fcb6fab3ad6faecULL, 0x6c44198c4a475817ULL
};

static u32 min(u32 x, u32 y)
{
    return x < y ? x : y;
}

static void store64(u64 x, unsigned char* y)
{
    for(int i = 0; i != 8; ++i)
        y[i] = (x >> ((7-i) * 8)) & 255;
}

static u64 load64(const unsigned char* y)
{
    u64 res = 0;
    for(int i = 0; i != 8; ++i)
        res |= u64(y[i]) << ((7-i) * 8);
    return res;
}

static u64 Ch(u64 x, u64 y, u64 z)  { return z ^ (x & (y ^ z)); }
static u64 Maj(u64 x, u64 y, u64 z) { return ((x | y) & z) | (x & y); }
static u64 Rot(u64 x, u64 n)        { return (x >> (n & 63)) | (x << (64 - (n & 63))); }
static u64 Sh(u64 x, u64 n)         { return x >> n; }
static u64 Sigma0(u64 x)            { return Rot(x, 28) ^ Rot(x, 34) ^ Rot(x, 39); }
static u64 Sigma1(u64 x)            { return Rot(x, 14) ^ Rot(x, 18) ^ Rot(x, 41); }
static u64 Gamma0(u64 x)            { return Rot(x, 1) ^ Rot(x, 8) ^ Sh(x, 7); }
static u64 Gamma1(u64 x)            { return Rot(x, 19) ^ Rot(x, 61) ^ Sh(x, 6); }

static void sha_compress(sha512_state& md, const unsigned char *buf)
{
    u64 S[8], W[80], t0, t1;

    // Copy state into S
    for(int i = 0; i < 8; i++)
        S[i] = md.state[i];

    // Copy the state into 1024-bits into W[0..15]
    for(int i = 0; i < 16; i++)
        W[i] = load64(buf + (8*i));

    // Fill W[16..79]
    for(int i = 16; i < 80; i++)
        W[i] = Gamma1(W[i - 2]) + W[i - 7] + Gamma0(W[i - 15]) + W[i - 16];

    // Compress
    auto RND = [&](u64 a, u64 b, u64 c, u64& d, u64 e, u64 f, u64 g, u64& h, u64 i)
    {
        t0 = h + Sigma1(e) + Ch(e, f, g) + K[i] + W[i];
        t1 = Sigma0(a) + Maj(a, b, c);
        d += t0;
        h  = t0 + t1;
    };

    for(int i = 0; i < 80; i += 8)
    {
        RND(S[0],S[1],S[2],S[3],S[4],S[5],S[6],S[7],i+0);
        RND(S[7],S[0],S[1],S[2],S[3],S[4],S[5],S[6],i+1);
        RND(S[6],S[7],S[0],S[1],S[2],S[3],S[4],S[5],i+2);
        RND(S[5],S[6],S[7],S[0],S[1],S[2],S[3],S[4],i+3);
        RND(S[4],S[5],S[6],S[7],S[0],S[1],S[2],S[3],i+4);
        RND(S[3],S[4],S[5],S[6],S[7],S[0],S[1],S[2],i+5);
        RND(S[2],S[3],S[4],S[5],S[6],S[7],S[0],S[1],i+6);
        RND(S[1],S[2],S[3],S[4],S[5],S[6],S[7],S[0],i+7);
    }

     // Feedback
     for(int i = 0; i < 8; i++)
         md.state[i] = md.state[i] + S[i];
}

// Public interface

void sha_init(sha512_state& md)
{
    md.curlen = 0;
    md.length = 0;
    md.state[0] = 0x6a09e667f3bcc908ULL;
    md.state[1] = 0xbb67ae8584caa73bULL;
    md.state[2] = 0x3c6ef372fe94f82bULL;
    md.state[3] = 0xa54ff53a5f1d36f1ULL;
    md.state[4] = 0x510e527fade682d1ULL;
    md.state[5] = 0x9b05688c2b3e6c1fULL;
    md.state[6] = 0x1f83d9abfb41bd6bULL;
    md.state[7] = 0x5be0cd19137e2179ULL;
}

void sha_process(sha512_state& md, const void* src, u32 inlen)
{
    const u32 block_size = sizeof(sha512_state::buf);
    auto in = static_cast<const unsigned char*>(src);

    while(inlen > 0)
    {
        if(md.curlen == 0 && inlen >= block_size)
        {
            sha_compress(md, in);
            md.length += block_size * 8;
            in        += block_size;
            inlen     -= block_size;
        }
        else
        {
            u32 n = min(inlen, (block_size - md.curlen));
            std::memcpy(md.buf + md.curlen, in, n);
            md.curlen += n;
            in        += n;
            inlen     -= n;

            if(md.curlen == block_size)
            {
                sha_compress(md, md.buf);
                md.length += 8*block_size;
                md.curlen = 0;
            }
        }
    }
}

void sha_done(sha512_state& md, void *out)
{
    // Increase the length of the message
    md.length += md.curlen * 8ULL;

    // Append the '1' bit
    md.buf[md.curlen++] = static_cast<unsigned char>(0x80);

    // If the length is currently above 112 bytes we append zeros then compress.
    // Then we can fall back to padding zeros and length encoding like normal.
    if(md.curlen > 112)
    {
        while(md.curlen < 128)
            md.buf[md.curlen++] = 0;
        sha_compress(md, md.buf);
        md.curlen = 0;
    }

    // Pad upto 120 bytes of zeroes
    // note: that from 112 to 120 is the 64 MSB of the length.  We assume that
    // you won't hash 2^64 bits of data... :-)
    while(md.curlen < 120)
        md.buf[md.curlen++] = 0;

    // Store length
    store64(md.length, md.buf+120);
    sha_compress(md, md.buf);

    // Copy output
    for(int i = 0; i < 8; i++)
        store64(md.state[i], static_cast<unsigned char*>(out)+(8*i));
}
