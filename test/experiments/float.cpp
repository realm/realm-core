#include <cstdio>
#include <stdint.h>

union Float_t
{
    Float_t(float num = 0.0f) : f(num) {}
    // Portable extraction of components.
    bool Negative() const { return (i >> 31) != 0; }
    int32_t RawMantissa() const { return i & ((1 << 23) - 1); }
    int32_t RawExponent() const { return (i >> 23) & 0xFF; }
 
    int32_t i;
    float f;
#ifdef _DEBUG
    struct
    {   // Bitfields for exploration. Do not use in production code.
        uint32_t mantissa : 23;
        uint32_t exponent : 8;
        uint32_t sign : 1;
    } parts;
#endif
};

void exploreFloat()
{
    Float_t num(1.0f);
    num.i -= 1;
    printf("Float value, representation, sign, exponent, mantissa\n");
    for (int i=0; i<100;i++)
    {
        // Breakpoint here.
        printf("%1.8e, 0x%08X, %d, %d, 0x%06X\n",
            num.f, num.i,
            num.parts.sign, num.parts.exponent, num.parts.mantissa);
    }
}