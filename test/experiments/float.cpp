/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <cstdio>
#include <cstdint>

union Float_t {
    Float_t(float num = 0.0f)
        : f(num)
    {
    }
    // Portable extraction of components.
    bool Negative() const
    {
        return (i >> 31) != 0;
    }
    int32_t RawMantissa() const
    {
        return i & ((1 << 23) - 1);
    }
    int32_t RawExponent() const
    {
        return (i >> 23) & 0xFF;
    }

    int32_t i;
    float f;
#ifdef _DEBUG
    struct {
        // Bitfields for exploration. Do not use in production code.
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
    for (int i = 0; i < 100; i++) {
        // Breakpoint here.
        printf("%1.8e, 0x%08X, %d, %d, 0x%06X\n", num.f, num.i, num.parts.sign, num.parts.exponent,
               num.parts.mantissa);
    }
}
