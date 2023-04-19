/******************************************************************************
  Copyright (c) 2018, Intel Corp.
  All rights reserved.

  Redistribution and use in source and binary forms, with or without 
  modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice, 
      this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright 
      notice, this list of conditions and the following disclaimer in the 
      documentation and/or other materials provided with the distribution.
    * Neither the name of Intel Corporation nor the names of its contributors 
      may be used to endorse or promote products derived from this software 
      without specific prior written permission.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
  THE POSSIBILITY OF SUCH DAMAGE.
******************************************************************************/

#include "bid_internal.h"

/*****************************************************************************
 *  BID64_quantumd
 ****************************************************************************/

/*
 Exceptions signaled: none

 The quantumdN functions compute the quantum of a finite argument. 
 If x is infinite, the result is +Inf. If x is NaN, the result is NaN.
*/


BID_TYPE0_FUNCTION_ARGTYPE1_NORND_DFP(BID_UINT64, bid64_quantum, BID_UINT64, x)

  BID_UINT64 res;
  int int_exp;

  // If x is infinite, the result is +Inf. If x is NaN, the result is NaN
  if ((x & MASK_INF) == MASK_INF) {
    res = x & ~SIGNMASK64;
    BID_RETURN (res);
  }
  else if ((x & MASK_NAN) == MASK_NAN) {
    res = x & QUIET_MASK64;
    BID_RETURN (res);
  }

  // Extract exponent
  if ((x & MASK_STEERING_BITS) == MASK_STEERING_BITS) {
    int_exp = (int)((x >> 51) & 0x3ff) - 398;
  }
  else {
    int_exp = ((int)(x >> 53) & 0x3ff) - 398;
  }

  res = (((long long int) int_exp) << 53 ) + 0x31c0000000000001ull;

  BID_RETURN (res);

}

