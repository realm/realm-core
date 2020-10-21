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

#define BID_128RES
#include "bid_internal.h"
#include "bid_trans.h"

/*****************************************************************************
 *  BID64_quantumd
 ****************************************************************************/

/*
 Exceptions signaled: none

 The quantumdN functions compute the quantum of a finite argument. 
 If x is infinite, the result is +Inf. If x is NaN, the result is NaN.
*/


BID128_FUNCTION_ARG1_NORND (bid128_quantum, x)

  BID_UINT128 res;
  int int_exp;


  // If x is infinite, the result is +Inf. If x is NaN, the result is NaN
  if ((x.w[1] & MASK_ANY_INF) == MASK_INF) {
    res.w[1] = 0x7800000000000000ull;
    res.w[0] = 0x0000000000000000ull;
    BID_RETURN (res);
  }
  else if ((x.w[1] & NAN_MASK64) == NAN_MASK64) {
    res.w[1] = x.w[1] & QUIET_MASK64;
    BID_RETURN (res);
  }

  // Extract exponent
  if ((x.w[1] & MASK_STEERING_BITS) == MASK_STEERING_BITS) {
    int_exp = (int)((x.w[1] >> 47) & 0x3fff) - 6176;
  }
  else {
    int_exp = ((int)(x.w[1] >> 49) & 0x3fff) - 6176;
  }

  // Form 10^new_exponent*1  
  res.w[1] = (((long long int) int_exp) << 49 ) + 0x3040000000000000ull;
  res.w[0] = 0x0000000000000001ull;

  BID_RETURN (res);

}

