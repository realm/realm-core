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

#ifndef __PAYLOAD_HPP__
#define __PAYLOAD_HPP__

#include "refs.hpp"

struct PayloadMgr {
    virtual void cow(Ref<DynType>& payload, int old_capacity, int new_capacity) = 0;
    virtual void free(Ref<DynType> payload, int capacity) = 0;
    virtual void read_internalbuffer(Ref<DynType> payload, int from) = 0;
    virtual void write_internalbuffer(Ref<DynType>& payload, int to, int capacity) = 0;
    virtual void init_internalbuffer() = 0;
    virtual void swap_internalbuffer(Ref<DynType>& payload, int index, int capacity) = 0;
    virtual Ref<DynType> commit(Ref<DynType> payload) = 0;
};

#endif
