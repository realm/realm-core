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

#ifndef __DB_HPP__
#define __DB_HPP__

#include "snapshot.hpp"

// interface class

class Db {
public:
    static Db& create(const char* fname);
    virtual const Snapshot& open_snapshot() = 0; // extend with version number
    virtual void release(const Snapshot&&) = 0;
    // build changes upon newest snapshot
    virtual Snapshot& create_changes() = 0;
    virtual void abort(Snapshot&&) = 0;
    virtual void commit(Snapshot&&) = 0;
};

#endif
