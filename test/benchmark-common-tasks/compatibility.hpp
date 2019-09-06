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

#include <realm.hpp>

namespace compatibility {

/// This shadows SharedGroupOptions::Durability
/// The indirection is necessary because old versions
/// of core should still be able to compile with this
/// benchmark test.
enum class RealmDurability {
    Full,
    MemOnly,
    Async
};

#ifdef REALM_CLUSTER_IF
using RdTrans = realm::ReadTransaction;
using WrtTrans = realm::WriteTransaction;
using DBRef = realm::DBRef;
#else
#include <realm/group_shared.hpp>
#define DB SharedGroup
#define DBOptions SharedGroupOptions
using DBRef = std::shared_ptr<realm::SharedGroup>;
class RdTrans : public realm::ReadTransaction {
public:
    RdTrans(DBRef ref)
        : realm::ReadTransaction(*ref){};
};

class WrtTrans : public realm::WriteTransaction {
public:
    WrtTrans(DBRef ref)
        : realm::WriteTransaction(*ref){};
};
using TransactionRef = std::unique_ptr<WrtTrans>;
#endif

DBRef create_new_shared_group(std::string path, RealmDurability level, const char* key);

} // end namespace compatibility

