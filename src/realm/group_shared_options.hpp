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

#ifndef REALM_GROUP_SHARED_OPTIONS_HPP
#define REALM_GROUP_SHARED_OPTIONS_HPP

namespace realm {

struct SharedGroupOptions {
    enum Durability {
        durability_Full,
        durability_MemOnly,
        durability_Async    ///< Not yet supported on windows.
    };
    Durability durability = durability_Full;
    const char* encryption_key = nullptr;
    bool allow_file_format_upgrade = true;
    std::function<void(int,int)> upgrade_callback = std::function<void(int,int)>();
    std::string temp_dir = sys_tmp_dir;
private:
    const static std::string sys_tmp_dir;
};

} // end namespace realm

#endif // REALM_GROUP_SHARED_OPTIONS_HPP
