/*************************************************************************
 *
 * Copyright 2019 Realm Inc.
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

#ifndef REALM_UTIL_FIFO_HELPER_HPP
#define REALM_UTIL_FIFO_HELPER_HPP

#include <string>

namespace realm {
namespace util {

// Attempts to create a FIFO file at the location determined by `path`.
// If the filesystem does not allow FIFO's at this location the path is hashed
// and used as the file name for the FIFO which is then created in `tmp_path`.
// This should usually be set to the systems global tmp folder.
//
// The `tmp_dir` must point to an existing writable directory. The path should end with a
// trailing `/`.
//
// If creating the FIFO at both of these locations, an exception is thrown.
// If a FIFO already exists at the given location, this method does nothing.
void create_fifo(std::string path, const std::string tmp_dir); // throws

} // namespace util
} // namespace realm

#endif // REALM_UTIL_FIFO_HELPER_HPP
