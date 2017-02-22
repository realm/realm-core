/*************************************************************************
 *
 * Copyright 2017 Realm Inc.
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

#import <Foundation/Foundation.h>

#include "util/test_path.hpp"
#include "test_all.hpp"

int main(int argc, const char *argv[])
{
    std::string tmp_dir{[NSTemporaryDirectory() UTF8String]};
    realm::test_util::set_test_path_prefix(tmp_dir);

    std::string resource_path{[[[NSBundle mainBundle] resourcePath] UTF8String]};
    realm::test_util::set_test_resource_path(resource_path + "/");

    test_all(0, nullptr);
}
