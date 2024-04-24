/*************************************************************************
 *
 * Copyright 2020 Realm Inc.
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

#ifndef REALM_UTIL_CF_STR_HPP
#define REALM_UTIL_CF_STR_HPP

#include <realm/util/features.h>

#if REALM_PLATFORM_APPLE

#include <realm/util/cf_ptr.hpp>

namespace realm::util {

inline const char* cfstring_to_cstring(CFStringRef cf_str, std::unique_ptr<char[]>& buffer)
{
    // If the CFString happens to store UTF-8 we can read its data directly
    if (const char* utf8 = CFStringGetCStringPtr(cf_str, kCFStringEncodingUTF8)) {
        return utf8;
    }

    // Otherwise we need to convert the CFString to UTF-8
    CFIndex length = CFStringGetLength(cf_str);
    CFIndex max_size = CFStringGetMaximumSizeForEncoding(length, kCFStringEncodingUTF8) + 1;
    buffer = std::make_unique<char[]>(max_size);
    CFStringGetCString(cf_str, buffer.get(), max_size, kCFStringEncodingUTF8);
    return buffer.get();
}

inline CFPtr<CFStringRef> string_view_to_cfstring(std::string_view string)
{
    if (!string.data()) {
        return CFPtr<CFStringRef>{};
    }
    auto result =
        adoptCF(CFStringCreateWithBytesNoCopy(kCFAllocatorDefault, reinterpret_cast<const UInt8*>(string.data()),
                                              string.size(), kCFStringEncodingUTF8, false, kCFAllocatorNull));
    if (!result) {
        throw std::bad_alloc();
    }
    return result;
}

} // namespace realm::util

#endif // REALM_PLATFORM_APPLE

#endif // REALM_UTIL_CF_STR_HPP
