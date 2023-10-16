/*************************************************************************
 *
 * Copyright 2023 Realm Inc.
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

#include <realm/array_encode.hpp>
#include <realm/array_flex.hpp>

using namespace realm;

ArrayEncode::ArrayEncode(Array& array)
    : Array(array.get_alloc())
    , m_array(array)
{
}

ArrayEncode* ArrayEncode::create_encoded_array(NodeHeader::Encoding encoding, Array& array)
{
    using Encoding = NodeHeader::Encoding;
    switch (encoding) {
        case Encoding::Flex:
            return new ArrayFlex(array); // TODO small ptr here?
        case Encoding::Packed:
        case Encoding::AofP:
        case Encoding::PofA:
        case Encoding::WTypBits:
        case Encoding::WTypMult:
        case Encoding::WTypIgn:
        default:
            return {}; // no other implementations for now.
    }
}